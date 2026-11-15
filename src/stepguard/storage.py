from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

from stepguard.exceptions import ConcurrentExecutionError

COMPLETED = "completed"
ROLLED_BACK = "rolled_back"
VALID_STATUSES = (COMPLETED, ROLLED_BACK)
DEFAULT_LOCK_TTL = 300.0  # seconds


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StepRecord:
    run_id: str
    step_name: str
    status: str
    output: Any = None
    created_at: str = field(default_factory=utc_now_iso)

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(
                f"Invalid step status {self.status!r}; expected one of {VALID_STATUSES}"
            )


class BaseStore(ABC):
    @abstractmethod
    def get_step(self, run_id: str, step_name: str) -> StepRecord | None: ...

    @abstractmethod
    def save_step(self, record: StepRecord) -> None:
        """Insert a step, or overwrite an existing one while keeping its original position."""

    @abstractmethod
    def get_run_steps(self, run_id: str) -> list[StepRecord]:
        """Return every step of a run in insertion order."""

    @abstractmethod
    def mark_rolled_back(self, run_id: str, step_name: str) -> None:
        """Set a step's status to rolled_back. Raises KeyError if the step does not exist."""

    # Locks expire lock_ttl seconds after they were acquired or last refreshed, so a run
    # left locked by a crashed worker can be resumed once its lock runs out.

    @abstractmethod
    def acquire_lock(self, run_id: str) -> str:
        """Claim a run and return an owner token; an expired lock is taken over.

        Raises ConcurrentExecutionError if the run holds a lock that has not expired.
        """

    @abstractmethod
    def refresh_lock(self, run_id: str, owner: str) -> None:
        """Extend a held lock. Raises ConcurrentExecutionError if `owner` no longer holds it."""

    @abstractmethod
    def release_lock(self, run_id: str, owner: str | None = None) -> None:
        """Release a run's lock, or with `owner`, only if that owner still holds it.

        Does nothing if the lock is not held.
        """

    def close(self) -> None:
        """Release any resources held by the store."""


def _missing_step(run_id: str, step_name: str) -> KeyError:
    return KeyError(f"No step {step_name!r} recorded for run {run_id!r}")


def _check_lock_ttl(lock_ttl: float) -> float:
    if lock_ttl <= 0:
        raise ValueError(f"lock_ttl must be positive, got {lock_ttl!r}")
    return lock_ttl


class MemoryStore(BaseStore):
    # Outputs are round-tripped through JSON so this store accepts and returns exactly
    # what SQLiteStore does, and callers never share mutable state with the store.

    def __init__(
        self, lock_ttl: float = DEFAULT_LOCK_TTL, clock: Callable[[], float] = time.time
    ) -> None:
        self.lock_ttl = _check_lock_ttl(lock_ttl)
        self._clock = clock
        self._lock = threading.Lock()
        self._runs: dict[str, dict[str, StepRecord]] = {}
        self._run_locks: dict[str, tuple[str, float]] = {}  # run_id -> (owner, expires_at)

    @staticmethod
    def _copy(record: StepRecord) -> StepRecord:
        return replace(record, output=json.loads(json.dumps(record.output)))

    def get_step(self, run_id: str, step_name: str) -> StepRecord | None:
        with self._lock:
            record = self._runs.get(run_id, {}).get(step_name)
            return self._copy(record) if record is not None else None

    def save_step(self, record: StepRecord) -> None:
        stored = self._copy(record)
        with self._lock:
            self._runs.setdefault(record.run_id, {})[record.step_name] = stored

    def get_run_steps(self, run_id: str) -> list[StepRecord]:
        with self._lock:
            return [self._copy(record) for record in self._runs.get(run_id, {}).values()]

    def mark_rolled_back(self, run_id: str, step_name: str) -> None:
        with self._lock:
            record = self._runs.get(run_id, {}).get(step_name)
            if record is None:
                raise _missing_step(run_id, step_name)
            record.status = ROLLED_BACK

    def acquire_lock(self, run_id: str) -> str:
        owner = uuid.uuid4().hex
        now = self._clock()
        with self._lock:
            held = self._run_locks.get(run_id)
            if held is not None and held[1] > now:
                raise ConcurrentExecutionError(run_id)
            self._run_locks[run_id] = (owner, now + self.lock_ttl)
        return owner

    def refresh_lock(self, run_id: str, owner: str) -> None:
        now = self._clock()
        with self._lock:
            held = self._run_locks.get(run_id)
            if held is None or held[0] != owner:
                raise ConcurrentExecutionError(run_id)
            self._run_locks[run_id] = (owner, now + self.lock_ttl)

    def release_lock(self, run_id: str, owner: str | None = None) -> None:
        with self._lock:
            held = self._run_locks.get(run_id)
            if held is not None and (owner is None or held[0] == owner):
                del self._run_locks[run_id]


class SQLiteStore(BaseStore):
    _COLUMNS = "run_id, step_name, status, output, created_at"

    def __init__(
        self,
        db_path: str = ":memory:",
        lock_ttl: float = DEFAULT_LOCK_TTL,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.db_path = db_path
        self.lock_ttl = _check_lock_ttl(lock_ttl)
        # Wall-clock time, so lock expiry means the same thing to every process sharing the file.
        self._clock = clock
        self._lock = threading.Lock()
        # One shared connection guarded by a lock; ":memory:" databases only live on one connection.
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS step_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    output TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (run_id, step_name)
                )
                """
            )
            # The primary key makes acquiring atomic, even across processes sharing the file.
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workflow_locks (
                    run_id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )

    @staticmethod
    def _to_record(row: tuple[str, str, str, str, str]) -> StepRecord:
        run_id, step_name, status, output, created_at = row
        return StepRecord(run_id, step_name, status, json.loads(output), created_at)

    def get_step(self, run_id: str, step_name: str) -> StepRecord | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {self._COLUMNS} FROM step_records WHERE run_id = ? AND step_name = ?",
                (run_id, step_name),
            ).fetchone()
        return self._to_record(row) if row else None

    def save_step(self, record: StepRecord) -> None:
        output = json.dumps(record.output)
        with self._lock, self._conn:
            # An upsert keeps the row's id, so an overwritten step keeps its position.
            self._conn.execute(
                f"""
                INSERT INTO step_records ({self._COLUMNS}) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (run_id, step_name) DO UPDATE SET
                    status = excluded.status,
                    output = excluded.output,
                    created_at = excluded.created_at
                """,
                (record.run_id, record.step_name, record.status, output, record.created_at),
            )

    def get_run_steps(self, run_id: str) -> list[StepRecord]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {self._COLUMNS} FROM step_records WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
        return [self._to_record(row) for row in rows]

    def mark_rolled_back(self, run_id: str, step_name: str) -> None:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE step_records SET status = ? WHERE run_id = ? AND step_name = ?",
                (ROLLED_BACK, run_id, step_name),
            )
        if cursor.rowcount == 0:
            raise _missing_step(run_id, step_name)

    def acquire_lock(self, run_id: str) -> str:
        owner = uuid.uuid4().hex
        now = self._clock()
        with self._lock, self._conn:
            # One atomic statement: insert a new lock, or take over the row only if it expired.
            cursor = self._conn.execute(
                """
                INSERT INTO workflow_locks (run_id, owner, expires_at) VALUES (?, ?, ?)
                ON CONFLICT (run_id) DO UPDATE SET
                    owner = excluded.owner,
                    expires_at = excluded.expires_at
                WHERE workflow_locks.expires_at <= ?
                """,
                (run_id, owner, now + self.lock_ttl, now),
            )
        if cursor.rowcount == 0:
            raise ConcurrentExecutionError(run_id)
        return owner

    def refresh_lock(self, run_id: str, owner: str) -> None:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE workflow_locks SET expires_at = ? WHERE run_id = ? AND owner = ?",
                (self._clock() + self.lock_ttl, run_id, owner),
            )
        if cursor.rowcount == 0:
            raise ConcurrentExecutionError(run_id)

    def release_lock(self, run_id: str, owner: str | None = None) -> None:
        with self._lock, self._conn:
            if owner is None:
                self._conn.execute("DELETE FROM workflow_locks WHERE run_id = ?", (run_id,))
            else:
                self._conn.execute(
                    "DELETE FROM workflow_locks WHERE run_id = ? AND owner = ?",
                    (run_id, owner),
                )

    def close(self) -> None:
        with self._lock:
            self._conn.close()
