from __future__ import annotations

import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

COMPLETED = "completed"
ROLLED_BACK = "rolled_back"
VALID_STATUSES = (COMPLETED, ROLLED_BACK)


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

    def close(self) -> None:
        """Release any resources held by the store."""


def _missing_step(run_id: str, step_name: str) -> KeyError:
    return KeyError(f"No step {step_name!r} recorded for run {run_id!r}")


class MemoryStore(BaseStore):
    # Outputs are round-tripped through JSON so this store accepts and returns exactly
    # what SQLiteStore does, and callers never share mutable state with the store.

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._runs: dict[str, dict[str, StepRecord]] = {}

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


class SQLiteStore(BaseStore):
    _COLUMNS = "run_id, step_name, status, output, created_at"

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
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

    def close(self) -> None:
        with self._lock:
            self._conn.close()
