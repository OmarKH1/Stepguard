import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stepguard import BaseStore, ConcurrentExecutionError, MemoryStore, SQLiteStore, StepRecord

FIXED_TIME = "2026-01-01T00:00:00+00:00"
LOCK_TTL = 60.0


@pytest.fixture
def store(request: pytest.FixtureRequest, tmp_path: Path, clock) -> Iterator[BaseStore]:
    if request.param == "memory":
        instance: BaseStore = MemoryStore(lock_ttl=LOCK_TTL, clock=clock)
    else:
        instance = SQLiteStore(str(tmp_path / "stepguard.db"), lock_ttl=LOCK_TTL, clock=clock)
    yield instance
    instance.close()


all_stores = pytest.mark.parametrize("store", ["memory", "sqlite"], indirect=True)


def completed(run_id: str, step_name: str, output: Any = None) -> StepRecord:
    return StepRecord(run_id, step_name, "completed", output, FIXED_TIME)


@all_stores
def test_save_and_get_step(store: BaseStore) -> None:
    record = completed("run-1", "charge_card", {"charge_id": "ch_123", "amount": 4200})
    store.save_step(record)

    assert store.get_step("run-1", "charge_card") == record


@all_stores
def test_get_step_returns_none_when_missing(store: BaseStore) -> None:
    store.save_step(completed("run-1", "charge_card"))

    assert store.get_step("run-1", "ship_order") is None
    assert store.get_step("run-2", "charge_card") is None


@all_stores
def test_get_run_steps_preserves_insertion_order(store: BaseStore) -> None:
    # Names are not alphabetical and timestamps are identical, so only insertion order can explain the result.
    store.save_step(completed("run-1", "reserve_stock"))
    store.save_step(completed("run-2", "unrelated_step"))
    store.save_step(completed("run-1", "charge_card"))
    store.save_step(completed("run-1", "create_shipment"))

    steps = store.get_run_steps("run-1")

    assert [s.step_name for s in steps] == ["reserve_stock", "charge_card", "create_shipment"]
    assert store.get_run_steps("run-missing") == []


@all_stores
def test_mark_rolled_back(store: BaseStore) -> None:
    store.save_step(completed("run-1", "reserve_stock", {"reservation_id": 7}))
    store.save_step(completed("run-1", "charge_card"))

    store.mark_rolled_back("run-1", "reserve_stock")

    rolled_back = store.get_step("run-1", "reserve_stock")
    assert rolled_back == StepRecord(
        "run-1", "reserve_stock", "rolled_back", {"reservation_id": 7}, FIXED_TIME
    )
    assert [s.status for s in store.get_run_steps("run-1")] == ["rolled_back", "completed"]


@all_stores
def test_mark_rolled_back_unknown_step_raises(store: BaseStore) -> None:
    with pytest.raises(KeyError):
        store.mark_rolled_back("run-1", "missing_step")


@all_stores
def test_save_step_overwrites_existing_step_in_place(store: BaseStore) -> None:
    store.save_step(completed("run-1", "first", 1))
    store.save_step(completed("run-1", "second", 2))
    store.save_step(completed("run-1", "first", 3))

    steps = store.get_run_steps("run-1")

    assert [(s.step_name, s.output) for s in steps] == [("first", 3), ("second", 2)]


@all_stores
def test_returned_output_is_independent_copy(store: BaseStore) -> None:
    output = {"items": [1, 2.5, "x", None, True], "nested": {"ok": False}}
    store.save_step(completed("run-1", "step", output))

    fetched = store.get_step("run-1", "step")
    assert fetched is not None
    fetched.output["items"].append("mutated")

    assert store.get_step("run-1", "step").output == output


@all_stores
def test_non_json_output_is_rejected(store: BaseStore) -> None:
    with pytest.raises(TypeError):
        store.save_step(completed("run-1", "step", object()))

    assert store.get_step("run-1", "step") is None


@all_stores
def test_concurrent_saves_from_multiple_threads(store: BaseStore) -> None:
    def save_many(worker: int) -> None:
        for i in range(25):
            store.save_step(completed("run-1", f"worker{worker}-step{i}", i))

    threads = [threading.Thread(target=save_many, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(store.get_run_steps("run-1")) == 100


@all_stores
def test_lock_blocks_second_acquire_until_released(store: BaseStore) -> None:
    store.acquire_lock("run-1")

    with pytest.raises(ConcurrentExecutionError):
        store.acquire_lock("run-1")
    store.acquire_lock("run-2")  # Locks are per run.

    store.release_lock("run-1")
    store.acquire_lock("run-1")


@all_stores
def test_release_lock_without_holding_it_is_a_no_op(store: BaseStore) -> None:
    store.release_lock("run-1")
    store.acquire_lock("run-1")


@all_stores
def test_only_one_thread_acquires_a_contended_lock(store: BaseStore) -> None:
    barrier = threading.Barrier(8)
    winners: list[int] = []

    def contend(worker: int) -> None:
        barrier.wait()
        try:
            store.acquire_lock("run-1")
            winners.append(worker)
        except ConcurrentExecutionError:
            pass

    threads = [threading.Thread(target=contend, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(winners) == 1


@all_stores
def test_expired_lock_can_be_taken_over(store: BaseStore, clock) -> None:
    store.acquire_lock("run-1")

    clock.advance(LOCK_TTL - 1)
    with pytest.raises(ConcurrentExecutionError):
        store.acquire_lock("run-1")

    clock.advance(1)
    store.acquire_lock("run-1")


@all_stores
def test_refresh_lock_extends_expiry(store: BaseStore, clock) -> None:
    owner = store.acquire_lock("run-1")
    clock.advance(LOCK_TTL - 1)
    store.refresh_lock("run-1", owner)

    clock.advance(LOCK_TTL - 1)  # Past the original expiry, but not the refreshed one.
    with pytest.raises(ConcurrentExecutionError):
        store.acquire_lock("run-1")


@all_stores
def test_previous_owner_cannot_refresh_or_release_after_takeover(
    store: BaseStore, clock
) -> None:
    stale_owner = store.acquire_lock("run-1")
    clock.advance(LOCK_TTL)
    store.acquire_lock("run-1")

    with pytest.raises(ConcurrentExecutionError):
        store.refresh_lock("run-1", stale_owner)
    store.release_lock("run-1", stale_owner)

    with pytest.raises(ConcurrentExecutionError):
        store.acquire_lock("run-1")  # The new owner still holds it.


@all_stores
def test_refresh_lock_raises_once_released(store: BaseStore) -> None:
    owner = store.acquire_lock("run-1")
    store.release_lock("run-1", owner)

    with pytest.raises(ConcurrentExecutionError):
        store.refresh_lock("run-1", owner)


def test_sqlite_lock_left_by_crashed_process_expires(tmp_path: Path, clock) -> None:
    db_path = str(tmp_path / "stepguard.db")
    crashed = SQLiteStore(db_path, lock_ttl=LOCK_TTL, clock=clock)
    crashed.acquire_lock("run-1")
    crashed.close()  # Gone without releasing, like a killed process.

    survivor = SQLiteStore(db_path, lock_ttl=LOCK_TTL, clock=clock)
    try:
        with pytest.raises(ConcurrentExecutionError):
            survivor.acquire_lock("run-1")

        clock.advance(LOCK_TTL)
        survivor.acquire_lock("run-1")
    finally:
        survivor.close()


def test_lock_ttl_must_be_positive() -> None:
    with pytest.raises(ValueError):
        MemoryStore(lock_ttl=0)
    with pytest.raises(ValueError):
        SQLiteStore(lock_ttl=-1)


def test_sqlite_lock_is_shared_between_store_instances(tmp_path: Path) -> None:
    # Two stores on one file stand in for two processes.
    db_path = str(tmp_path / "stepguard.db")
    first, second = SQLiteStore(db_path), SQLiteStore(db_path)
    try:
        first.acquire_lock("run-1")
        with pytest.raises(ConcurrentExecutionError):
            second.acquire_lock("run-1")

        first.release_lock("run-1")
        second.acquire_lock("run-1")
    finally:
        first.close()
        second.close()


def test_sqlite_store_persists_across_instances(tmp_path: Path) -> None:
    db_path = str(tmp_path / "stepguard.db")
    first = SQLiteStore(db_path)
    first.save_step(completed("run-1", "charge_card", {"charge_id": "ch_1"}))
    first.mark_rolled_back("run-1", "charge_card")
    first.close()

    second = SQLiteStore(db_path)
    try:
        assert second.get_step("run-1", "charge_card") == StepRecord(
            "run-1", "charge_card", "rolled_back", {"charge_id": "ch_1"}, FIXED_TIME
        )
    finally:
        second.close()


def test_step_record_rejects_unknown_status() -> None:
    with pytest.raises(ValueError):
        StepRecord("run-1", "step", "pending", None, FIXED_TIME)
