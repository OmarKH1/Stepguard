from unittest.mock import Mock, call

import pytest

from stepguard import MemoryStore, RollbackError, StepExecutionError, WorkflowContext


def fail_with(error: Exception):
    def step() -> None:
        raise error

    return step


def three_step_workflow(ctx: WorkflowContext, undos: Mock) -> None:
    ctx.step("reserve_stock", lambda: {"reservation_id": 1}, undo=undos.release_stock)
    ctx.step("charge_card", lambda: {"charge_id": "ch_1"}, undo=undos.refund_card)
    ctx.step("ship_order", fail_with(ValueError("carrier unavailable")))


def test_step_runs_func_and_records_output() -> None:
    store = MemoryStore()
    ctx = WorkflowContext(store, "order-1")

    result = ctx.step("total", lambda a, b, scale=1: (a + b) * scale, 2, 3, scale=10)

    assert result == 50
    record = store.get_step("order-1", "total")
    assert record is not None
    assert (record.status, record.output) == ("completed", 50)


def test_completed_step_is_not_rerun() -> None:
    store = MemoryStore()
    charge = Mock(return_value={"charge_id": "ch_1"})

    first = WorkflowContext(store, "order-1").step("charge_card", charge)
    second = WorkflowContext(store, "order-1").step("charge_card", charge)

    assert first == second == {"charge_id": "ch_1"}
    charge.assert_called_once()


def test_non_callable_undo_raises_type_error() -> None:
    store = MemoryStore()
    charge = Mock(return_value={"charge_id": "ch_1"})

    with pytest.raises(TypeError, match="undo must be callable"):
        WorkflowContext(store, "order-1").step("charge_card", charge, undo="refund_card")

    charge.assert_not_called()
    assert store.get_step("order-1", "charge_card") is None


def test_saga_rollback_on_step_failure() -> None:
    store = MemoryStore()
    undos = Mock()

    with pytest.raises(StepExecutionError) as exc_info:
        three_step_workflow(WorkflowContext(store, "order-1"), undos)

    assert exc_info.value.step_name == "ship_order"
    assert isinstance(exc_info.value.original_error, ValueError)
    # A shared parent mock records the undo calls in the order they happened.
    assert undos.mock_calls == [call.refund_card(), call.release_stock()]
    assert store.get_step("order-1", "reserve_stock").status == "rolled_back"
    assert store.get_step("order-1", "charge_card").status == "rolled_back"
    assert store.get_step("order-1", "ship_order") is None


def test_rollback_error_raised_if_undo_fails() -> None:
    store = MemoryStore()
    undos = Mock()
    undos.refund_card.side_effect = RuntimeError("refund service down")

    with pytest.raises(RollbackError) as exc_info:
        three_step_workflow(WorkflowContext(store, "order-1"), undos)

    error = exc_info.value
    assert error.failed_step == "ship_order"
    assert list(error.rollback_errors) == ["charge_card"]
    assert isinstance(error.rollback_errors["charge_card"], RuntimeError)
    # The failing undo did not stop the rest of the rollback.
    assert undos.mock_calls == [call.refund_card(), call.release_stock()]
    assert store.get_step("order-1", "charge_card").status == "completed"
    assert store.get_step("order-1", "reserve_stock").status == "rolled_back"


def test_resumed_run_compensates_steps_from_earlier_attempt() -> None:
    store = MemoryStore()
    undos = Mock()
    WorkflowContext(store, "order-1").step(
        "reserve_stock", lambda: {"reservation_id": 1}, undo=undos.release_stock
    )

    # A new context for the same run_id simulates resuming after a crash.
    with pytest.raises(StepExecutionError):
        three_step_workflow(WorkflowContext(store, "order-1"), undos)

    assert undos.mock_calls == [call.refund_card(), call.release_stock()]
    assert store.get_step("order-1", "reserve_stock").status == "rolled_back"
