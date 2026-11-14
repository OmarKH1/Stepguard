from __future__ import annotations

from collections.abc import Callable
from typing import Any

from stepguard.exceptions import RollbackError, StepExecutionError
from stepguard.storage import COMPLETED, BaseStore, StepRecord


class WorkflowContext:
    def __init__(self, store: BaseStore, run_id: str) -> None:
        self.store = store
        self.run_id = run_id
        self._undo_stack: list[tuple[str, Callable[[], Any]]] = []

    def step(
        self,
        step_name: str,
        func: Callable[..., Any],
        *args: Any,
        undo: Callable[[], Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        if undo is not None and not callable(undo):
            raise TypeError("undo must be callable")

        existing = self.store.get_step(self.run_id, step_name)
        if existing is not None and existing.status == COMPLETED:
            # Completed in an earlier attempt: skip func, but keep its undo so a later
            # failure in this run still compensates it.
            if undo is not None:
                self._undo_stack.append((step_name, undo))
            return existing.output

        try:
            output = func(*args, **kwargs)
        except Exception as error:
            self._execute_rollback(failed_step=step_name)
            raise StepExecutionError(step_name, error) from error

        self.store.save_step(StepRecord(self.run_id, step_name, COMPLETED, output))
        if undo is not None:
            self._undo_stack.append((step_name, undo))
        return output

    def _execute_rollback(self, failed_step: str) -> None:
        rollback_errors: dict[str, Exception] = {}
        for step_name, undo in reversed(self._undo_stack):
            try:
                undo()
            except Exception as error:
                # Leave the step marked completed: its effects were not compensated.
                rollback_errors[step_name] = error
                continue
            self.store.mark_rolled_back(self.run_id, step_name)
        self._undo_stack.clear()

        if rollback_errors:
            raise RollbackError(failed_step, rollback_errors)
