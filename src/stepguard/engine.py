from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Concatenate, ParamSpec, TypeVar

from stepguard.exceptions import RollbackError, StepExecutionError
from stepguard.storage import COMPLETED, BaseStore, StepRecord

P = ParamSpec("P")
R = TypeVar("R")


class WorkflowContext:
    def __init__(self, store: BaseStore, run_id: str, lock_owner: str | None = None) -> None:
        self.store = store
        self.run_id = run_id
        self.lock_owner = lock_owner
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

        if self.lock_owner is not None:
            # Renew the lease before doing work. If it expired and another worker took the
            # run over, this raises without rolling back: that worker now owns the run.
            self.store.refresh_lock(self.run_id, self.lock_owner)

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


def durable_workflow(
    store: BaseStore, run_id: Callable[P, str]
) -> Callable[[Callable[Concatenate[WorkflowContext, P], R]], Callable[P, R]]:
    """Run the decorated function as an exclusively locked workflow.

    `run_id` is called with the workflow's arguments and returns the run's id, e.g.
    `run_id=lambda order_id: f"order-{order_id}"`. The decorated function receives a
    WorkflowContext for that run as its first argument.

    The lock is renewed before each step runs, so the store's lock_ttl must be longer
    than the slowest single step, not the whole workflow.
    """

    def decorator(func: Callable[Concatenate[WorkflowContext, P], R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            key = run_id(*args, **kwargs)
            owner = store.acquire_lock(key)
            try:
                return func(WorkflowContext(store, key, lock_owner=owner), *args, **kwargs)
            finally:
                store.release_lock(key, owner)

        return wrapper

    return decorator
