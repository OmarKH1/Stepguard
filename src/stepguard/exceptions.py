class StepGuardError(Exception):
    """Base exception for all stepguard errors."""


class StepExecutionError(StepGuardError):
    def __init__(self, step_name: str, original_error: Exception) -> None:
        self.step_name = step_name
        self.original_error = original_error
        super().__init__(f"Step {step_name!r} failed: {original_error!r}")


class RollbackError(StepGuardError):
    def __init__(self, failed_step: str, rollback_errors: dict[str, Exception]) -> None:
        self.failed_step = failed_step
        self.rollback_errors = rollback_errors
        failed_undos = ", ".join(repr(name) for name in rollback_errors)
        super().__init__(
            f"Rollback after step {failed_step!r} failed; "
            f"compensations raised for: {failed_undos}"
        )


class WorkflowLockedError(StepGuardError):
    """Raised when a workflow run_id is already actively running."""
