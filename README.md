# stepguard

A lightweight, zero-dependency workflow engine for Python 3.10+ that makes multi-step
jobs crash-safe:

- **Idempotent steps:** each step's result is saved. Re-running the same run skips steps
  that already completed and returns their saved output.
- **Saga rollback:** when a step fails, the undo callbacks of the steps that completed
  run in reverse order.
- **Run locking:** only one worker can execute a given run at a time. Locks expire, so a
  run left locked by a crashed worker can be resumed.

It uses only the standard library. Results are kept in SQLite or in memory.

## Install

```bash
pip install -e .
```

## Quick start

```python
from stepguard import SQLiteStore, durable_workflow

store = SQLiteStore("orders.db")


def reserve_stock(order_id: int) -> dict:
    return {"reservation_id": f"res-{order_id}"}


def charge_card(order_id: int) -> dict:
    return {"charge_id": f"ch-{order_id}"}


def ship_order(order_id: int) -> dict:
    return {"tracking_number": f"trk-{order_id}"}


@durable_workflow(store, run_id=lambda order_id: f"order-{order_id}")
def process_order(ctx, order_id: int) -> dict:
    ctx.step(
        "reserve_stock", reserve_stock, order_id,
        undo=lambda: print(f"Releasing stock for order {order_id}"),
    )
    ctx.step(
        "charge_card", charge_card, order_id,
        undo=lambda: print(f"Refunding order {order_id}"),
    )
    return ctx.step("ship_order", ship_order, order_id)


print(process_order(42))  # {'tracking_number': 'trk-42'}
```

`run_id` receives the workflow's arguments and returns the id of the run, so calling
`process_order(42)` again works on the same run, `order-42`. Since all three steps are
already completed, nothing runs a second time and the saved result is returned.

## How it works

### Steps

```python
ctx.step(step_name, func, *args, undo=None, **kwargs)
```

- If `step_name` already completed in this run, `func` is skipped and the saved output is
  returned.
- Otherwise `func(*args, **kwargs)` runs, and its return value is saved as completed.
- Return values must be JSON-serializable (they are stored as JSON). Values come back as
  their JSON equivalents, so a tuple comes back as a list.
- `undo` is an optional zero-argument callable. Passing a value that isn't callable raises
  `TypeError`.

### Rollback

If a step raises, stepguard calls each registered `undo` in reverse order and marks those
steps `rolled_back`:

- If every undo succeeds, it raises `StepExecutionError`. The failing step is in
  `error.step_name` and the original exception in `error.original_error`.
- If any undo raises, the remaining undos still run, and then `RollbackError` is raised.
  `error.failed_step` names the step that triggered the rollback and `error.rollback_errors`
  maps each failed undo's step name to its exception. Steps whose undo failed stay marked
  `completed`, because their effects were not reversed.

Steps that completed in an earlier, crashed attempt are rolled back too. Running a run
again after a rollback executes the rolled-back steps again.

### Locking

`@durable_workflow` locks the run before calling your function and releases the lock in a
`finally` block. Starting a run that is already locked raises `ConcurrentExecutionError`.

A lock expires `lock_ttl` seconds (default 300) after it was taken or last renewed:

```python
store = SQLiteStore("orders.db", lock_ttl=120)
```

- The lock is renewed before each step runs, so `lock_ttl` must be longer than your slowest
  single step, not the whole workflow.
- Once a lock expires, another worker can take the run over. If the original worker then
  tries to run another step, it gets `ConcurrentExecutionError` and stops without rolling
  back, because the new worker now owns the run.

`SQLiteStore` locks are shared by every process that uses the same database file.

## Stores

| Store | Use |
| --- | --- |
| `SQLiteStore(db_path=":memory:", lock_ttl=300.0)` | Persistent storage in a SQLite file. Thread-safe and shared across processes. |
| `MemoryStore(lock_ttl=300.0)` | Thread-safe in-memory storage for tests. Behaves like `SQLiteStore`, including the JSON handling of outputs. |

Both implement `BaseStore`. Call `store.close()` when you're done with a `SQLiteStore`.

## Exceptions

```
StepGuardError
├── StepExecutionError        a step failed and was rolled back cleanly
├── RollbackError             one or more undo callbacks failed during rollback
└── WorkflowLockedError
    └── ConcurrentExecutionError   the run is locked by another worker
```

## Limitations

- `undo` callbacks take no arguments, so they can't read the step's output. Capture
  what they need in a closure.
- Rollback doesn't renew the lock. If the undo callbacks together take longer than
  `lock_ttl`, another worker could take over during the rollback.
- Lock expiry uses each machine's clock. That's fine for processes on one machine, but
  machines whose clocks disagree would see locks expire at different times.

## Development

```bash
python -m venv .venv
```

Activate it (`.venv\Scripts\activate` on Windows, `source .venv/bin/activate` elsewhere),
then:

```bash
pip install -e ".[dev]"
```

```bash
pytest -v
```

CI runs the test suite on Python 3.10 for every push and pull request to `main`
(`.github/workflows/ci.yml`).
