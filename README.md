[![postgres-tq Actions Status](https://github.com/flix-tech/postgres-tq/workflows/CI/CD%20Pipeline/badge.svg?branch=main)](https://github.com/flix-tech/postgres-tq/actions)
[![License](https://img.shields.io/github/license/flix-tech/postgres-tq)](https://pypi.org/project/postgres-tq/)
[![PyPI - Python Version](https://img.shields.io/pypi/v/postgres-tq)](https://pypi.org/project/postgres-tq/)

# Postgres Task Queue

This repo will contain the [Postgres](https://www.postgresql.org/) based task queue logic, similar to [our redis-tq](https://github.com/flix-tech/redis-tq) but based on postgres instead.

Similar to redis-tq, this package allows for sharing data between multiple processes or hosts.

Tasks support a "lease time". After that time other workers may consider this client to have crashed or stalled and pick up the item instead. The number of retries can also be configured.

By default it keeps all the queues and tasks in a single table `task_queue`. If you want to use a different table for different queues for example it could also be configured when instantiating the queue.

You can either set the `create_table=True` when instantiating the queue or create the table yourself with the following query:

```sql
CREATE TABLE task_queue (
    id UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    task JSONB NOT NULL,
    ttl INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processing BOOLEAN NOT NULL DEFAULT false,
    lease_timeout FLOAT,
    deadline TIMESTAMP,
    completed_at TIMESTAMP
)
```

## Installation

postgres-tq is available on [PyPI][] so you can simply install via:

```bash
$ pip install postgres-tq
```

[PyPI]: https://pypi.org/project/postgres-tq/

## How it works

It uses row level locks of postgres to mimic the atomic pop and atomic push of redis-tq when getting a new task from the queue:

```sql
UPDATE task_queue
SET processing = true,
    deadline =
        current_timestamp + CAST(lease_timeout || ' seconds' AS INTERVAL)
WHERE id = (
    SELECT id
    FROM task_queue
    WHERE completed_at IS NULL
        AND processing = false
        AND queue_name = <your_queue_name>
        AND ttl > 0
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, task;
```

Let's say two workers try to get a new task at the same time, assuming that they will probably see the same task to be picked up using the subquery:

```sql
SELECT id
FROM task_queue
WHERE completed_at IS NULL
    AND processing = false
    AND queue_name = <your_queue_name>
    AND ttl > 0
ORDER BY created_at
```

The first worker locks the row with the `FOR UPDATE` clause until the update is completed and committed. If we hadn't used the `SKIP LOCKED` clause, the second worker would have seen the same row and waited for the first worker to finish the update. However, since the first worker already updated it, the subquery would no longer be valid, and the second worker would return zero rows because `WHERE id = NULL`.

However, since we are using `FOR UPDATE SKIP LOCKED` the first worker locks the row for update and the second worker, skips that locked row and chooses another row for itself to update. This way we can avoid the race condition.

The other methods `complete()` and `reschedule()` work similarly under the hood.

## How to use

On the producing side, populate the queue with tasks and a respective lease timeout:

```py
from postgrestq import TaskQueue

task_queue = TaskQueue(POSTGRES_CONN_STR, queue_name, reset=True)

for i in range(10):
    task_queue.add(some_task, lease_timeout, ttl=3)
```

On the consuming side:

```py
from postgrestq import TaskQueue

task_queue = TaskQueue(POSTGRES_CONN_STR, queue_name, reset=True)
while True:
    task, task_id = task_queue.get()
    if task is not None:
        # do something with task and mark it as complete afterwards
        task_queue.complete(task_id)
    if task_queue.is_empty():
        break
    # task_queue.get is non-blocking, so you may want to sleep a
    # bit before the next iteration
    time.sleep(1)
```

Or you can even use the \_\_iter\_\_() method of the class TaskQueue and loop over the queue:

```py
from postgrestq import TaskQueue

task_queue = TaskQueue(POSTGRES_CONN_STR, queue_name, reset=True)

for task, id_ in taskqueue:
    # do something with task and it's automatically
    # marked as completed by the iterator at the end
    # of the iteration

```

If the consumer crashes (i.e. the task is not marked as completed after lease_timeout seconds), the task will be put back into the task queue. This rescheduling will happen at most ttl times and then the task will be dropped. A callback can be provided if you want to monitor such cases.

As the tasks are completed, they will remain in the `task_queue`
postgres table. The table will be deleted of its content if
initializing a `TaskQueue` instance with the `reset` flag to `true`
or if using the `prune_completed_tasks` method:

```py
from postgrestq import TaskQueue

# If reset=True, the full queue content will be deleted
task_queue = TaskQueue(POSTGRES_CONN_STR, queue_name, reset=False)

# Prune all tasks from queue completed more than 1 hour (in seconds)
# ago. Tasks in progress, not started and completed recently will
# stay in the postgres task_queue table
task_queue.prune_completed_tasks(3600)

```

## Running the tests

The tests will check a presence of an Postgres DB in the port 15432. To initiate one using docker you can run:

```bash
$ make run-postgres
```

Then run

```bash
$ pdm run make test
```

`pdm run` will ensure that the `make test` command is executed within the context of the virtual environment managed by `pdm`. Make sure you have `pdm` and `Docker` installed for this to work.

## License

This project is licensed under the [MIT License](LICENSE).
