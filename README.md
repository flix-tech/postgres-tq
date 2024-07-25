[![postgres-tq Actions Status](https://github.com/flix-tech/postgres-tq/workflows/CI/CD%20Pipeline/badge.svg?branch=main)](https://github.com/flix-tech/postgres-tq/actions)
[![License](https://img.shields.io/github/license/flix-tech/postgres-tq)](https://pypi.org/project/postgres-tq/)
[![PyPI - Python Version](https://img.shields.io/pypi/v/postgres-tq)](https://pypi.org/project/postgres-tq/)

# Postgres Task Queue

This library makes it possible to define a list of tasks to be persisted in a Postgres database and executed by multiple workers. Tasks are retried automatically after a given timeout and for a given number of time. Tasks are persisted in the database and executed in order of insertion unless a specific start timestamp is provided.

This is similar to [our redis-tq](https://github.com/flix-tech/redis-tq) but based on Postgres thanks to the `FOR UPDATE SKIP LOCKED` feature.

Similar to redis-tq, this package allows for sharing data between multiple processes or hosts.

Tasks support a "lease time". After that time other workers may consider this client to have crashed or stalled and pick up the item instead. The number of retries can also be configured.

By default it keeps all the queues and tasks in a single table `task_queue`. If you want to use a different table for different queues for example it could also be configured when instantiating the queue.

You can set the `create_table=True` when instantiating the queue to have the table created for you. If the table already exist it will not be touched.

When defining a task you can provide a `can_start_at` timestamp parameter, and the task will not be executed until then, which can be useful to schedule tasks. By default the current timestamp is used.

## Installation

postgres-tq is available on [PyPI][] so you can simply install via:

```bash
$ pip install postgres-tq
```

[PyPI]: https://pypi.org/project/postgres-tq/

## How to use

On the producing side, populate the queue with tasks and a respective lease timeout:

```py
from datetime import datetime, UTC, timedelta
from postgrestq import TaskQueue

task_queue = TaskQueue(
    POSTGRES_CONN_STR,
    queue_name, # name of the queue as a string
    reset=True, # delete existing tasks for this queue
    ttl_zero_callback=handle_failure, # will call handle_failure(task_id, task) when the task failed too many times
)

for i in range(10):
    task_queue.add(
        some_task,
        lease_timeout, # in seconds, after this interval it will be assumed to have failed (and the callback is called)
        ttl=3, # attempts before abandoning
        can_start_at=datetime.now(UTC) + timedelta(minutes=5), # start it not before than 5 minutes in the future
    )
```

On the consuming side:

```py
from postgrestq import TaskQueue

task_queue = TaskQueue(POSTGRES_CONN_STR, queue_name, reset=True)
while True:
    task, task_id, _queue_name = task_queue.get()
    if task is not None:
        # do something with task and mark it as complete afterwards
        task_queue.complete(task_id)
    if task_queue.is_empty():
        break
    # task_queue.get is non-blocking, so you may want to sleep a
    # bit before the next iteration
    time.sleep(1)
```

Notice that `get()` returns the queue name too, in case in future multi-queue is implemented.
At the moment it's always the same as the queue_name given to the class.

Or you can even use the \_\_iter\_\_() method of the class TaskQueue and loop over the queue:

```py
from postgrestq import TaskQueue

task_queue = TaskQueue(POSTGRES_CONN_STR, queue_name, reset=True)

for task, id_, queue_name in taskqueue:
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


## How it works

It uses row level locks of postgres to mimic the atomic pop and atomic push of redis-tq when getting a new task from the queue:

```sql
UPDATE task_queue
SET started_at = current_timestamp
WHERE id = (
    SELECT id
    FROM task_queue
    WHERE completed_at IS NULL
        AND started_at IS NULL
        AND queue_name = <your_queue_name>
        AND ttl > 0
        AND can_start_at <= current_timestamp
    ORDER BY can_start_at
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
    AND started_at IS NULL
    AND queue_name = <your_queue_name>
    AND ttl > 0
    AND can_start_at <= current_timestamp
ORDER BY can_start_at
```

The first worker locks the row with the `FOR UPDATE` clause until the update is completed and committed. If we hadn't used the `SKIP LOCKED` clause, the second worker would have seen the same row and waited for the first worker to finish the update. However, since the first worker already updated it, the subquery would no longer be valid, and the second worker would return zero rows because `WHERE id = NULL`.

However, since we are using `FOR UPDATE SKIP LOCKED` the first worker locks the row for update and the second worker, skips that locked row and chooses another row for itself to update. This way we can avoid the race condition.

The other methods `complete()` and `reschedule()` work similarly under the hood.

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
