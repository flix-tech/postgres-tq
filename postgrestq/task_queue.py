import json
import logging
from datetime import datetime

from uuid import uuid4, UUID
from typing import (
    Optional,
    Tuple,
    Iterator,
    Dict,
    Any,
    Callable,
    List,
    Sequence,
)

from psycopg import sql
from psycopg_pool import ConnectionPool

# supported only from 3.11 onwards:
# from datetime import UTC
# workaround for older versions:
from datetime import timezone
UTC = timezone.utc

logger = logging.getLogger(__name__)


class TaskQueue:
    def __init__(
        self,
        dsn: str,
        queue_name: str,
        table_name: str = "task_queue",
        reset: bool = False,
        create_table: bool = False,
        ttl_zero_callback: Optional[
            Callable[[UUID, Optional[str]], None]
        ] = None,
    ):
        """Initialize the task queue.

        Note: a task has to be at any given time either in the task
        queue or in the processing queue. If a task is moved from one
        queue to the other it has to be in an atomic fashion!

        Parameters
        ----------
        dsn : str
            connection string for the Postgres server
        queue_name : str
            name of the task queue
        table_name: str
            name of the table where the queue is stored
        reset : bool
            If true, reset existing tasks in the DB that have `queue_name` as
            the queue_name.
        create_table : bool
            If set to true it creates the table in the DB, it's nice to have
            if you are running the tests with a dummy DB
        ttl_zero_callback : callable
            a function that is called if a task's ttl <= 0. The callback
            needs to accept two parameters, the task_id and the task.

        """
        self._queue_name = queue_name
        self._dsn = dsn
        self._table_name = table_name

        # called when ttl <= 0 for a task
        self.ttl_zero_callback = ttl_zero_callback
        self.connect()
        if create_table:
            self._create_queue_table()

        if reset:
            self._reset()

    def connect(self) -> None:
        """
        Creates a ConnectionPool and waits until a connection to Postgres is
        established.
        """
        self.pool = ConnectionPool(self._dsn, open=True, min_size=2)
        # This will block the use of the pool until min_size connections
        # have been acquired
        self.pool.wait()
        logger.info("ConnectionPool is ready")

    def _create_queue_table(self) -> None:
        """
        Creates a task_queue table
        """
        # TODO: check if the table already exist
        # whether it has the same schema
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """CREATE TABLE IF NOT EXISTS {} (
                                id            UUID PRIMARY KEY,
                                queue_name    TEXT NOT NULL,
                                task          JSONB NOT NULL,
                                ttl           SMALLINT NOT NULL,
                                can_start_at  TIMESTAMPTZ NOT NULL
                                            DEFAULT CURRENT_TIMESTAMP,
                                lease_timeout FLOAT,
                                started_at    TIMESTAMPTZ,
                                completed_at  TIMESTAMPTZ
                            )"""
                    ).format(sql.Identifier(self._table_name))
                )
                cur.execute(
                    sql.SQL(
                        """CREATE INDEX IF NOT EXISTS
                            task_queue_queue_name_can_start_at_idx
                            ON {} (queue_name, can_start_at)
                        """
                    ).format(sql.Identifier(self._table_name))
                )
            conn.commit()

    def __len__(self) -> int:
        """
        Returns the length of processing or to be processed tasks
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        """
                    SELECT count(*) as count
                    FROM {}
                    WHERE queue_name = %s
                        AND completed_at IS NULL
                """
                    ).format(sql.Identifier(self._table_name)),
                    (self._queue_name,),
                )
                row = cursor.fetchone()
                count: int = row[0] if row else 0
                conn.commit()
                return count

    def add(
        self,
        task: Dict[str, Any],
        lease_timeout: float,
        ttl: int = 3,
        can_start_at: Optional[datetime] = None
    ) -> str:
        """Add a task to the task queue.

        Parameters
        ----------
        task : something that can be JSON-serialized
        lease_timeout : float
            lease timeout in seconds, i.e. how much time we give the
            task to process until we can assume it didn't succeed
        ttl : int
            Number of (re-)tries, including the initial one, in case the
            job dies.
        can_start_at : datetime
            The earliest time the task can be started.
            If None, set current time. For consistency the time is
            from the database clock. A task will not be started before
            this time.
        Returns
        -------
        task_id :
            The random UUID that was generated for this task
        """
        # make sure the timeout is an actual number, otherwise we'll run
        # into problems later when we calculate the actual deadline
        lease_timeout = float(lease_timeout)

        id_ = str(uuid4())

        serialized_task = self._serialize(task)
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                # store the task + metadata and put task-id into the task queue
                cursor.execute(
                    sql.SQL(
                        """
                    INSERT INTO {} (
                        id,
                        queue_name,
                        task,
                        ttl,
                        lease_timeout,
                        can_start_at
                    )
                    VALUES (%s, %s, %s, %s, %s,
                    COALESCE(%s, current_timestamp))
                """
                    ).format(sql.Identifier(self._table_name)),
                    (
                        id_, self._queue_name, serialized_task,
                        ttl, lease_timeout, can_start_at
                    ),
                )
                conn.commit()
        return id_

    def add_many(
        self,
        tasks: List[Dict[str, Any]],
        lease_timeout: float,
        ttl: int = 3,
        can_start_at: Optional[datetime] = None
    ) -> List[str]:
        """Like add(), but optimized for a batch of tasks.

        When inserting many tasks, it is faster than multiple calls to
        add() because it uses a single transaction.

        After the creation the tasks will be independent from each other.

        Parameters
        ----------
        tasks : list of something that can be JSON-serialized
        lease_timeout : float
            lease timeout in seconds, i.e. how much time we give the
            tasks to process until we can assume it didn't succeed
        ttl : int
            Number of (re-)tries, including the initial one, in case the
            job dies.
        can_start_at : datetime
            The earliest time the task can be started.
            If None, set current time. For consistency the time is
            from the database clock. A task will not be started before this
            time.
        Returns
        -------
        task_ids :
            List of random UUIDs that were generated for this task.
            The order is the same of the given tasks
        """
        # make sure the timeout is an actual number, otherwise we'll run
        # into problems later when we calculate the actual deadline
        lease_timeout = float(lease_timeout)
        ret_ids = []
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                for task in tasks:
                    id_ = str(uuid4())

                    serialized_task = self._serialize(task)

                    cursor.execute(
                        sql.SQL(
                            """
                        INSERT INTO {} (
                            id,
                            queue_name,
                            task,
                            ttl,
                            lease_timeout,
                            can_start_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, COALESCE(%s, current_timestamp)
                        )
                    """
                        ).format(sql.Identifier(self._table_name)),
                        (
                            id_, self._queue_name, serialized_task,
                            ttl, lease_timeout, can_start_at
                        ),
                    )
                    ret_ids.append(id_)
                conn.commit()
        return ret_ids

    def get(self) -> Tuple[
            Optional[Dict[str, Any]],
            Optional[UUID],
            Optional[str],
            ]:
        """Get a task from the task queue (non-blocking).

        This statement marks the next available task in the queue as
        started (being processed) and returns its ID and task details.
        The query uses a FOR UPDATE SKIP LOCKED clause to lock the selected
        task so that other workers can't select the same task simultaneously.

        After executing the query, the method fetches the result using
        cur.fetchone(). If no task is found, the method returns None, None.
        Otherwise, it returns the task and its ID.

        Note that this method is non-blocking, which means it returns
        immediately even if there is no task available in the queue.

        In order to mark that task as done, you have to do:

            >>> task, task_id, queue_name = taskqueue.get()
            >>> # do something
            >>> taskqueue.complete(task_id)

        After some time (i.e. `lease_timeout`) tasks expire and are
        marked as not being processed and the TTL is decreased by
        one. If TTL is still > 0 the task will be retried.

        Note, this method is non-blocking, i.e. it returns immediately
        even if there is nothing to return. See below for the return
        value for this case.

        Returns
        -------
        (task, task_id, queue_name) :
            The next item from the task list or (None, None, None) if it's
            empty

        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                    UPDATE {}
                    SET started_at = current_timestamp
                    WHERE id = (
                        SELECT id
                        FROM {}
                        WHERE completed_at IS NULL
                            AND started_at IS NULL
                            AND queue_name = %s
                            AND ttl > 0
                            AND can_start_at <= current_timestamp
                        ORDER BY can_start_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING id, task;"""
                    ).format(
                        sql.Identifier(self._table_name),
                        sql.Identifier(self._table_name),
                    ),
                    (self._queue_name,),
                )

                row = cur.fetchone()
                conn.commit()
                if row is None:
                    return None, None, None

                task_id, task = row
                logger.info(f"Got task with id {task_id}")
                return task, task_id, self._queue_name

    def get_many(self, amount: int) -> Sequence[
        Tuple[Optional[Dict[str, Any]], Optional[UUID], Optional[str]],
            ]:
        """Same as get() but retrieves multiple tasks.

        If there are less than `amount` tasks in the queue, it will return
        whatever is available.

        If no task is available it will return an empty list.

        This is faster than multiple calls to get(), as it uses a single query.

        Returns
        -------
        list of (task, task_id, queue_name) :
            The tasks and their IDs, and the queue_name

        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                    UPDATE {}
                    SET started_at = current_timestamp
                    WHERE id IN (
                        SELECT id
                        FROM {}
                        WHERE completed_at IS NULL
                            AND started_at IS NULL
                            AND queue_name = %s
                            AND ttl > 0
                            AND can_start_at <= current_timestamp
                        ORDER BY can_start_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    RETURNING task, id;"""
                    ).format(
                        sql.Identifier(self._table_name),
                        sql.Identifier(self._table_name),
                    ),
                    (self._queue_name, amount),
                )

                ret = []
                for task, task_id in cur.fetchall():
                    logger.info(f"Got task with id {task_id}")
                    ret.append((task, task_id, self._queue_name,))
                conn.commit()
            return ret

    def complete(self, task_id: Optional[UUID]) -> int:
        """Mark a task as completed.

        Marks a task as completed by setting completed_at column by
        the current timestamp.

        If the job is in the queue, which happens if it took too long
        and it expired, it is removed from there as well.


        Parameters
        ----------
        task_id : UUID | None
            the task ID

        Returns
        -------
        the number of updated rows: int

        """
        logger.info(f"Marking task {task_id} as completed")
        count = 0
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                    UPDATE {}
                    SET completed_at = current_timestamp
                    WHERE id = %s
                    AND completed_at is NULL"""
                    ).format(sql.Identifier(self._table_name)),
                    (task_id,),
                )
                count = cur.rowcount
                if count == 0:
                    logger.info(f"Task {task_id} was already completed")

            conn.commit()
        return count

    def is_empty(self) -> bool:
        """Check if the task queue is empty.

        Internally, this function also checks the currently processed
        tasks for expiration and teals with TTL and re-scheduling them
        into the task queue by marking them as not processing.

        Returns
        -------
        bool

        """
        self.check_expired_leases()
        return len(self) == 0

    def check_expired_leases(self) -> None:
        """Check for expired leases and put the task back if needed.

        This method goes through all tasks that are currently processed
        and checks if their deadline expired. If so, we assume the
        worker failed. We decrease the TTL and if TTL is still > 0 we
        reschedule the task into the task queue or, if the TTL is
        exhausted, we mark the task as completed by setting
        `completed_at` column with current timestamp and call the
        expired task callback if it's set.

        This means a task that takes longer than the lease_timeout can be
        executed more than once.

        Note: lease check is only performed against the tasks
        that are processing (started_at is not null).

        """
        # goes through all the tasks that are marked as started
        # and check the ones with expired timeout
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                    SELECT id
                    FROM {}
                    WHERE completed_at IS NULL
                        AND started_at IS NOT NULL
                        AND queue_name = %s
                        AND (
                            started_at +
                            (lease_timeout || ' seconds')::INTERVAL
                            ) < current_timestamp
                    ORDER BY can_start_at;
                """
                    ).format(sql.Identifier(self._table_name)),
                    (self._queue_name,),
                )
                expired_tasks = cur.fetchall()
                conn.commit()
                logger.debug(f"Expired tasks {expired_tasks}")

            for row in expired_tasks:
                task_id: UUID = row[0]
                logger.debug(f"Got expired task with id {task_id}")
                task, ttl = self.get_updated_expired_task(task_id)

                if ttl is None:
                    # race condition! between the time we got `key` from the
                    # set of tasks (this outer loop) and the time we tried
                    # to get that task from the queue, it has been completed
                    # and therefore deleted from the queue. In this case
                    # tasks is None and we can continue
                    logger.info(
                        f"Task {task_id} was marked completed while we "
                        "checked for expired leases, nothing to do."
                    )
                    continue

                if ttl <= 0:
                    logger.error(
                        f"Job {task} with id {task_id} "
                        "failed too many times, marking it as completed."
                    )
                    # # here committing to release the previous update lock
                    conn.commit()
                    self.complete(task_id)

                    if self.ttl_zero_callback:
                        self.ttl_zero_callback(task_id, task)
                conn.commit()

    def get_updated_expired_task(
        self, task_id: UUID
    ) -> Tuple[Optional[str], Optional[int]]:
        """
        Given the id of an expired task, it tries to reschedule it by
        marking it as not started, resetting the deadline
        and decreasing TTL by one. It returns None if the task is
        already updated (or being updated) by another worker.

        Returns
        -------
        (task, ttl) :
            The updated task and ttl values for the expired task with
            task_id after it's rescheduled

        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                    UPDATE {}
                    SET ttl = ttl - 1,
                        started_at = NULL
                    WHERE id = (
                        SELECT id
                        FROM {}
                        WHERE completed_at IS NULL
                            AND started_at IS NOT NULL
                            AND queue_name = %s
                            AND id = %s
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING task, ttl;
                """
                    ).format(
                        sql.Identifier(self._table_name),
                        sql.Identifier(self._table_name),
                    ),
                    (
                        self._queue_name,
                        task_id,
                    ),
                )
                updated_row = cur.fetchone()
                if updated_row is None:
                    return None, None

                task, ttl = updated_row
                task = self._serialize(task)
                return task, ttl

    def _serialize(self, task: Any) -> str:
        return json.dumps(task, sort_keys=True)

    def _deserialize(self, blob: str) -> Any:
        return json.loads(blob)

    def reschedule(
        self,
        task_id: UUID,
        decrease_ttl: Optional[bool] = False,
    ) -> None:
        """Move a task back from being processed to the task queue.

        Workers can use this method to "drop" a work unit in case of
        eviction (because of an external issue like terminating a machine
        by AWS and not because of a failure).
        Rescheduled work units are immediately available for processing again,
        and unless decrease_ttl is set to True, the TTL is not modified.

        This function can optionally modify the TTL, setting decrease_ttl to
        True. This allows to handle a failure quickly without waiting the
        lease_timeout.

        Parameters
        ----------
        task_id : UUID
            the task ID
        decrease_ttl : bool
            If True, decrease the TTL by one

        Raises
        ------
        ValueError :
            Task is not being processed, and cannot be re-scheduled

        """

        if not isinstance(task_id, UUID):
            raise ValueError("task_id must be a UUID")
        logger.info(f"Rescheduling task {task_id}..")
        decrease_ttl_sql = ""
        if decrease_ttl:
            decrease_ttl_sql = "ttl = ttl - 1,"

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                    UPDATE {}
                    SET {} started_at = NULL
                    WHERE id = (
                        SELECT id
                        FROM {}
                        WHERE started_at IS NOT NULL
                            AND id = %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id;"""
                    ).format(
                        sql.Identifier(self._table_name),
                        sql.SQL(decrease_ttl_sql),
                        sql.Identifier(self._table_name),
                    ),
                    (task_id,),
                )

                found = cur.fetchone()
                conn.commit()
                if found is None:
                    raise ValueError(f"Task {task_id} does not exist.")

    def _reset(self) -> None:
        """Delete all tasks in the DB with our queue name."""
        with self.pool.connection() as conn:
            conn.execute(
                sql.SQL("DELETE FROM {} WHERE queue_name = %s ").format(
                    sql.Identifier(self._table_name)
                ),
                (self._queue_name,),
            )
            conn.commit()

    def prune_completed_tasks(self, before: int) -> None:
        """Delete all completed tasks older than the given number of seconds.

        Parameters
        ----------
        before : int
            Seconds in the past from which completed task will be deleted

        """
        # Make sure the pruning time is an actual number
        before = int(before)
        logger.info(f"Pruning all tasks completed more than "
                    f"{before} second(s) ago.")

        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        """
                        DELETE FROM {}
                        WHERE queue_name = %s
                            AND completed_at IS NOT NULL
                            AND completed_at < current_timestamp - CAST(
                                %s || ' seconds' AS INTERVAL);
                        """
                    ).format(sql.Identifier(self._table_name)),
                    (self._queue_name, before),
                )

            conn.commit()

    def __iter__(
        self,
    ) -> Iterator[
                Tuple[
                    Dict[str, Any],
                    UUID,
                    str
                ]
            ]:
        """Iterate over tasks and mark them as complete.

        This allows to easily iterate over the tasks to process them:

            >>> for task, task_id in task_queue:
                    execute_task(task)

        it takes care of marking the tasks as done once they are processed
        and checking the emptiness of the queue before leaving.

        Notice that this iterator can wait for a long time waiting for work
        units to appear, depending on the value set as lease_timeout.

        Yields
        -------
        (any, UUID, str) :
            A tuple containing the task content, its id and the queue name

        """
        while True:
            task, id_, queue_name = self.get()
            if id_ is not None:
                # they are always None together, restrict the type
                assert task is not None
                assert queue_name is not None
                yield task, id_, queue_name
                self.complete(id_)
            if self.is_empty():
                logger.debug(
                    f"{self._queue_name} is empty. "
                    "Nothing to process anymore..."
                )
                break
