import json
import logging

from uuid import uuid4, UUID
from typing import Optional, Tuple, Iterator, Dict, Any, Callable

from psycopg import sql, connect


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
        Establish a connection to Postgres.
        If a connection already exists, it's overwritten.
        """
        self.conn = connect(self._dsn)

    def _create_queue_table(self) -> None:
        """
        Creates a task_queue table
        """
        # TODO: check if the table already exist
        # whether it has the same schema
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """CREATE TABLE IF NOT EXISTS  {} (
                            id UUID PRIMARY KEY,
                            queue_name TEXT NOT NULL,
                            task JSONB NOT NULL,
                            ttl INT NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            processing BOOLEAN NOT NULL DEFAULT false,
                            lease_timeout FLOAT,
                            deadline TIMESTAMP,
                            completed_at TIMESTAMP
                        )"""
                ).format(sql.Identifier(self._table_name))
            )

    def __len__(self) -> int:
        """
        Returns the length of processing or to be processed tasks
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                SELECT count(1) as count
                FROM {}
                WHERE queue_name = %s
                    AND completed_at IS NULL
            """
                ).format(sql.Identifier(self._table_name)),
                (self._queue_name,),
            )
            row = cursor.fetchone()
            count: int = row[0] if row else 0
            self.conn.commit()
            return count

    def add(
        self, task: Dict[str, Any], lease_timeout: float, ttl: int = 3
    ) -> None:
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

        """
        # make sure the timeout is an actual number, otherwise we'll run
        # into problems later when we calculate the actual deadline
        lease_timeout = float(lease_timeout)

        id_ = str(uuid4())

        serialized_task = self._serialize(task)

        with self.conn.cursor() as cursor:
            # store the task + metadata and put task-id into the task queue
            cursor.execute(
                sql.SQL(
                    """
                INSERT INTO {} (
                    id,
                    queue_name,
                    task,
                    ttl,
                    lease_timeout
                )
                VALUES (%s, %s, %s, %s, %s)
            """
                ).format(sql.Identifier(self._table_name)),
                (id_, self._queue_name, serialized_task, ttl, lease_timeout),
            )
            self.conn.commit()

    def get(self) -> Tuple[Optional[Dict[str, Any]], Optional[UUID]]:
        """Get a task from the task queue (non-blocking).

        This statement marks the next available task in the queue as
        "processing" and returns its ID and task details. The query
        uses a FOR UPDATE SKIP LOCKED clause to lock the selected
        task so that other workers can't select the same task simultaneously.

        After executing the query, the method fetches the result using
        cur.fetchone(). If no task is found, the method returns None, None.
        Otherwise, it returns the task and its ID.

        Note that this method is non-blocking, which means it returns
        immediately even if there is no task available in the queue..
        In order to mark that task as done, you have
        to use:

            >>> task, task_id = taskqueue.get()
            >>> # do something
            >>> taskqueue.complete(task_id)

        After some time (i.e. `lease_timeout`) tasks expire and are
        marked as not processing and the TTL is decreased by
        one. If TTL is still > 0 the task will be retried.

        Note, this method is non-blocking, i.e. it returns immediately
        even if there is nothing to return. See below for the return
        value for this case.

        Returns
        -------
        (task, task_id) :
            The next item from the task list or (None, None) if it's
            empty

        """
        conn = self.conn

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                UPDATE {}
                SET processing = true,
                    deadline =
                        NOW() + CAST(lease_timeout || ' seconds' AS INTERVAL)
                WHERE id = (
                    SELECT id
                    FROM {}
                    WHERE completed_at IS NULL
                        AND processing = false
                        AND queue_name = %s
                        AND ttl > 0
                    ORDER BY created_at
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
            if row is None:
                return None, None
            task_id, task = row
            logger.info(f"Got task with id {task_id}")
            conn.commit()
            return task, task_id

    def complete(self, task_id: Optional[UUID]) -> None:
        """Mark a task as completed.

        Marks a task as completed by setting completed_at column by
        the current timestamp.

        If the job is in the queue, which happens if it took too long
        and it expired, is removed from that too.


        Parameters
        ----------
        task_id : UUID | None
            the task ID

        """
        logger.info(f"Marking task {task_id} as completed")
        conn = self.conn
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                UPDATE {}
                SET completed_at = NOW(),
                    processing = false
                WHERE id = %s"""
                ).format(sql.Identifier(self._table_name)),
                (task_id,),
            )
            conn.commit()

    def is_empty(self) -> bool:
        """Check if the task queue is empty.

        Internally, this function also checks the currently processed
        tasks for expiration and teals with TTL and re-scheduling them
        into the task queue by marking them as not processing.

        Returns
        -------
        bool

        """
        self._check_expired_leases()
        return len(self) == 0

    def _check_expired_leases(self) -> None:
        """Check for expired leases.

        This method goes through all tasks that are currently processed
        and checks if their deadline expired. If not we assume the
        worker died. We decrease the TTL and if TTL is still > 0 we
        reschedule the task into the task queue or, if the TTL is
        exhausted, we mark the task as completed by setting
        `completed_at` column with current timestamp.

        Note: lease check is only performed against the tasks in
        that are processing.

        """
        # goes through all the tasks that are marked as processing
        # and check the ones with expired timeout
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                SELECT id
                FROM {}
                WHERE completed_at IS NULL
                    AND processing = true
                    AND queue_name = %s
                    AND deadline < NOW()
                ORDER BY created_at;
            """
                ).format(sql.Identifier(self._table_name)),
                (self._queue_name,),
            )
            expired_tasks = cur.fetchall()
            self.conn.commit()
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
                self.conn.commit()
                self.complete(task_id)

                if self.ttl_zero_callback:
                    self.ttl_zero_callback(task_id, task)
            self.conn.commit()

    def get_updated_expired_task(
        self, task_id: UUID
    ) -> Tuple[Optional[str], Optional[int]]:
        """
        Given the id of an expired task, it tries to reschedule the
        task by marking it as not processing, resetting the deadline
        and decreaasing TTL by one. It returns None if the task is
        already updated or (being updated) by another worker.

        Returns
        -------
        (task, ttl) :
            The updated task and ttl values for the expired task with
            task_id after it's rescheduled

        """
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                UPDATE {}
                SET ttl = ttl - 1,
                    processing = false,
                    deadline = NULL
                WHERE id = (
                    SELECT id
                    FROM {}
                    WHERE completed_at IS NULL
                        AND processing = true
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

    def reschedule(self, task_id: Optional[UUID]) -> None:
        """Move a task back from the processing- to the task queue.

        Workers can use this method to "drop" a work unit in case of
        eviction.

        This function does not modify the TTL.

        Parameters
        ----------
        task_id : str
            the task ID

        Raises
        ------
        ValueError :
            Task is not being processed, and cannot be re-scheduled

        """

        if not isinstance(task_id, UUID):
            raise ValueError("task_id must be a UUID")
        logger.info(f"Rescheduling task {task_id}..")
        conn = self.conn
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                UPDATE {}
                SET processing = false,
                    deadline = NULL
                WHERE id = (
                    SELECT id
                    FROM {}
                    WHERE processing = true
                        AND id = %s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id;"""
                ).format(
                    sql.Identifier(self._table_name),
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
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("DELETE FROM {} WHERE queue_name = %s ").format(
                    sql.Identifier(self._table_name)
                ),
                (self._queue_name,),
            )

            self.conn.commit()

    def __iter__(
        self,
    ) -> Iterator[Tuple[Optional[Dict[str, Any]], Optional[UUID]]]:
        """Iterate over tasks and mark them as complete.

        This allows to easily iterate over the tasks to process them:

            >>> for task in task_queue:
                    execute_task(task)

        it takes care of marking the tasks as done once they are processed
        and checking the emptiness of the queue before leaving.

        Notice that this iterator can wait for a long time waiting for work
        units to appear, depending on the value set as lease_timeout.

        Yields
        -------
        (any, str) :
            A tuple containing the task content and its id

        """
        while True:
            task, id_ = self.get()
            if id_ is not None:
                yield task, id_
                self.complete(id_)
            if self.is_empty():
                logger.debug(
                    f"{self._queue_name} is empty. "
                    "Nothing to process anymore..."
                )
                break
