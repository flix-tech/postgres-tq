UPDATE task_queue
                    SET processing = false,
                        deadline = NULL,
                        ttl = ttl - 1
                    WHERE id = (
                        SELECT id
                        FROM task_queue
                        WHERE completed_at IS NULL 
                            AND processing = true
                            AND queue_name = %s
                        ORDER BY created_at
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, task;