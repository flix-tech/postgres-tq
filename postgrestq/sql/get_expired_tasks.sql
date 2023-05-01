SELECT * FROM task_queue
WHERE completed_at IS NULL 
    AND processing = true
    AND queue_name = %s
    AND deadline < NOW()
ORDER BY created_at