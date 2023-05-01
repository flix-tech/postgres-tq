CREATE TABLE IF NOT EXISTS task_queue (
                    id UUID PRIMARY KEY,
                    queue_name TEXT NOT NULL,
                    task JSONB NOT NULL,
                    ttl INT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    processing BOOLEAN NOT NULL DEFAULT false,
                    lease_timeout FLOAT, 
                    deadline TIMESTAMP,
                    completed_at TIMESTAMP
            )

