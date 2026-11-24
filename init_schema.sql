
CREATE TABLE jobs (
                      id                SERIAL PRIMARY KEY,
                      file_location     TEXT NOT NULL,
                      status            TEXT NOT NULL DEFAULT 'CREATED'
                          CHECK (status IN ('CREATED','QUEUE','IN_PROGRESS','ERROR_JOB','SUCCESS','FAILED')),
                      attempts          INT NOT NULL DEFAULT 0,
                      error_attempts    INT NOT NULL DEFAULT 0,
                      worker_key        TEXT,
                      last_heartbeat_at TIMESTAMPTZ,
                      queued_at         TIMESTAMPTZ,
                      created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);