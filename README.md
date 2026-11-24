# Async Video Job Processing

A small backend service that accepts video-processing requests, runs them asynchronously, and stays correct when workers crash, jobs fail, or messages are delivered more than once. Built as a practice solution to the "Async Job Processing" task from a Senior Backend Engineer interview workbook.

## Stack

Node.js (ESM, Express), PostgreSQL, Redis (ioredis), Docker Compose.

## Components

| File | Role |
| --- | --- |
| `index.js` | HTTP API: `POST /jobs`, `GET /jobs/:id`, `GET /health` |
| `video-worker.js` | Consumes the queue, processes jobs, sends heartbeats (processing is simulated with `sleep`) |
| `watchdog.js` | Finds jobs stuck in `IN_PROGRESS` and re-queues them or marks them `FAILED` |
| `sweeper.js` | Recovers jobs that are in the database but missing from Redis (`CREATED` / `QUEUE`) |
| `test-jobs.js` | Creates jobs and injects duplicate queue entries to verify no double processing |
| `init_schema.sql` | Database schema |

## How to run

```bash
docker compose up -d
# create the table
docker compose exec -T postgres psql -U postgres -d video_service < init_schema.sql
npm install
node index.js          # API on :3000
node video-worker.js   # run several instances to test concurrency
node watchdog.js
node sweeper.js
```

The database URL can be overridden with the `DATABASE_URL` environment variable.

Create a job:

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{"file_location":"s3://bucket/video-1.mp4"}'
```

## Design

### Job lifecycle

`POST /jobs` receives a reference to a file that is already uploaded to object storage (not the file itself). The handler does three things in a fixed order:

1. `INSERT` the job (status `CREATED`).
2. `UPDATE` it to `QUEUE` and set `queued_at`.
3. `RPUSH` the job id to a Redis list.
   The database is the source of truth, so it is updated before Redis. If the process dies between steps 2 and 3, the job is in the database but not in Redis, and the sweeper recovers it. The reverse order would let a worker pick up an id before the database knows it is queued.

A worker takes ids with `BLPOP`, claims the job, processes it, and marks it `SUCCESS`. Clients read the state with `GET /jobs/:id`.

Statuses: `CREATED`, `QUEUE`, `IN_PROGRESS`, `SUCCESS`, `FAILED` (`ERROR_JOB` is defined in the schema but not used yet).

### Preventing double processing

Two mechanisms, both implemented as conditional `UPDATE` statements:

1. **Claim.** A worker runs `UPDATE ... SET status = 'IN_PROGRESS', attempts = attempts + 1 WHERE id = $1 AND status = 'QUEUE' RETURNING attempts`. Only one worker gets a row back; any other worker (or a duplicate queue entry) gets zero rows and skips the job.
2. **Attempt number as a fencing token.** `attempts` increases on every claim, so it works as the number of the current attempt. The worker remembers the value returned by its claim, and both its heartbeat and its final `UPDATE ... SET status = 'SUCCESS'` include `AND attempts = <my value>`. A late worker whose job was already re-queued and claimed by someone else gets zero rows and cannot overwrite the newer attempt. Checking `status` alone is not enough, because the old and the new attempt are both `IN_PROGRESS`. This is optimistic locking.
### Concurrency control

Each worker keeps an in-flight counter and does not take a new job while it equals `MAX_CONCURRENT` (2). The limit is derived from the resource limits of one pod (CPU and memory per transcoding task), not from live CPU metrics, which lag behind and would let a worker grab everything at once.

### Failure handling

1. **A worker dies mid-job.** The worker updates `last_heartbeat_at` every 5 seconds while it works. The watchdog finds `IN_PROGRESS` jobs with a stale heartbeat and returns them to `QUEUE` with a conditional `UPDATE` (`status` and `attempts` must still match). Another worker claims the job with a new attempt number; the old worker's late final `UPDATE` affects zero rows.
2. **The work itself throws.** The worker increments `error_attempts`. Below the limit (`MAX_ERROR_ATTEMPTS = 3`) the job goes back to `QUEUE` and is pushed to Redis again; at the limit it becomes `FAILED` and its id is pushed to a dead-letter list (`error_jobs_queue`). The database is updated first, then Redis.
3. **A poison job keeps killing workers.** No error reaches the worker's `catch`, so `error_attempts` does not move. The watchdog also caps `attempts`: at `MAX_ATTEMPTS` (5) it marks the job `FAILED` instead of re-queueing forever. A `FAILED` job with `error_attempts = 0` therefore points to a crashing or hanging job rather than a job that failed cleanly.
   `attempts` and `error_attempts` are kept separate on purpose: infrastructure failures should not be counted as failures of the job itself.

### Recovering lost queue entries

The sweeper handles jobs that exist in the database but not in Redis. `CREATED` jobs older than one minute are queued. `QUEUE` jobs are examined only when the Redis list is empty, because with a non-empty list they are most likely just waiting. Duplicate queue entries are harmless thanks to the claim step.

## Tests performed

Manual end-to-end checks with several worker processes:

- Duplicate queue entries (each job pushed three times, two workers): every job ends `SUCCESS` with `attempts = 1`.
- Worker killed mid-job: heartbeat stops, the watchdog re-queues, another worker finishes the job with `attempts = 2`.
- Heartbeat disabled and processing longer than the watchdog threshold: stale workers finish with zero updated rows and do not overwrite newer attempts.
- Random failures in processing: `error_attempts` grows to 3, then the job is `FAILED` and appears in the dead-letter list.
- Poison job (worker never reports an error): the job becomes `FAILED` at `attempts = 5`.
## Known limitations

- Delivery is at-least-once; real processing must be idempotent with respect to output files.
- No exponential backoff between retries.
- Nobody consumes the dead-letter list automatically.
- The watchdog threshold is set to 20 seconds for testing and should be larger in production (longer than several heartbeat intervals).
- No automated test suite or metrics yet; no authentication on the API.
- Redis is not a durable queue; its loss is covered by the database and the sweeper rather than by Redis persistence.
 