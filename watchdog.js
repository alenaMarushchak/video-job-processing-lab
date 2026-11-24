import pg from "pg";
import Redis from "ioredis";
import {
    JOBS_STATUS,
    QUEUE_NAMES
} from './constans.js';
import {sleep} from './helpers.js';

const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL ?? "postgres://postgres:pass@localhost:5432/video_service",
});
const redis = new Redis({host: "localhost"});
const MAX_ATTEMPTS = 5;

while (true) {
    try {
        const stuck = await pool.query(
            `SELECT id, attempts
             FROM jobs
             WHERE status = $1
               AND last_heartbeat_at < now() - interval '20 seconds'`,
            [JOBS_STATUS.IN_PROGRESS],
        );

        for (const job of stuck.rows) {
            const r = await pool.query(
                `UPDATE jobs
                 SET status = CASE WHEN attempts >= $1 THEN $2 ELSE $3 END,
                     queued_at = now()
                 WHERE id = $4 AND status = $5 AND attempts = $6
                     RETURNING status`,
                [MAX_ATTEMPTS, JOBS_STATUS.FAILED, JOBS_STATUS.QUEUE, job.id, JOBS_STATUS.IN_PROGRESS, job.attempts],
            );

            if (r.rows.length > 0) {
                if (r.rows[0].status === JOBS_STATUS.QUEUE) {
                    await redis.rpush(QUEUE_NAMES.JOBS, job.id);
                } else if (r.rows[0].status === JOBS_STATUS.FAILED) {
                    await redis.rpush(QUEUE_NAMES.ERROR_JOBS, job.id);
                }
            }
        }
    } catch (err) {
        console.error(err);
    }

    await sleep(30_000);
}