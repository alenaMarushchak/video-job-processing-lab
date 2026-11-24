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


while (true) {
    try {
        const queueLen = await redis.llen(QUEUE_NAMES.JOBS);
        const statuses = queueLen === 0
            ? [JOBS_STATUS.CREATED, JOBS_STATUS.QUEUE]
            : [JOBS_STATUS.CREATED];

        const stuck = await pool.query(
            `SELECT id FROM jobs
             WHERE status = ANY($1) AND COALESCE(queued_at, created_at) < now() - interval '1 minute'`,
            [statuses],
        );

        for (const job of stuck.rows) {
            const r = await pool.query(
                `UPDATE jobs SET status = $1, queued_at = now()
                 WHERE id = $2 AND status = ANY($3) AND COALESCE(queued_at, created_at) < now() - interval '1 minute'`,
                [JOBS_STATUS.QUEUE, job.id, statuses],
            );

            if (r.rowCount > 0) {
                await redis.rpush(QUEUE_NAMES.JOBS, job.id);
            }
        }
    } catch (err) {
        console.error(err);
    }
    await sleep(30_000);
}
