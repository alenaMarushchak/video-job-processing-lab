import pg from "pg";
import Redis from "ioredis";
import {
    JOBS_STATUS,
    QUEUE_NAMES
} from './constans.js';
import { sleep } from './helpers.js';

const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL ?? "postgres://postgres:pass@localhost:5432/video_service",
});
const redis = new Redis({host: "localhost"});

const WORKER_KEY = `worker-${process.pid}`;
const MAX_CONCURRENT = 2;
const MAX_ERROR_ATTEMPTS = 3

let inFlight = 0;

async function processJob(id) {
    let attempts;
    try {
        inFlight++;
        const updateRes = await pool.query(
            `UPDATE jobs
     SET status = $1, attempts = attempts + 1, worker_key = $2, last_heartbeat_at = now()
     WHERE id = $3 AND status = $4
     RETURNING attempts`,
            [JOBS_STATUS.IN_PROGRESS, WORKER_KEY, id, JOBS_STATUS.QUEUE],
        );

        if (updateRes.rows.length === 0) return;

        attempts = updateRes.rows[0].attempts;

        async function doWork() {
            let timer;
            try {
                timer = setInterval(async () => {
                   try {
                       const r = await pool.query("UPDATE jobs SET last_heartbeat_at = now() WHERE status = $1 AND worker_key = $2 AND attempts = $3 AND id=$4",
                           [JOBS_STATUS.IN_PROGRESS, WORKER_KEY, attempts, id]);
                       if (r.rowCount === 0) {
                           clearInterval(timer);
                       }
                   } catch (err) {
                       console.error(err);
                   }
                }, 5000);
                await sleep(30000);
            } catch (err) {
                console.error(err);
                throw err;
            }
            finally {
                clearInterval(timer);
            }
        }

        await doWork();

        const res = await pool.query(
            `UPDATE jobs
     SET status = $1, worker_key = $2, last_heartbeat_at = now()
     WHERE id = $3 AND status = $4 and attempts = $5
     RETURNING attempts`,
            [JOBS_STATUS.SUCCESS, WORKER_KEY, id, JOBS_STATUS.IN_PROGRESS, attempts],
        );

        if (res.rowCount === 1) {
            console.log("updated ", id);
        } else {
            console.log("Nothing changed");
        }
    } catch (err) {
        console.error(err);
        const res = await pool.query(
            `UPDATE jobs
     SET error_attempts = error_attempts + 1,
         status = CASE WHEN error_attempts + 1 >= $1 THEN $2 ELSE $3 END
     WHERE id = $4 AND status = $5 AND attempts = $6
     RETURNING status`,
            [MAX_ERROR_ATTEMPTS, JOBS_STATUS.FAILED, JOBS_STATUS.QUEUE, id, JOBS_STATUS.IN_PROGRESS, attempts],
        );

        if (res.rows.length > 0){
            if (res.rows[0].status === JOBS_STATUS.FAILED) {
                await redis.rpush(QUEUE_NAMES.ERROR_JOBS, id);
            } else if (res.rows[0].status === JOBS_STATUS.QUEUE) {
                await redis.rpush(QUEUE_NAMES.JOBS, id);
            }
        }
    } finally {
        inFlight--;
    }
}

while (true) {
    if (inFlight === MAX_CONCURRENT) {
        await sleep(200);
        continue;
    }

    const item = await redis.blpop(QUEUE_NAMES.JOBS, 5);
    if (!item) continue;

    const id = item[1];
    processJob(id)
}

