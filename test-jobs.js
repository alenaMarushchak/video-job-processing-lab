import pg from "pg";
import Redis from "ioredis";
import { QUEUE_NAMES } from "./constans.js";

const API = "http://localhost:3000";
const JOBS = 5;
const DUPLICATES = 2;

const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL ?? "postgres://postgres:pass@localhost:5432/video_service",
});
const redis = new Redis({ host: "localhost" });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const ids = [];
for (let i = 0; i < JOBS; i++) {
    const res = await fetch(`${API}/jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_location: `s3://bucket/video-${i}.mp4` }),
    });
    const body = await res.json();
    ids.push(body.id);
    for (let d = 0; d < DUPLICATES; d++) {
        await redis.rpush(QUEUE_NAMES.JOBS, body.id);
    }
}
console.log("created ids:", ids);

await sleep(8000);

const { rows } = await pool.query(
    "SELECT id, status, attempts, worker_key FROM jobs WHERE id = ANY($1) ORDER BY id",
    [ids],
);
console.table(rows);

await pool.end();
await redis.quit();