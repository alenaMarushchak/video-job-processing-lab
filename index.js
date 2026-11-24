import express from "express";
import pg from "pg";
import Redis from 'ioredis';

import {
    JOBS_STATUS,
    QUEUE_NAMES
} from './constans.js';

const port = 3000;
const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL ?? "postgres://postgres:pass@localhost:5432/video_service",
});
export const redis = new Redis({
    host: 'localhost',
    commandTimeout: 100,
    enableOfflineQueue: false,
    lazyConnect: true,
});
redis.on("error", (err) => console.error("redis error:", err.message));
await redis.connect();

const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
    try {
        await pool.query("SELECT 1");
        await redis.ping();
        res.json({ status: "ok", db: "ok", redis: "ok" });
    } catch (err) {
        res.status(503).json({ status: "error", message: err.message });
    }
});


app.get("/jobs/:id", async (req, res) => {
    const id = req.params.id;

    if (!id) return res.json({ status: "error id is required" });

    try {
        const getRes = await pool.query(
            `SELECT * FROM jobs WHERE id = $1
             `,
            [id]
        );

        const result = getRes.rows[0];

        if (!result) return res.status(404).json({ status: "Not found" });

        return res.json(result);
    } catch (err) {
       return res.status(503).json({ status: "error", message: err.message });
    }
});

app.post("/jobs", async (req, res) => {
    const file_location = typeof req.body?.file_location === "string" ? req.body.file_location.trim() : "";
    if (!file_location) {
        res.status(400).json({ error: "file_location is required" });
        return;
    }
    try{
        const insertRes = await pool.query(
            "INSERT INTO jobs (file_location) VALUES ($1) RETURNING id",
            [file_location],
        );
        const result = insertRes.rows[0];

        await pool.query(
            "UPDATE jobs SET status = $1, queued_at = now() WHERE id = $2",
            [JOBS_STATUS.QUEUE, result.id],
        );

        await redis.rpush(QUEUE_NAMES.JOBS, result.id);

        res.status(201).json({
            message: 'Video file started processing',
            id: result.id,
        });
    } catch (err) {
        res.status(503).json({ status: "error", message: err.message });
    }
});

const server = app.listen(port, () => {
    console.log(`video-api listening on port ${port}`);
});

process.on("SIGTERM", () => {
    console.log("SIGTERM received, shutting down");
    server.close(async () => {
        await pool.end();
        await redis.close();
        process.exit(0);
    });
});

//const item = await redis.blpop(QUEUE_NAMES.JOBS, 5);