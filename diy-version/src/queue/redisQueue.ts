// ─────────────────────────────────────────────────────────────────────────────
// Redis Task Queue
//
// Implements a two-queue pattern:
//   diy:tasks:ready    — Redis LIST  (RPUSH to enqueue, BLPOP to dequeue)
//   diy:tasks:delayed  — Redis ZSET  (score = execute_at timestamp in ms)
//
// The scheduler periodically moves expired delayed tasks to the ready queue.
// This is how retries with exponential backoff and timer-based wakeups work.
//
// COMPARISON to Temporal:
//   Temporal Task Queue  →  diy:tasks:ready  (immediate dispatch)
//   Temporal Timer       →  diy:tasks:delayed (time-indexed dispatch)
//   Temporal Retry       →  re-enqueue into diy:tasks:delayed with backoff score
//
// WHY two queues?
//   A plain list cannot represent "execute at time T". A sorted set (ZSET)
//   lets us store tasks with a score = timestamp, then range-query for
//   all tasks ready to execute (score <= now).
// ─────────────────────────────────────────────────────────────────────────────

import Redis from 'ioredis';
import type { QueueTask } from '../types';

export const READY_QUEUE    = 'diy:tasks:ready';
export const DELAYED_QUEUE  = 'diy:tasks:delayed';

let _redis: Redis | null = null;

export function getRedis(): Redis {
  if (!_redis) {
    _redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379', {
      maxRetriesPerRequest: 3,
    });
    _redis.on('error', (err) => console.error('[redis] Error:', err.message));
    _redis.on('connect', () => console.log('[redis] Connected'));
  }
  return _redis;
}

export async function waitForRedis(maxAttempts = 30): Promise<void> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await getRedis().ping();
      console.log('[redis] Redis connection established');
      return;
    } catch (err) {
      console.log(`[redis] Waiting for Redis... (attempt ${attempt}/${maxAttempts})`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('Could not connect to Redis');
}

// Enqueue a task for immediate execution
export async function enqueueImmediate(task: QueueTask): Promise<void> {
  await getRedis().rpush(READY_QUEUE, JSON.stringify(task));
  console.log(`[queue] → Enqueued immediate | taskId=${task.taskId} | type=${task.taskType} | workflowId=${task.workflowId}`);
}

// Enqueue a task to execute at a specific timestamp (delayed/retry)
export async function enqueueDelayed(task: QueueTask, executeAtMs: number): Promise<void> {
  await getRedis().zadd(DELAYED_QUEUE, executeAtMs, JSON.stringify(task));
  const delay = Math.round((executeAtMs - Date.now()) / 1000);
  console.log(`[queue] ⏰ Enqueued delayed | taskId=${task.taskId} | type=${task.taskType} | delay=${delay}s`);
}

// Block and wait for the next ready task (5-second timeout)
// Returns null if no task arrives within the timeout
export async function dequeueTask(timeoutSeconds = 5): Promise<QueueTask | null> {
  const result = await getRedis().blpop(READY_QUEUE, timeoutSeconds);
  if (!result) return null;
  const [, raw] = result;
  try {
    return JSON.parse(raw) as QueueTask;
  } catch {
    console.error('[queue] Failed to parse task:', raw);
    return null;
  }
}

// Move all due delayed tasks to the ready queue.
// Called by the scheduler process periodically.
export async function promoteDelayedTasks(): Promise<number> {
  const now = Date.now();
  const redis = getRedis();

  // Get all tasks with score (execute_at_ms) <= now
  const items = await redis.zrangebyscore(DELAYED_QUEUE, 0, now);
  if (items.length === 0) return 0;

  // Pipeline: remove from delayed set + add to ready list atomically
  const pipeline = redis.pipeline();
  for (const item of items) {
    pipeline.zrem(DELAYED_QUEUE, item);
    pipeline.rpush(READY_QUEUE, item);
  }
  await pipeline.exec();

  console.log(`[queue] ↑ Promoted ${items.length} delayed task(s) to ready queue`);
  return items.length;
}

// Peek at the queue lengths (for monitoring)
export async function queueStats(): Promise<{ ready: number; delayed: number }> {
  const redis = getRedis();
  const [ready, delayed] = await Promise.all([
    redis.llen(READY_QUEUE),
    redis.zcard(DELAYED_QUEUE),
  ]);
  return { ready, delayed };
}

export async function closeRedis(): Promise<void> {
  if (_redis) {
    await _redis.quit();
    _redis = null;
  }
}
