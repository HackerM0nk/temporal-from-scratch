# Side-by-Side Comparison: Temporal vs DIY

## Concept Mapping Table

| Concept | Temporal | DIY System |
|---------|----------|-----------|
| **Workflow** | A function that `await`s activities and sleeps. Its execution point is implicit in history. | A row in `subscription_workflows` with an explicit `state` column. |
| **Activity** | An `async function` exported from activities module. Temporal dispatches it to a worker. | Same `async function`, called directly by the worker after lock + idempotency checks. |
| **Workflow History** | Append-only event log maintained by Temporal Server. Used for replay to reconstruct execution state after crash. | `workflow_events` table. Append-only audit log. NOT used for replay (we replay from `state` column). |
| **Timer / Sleep** | `workflow.sleep(ms)` or `condition(fn, timeout)`. Timer stored server-side. Fires even if all workers are down. | `trial_end_at TIMESTAMPTZ` column + Scheduler process polls `SELECT WHERE trial_end_at <= NOW()`. If scheduler is down, timer fires late. |
| **Task Queue** | Named queue managed by Temporal Server. Workers long-poll for tasks. | Redis LIST (`diy:tasks:ready`). Workers use `BLPOP`. Scheduler uses sorted set for delayed tasks. |
| **Worker** | Process that polls Temporal Server for both workflow tasks (replay) and activity tasks (execute). | Process that polls Redis for tasks, acquires a DB lock, and directly executes activities. |
| **Retry** | Configured in `proxyActivities` retry policy. Temporal handles scheduling retries with backoff automatically. | Worker re-enqueues to Redis sorted set (`diy:tasks:delayed`) with score = `now + backoff_ms`. Scheduler promotes when due. |
| **Cancellation** | Signal (`defineSignal`, `setHandler`). Delivered in-process to the running workflow coroutine. | HTTP POST to API → updates `state` column to `CANCELLATION_REQUESTED` → worker reads state on next iteration. |
| **Replay** | Temporal replays workflow code from history on every workflow task. Completed activities are skipped (their results injected from history). | Not used. Worker checks `state` column to determine where the workflow is and what to do next. |
| **Idempotency** | History-based: a completed `ActivityTaskCompleted` event means "don't run again." Activity-level: idempotency keys with external APIs. | Explicit `idempotency_keys` table. Worker always checks before executing. Also needed for external API calls. |
| **Reconciliation** | Built-in. Temporal Server detects heartbeat timeouts, reschedules tasks, and handles worker failures automatically. | Explicit `reconciler.ts` process. Releases expired locks, detects stuck workflows, re-enqueues lost tasks. Runs every 10s. |
| **Sticky Worker** | Temporal caches workflow state on the worker that last processed it for performance. Falls back to full replay if unavailable. | Explicit `workflow_locks` table with TTL. Only one worker processes a given workflow at a time. |
| **Namespace** | Logical isolation for workflows. Multiple namespaces in one Temporal cluster. | Not implemented (would be a `tenant_id` column). |

---

## Code Comparison: Starting a Workflow

**Temporal:**
```typescript
// One line — Temporal handles all durability
const handle = await client.workflow.start(subscriptionWorkflow, {
  args: [customerId],
  taskQueue: 'subscription-workflows',
  workflowId: `subscription-${customerId}`,
});
```

**DIY:**
```typescript
// We must explicitly:
// 1. Create workflow row in DB
// 2. Record event
// 3. Enqueue first task
await withTransaction(async (client) => {
  await client.query(`INSERT INTO subscription_workflows ...`);
  await client.query(`INSERT INTO workflow_events ...`);
});
await enqueueImmediate({ taskType: TaskType.SEND_WELCOME_EMAIL, ... });
```

---

## Code Comparison: Durable Timer

**Temporal:**
```typescript
// Single line — durable, survives all crashes
await condition(() => isCancelled, 30_000);
// If worker crashes mid-sleep, timer resumes on restart. Zero extra code.
```

**DIY:**
```typescript
// Worker: set trial_end_at column
await client.query(`
  UPDATE subscription_workflows
  SET trial_end_at = NOW() + INTERVAL '30 seconds'
  WHERE id = $1
`);

// Scheduler (separate process, polls every 2s):
const expired = await query(`
  SELECT * FROM subscription_workflows
  WHERE state = 'WAITING_FOR_TRIAL_END'
    AND trial_end_at <= NOW()
`);
for (const wf of expired.rows) {
  await enqueueImmediate({ taskType: TaskType.CHARGE_MONTHLY_FEE, ... });
}
```

---

## Code Comparison: Retry Policy

**Temporal:**
```typescript
proxyActivities<typeof Activities>({
  retry: {
    maximumAttempts: 5,
    initialInterval: '2 seconds',
    backoffCoefficient: 2.0,
  },
});
// Temporal handles all retry scheduling automatically
```

**DIY:**
```typescript
// Worker catches failure, computes backoff, enqueues to delayed set
} catch (err) {
  const delayMs = Math.pow(2, attempt) * 1000;  // 2s, 4s, 8s, 16s, 32s
  const retryTask = { ...task, attempt: attempt + 1 };
  await enqueueDelayed(retryTask, Date.now() + delayMs);

  // Scheduler promotes from delayed → ready when time comes
  await redis.zadd('diy:tasks:delayed', Date.now() + delayMs, JSON.stringify(retryTask));
}
```

---

## Code Comparison: Idempotency

**Temporal:**
```typescript
// Temporal history guarantees this activity only runs once per workflow.
// The only idempotency key needed is for the external API call itself.
await chargeMonthlyFee({ customerId, workflowId });
// If worker crashes AFTER the activity completes, replay skips it.
```

**DIY:**
```typescript
// Must check idempotency manually before EVERY activity
const key = `${workflowId}:${taskType}`;
const existing = await query(
  'SELECT result FROM idempotency_keys WHERE key = $1', [key]
);
if (existing.rows.length > 0) {
  // Already ran — skip and return cached result
  return;
}

// ... execute activity ...

// Must INSERT idempotency key atomically with state update
await withTransaction(async (client) => {
  await client.query(`INSERT INTO idempotency_keys ...`);
  await client.query(`UPDATE subscription_workflows SET state = ... `);
});
```

---

## What Temporal Eliminates

| Complexity | Without Temporal | With Temporal |
|-----------|-----------------|---------------|
| Crash recovery | Reconciler process + expired lock detection | Built-in |
| Timer storage | `trial_end_at` column + scheduler polling loop | Built-in (`workflow.sleep`) |
| Idempotency tracking | `idempotency_keys` table + check before each activity | Built-in via history |
| Retry scheduling | Delayed Redis queue + backoff math | Config in `proxyActivities` |
| Concurrent execution prevention | `workflow_locks` table with TTL | Built-in (server serializes tasks) |
| Activity history | `activity_attempts` table | Built-in (workflow history events) |
| State machine | Explicit `WorkflowState` enum + `TRANSITIONS` table | Implicit in code execution point |
| Workflow visibility | Custom query endpoints | Temporal UI + `temporal workflow list` |
| Stuck workflow detection | Reconciler `updated_at` check | Built-in (server timeouts) |
| Signal delivery guarantee | DB write + poll | Built-in (signals durable in history) |

**Total DIY infrastructure code:** ~1500 lines across worker, scheduler, reconciler, schema
**Total Temporal infrastructure code:** ~0 lines (it's all in the Temporal cluster)
**Total workflow code (Temporal):** ~70 lines
**Total equivalent workflow code (DIY):** ~250 lines in worker + state machine
