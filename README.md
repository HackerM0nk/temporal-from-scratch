# durable-workflow-learning

The same subscription billing workflow implemented twice: once with [Temporal](https://temporal.io), once with PostgreSQL + Redis wired together by hand.

The DIY version makes the problems Temporal solves **explicit**. `workflow.sleep(30_000)` is one line. The equivalent in the DIY version is:

```typescript
// worker.ts — record the timer
await db.query(`
  UPDATE subscription_workflows
  SET state = 'WAITING_FOR_TRIAL_END',
      trial_end_at = NOW() + INTERVAL '30 seconds'
  WHERE id = $1
`, [workflowId]);

// scheduler.ts — separate process, polls every 2 seconds
const due = await db.query(`
  SELECT * FROM subscription_workflows
  WHERE state = 'WAITING_FOR_TRIAL_END' AND trial_end_at <= NOW()
`);
for (const wf of due.rows) await redis.rpush('diy:tasks:ready', chargeTask(wf));

// reconciler.ts — separate process, in case scheduler was down when timer fired
if (wf.state === 'WAITING_FOR_TRIAL_END' && wf.trial_end_at < Date.now()) {
  await redis.rpush('diy:tasks:ready', chargeTask(wf));
}
```

That's one timer. The same pattern repeats for retries, idempotency, crash recovery, and distributed locks. This repo lets you run both versions side-by-side, break them, and watch the recovery.

---

## The workflow

A subscription lifecycle with a 30-day free trial (30 seconds in the demo):

```
sign-up
  │
  ▼
sendWelcomeEmail
  │
  ▼
wait 30 days ──── cancel signal arrives ──► processSubscriptionCancellation
  │                                         sendSorryToSeeYouGoEmail
  ▼
chargeMonthlyFee           (retries on transient failure, idempotent)
  │
  ▼
sendEndOfTrialEmail
sendMonthlyChargeEmail
  │
  ▼
COMPLETED
```

---

## What each concept maps to

| | Temporal | DIY |
|---|---|---|
| Workflow state | implicit — current `await` in history | `state VARCHAR` column in `subscription_workflows` |
| Durable timer | `workflow.sleep()` / `condition(fn, timeout)` | `trial_end_at TIMESTAMPTZ` + `scheduler.ts` |
| Activity retry | `retry` config in `proxyActivities` | ZADD to `diy:tasks:delayed` sorted set with backoff score |
| Idempotency | history replay — completed activities skipped | `idempotency_keys` table, checked before every execution |
| Cancellation | `defineSignal` + `setHandler` | HTTP POST → UPDATE state + RPUSH cancel task |
| Crash recovery | server reassigns after heartbeat timeout | `reconciler.ts` — deletes expired locks, re-enqueues |
| Concurrent execution | server serialises workflow tasks | `workflow_locks` table with TTL |
| Dead letter | workflow fails after max retries | `dead_letter_tasks` table |

---

## Structure

```
temporal-version/src/
  workflows/subscriptionWorkflow.ts   the entire workflow — ~100 lines
  activities/subscriptionActivities.ts
  worker.ts                           registers workflows + activities with Temporal
  api.ts / cli.ts

diy-version/src/
  db/schema.sql                       6 tables — read this first
  orchestrator/stateMachine.ts        15 explicit states + transition table
  queue/redisQueue.ts                 READY list + DELAYED sorted set
  worker.ts                           lock → idempotency → execute → commit → enqueue
  scheduler.ts                        fires durable timers (polls trial_end_at)
  reconciler.ts                       repairs workflows after worker crashes
  api.ts / cli.ts

docs/
  architecture.md                     sequence diagrams for both versions
  comparison.md                       concept-by-concept mapping
  failure-scenarios.md                7 failure modes analysed for both systems
```

---

## Running it

Requires Docker and Node.js 20+.

```bash
git clone https://github.com/HackerM0nk/durable-workflow-learning
cd durable-workflow-learning
```

**Option 1 — everything in Docker:**

```bash
docker compose up
```

**Option 2 — infrastructure in Docker, workers local** (easier to kill things):

```bash
# infrastructure
docker compose up postgres redis temporal temporal-ui

# build both versions
(cd temporal-version && npm install && npm run build)
(cd diy-version && npm install && npm run build)
```

Then in separate terminals:

```bash
# Temporal worker
TEMPORAL_ADDRESS=localhost:7233 node temporal-version/dist/worker.js

# Temporal HTTP API
TEMPORAL_ADDRESS=localhost:7233 PORT=3000 node temporal-version/dist/api.js

# DIY API (also runs migrations)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 RUN_MIGRATIONS=true \
node diy-version/dist/api.js

# DIY worker
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 \
node diy-version/dist/worker.js

# DIY scheduler (fires timers)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 \
node diy-version/dist/scheduler.js

# DIY reconciler (crash recovery)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 \
node diy-version/dist/reconciler.js
```

Temporal UI: http://localhost:8080

---

## Demo

### Start a subscription

```bash
# Temporal
npx ts-node temporal-version/src/cli.ts start customer-123

# DIY
npx ts-node diy-version/src/cli.ts start customer-456
```

Both run the same workflow. The Temporal version finishes in ~35 seconds. The DIY version does too — you can watch the state change in PostgreSQL:

```sql
-- in psql: postgresql://postgres:postgres@localhost:5432/diy_workflows
SELECT state, trial_end_at, metadata FROM subscription_workflows;
SELECT event_type, event_data FROM workflow_events ORDER BY id;
SELECT activity_type, attempt_number, status FROM activity_attempts;
```

### Cancel during the trial

```bash
npx ts-node temporal-version/src/cli.ts cancel customer-123 "switched-plans"
npx ts-node diy-version/src/cli.ts cancel customer-456 "switched-plans"
```

### Check status

```bash
npx ts-node temporal-version/src/cli.ts status customer-123
npx ts-node temporal-version/src/cli.ts history customer-123   # prints all history events

npx ts-node diy-version/src/cli.ts status customer-456
npx ts-node diy-version/src/cli.ts history customer-456        # prints workflow_events + activity_attempts
```

---

## Crash recovery demo

### Temporal

```bash
npx ts-node temporal-version/src/cli.ts start crash-test

# kill the worker mid-flight
pkill -f "node dist/worker"

# workflow stays RUNNING — the timer lives in Temporal's DB, not in the worker process
npx ts-node temporal-version/src/cli.ts status crash-test

# restart — it resumes where it stopped, sendWelcomeEmail does not run again
TEMPORAL_ADDRESS=localhost:7233 node temporal-version/dist/worker.js
```

### DIY

```bash
npx ts-node diy-version/src/cli.ts start crash-diy

# kill the worker
pkill -f "node dist/worker"

# the lock is still in workflow_locks, workflow is stuck
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT state FROM subscription_workflows WHERE customer_id = 'crash-diy';"
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT locked_by, expires_at FROM workflow_locks;"

# wait ~40s — reconciler releases the lock, detects the stuck workflow, re-enqueues the task
# restart the worker — it picks up the task, checks idempotency, continues
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 \
node diy-version/dist/worker.js
```

### Retry demo (charge fails twice)

```bash
SIMULATE_CHARGE_FAILURE=true TEMPORAL_ADDRESS=localhost:7233 \
  node temporal-version/dist/worker.js
npx ts-node temporal-version/src/cli.ts start retry-test
# Temporal UI shows: ActivityTaskFailed × 2, ActivityTaskCompleted × 1

SIMULATE_CHARGE_FAILURE=true \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 \
node diy-version/dist/worker.js
npx ts-node diy-version/src/cli.ts start retry-test-diy
# After ~35s:
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT activity_type, attempt_number, status FROM activity_attempts ORDER BY scheduled_at;"
```

---

## Failure scenarios

| Scenario | Temporal | DIY |
|---|---|---|
| Worker crashes after email sends | History records the completion. On restart, replay skips the activity — email not re-sent. | Reconciler re-enqueues. Idempotency key on the email service prevents a duplicate send. |
| Worker crashes while waiting for trial | Timer is stored server-side. Worker crash is irrelevant — it fires regardless. | `trial_end_at` is in PostgreSQL. Scheduler is a separate process and still running. |
| Charge fails, retries with backoff | Automatic — configured in `proxyActivities`. | Worker writes to `diy:tasks:delayed` sorted set with score `now + 2^attempt * 1000`. Scheduler promotes it when the score elapses. |
| Same task delivered twice | Temporal deduplicates workflow tasks at the server. | `idempotency_keys` table — `INSERT ... ON CONFLICT DO NOTHING` before every activity. |
| Cancel during trial wait | Signal written to history, delivered on next workflow task. | HTTP POST updates `state = CANCELLATION_REQUESTED`, enqueues the cancel task immediately. |
| DB updated, Redis publish fails | Not possible — Temporal's task queue and state are the same system. | Reconciler detects `state = CHARGED` with no pending task and re-enqueues `SEND_END_OF_TRIAL_EMAIL`. |
| Worker holds lock, crashes | Server detects heartbeat timeout, reassigns. | Reconciler runs `DELETE FROM workflow_locks WHERE expires_at < NOW()`, then re-enqueues. |

Full analysis with exact state transitions and event records: [`docs/failure-scenarios.md`](docs/failure-scenarios.md)

---

## DIY state machine

```
STARTED
  └─► WELCOME_EMAIL_SCHEDULED ─► WELCOME_EMAIL_SENT
                                        │
                                        ▼
                              WAITING_FOR_TRIAL_END ◄── cancel arrives here
                               │              │
                 timer fires   │              │  cancel
                               ▼              ▼
                     CHARGE_SCHEDULED   CANCELLATION_REQUESTED
                               │              │
                               ▼              ▼
                           CHARGING      CANCELLING
                               │              │
                               ▼              ▼
                           CHARGED        CANCELLED ✓
                               │
                               ▼
                     END_OF_TRIAL_EMAIL_SENT
                               │
                               ▼
                           COMPLETED ✓

  FAILED ✗ — any state, when max retries are exhausted
```

In Temporal, these states are implicit — the current `await` in the workflow function is the state. In the DIY version they are a `VARCHAR` column you can `SELECT` at any time.

---

## DIY database tables

```sql
subscription_workflows   canonical state + trial_end_at timer + metadata
workflow_events          append-only log of every state transition and activity result
activity_attempts        one row per execution attempt, with status and error
idempotency_keys         completed activity fingerprints — keyed by workflowId:activityType
workflow_locks           distributed mutex with TTL
dead_letter_tasks        tasks that exhausted retries
```

Schema with column-level comments: [`diy-version/src/db/schema.sql`](diy-version/src/db/schema.sql)

---

## License

MIT
