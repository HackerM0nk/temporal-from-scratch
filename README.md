# Durable Workflow Learning

> Build the same subscription lifecycle **twice** — once with Temporal, once by hand — so you can see exactly what Temporal gives you for free.

---

## What is this?

Most engineers use Temporal without ever understanding the distributed systems problems it solves. This repo makes those problems **visible** by implementing an identical subscription workflow in two ways:

| | Temporal Version | DIY Version |
|---|---|---|
| **Stack** | Temporal TypeScript SDK | PostgreSQL + Redis + Node.js |
| **Timers** | `workflow.sleep()` | `trial_end_at` column + Scheduler process |
| **Crash recovery** | Automatic (server reassigns) | `reconciler.ts` — detects expired locks, re-enqueues |
| **Retry** | Config in `proxyActivities` | Exponential backoff via Redis sorted set |
| **Idempotency** | History-based replay | `idempotency_keys` table — checked before every activity |
| **Cancellation** | `defineSignal` + `setHandler` | HTTP POST → DB write → task enqueue |
| **Workflow state** | Implicit in execution point | Explicit `state` column in PostgreSQL |

**The goal isn't production polish. The goal is to make concepts easy to grasp, inspect, break, and understand.**

---

## The Business Workflow

A subscription lifecycle — the same steps run in both versions:

```
Customer signs up
    │
    ▼
Send welcome email
    │
    ▼
Wait 30 days (30 seconds in demo)  ◄──── cancellation signal can arrive here
    │                                              │
    ▼ (trial ends)                                 ▼ (cancelled)
Charge monthly fee ($29.99)           Process cancellation
    │                                 Send "sorry to see you go" email
    ▼
Send end-of-trial email
Send receipt email
    │
    ▼
COMPLETED
```

**Accelerated time:** 30 seconds = 30 days.

---

## Architecture

### Temporal Version

```
┌─────────────────────────────────────────────────────────────────┐
│                         Temporal Server                          │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────┐  │
│  │  Workflow    │    │   Activity   │    │  Workflow History  │  │
│  │  Task Queue  │    │  Task Queue  │    │   (PostgreSQL)     │  │
│  └──────┬───────┘    └──────┬───────┘    └───────────────────┘  │
└─────────┼────────────────── ┼─────────────────────────────────── ┘
          │                   │
          ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Temporal Worker                           │
│                                                                  │
│  ┌────────────────────────┐   ┌──────────────────────────────┐  │
│  │   Workflow Sandbox     │   │    Activity Executor         │  │
│  │   (V8 Isolate)         │   │    (Normal Node.js)          │  │
│  │                        │   │                              │  │
│  │  Deterministic replay  │   │  sendWelcomeEmail()          │  │
│  │  of workflow history   │   │  chargeMonthlyFee()          │  │
│  │                        │   │  sendEndOfTrialEmail()       │  │
│  │  sleep() ──► TimerCmd  │   │  sendMonthlyChargeEmail()    │  │
│  │  condition() ─► Wait   │   │  processSubscriptionCancel() │  │
│  │  signal ──► Handler    │   │  sendSorryToSeeYouGoEmail()  │  │
│  └────────────────────────┘   └──────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Key insight:** Temporal's workflow code runs in a deterministic sandbox. Every `await` produces an event in workflow history. If the worker crashes, the server replays history to restore execution state — no activity is re-run.

### DIY Version

```
┌────────────┐   HTTP POST    ┌──────────────────────────────────┐
│  CLI / API │───────────────►│            API Server            │
└────────────┘                │  • Creates workflow row in DB    │
                              │  • RPUSHes first task to Redis   │
                              └──────────┬───────────────────────┘
                                         │
              ┌──────────────────────────▼──────────────────────┐
              │                   PostgreSQL                     │
              │                                                  │
              │  subscription_workflows  (state machine)         │
              │  workflow_events         (audit log)             │
              │  activity_attempts       (retry history)         │
              │  idempotency_keys        (deduplication)         │
              │  workflow_locks          (distributed mutex)     │
              │  dead_letter_tasks       (exhausted retries)     │
              └──────────────────────────────────────────────────┘
                         ▲               ▲               ▲
                         │               │               │
              ┌──────────┴──┐   ┌────────┴─────┐  ┌─────┴──────────┐
              │   Worker    │   │  Scheduler   │  │  Reconciler    │
              │             │   │              │  │                │
              │ BLPOP Redis │   │ Poll DB for  │  │ Release expired│
              │ Acquire lock│   │ trial_end_at │  │ locks          │
              │ Check idem. │   │ ≤ NOW()      │  │                │
              │ Execute act.│   │              │  │ Detect stuck   │
              │ Commit state│   │ RPUSH charge │  │ workflows      │
              │ RPUSH next  │   │ task         │  │                │
              └──────┬──────┘   └──────────────┘  │ Re-enqueue    │
                     │                             │ lost tasks    │
              ┌──────▼──────────────────┐          └────────────────┘
              │          Redis          │
              │                        │
              │  diy:tasks:ready  LIST  │  ← immediate dispatch
              │  diy:tasks:delayed ZSET │  ← retry with backoff
              └─────────────────────────┘
```

**Key insight:** Every guarantee Temporal provides automatically — timers, crash recovery, retry, idempotency — is a separate piece of code you have to write yourself.

---

## DIY State Machine

```
                        ┌─────────┐
                        │ STARTED │
                        └────┬────┘
                             │ first task enqueued
                             ▼
               ┌────────────────────────────┐
               │  WELCOME_EMAIL_SCHEDULED   │
               └──────────────┬─────────────┘
                              │ worker executes sendWelcomeEmail
                              ▼
               ┌──────────────────────────┐
               │    WELCOME_EMAIL_SENT    │
               └──────────────┬───────────┘
                              │ trial_end_at = NOW() + 30s
                              ▼
               ┌──────────────────────────┐
               │   WAITING_FOR_TRIAL_END  │ ◄── cancel API can interrupt here
               └──────────────┬───────────┘
                              │ scheduler: trial_end_at ≤ NOW()
                              ▼
               ┌──────────────────────────┐
               │     CHARGE_SCHEDULED     │
               └──────────────┬───────────┘
                              │ worker starts chargeMonthlyFee
                              ▼
               ┌──────────────────────────┐
               │        CHARGING          │ ◄── crash here = reconciler repairs
               └──────────────┬───────────┘
                              │ charge succeeds
                              ▼
               ┌──────────────────────────┐
               │         CHARGED          │
               └──────────────┬───────────┘
                              │ sendEndOfTrialEmail
                              ▼
               ┌──────────────────────────┐
               │  END_OF_TRIAL_EMAIL_SENT │
               └──────────────┬───────────┘
                              │ sendMonthlyChargeEmail
                              ▼
               ┌──────────────────────────┐
               │        COMPLETED         │ ✓
               └──────────────────────────┘


  Cancellation path (can enter from WAITING_FOR_TRIAL_END or earlier):

  CANCELLATION_REQUESTED → CANCELLING → CANCELLED ✓

  Failure path (max retries exhausted):

  Any state → FAILED  (task moved to dead_letter_tasks)
```

---

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Node.js 20+
- `npm`

### Run Everything in Docker

```bash
git clone https://github.com/HackerM0nk/durable-workflow-learning
cd durable-workflow-learning

docker compose up
```

Services started:

| Service | URL |
|---------|-----|
| Temporal Server | `localhost:7233` |
| **Temporal UI** | **http://localhost:8080** |
| PostgreSQL | `localhost:5432` |
| Redis | `localhost:6379` |
| Temporal HTTP API | `localhost:3000` |
| DIY HTTP API | `localhost:3001` |

### Run Infrastructure in Docker, Workers Locally

Better for learning — you can kill/restart workers easily and see clean logs.

```bash
# Terminal 1 — Infrastructure only
docker compose up postgres redis temporal temporal-ui

# Terminal 2 — Temporal worker
cd temporal-version && npm install
TEMPORAL_ADDRESS=localhost:7233 node dist/worker.js

# Terminal 3 — Temporal API
cd temporal-version
TEMPORAL_ADDRESS=localhost:7233 PORT=3000 node dist/api.js

# Terminal 4 — DIY API (runs migrations on first start)
cd diy-version && npm install
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 RUN_MIGRATIONS=true node dist/api.js

# Terminal 5 — DIY Worker
cd diy-version
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 node dist/worker.js

# Terminal 6 — DIY Scheduler (fires timers)
cd diy-version
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 node dist/scheduler.js

# Terminal 7 — DIY Reconciler (crash recovery)
cd diy-version
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 node dist/reconciler.js
```

---

## Demo Commands

### Temporal

```bash
cd temporal-version

# Start a subscription (30s trial)
npx ts-node src/cli.ts start customer-123

# Cancel during trial
npx ts-node src/cli.ts cancel customer-123 "switched-plans"

# Check status
npx ts-node src/cli.ts status customer-123

# View full workflow history
npx ts-node src/cli.ts history customer-123

# Temporal UI (visual history + event timeline)
open http://localhost:8080
```

### DIY

```bash
cd diy-version

# Start a subscription
npx ts-node src/cli.ts start customer-456

# Cancel during trial
npx ts-node src/cli.ts cancel customer-456 "switched-plans"

# Check workflow state
npx ts-node src/cli.ts status customer-456

# View events + activity attempts
npx ts-node src/cli.ts history customer-456

# Inspect the database directly
psql postgresql://postgres:postgres@localhost:5432/diy_workflows

# Useful queries:
# SELECT customer_id, state, trial_end_at FROM subscription_workflows;
# SELECT event_type, event_data FROM workflow_events ORDER BY id;
# SELECT activity_type, attempt_number, status FROM activity_attempts;
# SELECT key FROM idempotency_keys;
# SELECT * FROM workflow_locks;
# SELECT * FROM dead_letter_tasks;
```

---

## Demo: Crash Recovery

### Temporal — Worker Crash

```bash
# 1. Start a workflow
npx ts-node temporal-version/src/cli.ts start crash-test

# 2. Kill the Temporal worker (Ctrl+C or docker kill dw-temporal-worker)

# 3. Workflow stays RUNNING — timer is server-side, not in worker memory
npx ts-node temporal-version/src/cli.ts status crash-test

# 4. Restart the worker
TEMPORAL_ADDRESS=localhost:7233 node temporal-version/dist/worker.js

# 5. Workflow resumes from exactly where it left off.
#    Watch the logs — sendWelcomeEmail does NOT run again.
```

### DIY — Worker Crash

```bash
# 1. Start a workflow
npx ts-node diy-version/src/cli.ts start crash-diy

# 2. Kill the DIY worker (Ctrl+C)

# 3. Check — workflow is stuck, lock still held
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT state FROM subscription_workflows WHERE customer_id='crash-diy';"
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT locked_by, expires_at FROM workflow_locks;"

# 4. Wait ~40s — reconciler releases lock, detects stuck state, re-enqueues
# [reconciler] 🔓 Released 1 expired lock(s)
# [reconciler] 🔧 Found 1 stuck workflow(s)
# [reconciler] ↺ Re-enqueued task | task=CHARGE_MONTHLY_FEE

# 5. Restart worker
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 node diy-version/dist/worker.js

# 6. Workflow completes. Idempotency key prevents any duplicate charges.
```

## Demo: Retry (Charge Fails Twice)

```bash
# Temporal
SIMULATE_CHARGE_FAILURE=true TEMPORAL_ADDRESS=localhost:7233 \
  node temporal-version/dist/worker.js

npx ts-node temporal-version/src/cli.ts start retry-demo
# → Watch retries in Temporal UI: ActivityTaskFailed x2, ActivityTaskCompleted x1

# DIY
SIMULATE_CHARGE_FAILURE=true \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
REDIS_URL=redis://localhost:6379 node diy-version/dist/worker.js

npx ts-node diy-version/src/cli.ts start retry-demo-diy
# → After ~35s, check:
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT activity_type, attempt_number, status FROM activity_attempts;"
# CHARGE_MONTHLY_FEE  1  FAILED
# CHARGE_MONTHLY_FEE  2  FAILED
# CHARGE_MONTHLY_FEE  3  COMPLETED
```

---

## Failure Scenarios

| # | Scenario | Temporal | DIY |
|---|----------|----------|-----|
| 1 | Worker crash after email | History records completion — email not re-sent | Reconciler re-enqueues; idempotency key prevents duplicate |
| 2 | Worker crash during trial wait | Timer is server-side — survives all crashes | `trial_end_at` in DB; scheduler still fires |
| 3 | Charge fails 2x then succeeds | Auto-retry via `proxyActivities` config | Exponential backoff via Redis sorted set |
| 4 | Duplicate task delivery | Server deduplicates workflow tasks | `idempotency_keys` table checked before every activity |
| 5 | Cancel during trial | Signal queued server-side; delivered on next activation | HTTP POST → DB write → task enqueued immediately |
| 6 | DB updated, queue publish fails | **Impossible** — queue IS the state store | Reconciler detects state ≠ queue; re-enqueues |
| 7 | Stuck workflow (lock expired) | Built-in heartbeat + reassignment | Reconciler: `DELETE workflow_locks WHERE expires_at < NOW()` |

Detailed analysis in [`docs/failure-scenarios.md`](docs/failure-scenarios.md).

---

## Repository Structure

```
durable-workflow-learning/
│
├── temporal-version/
│   └── src/
│       ├── workflows/
│       │   └── subscriptionWorkflow.ts   ← ~100 lines of durable workflow code
│       ├── activities/
│       │   └── subscriptionActivities.ts
│       ├── worker.ts                     ← polls Temporal, runs sandbox + activities
│       ├── api.ts                        ← HTTP API
│       └── cli.ts                        ← CLI: start / cancel / status / history
│
├── diy-version/
│   └── src/
│       ├── db/
│       │   └── schema.sql                ← 6 tables with explanatory comments
│       ├── orchestrator/
│       │   └── stateMachine.ts           ← explicit state machine (15 states)
│       ├── queue/
│       │   └── redisQueue.ts             ← READY list + DELAYED sorted set
│       ├── activities/
│       │   └── subscriptionActivities.ts ← same activities, called directly
│       ├── worker.ts      ← lock → idempotency → execute → commit → enqueue next
│       ├── scheduler.ts   ← polls trial_end_at (replaces workflow.sleep)
│       ├── reconciler.ts  ← crash recovery (replaces Temporal's heartbeat system)
│       ├── api.ts
│       └── cli.ts
│
├── docs/
│   ├── architecture.md         ← deep-dive with sequence diagrams
│   ├── comparison.md           ← side-by-side concept mapping
│   └── failure-scenarios.md    ← 7 scenarios, both systems analysed
│
└── scripts/
    ├── demo-temporal.sh
    ├── demo-diy.sh
    └── failure-scenarios/      ← runnable scripts for each scenario
```

---

## The Core Lesson

```
workflow.sleep(30_000)         ← 1 line in Temporal
```

In DIY, that 1 line requires:

```typescript
// In worker.ts — set the timer
await client.query(`
  UPDATE subscription_workflows
  SET state = 'WAITING_FOR_TRIAL_END',
      trial_end_at = NOW() + INTERVAL '30 seconds'
  WHERE id = $1
`);

// In scheduler.ts — poll for expiry (separate process)
const expired = await query(`
  SELECT * FROM subscription_workflows
  WHERE state = 'WAITING_FOR_TRIAL_END'
    AND trial_end_at <= NOW()
`);
for (const wf of expired.rows) {
  await enqueueImmediate({ taskType: 'CHARGE_MONTHLY_FEE', ... });
}

// In reconciler.ts — if scheduler was down when timer fired
if (workflow.state === 'WAITING_FOR_TRIAL_END' && workflow.trial_end_at < now) {
  await enqueueImmediate({ taskType: 'CHARGE_MONTHLY_FEE', ... });
}
```

Multiply this pattern across timers, retries, idempotency, crash recovery, and distributed locks — and you have a rough idea of what Temporal replaces.

---

## Tech Stack

- **Language:** TypeScript (single language across both versions)
- **Temporal SDK:** `@temporalio/worker`, `@temporalio/client`, `@temporalio/workflow` v1.9
- **Database:** PostgreSQL 15
- **Queue:** Redis 7
- **Runtime:** Node.js 20+
- **Infrastructure:** Docker Compose

---

## License

MIT
