# temporal-from-scratch

Two implementations of the same subscription billing workflow:

| | `temporal-version/` | `diy-version/` |
|---|---|---|
| Code | ~100 lines | ~1,500 lines |
| Timers | `workflow.sleep(30_000)` | `trial_end_at` column + `scheduler.ts` |
| Crash recovery | automatic | `reconciler.ts` process |
| Retry | retry policy config | `scheduled_tasks` table + backoff math |
| Idempotency | history replay | `idempotency_keys` table, checked before every activity |
| Cancellation | `defineSignal` | HTTP POST → DB write → MQ publish |
| Message queue | Temporal server | RabbitMQ (AMQP, message acknowledgement) |
| Workflow state | implicit in execution history | explicit `state` VARCHAR column in PostgreSQL |

The DIY version implements every guarantee the Temporal version inherits for free. It uses the same primitives — PostgreSQL, RabbitMQ, distributed locks, idempotency keys — that real production systems reach for. It just takes 15× more code.

---

## How Temporal works

Your workflow looks like ordinary sequential TypeScript. Under the hood, every `await` writes an event to an append-only history log. If the worker crashes, Temporal replays that history to restore execution state — completed steps are skipped, timers still fire, signals still deliver.

![Temporal Architecture](docs/diagrams/temporal-architecture.svg)

---

## What you build without it

The DIY version makes the hidden machinery explicit: four processes, six PostgreSQL tables, one RabbitMQ queue, and five failure modes to handle at every step.

![DIY Architecture](docs/diagrams/diy-architecture.svg)

---

## The workflow

A subscription lifecycle with a 30-day free trial (30 seconds in the demo):

```
Customer signs up
  ↓
sendWelcomeEmail
  ↓
sleep(30 days) ←─── cancelSubscription signal can arrive here
  │                              ↓
  │                  processSubscriptionCancellation
  │                  sendSorryToSeeYouGoEmail → CANCELLED
  ↓ (trial ends)
chargeMonthlyFee              ← idempotency key sent to payment processor
sendEndOfTrialEmail
sendMonthlyChargeEmail → COMPLETED
```

---

## Running it

**Requires:** Docker Desktop, Node.js 20+

```bash
git clone https://github.com/HackerM0nk/temporal-from-scratch
cd temporal-from-scratch
```

### Infrastructure in Docker, workers local (recommended)

```bash
docker compose up postgres rabbitmq temporal temporal-ui

# build once
(cd temporal-version && npm install && npm run build)
(cd diy-version && npm install && npm run build)
```

Open five terminals:

```bash
# Temporal worker
TEMPORAL_ADDRESS=localhost:7233 node temporal-version/dist/worker.js

# Temporal API
TEMPORAL_ADDRESS=localhost:7233 PORT=3000 node temporal-version/dist/api.js

# DIY API — runs schema migrations on first start
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
RUN_MIGRATIONS=true node diy-version/dist/api.js

# DIY Worker
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/worker.js

# DIY Scheduler + Reconciler (separate terminals)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/scheduler.js

DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
node diy-version/dist/reconciler.js
```

### Everything in Docker

```bash
docker compose up
```

---

## Demo commands

### Start a subscription

```bash
# Temporal (30s trial)
npx ts-node temporal-version/src/cli.ts start customer-123

# DIY (30s trial)
npx ts-node diy-version/src/cli.ts start customer-456
```

### Cancel during the trial

```bash
npx ts-node temporal-version/src/cli.ts cancel customer-123 "changed-my-mind"
npx ts-node diy-version/src/cli.ts cancel customer-456 "changed-my-mind"
```

### Watch the DIY machinery in real time

```sql
-- connect: psql postgresql://postgres:postgres@localhost:5432/diy_workflows

SELECT customer_id, state, trial_end_at FROM subscription_workflows;

SELECT event_type, event_data FROM workflow_events ORDER BY id DESC LIMIT 20;

SELECT activity_type, attempt_number, status, error_message
FROM activity_attempts ORDER BY scheduled_at;

-- retry tasks waiting for backoff delay
SELECT task_type, execute_after, published_at FROM scheduled_tasks;

-- active locks — should clear within 30s of any worker crash
SELECT workflow_id, locked_by, expires_at FROM workflow_locks;
```

**Temporal UI:** http://localhost:8080

**RabbitMQ Management UI:** http://localhost:15672 *(guest / guest)* — watch messages appear in the queue, go unacked while the worker processes them, disappear on ack.

---

## Crash recovery

### Temporal

```bash
npx ts-node temporal-version/src/cli.ts start crash-test

# kill the worker
pkill -f "node temporal-version/dist/worker"

# workflow stays RUNNING — the timer lives in Temporal's server, not the worker process
npx ts-node temporal-version/src/cli.ts status crash-test

# restart — resumes from exactly where it stopped, sendWelcomeEmail does not re-run
TEMPORAL_ADDRESS=localhost:7233 node temporal-version/dist/worker.js
```

### DIY

```bash
npx ts-node diy-version/src/cli.ts start crash-diy

# kill the worker
pkill -f "node diy-version/dist/worker"

# lock is still held in workflow_locks
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT locked_by, expires_at FROM workflow_locks;"

# wait ~40s — reconciler releases lock, inserts into scheduled_tasks,
# scheduler publishes to RabbitMQ
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/worker.js
```

### Retry (charge fails twice)

```bash
SIMULATE_CHARGE_FAILURE=true \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/worker.js

npx ts-node diy-version/src/cli.ts start retry-demo

# after ~35s:
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT activity_type, attempt_number, status FROM activity_attempts ORDER BY scheduled_at;"
# CHARGE_MONTHLY_FEE  1  FAILED
# CHARGE_MONTHLY_FEE  2  FAILED
# CHARGE_MONTHLY_FEE  3  COMPLETED
```

---

## Failure scenarios

| Scenario | Temporal | DIY |
|---|---|---|
| Worker crashes after email sends | History records completion. Restart: replay skips the activity — email not re-sent. | RabbitMQ redelivers (unacked). Idempotency key blocks re-execution. |
| Worker crashes mid-sleep | Timer is server-side. Crash has no effect — it fires regardless. | `trial_end_at` is in PostgreSQL. Scheduler is a separate process, keeps running. |
| Charge fails, retry with backoff | Automatic — configured in `proxyActivities`. | Worker INSERTs into `scheduled_tasks` with `execute_after = now + 2^n s`. Scheduler promotes when due. |
| Same task delivered twice | Temporal deduplicates at the server level. | `idempotency_keys` table — `INSERT ON CONFLICT DO NOTHING` before every execution. |
| Cancel during trial wait | Signal written to history, delivered on next workflow task. | HTTP POST updates `state = CANCELLATION_REQUESTED`, enqueues cancel task immediately. |
| DB committed, MQ publish fails | Not possible — task queue and state are the same system. | Reconciler: finds `state = CHARGED`, no pending task → INSERTs into `scheduled_tasks`. |
| Worker crashes holding lock | Server detects missed heartbeat, reassigns. | Reconciler: `DELETE FROM workflow_locks WHERE expires_at < NOW()`, then re-enqueues. |

---

## Why not Kafka? Why not Airflow?

**Kafka** is a distributed log for high-throughput event streaming. It's the wrong primitive for task queuing: no native delayed messages, consumer offset tracking adds state you'd manage yourself, and fan-out semantics conflict with "each task consumed exactly once." RabbitMQ's AMQP semantics — durable queues, per-message acknowledgement, consumer prefetch — are the right fit.

**Airflow** is a workflow scheduler for batch data pipelines. It schedules DAG runs on a cron schedule. It cannot express "wait indefinitely for this specific customer to cancel, then branch" — DAGs are static structures with no per-instance signal delivery and no support for long-running per-entity workflows at scale. The correct comparison class for Temporal is [Netflix Conductor](https://github.com/Netflix/conductor), [AWS Step Functions](https://aws.amazon.com/step-functions/), and [Azure Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview).

---

## Repository structure

```
temporal-version/src/
  workflows/subscriptionWorkflow.ts    the whole durable workflow — ~100 lines
  activities/subscriptionActivities.ts
  worker.ts / api.ts / cli.ts

diy-version/src/
  db/schema.sql                        6 tables — start here
  orchestrator/stateMachine.ts         15 explicit states + transition table
  queue/rabbitmq.ts                    AMQP consumer/publisher (replaces Redis)
  activities/subscriptionActivities.ts same business logic, called directly
  worker.ts       lock → idempotency → execute → atomic commit → publish next
  scheduler.ts    fires trial timers + promotes scheduled retries
  reconciler.ts   repairs workflows when workers crash or publishes fail
  api.ts / cli.ts

docs/
  diagrams/
    temporal-architecture.svg          Temporal internals diagram
    diy-architecture.svg               DIY system diagram
  architecture.md      sequence diagrams for both versions
  comparison.md        concept mapping table
  failure-scenarios.md 7 failure modes analysed
```

---

## Stack

| | Temporal version | DIY version |
|---|---|---|
| Language | TypeScript | TypeScript |
| Workflow engine | Temporal Server | hand-rolled (4 processes) |
| State store | Temporal's PostgreSQL | PostgreSQL (6 tables) |
| Task queue | Temporal's internal queue | RabbitMQ (AMQP) |
| Timers | server-side, durable | `trial_end_at` column + scheduler |
| Delayed retries | automatic | `scheduled_tasks` table |
| Management UI | Temporal UI :8080 | RabbitMQ Management :15672 |

---

## License

MIT
