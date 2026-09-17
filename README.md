# temporal-from-scratch

Two implementations of the same subscription billing workflow:

| | `temporal-version/` | `diy-version/` |
|---|---|---|
| Code | ~100 lines | ~1,500 lines |
| Timers | `workflow.sleep(30_000)` | `trial_end_at` column + `scheduler.ts` |
| Crash recovery | automatic | `reconciler.ts` process |
| Retry | retry policy config | `scheduled_tasks` table + backoff math |
| Replay deduplication | completed Activity result reused from history | `idempotency_keys` table, checked before every task |
| External side-effect safety | idempotency key or reconciliation still required | idempotency key or reconciliation still required |
| Cancellation | `defineSignal` | HTTP POST → DB write → MQ publish |
| Message queue | Temporal server | RabbitMQ (AMQP, message acknowledgement) |
| Workflow state | implicit in execution history | explicit `state` VARCHAR column in PostgreSQL |

The DIY version approximates the guarantees used by this example with PostgreSQL, RabbitMQ, distributed locks, idempotency records, a Scheduler, and a Reconciler. It is intentionally educational, not a claim that these few files reproduce every consistency, replication, visibility, and operational guarantee of Temporal Server.

---

## How Temporal works

Your workflow looks like ordinary sequential TypeScript. Temporal SDK operations such as scheduling an Activity, starting a Timer, or receiving a Signal add Events to an append-only history. Ordinary JavaScript `await`s do not automatically create Events. If a Worker crashes, Temporal replays history to reconstruct logical state. Completed Activity results are reused, durable Timers still fire, and recorded Signals remain available.

See [the architecture walkthrough](docs/architecture.md) for the replay and task-delivery sequence.

The phrase "resumes where it stopped" is logical, not literal. Temporal does not restore a JavaScript stack or RAM snapshot. A Worker re-executes deterministic Workflow code against recorded History until it reconstructs the same state. Completed Activity results are injected from History instead of re-running because of replay.

---

## From durable workflow to stateful multi-agent harness

The subscription example teaches the mechanism. The included agentic example applies it to one long-lived pull-request review:

```text
GitHub event
  -> SignalWithStart one PR Workflow
  -> durable quiet-window debounce
  -> immutable repository snapshot
  -> specialist planning
  -> parallel Flue specialist submissions
  -> judge and stable finding IDs
  -> final head-SHA freshness check
  -> idempotent GitHub reconciliation
  -> wait for the next commit
```

Temporal owns the process lifecycle: generations, Signals, Timers, fan-out, cancellation, retries, stale-publish prevention, and Continue-As-New. Flue owns one bounded agent submission: model turns, tools, skills, sandboxes, persistent agent state, and fresh-context subagents.

Read [Building Stateful Multi-Agent Systems with Temporal and Flue](docs/flue-temporal-agentic-harness.md) for the end-to-end design and failure model.

Run the simulated agentic workflow after starting the Temporal Worker:

```bash
# start one Workflow for the lifetime of PR 412
npm --prefix temporal-version run agentic -- start acme/payments 412 commit-a base-a

# a new commit restarts the durable quiet window and supersedes stale work
npm --prefix temporal-version run agentic -- commit acme/payments 412 commit-b base-a

# inspect deterministic Workflow state
npm --prefix temporal-version run agentic -- status acme/payments 412

# close the lifecycle
npm --prefix temporal-version run agentic -- close acme/payments 412
```

The demo Activities simulate Flue admission and settlement. The production boundary is deliberately split into two Activities:

```text
dispatchFlueSpecialist -> persist durable receipt in Temporal History
readFlueSpecialist(receipt) -> reattach until the admitted submission settles
```

This prevents a retry of the read side from sending a second model prompt.

---

## What you build without it

The DIY version makes the hidden machinery explicit: four processes, six PostgreSQL tables, one RabbitMQ queue, and five failure modes to handle at every step.

See [the DIY architecture](docs/diy-architecture.md) for the state machine, queue, scheduler, locks, and repair loop.

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

> **Simulation boundary:** the example Activities log stable idempotency keys but do not include a real email or payment provider with a durable deduplication ledger. The code therefore teaches where the key must cross the boundary; it does not by itself prove that an external effect happened once. A production integration must have the provider enforce the key or must reconcile desired state after retry.

| Scenario | Temporal | DIY |
|---|---|---|
| Worker crashes after completion is recorded | Replay reuses the result; the Activity is not scheduled again. | RabbitMQ may redeliver; the idempotency record skips completed work. |
| Worker crashes after an external send but before completion is recorded | Activity may execute again; the email provider must deduplicate a stable key. | Task may execute again; the email provider must deduplicate a stable key. |
| Worker crashes mid-sleep | Timer is server-side. Crash has no effect — it fires regardless. | `trial_end_at` is in PostgreSQL. Scheduler is a separate process, keeps running. |
| Charge fails, retry with backoff | Automatic — configured in `proxyActivities`. | Worker INSERTs into `scheduled_tasks` with `execute_after = now + 2^n s`. Scheduler promotes when due. |
| Same logical request arrives twice | Workflow IDs, request IDs, and application event IDs cover different duplicate classes; there is no blanket deduplication guarantee. | Stable task IDs plus `idempotency_keys`; external effects still need their own keys. |
| Cancel during trial wait | Signal written to history, delivered on next workflow task. | HTTP POST updates `state = CANCELLATION_REQUESTED`, enqueues cancel task immediately. |
| Orchestrator state advances but next task is not durably scheduled | Temporal commits its internal state transition and Commands through the service protocol; application code does not perform a separate DB-write-plus-broker-publish pair. | Reconciler: finds `state = CHARGED`, no pending task → INSERTs into `scheduled_tasks`. |
| Worker crashes during work | Workflow Tasks recover through Task timeout/replay; Activities recover through Start-to-Close or Heartbeat timeout. | RabbitMQ redelivers unacknowledged work; expired locks and the reconciler repair drift. |

---

## Why not Kafka? Why not Airflow?

**Kafka** is a distributed log optimized for ordered event streams and replayable consumer offsets. It can back work-processing systems, but delayed retries, per-item acknowledgement, and competing-consumer task semantics require additional design. This demo uses RabbitMQ because durable queues, manual acknowledgement, and consumer prefetch make the DIY task-delivery mechanism easier to see. Neither broker makes external side effects exactly once.

**Airflow** is primarily a batch data-pipeline orchestrator. Sensors and deferrable operators can wait efficiently, but Airflow's DAG-run/task-instance model is different from a message-driven, per-entity application process that receives Signals and Queries over months. Evaluate the semantics you need instead of treating either product as a drop-in replacement. Closer managed-orchestration comparisons include [Netflix Conductor](https://github.com/Netflix/conductor), [AWS Step Functions](https://aws.amazon.com/step-functions/), and [Azure Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview).

---

## Repository structure

```
temporal-version/src/
  workflows/subscriptionWorkflow.ts    the whole durable workflow — ~100 lines
  workflows/agenticReviewWorkflow.ts   long-lived PR lifecycle + specialist fan-out
  activities/subscriptionActivities.ts
  activities/agenticReviewActivities.ts Flue dispatch/read boundary + publish guard
  worker.ts / api.ts / cli.ts / agenticCli.ts

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
  flue-temporal-agentic-harness.md end-to-end multi-agent architecture
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
