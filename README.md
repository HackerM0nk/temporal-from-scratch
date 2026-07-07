# temporal-from-scratch

Two implementations of the same subscription billing workflow:

| | `temporal-version/` | `diy-version/` |
|---|---|---|
| Code | ~100 lines | ~1,500 lines |
| Timers | `workflow.sleep(30_000)` | `trial_end_at` column + `scheduler.ts` process |
| Crash recovery | automatic | `reconciler.ts` process |
| Retry | retry policy config | `scheduled_tasks` table + backoff math |
| Idempotency | history replay | `idempotency_keys` table checked before every activity |
| Cancellation | `defineSignal` | HTTP POST → DB write → MQ publish |
| Message queue | Temporal server | RabbitMQ (AMQP, message acknowledgement) |
| Workflow state | implicit in execution history | explicit `state` VARCHAR column in PostgreSQL |

The DIY version implements every guarantee the Temporal version inherits for free. It's not a toy — it uses the same primitives (PostgreSQL, RabbitMQ, distributed locks, idempotency keys) that real production teams reach for. It just takes 15× more code.

---

## What Temporal is actually doing

At the surface, a Temporal workflow is ordinary-looking sequential code:

```typescript
export async function subscriptionWorkflow(customerId: string) {
  await sendWelcomeEmail({ customerId, workflowId });

  const cancelled = await condition(() => isCancelled, 30_000); // ← durable timer

  if (cancelled) {
    await processSubscriptionCancellation({ customerId, workflowId, reason });
    await sendSorryToSeeYouGoEmail({ customerId, workflowId });
    return;
  }

  const charge = await chargeMonthlyFee({ customerId, workflowId });
  await sendEndOfTrialEmail({ customerId, workflowId, chargeId: charge.chargeId });
  await sendMonthlyChargeEmail({ customerId, workflowId, amount: charge.amount });
}
```

Under the hood, every `await` writes an event to an append-only history log. When a worker crashes and restarts, Temporal **replays** that history through the workflow code — each completed `await` returns instantly with its recorded result, no re-execution. The code picks up from the exact line it left off.

```mermaid
flowchart TD
    subgraph temporal["Inside Temporal Server"]
        direction TB
        H["Workflow History (append-only log)"]
        TQ["Workflow Task Queue"]
        AQ["Activity Task Queue"]
        TIMER["Durable Timer Store"]
    end

    subgraph worker["Your Worker Process"]
        direction TB
        WS["Workflow Sandbox\n(deterministic replay)"]
        AE["Activity Executor\n(real Node.js, real side effects)"]
    end

    WS -->|"await sleep(30d)\n→ StartTimer command"| TIMER
    TIMER -->|"timer fires\n→ TimerFired event"| H
    H -->|"WorkflowTask\n(replay + advance)"| WS

    WS -->|"await sendWelcomeEmail()\n→ ScheduleActivity command"| AQ
    AQ -->|"ActivityTask"| AE
    AE -->|"RespondActivityTaskCompleted"| H

    style temporal fill:#1a1a2e,color:#e0e0ff,stroke:#7c3aed
    style worker fill:#0f3460,color:#e0e0ff,stroke:#3b82f6
    style H fill:#312e81,color:#c7d2fe,stroke:#6366f1
    style TIMER fill:#312e81,color:#c7d2fe,stroke:#6366f1
```

**If the worker crashes:** The timer still lives in Temporal's server. The activity result still lives in history. When the worker restarts, it polls for the next task, replays history in milliseconds, and continues. The workflow code sees nothing unusual — it never knew the crash happened.

**If an activity fails:** Temporal reschedules it after the configured backoff. The workflow code never sees transient failures — it just `await`s the eventual success.

---

## What you build without it

The DIY version implements the same guarantees by hand. Four separate processes, six PostgreSQL tables, one RabbitMQ queue.

```mermaid
flowchart TD
    subgraph entry["Start / Cancel (API Server)"]
        API["POST /workflows/subscription"]
    end

    API -->|"INSERT\nstate = WELCOME_EMAIL_SCHEDULED"| PG

    subgraph pg["PostgreSQL — 6 tables"]
        direction TB
        T1[("subscription_workflows\nstate + trial_end_at")]
        T2[("workflow_events\naudit log")]
        T3[("activity_attempts\nretry history")]
        T4[("idempotency_keys\ndeduplication")]
        T5[("workflow_locks\ndistributed mutex")]
        T6[("scheduled_tasks\ndelayed retries")]
    end

    API -->|"PUBLISH\nSEND_WELCOME_EMAIL"| MQ

    subgraph mq["RabbitMQ"]
        Q["diy.tasks\ndurable queue"]
    end

    subgraph worker["Worker Process — every task"]
        direction TB
        W1["consume task\n(stays unacked until done)"]
        W2{"acquire\nworkflow_lock"}
        W3{"idempotency_keys\nalready done?"}
        W4["execute activity\n(email, charge, etc.)"]
        W5["ATOMIC:\nINSERT idempotency_keys\nUPDATE state\nINSERT workflow_events"]
        W6["PUBLISH next task"]
        W7["ack message"]
        W1 --> W2
        W2 -->|"locked by\nanother worker"| WX1["republish,\nreturn"]
        W2 -->|"acquired"| W3
        W3 -->|"yes"| WX2["skip,\nadvance state"]
        W3 -->|"no"| W4
        W4 -->|"throws"| W8["INSERT scheduled_tasks\nwith backoff delay"]
        W4 -->|"succeeds"| W5
        W5 --> W6
        W6 -->|"RabbitMQ down\nor crash here"| STUCK["workflow stuck\nreconciler repairs"]
        W6 --> W7
    end

    subgraph scheduler["Scheduler — polls every 2s"]
        direction TB
        SC1["SELECT trial_end_at ≤ NOW()\n→ PUBLISH CHARGE_MONTHLY_FEE"]
        SC2["SELECT scheduled_tasks\nexecute_after ≤ NOW()\n→ PUBLISH retry task"]
    end

    subgraph reconciler["Reconciler — polls every 10s"]
        direction TB
        RC1["DELETE workflow_locks\nWHERE expires_at < NOW()"]
        RC2["SELECT stuck workflows\nupdated_at > 60s AND no lock\n→ INSERT scheduled_tasks"]
    end

    MQ --> worker
    worker --> pg
    scheduler --> pg
    scheduler --> MQ
    reconciler --> pg
    reconciler --> pg

    style pg fill:#1a1a2e,color:#e0e0ff,stroke:#6366f1
    style mq fill:#0f3460,color:#e0e0ff,stroke:#3b82f6
    style worker fill:#14532d,color:#d1fae5,stroke:#22c55e
    style scheduler fill:#451a03,color:#fef3c7,stroke:#f59e0b
    style reconciler fill:#450a0a,color:#fee2e2,stroke:#ef4444
    style STUCK fill:#7f1d1d,color:#fca5a5,stroke:#dc2626
```

The five failure modes you must handle at every step:

1. **Lock held by another worker** — republish the task, let the other worker finish
2. **Idempotency key exists** — skip re-execution, advance state
3. **Activity throws** — insert into `scheduled_tasks` with exponential backoff; scheduler re-publishes when due
4. **DB committed but MQ publish failed** — workflow is stuck; reconciler re-enqueues via `scheduled_tasks`
5. **Worker crashed before ack** — RabbitMQ redelivers to next consumer automatically; idempotency check on re-processing prevents double execution

This is the same problem Temporal solves — just made visible.

---

## The workflow

A subscription lifecycle with a 30-day free trial (30 seconds in the demo, accelerated for learning):

```
Customer signs up
  ↓
sendWelcomeEmail
  ↓
sleep(30 days) ←── cancelSubscription signal can arrive any time here
  │                         ↓
  │                 processSubscriptionCancellation
  │                 sendSorryToSeeYouGoEmail
  │                         ↓
  │                    [ CANCELLED ]
  ↓ (trial ends)
chargeMonthlyFee            ← idempotency key sent to payment processor
sendEndOfTrialEmail
sendMonthlyChargeEmail
  ↓
[ COMPLETED ]
```

Activities: `sendWelcomeEmail`, `chargeMonthlyFee`, `sendEndOfTrialEmail`, `sendMonthlyChargeEmail`, `processSubscriptionCancellation`, `sendSorryToSeeYouGoEmail`

---

## Running it

**Requires:** Docker Desktop, Node.js 20+

```bash
git clone https://github.com/HackerM0nk/temporal-from-scratch
cd temporal-from-scratch
```

### Infrastructure only (recommended for learning)

```bash
docker compose up postgres rabbitmq temporal temporal-ui
```

Then in separate terminals:

```bash
# build once
(cd temporal-version && npm install && npm run build)
(cd diy-version && npm install && npm run build)

# Temporal worker
TEMPORAL_ADDRESS=localhost:7233 node temporal-version/dist/worker.js

# Temporal API
TEMPORAL_ADDRESS=localhost:7233 PORT=3000 node temporal-version/dist/api.js

# DIY API  (runs schema migrations on first start)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
RUN_MIGRATIONS=true node diy-version/dist/api.js

# DIY Worker  (consumes from RabbitMQ)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/worker.js

# DIY Scheduler  (fires trial timers + promotes retry tasks)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/scheduler.js

# DIY Reconciler  (repairs stuck workflows, PostgreSQL only)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
node diy-version/dist/reconciler.js
```

### Everything in Docker

```bash
docker compose up
```

---

## Demo

### Start a subscription

```bash
# Temporal
npx ts-node temporal-version/src/cli.ts start customer-123

# DIY
npx ts-node diy-version/src/cli.ts start customer-456
```

The trial runs for 30 seconds. Both workflows complete on their own.

### Watch the DIY machinery in real time

```bash
psql postgresql://postgres:postgres@localhost:5432/diy_workflows

-- Current state of all workflows
SELECT customer_id, state, trial_end_at FROM subscription_workflows;

-- Live event stream
SELECT event_type, event_data, created_at FROM workflow_events ORDER BY id DESC LIMIT 20;

-- Retry history
SELECT activity_type, attempt_number, status, error_message
FROM activity_attempts ORDER BY scheduled_at;

-- What's waiting for backoff delays
SELECT task_type, execute_after, published_at FROM scheduled_tasks;

-- Active locks (should clear within 30s of any worker crash)
SELECT workflow_id, locked_by, expires_at FROM workflow_locks;
```

### Temporal UI
```
http://localhost:8080
```

### RabbitMQ Management UI
```
http://localhost:15672  (guest / guest)
```

Watch messages appear and disappear as the worker processes them. During a worker crash, messages stay in the "unacked" state until RabbitMQ redelivers them.

### Cancel during the trial

```bash
npx ts-node temporal-version/src/cli.ts cancel customer-123 "changed-my-mind"
npx ts-node diy-version/src/cli.ts cancel customer-456 "changed-my-mind"
```

### Simulate retry (charge fails twice)

```bash
# start the worker with failure injection
SIMULATE_CHARGE_FAILURE=true \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/worker.js

npx ts-node diy-version/src/cli.ts start retry-demo

# after ~35s, inspect the retry history:
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT activity_type, attempt_number, status FROM activity_attempts ORDER BY scheduled_at;"
# CHARGE_MONTHLY_FEE  1  FAILED
# CHARGE_MONTHLY_FEE  2  FAILED
# CHARGE_MONTHLY_FEE  3  COMPLETED
```

### Crash recovery demo

```bash
npx ts-node diy-version/src/cli.ts start crash-demo

# kill the worker mid-flight
pkill -f "node diy-version/dist/worker"

# the lock is still in workflow_locks
psql postgresql://postgres:postgres@localhost:5432/diy_workflows \
  -c "SELECT locked_by, expires_at FROM workflow_locks;"

# wait ~40s — reconciler releases lock, inserts into scheduled_tasks,
# scheduler publishes to RabbitMQ
# restart the worker — it picks up, passes idempotency check, continues
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diy_workflows \
RABBITMQ_URL=amqp://guest:guest@localhost:5672 \
node diy-version/dist/worker.js
```

---

## Why not Kafka? Why not Airflow?

**Kafka** is a distributed log for high-throughput event streaming. It's the wrong primitive for task queuing: no native delayed messages, consumer offset tracking adds state you'd have to manage, and fan-out semantics are opposite of what you need here (each task consumed once, not broadcast). RabbitMQ's AMQP semantics — durable queues, per-message acknowledgement, consumer prefetch — are the right fit.

**Airflow** is a workflow *scheduler* for batch data pipelines. It schedules DAG runs on a cron schedule. It cannot express "wait indefinitely for this specific customer to cancel, then branch" — DAGs are static structures with no per-instance signal delivery. The category of problem Temporal solves (long-running, event-driven, per-entity workflows at scale) doesn't overlap with what Airflow is designed for.

---

## Stack

| Layer | Temporal version | DIY version |
|---|---|---|
| Language | TypeScript | TypeScript |
| Workflow engine | Temporal Server | hand-rolled (4 processes) |
| State store | Temporal's PostgreSQL | PostgreSQL (6 tables) |
| Task queue | Temporal's internal queue | RabbitMQ |
| Timers | server-side, durable | `trial_end_at` column + scheduler |
| Delayed retries | automatic | `scheduled_tasks` table |
| Management UI | Temporal UI :8080 | RabbitMQ Management :15672 |

---

## Repository structure

```
temporal-version/src/
  workflows/subscriptionWorkflow.ts    the entire durable workflow — ~100 lines
  activities/subscriptionActivities.ts
  worker.ts
  api.ts / cli.ts

diy-version/src/
  db/schema.sql                        6 tables — start here
  orchestrator/stateMachine.ts         15 explicit states + transition table
  queue/rabbitmq.ts                    AMQP consumer/publisher
  activities/subscriptionActivities.ts same business logic, called directly
  worker.ts       lock → idempotency → execute → atomic commit → publish next
  scheduler.ts    fires trial timers + promotes scheduled retries
  reconciler.ts   repairs workflows when workers crash or publishes fail
  api.ts / cli.ts

docs/
  architecture.md      sequence diagrams for both versions
  comparison.md        concept-by-concept mapping table
  failure-scenarios.md 7 failure modes analysed in both systems
```

---

## License

MIT
