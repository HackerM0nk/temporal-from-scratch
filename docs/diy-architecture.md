# DIY Architecture

## System Overview

```mermaid
graph TD
    CLI[CLI / HTTP API] -->|POST /workflows/subscription| API[API Server :3001]
    API -->|INSERT subscription_workflows| PG[(PostgreSQL)]
    API -->|RPUSH diy:tasks:ready| Redis[(Redis)]

    Worker[Worker Process] -->|BLPOP diy:tasks:ready| Redis
    Worker -->|SELECT, INSERT, UPDATE| PG
    Worker -->|Calls activities| Activity[Activity Functions]
    Activity -->|Simulated: email, charge| External[External Services]

    Scheduler[Scheduler Process] -->|SELECT trial_end_at < NOW| PG
    Scheduler -->|RPUSH diy:tasks:ready| Redis
    Scheduler -->|ZRANGEBYSCORE| Redis

    Reconciler[Reconciler Process] -->|SELECT stuck workflows| PG
    Reconciler -->|DELETE expired locks| PG
    Reconciler -->|RPUSH| Redis
```

## State Machine

```mermaid
stateDiagram-v2
    [*] --> STARTED : API creates workflow

    STARTED --> WELCOME_EMAIL_SCHEDULED : first task enqueued
    WELCOME_EMAIL_SCHEDULED --> WELCOME_EMAIL_SENT : worker executes sendWelcomeEmail

    WELCOME_EMAIL_SENT --> WAITING_FOR_TRIAL_END : trial_end_at set (=NOW+30s)

    WAITING_FOR_TRIAL_END --> CHARGE_SCHEDULED : scheduler detects trial_end_at passed
    CHARGE_SCHEDULED --> CHARGING : worker starts chargeMonthlyFee
    CHARGING --> CHARGED : charge succeeds
    CHARGED --> END_OF_TRIAL_EMAIL_SENT : sendEndOfTrialEmail
    END_OF_TRIAL_EMAIL_SENT --> COMPLETED : sendMonthlyChargeEmail

    WAITING_FOR_TRIAL_END --> CANCELLATION_REQUESTED : cancel API called
    WELCOME_EMAIL_SENT --> CANCELLATION_REQUESTED : cancel API called
    CHARGING --> CANCELLATION_REQUESTED : cancel API called (best-effort)

    CANCELLATION_REQUESTED --> CANCELLING : worker starts processSubscriptionCancellation
    CANCELLING --> CANCELLED : sendSorryToSeeYouGoEmail complete

    CHARGING --> FAILED : max retries exhausted
    CANCELLING --> FAILED : max retries exhausted
```

## Full Request Flow

```mermaid
sequenceDiagram
    participant CLI
    participant API
    participant PG as PostgreSQL
    participant Redis
    participant Worker
    participant Scheduler

    CLI->>API: POST /workflows/subscription { customerId: "customer-456" }

    API->>PG: BEGIN
    API->>PG: INSERT subscription_workflows (state=WELCOME_EMAIL_SCHEDULED)
    API->>PG: INSERT workflow_events (WORKFLOW_STARTED)
    API->>PG: COMMIT
    API->>Redis: RPUSH diy:tasks:ready { taskType: SEND_WELCOME_EMAIL }
    API-->>CLI: 201 { workflowId, state: WELCOME_EMAIL_SCHEDULED }

    Worker->>Redis: BLPOP diy:tasks:ready
    Redis-->>Worker: { taskType: SEND_WELCOME_EMAIL, workflowId }

    Worker->>PG: INSERT workflow_locks (acquire lock)
    Worker->>PG: SELECT idempotency_keys WHERE key = "wf-id:SEND_WELCOME_EMAIL"
    Note over Worker: Not found → proceed

    Worker->>Worker: sendWelcomeEmail() ← actual side effect happens here

    Worker->>PG: BEGIN
    Worker->>PG: INSERT idempotency_keys
    Worker->>PG: UPDATE subscription_workflows SET state=WELCOME_EMAIL_SENT
    Worker->>PG: UPDATE subscription_workflows SET state=WAITING_FOR_TRIAL_END, trial_end_at=+30s
    Worker->>PG: INSERT activity_attempts (COMPLETED)
    Worker->>PG: INSERT workflow_events (TASK_COMPLETED, TIMER_STARTED)
    Worker->>PG: COMMIT

    Worker->>PG: DELETE workflow_locks (release)
    Note over Worker: No next task enqueued — scheduler handles the timer

    Note over Scheduler: 30 seconds pass...

    Scheduler->>PG: SELECT * WHERE state=WAITING_FOR_TRIAL_END AND trial_end_at <= NOW()
    Scheduler->>PG: UPDATE state=CHARGE_SCHEDULED WHERE state=WAITING_FOR_TRIAL_END
    Scheduler->>Redis: RPUSH { taskType: CHARGE_MONTHLY_FEE }

    Worker->>Redis: BLPOP → { taskType: CHARGE_MONTHLY_FEE }
    Note over Worker: Acquire lock → check idempotency → execute charge → commit → release

    Note over Worker: ... continues through END_OF_TRIAL_EMAIL_SENT → COMPLETED
```

## Failure Recovery Flow

```mermaid
sequenceDiagram
    participant Worker
    participant PG as PostgreSQL
    participant Redis
    participant Reconciler

    Worker->>Redis: BLPOP → { taskType: CHARGE_MONTHLY_FEE }
    Worker->>PG: INSERT workflow_locks
    Worker->>Worker: chargeMonthlyFee() — activity starts

    Note over Worker: 💥 CRASH — lock still in DB, activity incomplete

    Note over Reconciler: 30 seconds later...

    Reconciler->>PG: DELETE FROM workflow_locks WHERE expires_at < NOW()
    Note over Reconciler: Lock released

    Reconciler->>PG: SELECT workflows WHERE state=CHARGING AND updated_at < 60s ago
    Note over Reconciler: Finds the stuck CHARGING workflow

    Reconciler->>Redis: RPUSH { taskType: CHARGE_MONTHLY_FEE, attempt: 1 }
    Note over Reconciler: Re-enqueued — worker will pick up and retry

    Note over Worker: Worker restarts

    Worker->>Redis: BLPOP → { taskType: CHARGE_MONTHLY_FEE, attempt: 1 }
    Worker->>PG: INSERT workflow_locks
    Worker->>PG: SELECT idempotency_keys WHERE key="wf-id:CHARGE_MONTHLY_FEE"
    Note over Worker: Not found → execute activity
    Worker->>Worker: chargeMonthlyFee() ← idempotency key on Stripe prevents double charge
```

## Database Tables and Their Purpose

### `subscription_workflows`
The canonical state of each workflow. Every other table references this.
The `state` column is the "program counter" of the workflow.

### `workflow_events`
Append-only history log. Every state transition, activity result, and signal is recorded here. This is NOT used for replay (unlike Temporal's history) — it's an audit log.

### `activity_attempts`
Tracks each attempt to run each activity. Shows retry history. Lets you answer: "How many times did we try to charge customer-456 and why did each attempt fail?"

### `idempotency_keys`
Prevents duplicate execution. Before running any activity, check this table. After completing, insert a record. The worker also checks this to handle duplicate queue messages.

### `workflow_locks`
Distributed mutex. Prevents two workers from processing the same workflow simultaneously. Has a TTL so crashed workers don't permanently block workflows.

### `dead_letter_tasks`
Activities that exhausted all retries. Requires manual intervention to re-drive or compensate.

## Processes and Their Responsibilities

| Process | Analogy in Temporal | Responsibility |
|---------|---------------------|----------------|
| **API** | Temporal Client | Accept start/cancel requests, create DB records, enqueue first task |
| **Worker** | Worker + Activity Worker | Poll Redis, acquire lock, check idempotency, execute activity, update state |
| **Scheduler** | Temporal Server (timers) | Poll for expired `trial_end_at`, enqueue CHARGE task; promote delayed queue |
| **Reconciler** | Temporal Server (heartbeats) | Release expired locks, detect stuck workflows, re-enqueue lost tasks |
