# Architecture Deep-Dive

This document explains how both implementations work internally, using sequence diagrams to show the exact flow of data, state, and control.

---

## Temporal Version

### What Makes a Workflow "Durable"

Temporal achieves durability through **event sourcing**. Every significant action in the workflow — activity scheduled, timer started, signal received — is appended as an immutable event to a history log. The workflow's current execution state is always reconstructable from this history.

```
Workflow History (append-only, stored in Temporal's DB)
───────────────────────────────────────────────────────
 1  WorkflowExecutionStarted   { input: "customer-123" }
 2  WorkflowTaskScheduled
 3  WorkflowTaskStarted
 4  WorkflowTaskCompleted      { commands: [ScheduleActivity(sendWelcomeEmail)] }
 5  ActivityTaskScheduled      { activityType: "sendWelcomeEmail" }
 6  ActivityTaskStarted
 7  ActivityTaskCompleted      { result: null }
 8  WorkflowTaskScheduled
 9  WorkflowTaskStarted
10  WorkflowTaskCompleted      { commands: [StartTimer(30000ms)] }
11  TimerStarted               { timerId: "1", fireAt: +30s }
    ...worker crashes here...
    ...30 seconds pass...
12  TimerFired                 { timerId: "1" }
13  WorkflowTaskScheduled
    ...worker restarts and picks this up...
```

When the worker restarts, it receives event 13 and **replays** events 1–12 through the workflow code. The code sees that `sendWelcomeEmail` already completed (event 7) and skips the actual call, injecting the result directly from history. The code sees that the timer already fired (event 12) and returns immediately from `condition()`. Execution resumes at the next `await`.

### Happy Path Sequence

```mermaid
sequenceDiagram
    participant CLI
    participant Server as Temporal Server
    participant History as Workflow History
    participant Worker
    participant Email as Email Service
    participant Stripe as Payment Processor

    CLI->>Server: StartWorkflow("customer-123")
    Server->>History: WorkflowExecutionStarted
    Server->>History: WorkflowTaskScheduled

    Worker->>Server: PollForWorkflowTask
    Server-->>Worker: WorkflowTask (run subscriptionWorkflow)
    Note over Worker: Executes code → hits await sendWelcomeEmail()<br/>Issues command: ScheduleActivity(sendWelcomeEmail)
    Worker->>Server: RespondWorkflowTaskCompleted
    Server->>History: ActivityTaskScheduled(sendWelcomeEmail)

    Worker->>Server: PollForActivityTask
    Server-->>Worker: ActivityTask(sendWelcomeEmail, attempt=1)
    Worker->>Email: POST /send {idempotencyKey:"welcome-email:wf-id"}
    Email-->>Worker: 200 OK
    Worker->>Server: RespondActivityTaskCompleted
    Server->>History: ActivityTaskCompleted

    Note over Server: Next workflow task scheduled automatically
    Worker->>Server: PollForWorkflowTask
    Note over Worker: REPLAY: sendWelcomeEmail already done (from history)<br/>Advances to: condition(isCancelled, 30000)<br/>Issues command: StartTimer(30000ms)
    Worker->>Server: RespondWorkflowTaskCompleted
    Server->>History: TimerStarted

    Note over Server: 30 seconds pass — server fires the timer
    Server->>History: TimerFired

    Worker->>Server: PollForWorkflowTask
    Note over Worker: REPLAY: welcome email done, timer fired<br/>Advances to: chargeMonthlyFee()
    Worker->>Server: RespondWorkflowTaskCompleted

    Worker->>Server: PollForActivityTask
    Server-->>Worker: ActivityTask(chargeMonthlyFee, attempt=1)
    Worker->>Stripe: POST /charge {idempotencyKey:"charge:wf-id:cycle-1"}
    Stripe-->>Worker: 200 OK { chargeId: "ch_xxx" }
    Worker->>Server: RespondActivityTaskCompleted({chargeId:"ch_xxx"})

    Note over Worker,Server: sendEndOfTrialEmail → sendMonthlyChargeEmail → COMPLETED
```

### Worker Crash and Recovery

```mermaid
sequenceDiagram
    participant Server as Temporal Server
    participant W1 as Worker (crashes)
    participant W2 as Worker (restarted)

    W1->>Server: PollForActivityTask
    Server-->>W1: ActivityTask(chargeMonthlyFee, attempt=1)
    W1->>W1: Calling Stripe API...
    Note over W1: 💥 CRASH — process killed

    Note over Server: startToCloseTimeout (30s) elapses<br/>No RespondActivityTaskCompleted received

    Server->>Server: Reschedule activity task (attempt=2)

    W2->>Server: PollForActivityTask
    Server-->>W2: ActivityTask(chargeMonthlyFee, attempt=2)
    Note over W2: Same idempotency key sent to Stripe<br/>Stripe returns original charge — no double billing
    W2->>Server: RespondActivityTaskCompleted
```

### Signal Delivery (Cancellation)

```mermaid
sequenceDiagram
    participant CLI
    participant Server as Temporal Server
    participant History as Workflow History
    participant Worker

    Note over Worker: Worker is DOWN

    CLI->>Server: SignalWorkflow("cancelSubscription", {reason:"..."})
    Server->>History: WorkflowExecutionSignaled
    Note over Server: Signal stored durably — not delivered yet

    Note over Worker: Worker restarts

    Worker->>Server: PollForWorkflowTask
    Server-->>Worker: WorkflowTask (includes signal event in history)
    Note over Worker: Replay sees WorkflowExecutionSignaled<br/>→ setHandler fires: isCancelled = true<br/>→ condition() returns true<br/>→ branches to cancellation path
    Worker->>Server: RespondWorkflowTaskCompleted([ScheduleActivity(processSubscriptionCancellation)])
```

---

## DIY Version

### Component Map

```mermaid
graph TB
    subgraph "Entry Points"
        CLI[CLI / curl]
        API[API Server :3001]
    end

    subgraph "Storage"
        PG[(PostgreSQL<br/>diy_workflows)]
        REDIS[(Redis)]
    end

    subgraph "Worker Processes"
        W[Worker<br/>polls Redis]
        S[Scheduler<br/>polls DB every 2s]
        R[Reconciler<br/>polls DB every 10s]
    end

    subgraph "Redis Queues"
        READY[diy:tasks:ready<br/>LIST — immediate]
        DELAYED[diy:tasks:delayed<br/>ZSET — by timestamp]
    end

    CLI --> API
    API -->|INSERT workflow row| PG
    API -->|RPUSH first task| READY

    W -->|BLPOP| READY
    W -->|acquire lock| PG
    W -->|check idempotency| PG
    W -->|execute activity| W
    W -->|atomic: update state + events + idem key| PG
    W -->|RPUSH next task| READY
    W -->|ZADD on failure| DELAYED

    S -->|SELECT trial_end_at ≤ NOW| PG
    S -->|UPDATE state = CHARGE_SCHEDULED| PG
    S -->|RPUSH charge task| READY
    S -->|ZRANGEBYSCORE → RPUSH| DELAYED

    R -->|DELETE expired locks| PG
    R -->|SELECT stuck workflows| PG
    R -->|RPUSH lost tasks| READY
```

### Happy Path Sequence

```mermaid
sequenceDiagram
    participant CLI
    participant API
    participant PG as PostgreSQL
    participant Redis
    participant Worker
    participant Scheduler

    CLI->>API: POST /workflows/subscription {customerId}

    API->>PG: BEGIN
    API->>PG: INSERT subscription_workflows (state=WELCOME_EMAIL_SCHEDULED)
    API->>PG: INSERT workflow_events (WORKFLOW_STARTED)
    API->>PG: COMMIT
    API->>Redis: RPUSH diy:tasks:ready {taskType:SEND_WELCOME_EMAIL}
    API-->>CLI: 201 {workflowId}

    Worker->>Redis: BLPOP diy:tasks:ready (blocking)
    Redis-->>Worker: {taskType:SEND_WELCOME_EMAIL, workflowId}

    Worker->>PG: INSERT workflow_locks (acquire)
    Worker->>PG: SELECT idempotency_keys WHERE key="{wfId}:SEND_WELCOME_EMAIL"
    Note over Worker: Not found — proceed

    Worker->>Worker: sendWelcomeEmail() ← side effect here

    Worker->>PG: BEGIN
    Worker->>PG: INSERT idempotency_keys (key, result)
    Worker->>PG: UPDATE subscription_workflows SET state=WAITING_FOR_TRIAL_END, trial_end_at=NOW()+30s
    Worker->>PG: INSERT activity_attempts (COMPLETED)
    Worker->>PG: INSERT workflow_events (TASK_COMPLETED, TIMER_STARTED)
    Worker->>PG: COMMIT

    Worker->>PG: DELETE workflow_locks (release)
    Note over Worker: No next task enqueued — scheduler handles the timer

    Note over Scheduler: Polls every 2s...
    Scheduler->>PG: SELECT WHERE state=WAITING_FOR_TRIAL_END AND trial_end_at≤NOW()
    Scheduler->>PG: UPDATE state=CHARGE_SCHEDULED WHERE state=WAITING_FOR_TRIAL_END
    Scheduler->>Redis: RPUSH {taskType:CHARGE_MONTHLY_FEE}

    Worker->>Redis: BLPOP
    Redis-->>Worker: {taskType:CHARGE_MONTHLY_FEE}
    Note over Worker: lock → idempotency → charge → commit → enqueue next
    Worker->>Redis: RPUSH {taskType:SEND_END_OF_TRIAL_EMAIL}

    Note over Worker: ... SEND_END_OF_TRIAL_EMAIL → SEND_MONTHLY_CHARGE_EMAIL → COMPLETED
```

### Crash Recovery (Reconciler)

```mermaid
sequenceDiagram
    participant Worker
    participant PG as PostgreSQL
    participant Redis
    participant Reconciler

    Worker->>Redis: BLPOP → {taskType:CHARGE_MONTHLY_FEE}
    Worker->>PG: INSERT workflow_locks (expires_at = NOW()+30s)
    Worker->>Worker: chargeMonthlyFee() — calling payment API...
    Note over Worker: 💥 CRASH — lock remains, state=CHARGING

    Note over Reconciler: 10s later — reconciler wakes

    Reconciler->>PG: DELETE workflow_locks WHERE expires_at < NOW()
    Note over PG: Lock released
    Reconciler->>PG: INSERT workflow_events (LOCK_EXPIRED)

    Reconciler->>PG: SELECT WHERE state IN (actionable states)<br/>AND updated_at < NOW()-60s AND no lock
    Note over PG: Finds workflow stuck in CHARGING

    Reconciler->>Redis: RPUSH {taskType:CHARGE_MONTHLY_FEE, attempt:1}
    Reconciler->>PG: INSERT workflow_events (RECONCILER_REQUEUED)

    Note over Worker: Worker restarts

    Worker->>Redis: BLPOP → {taskType:CHARGE_MONTHLY_FEE}
    Worker->>PG: INSERT workflow_locks
    Worker->>PG: SELECT idempotency_keys WHERE key="{wfId}:CHARGE_MONTHLY_FEE"
    Note over Worker: Not found (charge never completed)<br/>Execute chargeMonthlyFee — same idempotency key sent to Stripe<br/>Stripe: returns original result or accepts new charge safely
    Worker->>PG: BEGIN
    Worker->>PG: INSERT idempotency_keys
    Worker->>PG: UPDATE state = CHARGED
    Worker->>PG: COMMIT
```

### Retry with Exponential Backoff

```mermaid
sequenceDiagram
    participant Worker
    participant Redis
    participant Scheduler
    participant PG as PostgreSQL

    Worker->>Redis: BLPOP → {taskType:CHARGE_MONTHLY_FEE, attempt:1}
    Worker->>Worker: chargeMonthlyFee(attempt=1) → throws Error

    Worker->>PG: INSERT activity_attempts (status=FAILED)
    Worker->>PG: INSERT workflow_events (TASK_FAILED)
    Worker->>Redis: ZADD diy:tasks:delayed score=NOW()+2000 {attempt:2}
    Note over Redis: Task sits in sorted set for 2 seconds

    Scheduler->>Redis: ZRANGEBYSCORE 0 NOW() → finds task
    Scheduler->>Redis: ZREM + RPUSH (move to ready queue)

    Worker->>Redis: BLPOP → {attempt:2}
    Worker->>Worker: chargeMonthlyFee(attempt=2) → throws Error
    Worker->>Redis: ZADD score=NOW()+4000 {attempt:3}

    Note over Redis: 4 second delay (2^2 * 1000ms)

    Scheduler->>Redis: promotes task to ready
    Worker->>Redis: BLPOP → {attempt:3}
    Worker->>Worker: chargeMonthlyFee(attempt=3) → SUCCESS
    Worker->>PG: BEGIN
    Worker->>PG: INSERT idempotency_keys
    Worker->>PG: UPDATE state = CHARGED
    Worker->>PG: COMMIT
    Worker->>Redis: RPUSH {taskType:SEND_END_OF_TRIAL_EMAIL}
```

---

## Database Schema

```sql
-- Canonical state of each workflow run
-- The `state` column is the "program counter"
subscription_workflows
  id               UUID PRIMARY KEY
  customer_id      VARCHAR UNIQUE
  state            VARCHAR          -- STARTED → ... → COMPLETED | CANCELLED | FAILED
  trial_end_at     TIMESTAMPTZ      -- set after welcome email; polled by scheduler
  metadata         JSONB            -- { chargeId, amount } from activities
  cancellation_reason TEXT
  created_at, updated_at TIMESTAMPTZ

-- Append-only audit log (equivalent to Temporal's workflow history)
workflow_events
  id               BIGSERIAL PRIMARY KEY
  workflow_id      UUID → subscription_workflows
  event_type       VARCHAR  -- WORKFLOW_STARTED, TASK_COMPLETED, TIMER_FIRED, ...
  event_data       JSONB
  created_at       TIMESTAMPTZ

-- Every attempt to run every activity
activity_attempts
  id               UUID PRIMARY KEY
  workflow_id      UUID
  activity_type    VARCHAR   -- SEND_WELCOME_EMAIL, CHARGE_MONTHLY_FEE, ...
  attempt_number   INT
  status           VARCHAR   -- PENDING | RUNNING | COMPLETED | FAILED
  started_at, completed_at TIMESTAMPTZ
  error_message    TEXT
  result           JSONB

-- Deduplication: checked before every activity execution
idempotency_keys
  key              VARCHAR PRIMARY KEY  -- "{workflowId}:{activityType}"
  workflow_id      UUID
  activity_type    VARCHAR
  result           JSONB
  created_at       TIMESTAMPTZ

-- Distributed mutex: prevents two workers processing same workflow
workflow_locks
  workflow_id      UUID PRIMARY KEY
  locked_by        VARCHAR   -- worker process ID
  locked_at        TIMESTAMPTZ
  expires_at       TIMESTAMPTZ   -- TTL; reconciler cleans up expired locks

-- Activities that exhausted all retry attempts
dead_letter_tasks
  id               UUID PRIMARY KEY
  workflow_id      UUID
  activity_type    VARCHAR
  payload          JSONB
  error_message    TEXT
  retry_count      INT
  failed_at        TIMESTAMPTZ
```

---

## Concept Mapping

| Temporal concept | DIY equivalent |
|-----------------|----------------|
| Workflow execution | Row in `subscription_workflows` |
| Workflow history | `workflow_events` table (audit log, not used for replay) |
| Workflow state | `state` VARCHAR column — explicit, queryable |
| `workflow.sleep(ms)` | `trial_end_at` column + Scheduler polling `SELECT WHERE trial_end_at ≤ NOW()` |
| Activity task | Task JSON pushed to Redis `diy:tasks:ready` |
| Activity retry policy | Worker catches exception → `ZADD diy:tasks:delayed` with backoff score |
| Activity idempotency | `idempotency_keys` table — `INSERT ON CONFLICT DO NOTHING` |
| Signal | HTTP POST to cancel API → `UPDATE state=CANCELLATION_REQUESTED` + task enqueue |
| Sticky worker / serialisation | `workflow_locks` table with TTL |
| Server heartbeat timeout | Reconciler: `DELETE FROM workflow_locks WHERE expires_at < NOW()` |
| Workflow task reassignment | Reconciler: detects stale `updated_at` + no lock → re-enqueues |
| Temporal UI | `GET /workflows/subscription/:customerId/history` endpoint |
| Namespace | Not implemented (would be a `tenant_id` column) |
