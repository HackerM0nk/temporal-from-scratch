# Architecture

---

## Temporal version

Temporal achieves durability through event sourcing. Every action — activity scheduled, timer started, signal received — is appended to an immutable history log. Workflow state is always reconstructable from this log.

```
WorkflowExecutionStarted   { input: "customer-123" }
WorkflowTaskScheduled
WorkflowTaskStarted
WorkflowTaskCompleted      { commands: [ScheduleActivity(sendWelcomeEmail)] }
ActivityTaskScheduled      { activityType: "sendWelcomeEmail" }
ActivityTaskStarted
ActivityTaskCompleted      { result: null }
WorkflowTaskCompleted      { commands: [StartTimer(30000ms)] }
TimerStarted               { fireAt: +30s }
── worker crashes here ──
── 30 seconds pass ──
TimerFired
WorkflowTaskScheduled      ← worker restarts, picks this up
```

When the worker gets `WorkflowTaskScheduled` after the crash, it **replays** events 1–9 through the workflow code. `sendWelcomeEmail` already has a completion event so the SDK injects the result without calling the activity. `condition()` sees `TimerFired` and returns immediately. Execution continues at the next `await` — `chargeMonthlyFee`.

### Happy path

```mermaid
sequenceDiagram
    participant CLI
    participant Server as Temporal Server
    participant Worker
    participant Email as Email Service
    participant Stripe

    CLI->>Server: StartWorkflow("customer-123")
    Server->>Worker: WorkflowTask — run subscriptionWorkflow

    Note over Worker: Executes until first await<br/>Issues: ScheduleActivity(sendWelcomeEmail)

    Worker->>Server: RespondWorkflowTaskCompleted
    Server->>Worker: ActivityTask(sendWelcomeEmail, attempt=1)
    Worker->>Email: POST /send {idempotencyKey: "welcome-email:wf-id"}
    Worker->>Server: RespondActivityTaskCompleted

    Server->>Worker: WorkflowTask — replay from history
    Note over Worker: Replay: sendWelcomeEmail done (skipped)<br/>Issues: StartTimer(30000ms)
    Worker->>Server: RespondWorkflowTaskCompleted

    Note over Server: 30 seconds pass — server fires timer

    Server->>Worker: WorkflowTask — TimerFired in history
    Note over Worker: Replay: email done, timer done<br/>Issues: ScheduleActivity(chargeMonthlyFee)
    Worker->>Server: RespondWorkflowTaskCompleted

    Server->>Worker: ActivityTask(chargeMonthlyFee, attempt=1)
    Worker->>Stripe: POST /charge {idempotencyKey: "charge:wf-id:cycle-1"}
    Stripe-->>Worker: 200 OK {chargeId: "ch_xxx"}
    Worker->>Server: RespondActivityTaskCompleted

    Note over Worker,Server: → sendEndOfTrialEmail → sendMonthlyChargeEmail → COMPLETED
```

### Worker crash mid-activity

```mermaid
sequenceDiagram
    participant Server as Temporal Server
    participant W1 as Worker (crashes)
    participant W2 as Worker (restarted)

    Server->>W1: ActivityTask(chargeMonthlyFee, attempt=1)
    W1->>W1: Calling Stripe...
    Note over W1: 💥 crash

    Note over Server: startToCloseTimeout elapses (30s)<br/>No response received — reschedule

    Server->>W2: ActivityTask(chargeMonthlyFee, attempt=2)
    Note over W2: Same idempotency key sent to Stripe<br/>Stripe returns the original charge — no double billing
    W2->>Server: RespondActivityTaskCompleted
```

### Signal delivery while worker is down

```mermaid
sequenceDiagram
    participant CLI
    participant Server as Temporal Server
    participant Worker

    Note over Worker: Worker is down

    CLI->>Server: SignalWorkflow("cancelSubscription", {reason: "..."})
    Server->>Server: Append WorkflowExecutionSignaled to history

    Note over Worker: Worker restarts

    Server->>Worker: WorkflowTask — includes signal event
    Note over Worker: Replay → setHandler fires: isCancelled = true<br/>condition() returns true<br/>Branch to cancellation path
    Worker->>Server: RespondWorkflowTaskCompleted([ScheduleActivity(processSubscriptionCancellation)])
```

---

## DIY version

### Component overview

```mermaid
graph LR
    CLI[CLI / curl] --> API[API :3001]
    API -->|INSERT + RPUSH| PG[(PostgreSQL)]
    API -->|RPUSH| Redis[(Redis)]

    Worker -->|BLPOP| Redis
    Worker -->|lock, idem check, state update| PG
    Worker -->|RPUSH next task| Redis
    Worker -->|ZADD on failure| Redis

    Scheduler -->|SELECT trial_end_at ≤ NOW| PG
    Scheduler -->|UPDATE + RPUSH| PG
    Scheduler -->|ZRANGEBYSCORE → RPUSH| Redis

    Reconciler -->|DELETE expired locks| PG
    Reconciler -->|SELECT stuck workflows| PG
    Reconciler -->|RPUSH| Redis
```

### Happy path

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
    API->>PG: INSERT subscription_workflows (state = WELCOME_EMAIL_SCHEDULED)
    API->>PG: INSERT workflow_events (WORKFLOW_STARTED)
    API->>PG: COMMIT
    API->>Redis: RPUSH diy:tasks:ready {taskType: SEND_WELCOME_EMAIL}

    Worker->>Redis: BLPOP diy:tasks:ready
    Redis-->>Worker: {taskType: SEND_WELCOME_EMAIL, workflowId}
    Worker->>PG: INSERT workflow_locks (acquire, expires_at = +30s)
    Worker->>PG: SELECT idempotency_keys WHERE key = "wf-id:SEND_WELCOME_EMAIL"
    Note over Worker: Not found — execute

    Worker->>Worker: sendWelcomeEmail()

    Worker->>PG: BEGIN
    Worker->>PG: INSERT idempotency_keys
    Worker->>PG: UPDATE state = WAITING_FOR_TRIAL_END, trial_end_at = NOW() + 30s
    Worker->>PG: INSERT activity_attempts (COMPLETED)
    Worker->>PG: INSERT workflow_events (TASK_COMPLETED, TIMER_STARTED)
    Worker->>PG: COMMIT
    Worker->>PG: DELETE workflow_locks

    Note over Scheduler: Polls every 2s

    Scheduler->>PG: SELECT WHERE state = WAITING_FOR_TRIAL_END AND trial_end_at ≤ NOW()
    Scheduler->>PG: UPDATE state = CHARGE_SCHEDULED WHERE state = WAITING_FOR_TRIAL_END
    Scheduler->>PG: INSERT workflow_events (TIMER_FIRED)
    Scheduler->>Redis: RPUSH {taskType: CHARGE_MONTHLY_FEE}

    Worker->>Redis: BLPOP
    Note over Worker: lock → idempotency → charge → commit → RPUSH SEND_END_OF_TRIAL_EMAIL
    Note over Worker: → SEND_MONTHLY_CHARGE_EMAIL → state = COMPLETED
```

### Crash recovery

```mermaid
sequenceDiagram
    participant Worker
    participant PG as PostgreSQL
    participant Redis
    participant Reconciler

    Worker->>Redis: BLPOP → {taskType: CHARGE_MONTHLY_FEE}
    Worker->>PG: INSERT workflow_locks (expires_at = +30s)
    Worker->>Worker: chargeMonthlyFee()
    Note over Worker: 💥 crash — lock remains in DB, state = CHARGING

    Note over Reconciler: 10s later

    Reconciler->>PG: DELETE FROM workflow_locks WHERE expires_at < NOW() RETURNING *
    Reconciler->>PG: INSERT workflow_events (LOCK_EXPIRED)
    Reconciler->>PG: SELECT WHERE state IN (actionable states) AND updated_at < NOW()-60s AND no lock
    Note over PG: Returns the stuck CHARGING workflow
    Reconciler->>Redis: RPUSH {taskType: CHARGE_MONTHLY_FEE, attempt: 1}
    Reconciler->>PG: INSERT workflow_events (RECONCILER_REQUEUED)

    Note over Worker: Worker restarts

    Worker->>Redis: BLPOP → {taskType: CHARGE_MONTHLY_FEE}
    Worker->>PG: INSERT workflow_locks
    Worker->>PG: SELECT idempotency_keys WHERE key = "wf-id:CHARGE_MONTHLY_FEE"
    Note over Worker: Not found — charge never completed<br/>Execute with same idempotency key sent to Stripe
    Worker->>PG: BEGIN
    Worker->>PG: INSERT idempotency_keys
    Worker->>PG: UPDATE state = CHARGED
    Worker->>PG: COMMIT
```

### Retry with exponential backoff

```mermaid
sequenceDiagram
    participant Worker
    participant Redis
    participant Scheduler
    participant PG as PostgreSQL

    Worker->>Redis: BLPOP → {CHARGE_MONTHLY_FEE, attempt: 1}
    Worker->>Worker: chargeMonthlyFee(attempt=1) → Error
    Worker->>PG: INSERT activity_attempts (FAILED)
    Worker->>Redis: ZADD diy:tasks:delayed score=now+2000 {attempt: 2}

    Scheduler->>Redis: ZRANGEBYSCORE 0 now — finds task
    Scheduler->>Redis: ZREM + RPUSH

    Worker->>Redis: BLPOP → {attempt: 2}
    Worker->>Worker: chargeMonthlyFee(attempt=2) → Error
    Worker->>Redis: ZADD score=now+4000 {attempt: 3}

    Scheduler->>Redis: ZRANGEBYSCORE — promotes task

    Worker->>Redis: BLPOP → {attempt: 3}
    Worker->>Worker: chargeMonthlyFee(attempt=3) → OK
    Worker->>PG: INSERT idempotency_keys + UPDATE state = CHARGED
    Worker->>Redis: RPUSH {SEND_END_OF_TRIAL_EMAIL}
```

---

## Database schema

```sql
-- Canonical workflow state. `state` is the program counter.
subscription_workflows (
  id               UUID PRIMARY KEY,
  customer_id      VARCHAR UNIQUE,
  state            VARCHAR,       -- the only source of truth for "where are we"
  trial_end_at     TIMESTAMPTZ,   -- set after welcome email; polled by scheduler
  metadata         JSONB,         -- { chargeId, amount } propagated through tasks
  cancellation_reason TEXT,
  created_at       TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ
)

-- Append-only. Never updated, never deleted.
-- Unlike Temporal history, this is not used for replay — it's an audit log.
workflow_events (
  id          BIGSERIAL PRIMARY KEY,
  workflow_id UUID,
  event_type  VARCHAR,   -- WORKFLOW_STARTED | TASK_COMPLETED | TIMER_FIRED | RECONCILER_REQUEUED | …
  event_data  JSONB,
  created_at  TIMESTAMPTZ
)

-- One row per execution attempt. Shows retry history.
activity_attempts (
  id             UUID PRIMARY KEY,
  workflow_id    UUID,
  activity_type  VARCHAR,
  attempt_number INT,
  status         VARCHAR,   -- PENDING | RUNNING | COMPLETED | FAILED
  started_at     TIMESTAMPTZ,
  completed_at   TIMESTAMPTZ,
  error_message  TEXT,
  result         JSONB
)

-- Deduplication. Checked before every activity. Written atomically with state update.
-- Key format: "{workflowId}:{activityType}"
idempotency_keys (
  key           VARCHAR PRIMARY KEY,
  workflow_id   UUID,
  activity_type VARCHAR,
  result        JSONB,
  created_at    TIMESTAMPTZ
)

-- Distributed mutex. One row per active workflow, with a TTL.
-- Reconciler deletes rows where expires_at < NOW().
workflow_locks (
  workflow_id UUID PRIMARY KEY,
  locked_by   VARCHAR,     -- worker process identifier
  locked_at   TIMESTAMPTZ,
  expires_at  TIMESTAMPTZ
)

-- Tasks that exhausted all retry attempts.
dead_letter_tasks (
  id            UUID PRIMARY KEY,
  workflow_id   UUID,
  activity_type VARCHAR,
  payload       JSONB,
  error_message TEXT,
  retry_count   INT,
  failed_at     TIMESTAMPTZ
)
```

---

## Concept mapping

| Temporal | DIY |
|---|---|
| Workflow execution | row in `subscription_workflows` |
| Workflow history (for replay) | not applicable — DIY replays from `state` column, not events |
| `workflow_events` table | audit log only; not authoritative |
| `workflow.sleep(ms)` | `trial_end_at` column + `scheduler.ts` polling `SELECT WHERE trial_end_at ≤ NOW()` |
| Activity task on task queue | JSON object pushed to `diy:tasks:ready` Redis LIST |
| Retry policy | `ZADD diy:tasks:delayed` with score = `now + 2^attempt * 1000` |
| `defineSignal` + `setHandler` | HTTP POST → `UPDATE state = CANCELLATION_REQUESTED` + RPUSH |
| Server heartbeat / task reassignment | `reconciler.ts` — deletes expired locks, re-enqueues stuck tasks |
| Sticky queue (single-worker serialisation) | `workflow_locks` with TTL |
| `WorkflowExecutionFailed` status | row in `dead_letter_tasks` + `state = FAILED` |
