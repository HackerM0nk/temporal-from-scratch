# Temporal Architecture

## How It Works

```mermaid
sequenceDiagram
    participant CLI
    participant TemporalServer as Temporal Server
    participant History as Workflow History (DB)
    participant Worker
    participant EmailSvc as Email Service
    participant PaymentSvc as Payment Processor

    CLI->>TemporalServer: StartWorkflow(subscriptionWorkflow, "customer-123")
    TemporalServer->>History: Append WorkflowExecutionStarted
    TemporalServer->>History: Append WorkflowTaskScheduled

    Worker->>TemporalServer: PollForWorkflowTask
    TemporalServer-->>Worker: WorkflowTask (run workflow code)

    Note over Worker: Executes workflow code up to first await<br/>Produces "commands": [ScheduleActivity(sendWelcomeEmail)]

    Worker->>TemporalServer: RespondWorkflowTaskCompleted(commands)
    TemporalServer->>History: Append ActivityTaskScheduled(sendWelcomeEmail)

    Worker->>TemporalServer: PollForActivityTask
    TemporalServer-->>Worker: ActivityTask(sendWelcomeEmail)
    Worker->>EmailSvc: POST /send { idempotencyKey: "welcome-email:wf-123" }
    EmailSvc-->>Worker: 200 OK
    Worker->>TemporalServer: RespondActivityTaskCompleted(result)
    TemporalServer->>History: Append ActivityTaskCompleted(sendWelcomeEmail)

    Note over TemporalServer: Schedule next workflow task

    Worker->>TemporalServer: PollForWorkflowTask
    TemporalServer-->>Worker: WorkflowTask (replay from history)
    Note over Worker: REPLAY: step 1 (sendWelcomeEmail) — skips actual call<br/>Advances to: await sleep(30s)<br/>Produces: [StartTimer(30s)]

    Worker->>TemporalServer: RespondWorkflowTaskCompleted([StartTimer])
    TemporalServer->>History: Append TimerStarted(fireAt=+30s)

    Note over TemporalServer: 30 seconds pass...
    TemporalServer->>History: Append TimerFired

    Worker->>TemporalServer: PollForWorkflowTask
    Note over Worker: REPLAY: step 1 (skipped), step 2 timer (fired) ✓<br/>Advances to: chargeMonthlyFee

    Worker->>TemporalServer: RespondWorkflowTaskCompleted([ScheduleActivity(chargeMonthlyFee)])
    Worker->>TemporalServer: PollForActivityTask
    TemporalServer-->>Worker: ActivityTask(chargeMonthlyFee)
    Worker->>PaymentSvc: POST /charge { idempotencyKey: "charge:wf-123:cycle-1", amount: 29.99 }
    PaymentSvc-->>Worker: 200 OK { chargeId: "ch_xxx" }
    Worker->>TemporalServer: RespondActivityTaskCompleted({ chargeId: "ch_xxx" })

    Note over TemporalServer,History: Continues through emails → COMPLETED
```

## Worker Crash Recovery

```mermaid
sequenceDiagram
    participant Worker1 as Worker (crashes)
    participant TemporalServer as Temporal Server
    participant Worker2 as Worker (restarted)

    Worker1->>TemporalServer: PollForActivityTask → chargeMonthlyFee
    Worker1->>Worker1: Calling Stripe...

    Note over Worker1: 💥 CRASH

    Note over TemporalServer: Activity heartbeat timeout (startToCloseTimeout)<br/>After timeout: reschedule activity task

    Worker2->>TemporalServer: PollForActivityTask
    TemporalServer-->>Worker2: ActivityTask(chargeMonthlyFee, attempt=2)
    Note over Worker2: Stripe idempotency key prevents double charge
    Worker2->>TemporalServer: RespondActivityTaskCompleted
```

## Signal Delivery During Downtime

```mermaid
sequenceDiagram
    participant CLI
    participant TemporalServer as Temporal Server
    participant Worker

    Note over Worker: Worker is DOWN

    CLI->>TemporalServer: SignalWorkflow(cancelSubscription, { reason: "..." })
    TemporalServer->>TemporalServer: Append WorkflowExecutionSignaled to history
    Note over TemporalServer: Signal is stored — not lost

    Note over Worker: Worker restarts

    Worker->>TemporalServer: PollForWorkflowTask
    TemporalServer-->>Worker: WorkflowTask including the signal event
    Note over Worker: Replay → signal handler sets isCancelled=true<br/>→ branches to cancellation path
```

## Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| **Temporal Server** | Receives commands, stores history, schedules workflow/activity tasks, fires timers, delivers signals |
| **Worker** | Polls for tasks, executes workflow code (sandboxed replay), executes activities (real Node.js), reports results |
| **Workflow History** | Authoritative record of all events — the "source of truth" for workflow state. Enables crash recovery via replay |
| **Task Queue** | Named channel connecting server to workers. Workers poll their queue. Multiple workers on same queue = automatic load balancing |

## What Temporal Gives You For Free

| Problem | Temporal's Solution |
|---------|---------------------|
| Worker crash mid-activity | Reschedule after startToCloseTimeout |
| Worker crash during sleep | Timer stored server-side, fires when worker returns |
| Duplicate activity execution | History records completion; replay skips completed activities |
| Signal delivery to sleeping workflow | Signals stored in history; delivered on next workflow task |
| Concurrent workflow execution | Server assigns each workflow task to exactly one worker |
| Retry with backoff | Configured in `proxyActivities` retry policy |
| Activity timeout | `startToCloseTimeout` / `scheduleToCloseTimeout` |
| Long-running workflow state | Encoded in workflow history — survives all restarts |
