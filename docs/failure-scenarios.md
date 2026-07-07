# Failure Scenarios

## How to Run Each Scenario

Each scenario has a corresponding script in `scripts/failure-scenarios/`.
All commands assume you are in the repo root with infrastructure running.

---

## Scenario 1: Worker Crashes After Email Send

**Setup:** Start a workflow, kill the worker immediately after `sendWelcomeEmail` completes but before state is fully persisted.

### What Happens in Temporal

1. Worker executes `sendWelcomeEmail` activity
2. Temporal records `ActivityTaskCompleted(sendWelcomeEmail)` in history
3. Worker crashes (Ctrl+C)
4. Temporal detects heartbeat loss → reschedules the **workflow task** (not the activity)
5. New worker picks up workflow task, REPLAYS history
6. Replay sees `ActivityTaskCompleted(sendWelcomeEmail)` → skips the actual call, injects result
7. Replay advances to `sleep(30s)` → creates `TimerStarted` command
8. Execution resumes seamlessly — `sendWelcomeEmail` was NOT called twice

**Which component recovers:** Temporal Server (task timeout + reassignment)
**History records created:** `WorkflowTaskFailed` (if crash mid-task), then new `WorkflowTaskCompleted`
**Why idempotency still required:** The email HTTP request may have been sent but the worker crashed before Temporal recorded `ActivityTaskCompleted`. On retry (attempt 2), the email service needs an idempotency key to avoid a duplicate send.

### What Happens in DIY

1. Worker executes `sendWelcomeEmail`
2. Worker begins DB transaction: insert idempotency key + update state + insert event
3. **Case A: Crash before COMMIT** — transaction rolls back. State unchanged. No idempotency key. Redis task is gone (BLPOP already dequeued it). Reconciler detects `WELCOME_EMAIL_SCHEDULED` state + no lock + `updated_at` is old → re-enqueues. Worker re-executes `sendWelcomeEmail`. Idempotency key sent to email service prevents duplicate.
4. **Case B: Crash after COMMIT but before enqueue** — state is `WAITING_FOR_TRIAL_END`, `trial_end_at` is set. No next task in Redis. Scheduler detects `trial_end_at` eventually and fires the charge task. No data loss.

**Which component recovers:** Reconciler (Case A) or Scheduler (Case B)
**State/history records created:** `RECONCILER_REQUEUED` event in `workflow_events`
**Why idempotency still required:** Activity may have sent the email before the transaction — retry needs the idempotency key to avoid duplicates.

---

## Scenario 2: Worker Crashes During Trial Wait

**Setup:** Start a workflow, let welcome email send, then kill the worker. The workflow should be sleeping.

### What Happens in Temporal

1. Workflow issues `TimerStarted` command
2. Timer is stored server-side — **completely independent of the worker**
3. Worker crashes
4. Timer fires after 30 seconds
5. Worker restarts (or another worker picks up)
6. Worker gets workflow task with `TimerFired` event in history
7. Workflow replays past sleep, continues to `chargeMonthlyFee`
8. Zero impact — the timer was never in the worker's memory

**Which component recovers:** Temporal Server (self-sufficient)
**History records created:** `TimerFired` event

### What Happens in DIY

1. `trial_end_at` is set in the DB — not in the worker process memory
2. Worker crashes (or is killed)
3. Scheduler continues polling independently (different process)
4. After 30 seconds, Scheduler finds `WAITING_FOR_TRIAL_END` with `trial_end_at < NOW()`
5. Scheduler enqueues `CHARGE_MONTHLY_FEE` task
6. Any worker (new or restarted) picks it up and continues

**Which component recovers:** Scheduler (timer is in DB, not in worker memory)
**State/history records created:** `TIMER_FIRED` event, `CHARGE_SCHEDULED` state
**Why idempotency still required:** Scheduler could run twice before the state update is visible — `UPDATE WHERE state = 'WAITING_FOR_TRIAL_END'` with optimistic locking prevents double-scheduling.

---

## Scenario 3: Charge Activity Fails Twice Then Succeeds

**Setup:** Set `SIMULATE_CHARGE_FAILURE=true` before starting the worker.

### What Happens in Temporal

1. `chargeMonthlyFee` activity scheduled (attempt 1)
2. Worker throws error → Temporal records `ActivityTaskFailed`
3. Temporal waits `initialInterval` (2s), schedules retry (attempt 2)
4. Attempt 2 fails → wait 4s
5. Attempt 3 succeeds
6. Temporal records `ActivityTaskCompleted`, workflow advances
7. **The workflow code never saw the failures** — it just `await`ed the result

**Which component recovers:** Temporal Server (retry scheduler)
**History records:** `ActivityTaskScheduled`, `ActivityTaskFailed` ×2, `ActivityTaskCompleted`

### What Happens in DIY

1. Worker picks up `CHARGE_MONTHLY_FEE` task (attempt 1)
2. Activity throws → worker calls `handleActivityFailure`
3. Worker inserts `activity_attempts` record with status=FAILED
4. Worker calls `enqueueDelayed(task, now + 2000)` — ZADD to Redis sorted set with score = `now + 2s`
5. Scheduler promotes delayed task after 2s → RPUSH to ready queue
6. Worker picks up (attempt 2) → fails → `enqueueDelayed(now + 4s)`
7. Worker picks up (attempt 3) → succeeds
8. Idempotency key written, state transitions to `CHARGED`

**Which component recovers:** Worker (enqueues delayed retry) + Scheduler (promotes delayed tasks)
**DB records created:** 3 rows in `activity_attempts`, 3 `TASK_FAILED`/`TASK_COMPLETED` events
**Why idempotency still required:** If attempt 2 crashes after calling Stripe but before recording success, attempt 3 would re-call Stripe — same idempotency key prevents double charge.

---

## Scenario 4: Duplicate Task Delivery

**Setup:** Manually RPUSH the same task JSON to `diy:tasks:ready` twice (simulating Redis at-least-once delivery), or send the same start request twice to the Temporal version.

### What Happens in Temporal

Starting the same `workflowId` twice: `WorkflowExecutionAlreadyStartedError` on the second call.
Same signal twice: handler fires twice. The `isCancelled` flag is set to `true` both times — idempotent (setting a bool twice is safe).
Same activity task (impossible from outside — Temporal deduplicates at server): history prevents replay.

### What Happens in DIY

1. Two `CHARGE_MONTHLY_FEE` tasks in Redis queue
2. Worker 1 picks up task A → acquires lock → checks idempotency key (not found) → executes charge → writes idempotency key → releases lock
3. Worker 2 picks up task B → tries to acquire lock → **lock held by Worker 1** → re-queues with delay
4. After Worker 1 releases lock: Worker 2 acquires lock → checks idempotency key → **FOUND** → skips execution
5. Customer is only charged once ✓

**Idempotency table prevents the duplicate.** Without it, both workers could execute `chargeMonthlyFee` if they happened to run simultaneously (lock expired between check and acquire).

---

## Scenario 5: Customer Cancels During Trial Wait

**Setup:** Start workflow, wait for welcome email, then immediately cancel.

### What Happens in Temporal

1. Workflow is suspended at `condition(() => isCancelled, TRIAL_PERIOD_MS)`
2. `cancelSubscription` signal arrives → `isCancelled = true`
3. `condition()` returns `true` (condition met before timeout)
4. Workflow branches to cancellation path: `processSubscriptionCancellation` + `sendSorryToSeeYouGoEmail`
5. Timer is automatically cancelled (never fires)

**Which component handles:** Temporal Server (signal delivery) + Workflow code (conditional branch)

### What Happens in DIY

1. Workflow is in `WAITING_FOR_TRIAL_END` state, sleeping
2. Cancel API call: UPDATE state → `CANCELLATION_REQUESTED`, set `cancellation_reason`
3. RPUSH `PROCESS_CANCELLATION` task to Redis
4. Worker picks up cancellation task → executes `processSubscriptionCancellation` + `sendSorryToSeeYouGoEmail`
5. Scheduler checks for `WAITING_FOR_TRIAL_END` — workflow is no longer in that state → **scheduler does nothing** (the optimistic UPDATE would affect 0 rows)
6. `trial_end_at` is effectively ignored — cancellation wins

**Which component handles:** API (signal equivalent) + Worker (cancellation activities)
**Race condition:** If the Scheduler fires the charge task at the exact same time as the cancel API runs:
- Scheduler does `UPDATE WHERE state = 'WAITING_FOR_TRIAL_END'` → 0 rows updated (already changed to `CANCELLATION_REQUESTED`)
- Worker picks up charge task → checks state → `CANCELLATION_REQUESTED` is in `TERMINAL_STATES` effectively → would need explicit check. In our code we check `TERMINAL_STATES` which doesn't include `CANCELLATION_REQUESTED`, so worker acquires lock, sees state is `CANCELLATION_REQUESTED` → discards the charge task (state mismatch with expected transition).

---

## Scenario 6: Database Updated but Queue Publish Fails

**Setup:** Simulate a Redis crash between the DB commit and the RPUSH.

### What Happens in Temporal

**This scenario cannot happen.** Temporal's task queue and workflow state storage are both managed by the Temporal Server in the same transactional context. Publishing the next workflow task is atomic with the state update.

### What Happens in DIY

1. Worker commits DB transaction (idempotency key + state = CHARGED)
2. Redis RPUSH fails (connection lost, Redis restarted, etc.)
3. Worker crashes or continues without the RPUSH succeeding
4. Workflow is stuck in `CHARGED` state with no task in the queue

**Recovery — Reconciler:**
1. Reconciler runs every 10s
2. Finds workflow in state `CHARGED` with `updated_at > 60s ago` and no active lock
3. `CHARGED` maps to `TaskType.SEND_END_OF_TRIAL_EMAIL` in `STATE_TO_PENDING_TASK`
4. Reconciler checks idempotency — `SEND_END_OF_TRIAL_EMAIL` not yet done
5. Reconciler re-enqueues `SEND_END_OF_TRIAL_EMAIL` task

**This is the most important scenario for DIY systems.** It demonstrates why you need a reconciler: you cannot atomically update a database AND publish to a message queue in a single transaction (two-phase commit is impractical). The reconciler is the compensating mechanism.

**State/history created:** `RECONCILER_REQUEUED` event

---

## Scenario 7: Stuck Workflow Repaired by Reconciliation

**Setup:** Kill the DIY worker while it holds a lock (e.g., in the middle of `chargeMonthlyFee`).

**Steps to observe:**
```bash
# 1. Start a workflow
npx ts-node diy-version/src/cli.ts start customer-789

# 2. Watch worker logs — wait for "Calling payment processor"
# 3. Kill the worker immediately
pkill -f "ts-node src/worker.ts"

# 4. Check state — should be CHARGING with a lock
psql $DATABASE_URL -c "SELECT state, updated_at FROM subscription_workflows WHERE customer_id='customer-789';"
psql $DATABASE_URL -c "SELECT * FROM workflow_locks;"

# 5. Wait 30 seconds (lock expires) + 10 seconds (reconciler runs)
# 6. Check again — should be CHARGING → lock gone, new task in queue
# 7. Restart worker — it picks up the task and continues
```

### What the Reconciler Does

```
[reconciler] 🔓 Released 1 expired lock(s):
[reconciler]    workflowId=<uuid> was locked by worker-12345-abc

[reconciler] 🔧 Found 1 stuck workflow(s)
[reconciler] ↺ Re-enqueued task | workflowId=<uuid> | state=CHARGING | task=CHARGE_MONTHLY_FEE
```

### Why the Charge is Not Duplicated

When the worker re-executes `chargeMonthlyFee`:
1. It checks `idempotency_keys` for `{workflowId}:CHARGE_MONTHLY_FEE`
2. If the charge succeeded before the crash: key exists → skips execution, returns cached result
3. If the charge was in-flight when crash happened: key absent → re-calls Stripe with same idempotency key → Stripe returns original charge result

The combination of **idempotency_keys table** (workflow-level) + **idempotency key on Stripe** (API-level) guarantees at-most-once charging.
