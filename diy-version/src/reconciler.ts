// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow Reconciler
//
// The reconciler is the "catch-all" safety net. It runs periodically and
// looks for three categories of inconsistency:
//
//   1. EXPIRED LOCKS:
//      A worker acquired a lock, crashed, and left the lock in place.
//      Action: Release the lock so another worker can process the workflow.
//
//   2. STUCK WORKFLOWS (state-queue mismatch):
//      A worker updated the workflow state and crashed before enqueueing the
//      next task. The workflow is in a non-waiting, non-terminal state with
//      no task in the queue.
//      Action: Re-enqueue the appropriate task.
//
//   3. TIMED-OUT ACTIVITIES:
//      A worker picked up a task (set status=RUNNING) and crashed before
//      completing or failing it. The activity_attempts row is stuck at RUNNING.
//      Action: Mark as FAILED and schedule a retry.
//
// TEMPORAL EQUIVALENT:
//   Temporal handles all three of these automatically via:
//   - Schedule-to-start timeout → activity reassigned if worker dies
//   - Server-side heartbeat detection → worker considered dead
//   - Durable timer queue → can't get stuck between state + queue
//
//   The reconciler is the DIY equivalent of all that built-in machinery.
//
// Run frequency: every 10 seconds is usually sufficient.
// ─────────────────────────────────────────────────────────────────────────────

import { v4 as uuidv4 } from 'uuid';
import { waitForDb, query } from './db/client';
import { waitForRedis, enqueueImmediate, enqueueDelayed } from './queue/redisQueue';
import { WorkflowState, TaskType, TRANSITIONS, TERMINAL_STATES } from './orchestrator/stateMachine';
import type { QueueTask, WorkflowRow } from './types';

const RECONCILE_INTERVAL_MS    = 10_000;  // Run every 10 seconds
const ACTIVITY_TIMEOUT_SECONDS = 60;      // Activities running > 60s are considered stuck

// ── Map from workflow state to the task that should have been running ──────────
// If a workflow is in state X but has no pending task, something went wrong.
// This map tells us which task to re-enqueue.
const STATE_TO_PENDING_TASK: Partial<Record<WorkflowState, TaskType>> = {
  [WorkflowState.WELCOME_EMAIL_SCHEDULED]: TaskType.SEND_WELCOME_EMAIL,
  [WorkflowState.CHARGE_SCHEDULED]:        TaskType.CHARGE_MONTHLY_FEE,
  [WorkflowState.CHARGING]:               TaskType.CHARGE_MONTHLY_FEE,
  [WorkflowState.CHARGED]:                TaskType.SEND_END_OF_TRIAL_EMAIL,
  [WorkflowState.END_OF_TRIAL_EMAIL_SENT]: TaskType.SEND_MONTHLY_CHARGE_EMAIL,
  [WorkflowState.CANCELLATION_REQUESTED]: TaskType.PROCESS_CANCELLATION,
  [WorkflowState.CANCELLING]:             TaskType.SEND_SORRY_EMAIL,
};

// ── Step 1: Release expired locks ─────────────────────────────────────────────
async function releaseExpiredLocks(): Promise<void> {
  const result = await query(`
    DELETE FROM workflow_locks
    WHERE expires_at < NOW()
    RETURNING workflow_id, locked_by
  `);

  if (result.rows.length > 0) {
    console.log(`[reconciler] 🔓 Released ${result.rows.length} expired lock(s):`);
    for (const row of result.rows) {
      console.log(`[reconciler]    workflowId=${row.workflow_id} was locked by ${row.locked_by}`);

      await query(`
        INSERT INTO workflow_events (workflow_id, event_type, event_data)
        VALUES ($1, 'LOCK_EXPIRED', $2)
      `, [row.workflow_id, JSON.stringify({ lockedBy: row.locked_by, reason: 'lock_expired' })]);
    }
  }
}

// ── Step 2: Detect and repair stuck workflows ─────────────────────────────────
// A "stuck" workflow is one that:
//   - Is in an actionable (non-terminal, non-waiting) state
//   - Has not been updated in more than ACTIVITY_TIMEOUT_SECONDS
//   - Does not currently hold a lock (i.e., no worker is processing it)
async function repairStuckWorkflows(): Promise<void> {
  const actionableStates = Object.keys(STATE_TO_PENDING_TASK);

  const result = await query<WorkflowRow>(`
    SELECT sw.*
    FROM subscription_workflows sw
    LEFT JOIN workflow_locks wl ON sw.id = wl.workflow_id
    WHERE sw.state = ANY($1::text[])
      AND sw.updated_at < NOW() - INTERVAL '${ACTIVITY_TIMEOUT_SECONDS} seconds'
      AND wl.workflow_id IS NULL  -- not currently locked
    LIMIT 20
  `, [actionableStates]);

  if (result.rows.length === 0) return;

  console.log(`[reconciler] 🔧 Found ${result.rows.length} stuck workflow(s)`);

  for (const workflow of result.rows) {
    const taskType = STATE_TO_PENDING_TASK[workflow.state];
    if (!taskType) continue;

    // Check for existing idempotency key — activity may have completed
    // but the state didn't get updated (crash between activity success and
    // state update commit, which should be impossible with our transaction,
    // but defensive check is cheap)
    const idem = await query(`
      SELECT key FROM idempotency_keys
      WHERE workflow_id = $1 AND activity_type = $2
    `, [workflow.id, taskType]);

    const context: Record<string, any> = { ...workflow.metadata };
    if (workflow.cancellation_reason) {
      context.reason = workflow.cancellation_reason;
    }

    const task: QueueTask = {
      taskId: uuidv4(),
      workflowId: workflow.id,
      customerId: workflow.customer_id,
      taskType,
      attempt: 1,
      context,
    };

    await enqueueImmediate(task);

    console.log(`[reconciler] ↺ Re-enqueued task | workflowId=${workflow.id} | state=${workflow.state} | task=${taskType}`);

    await query(`
      INSERT INTO workflow_events (workflow_id, event_type, event_data)
      VALUES ($1, 'RECONCILER_REQUEUED', $2)
    `, [workflow.id, JSON.stringify({ taskType, reason: 'stuck_workflow_detected' })]);
  }
}

// ── Step 3: Detect and reset timed-out activity attempts ─────────────────────
async function resetTimedOutActivities(): Promise<void> {
  const result = await query(`
    SELECT aa.*, sw.customer_id
    FROM activity_attempts aa
    JOIN subscription_workflows sw ON aa.workflow_id = sw.id
    WHERE aa.status = 'RUNNING'
      AND aa.started_at < NOW() - INTERVAL '${ACTIVITY_TIMEOUT_SECONDS} seconds'
    LIMIT 20
  `);

  if (result.rows.length === 0) return;

  console.log(`[reconciler] ⏱  Found ${result.rows.length} timed-out activity attempt(s)`);

  for (const row of result.rows) {
    await query(`
      UPDATE activity_attempts
      SET status = 'FAILED', completed_at = NOW(), error_message = 'Timed out — worker likely crashed'
      WHERE id = $1
    `, [row.id]);

    console.log(`[reconciler] ✗ Marked attempt as timed-out | id=${row.id} | type=${row.activity_type}`);
  }
  // Note: repairStuckWorkflows() will then re-enqueue these workflows
}

// ── Main loop ─────────────────────────────────────────────────────────────────
async function main() {
  console.log('[reconciler] DIY Reconciler starting');
  await waitForDb();
  await waitForRedis();
  console.log('[reconciler] ✓ Ready — reconciling every', RECONCILE_INTERVAL_MS, 'ms');

  while (true) {
    try {
      await releaseExpiredLocks();
      await resetTimedOutActivities();
      await repairStuckWorkflows();
    } catch (err: any) {
      console.error('[reconciler] Error in reconcile cycle:', err.message);
    }
    await new Promise((r) => setTimeout(r, RECONCILE_INTERVAL_MS));
  }
}

main().catch((err) => {
  console.error('[reconciler] Fatal error:', err);
  process.exit(1);
});
