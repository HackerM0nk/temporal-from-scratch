// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow Reconciler
//
// Runs every 10 seconds. Fixes three categories of inconsistency:
//
//   1. EXPIRED LOCKS
//      A worker acquired workflow_locks and crashed. The lock expired.
//      Action: DELETE the lock row so another worker can proceed.
//      With RabbitMQ: RabbitMQ already redelivered the message to another
//      consumer. But the PostgreSQL lock may still be blocking — this clears it.
//
//   2. STUCK WORKFLOWS (state ≠ what's in RabbitMQ)
//      A worker committed a state update to PostgreSQL but crashed before
//      publishing the next task to RabbitMQ. The workflow is in a state
//      that implies a task should be running, but nothing is moving.
//      Action: INSERT into scheduled_tasks with execute_after = NOW() so
//      the scheduler publishes the missing task on its next poll.
//
//      This is the fundamental dual-write problem: you cannot atomically
//      update a database AND publish to a message broker in one transaction.
//      Temporal eliminates this problem entirely — it is both the state store
//      and the task queue. Cadence, Netflix Conductor, and AWS Step Functions
//      use the same approach for the same reason.
//
//   3. TIMED-OUT ACTIVITY ATTEMPTS
//      A worker set status=RUNNING in activity_attempts but crashed before
//      completing. Mark as FAILED so the retry count is accurate.
//      The stuck-workflow check (2) will then re-enqueue the task.
//
// The reconciler only touches PostgreSQL. It inserts into scheduled_tasks
// for republishing rather than calling publishTask() directly. This means
// the reconciler works even when RabbitMQ is temporarily down.
// ─────────────────────────────────────────────────────────────────────────────

import { v4 as uuidv4 } from 'uuid';
import { waitForDb, query } from './db/client';
import { WorkflowState, TaskType, TERMINAL_STATES } from './orchestrator/stateMachine';
import type { WorkflowRow } from './types';

const RECONCILE_INTERVAL_MS   = 10_000;
const ACTIVITY_TIMEOUT_SECONDS = 60;

// States that imply a task should be in flight
const STATE_TO_TASK: Partial<Record<WorkflowState, TaskType>> = {
  [WorkflowState.WELCOME_EMAIL_SCHEDULED]: TaskType.SEND_WELCOME_EMAIL,
  [WorkflowState.CHARGE_SCHEDULED]:        TaskType.CHARGE_MONTHLY_FEE,
  [WorkflowState.CHARGING]:                TaskType.CHARGE_MONTHLY_FEE,
  [WorkflowState.CHARGED]:                 TaskType.SEND_END_OF_TRIAL_EMAIL,
  [WorkflowState.END_OF_TRIAL_EMAIL_SENT]: TaskType.SEND_MONTHLY_CHARGE_EMAIL,
  [WorkflowState.CANCELLATION_REQUESTED]:  TaskType.PROCESS_CANCELLATION,
  [WorkflowState.CANCELLING]:              TaskType.SEND_SORRY_EMAIL,
};

async function releaseExpiredLocks(): Promise<void> {
  const result = await query(`
    DELETE FROM workflow_locks WHERE expires_at < NOW()
    RETURNING workflow_id, locked_by
  `);
  if (!result.rows.length) return;

  console.log(`[reconciler] 🔓 Released ${result.rows.length} expired lock(s)`);
  for (const row of result.rows) {
    console.log(`[reconciler]    workflow=${row.workflow_id} was locked by ${row.locked_by}`);
    await query(
      `INSERT INTO workflow_events (workflow_id, event_type, event_data) VALUES ($1, 'LOCK_EXPIRED', $2)`,
      [row.workflow_id, JSON.stringify({ lockedBy: row.locked_by })],
    );
  }
}

async function resetTimedOutAttempts(): Promise<void> {
  const result = await query(`
    UPDATE activity_attempts
    SET status='FAILED', completed_at=NOW(), error_message='Timed out — worker likely crashed'
    WHERE status='RUNNING'
      AND started_at < NOW() - INTERVAL '${ACTIVITY_TIMEOUT_SECONDS} seconds'
    RETURNING id, workflow_id, activity_type
  `);
  if (!result.rows.length) return;
  console.log(`[reconciler] ⏱  Reset ${result.rows.length} timed-out attempt(s)`);
}

async function repairStuckWorkflows(): Promise<void> {
  const actionableStates = Object.keys(STATE_TO_TASK);
  const result = await query<WorkflowRow>(`
    SELECT sw.*
    FROM subscription_workflows sw
    LEFT JOIN workflow_locks wl ON sw.id = wl.workflow_id
    WHERE sw.state = ANY($1::text[])
      AND sw.updated_at < NOW() - INTERVAL '${ACTIVITY_TIMEOUT_SECONDS} seconds'
      AND wl.workflow_id IS NULL
    LIMIT 20
  `, [actionableStates]);

  if (!result.rows.length) return;
  console.log(`[reconciler] 🔧 Found ${result.rows.length} stuck workflow(s)`);

  for (const wf of result.rows) {
    const taskType = STATE_TO_TASK[wf.state];
    if (!taskType) continue;

    const context: Record<string, any> = { ...(wf.metadata ?? {}) };
    if (wf.cancellation_reason) context.reason = wf.cancellation_reason;

    // Insert into scheduled_tasks rather than publishing directly to RabbitMQ.
    // This keeps the reconciler PostgreSQL-only and lets the scheduler handle
    // publishing — works even if RabbitMQ is temporarily unavailable.
    await query(`
      INSERT INTO scheduled_tasks
        (id, workflow_id, customer_id, task_type, execute_after, attempt, context)
      VALUES ($1, $2, $3, $4, NOW(), 1, $5)
    `, [uuidv4(), wf.id, wf.customer_id, taskType, JSON.stringify(context)]);

    await query(
      `INSERT INTO workflow_events (workflow_id, event_type, event_data) VALUES ($1, 'RECONCILER_REQUEUED', $2)`,
      [wf.id, JSON.stringify({ taskType, reason: 'stuck_workflow' })],
    );

    console.log(`[reconciler] ↺ Re-enqueued | workflow=${wf.id} | state=${wf.state} | task=${taskType}`);
  }
}

async function main() {
  console.log('[reconciler] Starting');
  await waitForDb();
  console.log(`[reconciler] ✓ Reconciling every ${RECONCILE_INTERVAL_MS}ms`);

  while (true) {
    try {
      await releaseExpiredLocks();
      await resetTimedOutAttempts();
      await repairStuckWorkflows();
    } catch (err: any) {
      console.error('[reconciler] Error:', err.message);
    }
    await new Promise((r) => setTimeout(r, RECONCILE_INTERVAL_MS));
  }
}

main().catch((err) => {
  console.error('[reconciler] Fatal:', err);
  process.exit(1);
});
