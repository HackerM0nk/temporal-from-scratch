// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow Scheduler
//
// The scheduler is responsible for TWO things:
//
//   1. Timer-based wakeup:
//      Polls for workflows in WAITING_FOR_TRIAL_END state where trial_end_at
//      has passed. Enqueues a CHARGE_MONTHLY_FEE task for each.
//
//      TEMPORAL EQUIVALENT: workflow.sleep() / workflow.condition(fn, timeout)
//      Temporal stores a timer server-side and delivers it automatically.
//      Here, we poll a database column every few seconds.
//
//   2. Delayed task promotion:
//      Moves ready delayed tasks (retry queue) to the immediate queue.
//      This is also done by the worker, but the scheduler ensures it happens
//      even if all workers are idle.
//
// WHY is this a separate process?
//   If the scheduler logic lived in the worker, a single worker crash would
//   mean no timer wakeups. By running it separately, timers still fire even
//   when workers are down (the tasks queue up and drain when workers restart).
//
// FAILURE MODE: If THIS process crashes, timers will fire late (by up to
//   POLL_INTERVAL_MS). That is acceptable. The reconciler provides a backup.
// ─────────────────────────────────────────────────────────────────────────────

import { v4 as uuidv4 } from 'uuid';
import { waitForDb, query, withTransaction } from './db/client';
import { waitForRedis, enqueueImmediate, promoteDelayedTasks } from './queue/redisQueue';
import { WorkflowState, TaskType } from './orchestrator/stateMachine';
import type { QueueTask, WorkflowRow } from './types';

const POLL_INTERVAL_MS = 2000;  // Check for due timers every 2 seconds

async function wakeExpiredTrials(): Promise<void> {
  // Find all workflows whose trial period has ended but haven't been advanced
  const result = await query<WorkflowRow>(`
    SELECT *
    FROM subscription_workflows
    WHERE state = $1
      AND trial_end_at IS NOT NULL
      AND trial_end_at <= NOW()
  `, [WorkflowState.WAITING_FOR_TRIAL_END]);

  if (result.rows.length === 0) return;

  console.log(`[scheduler] ⏰ ${result.rows.length} trial(s) expired — enqueuing charge tasks`);

  for (const workflow of result.rows) {
    console.log(`[scheduler] ⏰ Trial ended | workflowId=${workflow.id} | customerId=${workflow.customer_id}`);

    // KEY CONCEPT: Check for an existing idempotency key before enqueuing.
    // If the scheduler runs twice (e.g., after a crash and restart), we should
    // not enqueue two CHARGE tasks for the same workflow.
    const existing = await query(`
      SELECT key FROM idempotency_keys
      WHERE workflow_id = $1 AND activity_type = $2
    `, [workflow.id, TaskType.CHARGE_MONTHLY_FEE]);

    if (existing.rows.length > 0) {
      console.log(`[scheduler] ⊘ Charge already exists for ${workflow.id} — skipping`);
      continue;
    }

    // Transition state from WAITING_FOR_TRIAL_END → CHARGE_SCHEDULED
    // This prevents two scheduler instances from double-scheduling
    const updated = await query(`
      UPDATE subscription_workflows
      SET state = $1, updated_at = NOW()
      WHERE id = $2 AND state = $3  -- optimistic: only update if still waiting
      RETURNING id
    `, [WorkflowState.CHARGE_SCHEDULED, workflow.id, WorkflowState.WAITING_FOR_TRIAL_END]);

    if (!updated.rowCount || updated.rowCount === 0) {
      console.log(`[scheduler] Race condition: ${workflow.id} state changed before we could schedule`);
      continue;
    }

    // Record the timer-fired event
    await query(`
      INSERT INTO workflow_events (workflow_id, event_type, event_data)
      VALUES ($1, 'TIMER_FIRED', $2)
    `, [workflow.id, JSON.stringify({ triggeredBy: 'trial-end-scheduler' })]);

    const task: QueueTask = {
      taskId: uuidv4(),
      workflowId: workflow.id,
      customerId: workflow.customer_id,
      taskType: TaskType.CHARGE_MONTHLY_FEE,
      attempt: 1,
      context: {},
    };

    await enqueueImmediate(task);
    console.log(`[scheduler] → Enqueued CHARGE_MONTHLY_FEE for ${workflow.customer_id}`);
  }
}

async function main() {
  console.log('[scheduler] DIY Scheduler starting');
  await waitForDb();
  await waitForRedis();
  console.log('[scheduler] ✓ Ready — polling for expired trials every', POLL_INTERVAL_MS, 'ms');

  while (true) {
    try {
      await promoteDelayedTasks();
      await wakeExpiredTrials();
    } catch (err: any) {
      console.error('[scheduler] Error in poll cycle:', err.message);
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

main().catch((err) => {
  console.error('[scheduler] Fatal error:', err);
  process.exit(1);
});
