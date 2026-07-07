// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow Scheduler
//
// Polls PostgreSQL every 2 seconds for two categories of due work:
//
//   1. Trial timers expired:
//      Workflows in WAITING_FOR_TRIAL_END whose trial_end_at has passed.
//      Publishes CHARGE_MONTHLY_FEE to RabbitMQ.
//
//      Previously: Redis ZSET scored by timestamp, worker promoted due tasks.
//      Now: trial_end_at column in PostgreSQL, scheduler publishes directly.
//      Temporal equivalent: workflow.sleep() / condition(fn, timeout) —
//      the timer lives inside Temporal's server, not in any process's memory.
//
//   2. Scheduled retry tasks:
//      Rows in the scheduled_tasks table whose execute_after has passed and
//      published_at is NULL (not yet sent to RabbitMQ).
//      Publishes the task to RabbitMQ and marks published_at.
//
//      Previously: Redis ZSET diy:tasks:delayed, promoted by scheduler.
//      Now: scheduled_tasks in PostgreSQL, same scheduler handles both.
//      Temporal equivalent: automatic retry scheduling in proxyActivities config.
//
// WHY is this a separate process?
//   If timer logic lived in the worker, a worker crash would silently stop all
//   timers from firing. As a separate process, timers continue to fire even
//   when all workers are down — tasks queue up in RabbitMQ and drain when
//   workers restart.
// ─────────────────────────────────────────────────────────────────────────────

import { v4 as uuidv4 } from 'uuid';
import { waitForDb, query } from './db/client';
import { waitForRabbitMQ, publishTask } from './queue/rabbitmq';
import { WorkflowState, TaskType } from './orchestrator/stateMachine';
import type { QueueTask, WorkflowRow } from './types';

const POLL_INTERVAL_MS = 2000;

// ── 1. Fire expired trial timers ──────────────────────────────────────────────
async function fireExpiredTrials(): Promise<void> {
  const result = await query<WorkflowRow>(`
    SELECT * FROM subscription_workflows
    WHERE state = $1
      AND trial_end_at IS NOT NULL
      AND trial_end_at <= NOW()
  `, [WorkflowState.WAITING_FOR_TRIAL_END]);

  if (!result.rows.length) return;

  console.log(`[scheduler] ⏰ ${result.rows.length} trial(s) expired`);

  for (const wf of result.rows) {
    // Optimistic update: only advance if still in the expected state.
    // Prevents duplicate charge tasks if scheduler runs twice before DB updates.
    const updated = await query(`
      UPDATE subscription_workflows
      SET state = $1, updated_at = NOW()
      WHERE id = $2 AND state = $3
      RETURNING id
    `, [WorkflowState.CHARGE_SCHEDULED, wf.id, WorkflowState.WAITING_FOR_TRIAL_END]);

    if (!updated.rowCount || updated.rowCount === 0) continue; // already advanced

    await query(
      `INSERT INTO workflow_events (workflow_id, event_type, event_data) VALUES ($1, 'TIMER_FIRED', $2)`,
      [wf.id, JSON.stringify({ triggeredBy: 'scheduler' })],
    );

    await publishTask({
      taskId: uuidv4(),
      workflowId: wf.id,
      customerId: wf.customer_id,
      taskType: TaskType.CHARGE_MONTHLY_FEE,
      attempt: 1,
      context: {},
    });

    console.log(`[scheduler] → Published CHARGE_MONTHLY_FEE for ${wf.customer_id}`);
  }
}

// ── 2. Promote due scheduled tasks ───────────────────────────────────────────
// Rows in scheduled_tasks represent retry tasks waiting for their backoff delay.
// When execute_after passes, publish to RabbitMQ and mark published_at.
// published_at prevents re-publishing on scheduler restart.
async function promoteScheduledTasks(): Promise<void> {
  const result = await query(`
    SELECT * FROM scheduled_tasks
    WHERE execute_after <= NOW()
      AND published_at IS NULL
    LIMIT 50
  `);

  if (!result.rows.length) return;

  console.log(`[scheduler] ↑ Promoting ${result.rows.length} scheduled task(s)`);

  for (const row of result.rows) {
    // Mark as published first (idempotent gate) then publish to RabbitMQ.
    // If we crash after the UPDATE but before publishTask, the row has
    // published_at set and won't be retried — the reconciler handles this case.
    const marked = await query(`
      UPDATE scheduled_tasks SET published_at = NOW()
      WHERE id = $1 AND published_at IS NULL
      RETURNING id
    `, [row.id]);

    if (!marked.rowCount || marked.rowCount === 0) continue; // another scheduler got there first

    const task: QueueTask = {
      taskId: uuidv4(),
      workflowId: row.workflow_id,
      customerId: row.customer_id,
      taskType: row.task_type,
      attempt: row.attempt,
      context: row.context ?? {},
    };

    await publishTask(task);
    console.log(`[scheduler] → Published retry task | type=${row.task_type} | attempt=${row.attempt}`);
  }
}

// ── Main loop ─────────────────────────────────────────────────────────────────
async function main() {
  console.log('[scheduler] Starting');
  await waitForDb();
  await waitForRabbitMQ();
  console.log(`[scheduler] ✓ Polling every ${POLL_INTERVAL_MS}ms`);

  while (true) {
    try {
      await fireExpiredTrials();
      await promoteScheduledTasks();
    } catch (err: any) {
      console.error('[scheduler] Error:', err.message);
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

main().catch((err) => {
  console.error('[scheduler] Fatal:', err);
  process.exit(1);
});
