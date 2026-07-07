// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow Worker
//
// The worker is responsible for:
//   1. Polling Redis for ready tasks
//   2. Acquiring a distributed lock on the workflow
//   3. Checking idempotency (skip if already done)
//   4. Executing the activity
//   5. Atomically updating workflow state + recording events
//   6. Enqueueing the next task
//   7. Releasing the lock
//
// EVERYTHING Temporal does automatically, we do manually here.
//
// KEY FAILURE MODES handled:
//   - Worker crash before step 5: task replayed, idempotency check skips re-execution
//   - Worker crash during step 5: activity re-executed (must be idempotent)
//   - Worker crash after step 5 but before step 6: reconciler re-enqueues
//   - Two workers pick up same task: first to acquire lock wins, other re-queues
//
// Kill this process at any time — workflows will be repaired by the reconciler.
// ─────────────────────────────────────────────────────────────────────────────

import { v4 as uuidv4 } from 'uuid';
import { PoolClient } from 'pg';
import { waitForDb, query, withTransaction } from './db/client';
import { waitForRedis, dequeueTask, enqueueImmediate, enqueueDelayed, promoteDelayedTasks } from './queue/redisQueue';
import { runMigrations } from './db/migrations';
import { WorkflowState, TaskType, TRANSITIONS, TERMINAL_STATES } from './orchestrator/stateMachine';
import * as activities from './activities/subscriptionActivities';
import type { QueueTask, WorkflowRow, ChargeResult } from './types';

const WORKER_ID = process.env.WORKER_ID ?? `worker-${process.pid}-${uuidv4().slice(0, 8)}`;
const LOCK_EXPIRY_SECONDS = 30;
const MAX_RETRY_ATTEMPTS = 5;

// ── Retry backoff: 2^attempt seconds (2s, 4s, 8s, 16s, 32s) ──────────────────
function retryDelayMs(attempt: number): number {
  return Math.pow(2, attempt) * 1000;
}

// ── Acquire distributed lock ──────────────────────────────────────────────────
// Uses an INSERT ... ON CONFLICT pattern.
// Returns true if lock was acquired, false if another worker holds it.
async function acquireLock(workflowId: string): Promise<boolean> {
  const result = await query<{ workflow_id: string }>(`
    INSERT INTO workflow_locks (workflow_id, locked_by, locked_at, expires_at)
    VALUES ($1, $2, NOW(), NOW() + INTERVAL '${LOCK_EXPIRY_SECONDS} seconds')
    ON CONFLICT (workflow_id) DO UPDATE
      SET locked_by  = EXCLUDED.locked_by,
          locked_at  = NOW(),
          expires_at = NOW() + INTERVAL '${LOCK_EXPIRY_SECONDS} seconds'
      WHERE workflow_locks.expires_at < NOW()  -- only steal an EXPIRED lock
    RETURNING workflow_id
  `, [workflowId, WORKER_ID]);

  return result.rowCount !== null && result.rowCount > 0;
}

async function releaseLock(workflowId: string): Promise<void> {
  await query(`
    DELETE FROM workflow_locks
    WHERE workflow_id = $1 AND locked_by = $2
  `, [workflowId, WORKER_ID]);
}

// ── Record a workflow event (append-only audit log) ───────────────────────────
async function insertEvent(
  client: PoolClient,
  workflowId: string,
  eventType: string,
  eventData: object,
): Promise<void> {
  await client.query(`
    INSERT INTO workflow_events (workflow_id, event_type, event_data)
    VALUES ($1, $2, $3)
  `, [workflowId, eventType, JSON.stringify(eventData)]);
}

// ── Execute the correct activity function for a given task type ───────────────
async function executeActivity(task: QueueTask): Promise<any> {
  const { taskType, customerId, workflowId, attempt, context = {} } = task;

  switch (taskType) {
    case TaskType.SEND_WELCOME_EMAIL:
      return activities.sendWelcomeEmail(customerId, workflowId);

    case TaskType.CHARGE_MONTHLY_FEE:
      return activities.chargeMonthlyFee(customerId, workflowId, attempt);

    case TaskType.SEND_END_OF_TRIAL_EMAIL:
      return activities.sendEndOfTrialEmail(customerId, workflowId, context.chargeId ?? '');

    case TaskType.SEND_MONTHLY_CHARGE_EMAIL:
      return activities.sendMonthlyChargeEmail(customerId, workflowId, context.amount ?? 0);

    case TaskType.PROCESS_CANCELLATION:
      return activities.processSubscriptionCancellation(customerId, workflowId, context.reason ?? '');

    case TaskType.SEND_SORRY_EMAIL:
      return activities.sendSorryToSeeYouGoEmail(customerId, workflowId);

    default:
      throw new Error(`Unknown task type: ${taskType}`);
  }
}

// ── Main task processing logic ────────────────────────────────────────────────
async function processTask(task: QueueTask): Promise<void> {
  const { taskId, workflowId, taskType, attempt } = task;
  const prefix = `[${WORKER_ID}]`;

  console.log(`${prefix} ← Picked up task | type=${taskType} | workflow=${workflowId} | attempt=${attempt}`);

  // ── 1. Acquire lock ────────────────────────────────────────────────────────
  // KEY CONCEPT: Without this lock, two workers could process the same workflow
  // concurrently, leading to duplicate charges, inconsistent state, etc.
  // Temporal prevents this by having the server assign one task at a time.
  const locked = await acquireLock(workflowId);
  if (!locked) {
    console.log(`${prefix} ✗ Could not acquire lock for ${workflowId} — re-queuing task`);
    // Re-queue with a short delay to avoid a spin loop
    await enqueueDelayed(task, Date.now() + 3000);
    return;
  }

  try {
    // ── 2. Fetch workflow row and check state ──────────────────────────────────
    const wfResult = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE id = $1', [workflowId]
    );
    if (wfResult.rows.length === 0) {
      console.log(`${prefix} ✗ Workflow ${workflowId} not found — discarding task`);
      return;
    }
    const workflow = wfResult.rows[0];

    if (TERMINAL_STATES.has(workflow.state)) {
      console.log(`${prefix} ✗ Workflow ${workflowId} is in terminal state ${workflow.state} — discarding task`);
      return;
    }

    // ── 3. Check idempotency ───────────────────────────────────────────────────
    // KEY CONCEPT: Redis can deliver the same message twice (at-least-once).
    // We store completed activities in idempotency_keys. If we've already
    // done this activity for this workflow, we return the cached result.
    //
    // In Temporal, this check is done implicitly: the workflow history has a
    // "ActivityTaskCompleted" event, so replay skips the activity entirely.
    const idempotencyKey = `${workflowId}:${taskType}`;
    const idemResult = await query(
      'SELECT result FROM idempotency_keys WHERE key = $1', [idempotencyKey]
    );

    if (idemResult.rows.length > 0) {
      console.log(`${prefix} ⊘ Idempotent skip | key=${idempotencyKey} — already completed, ensuring next step`);
      // Activity was already done; make sure the next task is queued
      const cachedResult = idemResult.rows[0].result;
      await ensureNextTaskQueued(task, workflow, cachedResult);
      return;
    }

    // ── 4. Record "TASK_STARTED" event ────────────────────────────────────────
    const attemptId = uuidv4();
    await withTransaction(async (client) => {
      await client.query(`
        INSERT INTO activity_attempts (id, workflow_id, activity_type, attempt_number, status, started_at)
        VALUES ($1, $2, $3, $4, 'RUNNING', NOW())
      `, [attemptId, workflowId, taskType, attempt]);

      await insertEvent(client, workflowId, 'TASK_STARTED', { taskType, attempt, taskId });
    });

    // ── 5. Execute the activity ────────────────────────────────────────────────
    // KEY CONCEPT: This is the ONLY place where side effects happen.
    // If this crashes, the worker restarts, picks up the task again (from Redis
    // or after reconciler re-queues it), and re-executes — which is safe because
    // activities use idempotency keys with the external system.
    let activityResult: any = null;
    try {
      activityResult = await executeActivity(task);
    } catch (err: any) {
      await handleActivityFailure(task, attemptId, err);
      return;
    }

    // ── 6. Atomic: record result + transition state + emit event ───────────────
    // KEY CONCEPT: This is a single ACID transaction. Either ALL of these
    // writes happen, or NONE do. This prevents partial updates like:
    //   "idempotency key set but state not updated" or vice versa.
    //
    // If this transaction commits and the worker crashes before step 7,
    // the reconciler will detect the workflow is in a "completed" state
    // with no pending next task and will re-enqueue the next task.
    const transition = TRANSITIONS[taskType as TaskType];
    const newState = transition.onComplete;

    // Build metadata updates based on the activity result
    const metadataUpdate: Record<string, any> = {};
    if (taskType === TaskType.CHARGE_MONTHLY_FEE && activityResult) {
      const charge = activityResult as ChargeResult;
      metadataUpdate.chargeId = charge.chargeId;
      metadataUpdate.amount = charge.amount;
    }

    await withTransaction(async (client) => {
      // Store idempotency key with the result
      await client.query(`
        INSERT INTO idempotency_keys (key, workflow_id, activity_type, result)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (key) DO NOTHING
      `, [idempotencyKey, workflowId, taskType, JSON.stringify(activityResult)]);

      // Transition workflow state
      await client.query(`
        UPDATE subscription_workflows
        SET state      = $1,
            updated_at = NOW(),
            metadata   = metadata || $2::jsonb
        WHERE id = $3
      `, [newState, JSON.stringify(metadataUpdate), workflowId]);

      // Mark activity attempt as completed
      await client.query(`
        UPDATE activity_attempts
        SET status = 'COMPLETED', completed_at = NOW(), result = $1
        WHERE id = $2
      `, [JSON.stringify(activityResult), attemptId]);

      // Emit completion event
      await insertEvent(client, workflowId, 'TASK_COMPLETED', {
        taskType,
        attempt,
        newState,
        result: activityResult,
      });

      // Special: after WELCOME_EMAIL_SENT, set the trial timer
      // This is equivalent to workflow.sleep(TRIAL_PERIOD_MS) in Temporal
      if (newState === WorkflowState.WELCOME_EMAIL_SENT) {
        const trialSeconds = parseInt(process.env.TRIAL_PERIOD_SECONDS ?? '30', 10);
        await client.query(`
          UPDATE subscription_workflows
          SET state        = $1,
              trial_end_at = NOW() + ($2 || ' seconds')::INTERVAL
          WHERE id = $3
        `, [WorkflowState.WAITING_FOR_TRIAL_END, trialSeconds, workflowId]);

        await insertEvent(client, workflowId, 'TIMER_STARTED', {
          trialSeconds,
          note: 'Scheduler will wake this workflow when trial_end_at passes',
        });
      }
    });

    console.log(`${prefix} ✓ Task complete | type=${taskType} | newState=${
      newState === WorkflowState.WELCOME_EMAIL_SENT
        ? WorkflowState.WAITING_FOR_TRIAL_END  // override for display
        : newState
    }`);

    // ── 7. Enqueue next task ──────────────────────────────────────────────────
    // KEY CONCEPT: This happens AFTER the DB transaction. If it fails,
    // the workflow state is correct in DB but the task is not in Redis.
    // The reconciler detects this (workflow stuck in a non-waiting state
    // with no pending task) and re-enqueues.
    //
    // This is one of the hardest problems in DIY orchestration — maintaining
    // consistency between your database and your queue. Temporal eliminates
    // this problem by making the task queue and state storage the same system.
    if (transition.nextTask && newState !== WorkflowState.WAITING_FOR_TRIAL_END) {
      const nextTask: QueueTask = {
        taskId: uuidv4(),
        workflowId,
        customerId: task.customerId,
        taskType: transition.nextTask,
        attempt: 1,
        context: { ...task.context, ...metadataUpdate },
      };
      await enqueueImmediate(nextTask);
    }

  } finally {
    await releaseLock(workflowId);
  }
}

// ── Handle activity failure: retry or dead-letter ─────────────────────────────
async function handleActivityFailure(
  task: QueueTask,
  attemptId: string,
  err: Error,
): Promise<void> {
  const { workflowId, taskType, attempt } = task;
  console.log(`[${WORKER_ID}] ✗ Activity failed | type=${taskType} | attempt=${attempt}/${MAX_RETRY_ATTEMPTS} | error=${err.message}`);

  // Record the failure
  await withTransaction(async (client) => {
    await client.query(`
      UPDATE activity_attempts
      SET status = 'FAILED', completed_at = NOW(), error_message = $1
      WHERE id = $2
    `, [err.message, attemptId]);

    await insertEvent(client, workflowId, 'TASK_FAILED', {
      taskType, attempt, error: err.message,
    });
  });

  if (attempt >= MAX_RETRY_ATTEMPTS) {
    // Move to dead letter
    console.log(`[${WORKER_ID}] 💀 Dead-lettering task | type=${taskType} | exhausted ${MAX_RETRY_ATTEMPTS} attempts`);
    await withTransaction(async (client) => {
      await client.query(`
        INSERT INTO dead_letter_tasks (id, workflow_id, activity_type, payload, error_message, retry_count)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [uuidv4(), workflowId, taskType, JSON.stringify(task), err.message, attempt]);

      await client.query(`
        UPDATE subscription_workflows SET state = $1, updated_at = NOW() WHERE id = $2
      `, [WorkflowState.FAILED, workflowId]);

      await insertEvent(client, workflowId, 'WORKFLOW_FAILED', {
        reason: `Activity ${taskType} exhausted retries`,
        error: err.message,
      });
    });
    return;
  }

  // Schedule retry with exponential backoff
  const delayMs = retryDelayMs(attempt);
  const retryTask: QueueTask = { ...task, attempt: attempt + 1 };
  await enqueueDelayed(retryTask, Date.now() + delayMs);
  console.log(`[${WORKER_ID}] ↺ Retry scheduled | type=${taskType} | attempt=${attempt + 1} | delay=${delayMs / 1000}s`);
}

// ── After idempotent skip: ensure next task is queued ─────────────────────────
async function ensureNextTaskQueued(
  task: QueueTask,
  workflow: WorkflowRow,
  cachedResult: any,
): Promise<void> {
  const transition = TRANSITIONS[task.taskType as TaskType];
  if (!transition.nextTask) return;

  const wfResult = await query<WorkflowRow>(
    'SELECT * FROM subscription_workflows WHERE id = $1', [task.workflowId]
  );
  const current = wfResult.rows[0];
  if (!current) return;

  // Only enqueue if the workflow is stuck at the expected "complete" state
  if (current.state === transition.onComplete) {
    const context = { ...task.context, ...current.metadata };
    await enqueueImmediate({
      taskId: uuidv4(),
      workflowId: task.workflowId,
      customerId: task.customerId,
      taskType: transition.nextTask,
      attempt: 1,
      context,
    });
  }
}

// ── Main loop ─────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[${WORKER_ID}] DIY Worker starting`);
  console.log(`[${WORKER_ID}] DATABASE_URL: ${process.env.DATABASE_URL}`);
  console.log(`[${WORKER_ID}] REDIS_URL: ${process.env.REDIS_URL}`);
  console.log(`[${WORKER_ID}] SIMULATE_CHARGE_FAILURE: ${process.env.SIMULATE_CHARGE_FAILURE}`);

  await waitForDb();
  await runMigrations();
  await waitForRedis();

  console.log(`[${WORKER_ID}] ✓ Ready — polling for tasks`);
  console.log(`[${WORKER_ID}] Kill this process at any time — the reconciler will repair stuck workflows`);

  while (true) {
    // Promote any delayed tasks that are now due
    await promoteDelayedTasks();

    // Block-wait for the next task (5s timeout)
    const task = await dequeueTask(5);
    if (!task) continue;  // timeout — loop back and check delayed queue

    // Process asynchronously — in production, use a worker pool for concurrency
    processTask(task).catch((err) => {
      console.error(`[${WORKER_ID}] Unhandled error processing task:`, err);
    });
  }
}

main().catch((err) => {
  console.error('[worker] Fatal error:', err);
  process.exit(1);
});
