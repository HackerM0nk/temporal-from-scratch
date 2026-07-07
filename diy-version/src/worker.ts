// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow Worker
//
// Consumes tasks from RabbitMQ. For each task:
//
//   1. Acquire a distributed lock in PostgreSQL (one worker per workflow at a time)
//   2. Check the idempotency_keys table (skip if already done)
//   3. Execute the activity (the actual side effect)
//   4. Commit atomically: idempotency key + state transition + event + attempt
//   5. Publish the next task directly to RabbitMQ
//   6. ACK the message (tell RabbitMQ we're done — remove it from the queue)
//
// KEY DIFFERENCE from the old Redis version:
//
//   Previously, BLPOP removed the message immediately. A worker crash between
//   step 1 and step 6 meant the message was gone. Recovery depended on the
//   reconciler detecting a stuck workflow_lock after 60 seconds.
//
//   Now, the message stays "unacknowledged" in RabbitMQ until step 6. If this
//   process crashes at any point, RabbitMQ redelivers the message to another
//   consumer automatically. The reconciler still runs, but it's a backstop for
//   state/queue drift — not the primary crash-recovery mechanism.
//
//   Idempotency is still required: RabbitMQ redelivers on consumer crash, so
//   the same task can arrive twice. We check idempotency_keys before executing.
// ─────────────────────────────────────────────────────────────────────────────

import { v4 as uuidv4 } from 'uuid';
import { PoolClient } from 'pg';
import { waitForDb, query, withTransaction } from './db/client';
import { waitForRabbitMQ, startConsumer, publishTask } from './queue/rabbitmq';
import { runMigrations } from './db/migrations';
import { WorkflowState, TaskType, TRANSITIONS, TERMINAL_STATES } from './orchestrator/stateMachine';
import * as activities from './activities/subscriptionActivities';
import type { QueueTask, WorkflowRow, ChargeResult } from './types';

const WORKER_ID = process.env.WORKER_ID ?? `worker-${process.pid}-${uuidv4().slice(0, 8)}`;
const LOCK_EXPIRY_SECONDS = 30;
const MAX_RETRY_ATTEMPTS = 5;

function retryDelayMs(attempt: number): number {
  return Math.pow(2, attempt) * 1000; // 2s, 4s, 8s, 16s, 32s
}

// ── Distributed lock (PostgreSQL) ─────────────────────────────────────────────
async function acquireLock(workflowId: string): Promise<boolean> {
  const result = await query(`
    INSERT INTO workflow_locks (workflow_id, locked_by, locked_at, expires_at)
    VALUES ($1, $2, NOW(), NOW() + INTERVAL '${LOCK_EXPIRY_SECONDS} seconds')
    ON CONFLICT (workflow_id) DO UPDATE
      SET locked_by  = EXCLUDED.locked_by,
          locked_at  = NOW(),
          expires_at = NOW() + INTERVAL '${LOCK_EXPIRY_SECONDS} seconds'
      WHERE workflow_locks.expires_at < NOW()
    RETURNING workflow_id
  `, [workflowId, WORKER_ID]);
  return (result.rowCount ?? 0) > 0;
}

async function releaseLock(workflowId: string): Promise<void> {
  await query(
    `DELETE FROM workflow_locks WHERE workflow_id = $1 AND locked_by = $2`,
    [workflowId, WORKER_ID],
  );
}

// ── Workflow event log ────────────────────────────────────────────────────────
async function insertEvent(client: PoolClient, workflowId: string, eventType: string, data: object) {
  await client.query(
    `INSERT INTO workflow_events (workflow_id, event_type, event_data) VALUES ($1, $2, $3)`,
    [workflowId, eventType, JSON.stringify(data)],
  );
}

// ── Activity dispatch ─────────────────────────────────────────────────────────
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

// ── Main task processor ───────────────────────────────────────────────────────
async function processTask(task: QueueTask): Promise<void> {
  const { taskId, workflowId, taskType, attempt } = task;
  const P = `[${WORKER_ID}]`;

  console.log(`${P} ← Received | type=${taskType} | workflow=${workflowId} | attempt=${attempt}`);

  // ── 1. Acquire workflow lock ─────────────────────────────────────────────────
  // Prevents two workers from processing the same workflow concurrently.
  // If we can't acquire the lock, a different worker already has this workflow.
  // RabbitMQ will redeliver when the other consumer's in-flight message is done.
  const locked = await acquireLock(workflowId);
  if (!locked) {
    console.log(`${P} ✗ Lock held by another worker — requeueing`);
    // Brief sleep so we don't spin tight if many workers are competing
    await new Promise((r) => setTimeout(r, 1000));
    await publishTask(task); // publish a fresh copy; caller will ACK the current one
    return;
  }

  try {
    // ── 2. Validate workflow state ───────────────────────────────────────────
    const wfResult = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE id = $1',
      [workflowId],
    );
    if (!wfResult.rows.length) {
      console.log(`${P} ✗ Workflow ${workflowId} not found — discarding`);
      return;
    }
    const workflow = wfResult.rows[0];
    if (TERMINAL_STATES.has(workflow.state)) {
      console.log(`${P} ✗ Workflow is in terminal state ${workflow.state} — discarding`);
      return;
    }

    // ── 3. Idempotency check ─────────────────────────────────────────────────
    // RabbitMQ at-least-once delivery means the same task can arrive twice:
    //   - consumer crash after activity completed but before ACK
    //   - network blip causes RabbitMQ to redeliver
    // We check the idempotency_keys table before executing.
    const idempKey = `${workflowId}:${taskType}`;
    const idem = await query(
      'SELECT result FROM idempotency_keys WHERE key = $1',
      [idempKey],
    );
    if (idem.rows.length > 0) {
      console.log(`${P} ⊘ Idempotent skip | key=${idempKey}`);
      await ensureNextTask(task, idem.rows[0].result);
      return;
    }

    // ── 4. Mark activity as RUNNING ──────────────────────────────────────────
    const attemptId = uuidv4();
    await withTransaction(async (client) => {
      await client.query(
        `INSERT INTO activity_attempts (id, workflow_id, activity_type, attempt_number, status, started_at)
         VALUES ($1, $2, $3, $4, 'RUNNING', NOW())`,
        [attemptId, workflowId, taskType, attempt],
      );
      await insertEvent(client, workflowId, 'TASK_STARTED', { taskType, attempt, taskId });
    });

    // ── 5. Execute the activity ──────────────────────────────────────────────
    // This is the ONLY place a side effect happens (email sent, card charged).
    // The worker may crash here. On RabbitMQ redelivery, the idempotency check
    // above will prevent re-execution. Activities must still use idempotency
    // keys with external APIs (Stripe, email service) because they may receive
    // the HTTP request before we crash.
    let result: any = null;
    try {
      result = await executeActivity(task);
    } catch (err: any) {
      await handleFailure(task, attemptId, err);
      return;
    }

    // ── 6. Atomic commit: idempotency + state + event + attempt ──────────────
    const transition = TRANSITIONS[taskType as TaskType];
    const newState = transition.onComplete;
    const metaUpdate: Record<string, any> = {};
    if (taskType === TaskType.CHARGE_MONTHLY_FEE && result) {
      metaUpdate.chargeId = (result as ChargeResult).chargeId;
      metaUpdate.amount   = (result as ChargeResult).amount;
    }

    await withTransaction(async (client) => {
      await client.query(
        `INSERT INTO idempotency_keys (key, workflow_id, activity_type, result)
         VALUES ($1, $2, $3, $4) ON CONFLICT (key) DO NOTHING`,
        [idempKey, workflowId, taskType, JSON.stringify(result)],
      );
      await client.query(
        `UPDATE subscription_workflows
         SET state = $1, updated_at = NOW(), metadata = metadata || $2::jsonb
         WHERE id = $3`,
        [newState, JSON.stringify(metaUpdate), workflowId],
      );
      await client.query(
        `UPDATE activity_attempts SET status='COMPLETED', completed_at=NOW(), result=$1 WHERE id=$2`,
        [JSON.stringify(result), attemptId],
      );
      await insertEvent(client, workflowId, 'TASK_COMPLETED', { taskType, attempt, newState, result });

      // After welcome email: set the trial timer (scheduler fires it)
      if (newState === WorkflowState.WELCOME_EMAIL_SENT) {
        const trialSecs = parseInt(process.env.TRIAL_PERIOD_SECONDS ?? '30', 10);
        await client.query(
          `UPDATE subscription_workflows
           SET state = $1, trial_end_at = NOW() + ($2 || ' seconds')::INTERVAL
           WHERE id = $3`,
          [WorkflowState.WAITING_FOR_TRIAL_END, trialSecs, workflowId],
        );
        await insertEvent(client, workflowId, 'TIMER_STARTED', {
          trialSeconds: trialSecs,
          note: 'Scheduler will publish CHARGE_MONTHLY_FEE when trial_end_at passes',
        });
      }
    });

    const displayState = newState === WorkflowState.WELCOME_EMAIL_SENT
      ? WorkflowState.WAITING_FOR_TRIAL_END
      : newState;
    console.log(`${P} ✓ Done | type=${taskType} | state→${displayState}`);

    // ── 7. Publish next task to RabbitMQ ─────────────────────────────────────
    // This happens AFTER the DB commit. If RabbitMQ is down here, the workflow
    // gets stuck — the reconciler detects it and republishes via scheduled_tasks.
    // This is the unavoidable dual-write problem in any system that separates
    // state store from message queue. Temporal eliminates it because both live
    // in the same system.
    if (transition.nextTask && newState !== WorkflowState.WAITING_FOR_TRIAL_END) {
      await publishTask({
        taskId: uuidv4(),
        workflowId,
        customerId: task.customerId,
        taskType: transition.nextTask,
        attempt: 1,
        context: { ...task.context, ...metaUpdate },
      });
    }

  } finally {
    await releaseLock(workflowId);
  }
}

// ── Failure handling: retry backoff via scheduled_tasks ───────────────────────
async function handleFailure(task: QueueTask, attemptId: string, err: Error): Promise<void> {
  const { workflowId, taskType, attempt } = task;
  console.log(`[${WORKER_ID}] ✗ Activity failed | type=${taskType} | attempt=${attempt}/${MAX_RETRY_ATTEMPTS} | error=${err.message}`);

  await withTransaction(async (client) => {
    await client.query(
      `UPDATE activity_attempts SET status='FAILED', completed_at=NOW(), error_message=$1 WHERE id=$2`,
      [err.message, attemptId],
    );
    await insertEvent(client, workflowId, 'TASK_FAILED', { taskType, attempt, error: err.message });
  });

  if (attempt >= MAX_RETRY_ATTEMPTS) {
    console.log(`[${WORKER_ID}] 💀 Dead-lettering | type=${taskType} | exhausted ${MAX_RETRY_ATTEMPTS} attempts`);
    await withTransaction(async (client) => {
      await client.query(
        `INSERT INTO dead_letter_tasks (id, workflow_id, activity_type, payload, error_message, retry_count)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [uuidv4(), workflowId, taskType, JSON.stringify(task), err.message, attempt],
      );
      await client.query(
        `UPDATE subscription_workflows SET state=$1, updated_at=NOW() WHERE id=$2`,
        [WorkflowState.FAILED, workflowId],
      );
      await insertEvent(client, workflowId, 'WORKFLOW_FAILED', {
        reason: `${taskType} exhausted retries`,
        error: err.message,
      });
    });
    return;
  }

  // Schedule retry: INSERT into scheduled_tasks with execute_after = backoff delay.
  // The scheduler (separate process) publishes it to RabbitMQ when the time comes.
  // This replaces the old ZADD to Redis sorted set.
  const delayMs = retryDelayMs(attempt);
  const executeAfter = new Date(Date.now() + delayMs).toISOString();
  await query(
    `INSERT INTO scheduled_tasks (id, workflow_id, customer_id, task_type, execute_after, attempt, context)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [uuidv4(), workflowId, task.customerId, taskType, executeAfter, attempt + 1, JSON.stringify(task.context ?? {})],
  );
  console.log(`[${WORKER_ID}] ↺ Retry scheduled | type=${taskType} | attempt=${attempt + 1} | delay=${delayMs / 1000}s | via scheduled_tasks`);
}

// ── After idempotent skip: make sure next task is published ───────────────────
async function ensureNextTask(task: QueueTask, cachedResult: any): Promise<void> {
  const transition = TRANSITIONS[task.taskType as TaskType];
  if (!transition.nextTask) return;
  const wf = await query<WorkflowRow>(
    'SELECT * FROM subscription_workflows WHERE id = $1', [task.workflowId]
  );
  if (!wf.rows.length) return;
  const current = wf.rows[0];
  if (current.state === transition.onComplete) {
    await publishTask({
      taskId: uuidv4(),
      workflowId: task.workflowId,
      customerId: task.customerId,
      taskType: transition.nextTask,
      attempt: 1,
      context: { ...task.context, ...current.metadata },
    });
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────
async function main() {
  console.log(`[${WORKER_ID}] DIY Worker starting`);
  console.log(`[${WORKER_ID}] DATABASE_URL: ${process.env.DATABASE_URL}`);
  console.log(`[${WORKER_ID}] RABBITMQ_URL: ${process.env.RABBITMQ_URL}`);
  console.log(`[${WORKER_ID}] SIMULATE_CHARGE_FAILURE: ${process.env.SIMULATE_CHARGE_FAILURE}`);

  await waitForDb();
  await runMigrations();
  await waitForRabbitMQ();

  console.log(`[${WORKER_ID}] ✓ Ready`);
  console.log(`[${WORKER_ID}] Kill this process — RabbitMQ will redeliver unacked messages to another worker`);

  await startConsumer(async (task, ack, nack) => {
    try {
      await processTask(task);
      // ACK tells RabbitMQ we handled it — remove from queue
      ack();
    } catch (err: any) {
      console.error(`[${WORKER_ID}] Unhandled error in processTask:`, err.message);
      // ACK even on unhandled error so we don't loop forever on a poison message.
      // The reconciler will re-enqueue if the workflow is genuinely stuck.
      ack();
    }
  });

  // Keep process alive — the consumer runs on amqplib's event loop
  await new Promise(() => {});
}

main().catch((err) => {
  console.error('[worker] Fatal:', err);
  process.exit(1);
});
