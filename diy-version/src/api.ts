// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow API
//
// HTTP API for starting, cancelling, and inspecting DIY subscription workflows.
// ─────────────────────────────────────────────────────────────────────────────

import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { waitForDb, query, withTransaction } from './db/client';
import { waitForRedis, enqueueImmediate, queueStats } from './queue/redisQueue';
import { runMigrations } from './db/migrations';
import { WorkflowState, TaskType } from './orchestrator/stateMachine';
import type { QueueTask, WorkflowRow } from './types';

const PORT = parseInt(process.env.PORT ?? '3001', 10);

async function main() {
  await waitForDb();
  if (process.env.RUN_MIGRATIONS === 'true') {
    await runMigrations();
  }
  await waitForRedis();

  const app = express();
  app.use(express.json());

  // ── POST /workflows/subscription ──────────────────────────────────────────
  app.post('/workflows/subscription', async (req, res) => {
    const { customerId } = req.body;
    if (!customerId) return res.status(400).json({ error: 'customerId is required' });

    // Check for existing active workflow (idempotent start)
    const existing = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [customerId]
    );
    if (existing.rows.length > 0) {
      const wf = existing.rows[0];
      if (!([WorkflowState.COMPLETED, WorkflowState.CANCELLED, WorkflowState.FAILED] as string[]).includes(wf.state)) {
        return res.status(409).json({
          error: `Workflow for ${customerId} already running`,
          workflowId: wf.id,
          state: wf.state,
        });
      }
    }

    const workflowId = uuidv4();
    console.log(`[diy-api] Starting workflow | workflowId=${workflowId} | customerId=${customerId}`);

    // Create workflow record + enqueue first task in one transaction
    await withTransaction(async (client) => {
      await client.query(`
        INSERT INTO subscription_workflows (id, customer_id, state)
        VALUES ($1, $2, $3)
        ON CONFLICT (customer_id) DO UPDATE
          SET id = EXCLUDED.id, state = EXCLUDED.state, updated_at = NOW()
          WHERE subscription_workflows.state IN ('COMPLETED', 'CANCELLED', 'FAILED')
      `, [workflowId, customerId, WorkflowState.WELCOME_EMAIL_SCHEDULED]);

      await client.query(`
        INSERT INTO workflow_events (workflow_id, event_type, event_data)
        VALUES ($1, 'WORKFLOW_STARTED', $2)
      `, [workflowId, JSON.stringify({ customerId })]);
    });

    const task: QueueTask = {
      taskId: uuidv4(),
      workflowId,
      customerId,
      taskType: TaskType.SEND_WELCOME_EMAIL,
      attempt: 1,
    };

    await enqueueImmediate(task);

    console.log(`[diy-api] ✓ Workflow started | workflowId=${workflowId}`);
    return res.status(201).json({
      workflowId,
      customerId,
      state: WorkflowState.WELCOME_EMAIL_SCHEDULED,
      message: `Subscription workflow started for ${customerId}`,
    });
  });

  // ── POST /workflows/subscription/:customerId/cancel ───────────────────────
  app.post('/workflows/subscription/:customerId/cancel', async (req, res) => {
    const { customerId } = req.params;
    const reason = req.body.reason ?? 'user-requested';

    const result = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [customerId]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: `No workflow found for ${customerId}` });
    }

    const workflow = result.rows[0];

    // Cannot cancel a terminal workflow
    if ([WorkflowState.COMPLETED, WorkflowState.CANCELLED, WorkflowState.FAILED].includes(workflow.state as WorkflowState)) {
      return res.status(409).json({ error: `Workflow is in terminal state: ${workflow.state}` });
    }

    // KEY CONCEPT: This is the DIY equivalent of Temporal's signal mechanism.
    // We write directly to the database rather than delivering a message to
    // a running workflow instance. The worker reads the cancellation state
    // on its next wake-up.
    //
    // If the workflow is in WAITING_FOR_TRIAL_END, the scheduler won't fire
    // the charge task (it checks current state). The cancellation task gets
    // enqueued immediately instead.
    console.log(`[diy-api] Cancelling workflow | workflowId=${workflow.id} | reason=${reason}`);

    await withTransaction(async (client) => {
      await client.query(`
        UPDATE subscription_workflows
        SET state = $1,
            cancellation_reason = $2,
            updated_at = NOW()
        WHERE id = $3
      `, [WorkflowState.CANCELLATION_REQUESTED, reason, workflow.id]);

      await client.query(`
        INSERT INTO workflow_events (workflow_id, event_type, event_data)
        VALUES ($1, 'CANCELLATION_REQUESTED', $2)
      `, [workflow.id, JSON.stringify({ reason, requestedAt: new Date().toISOString() })]);
    });

    // Enqueue the cancellation task
    const task: QueueTask = {
      taskId: uuidv4(),
      workflowId: workflow.id,
      customerId,
      taskType: TaskType.PROCESS_CANCELLATION,
      attempt: 1,
      context: { reason },
    };
    await enqueueImmediate(task);

    console.log(`[diy-api] ✓ Cancellation requested | workflowId=${workflow.id}`);
    return res.json({
      workflowId: workflow.id,
      state: WorkflowState.CANCELLATION_REQUESTED,
      message: `Cancellation initiated for ${customerId}`,
    });
  });

  // ── GET /workflows/subscription/:customerId ───────────────────────────────
  app.get('/workflows/subscription/:customerId', async (req, res) => {
    const { customerId } = req.params;
    const wfResult = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [customerId]
    );
    if (wfResult.rows.length === 0) {
      return res.status(404).json({ error: `No workflow found for ${customerId}` });
    }
    return res.json(wfResult.rows[0]);
  });

  // ── GET /workflows/subscription/:customerId/history ───────────────────────
  app.get('/workflows/subscription/:customerId/history', async (req, res) => {
    const { customerId } = req.params;
    const wfResult = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [customerId]
    );
    if (wfResult.rows.length === 0) {
      return res.status(404).json({ error: `No workflow found for ${customerId}` });
    }
    const workflowId = wfResult.rows[0].id;
    const events = await query(
      'SELECT * FROM workflow_events WHERE workflow_id = $1 ORDER BY id',
      [workflowId]
    );
    const attempts = await query(
      'SELECT * FROM activity_attempts WHERE workflow_id = $1 ORDER BY scheduled_at',
      [workflowId]
    );
    return res.json({
      workflow: wfResult.rows[0],
      events: events.rows,
      activityAttempts: attempts.rows,
    });
  });

  // ── GET /queue/stats ──────────────────────────────────────────────────────
  app.get('/queue/stats', async (_req, res) => {
    const stats = await queueStats();
    return res.json(stats);
  });

  app.listen(PORT, () => {
    console.log(`[diy-api] HTTP API listening on port ${PORT}`);
    console.log(`[diy-api] POST   /workflows/subscription`);
    console.log(`[diy-api] POST   /workflows/subscription/:customerId/cancel`);
    console.log(`[diy-api] GET    /workflows/subscription/:customerId`);
    console.log(`[diy-api] GET    /workflows/subscription/:customerId/history`);
    console.log(`[diy-api] GET    /queue/stats`);
  });
}

main().catch((err) => {
  console.error('[diy-api] Fatal error:', err);
  process.exit(1);
});
