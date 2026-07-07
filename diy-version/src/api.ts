import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { waitForDb, query, withTransaction } from './db/client';
import { waitForRabbitMQ, publishTask, queueDepth } from './queue/rabbitmq';
import { runMigrations } from './db/migrations';
import { WorkflowState, TaskType } from './orchestrator/stateMachine';
import type { QueueTask, WorkflowRow } from './types';

const PORT = parseInt(process.env.PORT ?? '3001', 10);

async function main() {
  await waitForDb();
  if (process.env.RUN_MIGRATIONS === 'true') await runMigrations();
  await waitForRabbitMQ();

  const app = express();
  app.use(express.json());

  // POST /workflows/subscription
  app.post('/workflows/subscription', async (req, res) => {
    const { customerId } = req.body;
    if (!customerId) return res.status(400).json({ error: 'customerId required' });

    const existing = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [customerId]
    );
    if (existing.rows.length) {
      const wf = existing.rows[0];
      const terminal = [WorkflowState.COMPLETED, WorkflowState.CANCELLED, WorkflowState.FAILED] as string[];
      if (!terminal.includes(wf.state)) {
        return res.status(409).json({ error: `Workflow for ${customerId} already active`, state: wf.state });
      }
    }

    const workflowId = uuidv4();
    console.log(`[api] Starting workflow | workflowId=${workflowId} | customer=${customerId}`);

    await withTransaction(async (client) => {
      await client.query(
        `INSERT INTO subscription_workflows (id, customer_id, state)
         VALUES ($1, $2, $3)
         ON CONFLICT (customer_id) DO UPDATE
           SET id=$1, state=$3, metadata='{}', trial_end_at=NULL, cancellation_reason=NULL, updated_at=NOW()
           WHERE subscription_workflows.state IN ('COMPLETED','CANCELLED','FAILED')`,
        [workflowId, customerId, WorkflowState.WELCOME_EMAIL_SCHEDULED],
      );
      await client.query(
        `INSERT INTO workflow_events (workflow_id, event_type, event_data) VALUES ($1, 'WORKFLOW_STARTED', $2)`,
        [workflowId, JSON.stringify({ customerId })],
      );
    });

    await publishTask({
      taskId: uuidv4(),
      workflowId,
      customerId,
      taskType: TaskType.SEND_WELCOME_EMAIL,
      attempt: 1,
    });

    return res.status(201).json({ workflowId, customerId, state: WorkflowState.WELCOME_EMAIL_SCHEDULED });
  });

  // POST /workflows/subscription/:customerId/cancel
  app.post('/workflows/subscription/:customerId/cancel', async (req, res) => {
    const { customerId } = req.params;
    const reason = req.body.reason ?? 'user-requested';

    const result = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [customerId]
    );
    if (!result.rows.length) return res.status(404).json({ error: `No workflow for ${customerId}` });
    const wf = result.rows[0];

    const terminal = [WorkflowState.COMPLETED, WorkflowState.CANCELLED, WorkflowState.FAILED] as string[];
    if (terminal.includes(wf.state)) {
      return res.status(409).json({ error: `Workflow is already in terminal state: ${wf.state}` });
    }

    console.log(`[api] Cancelling | workflowId=${wf.id} | reason=${reason}`);
    await withTransaction(async (client) => {
      await client.query(
        `UPDATE subscription_workflows SET state=$1, cancellation_reason=$2, updated_at=NOW() WHERE id=$3`,
        [WorkflowState.CANCELLATION_REQUESTED, reason, wf.id],
      );
      await client.query(
        `INSERT INTO workflow_events (workflow_id, event_type, event_data) VALUES ($1, 'CANCELLATION_REQUESTED', $2)`,
        [wf.id, JSON.stringify({ reason })],
      );
    });

    await publishTask({
      taskId: uuidv4(),
      workflowId: wf.id,
      customerId,
      taskType: TaskType.PROCESS_CANCELLATION,
      attempt: 1,
      context: { reason },
    });

    return res.json({ workflowId: wf.id, state: WorkflowState.CANCELLATION_REQUESTED });
  });

  // GET /workflows/subscription/:customerId
  app.get('/workflows/subscription/:customerId', async (req, res) => {
    const result = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [req.params.customerId]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    return res.json(result.rows[0]);
  });

  // GET /workflows/subscription/:customerId/history
  app.get('/workflows/subscription/:customerId/history', async (req, res) => {
    const wf = await query<WorkflowRow>(
      'SELECT * FROM subscription_workflows WHERE customer_id = $1', [req.params.customerId]
    );
    if (!wf.rows.length) return res.status(404).json({ error: 'Not found' });
    const id = wf.rows[0].id;
    const [events, attempts] = await Promise.all([
      query('SELECT * FROM workflow_events WHERE workflow_id=$1 ORDER BY id', [id]),
      query('SELECT * FROM activity_attempts WHERE workflow_id=$1 ORDER BY scheduled_at', [id]),
    ]);
    return res.json({ workflow: wf.rows[0], events: events.rows, activityAttempts: attempts.rows });
  });

  // GET /queue/stats
  app.get('/queue/stats', async (_req, res) => {
    const stats = await queueDepth();
    return res.json(stats);
  });

  app.listen(PORT, () => {
    console.log(`[api] Listening on :${PORT}`);
  });
}

main().catch((err) => { console.error('[api] Fatal:', err); process.exit(1); });
