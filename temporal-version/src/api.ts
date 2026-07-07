// ─────────────────────────────────────────────────────────────────────────────
// Temporal Starter API
//
// HTTP API for starting and managing subscription workflows.
// The CLI wraps this API for local development convenience.
// ─────────────────────────────────────────────────────────────────────────────

import express from 'express';
import { Client, Connection, WorkflowNotFoundError } from '@temporalio/client';
import { subscriptionWorkflow, cancelSubscriptionSignal } from './workflows/subscriptionWorkflow';

const TEMPORAL_ADDRESS = process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
const TEMPORAL_NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? 'default';
const TASK_QUEUE = process.env.TASK_QUEUE ?? 'subscription-workflows';
const PORT = parseInt(process.env.PORT ?? '3000', 10);

async function createClient(): Promise<Client> {
  const connection = await Connection.connect({ address: TEMPORAL_ADDRESS });
  return new Client({ connection, namespace: TEMPORAL_NAMESPACE });
}

async function connectWithRetry(maxAttempts = 30): Promise<Client> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const client = await createClient();
      console.log(`[temporal-api] Connected to Temporal at ${TEMPORAL_ADDRESS}`);
      return client;
    } catch (err) {
      console.log(`[temporal-api] Waiting for Temporal... (attempt ${attempt}/${maxAttempts})`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('Could not connect to Temporal');
}

async function main() {
  const client = await connectWithRetry();
  const app = express();
  app.use(express.json());

  // POST /workflows/subscription
  // Body: { customerId: string }
  app.post('/workflows/subscription', async (req, res) => {
    const { customerId } = req.body;
    if (!customerId) {
      return res.status(400).json({ error: 'customerId is required' });
    }

    const workflowId = `subscription-${customerId}`;
    console.log(`[temporal-api] Starting workflow | workflowId=${workflowId}`);

    try {
      const handle = await client.workflow.start(subscriptionWorkflow, {
        args: [customerId],
        taskQueue: TASK_QUEUE,
        workflowId,
        // Prevent accidental duplicate starts for the same customer
        workflowIdReusePolicy: 'ALLOW_DUPLICATE_FAILED_ONLY',
      });

      console.log(`[temporal-api] ✓ Workflow started | workflowId=${workflowId} | runId=${handle.firstExecutionRunId}`);
      return res.status(201).json({
        workflowId: handle.workflowId,
        runId: handle.firstExecutionRunId,
        message: `Subscription workflow started for ${customerId}`,
      });
    } catch (err: any) {
      if (err.message?.includes('already exists')) {
        return res.status(409).json({ error: `Workflow for ${customerId} is already running` });
      }
      console.error('[temporal-api] Error starting workflow:', err);
      return res.status(500).json({ error: err.message });
    }
  });

  // POST /workflows/subscription/:customerId/cancel
  // Body: { reason?: string }
  app.post('/workflows/subscription/:customerId/cancel', async (req, res) => {
    const { customerId } = req.params;
    const reason = req.body.reason ?? 'user-requested';
    const workflowId = `subscription-${customerId}`;

    console.log(`[temporal-api] Sending cancel signal | workflowId=${workflowId} | reason=${reason}`);

    try {
      const handle = client.workflow.getHandle(workflowId);
      await handle.signal(cancelSubscriptionSignal, { reason });

      console.log(`[temporal-api] ✓ Cancel signal sent | workflowId=${workflowId}`);
      return res.json({ message: `Cancellation signal sent to ${workflowId}` });
    } catch (err: any) {
      if (err instanceof WorkflowNotFoundError) {
        return res.status(404).json({ error: `No active workflow found for ${customerId}` });
      }
      console.error('[temporal-api] Error cancelling workflow:', err);
      return res.status(500).json({ error: err.message });
    }
  });

  // GET /workflows/subscription/:customerId
  app.get('/workflows/subscription/:customerId', async (req, res) => {
    const { customerId } = req.params;
    const workflowId = `subscription-${customerId}`;

    try {
      const handle = client.workflow.getHandle(workflowId);
      const description = await handle.describe();

      return res.json({
        workflowId: description.workflowId,
        runId: description.runId,
        status: description.status.name,
        startTime: description.startTime,
        closeTime: description.closeTime,
      });
    } catch (err: any) {
      if (err instanceof WorkflowNotFoundError) {
        return res.status(404).json({ error: `No workflow found for ${customerId}` });
      }
      return res.status(500).json({ error: err.message });
    }
  });

  app.listen(PORT, () => {
    console.log(`[temporal-api] HTTP API listening on port ${PORT}`);
    console.log(`[temporal-api] POST   /workflows/subscription`);
    console.log(`[temporal-api] POST   /workflows/subscription/:customerId/cancel`);
    console.log(`[temporal-api] GET    /workflows/subscription/:customerId`);
  });
}

main().catch((err) => {
  console.error('[temporal-api] Fatal error:', err);
  process.exit(1);
});
