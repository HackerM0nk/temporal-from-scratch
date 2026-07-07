// ─────────────────────────────────────────────────────────────────────────────
// Temporal Worker
//
// The worker is responsible for:
//   1. Polling Temporal server for workflow tasks (to replay/advance workflows)
//   2. Polling Temporal server for activity tasks (to execute activities)
//   3. Executing workflow code in a deterministic sandbox (V8 isolate)
//   4. Executing activity code in normal Node.js
//
// KEY CONCEPT — Worker crash:
//   If this process dies, Temporal detects the missed heartbeat and reassigns
//   in-progress tasks to another worker. No workflow state is lost because
//   all state lives in Temporal's database, not in this process's memory.
//
// Try it: start a subscription, then kill this process (Ctrl+C), then restart.
// The workflow will resume from exactly where it left off.
// ─────────────────────────────────────────────────────────────────────────────

import { Worker, NativeConnection } from '@temporalio/worker';
import * as activities from './activities/subscriptionActivities';
import path from 'path';

const TEMPORAL_ADDRESS = process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
const TEMPORAL_NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? 'default';
const TASK_QUEUE = process.env.TASK_QUEUE ?? 'subscription-workflows';

async function connectWithRetry(address: string, maxAttempts = 30): Promise<NativeConnection> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const conn = await NativeConnection.connect({ address });
      console.log(`[worker] Connected to Temporal at ${address}`);
      return conn;
    } catch (err) {
      console.log(`[worker] Waiting for Temporal server... (attempt ${attempt}/${maxAttempts})`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error(`Could not connect to Temporal at ${address} after ${maxAttempts} attempts`);
}

async function main() {
  console.log(`[worker] Starting Temporal worker`);
  console.log(`[worker] Address:    ${TEMPORAL_ADDRESS}`);
  console.log(`[worker] Namespace:  ${TEMPORAL_NAMESPACE}`);
  console.log(`[worker] Task Queue: ${TASK_QUEUE}`);
  console.log(`[worker] SIMULATE_CHARGE_FAILURE: ${process.env.SIMULATE_CHARGE_FAILURE}`);
  console.log(`[worker] TRIAL_PERIOD_MS: ${process.env.TRIAL_PERIOD_MS ?? '30000'}`);

  const connection = await connectWithRetry(TEMPORAL_ADDRESS);

  const worker = await Worker.create({
    connection,
    namespace: TEMPORAL_NAMESPACE,
    taskQueue: TASK_QUEUE,

    // workflowsPath tells the Worker where to find workflow definitions.
    // The SDK bundles this file with webpack into a deterministic V8 sandbox.
    // We must pass the path WITH extension — the bundler does a statSync on
    // the exact path. Detect ts-node vs compiled runtime by checking __filename.
    workflowsPath: path.resolve(
      __dirname,
      `./workflows/subscriptionWorkflow${__filename.endsWith('.ts') ? '.ts' : '.js'}`,
    ),

    // Activities run in normal Node.js — just pass the module object.
    activities,
  });

  console.log(`[worker] Worker created — polling for tasks on queue "${TASK_QUEUE}"`);
  console.log(`[worker] Kill this process at any time — workflows will resume on restart`);

  // worker.run() is a long-running loop that polls for tasks.
  // It rejects if the worker encounters an unrecoverable error.
  await worker.run();
}

main().catch((err) => {
  console.error('[worker] Fatal error:', err);
  process.exit(1);
});
