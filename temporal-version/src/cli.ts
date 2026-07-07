#!/usr/bin/env ts-node
// ─────────────────────────────────────────────────────────────────────────────
// Temporal CLI
//
// Usage:
//   npx ts-node src/cli.ts start  <customerId>
//   npx ts-node src/cli.ts cancel <customerId> [reason]
//   npx ts-node src/cli.ts status <customerId>
//   npx ts-node src/cli.ts history <customerId>
//
// Or if API is running (docker compose):
//   Uses the HTTP API at http://localhost:3000
// ─────────────────────────────────────────────────────────────────────────────

import { Client, Connection, WorkflowNotFoundError } from '@temporalio/client';
import { subscriptionWorkflow, cancelSubscriptionSignal } from './workflows/subscriptionWorkflow';

const TEMPORAL_ADDRESS = process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
const TEMPORAL_NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? 'default';
const TASK_QUEUE = process.env.TASK_QUEUE ?? 'subscription-workflows';

const [,, command, customerId, ...rest] = process.argv;

function usage() {
  console.log(`
Temporal Subscription CLI

Commands:
  start  <customerId>            Start a subscription workflow
  cancel <customerId> [reason]   Send cancellation signal
  status <customerId>            Show workflow status
  history <customerId>           List workflow history events

Examples:
  npx ts-node src/cli.ts start customer-123
  npx ts-node src/cli.ts cancel customer-123 "switched-plans"
  npx ts-node src/cli.ts status customer-123
  npx ts-node src/cli.ts history customer-123
`);
}

async function main() {
  if (!command || !customerId) {
    usage();
    process.exit(1);
  }

  const connection = await Connection.connect({ address: TEMPORAL_ADDRESS });
  const client = new Client({ connection, namespace: TEMPORAL_NAMESPACE });
  const workflowId = `subscription-${customerId}`;

  switch (command) {
    case 'start': {
      console.log(`Starting subscription workflow for ${customerId}...`);
      const handle = await client.workflow.start(subscriptionWorkflow, {
        args: [customerId],
        taskQueue: TASK_QUEUE,
        workflowId,
        workflowIdReusePolicy: 'ALLOW_DUPLICATE_FAILED_ONLY',
      });
      console.log(`✓ Workflow started`);
      console.log(`  Workflow ID : ${handle.workflowId}`);
      console.log(`  Run ID      : ${handle.firstExecutionRunId}`);
      console.log(`  UI          : http://localhost:8080/namespaces/default/workflows/${handle.workflowId}`);
      break;
    }

    case 'cancel': {
      const reason = rest[0] ?? 'user-requested';
      console.log(`Sending cancellation signal to ${workflowId} (reason: ${reason})...`);
      const handle = client.workflow.getHandle(workflowId);
      await handle.signal(cancelSubscriptionSignal, { reason });
      console.log(`✓ Cancellation signal sent`);
      break;
    }

    case 'status': {
      const handle = client.workflow.getHandle(workflowId);
      const desc = await handle.describe();
      console.log(`Workflow Status for ${workflowId}:`);
      console.log(`  Status    : ${desc.status.name}`);
      console.log(`  Start     : ${desc.startTime?.toISOString()}`);
      console.log(`  Close     : ${desc.closeTime?.toISOString() ?? '(still running)'}`);
      console.log(`  Run ID    : ${desc.runId}`);
      break;
    }

    case 'history': {
      const handle = client.workflow.getHandle(workflowId);
      console.log(`Workflow History for ${workflowId}:\n`);
      const history = await handle.fetchHistory();
      const events = history.events ?? [];
      let eventCount = 0;
      for (const event of events) {
        eventCount++;
        const eventType = event.eventType?.toString().replace('EVENT_TYPE_', '') ?? 'UNKNOWN';
        const ts = event.eventTime ? new Date(Number(event.eventTime.seconds) * 1000).toISOString() : '';
        console.log(`  ${String(eventCount).padStart(3, ' ')}. [${ts}] ${eventType}`);
      }
      console.log(`\nTotal events: ${eventCount}`);
      console.log(`\nTip: Open the Temporal UI for a visual history:`);
      console.log(`     http://localhost:8080/namespaces/default/workflows/${workflowId}`);
      break;
    }

    default:
      console.error(`Unknown command: ${command}`);
      usage();
      process.exit(1);
  }

  await connection.close();
}

main().catch((err) => {
  if (err instanceof WorkflowNotFoundError) {
    console.error(`No workflow found for customer ID: ${customerId}`);
    console.error(`Make sure you've started the workflow first.`);
  } else {
    console.error('Error:', err.message);
  }
  process.exit(1);
});
