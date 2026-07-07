#!/usr/bin/env ts-node
// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow CLI
//
// Wraps the HTTP API at http://localhost:3001
//
// Usage:
//   npx ts-node src/cli.ts start  <customerId>
//   npx ts-node src/cli.ts cancel <customerId> [reason]
//   npx ts-node src/cli.ts status <customerId>
//   npx ts-node src/cli.ts history <customerId>
//   npx ts-node src/cli.ts inspect             # show all table contents
// ─────────────────────────────────────────────────────────────────────────────

import http from 'http';

const API_BASE = process.env.DIY_API_URL ?? 'http://localhost:3001';

const [,, command, customerId, ...rest] = process.argv;

function usage() {
  console.log(`
DIY Subscription Workflow CLI

Commands:
  start  <customerId>            Start a subscription workflow
  cancel <customerId> [reason]   Request cancellation
  status <customerId>            Show workflow state
  history <customerId>           Show events + activity attempts
  inspect                        Show queue stats

Examples:
  npx ts-node src/cli.ts start customer-456
  npx ts-node src/cli.ts cancel customer-456 "switched-plans"
  npx ts-node src/cli.ts status customer-456
  npx ts-node src/cli.ts history customer-456
`);
}

async function httpRequest(
  method: string,
  path: string,
  body?: object,
): Promise<any> {
  return new Promise((resolve, reject) => {
    const url = new URL(path, API_BASE);
    const bodyStr = body ? JSON.stringify(body) : undefined;

    const options: http.RequestOptions = {
      hostname: url.hostname,
      port: url.port || 3001,
      path: url.pathname,
      method,
      headers: {
        'Content-Type': 'application/json',
        ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
      },
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (res.statusCode && res.statusCode >= 400) {
            reject(new Error(parsed.error ?? `HTTP ${res.statusCode}`));
          } else {
            resolve(parsed);
          }
        } catch {
          reject(new Error(`Invalid JSON response: ${data}`));
        }
      });
    });

    req.on('error', reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

function printTable(rows: any[]) {
  if (rows.length === 0) { console.log('  (none)'); return; }
  console.log(JSON.stringify(rows, null, 2));
}

async function main() {
  if (!command) { usage(); process.exit(1); }

  switch (command) {
    case 'start': {
      if (!customerId) { console.error('customerId required'); process.exit(1); }
      const result = await httpRequest('POST', '/workflows/subscription', { customerId });
      console.log('✓ Workflow started');
      console.log(`  Workflow ID : ${result.workflowId}`);
      console.log(`  State       : ${result.state}`);
      break;
    }

    case 'cancel': {
      if (!customerId) { console.error('customerId required'); process.exit(1); }
      const reason = rest[0] ?? 'user-requested';
      const result = await httpRequest('POST', `/workflows/subscription/${customerId}/cancel`, { reason });
      console.log('✓ Cancellation requested');
      console.log(`  State: ${result.state}`);
      break;
    }

    case 'status': {
      if (!customerId) { console.error('customerId required'); process.exit(1); }
      const result = await httpRequest('GET', `/workflows/subscription/${customerId}`);
      console.log(`Workflow Status for ${customerId}:`);
      console.log(`  ID          : ${result.id}`);
      console.log(`  State       : ${result.state}`);
      console.log(`  Trial Ends  : ${result.trial_end_at ?? '(not started)'}`);
      console.log(`  Cancel Reason: ${result.cancellation_reason ?? '(none)'}`);
      console.log(`  Metadata    : ${JSON.stringify(result.metadata)}`);
      console.log(`  Created     : ${result.created_at}`);
      console.log(`  Updated     : ${result.updated_at}`);
      break;
    }

    case 'history': {
      if (!customerId) { console.error('customerId required'); process.exit(1); }
      const result = await httpRequest('GET', `/workflows/subscription/${customerId}/history`);
      console.log(`\n── Workflow ───────────────────────────────────────────`);
      console.log(`  State: ${result.workflow.state}`);

      console.log(`\n── Events (workflow_events table) ─────────────────────`);
      for (const ev of result.events) {
        console.log(`  ${String(ev.id).padStart(4)}. [${ev.created_at}] ${ev.event_type}`);
        if (Object.keys(ev.event_data).length > 0) {
          console.log(`         ${JSON.stringify(ev.event_data)}`);
        }
      }

      console.log(`\n── Activity Attempts (activity_attempts table) ─────────`);
      for (const a of result.activityAttempts) {
        const status = a.status === 'COMPLETED' ? '✓' : a.status === 'FAILED' ? '✗' : '⋯';
        console.log(`  ${status} ${a.activity_type} (attempt ${a.attempt_number}) — ${a.status}`);
        if (a.error_message) console.log(`    Error: ${a.error_message}`);
      }
      break;
    }

    case 'inspect': {
      const stats = await httpRequest('GET', '/queue/stats');
      console.log('\n── RabbitMQ Queue Stats ───────────────────────────────');
      console.log(`  Messages ready    : ${stats.ready}`);
      console.log(`  Messages unacked  : ${stats.unacked}`);
      console.log(`  Management UI     : http://localhost:15672  (guest/guest)`);
      break;
    }

    default:
      console.error(`Unknown command: ${command}`);
      usage();
      process.exit(1);
  }
}

main().catch((err) => {
  console.error('Error:', err.message);
  console.error('Is the API running? Start it with: npm run start:api');
  process.exit(1);
});
