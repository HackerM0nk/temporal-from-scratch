import { Connection, Client } from '@temporalio/client';
import {
  agenticReviewWorkflow,
  closePullRequestSignal,
  newCommitSignal,
  reviewStatusQuery,
} from './workflows/agenticReviewWorkflow';
import type { CommitEvent } from './shared/agenticTypes';

const address = process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
const taskQueue = process.env.TASK_QUEUE ?? 'subscription-workflows';

function usage(): never {
  console.error('Usage:');
  console.error('  agenticCli start <repo> <pr> <headSha> <baseSha>');
  console.error('  agenticCli commit <repo> <pr> <headSha> <baseSha>');
  console.error('  agenticCli status <repo> <pr>');
  console.error('  agenticCli close <repo> <pr>');
  process.exit(1);
}

function workflowId(repository: string, pr: number): string {
  return `cerberus:${repository}:pr-${pr}`;
}

function commit(headSha: string, baseSha: string): CommitEvent {
  return {
    eventId: `demo:${headSha}`,
    headSha,
    baseSha,
    receivedAt: new Date().toISOString(),
  };
}

async function main() {
  const [command, repository, rawPr, headSha, baseSha] = process.argv.slice(2);
  if (!command || !repository || !rawPr) usage();
  const pullRequestNumber = Number(rawPr);
  const connection = await Connection.connect({ address });
  const client = new Client({ connection });
  const id = workflowId(repository, pullRequestNumber);

  if (command === 'start') {
    if (!headSha || !baseSha) usage();
    const event = commit(headSha, baseSha);
    await client.workflow.signalWithStart(agenticReviewWorkflow, {
      workflowId: id,
      taskQueue,
      args: [{
        repository,
        pullRequestNumber,
        initialCommit: event,
        quietPeriodMs: 5_000,
      }],
      signal: newCommitSignal,
      signalArgs: [event],
    });
    console.log(`started ${id}`);
  } else if (command === 'commit') {
    if (!headSha || !baseSha) usage();
    const event = commit(headSha, baseSha);
    // Signal-With-Start closes the webhook race: the same request either
    // starts the PR lifecycle or signals the already-running lifecycle.
    await client.workflow.signalWithStart(agenticReviewWorkflow, {
      workflowId: id,
      taskQueue,
      args: [{
        repository,
        pullRequestNumber,
        initialCommit: event,
        quietPeriodMs: 5_000,
      }],
      signal: newCommitSignal,
      signalArgs: [event],
    });
    console.log(`signalled ${id} with ${headSha}`);
  } else if (command === 'status') {
    console.log(await client.workflow.getHandle(id).query(reviewStatusQuery));
  } else if (command === 'close') {
    await client.workflow.getHandle(id).signal(closePullRequestSignal);
    console.log(`closed ${id}`);
  } else {
    usage();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
