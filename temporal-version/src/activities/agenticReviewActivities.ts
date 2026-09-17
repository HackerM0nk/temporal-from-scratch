import { Context } from '@temporalio/activity';
import { createHash } from 'crypto';
import type {
  FlueAdmission,
  FindingCandidate,
  RepositorySnapshot,
  ReviewPlan,
  SpecialistName,
  SpecialistResult,
  StableFinding,
} from '../shared/agenticTypes';

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Demo Activity. Production code should attach a content-addressed PV/PVC or
 * object-store snapshot and return references, never repository contents.
 */
export async function prepareRepository(input: {
  repository: string;
  pullRequestNumber: number;
  headSha: string;
  baseSha: string;
}): Promise<RepositorySnapshot> {
  await delay(150);
  const prefix = `s3://cerberus-runs/${input.repository}/${input.pullRequestNumber}/${input.headSha}`;
  return {
    headSha: input.headSha,
    mergeBase: input.baseSha,
    snapshotUri: `${prefix}/repository.tar.zst`,
    contextManifestUri: `${prefix}/context.json`,
    changedFiles: ['billing/views.ts', 'auth/permissions.ts', 'config/example.env'],
  };
}

/**
 * This is an Activity because a real router may call a model. Returning a
 * closed schema keeps nondeterministic reasoning outside Workflow code.
 */
export async function buildReviewPlan(snapshot: RepositorySnapshot): Promise<ReviewPlan> {
  const specialists = new Set<SpecialistName>(['authorization']);
  if (snapshot.changedFiles.some((file) => file.includes('views'))) specialists.add('injection');
  if (snapshot.changedFiles.some((file) => file.includes('env'))) specialists.add('secrets');
  return {
    planVersion: 'security-v1',
    specialists: [...specialists],
    maxModelCalls: 24,
    maxWallClockSeconds: 240,
  };
}

/**
 * First half of the Flue boundary. A real implementation calls dispatch/send
 * and returns immediately after admission. This Activity can execute more than
 * once, so production code must use Flue's create-only admission (`uid: null`)
 * or an equivalent idempotent lookup keyed by the stable conversation identity.
 */
export async function dispatchFlueSpecialist(input: {
  reviewRunId: string;
  specialist: SpecialistName;
  snapshot: RepositorySnapshot;
}): Promise<FlueAdmission> {
  await delay(50);
  const conversationId = `${input.reviewRunId}:${input.specialist}`;
  return {
    conversationId,
    submissionId: `submission:${conversationId}`,
    receipt: `flue-receipt:${conversationId}`,
    specialist: input.specialist,
    headSha: input.snapshot.headSha,
  };
}

/**
 * Second half of the Flue boundary. On retry, production code calls
 * read(receipt) and reattaches to the admitted submission instead of prompting
 * the agent again. Heartbeats expose progress and deliver cancellation.
 */
export async function readFlueSpecialist(input: {
  admission: FlueAdmission;
  snapshot: RepositorySnapshot;
}): Promise<SpecialistResult> {
  const ctx = Context.current();
  for (let step = 1; step <= 4; step += 1) {
    ctx.heartbeat({
      receipt: input.admission.receipt,
      completedSteps: step - 1,
      totalSteps: 4,
    });
    await delay(125);
  }

  const findings: FindingCandidate[] = input.admission.specialist === 'authorization'
    ? [{
        specialist: 'authorization',
        family: 'cross_tenant_authorization',
        symbol: 'BillingView.getInvoice',
        location: 'billing/views.ts:42',
        rootCause: 'Invoice lookup is not bound to the authenticated company.',
        evidence: 'request.invoiceId -> global lookup -> response',
        severity: 'HIGH',
      }]
    : [];

  const prefix = `s3://cerberus-runs/agent-traces/${input.admission.conversationId}`;
  return {
    admission: input.admission,
    findings,
    promptUri: `${prefix}/prompt.json`,
    transcriptUri: `${prefix}/transcript.jsonl`,
    model: 'simulated-flue-agent',
    inputTokens: 1_000,
    outputTokens: 250,
  };
}

export async function judgeFindings(input: {
  repository: string;
  headSha: string;
  generation: number;
  results: SpecialistResult[];
}): Promise<StableFinding[]> {
  const unique = new Map<string, StableFinding>();
  for (const result of input.results) {
    for (const candidate of result.findings) {
      const identity = [
        input.repository,
        candidate.family,
        candidate.symbol,
        candidate.rootCause.trim().toLowerCase(),
      ].join('|');
      const findingId = createHash('sha256').update(identity).digest('hex').slice(0, 16);
      unique.set(findingId, {
        ...candidate,
        findingId,
        evidenceUri: `s3://cerberus-runs/findings/${findingId}/g${input.generation}.json`,
      });
    }
  }
  return [...unique.values()];
}

/** Demo hook for the final freshness check. */
export async function fetchCurrentHead(input: { expectedHeadSha: string }): Promise<string> {
  return process.env.SIMULATE_NEW_HEAD_SHA ?? input.expectedHeadSha;
}

/**
 * Production implementation must conditionally write against expectedHeadSha
 * and reconcile by stable findingId. Retrying should converge, not duplicate.
 */
export async function publishReview(input: {
  repository: string;
  pullRequestNumber: number;
  generation: number;
  expectedHeadSha: string;
  findings: StableFinding[];
}): Promise<{ publishedFindingIds: string[] }> {
  const idempotencyKey = `${input.repository}:${input.pullRequestNumber}:g${input.generation}:${input.expectedHeadSha}`;
  console.log('[publishReview] reconciled', {
    idempotencyKey,
    findingIds: input.findings.map((finding) => finding.findingId),
  });
  return { publishedFindingIds: input.findings.map((finding) => finding.findingId) };
}
