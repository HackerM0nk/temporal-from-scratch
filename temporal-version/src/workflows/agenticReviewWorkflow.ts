import {
  CancellationScope,
  condition,
  continueAsNew,
  defineQuery,
  defineSignal,
  log,
  proxyActivities,
  setHandler,
} from '@temporalio/workflow';
import type * as AgentActivities from '../activities/agenticReviewActivities';
import type {
  AgenticReviewInput,
  AgenticReviewStatus,
  CommitEvent,
  SpecialistResult,
} from '../shared/agenticTypes';

export const newCommitSignal = defineSignal<[CommitEvent]>('newCommit');
export const closePullRequestSignal = defineSignal<[]>('closePullRequest');
export const reviewStatusQuery = defineQuery<AgenticReviewStatus>('reviewStatus');

const shortActivities = proxyActivities<typeof AgentActivities>({
  startToCloseTimeout: '1 minute',
  retry: {
    maximumAttempts: 5,
    initialInterval: '1 second',
    backoffCoefficient: 2,
  },
});

const agentActivities = proxyActivities<typeof AgentActivities>({
  startToCloseTimeout: '5 minutes',
  heartbeatTimeout: '10 seconds',
  retry: {
    maximumAttempts: 3,
    initialInterval: '2 seconds',
    backoffCoefficient: 2,
  },
});

/**
 * One long-lived Workflow Execution represents one pull request lifecycle.
 * Flue owns each bounded specialist submission. Temporal owns generations,
 * supersession, timers, cancellation, fan-out, and guarded publication.
 */
export async function agenticReviewWorkflow(input: AgenticReviewInput): Promise<void> {
  const quietPeriodMs = input.quietPeriodMs ?? 90_000;
  const maxGenerationsPerRun = input.maxGenerationsPerRun ?? 20;

  let latestCommit = input.initialCommit;
  let reviewedHeadSha = input.reviewedHeadSha;
  let generation = input.generation ?? 0;
  let generationsInThisRun = 0;
  let closed = false;
  let phase: AgenticReviewStatus['phase'] = 'DEBOUNCING';
  let activeSpecialists: AgenticReviewStatus['activeSpecialists'] = [];
  let openFindingIds = new Set(input.openFindingIds ?? []);
  const processedEventIds = new Set(input.processedEventIds ?? [input.initialCommit.eventId]);

  // Handlers are synchronous. They mutate only deterministic Workflow state.
  setHandler(newCommitSignal, (event) => {
    if (processedEventIds.has(event.eventId)) return;
    processedEventIds.add(event.eventId);
    latestCommit = event;
    log.info('new commit admitted', { eventId: event.eventId, headSha: event.headSha });
  });
  setHandler(closePullRequestSignal, () => {
    closed = true;
  });
  setHandler(reviewStatusQuery, () => ({
    repository: input.repository,
    pullRequestNumber: input.pullRequestNumber,
    lifecycle: closed ? 'CLOSED' : 'OPEN',
    phase,
    latestHeadSha: latestCommit.headSha,
    reviewedHeadSha,
    generation,
    activeSpecialists,
    openFindingIds: [...openFindingIds],
  }));

  while (!closed) {
    await condition(() => closed || latestCommit.headSha !== reviewedHeadSha);
    if (closed) break;

    phase = 'DEBOUNCING';
    // Restart the quiet window whenever a newer commit arrives.
    while (!closed) {
      const candidate = latestCommit.headSha;
      const changedDuringWindow = await condition(
        () => closed || latestCommit.headSha !== candidate,
        quietPeriodMs,
      );
      if (closed) break;
      if (!changedDuringWindow) break;
    }
    if (closed) break;

    const target = latestCommit;
    generation += 1;
    generationsInThisRun += 1;
    const reviewRunId = [
      input.repository,
      `pr-${input.pullRequestNumber}`,
      `g${generation}`,
      target.headSha,
    ].join(':');

    phase = 'PREPARING';
    const snapshot = await shortActivities.prepareRepository({
      repository: input.repository,
      pullRequestNumber: input.pullRequestNumber,
      headSha: target.headSha,
      baseSha: target.baseSha,
    });
    const plan = await shortActivities.buildReviewPlan(snapshot);
    if (closed || latestCommit.headSha !== target.headSha) {
      log.info('generation superseded during preparation', {
        generation,
        reviewedHeadSha: target.headSha,
        latestHeadSha: latestCommit.headSha,
      });
      continue;
    }
    activeSpecialists = plan.specialists;

    // Dispatch and read are separate durable boundaries. Once the receipt is
    // recorded, a read retry reattaches instead of sending a second prompt.
    phase = 'REVIEWING';
    const admissions = await Promise.all(plan.specialists.map((specialist) =>
      shortActivities.dispatchFlueSpecialist({ reviewRunId, specialist, snapshot })
    ));

    const readScope = new CancellationScope();
    const resultsPromise = readScope.run(() => Promise.all(admissions.map((admission) =>
      agentActivities.readFlueSpecialist({ admission, snapshot })
    )));

    const outcome = await Promise.race([
      resultsPromise.then((results) => ({ kind: 'completed' as const, results })),
      condition(() => closed || latestCommit.headSha !== target.headSha)
        .then(() => ({ kind: 'superseded' as const })),
    ]);

    if (outcome.kind === 'superseded') {
      readScope.cancel();
      try {
        await resultsPromise;
      } catch {
        // Cancellation is expected. Completed Activity results remain in history.
      }
      activeSpecialists = [];
      log.info('generation superseded', {
        generation,
        reviewedHeadSha: target.headSha,
        latestHeadSha: latestCommit.headSha,
      });
      continue;
    }

    const specialistResults: SpecialistResult[] = outcome.results;
    activeSpecialists = [];
    phase = 'JUDGING';
    const findings = await shortActivities.judgeFindings({
      repository: input.repository,
      headSha: target.headSha,
      generation,
      results: specialistResults,
    });

    phase = 'PUBLISHING';
    const currentHead = await shortActivities.fetchCurrentHead({
      expectedHeadSha: target.headSha,
    });
    if (closed || currentHead !== target.headSha || latestCommit.headSha !== target.headSha) {
      log.info('stale publish prevented', {
        generation,
        reviewedHeadSha: target.headSha,
        closed,
        currentHead,
        latestHeadSha: latestCommit.headSha,
      });
      continue;
    }

    const published = await shortActivities.publishReview({
      repository: input.repository,
      pullRequestNumber: input.pullRequestNumber,
      generation,
      expectedHeadSha: target.headSha,
      findings,
    });
    openFindingIds = new Set(published.publishedFindingIds);
    reviewedHeadSha = target.headSha;
    phase = 'WAITING';

    // Bound history deliberately. Newer SDKs also expose a server-provided
    // Continue-As-New suggestion, but an explicit generation bound keeps this
    // teaching example compatible with the SDK version pinned by the repo.
    if (generationsInThisRun >= maxGenerationsPerRun) {
      await continueAsNew<typeof agenticReviewWorkflow>({
        ...input,
        initialCommit: latestCommit,
        generation,
        reviewedHeadSha,
        openFindingIds: [...openFindingIds],
        processedEventIds: [...processedEventIds].slice(-500),
      });
    }
  }

  phase = 'CLOSED';
}
