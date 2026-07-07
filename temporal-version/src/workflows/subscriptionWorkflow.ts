// ─────────────────────────────────────────────────────────────────────────────
// Subscription Workflow — Temporal Version
//
// This file runs inside Temporal's WORKFLOW SANDBOX — a deterministic V8
// isolate. Rules:
//   ✗ No Date.now() or Math.random() — use workflow.now() instead
//   ✗ No direct I/O (network, disk, timers) — use activities and sleep()
//   ✓ Only pure TypeScript + @temporalio/workflow imports
//
// KEY CONCEPT — Durability via Event Sourcing:
//   Every await in this function corresponds to an event in the workflow
//   history. If the worker crashes and restarts, Temporal REPLAYS the history
//   to restore the workflow's execution state — advancing each await point
//   without actually re-executing the underlying side effect.
//
//   Crash at step 3? On restart, steps 1-2 are replayed (instantly, from
//   history), and execution resumes at step 3. The external world is never
//   called twice for completed steps.
// ─────────────────────────────────────────────────────────────────────────────

import {
  proxyActivities,
  condition,
  defineSignal,
  setHandler,
  log,
  workflowInfo,
} from '@temporalio/workflow';

// Import TYPES only — workflow sandbox cannot import implementations
import type * as Activities from '../activities/subscriptionActivities';
import type {
  ChargeResult,
} from '../shared/types';

// ── Accelerated Time ──────────────────────────────────────────────────────────
// 30 seconds of real time represents 30 days in the simulation.
//
// KEY CONSTRAINT: Workflow code runs in a deterministic V8 sandbox.
// `process`, `Date.now()`, `Math.random()`, `setTimeout` are all FORBIDDEN.
// To make the trial duration configurable, pass it as a workflow argument
// or use a hardcoded constant. We hardcode here for clarity.
const TRIAL_PERIOD_MS = 30_000; // 30 seconds = 30 days (accelerated)

// ── Signal Definitions ────────────────────────────────────────────────────────
// Signals are typed messages that external code sends to a running workflow.
// They are stored durably in workflow history — if a signal arrives while the
// worker is down, it is delivered when the worker comes back up.
export const cancelSubscriptionSignal = defineSignal<[{ reason: string }]>('cancelSubscription');

// ── Activity Proxy ────────────────────────────────────────────────────────────
// proxyActivities() does NOT call the activities directly. It returns stub
// functions that, when called, schedule an activity task on the task queue.
// The workflow suspends at that await point until the activity completes.
//
// If an activity fails, Temporal retries it automatically per the retry policy
// defined here. The workflow code never sees transient failures.
const {
  sendWelcomeEmail,
  chargeMonthlyFee,
  sendEndOfTrialEmail,
  sendMonthlyChargeEmail,
  processSubscriptionCancellation,
  sendSorryToSeeYouGoEmail,
} = proxyActivities<typeof Activities>({
  startToCloseTimeout: '30 seconds',  // each activity must complete within 30s
  retry: {
    maximumAttempts: 5,
    initialInterval: '2 seconds',     // first retry after 2s
    backoffCoefficient: 2.0,          // 2s, 4s, 8s, 16s …
    maximumInterval: '20 seconds',    // never wait more than 20s
    // Throw NonRetryableError from an activity to skip retries (e.g., invalid card)
    nonRetryableErrorTypes: ['NonRetryableError'],
  },
});

// ── Main Workflow ─────────────────────────────────────────────────────────────
export async function subscriptionWorkflow(customerId: string): Promise<void> {
  const { workflowId, runId } = workflowInfo();

  // Mutable state that signal handlers can mutate.
  // Temporal serializes signal delivery, so no race conditions here.
  let isCancelled = false;
  let cancellationReason = 'unknown';

  // ── Signal Handler ───────────────────────────────────────────────────────────
  // setHandler() registers a handler for incoming signals.
  // Temporal guarantees at-least-once delivery: if a signal arrives while the
  // workflow is sleeping, it is queued and delivered when the workflow wakes.
  setHandler(cancelSubscriptionSignal, ({ reason }) => {
    log.info('[SIGNAL] Cancel request received', { customerId, reason });
    isCancelled = true;
    cancellationReason = reason;
  });

  log.info('[START] Subscription workflow started', {
    customerId,
    workflowId,
    runId,
    trialPeriodMs: TRIAL_PERIOD_MS,
    note: 'Crash this worker at any point — workflow state is safe in Temporal',
  });

  // ── Step 1: Send Welcome Email ────────────────────────────────────────────────
  // Temporal records "ActivityTaskCompleted" in history when this finishes.
  // A crash between here and step 2 will replay step 1 from history —
  // the email service is NOT called again.
  log.info('[STEP 1/5] Sending welcome email', { customerId });
  await sendWelcomeEmail({ customerId, workflowId });

  // ── Step 2: Durable Timer (Trial Period) ──────────────────────────────────────
  // condition(predicate, timeout) is a durable wait. Temporal records a
  // "TimerStarted" event in history. If the worker restarts mid-wait:
  //   - Timer has NOT elapsed: workflow sleeps until it fires
  //   - Timer HAS elapsed: condition() returns immediately (no re-sleeping)
  //
  // Returns:
  //   true  → isCancelled became true (cancel signal received)
  //   false → timeout elapsed (trial period ended)
  log.info('[STEP 2/5] Trial period started — waiting or watching for cancellation', {
    customerId,
    trialPeriodMs: TRIAL_PERIOD_MS,
    realWorldEquivalent: '30 days',
  });

  const cancelledDuringTrial = await condition(
    () => isCancelled,
    TRIAL_PERIOD_MS,
  );

  // ── Step 3: Branch — Cancelled vs Trial Ended ─────────────────────────────────
  if (cancelledDuringTrial) {
    log.info('[STEP 3/5] Subscription cancelled during trial', {
      customerId,
      reason: cancellationReason,
    });

    await processSubscriptionCancellation({
      customerId,
      workflowId,
      reason: cancellationReason,
    });
    await sendSorryToSeeYouGoEmail({ customerId, workflowId });

    log.info('[DONE] Workflow finished with status: CANCELLED', { customerId });
    return;
  }

  // ── Step 4: Charge Monthly Fee ────────────────────────────────────────────────
  // KEY CONCEPT — Why idempotency keys still matter here:
  //   Temporal will NOT retry a *completed* activity. But the activity itself
  //   might have sent the charge to Stripe and crashed before returning.
  //   On retry, the same idempotency key is sent to Stripe, which returns
  //   the original charge response rather than charging again.
  log.info('[STEP 4/5] Trial ended — charging customer', { customerId });

  let chargeResult: ChargeResult;
  chargeResult = await chargeMonthlyFee({ customerId, workflowId });

  log.info('[STEP 4/5] Charge successful', {
    customerId,
    chargeId: chargeResult.chargeId,
    amount: chargeResult.amount,
    currency: chargeResult.currency,
  });

  // ── Step 5: Send Confirmation Emails ──────────────────────────────────────────
  await sendEndOfTrialEmail({
    customerId,
    workflowId,
    chargeId: chargeResult.chargeId,
  });

  await sendMonthlyChargeEmail({
    customerId,
    workflowId,
    amount: chargeResult.amount,
  });

  log.info('[DONE] Workflow finished with status: COMPLETED', { customerId });
}
