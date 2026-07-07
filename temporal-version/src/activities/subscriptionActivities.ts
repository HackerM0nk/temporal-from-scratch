// ─────────────────────────────────────────────────────────────────────────────
// Subscription Activities — Temporal Version
//
// Activities are the units of work that interact with the outside world.
// They run in NORMAL Node.js (not the sandbox), so they can use databases,
// HTTP clients, etc.
//
// KEY CONCEPT — Activity vs Workflow code:
//   Workflow code:  pure, deterministic, replayed from history, NO side effects
//   Activity code:  impure, talks to external systems, has real side effects
//
// KEY CONCEPT — Idempotency:
//   Temporal will NOT re-run a completed activity when replaying a workflow.
//   But if an activity crashes mid-execution (e.g., network timeout while
//   calling Stripe), Temporal WILL retry it. So activities must be safe to
//   run more than once — they use idempotency keys on external APIs.
// ─────────────────────────────────────────────────────────────────────────────

import { Context } from '@temporalio/activity';
import type {
  SendWelcomeEmailInput,
  ChargeMonthlyFeeInput,
  ChargeResult,
  SendEndOfTrialEmailInput,
  SendMonthlyChargeEmailInput,
  ProcessCancellationInput,
  SendSorryEmailInput,
} from '../shared/types';

// ── Logging helpers ───────────────────────────────────────────────────────────

function log(activity: string, customerId: string, msg: string, data?: object) {
  const ts = new Date().toISOString();
  const extra = data ? ` | ${JSON.stringify(data)}` : '';
  console.log(`[${ts}] [temporal-activity] [${activity}] customer=${customerId} | ${msg}${extra}`);
}

// ── Simulated external call ───────────────────────────────────────────────────
// Pretends to call an email service, payment processor, etc.
async function simulateExternalCall(ms: number, service: string): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Activities ────────────────────────────────────────────────────────────────

export async function sendWelcomeEmail(input: SendWelcomeEmailInput): Promise<void> {
  const { customerId, workflowId } = input;
  // The idempotency key is scoped to this workflow run.
  // Even if this activity is retried, the email service only sends once.
  const idempotencyKey = `welcome-email:${workflowId}`;

  log('sendWelcomeEmail', customerId, 'Sending welcome email to email-service', { idempotencyKey });

  await simulateExternalCall(300, 'email-service');

  log('sendWelcomeEmail', customerId, '✓ Welcome email sent', {
    to: `${customerId}@example.com`,
    subject: 'Welcome to the service!',
  });
}

export async function chargeMonthlyFee(input: ChargeMonthlyFeeInput): Promise<ChargeResult> {
  const { customerId, workflowId } = input;

  // KEY CONCEPT — Idempotency key for payment:
  // We derive this from the workflowId, making it stable across retries.
  // If the payment processor received our request but we timed out, retrying
  // with the same key returns the original charge result instead of charging again.
  const idempotencyKey = `charge:${workflowId}:cycle-1`;

  // Temporal gives us the attempt number — use it for demo logging
  const attempt = Context.current().info.attempt;

  log('chargeMonthlyFee', customerId, `Calling payment processor (attempt ${attempt}/5)`, {
    idempotencyKey,
    amount: 29.99,
  });

  // Demo: simulate failures on first N attempts to show retry behavior.
  // Set SIMULATE_CHARGE_FAILURE=true before starting the worker.
  if (process.env.SIMULATE_CHARGE_FAILURE === 'true' && attempt <= 2) {
    log('chargeMonthlyFee', customerId,
      `✗ Payment processor returned 503 (simulated failure, attempt ${attempt}/5) — Temporal will retry`,
    );
    throw new Error(`Payment service temporarily unavailable (simulated, attempt ${attempt})`);
  }

  await simulateExternalCall(500, 'payment-processor');

  const result: ChargeResult = {
    chargeId: `ch_${customerId}_${Date.now()}`,
    amount: 29.99,
    currency: 'USD',
  };

  log('chargeMonthlyFee', customerId, '✓ Charge successful', result);
  return result;
}

export async function sendEndOfTrialEmail(input: SendEndOfTrialEmailInput): Promise<void> {
  const { customerId, workflowId, chargeId } = input;
  const idempotencyKey = `end-of-trial-email:${workflowId}`;

  log('sendEndOfTrialEmail', customerId, 'Sending end-of-trial email', { idempotencyKey, chargeId });
  await simulateExternalCall(300, 'email-service');
  log('sendEndOfTrialEmail', customerId, '✓ End-of-trial email sent', {
    subject: 'Your free trial has ended',
  });
}

export async function sendMonthlyChargeEmail(input: SendMonthlyChargeEmailInput): Promise<void> {
  const { customerId, workflowId, amount } = input;
  const idempotencyKey = `monthly-charge-email:${workflowId}`;

  log('sendMonthlyChargeEmail', customerId, 'Sending charge receipt email', { idempotencyKey, amount });
  await simulateExternalCall(300, 'email-service');
  log('sendMonthlyChargeEmail', customerId, `✓ Receipt email sent | amount=$${amount}`);
}

export async function processSubscriptionCancellation(input: ProcessCancellationInput): Promise<void> {
  const { customerId, workflowId, reason } = input;
  const idempotencyKey = `cancellation:${workflowId}`;

  log('processSubscriptionCancellation', customerId,
    'Processing cancellation in billing system', { idempotencyKey, reason },
  );
  await simulateExternalCall(400, 'billing-service');
  log('processSubscriptionCancellation', customerId, '✓ Subscription cancelled in billing system');
}

export async function sendSorryToSeeYouGoEmail(input: SendSorryEmailInput): Promise<void> {
  const { customerId, workflowId } = input;
  const idempotencyKey = `sorry-email:${workflowId}`;

  log('sendSorryToSeeYouGoEmail', customerId, 'Sending cancellation confirmation email', { idempotencyKey });
  await simulateExternalCall(300, 'email-service');
  log('sendSorryToSeeYouGoEmail', customerId, '✓ Cancellation email sent');
}
