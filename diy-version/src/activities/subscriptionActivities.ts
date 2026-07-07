// ─────────────────────────────────────────────────────────────────────────────
// Subscription Activities — DIY Version
//
// Same business logic as the Temporal version, but called directly by the
// DIY worker. The worker is responsible for:
//   - retry logic (Temporal does this automatically)
//   - idempotency checks (Temporal does this via history)
//   - state transitions (Temporal does this implicitly)
//
// The activities themselves are the same — they simulate external API calls.
// ─────────────────────────────────────────────────────────────────────────────

import type { ChargeResult } from '../types';

function log(activity: string, customerId: string, msg: string, data?: object) {
  const ts = new Date().toISOString();
  const extra = data ? ` | ${JSON.stringify(data)}` : '';
  console.log(`[${ts}] [diy-activity] [${activity}] customer=${customerId} | ${msg}${extra}`);
}

async function simulateExternalCall(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

export async function sendWelcomeEmail(customerId: string, workflowId: string): Promise<void> {
  const idempotencyKey = `welcome-email:${workflowId}`;
  log('sendWelcomeEmail', customerId, 'Sending to email-service', { idempotencyKey });
  await simulateExternalCall(300);
  log('sendWelcomeEmail', customerId, '✓ Welcome email sent', {
    to: `${customerId}@example.com`,
  });
}

export async function chargeMonthlyFee(
  customerId: string,
  workflowId: string,
  attempt: number,
): Promise<ChargeResult> {
  // Idempotency key is stable across retries — same workflowId, same cycle
  const idempotencyKey = `charge:${workflowId}:cycle-1`;
  log('chargeMonthlyFee', customerId, `Calling payment processor (attempt ${attempt}/5)`, {
    idempotencyKey,
    amount: 29.99,
  });

  // Demo: simulate failures on first N attempts
  if (process.env.SIMULATE_CHARGE_FAILURE === 'true' && attempt <= 2) {
    log('chargeMonthlyFee', customerId,
      `✗ Payment processor returned 503 (simulated, attempt ${attempt}) — worker will retry`,
    );
    throw new Error(`Payment service temporarily unavailable (simulated, attempt ${attempt})`);
  }

  await simulateExternalCall(500);

  const result: ChargeResult = {
    chargeId: `ch_${customerId}_${Date.now()}`,
    amount: 29.99,
    currency: 'USD',
  };

  log('chargeMonthlyFee', customerId, '✓ Charge successful', result);
  return result;
}

export async function sendEndOfTrialEmail(
  customerId: string,
  workflowId: string,
  chargeId: string,
): Promise<void> {
  const idempotencyKey = `end-of-trial-email:${workflowId}`;
  log('sendEndOfTrialEmail', customerId, 'Sending to email-service', { idempotencyKey, chargeId });
  await simulateExternalCall(300);
  log('sendEndOfTrialEmail', customerId, '✓ End-of-trial email sent');
}

export async function sendMonthlyChargeEmail(
  customerId: string,
  workflowId: string,
  amount: number,
): Promise<void> {
  const idempotencyKey = `monthly-charge-email:${workflowId}`;
  log('sendMonthlyChargeEmail', customerId, 'Sending receipt email', { idempotencyKey, amount });
  await simulateExternalCall(300);
  log('sendMonthlyChargeEmail', customerId, `✓ Receipt email sent | amount=$${amount}`);
}

export async function processSubscriptionCancellation(
  customerId: string,
  workflowId: string,
  reason: string,
): Promise<void> {
  const idempotencyKey = `cancellation:${workflowId}`;
  log('processSubscriptionCancellation', customerId, 'Cancelling in billing system', {
    idempotencyKey,
    reason,
  });
  await simulateExternalCall(400);
  log('processSubscriptionCancellation', customerId, '✓ Subscription cancelled in billing system');
}

export async function sendSorryToSeeYouGoEmail(
  customerId: string,
  workflowId: string,
): Promise<void> {
  const idempotencyKey = `sorry-email:${workflowId}`;
  log('sendSorryToSeeYouGoEmail', customerId, 'Sending cancellation confirmation', { idempotencyKey });
  await simulateExternalCall(300);
  log('sendSorryToSeeYouGoEmail', customerId, '✓ Cancellation email sent');
}
