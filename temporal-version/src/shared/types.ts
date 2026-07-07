// ─── Shared types used by both workflow and activities ────────────────────────
// These are pure data types — no business logic, no imports from Temporal SDK.
// They can be safely imported in both the workflow sandbox and activities.

export interface SendWelcomeEmailInput {
  customerId: string;
  workflowId: string;
}

export interface ChargeMonthlyFeeInput {
  customerId: string;
  workflowId: string;
}

export interface ChargeResult {
  chargeId: string;
  amount: number;
  currency: string;
}

export interface SendEndOfTrialEmailInput {
  customerId: string;
  workflowId: string;
  chargeId: string;
}

export interface SendMonthlyChargeEmailInput {
  customerId: string;
  workflowId: string;
  amount: number;
}

export interface ProcessCancellationInput {
  customerId: string;
  workflowId: string;
  reason: string;
}

export interface SendSorryEmailInput {
  customerId: string;
  workflowId: string;
}
