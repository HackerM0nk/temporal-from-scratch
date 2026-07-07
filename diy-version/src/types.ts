// ─────────────────────────────────────────────────────────────────────────────
// Shared Types — DIY Version
// ─────────────────────────────────────────────────────────────────────────────

import { WorkflowState, TaskType } from './orchestrator/stateMachine';

// A task message as stored in the Redis queue
export interface QueueTask {
  taskId: string;
  workflowId: string;
  customerId: string;
  taskType: TaskType;
  attempt: number;
  // ISO string — used for delayed tasks (retry with backoff)
  executeAfter?: string;
  // Context metadata passed from prior activities
  context?: Record<string, any>;
}

// A workflow row from subscription_workflows
export interface WorkflowRow {
  id: string;
  customer_id: string;
  state: WorkflowState;
  trial_end_at: Date | null;
  metadata: Record<string, any>;
  cancellation_reason: string | null;
  created_at: Date;
  updated_at: Date;
}

// Result from chargeMonthlyFee
export interface ChargeResult {
  chargeId: string;
  amount: number;
  currency: string;
}
