// ─────────────────────────────────────────────────────────────────────────────
// DIY Workflow State Machine
//
// This file defines every possible state a subscription workflow can be in,
// and exactly which states can transition to which other states.
//
// In Temporal, this state machine is IMPLICIT — it lives in the workflow code
// as the current execution point (which await the workflow is suspended at).
// Here we make it EXPLICIT as a first-class enum that lives in the database.
//
// COMPARISON:
//   Temporal:  Workflow state = "the line of code currently suspended at"
//              Stored as: implicit in workflow history replay
//   DIY:       Workflow state = a string column in subscription_workflows table
//              Stored as: explicit VARCHAR in PostgreSQL
// ─────────────────────────────────────────────────────────────────────────────

// ── States ────────────────────────────────────────────────────────────────────
export enum WorkflowState {
  // Happy path ─────────────────────────────────────────────────────────────────
  STARTED                  = 'STARTED',
  WELCOME_EMAIL_SCHEDULED  = 'WELCOME_EMAIL_SCHEDULED',
  WELCOME_EMAIL_SENT       = 'WELCOME_EMAIL_SENT',
  WAITING_FOR_TRIAL_END    = 'WAITING_FOR_TRIAL_END',   // ← durable timer
  CHARGE_SCHEDULED         = 'CHARGE_SCHEDULED',
  CHARGING                 = 'CHARGING',
  CHARGED                  = 'CHARGED',
  END_OF_TRIAL_EMAIL_SENT  = 'END_OF_TRIAL_EMAIL_SENT',
  COMPLETED                = 'COMPLETED',

  // Cancellation path ───────────────────────────────────────────────────────────
  CANCELLATION_REQUESTED   = 'CANCELLATION_REQUESTED',
  CANCELLING               = 'CANCELLING',
  CANCELLED                = 'CANCELLED',

  // Failure path ────────────────────────────────────────────────────────────────
  FAILED                   = 'FAILED',
}

// ── Task Types ────────────────────────────────────────────────────────────────
// Tasks are messages pushed to the Redis queue. Each maps to one "activity"
// in Temporal terms — a single unit of side-effecting work.
export enum TaskType {
  SEND_WELCOME_EMAIL       = 'SEND_WELCOME_EMAIL',
  CHARGE_MONTHLY_FEE       = 'CHARGE_MONTHLY_FEE',
  SEND_END_OF_TRIAL_EMAIL  = 'SEND_END_OF_TRIAL_EMAIL',
  SEND_MONTHLY_CHARGE_EMAIL = 'SEND_MONTHLY_CHARGE_EMAIL',
  PROCESS_CANCELLATION     = 'PROCESS_CANCELLATION',
  SEND_SORRY_EMAIL         = 'SEND_SORRY_EMAIL',
}

// ── State Transition Table ────────────────────────────────────────────────────
// Defines what happens to the workflow state when a task is scheduled,
// when it starts executing, and when it completes.
//
// In Temporal, the "program counter" advances automatically as the workflow
// code runs. Here we must encode every transition explicitly.
export interface Transition {
  // State to set when this task is enqueued (before execution starts)
  onSchedule: WorkflowState;
  // State to set when a worker picks up the task (execution in progress)
  onStart: WorkflowState;
  // State to set when the task completes successfully
  onComplete: WorkflowState;
  // If set, automatically enqueue this task after onComplete
  nextTask?: TaskType;
}

export const TRANSITIONS: Record<TaskType, Transition> = {
  [TaskType.SEND_WELCOME_EMAIL]: {
    onSchedule: WorkflowState.WELCOME_EMAIL_SCHEDULED,
    onStart:    WorkflowState.WELCOME_EMAIL_SCHEDULED,
    onComplete: WorkflowState.WELCOME_EMAIL_SENT,
    // No nextTask — after welcome email, the SCHEDULER wakes us up when trial ends
    // This is the equivalent of workflow.sleep() in Temporal
  },
  [TaskType.CHARGE_MONTHLY_FEE]: {
    onSchedule: WorkflowState.CHARGE_SCHEDULED,
    onStart:    WorkflowState.CHARGING,
    onComplete: WorkflowState.CHARGED,
    nextTask:   TaskType.SEND_END_OF_TRIAL_EMAIL,
  },
  [TaskType.SEND_END_OF_TRIAL_EMAIL]: {
    onSchedule: WorkflowState.CHARGED,
    onStart:    WorkflowState.CHARGED,
    onComplete: WorkflowState.END_OF_TRIAL_EMAIL_SENT,
    nextTask:   TaskType.SEND_MONTHLY_CHARGE_EMAIL,
  },
  [TaskType.SEND_MONTHLY_CHARGE_EMAIL]: {
    onSchedule: WorkflowState.END_OF_TRIAL_EMAIL_SENT,
    onStart:    WorkflowState.END_OF_TRIAL_EMAIL_SENT,
    onComplete: WorkflowState.COMPLETED,
  },
  [TaskType.PROCESS_CANCELLATION]: {
    onSchedule: WorkflowState.CANCELLATION_REQUESTED,
    onStart:    WorkflowState.CANCELLING,
    onComplete: WorkflowState.CANCELLING,
    nextTask:   TaskType.SEND_SORRY_EMAIL,
  },
  [TaskType.SEND_SORRY_EMAIL]: {
    onSchedule: WorkflowState.CANCELLING,
    onStart:    WorkflowState.CANCELLING,
    onComplete: WorkflowState.CANCELLED,
  },
};

// Terminal states — a workflow in one of these states should not be processed
export const TERMINAL_STATES = new Set<WorkflowState>([
  WorkflowState.COMPLETED,
  WorkflowState.CANCELLED,
  WorkflowState.FAILED,
]);

// States where a new CHARGE_MONTHLY_FEE task can be enqueued (by scheduler)
export const SCHEDULABLE_TIMER_STATE = WorkflowState.WAITING_FOR_TRIAL_END;
