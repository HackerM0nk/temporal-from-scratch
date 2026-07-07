-- ─────────────────────────────────────────────────────────────────────────────
-- DIY Workflow Orchestrator — Database Schema
--
-- This schema IS the durable workflow engine. Compare to Temporal:
--
--   subscription_workflows  ← workflow execution record (Temporal: workflow run)
--   workflow_events         ← append-only audit log (Temporal: workflow history)
--   activity_attempts       ← activity execution record (Temporal: activity task)
--   idempotency_keys        ← deduplication (Temporal: history-based dedup)
--   workflow_locks          ← exclusive execution (Temporal: sticky worker)
--   dead_letter_tasks       ← exhausted retries (Temporal: workflow failed state)
-- ─────────────────────────────────────────────────────────────────────────────

-- ── subscription_workflows ───────────────────────────────────────────────────
-- One row per workflow execution. This is the canonical "where is this workflow"
-- record. The `state` column is the workflow's program counter — every field
-- Temporal tracks implicitly in its history, we track explicitly here.
CREATE TABLE IF NOT EXISTS subscription_workflows (
  id             UUID PRIMARY KEY,
  customer_id    VARCHAR(255) NOT NULL UNIQUE,

  -- Current state in the state machine. In Temporal, this is implicit in the
  -- workflow code's execution point. Here we make it explicit and queryable.
  state          VARCHAR(100) NOT NULL DEFAULT 'STARTED',

  -- Timer for the trial period. Temporal stores this as a TimerStarted event
  -- and fires it server-side. Here we store the target timestamp and poll it.
  trial_end_at   TIMESTAMPTZ,

  -- Metadata bag for storing activity results (e.g., chargeId)
  metadata       JSONB NOT NULL DEFAULT '{}',

  -- Cancellation reason, set when CANCELLATION_REQUESTED state is entered
  cancellation_reason TEXT,

  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflows_state         ON subscription_workflows(state);
CREATE INDEX IF NOT EXISTS idx_workflows_trial_end_at  ON subscription_workflows(trial_end_at)
  WHERE state = 'WAITING_FOR_TRIAL_END';


-- ── workflow_events ──────────────────────────────────────────────────────────
-- Append-only event log. Every state transition, activity start/complete/fail,
-- signal received, and timer fired is recorded here.
--
-- In Temporal, this IS the workflow history — it's authoritative and used for
-- replay. In our DIY system, it's an audit log (we replay from state, not events).
-- This is a key architectural difference.
CREATE TABLE IF NOT EXISTS workflow_events (
  id             BIGSERIAL PRIMARY KEY,
  workflow_id    UUID NOT NULL REFERENCES subscription_workflows(id),
  event_type     VARCHAR(100) NOT NULL,
  event_data     JSONB NOT NULL DEFAULT '{}',
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_workflow_id ON workflow_events(workflow_id, id);


-- ── activity_attempts ────────────────────────────────────────────────────────
-- Tracks every attempt to execute an activity. Includes retry history.
-- In Temporal, this lives in workflow history as ActivityTaskScheduled /
-- ActivityTaskStarted / ActivityTaskCompleted events.
CREATE TABLE IF NOT EXISTS activity_attempts (
  id              UUID PRIMARY KEY,
  workflow_id     UUID NOT NULL REFERENCES subscription_workflows(id),
  activity_type   VARCHAR(100) NOT NULL,
  attempt_number  INT NOT NULL DEFAULT 1,

  -- PENDING → RUNNING → COMPLETED | FAILED
  status          VARCHAR(50) NOT NULL DEFAULT 'PENDING',

  scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at      TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,

  error_message   TEXT,
  result          JSONB
);

CREATE INDEX IF NOT EXISTS idx_attempts_workflow_id ON activity_attempts(workflow_id, activity_type);


-- ── idempotency_keys ─────────────────────────────────────────────────────────
-- Deduplication table. Before executing any side-effecting activity, the worker
-- checks this table. If the key exists, it returns the cached result.
--
-- In Temporal, activity deduplication is handled by workflow history:
-- a completed ActivityTaskCompleted event means "don't run this again."
-- In our DIY system, we implement the same guarantee with this table.
--
-- Key format: "{workflowId}:{activityType}"
-- This means: within one workflow execution, each activity runs at most once.
CREATE TABLE IF NOT EXISTS idempotency_keys (
  key            VARCHAR(500) PRIMARY KEY,
  workflow_id    UUID NOT NULL REFERENCES subscription_workflows(id),
  activity_type  VARCHAR(100) NOT NULL,
  result         JSONB,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ── workflow_locks ───────────────────────────────────────────────────────────
-- Distributed lock table. Ensures only ONE worker processes a given workflow
-- at a time, preventing split-brain (two workers advancing the same workflow).
--
-- In Temporal, this is handled by the server assigning tasks to workers.
-- Each workflow execution has at most one active task at a time on the server.
-- Here, we implement the same guarantee with this advisory lock table.
--
-- Locks expire automatically so a crashed worker doesn't permanently block
-- a workflow (reconciler cleans up expired locks).
CREATE TABLE IF NOT EXISTS workflow_locks (
  workflow_id    UUID PRIMARY KEY REFERENCES subscription_workflows(id),
  locked_by      VARCHAR(255) NOT NULL,  -- worker process ID
  locked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at     TIMESTAMPTZ NOT NULL
);


-- ── dead_letter_tasks ────────────────────────────────────────────────────────
-- Tasks that have exhausted all retry attempts. These represent workflows that
-- have failed permanently and require manual intervention.
--
-- In Temporal, this manifests as a WorkflowExecutionFailed status with the
-- error details in history. Here, we make it a first-class table.
CREATE TABLE IF NOT EXISTS dead_letter_tasks (
  id             UUID PRIMARY KEY,
  workflow_id    UUID NOT NULL REFERENCES subscription_workflows(id),
  activity_type  VARCHAR(100) NOT NULL,
  payload        JSONB NOT NULL DEFAULT '{}',
  error_message  TEXT,
  retry_count    INT NOT NULL DEFAULT 0,
  failed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
