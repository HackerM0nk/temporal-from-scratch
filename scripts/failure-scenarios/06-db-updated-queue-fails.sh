#!/usr/bin/env bash
# Scenario 6: DB state updated but Redis RPUSH fails
#
# This is the hardest consistency problem in DIY orchestration.
# We simulate it by pausing Redis after a DB commit.
set -e
BLUE='\033[0;34m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'
print() { echo -e "${BLUE}$1${NC}"; }
warn() { echo -e "${YELLOW}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }

DB_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/diy_workflows}"
cd "$(dirname "$0")/../../diy-version"

print "\n═══ Scenario 6: DB Updated, Queue Publish Fails ════"

CUSTOMER="db-queue-fail-$(date +%s)"
print "\n1. Start workflow and wait for welcome email to complete..."
npx ts-node src/cli.ts start "$CUSTOMER" 2>/dev/null || true
sleep 5

print "\n2. Manually advance to CHARGED state (simulating worker committed but Redis failed):"
WF_ID=$(psql "$DB_URL" -t -c "SELECT id FROM subscription_workflows WHERE customer_id='$CUSTOMER'" 2>/dev/null | tr -d ' \n')

# Manually set state to CHARGED (as if CHARGE_MONTHLY_FEE completed but next task wasn't queued)
psql "$DB_URL" -c "
  UPDATE subscription_workflows
  SET state='CHARGED', metadata='{\"chargeId\":\"ch_simulated_123\",\"amount\":29.99}'
  WHERE id='$WF_ID';

  INSERT INTO idempotency_keys (key, workflow_id, activity_type, result)
  VALUES ('$WF_ID:CHARGE_MONTHLY_FEE', '$WF_ID', 'CHARGE_MONTHLY_FEE', '{\"chargeId\":\"ch_simulated_123\",\"amount\":29.99}');

  INSERT INTO workflow_events (workflow_id, event_type, event_data)
  VALUES ('$WF_ID', 'SIMULATED_QUEUE_FAILURE', '{\"note\":\"DB committed but Redis RPUSH failed\"}'::jsonb);
" 2>/dev/null || warn "Could not update state (psql not available locally)"

print "\n3. Current state (stuck in CHARGED with no task in Redis):"
psql "$DB_URL" -c "SELECT state, metadata FROM subscription_workflows WHERE id='$WF_ID';" 2>/dev/null || true

warn "\n4. The workflow is now STUCK:"
warn "   - state=CHARGED in DB"
warn "   - No SEND_END_OF_TRIAL_EMAIL task in Redis"
warn "   - Worker is idle"

print "\n5. Wait for reconciler to detect and repair (up to 70s)..."
print "   Reconciler looks for: state=CHARGED, updated_at > 60s ago, no lock"
print "   Maps CHARGED → SEND_END_OF_TRIAL_EMAIL"
print "   Re-enqueues the task"

success "\nReconciler output to expect:"
success "  [reconciler] 🔧 Found 1 stuck workflow(s)"
success "  [reconciler] ↺ Re-enqueued task | state=CHARGED | task=SEND_END_OF_TRIAL_EMAIL"

print "\n6. This scenario is IMPOSSIBLE with Temporal:"
print "   Temporal atomically creates the next workflow task with the state update."
print "   The queue (task queue) IS the state — they're the same system."
