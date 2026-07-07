#!/usr/bin/env bash
# Scenario 7: Stuck workflow repaired by reconciliation
#
# Simulates: worker holds lock and crashes, leaving workflow stuck in CHARGING state
set -e
BLUE='\033[0;34m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'
print() { echo -e "${BLUE}$1${NC}"; }
warn() { echo -e "${YELLOW}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }

DB_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/diy_workflows}"

cd "$(dirname "$0")/../../diy-version"

print "\n═══ Scenario 7: Stuck Workflow Reconciliation ════════"

CUSTOMER="stuck-workflow-$(date +%s)"

print "\n1. Start a workflow:"
npx ts-node src/cli.ts start "$CUSTOMER" 2>/dev/null || true

print "\n2. Wait for trial to expire (~35s)..."
sleep 35

print "\n3. Current state (should be CHARGE_SCHEDULED or CHARGING):"
psql "$DB_URL" -c "SELECT state, updated_at FROM subscription_workflows WHERE customer_id='$CUSTOMER';" 2>/dev/null || true

warn "\n4. NOW: Stop the DIY worker to simulate a crash"
warn "   docker stop dw-diy-worker"
warn "   (Or if running locally: kill the ts-node worker process)"
warn ""
warn "   If workflow is in CHARGING state, manually inject a lock to simulate crash:"
WF_ID=$(psql "$DB_URL" -t -c "SELECT id FROM subscription_workflows WHERE customer_id='$CUSTOMER'" 2>/dev/null | tr -d ' \n')
warn "   psql \$DATABASE_URL -c \"INSERT INTO workflow_locks (workflow_id, locked_by, locked_at, expires_at) VALUES ('$WF_ID', 'dead-worker-99', NOW(), NOW() + INTERVAL '1 second');\""

print "\n5. Check lock table:"
psql "$DB_URL" -c "SELECT * FROM workflow_locks;" 2>/dev/null || true

print "\n6. Wait 40s for reconciler to:"
print "   - Release expired lock (after 30s)"
print "   - Detect stuck CHARGING workflow (after 60s from updated_at)"
print "   - Re-enqueue CHARGE_MONTHLY_FEE task"

sleep 15

print "\n7. Reconciler events in workflow_events:"
psql "$DB_URL" -c "SELECT event_type, event_data, created_at FROM workflow_events WHERE workflow_id='$WF_ID' ORDER BY id DESC LIMIT 5;" 2>/dev/null || true

print "\n8. Restart worker: docker start dw-diy-worker"
print "   Worker will pick up the re-enqueued charge task."
print "   Idempotency check: if charge already completed → skip, advance state."
print "   If charge was in-flight → retry with same idempotency key → no double charge."

success "\nReconciler log lines to look for:"
success "  [reconciler] 🔓 Released 1 expired lock(s)"
success "  [reconciler] 🔧 Found 1 stuck workflow(s)"
success "  [reconciler] ↺ Re-enqueued task | state=CHARGING | task=CHARGE_MONTHLY_FEE"
