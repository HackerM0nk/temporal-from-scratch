#!/usr/bin/env bash
# Scenario 3: Charge activity fails 2 times then succeeds on attempt 3
set -e
BLUE='\033[0;34m'; NC='\033[0m'
print() { echo -e "${BLUE}$1${NC}"; }

print "\n[Temporal] Scenario 3: Charge fails twice"
print "Steps:"
print "  1. Set SIMULATE_CHARGE_FAILURE=true in temporal-worker container:"
print "     docker compose stop temporal-worker"
print "     SIMULATE_CHARGE_FAILURE=true docker compose up -d temporal-worker"
print "  2. Start workflow: npx ts-node temporal-version/src/cli.ts start fail-charge-temporal"
print "  3. Wait ~30s for trial to end, then watch worker logs"
print "  4. Observe: 'Payment service temporarily unavailable' x2, then success on attempt 3"
print "  5. Check UI history: ActivityTaskFailed x2, ActivityTaskCompleted x1"

print "\n[DIY] Scenario 3: Charge fails twice"
print "Steps:"
print "  1. docker compose stop diy-worker"
print "  2. SIMULATE_CHARGE_FAILURE=true docker compose up -d diy-worker"
print "  3. Start workflow: npx ts-node diy-version/src/cli.ts start fail-charge-diy"
print "  4. After ~35s: check activity_attempts"
print "     psql postgres://postgres:postgres@localhost:5432/diy_workflows -c"
print "       'SELECT activity_type, attempt_number, status, error_message FROM activity_attempts ORDER BY scheduled_at;'"
print "  5. Observe: CHARGE_MONTHLY_FEE attempt_1 FAILED, attempt_2 FAILED, attempt_3 COMPLETED"
