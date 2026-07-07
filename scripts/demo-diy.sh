#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DIY Version — Demo Script
#
# Prerequisites:
#   docker compose up postgres redis
#   cd diy-version && npm install
#   npm run start:api &     # Terminal 1
#   npm run start:worker &  # Terminal 2
#   npm run start:scheduler & # Terminal 3
#   npm run start:reconciler & # Terminal 4
# ─────────────────────────────────────────────────────────────────────────────
set -e

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

cd "$(dirname "$0")/../diy-version"
DB_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/diy_workflows}"

print() { echo -e "${BLUE}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }
warn() { echo -e "${YELLOW}$1${NC}"; }
pause() { echo -e "${YELLOW}[Press Enter to continue]${NC}"; read -r; }

db() { psql "$DB_URL" -c "$1" 2>/dev/null || echo "(db not ready)"; }

# ── Demo 1: Happy Path + Database Inspection ──────────────────────────────────
print "\n═══════════════════════════════════════════════"
print " DEMO 1: Happy Path + Inspect the Machinery"
print "═══════════════════════════════════════════════"

CUSTOMER="demo-diy-$(date +%s)"
print "\nStarting subscription for $CUSTOMER..."
npx ts-node src/cli.ts start "$CUSTOMER"

print "\n── subscription_workflows table (the state machine) ──"
db "SELECT id, customer_id, state, trial_end_at FROM subscription_workflows WHERE customer_id='$CUSTOMER';"

pause

print "\n── After welcome email sent — inspect all tables ──"
npx ts-node src/cli.ts history "$CUSTOMER" 2>/dev/null || true

print "\n── workflow_events (our event log) ──"
db "SELECT event_type, event_data, created_at FROM workflow_events ORDER BY id DESC LIMIT 10;"

print "\n── idempotency_keys (completed activities) ──"
db "SELECT key, activity_type, created_at FROM idempotency_keys ORDER BY created_at DESC LIMIT 5;"

print "\nWaiting 35 seconds for trial to expire..."
sleep 35

print "\n── State after trial expires ──"
db "SELECT customer_id, state, metadata FROM subscription_workflows WHERE customer_id='$CUSTOMER';"

print "\n── Full event history ──"
npx ts-node src/cli.ts history "$CUSTOMER" 2>/dev/null || true

# ── Demo 2: Cancellation ─────────────────────────────────────────────────────
print "\n═══════════════════════════════════════════════"
print " DEMO 2: Cancellation During Trial"
print "═══════════════════════════════════════════════"

CUSTOMER2="demo-cancel-diy-$(date +%s)"
print "\nStarting subscription for $CUSTOMER2..."
npx ts-node src/cli.ts start "$CUSTOMER2"
sleep 3

print "\nRequesting cancellation..."
npx ts-node src/cli.ts cancel "$CUSTOMER2" "found-a-better-deal"

sleep 5
print "\n── State after cancellation ──"
db "SELECT customer_id, state, cancellation_reason FROM subscription_workflows WHERE customer_id='$CUSTOMER2';"

print "\n── Cancellation events ──"
db "SELECT event_type, event_data FROM workflow_events ORDER BY id DESC LIMIT 5;"

# ── Demo 3: Idempotency ───────────────────────────────────────────────────────
print "\n═══════════════════════════════════════════════"
print " DEMO 3: Duplicate Task Delivery"
print "═══════════════════════════════════════════════"

CUSTOMER3="demo-idem-$(date +%s)"
npx ts-node src/cli.ts start "$CUSTOMER3"
sleep 5

print "\nGetting workflow ID..."
WF_ID=$(psql "$DB_URL" -t -c "SELECT id FROM subscription_workflows WHERE customer_id='$CUSTOMER3'" 2>/dev/null | tr -d ' ')

print "Manually injecting duplicate task into Redis queue..."
redis-cli rpush diy:tasks:ready "{\"taskId\":\"dup-$(date +%s)\",\"workflowId\":\"$WF_ID\",\"customerId\":\"$CUSTOMER3\",\"taskType\":\"SEND_WELCOME_EMAIL\",\"attempt\":1}" 2>/dev/null || warn "redis-cli not found locally — skip this step"

print "Worker will pick up duplicate and hit idempotency check..."
sleep 3
db "SELECT key, activity_type FROM idempotency_keys WHERE workflow_id='$WF_ID';"

success "\n✓ DIY Demo complete!"
print "\nKey tables to inspect in psql:"
print "  \\c diy_workflows"
print "  SELECT * FROM subscription_workflows;"
print "  SELECT * FROM workflow_events ORDER BY id;"
print "  SELECT * FROM activity_attempts;"
print "  SELECT * FROM idempotency_keys;"
print "  SELECT * FROM workflow_locks;"
print "  SELECT * FROM dead_letter_tasks;"
