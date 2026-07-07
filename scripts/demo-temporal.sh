#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Temporal Version — Demo Script
#
# Prerequisites:
#   docker compose up postgres redis temporal temporal-ui
#   cd temporal-version && npm install && npm run start:worker &
#   cd temporal-version && npm run start:api &
# ─────────────────────────────────────────────────────────────────────────────
set -e

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

cd "$(dirname "$0")/../temporal-version"

print() { echo -e "${BLUE}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }
pause() { echo -e "${YELLOW}[Press Enter to continue]${NC}"; read -r; }

# ── Demo 1: Full Happy Path ──────────────────────────────────────────────────
print "\n═══════════════════════════════════════════════"
print " DEMO 1: Full Happy Path (30s = 30 days)"
print "═══════════════════════════════════════════════"

CUSTOMER="demo-temporal-$(date +%s)"
print "\nStarting subscription for $CUSTOMER..."
npx ts-node src/cli.ts start "$CUSTOMER"

print "\nWorkflow is now:"
print "  1. Sending welcome email"
print "  2. Waiting 30 seconds (= 30 days trial)"
print "  3. Charging customer"
print "  4. Sending receipt emails"
print "\nWatch the worker logs to see each step."
print "UI: http://localhost:8080/namespaces/default/workflows/subscription-$CUSTOMER"
pause

print "\nChecking status..."
npx ts-node src/cli.ts status "$CUSTOMER" || true

# ── Demo 2: Cancellation ─────────────────────────────────────────────────────
print "\n═══════════════════════════════════════════════"
print " DEMO 2: Cancel During Trial Wait"
print "═══════════════════════════════════════════════"

CUSTOMER2="demo-cancel-$(date +%s)"
print "\nStarting subscription for $CUSTOMER2..."
npx ts-node src/cli.ts start "$CUSTOMER2"

sleep 2
print "\nSending cancellation signal..."
npx ts-node src/cli.ts cancel "$CUSTOMER2" "switched-to-competitor"

print "\nCheck history to see signal delivery:"
sleep 5
npx ts-node src/cli.ts history "$CUSTOMER2" || true

# ── Demo 3: Worker Crash Recovery ────────────────────────────────────────────
print "\n═══════════════════════════════════════════════"
print " DEMO 3: Crash Recovery"
print "═══════════════════════════════════════════════"
print "\nTo demo crash recovery:"
print "  1. Start a workflow: npx ts-node src/cli.ts start crash-test-$(date +%s)"
print "  2. Immediately kill the worker (Ctrl+C)"
print "  3. Restart the worker: npm run start:worker"
print "  4. Observe: workflow resumes from exactly where it left off"
print "\nKey insight: The worker holds NO in-memory state."
print "All state lives in Temporal's database via workflow history."

success "\n✓ Demo complete!"
