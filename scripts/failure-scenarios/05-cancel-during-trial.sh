#!/usr/bin/env bash
# Scenario 5: Customer cancels during the trial wait period
set -e
BLUE='\033[0;34m'; GREEN='\033[0;32m'; NC='\033[0m'
print() { echo -e "${BLUE}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }

print "\n═══ Scenario 5: Cancel During Trial ════════════════"

CUSTOMER_T="cancel-trial-temporal-$(date +%s)"
CUSTOMER_D="cancel-trial-diy-$(date +%s)"

# ── Temporal ──────────────────────────────────────────────────────────────────
print "\n[Temporal] Starting $CUSTOMER_T"
cd "$(dirname "$0")/../../temporal-version"
npx ts-node src/cli.ts start "$CUSTOMER_T" 2>/dev/null || true

sleep 3
print "Sending cancel signal..."
npx ts-node src/cli.ts cancel "$CUSTOMER_T" "found-better-deal"

sleep 5
print "Status:"
npx ts-node src/cli.ts status "$CUSTOMER_T" 2>/dev/null || true

# ── DIY ───────────────────────────────────────────────────────────────────────
print "\n[DIY] Starting $CUSTOMER_D"
cd "$(dirname "$0")/../../diy-version"
npx ts-node src/cli.ts start "$CUSTOMER_D" 2>/dev/null || true

sleep 3
print "Sending cancel request..."
npx ts-node src/cli.ts cancel "$CUSTOMER_D" "found-better-deal"

sleep 5
print "History:"
npx ts-node src/cli.ts history "$CUSTOMER_D" 2>/dev/null || true

success "\nObserve:"
success "  - Customer was NOT charged (cancelled before trial_end_at)"
success "  - processSubscriptionCancellation + sendSorryToSeeYouGoEmail executed"
success "  - DIY: scheduler found no WAITING_FOR_TRIAL_END rows → no charge task queued"
