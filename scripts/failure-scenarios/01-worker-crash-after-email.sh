#!/usr/bin/env bash
# Scenario 1: Worker crashes after sendWelcomeEmail completes
#
# What to observe:
#   Temporal: workflow resumes from step 2 on worker restart, email NOT sent again
#   DIY:      reconciler re-enqueues, idempotency key prevents duplicate email
set -e
BLUE='\033[0;34m'; NC='\033[0m'
print() { echo -e "${BLUE}$1${NC}"; }

# ── Temporal ──────────────────────────────────────────────────────────────────
print "\n[Temporal] Scenario 1: Worker crash after email send"
print "Steps:"
print "  1. Start workflow: npx ts-node temporal-version/src/cli.ts start crash-email-temporal"
print "  2. Watch worker logs — wait for '[STEP 1/5] Sending welcome email'"
print "  3. IMMEDIATELY kill the temporal-worker: docker kill dw-temporal-worker"
print "  4. Restart: docker start dw-temporal-worker"
print "  5. Observe: workflow continues from STEP 2 (trial wait), email not re-sent"
print ""
print "Why: Temporal recorded 'ActivityTaskCompleted(sendWelcomeEmail)' in history."
print "     Replay sees this record and skips the activity."

# ── DIY ───────────────────────────────────────────────────────────────────────
print "\n[DIY] Scenario 1: Worker crash after email send"
print "Steps:"
print "  1. Start workflow: npx ts-node diy-version/src/cli.ts start crash-email-diy"
print "  2. Kill DIY worker: docker kill dw-diy-worker"
print "  3. Check state: psql \$DATABASE_URL -c \"SELECT state FROM subscription_workflows WHERE customer_id='crash-email-diy'\""
print "  4. Wait 40s for reconciler to detect + re-enqueue"
print "  5. Restart worker: docker start dw-diy-worker"
print "  6. Check idempotency_keys — welcome email only appears once"
print ""
print "Why: reconciler detects WELCOME_EMAIL_SCHEDULED with no lock → re-enqueues."
print "     Worker checks idempotency_keys before executing → prevents duplicate email."
