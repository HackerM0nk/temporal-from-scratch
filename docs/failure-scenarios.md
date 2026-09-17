# Failure Scenarios and the Exact Guarantees

The useful question is never only "does it retry?" Ask:

1. What durable evidence exists?
2. What operation may execute again?
3. What external effect needs idempotency?
4. Which component detects the failure?

## 1. Workflow Worker dies after an Activity completed

If Temporal already recorded `ActivityTaskCompleted`, another Workflow Worker replays History and receives the recorded result. Replay does not schedule the Activity again.

If the Activity performed the external effect but its Worker died before reporting completion, Temporal has no completion Event. The Activity can execute again after its Start-to-Close or Heartbeat timeout. The email or payment API must deduplicate a stable operation key.

In the DIY system, RabbitMQ redelivers an unacknowledged message. If the database idempotency record exists, the consumer skips the completed task. If the external effect happened before that record committed, the external provider must deduplicate the same key.

## 2. All application Workers are down during a Timer

Temporal stores the Timer in the Service. It fires without an application Worker. A Workflow Task waits until a compatible Worker returns.

The DIY system stores `trial_end_at` in PostgreSQL. The Scheduler must be alive to discover that it is due and publish the next RabbitMQ task. If the Scheduler is down, execution is late but the deadline is not forgotten.

## 3. An Activity fails twice and then succeeds

Temporal records the failed attempts and durable retry backoff. Workflow code remains suspended on the Activity call until it succeeds or exhausts policy.

The DIY Worker records each attempt and inserts a future `scheduled_tasks` row. The Scheduler publishes it when due. Publishing can be duplicated around a crash, so the consumer remains idempotent.

## 4. The same logical request arrives twice

There is no single blanket "Temporal deduplicates everything" guarantee.

- A stable Workflow ID controls duplicate Workflow starts according to the Workflow ID reuse/conflict policy.
- Retried client requests use request IDs for transport-level deduplication.
- Two independently sent but logically identical Signals may both be accepted; include an application event ID and deduplicate in Workflow state when needed.
- Updates have Update IDs and server-side deduplication within a Workflow Run.
- Activity retries can execute Activity code more than once.

The DIY system likewise needs stable workflow, task, message, and external-operation identifiers for different boundaries.

## 5. Cancellation races the trial deadline

Temporal records both the Signal and Timer Events in a total order for the Workflow. Workflow code observes that order and takes a deterministic branch. Cancellation of already-running Activities remains cooperative.

The DIY API and Scheduler race through PostgreSQL state transitions. Correctness depends on conditional updates such as `UPDATE ... WHERE state = 'WAITING_FOR_TRIAL_END'`, plus task-time state validation.

## 6. Database commit succeeds and RabbitMQ publish fails

The DIY Worker has a dual-write boundary:

```text
commit state in PostgreSQL
publish next task to RabbitMQ
```

A crash between those operations leaves durable state with no queued task. The repository's Reconciler repairs the drift. A stronger production pattern is a transactional outbox: write state and an outbox row in one database transaction, then let a relay publish and mark the outbox row delivered.

Within Temporal, Workflow mutable state and creation of its internal Tasks are managed by the Temporal Service. This removes that application-level database/broker dual write. External systems called by Activities still create their own distributed transaction boundaries.

## 7. Worker dies while holding the DIY lock

RabbitMQ detects the closed consumer connection and redelivers the unacknowledged message. The next Worker may still be blocked by the PostgreSQL lock row until it expires. The Reconciler removes expired locks and repairs state/queue drift.

Temporal Workflow correctness does not depend on an application lock row. Workflow Tasks for one Workflow Execution are serialized by the Service. Sticky execution may cache state on a Worker, but the cache is optional; another Worker can replay History.

## 8. A new PR commit arrives while agents review an old commit

The PR Workflow records the new-commit Signal and updates `latestHeadSha`. It cancels or abandons unfinished old-generation work, records that generation as `SUPERSEDED`, and begins a new quiet window.

Completed old-generation results remain useful for audit and evals but cannot publish. Immediately before publication, an Activity fetches the current GitHub head SHA. The publish Activity also performs a conditional freshness check to close the check-to-use race.

## 9. Flue admits a specialist submission and the Temporal Activity dies

Split integration into two Activities:

```text
dispatch -> receipt
read(receipt) -> terminal result
```

After Temporal records the receipt, retries of `read` reattach to the same Flue submission. The remaining crash window is inside dispatch: Flue may admit the prompt before the Activity returns its receipt. Use a stable create-only conversation/submission identity derived from review generation and specialist so retry resolves or rejects the duplicate instead of silently doing the work twice.

## 10. Flue crashes during an agent tool call

Flue recovery uses durable conversation evidence:

- recorded tool results are reused;
- unresolved ordinary tool calls become explicit unknown-outcome results rather than being blindly repeated;
- delegated tasks reattach to their own transcripts;
- a `durable: true` tool replays completed `step.do` checkpoints and resumes unfinished work.

`step.do` is still exactly-once-recorded and at-least-once-executed. External side effects inside a step require idempotency.

## Failure-injection checklist

Automate tests that kill a process at each named boundary:

- before Activity dispatch;
- after external effect but before Activity completion;
- after Activity completion but before the next Workflow Task;
- during durable Timer wait;
- after Flue admission but before receipt persistence;
- during Flue read;
- during specialist fan-out;
- after stale-head check but before publish;
- after one GitHub comment but before publish Activity completion;
- immediately before Continue-As-New.

For every test, assert external effects, Workflow History, current state, retries, and terminal classification.
