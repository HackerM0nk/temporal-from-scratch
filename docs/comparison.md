# Side-by-Side Comparison: Temporal and the DIY System

This comparison is scoped to the subscription example. The DIY code demonstrates the mechanisms an application team starts assembling. It does not reproduce Temporal Server's full distributed-systems guarantees.

## Concept mapping

| Concept | Temporal | DIY implementation |
|---|---|---|
| Workflow | Deterministic function reconstructed from History | Row in `subscription_workflows` plus an explicit state machine |
| Activity | Retryable external operation dispatched by Temporal | Function invoked by the RabbitMQ consumer |
| Event History | Authoritative ordered event stream used for replay | `workflow_events` audit log; current state is authoritative |
| Durable timer | Server Timer Event | `trial_end_at` or `scheduled_tasks.execute_after` plus Scheduler polling |
| Ready task delivery | Temporal Task Queue and Worker polling | Durable RabbitMQ queue with manual acknowledgements |
| Delayed retry | Temporal retry state and Timer | PostgreSQL `scheduled_tasks` row promoted by Scheduler |
| Worker failure | Workflow Task replay; Activity timeout or heartbeat | RabbitMQ redelivery, lock expiry, and Reconciler backstop |
| Process state | Rebuilt from code and Event History | Explicit `state` and metadata columns |
| Completed-step replay | Recorded Activity result is injected during replay | `idempotency_keys` result is read and the task is skipped |
| External side-effect safety | Application-level idempotency or reconciliation | Application-level idempotency or reconciliation |
| Cancellation request | Durable Signal plus cooperative cancellation | API state transition plus cancellation task |
| Visibility | Temporal UI, APIs, Search Attributes, metrics | SQL queries, RabbitMQ UI, logs |

## The key distinction: replay is not retry

Workflow replay re-executes deterministic orchestration code to reconstruct state. It does not re-run a completed Activity merely because replay reached the corresponding call.

Activity retry is different. If an Activity made an external change but failed before Temporal recorded completion, it may execute again. The external API still needs a stable idempotency key or a reconciliation algorithm.

The DIY system has the same ambiguity. A consumer can send an email and die before committing its idempotency record. RabbitMQ redelivers the message, so the email provider must recognize the same operation key.

## Starting a Workflow

Temporal:

```ts
await client.workflow.start(subscriptionWorkflow, {
  workflowId: `subscription-${customerId}`,
  taskQueue: 'subscription-workflows',
  args: [customerId],
});
```

DIY:

```text
transaction:
  insert subscription_workflows row
  insert workflow_events row
publish SEND_WELCOME_EMAIL to RabbitMQ
```

The DIY API has a database and broker dual write. A production design normally uses a transactional outbox or an equivalent repair mechanism. This repository keeps the Reconciler visible because it is educational.

## Durable waiting

Temporal:

```ts
const cancelled = await condition(() => isCancelled, 30_000);
```

Temporal persists the Timer. No Worker remains occupied while waiting.

DIY:

```text
store trial_end_at in PostgreSQL
Scheduler polls due rows
Scheduler publishes CHARGE_MONTHLY_FEE to RabbitMQ
```

If the Scheduler is unavailable, the deadline remains durable but execution begins late.

## Retry scheduling

Temporal records attempt state and backoff durably from Activity options.

The DIY Worker records a failed `activity_attempts` row and inserts a `scheduled_tasks` row. The Scheduler later publishes the due task to RabbitMQ and marks the scheduled row published.

Publishing and marking `published_at` are not one atomic operation. A crash can therefore produce either a duplicate publication or a row that needs reconciliation. Consumers must remain idempotent.

## Locking is not sticky execution

The DIY `workflow_locks` table prevents two consumers from advancing one workflow simultaneously.

Temporal sticky execution is primarily a performance optimization: it routes a Workflow's next Workflow Task back to a Worker that has cached its in-memory state. Correctness does not depend on that cache. If sticky routing is unavailable, another Worker rebuilds state from History.

These concepts should not be presented as equivalent.

## What the simplified DIY version still omits

- replicated control-plane behavior;
- shard ownership and failover;
- durable visibility indexing;
- Worker Build ID and Workflow Versioning strategies;
- namespace isolation and quotas;
- archival and retention;
- multi-cluster replication;
- robust transactional outbox semantics;
- production dead-letter and operator recovery workflows;
- payload codecs, encryption, and data-converter compatibility;
- exhaustive race testing.

That omission is the lesson. Correct workflow execution is a platform-sized responsibility, not only a state enum plus a queue.
