# Architecture Walkthrough

The repository contains two layers of learning:

1. A subscription workflow implemented with Temporal and with a DIY PostgreSQL/RabbitMQ orchestrator.
2. A stateful multi-agent PR-review workflow that combines Temporal lifecycle orchestration with Flue agent submissions.

## Temporal application path

```mermaid
sequenceDiagram
    participant C as Client
    participant T as Temporal Service
    participant W as Workflow Worker
    participant A as Activity Worker
    participant X as External system

    C->>T: Start Workflow
    T->>W: Workflow Task with History
    W->>W: Replay deterministic code
    W->>T: Command: Schedule Activity
    T->>A: Activity Task
    A->>X: External operation with idempotency key
    X-->>A: Result
    A->>T: Complete Activity
    T->>W: New Workflow Task
    W->>W: Replay and inject recorded result
```

The Temporal Service executes neither Workflow nor Activity application code. Application Workers poll Task Queues. Workflow Workers reconstruct state and emit Commands. Activity Workers perform I/O.

## What is persisted

Temporal persists ordered Event History and service-managed mutable state. History contains Workflow inputs, Activity scheduling and results, Timers, Signals, and terminal Events. It does not contain every local assignment or every JavaScript `await`.

Sticky execution may cache a Workflow's in-memory state on a Worker for speed. The cache is disposable. History remains authoritative.

## DIY path

See [DIY Orchestrator Architecture](diy-architecture.md). Its explicit state column plays the role of a program counter. RabbitMQ transports ready work. PostgreSQL stores timers, delayed retries, locks, attempts, and repair state.

## Multi-agent path

```mermaid
sequenceDiagram
    participant G as GitHub
    participant T as Temporal PR Workflow
    participant R as Repository Activity
    participant F as Flue
    participant P as Publisher

    G->>T: SignalWithStart(new commit)
    T->>T: Durable quiet-window Timer
    T->>R: Prepare immutable snapshot
    R-->>T: Artifact references
    par specialists
        T->>F: Dispatch authorization agent
        T->>F: Dispatch injection agent
        T->>F: Dispatch secrets agent
    end
    F-->>T: Durable receipts, then settled results
    T->>T: Judge, deduplicate, stable finding IDs
    T->>G: Read current head SHA
    alt reviewed SHA is current
        T->>P: Reconcile comments idempotently
    else superseded
        T->>T: Discard publication and debounce latest SHA
    end
```

Read [Building Stateful Multi-Agent Systems with Temporal and Flue](flue-temporal-agentic-harness.md) for state ownership, cancellation, crash windows, Continue-As-New, security boundaries, and evaluation design.

## Guarantee boundaries

| Boundary | Guarantee | Remaining responsibility |
|---|---|---|
| Workflow replay | Deterministically reconstruct logical state | Version Workflow code safely |
| Completed Activity in History | Replay reuses the result | In-flight Activity may execute again |
| Activity retry | Durable retry scheduling | Idempotent external effects |
| Signal | Durable message in History | Application event IDs for logical duplicates |
| Flue admission | One durable terminal outcome for admitted submission | Make dispatch admission retry-safe |
| Flue `read(receipt)` | Reattach to submission state | Persist receipt before later steps |
| Flue durable tool | Completed `step.do` results replay | Step function may execute more than once |
| GitHub publication | None automatically | Stable IDs, freshness check, reconciliation |
