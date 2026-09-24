# Implement custom persistence

Implement a storage backend that preserves FlowRun's atomicity and ownership guarantees.

## Overview

``WorkflowPersistence`` lets an application replace the included in-memory or SQLite stores.

### Required guarantees

Every state-changing protocol call must be atomic for one run. In particular, creating or claiming a run, starting a step, recording a failure, completing a step, heartbeating, timing out a run, and finishing a run must not expose an intermediate state.

Never execute `Workflow.run` or a `context.step` operation while holding a database transaction or lock. FlowRun calls persistence before and after user code; user code can be slow, suspend, call back into the application, or perform I/O.

### Ownership and multi-process safety

Each running record has an owner generation. Claiming a suspended run increments it. A successful timeout also revokes the active generation. Mutation methods must reject a mismatched or non-running owner with ``FlowRunError/executionRevoked(_:)`` so a late process cannot overwrite a newer result.

``SQLiteWorkflowPersistence`` is the reference implementation for file-backed, multi-process coordination. ``InMemoryWorkflowPersistence`` is useful for tests and transient application state, but discards every run with its instance.

### Validation

Run `PersistenceContractTests` against a new store implementation. Add integration coverage for concurrent claim attempts, cancellation and resume, retry counts, failed checkpoint replay, stale-run timeout, and payload round trips.
