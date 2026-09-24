# ``FlowRun``

Build durable, sequential Swift workflows with checkpointed steps.

## Overview

FlowRun persists a workflow's input and the output of each completed step. When a suspended run resumes, completed steps return their saved output and do not execute their operations again. The workflow body itself starts again, so code outside a `context.step` call must be safe to repeat.

Use FlowRun for application workflows that need observable state, cooperative cancellation, retries, and recovery after a process restart. Side effects are at-least-once: use idempotency keys for calls such as charging a card or sending a request to another system.

### Essentials

- <doc:Build-a-Resumable-Order-Workflow>
- <doc:FlowRun-Mental-Model>
- <doc:Checkpointing-and-Replay>

### Operations

- <doc:Operating-FlowRun>
- <doc:Retries-and-Failures>
- <doc:External-Effects-and-Idempotency>

### Extending FlowRun

- <doc:Custom-Persistence>

## Topics

### Defining and running workflows

- ``Workflow``
- ``Runner``
- ``RunHandle``
- ``WorkflowContext``
- ``RunID``

### Retry and failure behavior

- ``RetryPolicy``
- ``BackoffStrategy``
- ``FatalWorkflowError``
- ``StepExecutionError``
- ``FlowRunError``

### Persistence and diagnostics

- ``WorkflowPersistence``
- ``SQLiteWorkflowPersistence``
- ``InMemoryWorkflowPersistence``
- ``RunSnapshot``
- ``StepSnapshot``
- ``RunStatus``
- ``StepStatus``
- ``FailureRecord``
- ``FailureKind``
