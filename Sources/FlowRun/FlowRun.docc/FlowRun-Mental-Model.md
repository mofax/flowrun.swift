# FlowRun mental model

Understand FlowRun's runs, checkpoints, replay behavior, and concurrency boundaries.

## Overview

FlowRun coordinates one sequential workflow run at a time while allowing independent runs to execute concurrently.

### The main objects

A ``Workflow`` defines a typed input, typed output, and stable identifier. ``Runner`` creates or resumes a run and returns a ``RunHandle``. The workflow receives a ``WorkflowContext`` and wraps durable units of work in `context.step` calls.

Each run has an ID, a status, timestamps, an ownership generation, and an ordered list of step records. A completed step owns a persisted output. A step that has not completed has no reusable output.

### What executes during recovery

On ``Runner/resume(_:id:)``, FlowRun decodes the saved input and invokes `Workflow.run(input:context:)` from the beginning. At each step call, FlowRun compares the current step ID to the next saved checkpoint:

1. A matching completed checkpoint decodes and returns its saved output.
2. A matching unfinished checkpoint runs its operation again.
3. A renamed, reordered, omitted, or incompatible checkpoint fails the run rather than silently changing its history.

As a result, keep effects that must not repeat inside a step, and ensure all code outside a step is safe to execute on every resume.

### Concurrency boundaries

Steps in one context are serial. Calling two steps concurrently produces ``FlowRunError/concurrentStep``. Separate runs may execute concurrently. A persistence implementation atomically changes state for one run but must never hold a transaction or lock while user workflow code executes.
