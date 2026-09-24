# External effects and idempotency

Design side-effecting steps for FlowRun's durable, at-least-once execution model.

## Overview

FlowRun provides durable checkpoints, not exactly-once effect delivery.

An operation can perform an external effect and then stop before its result is checkpointed. Retrying or resuming then invokes the operation again. A timed-out executor can also continue user code after FlowRun has revoked its ownership. Conversely, a timeout can happen before an operation executes. Do not infer a delivery guarantee from a run's final state.

### Use idempotency keys

For retries of one effect within one run, derive a key from the run ID and the logical effect:

```swift
let receipt: Receipt = try await context.step(id: "charge") {
    try await payments.charge(
        order: order,
        idempotencyKey: "\(context.runID):charge"
    )
}
```

Use a business-level key when a replacement run represents the same real-world effect. Persist or delegate idempotency to the external system when possible. Keep the scope of a key narrow enough to distinguish legitimately separate effects.
