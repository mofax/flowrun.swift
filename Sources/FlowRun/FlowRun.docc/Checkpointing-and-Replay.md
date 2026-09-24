# Checkpointing and replay

Keep persisted workflow inputs, checkpoints, and outputs compatible across recovery.

## Overview

Completed checkpoints are FlowRun's recovery boundary.

### Stable identities

Use a stable, nonempty ``Workflow/identifier`` and stable, nonempty step IDs. A workflow identifier selects the persisted input and run history. Step IDs and their order identify each persisted checkpoint.

```swift
struct OrderWorkflow: Workflow {
    static let identifier = "orders.fulfillment.v1"

    func run(input: Order, context: WorkflowContext) async throws -> Receipt {
        let receipt: Receipt = try await context.step(id: "charge") {
            try await payments.charge(input)
        }
        return try await context.step(id: "ship") {
            try await shipping.createShipment(for: receipt)
        }
    }
}
```

### Persisted values are the live values

Inputs, completed step values, and final outputs must conform to `Codable & Sendable`. FlowRun JSON-encodes and then decodes them before exposing them to workflow code or callers. This makes live and later-loaded values behave the same way, but an encoding or decoding failure fails the run.

`Codable` compatibility is not semantic compatibility. A data shape can still decode after its business meaning changes. If a change must invalidate old runs, version the workflow identifier or affected step ID. If old runs must keep working, retain compatible decoding and the same step sequence.

### Safe changes

Adding a new step after all existing steps is generally safe for a completed run only when that run will never resume. For resumable runs, preserve every existing checkpoint in sequence. Test a suspended old run against the new workflow before releasing a compatibility-sensitive change.
