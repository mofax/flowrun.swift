# Build a resumable order workflow

Create a retryable, checkpointed workflow that can recover after cancellation.

## Overview

Create an order workflow that charges once logically, retries transient service failures, and can resume after cancellation.

### Configure durable storage

Configure the process-wide runner at startup. The database's parent directory must already exist.

```swift
import FlowRun

let persistence = try SQLiteWorkflowPersistence(url: databaseURL)
try await Runner.shared.configure(persistence: persistence)
```

### Define stable models and checkpoints

The workflow and its IDs are part of persisted state. Keep them stable while runs can resume.

```swift
struct Order: Codable, Sendable {
    let id: String
    let amount: Int
}

struct Receipt: Codable, Sendable {
    let chargeID: String
}

struct OrderWorkflow: Workflow {
    static let identifier = "orders.fulfillment.v1"

    func run(input: Order, context: WorkflowContext) async throws -> Receipt {
        let receipt: Receipt = try await context.step(
            id: "charge",
            retry: RetryPolicy(retries: 2, backoff: .fixed(.milliseconds(250)))
        ) {
            try await payments.charge(
                order: input,
                idempotencyKey: "\(context.runID):charge"
            )
        }

        return try await context.step(id: "ship") {
            try await shipping.createShipment(for: receipt)
        }
    }
}
```

### Start and observe the run

```swift
let handle = try await Runner.shared.start(OrderWorkflow(), input: order)
let receipt = try await handle.value()
let snapshot = try await Runner.shared.snapshot(id: handle.id)
```

If `charge` completed before an interruption, resuming returns its saved `Receipt`; the payment operation does not execute again. If `ship` was interrupted, it runs again, so make shipment creation idempotent too.

### Cancel, then resume

```swift
handle.cancel()
_ = try? await handle.value()

if try await Runner.shared.snapshot(id: handle.id)?.status == .suspended {
    let resumed = try await Runner.shared.resume(OrderWorkflow(), id: handle.id)
    let receipt = try await resumed.value()
}
```

In production, periodically call ``Runner/timeoutStaleRuns(olderThan:)`` to terminally close runs abandoned by a crashed process.
