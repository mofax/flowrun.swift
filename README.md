# FlowRun

FlowRun runs sequential Swift workflows with persisted step checkpoints, retries, cancellation, and run snapshots. A completed step returns its saved output during replay, so its operation is not repeated.

## Requirements

- Swift 6.3 or later
- macOS 13+, iOS 16+, tvOS 16+, watchOS 9+, or visionOS 1+
- SQLite for file-backed persistence

## Add the package

Add this checkout as a local Swift package dependency and select its `FlowRun` library product:

```swift
dependencies: [.package(path: "../flowrun.swift")],
targets: [
    .target(
        name: "YourTarget",
        dependencies: [.product(name: "FlowRun", package: "flowrun.swift")]
    )
]
```

## Configure the process-wide runner

Configure `Runner.shared` once at application startup, before starting or reading any run. A second configuration throws `runnerAlreadyConfigured`; use before configuration throws `runnerNotConfigured`.

```swift
import FlowRun

let store = try SQLiteWorkflowPersistence(url: databaseURL) // A file URL in an existing directory.
try await Runner.shared.configure(persistence: store)
```

`InMemoryWorkflowPersistence()` is useful for tests and transient runs. SQLite persists records across restarts, and separate processes can open the same database. Each process has its own `Runner.shared`.

## Run a workflow

```swift
struct NumberWorkflow: Workflow {
    static let identifier = "example.number.v1"

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        let doubled: Int = try await context.step(id: "double") {
            input * 2
        }
        return try await context.step(id: "increment") {
            doubled + 1
        }
    }
}

let handle = try await Runner.shared.start(NumberWorkflow(), input: 21)
let result = try await handle.value() // 43
let saved = try await Runner.shared.output(for: NumberWorkflow.self, id: handle.id) // 43
let snapshot = try await Runner.shared.snapshot(id: handle.id)
```

Inputs, outputs, and step results must be `Codable & Sendable`. `start` executes the decoded representation of its persisted input, and steps and handles return the decoded representation of their saved outputs. Choose stable, nonempty workflow identifiers and step IDs. On replay, a completed step decodes its saved output as the current Swift result type. Incompatible bytes fail decoding before the step operation runs. `Codable` cannot detect a change in meaning when the bytes still decode; change the workflow identifier or step ID if such a change must invalidate older runs.

Runs can execute concurrently. Steps within one run execute serially. Code outside a `context.step` call runs again on resume, including side effects and control flow in the workflow body.

## Retries, cancellation, and results

Use a `RetryPolicy` for transient step failures:

```swift
let value: Int = try await context.step(
    id: "fetch-value",
    retry: RetryPolicy(retries: 2, backoff: .fixed(.milliseconds(250)))
) {
    try await fetchValue()
}
```

`retries` counts attempts after the first. Backoff may be immediate, fixed, or exponential. A `FatalWorkflowError` skips remaining retries. A terminal step failure makes `handle.value()` throw `StepExecutionError`. The snapshot retains portable status, failure kind, and per-step counts. `output(for:id:)` reads a successful result even after its original handle or process is gone; it returns `nil` for a missing or unsuccessful run.

For cooperative cancellation, cancel the handle and await its value before resuming:

```swift
handle.cancel()
_ = try? await handle.value()

if try await Runner.shared.snapshot(id: handle.id)?.status == .suspended {
    let resumed = try await Runner.shared.resume(NumberWorkflow(), id: handle.id)
    let result = try await resumed.value()
}
```

Completed steps replay from saved outputs. The interrupted step executes again. A failure already counted before cancellation during backoff still counts after resume. A fatal or other terminal error that races cancellation remains a failure.

## Timeouts and external effects

The runner heartbeats while a run is active (every 10 seconds by default). Your application can explicitly sweep runs whose heartbeat is older than a chosen threshold:

```swift
let timedOutIDs = try await Runner.shared.timeoutStaleRuns(olderThan: 60)
```

A sweep atomically marks overdue `running` runs `.timedOut` and revokes their owners. A timed-out run is terminal and cannot resume. A crash therefore leaves a run `running` until your application invokes the sweep; FlowRun does not automatically replay a crashed running run. Use a threshold comfortably longer than the configured heartbeat interval.

Retried steps can execute an external effect **more than once**. A process can stop after an effect but before checkpointing its output; a late executor can also continue user code after timeout even though its next checkpoint write is rejected. A terminal timeout may stop a run before a step executes, so FlowRun does not guarantee delivery. Use an idempotency key such as `"\(context.runID):charge"` for retries of the same run and a business-level key when starting a replacement run. Run IDs are available as `context.runID` inside workflow code.

## Development

Run `swift test` from the package directory.
