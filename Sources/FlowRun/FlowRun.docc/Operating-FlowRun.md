# Operating FlowRun

Safely cancel, resume, inspect, and time out durable workflow runs.

## Overview

Configure ``Runner/shared`` once during application startup, before starting or reading a run. Each process has its own runner, even when processes share a SQLite database.

### Cancel and resume a run

Cancellation is cooperative. Cancel the handle and await its value before checking its snapshot. A cancelled active run becomes `suspended` unless a terminal error wins the race.

```swift
handle.cancel()
_ = try? await handle.value()

if try await Runner.shared.snapshot(id: handle.id)?.status == .suspended {
    let resumed = try await Runner.shared.resume(OrderWorkflow(), id: handle.id)
    let receipt = try await resumed.value()
}
```

Completed steps replay their saved output. The interrupted step runs again.

### Sweep stale runs

While a run is active, FlowRun writes a heartbeat at the configured interval, ten seconds by default. Applications must actively call ``Runner/timeoutStaleRuns(olderThan:)`` to mark abandoned `running` runs terminally timed out.

Choose a timeout comfortably longer than the heartbeat interval and monitor the returned run IDs. A timed-out run cannot resume. A process that has lost ownership can still finish user code, but its next checkpoint write is rejected; design effects for that possibility.

### Inspecting state

Use ``Runner/snapshot(id:)`` for status, step counts, timestamps, and portable failure diagnostics. Use ``Runner/output(for:id:)`` only for a successful run and only with its matching workflow type. It returns `nil` for missing or unsuccessful runs.
