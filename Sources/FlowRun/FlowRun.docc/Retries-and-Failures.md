# Retries and failures

Configure retryable checkpoint work and understand FlowRun's terminal failure behavior.

## Overview

Attach a ``RetryPolicy`` to an individual checkpointed step when its operation can fail transiently.

```swift
let response: Response = try await context.step(
    id: "fetch-customer",
    retry: RetryPolicy(retries: 2, backoff: .exponential(
        initial: .milliseconds(250), multiplier: 2, maximum: .seconds(5)
    ))
) {
    try await client.fetchCustomer()
}
```

`retries` is the number of attempts after the first, so the example runs at most three times. Failures and attempts persist. If cancellation occurs during backoff, the recorded failure budget is retained when the run resumes.

### Terminal failures

Throw ``FatalWorkflowError`` when retrying cannot help. A fatal error skips remaining attempts and backoff. A normal step failure becomes ``StepExecutionError`` after the retry budget is exhausted. Its `stepID`, `attempts`, `kind`, and underlying live error help callers make an immediate decision; a portable ``FailureRecord`` is stored for later inspection.

Errors from FlowRun itself, such as invalid IDs, serialization failures, or changed step order, are runtime failures and are not retried as user-step failures.
