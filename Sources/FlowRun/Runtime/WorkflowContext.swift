import Foundation

/// Performs serial, checkpointed steps within one workflow run.
public actor WorkflowContext {
    public nonisolated let runID: RunID
    private let generation: UInt64
    private let persistence: any WorkflowPersistence
    private let retrySleeper: any RetrySleeper
    private let checkpoints: [StepRecord]
    private var cursor = 0
    private var visitedIDs: Set<String> = []
    private var activeStep = false
    private var terminalStepError: StepExecutionError?

    init(
        runID: RunID,
        generation: UInt64,
        persistence: any WorkflowPersistence,
        retrySleeper: any RetrySleeper,
        checkpoints: [StepRecord]
    ) {
        self.runID = runID
        self.generation = generation
        self.persistence = persistence
        self.retrySleeper = retrySleeper
        self.checkpoints = checkpoints
    }

    /// Use for bounded synchronous work. Blocking I/O should use the async overload.
    public func step<Value: Codable & Sendable>(
        id: String,
        retry: RetryPolicy = .none,
        operation: @Sendable () throws -> Value
    ) async throws -> Value {
        try await performStep(id: id, retry: retry) { try operation() }
    }

    public func step<Value: Codable & Sendable>(
        id: String,
        retry: RetryPolicy = .none,
        operation: @Sendable () async throws -> Value
    ) async throws -> Value {
        try await performStep(id: id, retry: retry, operation: operation)
    }

    func validateCompletion() throws {
        if let terminalStepError { throw terminalStepError }
        guard !activeStep else { throw FlowRunError.concurrentStep }
        if cursor < checkpoints.count {
            throw FlowRunError.unfinishedCheckpoint(checkpoints[cursor].id)
        }
    }

    private func performStep<Value: Codable & Sendable>(
        id: String,
        retry: RetryPolicy,
        operation: @Sendable () async throws -> Value
    ) async throws -> Value {
        guard !activeStep else { throw FlowRunError.concurrentStep }
        if let terminalStepError { throw terminalStepError }
        guard !id.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FlowRunError.invalidStepID
        }
        guard !visitedIDs.contains(id) else { throw FlowRunError.duplicateStepID(id) }
        try retry.validate()
        try Task.checkCancellation()

        activeStep = true
        defer { activeStep = false }
        visitedIDs.insert(id)

        let index = cursor
        if index < checkpoints.count {
            let checkpoint = checkpoints[index]
            guard checkpoint.id == id else {
                throw FlowRunError.stepOrderChanged(expected: checkpoint.id, actual: id)
            }
            if checkpoint.status == .succeeded {
                guard let output = checkpoint.output else {
                    throw FlowRunError.invalidState("Completed step has no output")
                }
                do {
                    let value = try JSONDecoder().decode(Value.self, from: output)
                    cursor += 1
                    return value
                } catch {
                    throw FlowRunError.decodingFailed("Step \(id): \(error)")
                }
            }
        }
        cursor += 1

        while true {
            try Task.checkCancellation()
            _ = try await persistence.startStep(
                runID: runID,
                generation: generation,
                index: index,
                stepID: id
            )
            try Task.checkCancellation()
            let value: Value
            do {
                value = try await operation()
            } catch {
                if error is CancellationError { throw error }
                if let flowError = error as? FlowRunError, case .executionRevoked = flowError {
                    throw error
                }
                let kind: FailureKind
                if error is FatalWorkflowError {
                    kind = .fatal
                } else if error is FlowRunError {
                    kind = .runtime
                } else {
                    kind = .step
                }
                let failure = FailureRecord(
                    kind: kind,
                    message: String(describing: error),
                    stepID: id
                )
                let failed = try await persistence.recordStepFailure(
                    runID: runID, generation: generation, index: index, failure: failure
                )
                let stepError = StepExecutionError(
                    stepID: id,
                    attempts: failed.attempts,
                    kind: kind,
                    underlying: error
                )
                if kind != .step || failed.failures > retry.retries {
                    terminalStepError = stepError
                    throw stepError
                }
                let delay = retry.backoff.delay(afterFailure: failed.failures)
                if delay > .zero { try await retrySleeper.sleep(for: delay) }
                continue
            }
            try Task.checkCancellation()
            let encoded: Data
            do {
                encoded = try JSONEncoder().encode(value)
            } catch {
                throw FlowRunError.encodingFailed("Step \(id): \(error)")
            }
            let persistedValue: Value
            do {
                persistedValue = try JSONDecoder().decode(Value.self, from: encoded)
            } catch {
                throw FlowRunError.decodingFailed("Step \(id): \(error)")
            }
            _ = try await persistence.completeStep(
                runID: runID, generation: generation, index: index, output: encoded
            )
            return persistedValue
        }
    }
}
