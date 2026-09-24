import Foundation

public struct RunHandle<Output: Codable & Sendable>: Sendable {
    public let id: RunID
    private let task: Task<Output, Error>

    init(id: RunID, task: Task<Output, Error>) {
        self.id = id
        self.task = task
    }

    public func value() async throws -> Output { try await task.value }

    /// Requests cancellation. Await value() before resuming to observe the suspended state.
    public func cancel() { task.cancel() }
}

/// The one workflow runner in this process. Configure it once at startup.
public actor Runner {
    public static let shared = Runner()
    private var engine: RunEngine?

    private init() {}

    public func configure(
        persistence: any WorkflowPersistence,
        heartbeatInterval: Duration = .seconds(10)
    ) throws {
        guard engine == nil else { throw FlowRunError.runnerAlreadyConfigured }
        guard heartbeatInterval > .zero else { throw FlowRunError.invalidHeartbeatInterval }
        engine = RunEngine(persistence: persistence, heartbeatInterval: heartbeatInterval)
    }

    @discardableResult
    public func start<W: Workflow>(
        _ workflow: W, input: W.Input, id: RunID = RunID()
    ) async throws -> RunHandle<W.Output> {
        try await configuredEngine().start(workflow, input: input, id: id)
    }

    @discardableResult
    public func resume<W: Workflow>(_ workflow: W, id: RunID) async throws -> RunHandle<W.Output> {
        try await configuredEngine().resume(workflow, id: id)
    }

    public func snapshot(id: RunID) async throws -> RunSnapshot? {
        try await configuredEngine().snapshot(id: id)
    }

    public func output<W: Workflow>(for workflow: W.Type, id: RunID) async throws -> W.Output? {
        try await configuredEngine().output(for: workflow, id: id)
    }

    /// Explicitly marks overdue running runs as terminally timed out.
    public func timeoutStaleRuns(olderThan seconds: TimeInterval) async throws -> [RunID] {
        try await configuredEngine().timeoutStaleRuns(olderThan: seconds)
    }

    private func configuredEngine() throws -> RunEngine {
        guard let engine else { throw FlowRunError.runnerNotConfigured }
        return engine
    }
}

/// Injectable execution core; only Runner.shared exposes it to package clients.
struct RunEngine: Sendable {
    let persistence: any WorkflowPersistence
    let retrySleeper: any RetrySleeper
    let heartbeatInterval: Duration

    init(
        persistence: any WorkflowPersistence,
        retrySleeper: any RetrySleeper = TaskRetrySleeper(),
        heartbeatInterval: Duration = .seconds(10)
    ) {
        self.persistence = persistence
        self.retrySleeper = retrySleeper
        self.heartbeatInterval = heartbeatInterval
    }

    @discardableResult
    func start<W: Workflow>(
        _ workflow: W, input: W.Input, id: RunID = RunID()
    ) async throws -> RunHandle<W.Output> {
        guard !W.identifier.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FlowRunError.invalidWorkflowIdentifier
        }
        let inputData: Data
        do {
            inputData = try JSONEncoder().encode(input)
        } catch {
            throw FlowRunError.encodingFailed("Workflow input: \(error)")
        }
        let persistedInput: W.Input
        do {
            persistedInput = try JSONDecoder().decode(W.Input.self, from: inputData)
        } catch {
            throw FlowRunError.decodingFailed("Workflow input: \(error)")
        }
        let record = try await persistence.createRun(
            RunRecord(id: id, workflowID: W.identifier, input: inputData)
        )
        return launch(workflow, input: persistedInput, record: record)
    }

    @discardableResult
    func resume<W: Workflow>(_ workflow: W, id: RunID) async throws -> RunHandle<W.Output> {
        let record = try await persistence.claimSuspendedRun(id: id, workflowID: W.identifier)
        let input: W.Input
        do {
            input = try JSONDecoder().decode(W.Input.self, from: record.input)
        } catch {
            let failure = FailureRecord(kind: .runtime, message: "Workflow input cannot be decoded: \(error)")
            try await persistence.finishRun(
                runID: id, generation: record.ownerGeneration, status: .failed, output: nil, failure: failure
            )
            throw FlowRunError.decodingFailed("Workflow input: \(error)")
        }
        return launch(workflow, input: input, record: record)
    }

    func snapshot(id: RunID) async throws -> RunSnapshot? {
        guard let record = try await persistence.loadRun(id: id) else { return nil }
        return RunSnapshot(record)
    }

    func output<W: Workflow>(for workflow: W.Type, id: RunID) async throws -> W.Output? {
        guard let record = try await persistence.loadRun(id: id) else { return nil }
        guard record.workflowID == W.identifier else {
            throw FlowRunError.workflowMismatch(expected: record.workflowID, actual: W.identifier)
        }
        guard record.status == .succeeded else { return nil }
        guard let bytes = record.output else {
            throw FlowRunError.invalidState("Successful run has no output")
        }
        do {
            return try JSONDecoder().decode(W.Output.self, from: bytes)
        } catch {
            throw FlowRunError.decodingFailed("Workflow output: \(error)")
        }
    }

    func timeoutStaleRuns(olderThan seconds: TimeInterval) async throws -> [RunID] {
        guard seconds.isFinite, seconds > 0 else { throw FlowRunError.invalidTimeout }
        return try await persistence.timeoutStaleRuns(before: Date().addingTimeInterval(-seconds))
    }

    private func launch<W: Workflow>(
        _ workflow: W, input: W.Input, record: RunRecord
    ) -> RunHandle<W.Output> {
        let persistence = self.persistence
        let retrySleeper = self.retrySleeper
        let heartbeatInterval = self.heartbeatInterval
        let id = record.id
        let generation = record.ownerGeneration
        let task = Task.detached {
            try await Self.execute(
                workflow, input: input, id: id, generation: generation,
                checkpoints: record.steps, persistence: persistence, retrySleeper: retrySleeper
            )
        }
        let heartbeat = Task.detached {
            do {
                while true {
                    try await Task.sleep(for: heartbeatInterval)
                    try Task.checkCancellation()
                    try await persistence.heartbeat(runID: id, generation: generation)
                }
            } catch is CancellationError {
                // The execution task completed.
            } catch {
                // Execution must stop if its ownership can no longer be maintained.
                task.cancel()
            }
        }
        Task.detached {
            _ = await task.result
            heartbeat.cancel()
        }
        return RunHandle(id: id, task: task)
    }

    private static func execute<W: Workflow>(
        _ workflow: W,
        input: W.Input,
        id: RunID,
        generation: UInt64,
        checkpoints: [StepRecord],
        persistence: any WorkflowPersistence,
        retrySleeper: any RetrySleeper
    ) async throws -> W.Output {
        let context = WorkflowContext(
            runID: id, generation: generation, persistence: persistence,
            retrySleeper: retrySleeper, checkpoints: checkpoints
        )
        do {
            try Task.checkCancellation()
            let output = try await workflow.run(input: input, context: context)
            try await context.validateCompletion()
            try Task.checkCancellation()
            let outputData: Data
            do {
                outputData = try JSONEncoder().encode(output)
            } catch {
                throw FlowRunError.encodingFailed("Workflow output: \(error)")
            }
            let persistedOutput: W.Output
            do {
                persistedOutput = try JSONDecoder().decode(W.Output.self, from: outputData)
            } catch {
                throw FlowRunError.decodingFailed("Workflow output: \(error)")
            }
            try Task.checkCancellation()
            try await persistence.finishRun(
                runID: id, generation: generation, status: .succeeded, output: outputData, failure: nil
            )
            return persistedOutput
        } catch {
            if let flowError = error as? FlowRunError, case .executionRevoked = flowError {
                throw error
            }
            if error is CancellationError {
                try await persistence.finishRun(
                    runID: id, generation: generation, status: .suspended, output: nil, failure: nil
                )
                throw CancellationError()
            }
            let failure: FailureRecord
            if let stepError = error as? StepExecutionError {
                failure = FailureRecord(
                    kind: stepError.kind,
                    message: String(describing: stepError.underlying),
                    stepID: stepError.stepID
                )
            } else if let fatal = error as? FatalWorkflowError {
                failure = FailureRecord(kind: .fatal, message: String(describing: fatal.underlying))
            } else if error is FlowRunError {
                failure = FailureRecord(kind: .runtime, message: String(describing: error))
            } else {
                failure = FailureRecord(kind: .workflow, message: String(describing: error))
            }
            try await persistence.finishRun(
                runID: id, generation: generation, status: .failed, output: nil, failure: failure
            )
            throw error
        }
    }
}
