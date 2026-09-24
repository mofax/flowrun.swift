import Foundation

/// Actor-backed persistence for development and tests. Records disappear with this instance.
public actor InMemoryWorkflowPersistence: WorkflowPersistence {
    private var runs: [RunID: RunRecord] = [:]

    /// Creates an empty, transient persistence store.
    public init() {}

    /// Atomically stores a new run in memory.
    public func createRun(_ record: RunRecord) throws -> RunRecord {
        guard runs[record.id] == nil else { throw FlowRunError.runAlreadyExists(record.id) }
        try RunTransition.validateNew(record)
        runs[record.id] = record
        return record
    }

    /// Returns an in-memory run record, if present.
    public func loadRun(id: RunID) -> RunRecord? { runs[id] }

    /// Claims a suspended in-memory run for a matching workflow identifier.
    public func claimSuspendedRun(id: RunID, workflowID: String) throws -> RunRecord {
        var record = try existing(id)
        try RunTransition.claim(&record, workflowID: workflowID, now: Date())
        runs[id] = record
        return record
    }

    /// Updates the heartbeat for the current run owner.
    public func heartbeat(runID: RunID, generation: UInt64) throws {
        var record = try existing(runID)
        try RunTransition.heartbeat(&record, generation: generation, now: Date())
        runs[runID] = record
    }

    /// Marks stale running records as timed out.
    public func timeoutStaleRuns(before cutoff: Date) throws -> [RunID] {
        var timedOut: [RunID] = []
        for id in runs.keys {
            guard var record = runs[id] else { continue }
            if try RunTransition.timeout(&record, before: cutoff, now: Date()) {
                runs[id] = record
                timedOut.append(id)
            }
        }
        return timedOut
    }

    /// Starts the next or interrupted checkpoint.
    public func startStep(
        runID: RunID, generation: UInt64, index: Int, stepID: String
    ) throws -> StepRecord {
        var record = try existing(runID)
        let step = try RunTransition.startStep(
            &record, generation: generation, index: index, stepID: stepID, now: Date()
        )
        runs[runID] = record
        return step
    }

    /// Records a failed checkpoint attempt.
    public func recordStepFailure(
        runID: RunID, generation: UInt64, index: Int, failure: FailureRecord
    ) throws -> StepRecord {
        var record = try existing(runID)
        let step = try RunTransition.recordFailure(
            &record, generation: generation, index: index, failure: failure, now: Date()
        )
        runs[runID] = record
        return step
    }

    /// Saves a successful checkpoint output.
    public func completeStep(runID: RunID, generation: UInt64, index: Int, output: Data) throws -> StepRecord {
        var record = try existing(runID)
        let step = try RunTransition.completeStep(
            &record, generation: generation, index: index, output: output, now: Date()
        )
        runs[runID] = record
        return step
    }

    /// Moves a run to a valid terminal state.
    public func finishRun(
        runID: RunID, generation: UInt64, status: RunStatus, output: Data?, failure: FailureRecord?
    ) throws {
        var record = try existing(runID)
        try RunTransition.finish(
            &record, generation: generation, status: status, output: output, failure: failure, now: Date()
        )
        runs[runID] = record
    }

    private func existing(_ id: RunID) throws -> RunRecord {
        guard let record = runs[id] else { throw FlowRunError.runNotFound(id) }
        return record
    }
}
