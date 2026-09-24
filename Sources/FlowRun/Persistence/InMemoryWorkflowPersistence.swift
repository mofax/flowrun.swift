import Foundation

/// Actor-backed persistence for development and tests. Records disappear with this instance.
public actor InMemoryWorkflowPersistence: WorkflowPersistence {
    private var runs: [RunID: RunRecord] = [:]

    public init() {}

    public func createRun(_ record: RunRecord) throws -> RunRecord {
        guard runs[record.id] == nil else { throw FlowRunError.runAlreadyExists(record.id) }
        try RunTransition.validateNew(record)
        runs[record.id] = record
        return record
    }

    public func loadRun(id: RunID) -> RunRecord? { runs[id] }

    public func claimSuspendedRun(id: RunID, workflowID: String) throws -> RunRecord {
        var record = try existing(id)
        try RunTransition.claim(&record, workflowID: workflowID, now: Date())
        runs[id] = record
        return record
    }

    public func heartbeat(runID: RunID, generation: UInt64) throws {
        var record = try existing(runID)
        try RunTransition.heartbeat(&record, generation: generation, now: Date())
        runs[runID] = record
    }

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

    public func completeStep(runID: RunID, generation: UInt64, index: Int, output: Data) throws -> StepRecord {
        var record = try existing(runID)
        let step = try RunTransition.completeStep(
            &record, generation: generation, index: index, output: output, now: Date()
        )
        runs[runID] = record
        return step
    }

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
