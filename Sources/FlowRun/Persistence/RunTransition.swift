import Foundation

/// The only place where persisted run and step state may change. Stores make
/// each call atomic; no user-supplied operation runs inside a store transaction.
enum RunTransition {
    static func validateNew(_ record: RunRecord) throws {
        guard record.status == .running,
              record.steps.isEmpty,
              record.output == nil,
              record.failure == nil,
              record.ownerGeneration == 1 else {
            throw FlowRunError.invalidState("A new run must be running with no steps")
        }
    }

    static func claim(_ record: inout RunRecord, workflowID: String, now: Date) throws {
        guard record.workflowID == workflowID else {
            throw FlowRunError.workflowMismatch(expected: record.workflowID, actual: workflowID)
        }
        guard record.status == .suspended else { throw FlowRunError.runNotSuspended(record.id) }
        guard record.ownerGeneration < UInt64.max else {
            throw FlowRunError.invalidState("Owner generation exhausted")
        }
        record.ownerGeneration += 1
        record.status = .running
        record.heartbeatAt = now
        record.updatedAt = now
    }

    static func heartbeat(_ record: inout RunRecord, generation: UInt64, now: Date) throws {
        try requireOwner(record, generation: generation)
        record.heartbeatAt = now
        record.updatedAt = now
    }

    static func startStep(
        _ record: inout RunRecord,
        generation: UInt64,
        index: Int,
        stepID: String,
        now: Date
    ) throws -> StepRecord {
        try requireOwner(record, generation: generation)
        guard !stepID.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FlowRunError.invalidStepID
        }
        guard index >= 0, index <= record.steps.count else {
            throw FlowRunError.invalidState("Step index is not the next checkpoint")
        }
        if index == record.steps.count {
            guard record.steps.allSatisfy({ $0.status == .succeeded }) else {
                throw FlowRunError.invalidState("An earlier step is unfinished")
            }
            record.steps.append(StepRecord(id: stepID, index: index, status: .running))
        } else {
            guard record.steps[..<index].allSatisfy({ $0.status == .succeeded }) else {
                throw FlowRunError.invalidState("An earlier step is unfinished")
            }
            guard record.steps[index].id == stepID else {
                throw FlowRunError.stepOrderChanged(expected: record.steps[index].id, actual: stepID)
            }
            guard record.steps[index].status == .failed || record.steps[index].status == .suspended else {
                throw FlowRunError.invalidState("Only a failed or suspended step can be restarted")
            }
        }
        record.steps[index].status = .running
        record.steps[index].attempts += 1
        record.updatedAt = now
        return record.steps[index]
    }

    static func recordFailure(
        _ record: inout RunRecord,
        generation: UInt64,
        index: Int,
        failure: FailureRecord,
        now: Date
    ) throws -> StepRecord {
        try requireOwner(record, generation: generation)
        guard record.steps.indices.contains(index), record.steps[index].status == .running else {
            throw FlowRunError.invalidState("Only a running step can fail")
        }
        record.steps[index].status = .failed
        record.steps[index].failures += 1
        record.steps[index].lastFailure = failure
        record.updatedAt = now
        return record.steps[index]
    }

    static func completeStep(
        _ record: inout RunRecord,
        generation: UInt64,
        index: Int,
        output: Data,
        now: Date
    ) throws -> StepRecord {
        try requireOwner(record, generation: generation)
        guard record.steps.indices.contains(index), record.steps[index].status == .running else {
            throw FlowRunError.invalidState("Only a running step can complete")
        }
        record.steps[index].output = output
        record.steps[index].status = .succeeded
        record.updatedAt = now
        return record.steps[index]
    }

    static func finish(
        _ record: inout RunRecord,
        generation: UInt64,
        status: RunStatus,
        output: Data?,
        failure: FailureRecord?,
        now: Date
    ) throws {
        try requireOwner(record, generation: generation)
        switch status {
        case .succeeded:
            guard output != nil, failure == nil, record.steps.allSatisfy({ $0.status == .succeeded }) else {
                throw FlowRunError.invalidState("Successful run has unfinished work")
            }
        case .suspended:
            guard output == nil, failure == nil else {
                throw FlowRunError.invalidState("Suspended run cannot have a result")
            }
            if let index = record.steps.indices.last,
               record.steps[index].status == .running || record.steps[index].status == .failed {
                record.steps[index].status = .suspended
            }
        case .failed:
            guard failure != nil, output == nil else {
                throw FlowRunError.invalidState("Failed run requires failure details")
            }
            if let index = record.steps.indices.last, record.steps[index].status == .running {
                record.steps[index].status = .failed
                record.steps[index].lastFailure = failure
            }
        case .running, .timedOut:
            throw FlowRunError.invalidState("Invalid finish status")
        }
        record.status = status
        record.output = output
        record.failure = failure
        record.updatedAt = now
    }

    @discardableResult
    static func timeout(_ record: inout RunRecord, before cutoff: Date, now: Date) throws -> Bool {
        guard record.status == .running, record.heartbeatAt <= cutoff else { return false }
        guard record.ownerGeneration < UInt64.max else {
            throw FlowRunError.invalidState("Owner generation exhausted")
        }
        let failure = FailureRecord(kind: .timeout, message: "Run heartbeat expired")
        if let index = record.steps.indices.last, record.steps[index].status == .running {
            record.steps[index].status = .failed
            record.steps[index].lastFailure = failure
        }
        record.ownerGeneration += 1
        record.status = .timedOut
        record.failure = failure
        record.updatedAt = now
        return true
    }

    private static func requireOwner(_ record: RunRecord, generation: UInt64) throws {
        guard record.status == .running, record.ownerGeneration == generation else {
            throw FlowRunError.executionRevoked(record.id)
        }
    }
}
