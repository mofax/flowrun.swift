import Foundation

public enum RunStatus: String, Codable, Sendable {
    case running
    case suspended
    case succeeded
    case failed
    case timedOut
}

public enum StepStatus: String, Codable, Sendable {
    case running
    case suspended
    case succeeded
    case failed
}

public enum FailureKind: String, Codable, Sendable {
    case step
    case fatal
    case workflow
    case runtime
    case timeout
}

/// Portable diagnostic data. The original Swift error remains available to the live caller.
public struct FailureRecord: Codable, Sendable, Equatable {
    public let kind: FailureKind
    public let message: String
    public let stepID: String?

    public init(kind: FailureKind, message: String, stepID: String? = nil) {
        self.kind = kind
        self.message = message
        self.stepID = stepID
    }
}

public struct StepRecord: Codable, Sendable {
    public let id: String
    public let index: Int
    public var status: StepStatus
    public var output: Data?
    public var attempts: Int
    public var failures: Int
    public var lastFailure: FailureRecord?

    public init(
        id: String,
        index: Int,
        status: StepStatus,
        output: Data? = nil,
        attempts: Int = 0,
        failures: Int = 0,
        lastFailure: FailureRecord? = nil
    ) {
        self.id = id
        self.index = index
        self.status = status
        self.output = output
        self.attempts = attempts
        self.failures = failures
        self.lastFailure = lastFailure
    }
}

public struct RunRecord: Codable, Sendable {
    public let id: RunID
    public let workflowID: String
    public let input: Data
    public var status: RunStatus
    public var steps: [StepRecord]
    public var output: Data?
    public var failure: FailureRecord?
    public let createdAt: Date
    public var updatedAt: Date
    public var ownerGeneration: UInt64
    public var heartbeatAt: Date

    public init(id: RunID, workflowID: String, input: Data, createdAt: Date = Date()) {
        self.id = id
        self.workflowID = workflowID
        self.input = input
        self.status = .running
        self.steps = []
        self.output = nil
        self.failure = nil
        self.createdAt = createdAt
        self.updatedAt = createdAt
        self.ownerGeneration = 1
        self.heartbeatAt = createdAt
    }
}

public struct StepSnapshot: Sendable {
    public let id: String
    public let index: Int
    public let status: StepStatus
    public let attempts: Int
    public let failures: Int
    public let lastFailure: FailureRecord?
}

public struct RunSnapshot: Sendable {
    public let id: RunID
    public let workflowID: String
    public let status: RunStatus
    public let steps: [StepSnapshot]
    public let failure: FailureRecord?
    public let createdAt: Date
    public let updatedAt: Date
    public let heartbeatAt: Date

    init(_ record: RunRecord) {
        id = record.id
        workflowID = record.workflowID
        status = record.status
        steps = record.steps.map {
            StepSnapshot(
                id: $0.id,
                index: $0.index,
                status: $0.status,
                attempts: $0.attempts,
                failures: $0.failures,
                lastFailure: $0.lastFailure
            )
        }
        failure = record.failure
        createdAt = record.createdAt
        updatedAt = record.updatedAt
        heartbeatAt = record.heartbeatAt
    }
}

/// Each method that changes state must be atomic for one run. Implementations must not
/// hold a lock or transaction while executing user-supplied workflow or step code.
public protocol WorkflowPersistence: Sendable {
    func createRun(_ record: RunRecord) async throws -> RunRecord
    func loadRun(id: RunID) async throws -> RunRecord?
    func claimSuspendedRun(id: RunID, workflowID: String) async throws -> RunRecord
    func heartbeat(runID: RunID, generation: UInt64) async throws
    func timeoutStaleRuns(before cutoff: Date) async throws -> [RunID]
    func startStep(runID: RunID, generation: UInt64, index: Int, stepID: String) async throws -> StepRecord
    func recordStepFailure(runID: RunID, generation: UInt64, index: Int, failure: FailureRecord) async throws -> StepRecord
    func completeStep(runID: RunID, generation: UInt64, index: Int, output: Data) async throws -> StepRecord
    func finishRun(runID: RunID, generation: UInt64, status: RunStatus, output: Data?, failure: FailureRecord?) async throws
}
