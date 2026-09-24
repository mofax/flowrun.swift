import Foundation

/// The lifecycle state of a persisted workflow run.
public enum RunStatus: String, Codable, Sendable {
    /// The run is actively owned and may write checkpoints.
    case running
    /// Cooperative cancellation stopped the run and allows it to resume.
    case suspended
    /// Every step and the final output completed successfully.
    case succeeded
    /// A terminal workflow, runtime, or step failure ended the run.
    case failed
    /// A stale-run sweep revoked the run's owner and ended it permanently.
    case timedOut
}

/// The lifecycle state of a persisted checkpoint.
public enum StepStatus: String, Codable, Sendable {
    /// The step operation is currently executing.
    case running
    /// The step was interrupted and will execute again when the run resumes.
    case suspended
    /// The step output was persisted and can replay on resume.
    case succeeded
    /// The step's most recent attempt failed.
    case failed
}

/// The portable category of a terminal or most-recent failure.
public enum FailureKind: String, Codable, Sendable {
    /// A user step operation exhausted its retry budget.
    case step
    /// A ``FatalWorkflowError`` bypassed retry.
    case fatal
    /// Workflow body code outside a terminal step threw an error.
    case workflow
    /// FlowRun validation, serialization, or persistence behavior failed.
    case runtime
    /// A stale heartbeat caused the run to time out.
    case timeout
}

/// Portable diagnostic data. The original Swift error remains available to the live caller.
public struct FailureRecord: Codable, Sendable, Equatable {
    /// The category used for later diagnostics.
    public let kind: FailureKind
    /// A portable string representation of the original error.
    public let message: String
    /// The failed step, when the failure belongs to a checkpoint.
    public let stepID: String?

    /// Creates portable diagnostic data for a failure.
    public init(kind: FailureKind, message: String, stepID: String? = nil) {
        self.kind = kind
        self.message = message
        self.stepID = stepID
    }
}

/// Persisted state and output for one ordered checkpoint.
public struct StepRecord: Codable, Sendable {
    /// The stable identifier supplied to `context.step`.
    public let id: String
    /// The zero-based position of this step in its run.
    public let index: Int
    /// The checkpoint's current state.
    public var status: StepStatus
    /// JSON-encoded output, present only after a successful checkpoint.
    public var output: Data?
    /// The number of times FlowRun began this checkpoint.
    public var attempts: Int
    /// The number of failed attempts for this checkpoint.
    public var failures: Int
    /// Diagnostic data from the most recent failed attempt.
    public var lastFailure: FailureRecord?

    /// Creates a checkpoint record, primarily for persistence implementations.
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

/// The complete persisted representation of a workflow run.
public struct RunRecord: Codable, Sendable {
    /// The unique ID of this run.
    public let id: RunID
    /// The stable workflow identifier that owns this run.
    public let workflowID: String
    /// JSON-encoded workflow input.
    public let input: Data
    /// The run's current lifecycle state.
    public var status: RunStatus
    /// Ordered persisted checkpoint records.
    public var steps: [StepRecord]
    /// JSON-encoded final output for a successful run.
    public var output: Data?
    /// Portable terminal failure data for an unsuccessful run.
    public var failure: FailureRecord?
    /// When the run was first persisted.
    public let createdAt: Date
    /// When any persisted state last changed.
    public var updatedAt: Date
    /// The generation authorized to mutate a currently running record.
    public var ownerGeneration: UInt64
    /// The most recent activity heartbeat from the active owner.
    public var heartbeatAt: Date

    /// Creates a new running record with no checkpoints or final result.
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

/// Read-only, payload-free diagnostic state for one checkpoint.
public struct StepSnapshot: Sendable {
    /// The stable checkpoint identifier.
    public let id: String
    /// The zero-based checkpoint position.
    public let index: Int
    /// The checkpoint's current state.
    public let status: StepStatus
    /// The number of started attempts.
    public let attempts: Int
    /// The number of failed attempts.
    public let failures: Int
    /// The most recent portable failure, if any.
    public let lastFailure: FailureRecord?
}

/// Read-only, payload-free diagnostic state for a workflow run.
public struct RunSnapshot: Sendable {
    /// The unique run identifier.
    public let id: RunID
    /// The stable workflow identifier.
    public let workflowID: String
    /// The run's lifecycle state.
    public let status: RunStatus
    /// Ordered checkpoint diagnostics.
    public let steps: [StepSnapshot]
    /// Portable terminal failure details, if any.
    public let failure: FailureRecord?
    /// When the run was created.
    public let createdAt: Date
    /// When persisted state last changed.
    public let updatedAt: Date
    /// The active owner's most recent heartbeat.
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
    /// Atomically stores a new running record or throws when its ID already exists.
    func createRun(_ record: RunRecord) async throws -> RunRecord
    /// Returns the complete persisted record for a run, if it exists.
    func loadRun(id: RunID) async throws -> RunRecord?
    /// Atomically claims a suspended matching workflow and advances its owner generation.
    func claimSuspendedRun(id: RunID, workflowID: String) async throws -> RunRecord
    /// Records activity for the current owner generation of a running run.
    func heartbeat(runID: RunID, generation: UInt64) async throws
    /// Atomically times out running records whose heartbeat is no later than `cutoff`.
    func timeoutStaleRuns(before cutoff: Date) async throws -> [RunID]
    /// Atomically begins the next or interrupted checkpoint for the current owner.
    func startStep(runID: RunID, generation: UInt64, index: Int, stepID: String) async throws -> StepRecord
    /// Atomically records one checkpoint failure for the current owner.
    func recordStepFailure(runID: RunID, generation: UInt64, index: Int, failure: FailureRecord) async throws -> StepRecord
    /// Atomically saves a successful checkpoint output for the current owner.
    func completeStep(runID: RunID, generation: UInt64, index: Int, output: Data) async throws -> StepRecord
    /// Atomically moves a run to its successful, suspended, or failed terminal state.
    func finishRun(runID: RunID, generation: UInt64, status: RunStatus, output: Data?, failure: FailureRecord?) async throws
}
