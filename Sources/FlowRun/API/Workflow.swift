import Foundation

/// A workflow definition. Pass the same definition when resuming a suspended run.
public protocol Workflow: Sendable {
    /// The persisted value supplied when the workflow starts or resumes.
    associatedtype Input: Codable & Sendable
    /// The persisted value produced when every step completes successfully.
    associatedtype Output: Codable & Sendable

    /// A stable identifier for this workflow's persisted input and checkpoints.
    static var identifier: String { get }

    /// Runs the workflow body, defining checkpointed work through `context`.
    ///
    /// The body executes again when a suspended run resumes; completed step calls
    /// return persisted outputs instead of rerunning their operations.
    func run(input: Input, context: WorkflowContext) async throws -> Output
}

/// A unique identifier for one persisted workflow run.
public struct RunID: Hashable, Codable, Sendable, CustomStringConvertible {
    /// The UUID backing this run identifier.
    public let rawValue: UUID

    /// Creates an identifier from `rawValue`, or from a newly generated UUID.
    public init(_ rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }

    /// The UUID string used to represent this run in logs and persistence stores.
    public var description: String { rawValue.uuidString }
}

/// Throw this from a step to bypass its remaining retries.
public struct FatalWorkflowError: Error, Sendable {
    /// The error that made retrying inappropriate.
    public let underlying: any Error & Sendable

    /// Wraps an error that must immediately terminate the current step or workflow.
    public init(_ underlying: any Error & Sendable) {
        self.underlying = underlying
    }
}

/// The live error returned by a step that cannot continue.
public struct StepExecutionError: Error, Sendable {
    /// The checkpoint that could not complete.
    public let stepID: String
    /// Total attempts made, including the first attempt.
    public let attempts: Int
    /// The persisted classification of the failure.
    public let kind: FailureKind
    /// The original live error thrown by the step operation.
    public let underlying: any Error & Sendable

    /// Whether the step failed with ``FatalWorkflowError``.
    public var isFatal: Bool { kind == .fatal }

    /// Creates an error describing a terminal checkpoint failure.
    public init(stepID: String, attempts: Int, kind: FailureKind, underlying: any Error & Sendable) {
        self.stepID = stepID
        self.attempts = attempts
        self.kind = kind
        self.underlying = underlying
    }
}

/// Errors reported by FlowRun before or while it manages a workflow run.
public enum FlowRunError: Error, Sendable, Equatable {
    /// The supplied run identifier is already present in the persistence store.
    case runAlreadyExists(RunID)
    /// No persisted run has the supplied identifier.
    case runNotFound(RunID)
    /// The run is not suspended and therefore cannot be resumed.
    case runNotSuspended(RunID)
    /// A requested workflow identifier differs from the persisted workflow identifier.
    case workflowMismatch(expected: String, actual: String)
    /// A workflow identifier is empty or contains only whitespace.
    case invalidWorkflowIdentifier
    /// A step identifier is empty or contains only whitespace.
    case invalidStepID
    /// A retry count or backoff configuration is invalid.
    case invalidRetryPolicy
    /// A stale-run timeout is non-finite or not positive.
    case invalidTimeout
    /// A runner heartbeat interval is not positive.
    case invalidHeartbeatInterval
    /// ``Runner/shared`` has not been configured with persistence.
    case runnerNotConfigured
    /// ``Runner/shared`` was configured more than once in the same process.
    case runnerAlreadyConfigured
    /// The run's owner generation was revoked by another owner or a timeout.
    case executionRevoked(RunID)
    /// Two steps were invoked concurrently from one workflow context.
    case concurrentStep
    /// A workflow invoked the same step identifier more than once.
    case duplicateStepID(String)
    /// The next workflow step differs from its persisted checkpoint.
    case stepOrderChanged(expected: String, actual: String)
    /// The workflow ended before consuming a persisted unfinished checkpoint.
    case unfinishedCheckpoint(String)
    /// Persisted data or a requested state transition violates FlowRun invariants.
    case invalidState(String)
    /// A workflow input, step value, or output could not be JSON-encoded.
    case encodingFailed(String)
    /// Persisted workflow data could not be JSON-decoded into the requested type.
    case decodingFailed(String)
    /// The persistence implementation reported a storage failure.
    case persistenceFailed(String)
}
