import Foundation

/// A workflow definition. Pass the same definition when resuming a suspended run.
public protocol Workflow: Sendable {
    associatedtype Input: Codable & Sendable
    associatedtype Output: Codable & Sendable

    /// A stable identifier for this workflow's persisted input and checkpoints.
    static var identifier: String { get }

    func run(input: Input, context: WorkflowContext) async throws -> Output
}

public struct RunID: Hashable, Codable, Sendable, CustomStringConvertible {
    public let rawValue: UUID

    public init(_ rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }

    public var description: String { rawValue.uuidString }
}

/// Throw this from a step to bypass its remaining retries.
public struct FatalWorkflowError: Error, Sendable {
    public let underlying: any Error & Sendable

    public init(_ underlying: any Error & Sendable) {
        self.underlying = underlying
    }
}

/// The live error returned by a step that cannot continue.
public struct StepExecutionError: Error, Sendable {
    public let stepID: String
    public let attempts: Int
    public let kind: FailureKind
    public let underlying: any Error & Sendable

    public var isFatal: Bool { kind == .fatal }

    public init(stepID: String, attempts: Int, kind: FailureKind, underlying: any Error & Sendable) {
        self.stepID = stepID
        self.attempts = attempts
        self.kind = kind
        self.underlying = underlying
    }
}

public enum FlowRunError: Error, Sendable, Equatable {
    case runAlreadyExists(RunID)
    case runNotFound(RunID)
    case runNotSuspended(RunID)
    case workflowMismatch(expected: String, actual: String)
    case invalidWorkflowIdentifier
    case invalidStepID
    case invalidRetryPolicy
    case invalidTimeout
    case invalidHeartbeatInterval
    case runnerNotConfigured
    case runnerAlreadyConfigured
    case executionRevoked(RunID)
    case concurrentStep
    case duplicateStepID(String)
    case stepOrderChanged(expected: String, actual: String)
    case unfinishedCheckpoint(String)
    case invalidState(String)
    case encodingFailed(String)
    case decodingFailed(String)
    case persistenceFailed(String)
}
