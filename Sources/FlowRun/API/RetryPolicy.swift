import Foundation

/// Determines how long FlowRun waits after a retryable step failure.
public enum BackoffStrategy: Sendable, Equatable {
    /// Retry without waiting.
    case immediate
    /// Wait the same duration after every failed attempt.
    case fixed(Duration)
    /// Increase delay from an initial value, bounded by a maximum.
    case exponential(initial: Duration, multiplier: Int, maximum: Duration)

    /// Delay after the given numbered failure, starting at 1.
    func delay(afterFailure failure: Int) -> Duration {
        guard failure > 0 else { return .zero }
        switch self {
        case .immediate:
            return .zero
        case .fixed(let delay):
            return delay
        case .exponential(let initial, let multiplier, let maximum):
            var delay = initial
            if failure > 1 {
                for _ in 1..<failure {
                    if delay >= maximum / multiplier { return maximum }
                    delay *= multiplier
                }
            }
            return min(delay, maximum)
        }
    }

    func validate() throws {
        switch self {
        case .immediate:
            break
        case .fixed(let delay):
            guard delay >= .zero else { throw FlowRunError.invalidRetryPolicy }
        case .exponential(let initial, let multiplier, let maximum):
            guard initial > .zero, multiplier >= 2, maximum >= initial else {
                throw FlowRunError.invalidRetryPolicy
            }
        }
    }
}

/// Configures retry attempts for one checkpointed step.
public struct RetryPolicy: Sendable, Equatable {
    /// Number of additional attempts after the first execution.
    public let retries: Int
    public let backoff: BackoffStrategy

    /// Creates a policy with additional retry attempts and a waiting strategy.
    ///
    /// Validation occurs when a step starts; negative retry counts and invalid
    /// backoff values cause ``FlowRunError/invalidRetryPolicy``.
    public init(retries: Int = 0, backoff: BackoffStrategy = .immediate) {
        self.retries = retries
        self.backoff = backoff
    }

    /// A policy that makes one attempt and does not retry.
    public static let none = RetryPolicy()

    func validate() throws {
        guard retries >= 0, retries < Int.max else { throw FlowRunError.invalidRetryPolicy }
        try backoff.validate()
    }
}
