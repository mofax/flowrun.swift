import Foundation

public enum BackoffStrategy: Sendable, Equatable {
    case immediate
    case fixed(Duration)
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

public struct RetryPolicy: Sendable, Equatable {
    /// Number of additional attempts after the first execution.
    public let retries: Int
    public let backoff: BackoffStrategy

    public init(retries: Int = 0, backoff: BackoffStrategy = .immediate) {
        self.retries = retries
        self.backoff = backoff
    }

    public static let none = RetryPolicy()

    func validate() throws {
        guard retries >= 0, retries < Int.max else { throw FlowRunError.invalidRetryPolicy }
        try backoff.validate()
    }
}
