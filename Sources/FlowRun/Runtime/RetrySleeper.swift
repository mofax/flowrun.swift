import Foundation

/// Kept internal so retry timing can be exercised without wall-clock sleeps.
protocol RetrySleeper: Sendable {
    func sleep(for duration: Duration) async throws
}

struct TaskRetrySleeper: RetrySleeper {
    func sleep(for duration: Duration) async throws {
        try await Task.sleep(for: duration)
    }
}
