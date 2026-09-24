import Foundation
import Testing
@testable import FlowRun

enum FixtureError: Error, Sendable, Equatable {
    case transient
    case fatal
    case body
    case cannotEncode
}

actor EventLog {
    private var entries: [String] = []

    func append(_ entry: String) { entries.append(entry) }
    func all() -> [String] { entries }
}

actor InvocationCounter {
    private var count = 0

    func next() -> Int {
        count += 1
        return count
    }

    func value() -> Int { count }
}

actor RecordingSleeper: RetrySleeper {
    private var requested: [Duration] = []

    func sleep(for duration: Duration) async throws {
        requested.append(duration)
        try Task.checkCancellation()
    }

    func delays() -> [Duration] { requested }
}

actor BlockingSleeper: RetrySleeper {
    private var requested: [Duration] = []

    func sleep(for duration: Duration) async throws {
        requested.append(duration)
        try await Task.sleep(for: .seconds(30))
    }

    func delays() -> [Duration] { requested }
}

func waitUntil(
    timeout: Duration = .seconds(3),
    condition: @Sendable () async throws -> Bool
) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: timeout)
    while clock.now < deadline {
        if try await condition() { return }
        try await Task.sleep(for: .milliseconds(2))
    }
    throw FlowRunError.invalidState("Timed out waiting for test condition")
}

func capture<T: Sendable>(
    _ operation: @Sendable () async throws -> T
) async -> Result<T, Error> {
    do { return .success(try await operation()) }
    catch { return .failure(error) }
}

func requireFailure<T>(_ result: Result<T, Error>) throws -> any Error {
    switch result {
    case .success:
        Issue.record("Expected failure, got success")
        throw FlowRunError.invalidState("Expected failure")
    case .failure(let error):
        return error
    }
}
