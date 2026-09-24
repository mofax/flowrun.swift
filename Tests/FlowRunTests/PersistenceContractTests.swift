import Foundation
import Testing
@testable import FlowRun

private func newRun(_ id: RunID, workflowID: String = "tests.store") throws -> RunRecord {
    RunRecord(id: id, workflowID: workflowID, input: try JSONEncoder().encode(["input": 17]))
}

@Test func createAndClaimAreAtomicAcrossContendingCallers() async throws {
    let store = InMemoryWorkflowPersistence()
    let id = RunID()
    _ = try await store.createRun(newRun(id))
    let duplicate = await capture { try await store.createRun(newRun(id)) }
    #expect(try requireFailure(duplicate) as? FlowRunError == .runAlreadyExists(id))
    try await store.finishRun(runID: id, generation: 1, status: .suspended, output: nil, failure: nil)

    let wrongWorkflow = await capture {
        try await store.claimSuspendedRun(id: id, workflowID: "different")
    }
    #expect(try requireFailure(wrongWorkflow) as? FlowRunError ==
        .workflowMismatch(expected: "tests.store", actual: "different"))
    #expect(await store.loadRun(id: id)?.status == .suspended)

    async let first = capture { try await store.claimSuspendedRun(id: id, workflowID: "tests.store") }
    async let second = capture { try await store.claimSuspendedRun(id: id, workflowID: "tests.store") }
    let outcomes = await [first, second]
    let winners = outcomes.compactMap { try? $0.get() }
    let losers = outcomes.compactMap { result -> (any Error)? in
        if case .failure(let error) = result { return error }
        return nil
    }
    #expect(winners.count == 1)
    #expect(losers.count == 1)
    #expect(losers.first as? FlowRunError == .runNotSuspended(id))
    #expect(await store.loadRun(id: id)?.status == .running)
}

@Test func storeProtectsCheckpointOrderAndTerminalState() async throws {
    let store = InMemoryWorkflowPersistence()
    let id = RunID()
    _ = try await store.createRun(newRun(id))
    let bytes = try JSONEncoder().encode(123)

    let beforeStart = await capture { try await store.completeStep(runID: id, generation: 1, index: 0, output: bytes) }
    #expect(try requireFailure(beforeStart) is FlowRunError)
    _ = try await store.startStep(runID: id, generation: 1, index: 0, stepID: "first")

    let runningAgain = await capture {
        try await store.startStep(runID: id, generation: 1, index: 0, stepID: "first")
    }
    #expect(try requireFailure(runningAgain) is FlowRunError)

    let skip = await capture {
        try await store.startStep(runID: id, generation: 1, index: 1, stepID: "second")
    }
    #expect(try requireFailure(skip) is FlowRunError)
    let renamed = await capture {
        try await store.startStep(runID: id, generation: 1, index: 0, stepID: "different")
    }
    #expect(try requireFailure(renamed) as? FlowRunError ==
        .stepOrderChanged(expected: "first", actual: "different"))
    _ = try await store.completeStep(runID: id, generation: 1, index: 0, output: bytes)
    let overwrite = await capture {
        try await store.startStep(runID: id, generation: 1, index: 0, stepID: "first")
    }
    #expect(try requireFailure(overwrite) is FlowRunError)
    _ = try await store.startStep(runID: id, generation: 1, index: 1, stepID: "second")
    let prematureSuccess = await capture {
        try await store.finishRun(runID: id, generation: 1, status: .succeeded, output: bytes, failure: nil)
    }
    #expect(try requireFailure(prematureSuccess) is FlowRunError)
    #expect(await store.loadRun(id: id)?.status == .running)

    _ = try await store.completeStep(runID: id, generation: 1, index: 1, output: bytes)
    try await store.finishRun(runID: id, generation: 1, status: .succeeded, output: bytes, failure: nil)
    let finished = try #require(await store.loadRun(id: id))
    #expect(finished.status == .succeeded)
    #expect(finished.steps.map(\.id) == ["first", "second"])
    #expect(finished.steps.map(\.status) == [.succeeded, .succeeded])
    #expect(finished.steps.map(\.attempts) == [1, 1])
    #expect(finished.steps.map(\.output) == [bytes, bytes])

    let restart = await capture {
        try await store.claimSuspendedRun(id: id, workflowID: "tests.store")
    }
    #expect(try requireFailure(restart) as? FlowRunError == .runNotSuspended(id))
    #expect(await store.loadRun(id: id)?.status == .succeeded)
}
