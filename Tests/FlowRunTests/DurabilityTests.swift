import Foundation
import Testing
@testable import FlowRun

private struct DurableNumberWorkflow: Workflow {
    static let identifier = "tests.durable-number.v1"

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        try await context.step(id: "double") {
            input * 2
        }
    }
}

private struct OtherNumberWorkflow: Workflow {
    static let identifier = "tests.other-number.v1"

    func run(input: Int, context: WorkflowContext) async throws -> Int { input }
}

private struct LossyInput: Codable, Sendable {
    let number: Int

    init(number: Int) { self.number = number }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        number = try container.decode(Int.self) + 100
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(number)
    }
}

private struct LossyInputWorkflow: Workflow {
    static let identifier = "tests.lossy-input.v1"

    func run(input: LossyInput, context: WorkflowContext) async throws -> Int { input.number }
}

private struct LossyCheckpointWorkflow: Workflow {
    static let identifier = "tests.lossy-checkpoint.v1"

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        let saved: LossyInput = try await context.step(id: "lossy") {
            LossyInput(number: input)
        }
        return saved.number
    }
}

private struct LossyFinalWorkflow: Workflow {
    static let identifier = "tests.lossy-final.v1"

    func run(input: Int, context: WorkflowContext) async throws -> LossyInput {
        LossyInput(number: input)
    }
}

private actor FatalGate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var entered = false

    func wait() async {
        await withCheckedContinuation { continuation in
            self.continuation = continuation
            entered = true
        }
    }

    func hasEntered() -> Bool { entered }

    func release() {
        continuation?.resume()
        continuation = nil
    }
}

private struct FatalRaceWorkflow: Workflow {
    static let identifier = "tests.fatal-race.v1"
    let gate: FatalGate

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        try await context.step(id: "fatal") {
            await gate.wait()
            throw FatalWorkflowError(FixtureError.fatal)
        }
    }
}

private struct FirstSchema: Codable, Sendable {
    let value: Int
}

private struct RenamedSchema: Codable, Sendable {
    let value: Int
}

private struct CompatibleCodableWorkflow: Workflow {
    static let identifier = "tests.compatible-codable.v1"
    let renamed: Bool
    let pause: Bool
    let calls: InvocationCounter

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        let result: Int
        if renamed {
            let value: RenamedSchema = try await context.step(
                id: "value"
            ) {
                _ = await calls.next()
                return RenamedSchema(value: input)
            }
            result = value.value
        } else {
            let value: FirstSchema = try await context.step(
                id: "value"
            ) {
                _ = await calls.next()
                return FirstSchema(value: input)
            }
            result = value.value
        }
        return try await context.step(id: "tail") {
            if pause { try await Task.sleep(for: .seconds(30)) }
            return result + 1
        }
    }
}

private struct LongRunningWorkflow: Workflow {
    static let identifier = "tests.long-running.v1"

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        try await context.step(id: "wait") {
            try await Task.sleep(for: .seconds(30))
            return input
        }
    }
}

private struct RetryOnceWorkflow: Workflow {
    static let identifier = "tests.retry-once.v1"
    let calls: InvocationCounter

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        try await context.step(id: "unstable", retry: RetryPolicy(retries: 1)) {
            let attempt = await calls.next()
            if attempt == 1 { throw FixtureError.transient }
            return input + attempt
        }
    }
}

private enum PersistenceBackend: CaseIterable, Sendable {
    case memory
    case sqlite
}

private func temporaryDatabaseURL() throws -> (directory: URL, file: URL) {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent("flowrun-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    return (directory, directory.appendingPathComponent("runs.sqlite"))
}

@Test func sharedRunnerConfiguresOnceAndRunsConcurrently() async throws {
    let runner = Runner.shared
    let unconfigured = await capture { try await runner.snapshot(id: RunID()) }
    #expect(try requireFailure(unconfigured) as? FlowRunError == .runnerNotConfigured)

    let store = InMemoryWorkflowPersistence()
    let invalidInterval = await capture {
        try await runner.configure(persistence: store, heartbeatInterval: .zero)
    }
    #expect(try requireFailure(invalidInterval) as? FlowRunError == .invalidHeartbeatInterval)
    try await runner.configure(persistence: store)
    let duplicate = await capture { try await runner.configure(persistence: store) }
    #expect(try requireFailure(duplicate) as? FlowRunError == .runnerAlreadyConfigured)

    async let first = runner.start(DurableNumberWorkflow(), input: 10)
    async let second = runner.start(DurableNumberWorkflow(), input: 21)
    let handles = try await [first, second]
    #expect(try await handles[0].value() == 20)
    #expect(try await handles[1].value() == 42)
    #expect(try await runner.output(for: DurableNumberWorkflow.self, id: handles[1].id) == 42)
}

@Test func startExecutesThePersistedInputRepresentation() async throws {
    let store = InMemoryWorkflowPersistence()
    let engine = RunEngine(persistence: store)
    let handle = try await engine.start(LossyInputWorkflow(), input: LossyInput(number: 7))
    #expect(try await handle.value() == 107)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(try JSONDecoder().decode(Int.self, from: record.input) == 7)
    #expect(try await engine.output(for: LossyInputWorkflow.self, id: handle.id) == 107)
}

@Test func liveStepAndFinalValuesMatchPersistedReads() async throws {
    let engine = RunEngine(persistence: InMemoryWorkflowPersistence())
    let step = try await engine.start(LossyCheckpointWorkflow(), input: 7)
    #expect(try await step.value() == 107)
    #expect(try await engine.output(for: LossyCheckpointWorkflow.self, id: step.id) == 107)

    let final = try await engine.start(LossyFinalWorkflow(), input: 7)
    #expect(try await final.value().number == 107)
    #expect(try await engine.output(for: LossyFinalWorkflow.self, id: final.id)?.number == 107)
}

@Test func fatalStepWinsWhenCancellationRacesItsError() async throws {
    let store = InMemoryWorkflowPersistence()
    let gate = FatalGate()
    let engine = RunEngine(persistence: store)
    let handle = try await engine.start(FatalRaceWorkflow(gate: gate), input: 0)
    try await waitUntil { await gate.hasEntered() }
    handle.cancel()
    await gate.release()

    let error = try requireFailure(await capture { try await handle.value() })
    let stepError = try #require(error as? StepExecutionError)
    #expect(stepError.kind == .fatal)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .fatal)
    #expect(record.steps[0].failures == 1)
}

@Test func compatibleCodableSurvivesASwiftTypeRename() async throws {
    let store = InMemoryWorkflowPersistence()
    let calls = InvocationCounter()
    let engine = RunEngine(persistence: store)
    let original = CompatibleCodableWorkflow(renamed: false, pause: true, calls: calls)
    let handle = try await engine.start(original, input: 4)
    try await waitUntil { await store.loadRun(id: handle.id)?.steps.count == 2 }
    handle.cancel()
    let cancellation = try requireFailure(await capture { try await handle.value() })
    #expect(cancellation is CancellationError)

    let renamed = CompatibleCodableWorkflow(renamed: true, pause: false, calls: calls)
    let resumed = try await engine.resume(renamed, id: handle.id)
    #expect(try await resumed.value() == 5)
    #expect(await calls.value() == 1)
    #expect(await store.loadRun(id: handle.id)?.steps[0].id == "value")
}

@Test func sqliteReopenAndTypedOutputRead() async throws {
    let location = try temporaryDatabaseURL()
    defer { try? FileManager.default.removeItem(at: location.directory) }
    let firstStore = try SQLiteWorkflowPersistence(url: location.file)
    let firstEngine = RunEngine(persistence: firstStore)
    let handle = try await firstEngine.start(DurableNumberWorkflow(), input: 21)
    #expect(try await handle.value() == 42)

    let reopened = try SQLiteWorkflowPersistence(url: location.file)
    let secondEngine = RunEngine(persistence: reopened)
    #expect(try await secondEngine.output(for: DurableNumberWorkflow.self, id: handle.id) == 42)
    #expect(try await secondEngine.output(for: DurableNumberWorkflow.self, id: RunID()) == nil)
    let wrong = await capture { try await secondEngine.output(for: OtherNumberWorkflow.self, id: handle.id) }
    #expect(try requireFailure(wrong) as? FlowRunError == .workflowMismatch(
        expected: DurableNumberWorkflow.identifier, actual: OtherNumberWorkflow.identifier
    ))
}

@Test func sqliteConnectionsGrantOnlyOneSuspendedClaim() async throws {
    let location = try temporaryDatabaseURL()
    defer { try? FileManager.default.removeItem(at: location.directory) }
    let first = try SQLiteWorkflowPersistence(url: location.file)
    let second = try SQLiteWorkflowPersistence(url: location.file)
    let id = RunID()
    _ = try await first.createRun(RunRecord(id: id, workflowID: "tests.sqlite-claim", input: Data("1".utf8)))
    try await first.finishRun(runID: id, generation: 1, status: .suspended, output: nil, failure: nil)

    async let left = capture { try await first.claimSuspendedRun(id: id, workflowID: "tests.sqlite-claim") }
    async let right = capture { try await second.claimSuspendedRun(id: id, workflowID: "tests.sqlite-claim") }
    let results = await [left, right]
    #expect(results.compactMap { try? $0.get() }.count == 1)
    #expect(results.compactMap { try? $0.get() }.first?.ownerGeneration == 2)
    #expect(results.compactMap { result -> (any Error)? in
        if case .failure(let error) = result { return error }
        return nil
    }.first as? FlowRunError == .runNotSuspended(id))
    let staleWrite = await capture {
        try await first.startStep(runID: id, generation: 1, index: 0, stepID: "step")
    }
    #expect(try requireFailure(staleWrite) as? FlowRunError == .executionRevoked(id))
}

@Test(arguments: PersistenceBackend.allCases)
private func replayAndRetryUseTheSameTransitions(backend: PersistenceBackend) async throws {
    let location = try temporaryDatabaseURL()
    defer { try? FileManager.default.removeItem(at: location.directory) }
    let store: any WorkflowPersistence = try backend == .memory
        ? InMemoryWorkflowPersistence()
        : SQLiteWorkflowPersistence(url: location.file)
    let calls = InvocationCounter()
    let engine = RunEngine(persistence: store)
    let retry = try await engine.start(RetryOnceWorkflow(calls: calls), input: 10)
    #expect(try await retry.value() == 12)
    let retryRecord = try #require(await store.loadRun(id: retry.id))
    #expect(retryRecord.steps[0].attempts == 2)
    #expect(retryRecord.steps[0].failures == 1)

    let first = try await engine.start(
        CompatibleCodableWorkflow(renamed: false, pause: true, calls: calls), input: 4
    )
    try await waitUntil { try await store.loadRun(id: first.id)?.steps.count == 2 }
    first.cancel()
    let cancellation = try requireFailure(await capture { try await first.value() })
    #expect(cancellation is CancellationError)

    let resumedStore: any WorkflowPersistence = try backend == .memory
        ? store
        : SQLiteWorkflowPersistence(url: location.file)
    let resumedEngine = RunEngine(persistence: resumedStore)
    let resumed = try await resumedEngine.resume(
        CompatibleCodableWorkflow(renamed: true, pause: false, calls: calls), id: first.id
    )
    #expect(try await resumed.value() == 5)
    let record = try #require(await resumedStore.loadRun(id: first.id))
    #expect(record.steps.map(\.attempts) == [1, 2])
    #expect(record.status == .succeeded)
}

@Test func activeEngineHeartbeatsDuringALongStep() async throws {
    let store = InMemoryWorkflowPersistence()
    let engine = RunEngine(persistence: store, heartbeatInterval: .milliseconds(5))
    let handle = try await engine.start(LongRunningWorkflow(), input: 1)
    do {
        try await waitUntil { await store.loadRun(id: handle.id)?.steps.count == 1 }
        let initial = try #require(await store.loadRun(id: handle.id))
        try await waitUntil {
            guard let current = await store.loadRun(id: handle.id) else { return false }
            return current.heartbeatAt > initial.heartbeatAt
        }
    } catch {
        handle.cancel()
        _ = try? await handle.value()
        throw error
    }
    handle.cancel()
    let cancellation = try requireFailure(await capture { try await handle.value() })
    #expect(cancellation is CancellationError)
}

@Test func heartbeatAndTimeoutSweepAreAtomicAcrossConnections() async throws {
    let location = try temporaryDatabaseURL()
    defer { try? FileManager.default.removeItem(at: location.directory) }
    let heartbeater = try SQLiteWorkflowPersistence(url: location.file)
    let sweeper = try SQLiteWorkflowPersistence(url: location.file)
    let id = RunID()
    _ = try await heartbeater.createRun(RunRecord(
        id: id, workflowID: "tests.race", input: Data("1".utf8),
        createdAt: Date().addingTimeInterval(-120)
    ))
    let cutoff = Date().addingTimeInterval(-60)
    async let heartbeat = capture { try await heartbeater.heartbeat(runID: id, generation: 1) }
    async let sweep = sweeper.timeoutStaleRuns(before: cutoff)
    let beatResult = await heartbeat
    let swept = try await sweep
    let final = try #require(await sweeper.loadRun(id: id))
    if swept.isEmpty {
        #expect(try beatResult.get() == ())
        #expect(final.status == .running)
        #expect(final.ownerGeneration == 1)
    } else {
        #expect(swept == [id])
        #expect(final.status == .timedOut)
        #expect(final.ownerGeneration == 2)
        #expect(try requireFailure(beatResult) as? FlowRunError == .executionRevoked(id))
    }
}

@Test func heartbeatProtectsLiveRunsAndTimeoutFencesStaleOnes() async throws {
    let location = try temporaryDatabaseURL()
    defer { try? FileManager.default.removeItem(at: location.directory) }
    let store = try SQLiteWorkflowPersistence(url: location.file)
    let oldDate = Date().addingTimeInterval(-120)
    let liveID = RunID()
    let staleID = RunID()
    _ = try await store.createRun(RunRecord(
        id: liveID, workflowID: "tests.timeout", input: Data("1".utf8), createdAt: oldDate
    ))
    _ = try await store.createRun(RunRecord(
        id: staleID, workflowID: "tests.timeout", input: Data("1".utf8), createdAt: oldDate
    ))
    try await store.heartbeat(runID: liveID, generation: 1)
    let engine = RunEngine(persistence: store)
    let timedOut = try await engine.timeoutStaleRuns(olderThan: 60)
    #expect(timedOut == [staleID])
    #expect(try await store.loadRun(id: liveID)?.status == .running)
    let expired = try #require(await store.loadRun(id: staleID))
    #expect(expired.status == .timedOut)
    #expect(expired.failure?.kind == .timeout)
    #expect(expired.ownerGeneration == 2)
    let staleWrite = await capture {
        try await store.startStep(runID: staleID, generation: 1, index: 0, stepID: "step")
    }
    #expect(try requireFailure(staleWrite) as? FlowRunError == .executionRevoked(staleID))
    let resume = await capture { try await engine.resume(DurableNumberWorkflow(), id: staleID) }
    #expect(try requireFailure(resume) as? FlowRunError == .workflowMismatch(
        expected: "tests.timeout", actual: DurableNumberWorkflow.identifier
    ))
    let claim = await capture { try await store.claimSuspendedRun(id: staleID, workflowID: "tests.timeout") }
    #expect(try requireFailure(claim) as? FlowRunError == .runNotSuspended(staleID))
}
