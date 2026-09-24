import Foundation
import Testing
@testable import FlowRun

private struct MixedStepsWorkflow: Workflow {
    static let identifier = "tests.mixed-steps.v1"
    let log: EventLog

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        let doubled: Int = try await context.step(id: "sync") { input * 2 }
        await log.append("sync:\(doubled)")
        let final: Int = try await context.step(id: "async") {
            await log.append("async")
            return doubled + 1
        }
        return final
    }
}

private struct InvalidPolicyWorkflow: Workflow {
    static let identifier = "tests.invalid-policy.v1"
    let calls: InvocationCounter

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        try await context.step(id: "never-started", retry: RetryPolicy(retries: -1)) {
            await calls.next()
        }
    }
}

private struct UnencodableInput: Codable, Sendable {
    init() {}
    init(from decoder: Decoder) throws {}
    func encode(to encoder: Encoder) throws { throw FixtureError.cannotEncode }
}

private struct InputEncodingWorkflow: Workflow {
    static let identifier = "tests.input-encoding.v1"
    let calls: InvocationCounter

    func run(input: UnencodableInput, context: WorkflowContext) async throws -> Int {
        await calls.next()
    }
}

private struct UnencodableOutput: Codable, Sendable {
    init() {}
    init(from decoder: Decoder) throws {}
    func encode(to encoder: Encoder) throws { throw FixtureError.cannotEncode }
}

private struct OutputEncodingWorkflow: Workflow {
    static let identifier = "tests.output-encoding.v1"

    func run(input: Int, context: WorkflowContext) async throws -> UnencodableOutput {
        let _: Int = try await context.step(id: "completed") { input * 3 }
        return UnencodableOutput()
    }
}

private struct FatalBodyWorkflow: Workflow {
    static let identifier = "tests.fatal-body.v1"

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        throw FatalWorkflowError(FixtureError.fatal)
    }
}

@Test func synchronousAndAsyncStepOverloadsProduceOrderedCheckpoints() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let log = EventLog()
    let handle = try await runner.start(MixedStepsWorkflow(log: log), input: 9)

    #expect(try await handle.value() == 19)
    #expect(await log.all() == ["sync:18", "async"])
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .succeeded)
    #expect(record.steps.map(\.id) == ["sync", "async"])
    #expect(record.steps.map(\.attempts) == [1, 1])
    #expect(try JSONDecoder().decode(Int.self, from: #require(record.steps[0].output)) == 18)
    #expect(try JSONDecoder().decode(Int.self, from: #require(record.steps[1].output)) == 19)
}

@Test func invalidRetryPolicyFailsBeforeCreatingAStepOrRunningItsAction() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let calls = InvocationCounter()
    let workflow = InvalidPolicyWorkflow(calls: calls)
    let handle = try await runner.start(workflow, input: 1)

    let error = try requireFailure(await capture { try await handle.value() })
    #expect(error as? FlowRunError == .invalidRetryPolicy)
    #expect(await calls.value() == 0)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .runtime)
    #expect(record.steps.isEmpty)
    let resume = await capture { try await runner.resume(workflow, id: handle.id) }
    #expect(try requireFailure(resume) as? FlowRunError == .runNotSuspended(handle.id))
}

@Test func inputAndFinalOutputEncodingFailuresRespectCheckpointBoundaries() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let calls = InvocationCounter()
    let missingID = RunID()
    let inputFailure = await capture {
        try await runner.start(InputEncodingWorkflow(calls: calls), input: UnencodableInput(), id: missingID)
    }
    guard case FlowRunError.encodingFailed = try requireFailure(inputFailure) else {
        Issue.record("Expected input encoding failure")
        return
    }
    #expect(await calls.value() == 0)
    #expect(await store.loadRun(id: missingID) == nil)

    let outputRun = try await runner.start(OutputEncodingWorkflow(), input: 4)
    let outputFailure = try requireFailure(await capture { try await outputRun.value() })
    guard case FlowRunError.encodingFailed = outputFailure else {
        Issue.record("Expected output encoding failure")
        return
    }
    let record = try #require(await store.loadRun(id: outputRun.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .runtime)
    #expect(record.steps.count == 1)
    #expect(record.steps[0].status == .succeeded)
    #expect(try JSONDecoder().decode(Int.self, from: #require(record.steps[0].output)) == 12)
    #expect(record.output == nil)
}

@Test func fatalErrorOutsideAStepFailsImmediately() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let handle = try await runner.start(FatalBodyWorkflow(), input: 0)
    let error = try requireFailure(await capture { try await handle.value() })
    let fatal = try #require(error as? FatalWorkflowError)
    #expect(fatal.underlying as? FixtureError == .fatal)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .fatal)
    #expect(record.steps.isEmpty)
}
