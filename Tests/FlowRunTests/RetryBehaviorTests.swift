import Foundation
import Testing
@testable import FlowRun

private actor AttemptScript {
    let failThrough: Int
    let fatal: Bool
    private var calls = 0
    private var instants: [ContinuousClock.Instant] = []

    init(failThrough: Int, fatal: Bool = false) {
        self.failThrough = failThrough
        self.fatal = fatal
    }

    func execute() throws -> Int {
        calls += 1
        instants.append(ContinuousClock().now)
        if calls <= failThrough {
            if fatal { throw FatalWorkflowError(FixtureError.fatal) }
            throw FixtureError.transient
        }
        return calls
    }

    func callCount() -> Int { calls }
    func callInstants() -> [ContinuousClock.Instant] { instants }
}

@Test func defaultRunnerActuallyWaitsBetweenFixedBackoffAttempts() async throws {
    let runner = RunEngine(persistence: InMemoryWorkflowPersistence())
    let script = AttemptScript(failThrough: 1)
    let handle = try await runner.start(
        ScriptedWorkflow(
            script: script,
            policy: RetryPolicy(retries: 1, backoff: .fixed(.milliseconds(25))),
            log: EventLog()
        ),
        input: 0
    )

    #expect(try await handle.value() == 2)
    let instants = await script.callInstants()
    #expect(instants.count == 2)
    #expect(instants[1] - instants[0] >= .milliseconds(20))
}

private struct ScriptedWorkflow: Workflow {
    static let identifier = "tests.scripted.v1"
    let script: AttemptScript
    let policy: RetryPolicy
    let log: EventLog

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        await log.append("body:\(input)")
        let attempt: Int = try await context.step(id: "operation", retry: policy) {
            try await script.execute()
        }
        return input + attempt
    }
}

private struct BodyFailureWorkflow: Workflow {
    static let identifier = "tests.body-failure.v1"
    let log: EventLog

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        await log.append("body")
        let _: Int = try await context.step(id: "completed") {
            await log.append("step")
            return input + 1
        }
        throw FixtureError.body
    }
}

private struct CatchingWorkflow: Workflow {
    static let identifier = "tests.catching.v1"
    let script: AttemptScript

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        do {
            let _: Int = try await context.step(id: "exhausted", retry: RetryPolicy(retries: 1)) {
                try await script.execute()
            }
        } catch {
            return 999
        }
        return 0
    }
}

private struct UnencodableValue: Codable, Sendable {
    init() {}
    init(from decoder: Decoder) throws {}
    func encode(to encoder: Encoder) throws { throw FixtureError.cannotEncode }
}

private struct UnencodableStepWorkflow: Workflow {
    static let identifier = "tests.unencodable-step.v1"
    let calls: InvocationCounter

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        let _: UnencodableValue = try await context.step(
            id: "cannot-encode",
            retry: RetryPolicy(retries: 4)
        ) {
            _ = await calls.next()
            return UnencodableValue()
        }
        return input
    }
}

@Test func retryStrategiesAreAppliedByTheRunner() async throws {
    struct Scenario {
        let strategy: BackoffStrategy
        let expectedDelays: [Duration]
    }
    let scenarios = [
        Scenario(strategy: .immediate, expectedDelays: []),
        Scenario(strategy: .fixed(.milliseconds(7)), expectedDelays: [
            .milliseconds(7), .milliseconds(7), .milliseconds(7)
        ]),
        Scenario(
            strategy: .exponential(initial: .milliseconds(10), multiplier: 2, maximum: .milliseconds(25)),
            expectedDelays: [.milliseconds(10), .milliseconds(20), .milliseconds(25)]
        )
    ]

    for scenario in scenarios {
        let store = InMemoryWorkflowPersistence()
        let sleeper = RecordingSleeper()
        let runner = RunEngine(persistence: store, retrySleeper: sleeper)
        let script = AttemptScript(failThrough: 3)
        let log = EventLog()
        let handle = try await runner.start(
            ScriptedWorkflow(script: script, policy: RetryPolicy(retries: 3, backoff: scenario.strategy), log: log),
            input: 40
        )

        #expect(try await handle.value() == 44)
        #expect(await script.callCount() == 4)
        #expect(await log.all() == ["body:40"])
        #expect(await sleeper.delays() == scenario.expectedDelays)
        let record = try #require(await store.loadRun(id: handle.id))
        #expect(record.status == .succeeded)
        #expect(record.steps.count == 1)
        #expect(record.steps[0].attempts == 4)
        #expect(record.steps[0].failures == 3)
        #expect(record.steps[0].status == .succeeded)
        #expect(try JSONDecoder().decode(Int.self, from: #require(record.steps[0].output)) == 4)
        #expect(try JSONDecoder().decode(Int.self, from: #require(record.output)) == 44)
    }
}

@Test func exhaustedStepFailsTheRunAfterExactlyTheConfiguredAttempts() async throws {
    let store = InMemoryWorkflowPersistence()
    let sleeper = RecordingSleeper()
    let runner = RunEngine(persistence: store, retrySleeper: sleeper)
    let script = AttemptScript(failThrough: 100)
    let log = EventLog()
    let handle = try await runner.start(
        ScriptedWorkflow(
            script: script,
            policy: RetryPolicy(retries: 2, backoff: .fixed(.milliseconds(5))),
            log: log
        ),
        input: 0
    )

    let error = try requireFailure(await capture { try await handle.value() })
    let stepError = try #require(error as? StepExecutionError)
    #expect(stepError.stepID == "operation")
    #expect(stepError.attempts == 3)
    #expect(stepError.kind == .step)
    #expect(stepError.underlying as? FixtureError == .transient)
    #expect(await script.callCount() == 3)
    #expect(await log.all() == ["body:0"])
    #expect(await sleeper.delays() == [.milliseconds(5), .milliseconds(5)])
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.output == nil)
    #expect(record.failure?.kind == .step)
    #expect(record.failure?.stepID == "operation")
    #expect(record.steps[0].status == .failed)
    #expect(record.steps[0].attempts == 3)
    #expect(record.steps[0].failures == 3)
}

@Test func fatalStepSkipsAllRemainingAttemptsAndBackoff() async throws {
    let store = InMemoryWorkflowPersistence()
    let sleeper = RecordingSleeper()
    let runner = RunEngine(persistence: store, retrySleeper: sleeper)
    let script = AttemptScript(failThrough: 100, fatal: true)
    let handle = try await runner.start(
        ScriptedWorkflow(script: script, policy: RetryPolicy(retries: 20, backoff: .fixed(.seconds(1))), log: EventLog()),
        input: 0
    )

    let error = try requireFailure(await capture { try await handle.value() })
    let stepError = try #require(error as? StepExecutionError)
    #expect(stepError.kind == .fatal)
    #expect(stepError.attempts == 1)
    #expect(stepError.underlying is FatalWorkflowError)
    #expect(await script.callCount() == 1)
    #expect(await sleeper.delays().isEmpty)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .fatal)
    #expect(record.steps[0].lastFailure?.kind == .fatal)
}

@Test func failureAfterACompletedStepDoesNotRestartTheWorkflow() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let log = EventLog()
    let handle = try await runner.start(BodyFailureWorkflow(log: log), input: 8)

    let error = try requireFailure(await capture { try await handle.value() })
    #expect(error as? FixtureError == .body)
    #expect(await log.all() == ["body", "step"])
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .workflow)
    #expect(record.steps[0].status == .succeeded)
    #expect(try JSONDecoder().decode(Int.self, from: #require(record.steps[0].output)) == 9)
}

@Test func catchingAnExhaustedStepCannotTurnTheRunIntoSuccess() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let script = AttemptScript(failThrough: 100)
    let handle = try await runner.start(CatchingWorkflow(script: script), input: 0)

    let error = try requireFailure(await capture { try await handle.value() })
    let stepError = try #require(error as? StepExecutionError)
    #expect(stepError.attempts == 2)
    #expect(await script.callCount() == 2)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.output == nil)
}

@Test func encodingFailureDoesNotRetryTheActionOrLeaveARunningStep() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let calls = InvocationCounter()
    let handle = try await runner.start(UnencodableStepWorkflow(calls: calls), input: 7)

    let error = try requireFailure(await capture { try await handle.value() })
    guard case FlowRunError.encodingFailed = error else {
        Issue.record("Expected encoding failure, got \(error)")
        return
    }
    #expect(await calls.value() == 1)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .runtime)
    #expect(record.steps[0].attempts == 1)
    #expect(record.steps[0].failures == 0)
    #expect(record.steps[0].status == .failed)
    #expect(record.steps[0].output == nil)
}
