import Foundation
import Testing
@testable import FlowRun

private struct OrderInput: Codable, Sendable, Equatable {
    let orderID: String
    let amount: Int
}

private struct Receipt: Codable, Sendable, Equatable {
    let reference: String
    let amount: Int
}

private struct OrderOutput: Codable, Sendable, Equatable {
    let receipt: Receipt
    let shipment: String
}

private actor ShippingAction {
    let pauseEveryTime: Bool
    private var starts = 0

    init(pauseEveryTime: Bool = false) {
        self.pauseEveryTime = pauseEveryTime
    }

    func execute(log: EventLog) async throws -> String {
        starts += 1
        let attempt = starts
        await log.append("ship:\(attempt)")
        if pauseEveryTime || attempt == 1 {
            try await Task.sleep(for: .seconds(30))
        }
        return "shipment-\(attempt)"
    }

    func startCount() -> Int { starts }
}

private enum CheckpointVariant: Sendable {
    case original
    case renamedStep
    case changedOutputType
    case omitsPendingStep
}

private struct OrderWorkflow: Workflow {
    static let identifier = "tests.order.v1"
    let log: EventLog
    let shipping: ShippingAction
    let variant: CheckpointVariant

    func run(input: OrderInput, context: WorkflowContext) async throws -> OrderOutput {
        await log.append("body:\(input.orderID):\(input.amount)")
        let receipt: Receipt
        switch variant {
        case .original, .omitsPendingStep:
            receipt = try await context.step(id: "charge") {
                await log.append("charge")
                return Receipt(reference: "receipt-\(input.orderID)", amount: input.amount)
            }
        case .renamedStep:
            receipt = try await context.step(id: "renamed-charge") {
                await log.append("renamed-charge")
                return Receipt(reference: "wrong", amount: input.amount)
            }
        case .changedOutputType:
            let _: String = try await context.step(id: "charge") {
                await log.append("changed-type")
                return "wrong"
            }
            receipt = Receipt(reference: "wrong", amount: input.amount)
        }
        if case .omitsPendingStep = variant {
            return OrderOutput(receipt: receipt, shipment: "skipped")
        }
        let shipment: String = try await context.step(id: "ship") {
            try await shipping.execute(log: log)
        }
        return OrderOutput(receipt: receipt, shipment: shipment)
    }
}

private struct WrongInputWorkflow: Workflow {
    static let identifier = OrderWorkflow.identifier
    let calls: InvocationCounter

    func run(input: String, context: WorkflowContext) async throws -> OrderOutput {
        _ = await calls.next()
        return OrderOutput(receipt: Receipt(reference: input, amount: 0), shipment: "none")
    }
}

private struct FailingStepWorkflow: Workflow {
    static let identifier = "tests.failing-resume.v1"
    let calls: InvocationCounter

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        try await context.step(id: "fails", retry: RetryPolicy(retries: 1, backoff: .fixed(.seconds(5)))) {
            _ = await calls.next()
            throw FixtureError.transient
        }
    }
}

private func suspendOrderRun(
    runner: RunEngine,
    workflow: OrderWorkflow,
    input: OrderInput
) async throws -> RunID {
    let handle = try await runner.start(workflow, input: input)
    do {
        try await waitUntil { await workflow.shipping.startCount() == 1 }
    } catch {
        handle.cancel()
        _ = try? await handle.value()
        throw error
    }
    handle.cancel()
    let error = try requireFailure(await capture { try await handle.value() })
    #expect(error is CancellationError)
    return handle.id
}

@Test func resumeUsesPersistedInputAndReplaysOnlyCompletedOutputs() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let log = EventLog()
    let shipping = ShippingAction()
    let workflow = OrderWorkflow(log: log, shipping: shipping, variant: .original)
    let input = OrderInput(orderID: "A-17", amount: 730)
    let id = try await suspendOrderRun(runner: runner, workflow: workflow, input: input)

    let suspended = try #require(await store.loadRun(id: id))
    #expect(suspended.status == .suspended)
    #expect(try JSONDecoder().decode(OrderInput.self, from: suspended.input) == input)
    #expect(suspended.steps.map(\.status) == [.succeeded, .suspended])
    #expect(suspended.steps.map(\.attempts) == [1, 1])
    #expect(try JSONDecoder().decode(Receipt.self, from: #require(suspended.steps[0].output)) ==
        Receipt(reference: "receipt-A-17", amount: 730))
    #expect(suspended.steps[1].output == nil)

    let resumed = try await RunEngine(persistence: store).resume(workflow, id: id)
    let result = try await resumed.value()
    #expect(result == OrderOutput(
        receipt: Receipt(reference: "receipt-A-17", amount: 730),
        shipment: "shipment-2"
    ))
    #expect(await log.all() == [
        "body:A-17:730", "charge", "ship:1",
        "body:A-17:730", "ship:2"
    ])
    let finished = try #require(await store.loadRun(id: id))
    #expect(finished.status == .succeeded)
    #expect(finished.steps.map(\.attempts) == [1, 2])
    #expect(finished.steps.map(\.status) == [.succeeded, .succeeded])
    #expect(try JSONDecoder().decode(OrderOutput.self, from: #require(finished.output)) == result)
}

@Test func changedStepIdentityOrIncompatibleCodableOutputFailsBeforeAnyChangedOperationRuns() async throws {
    for variant in [CheckpointVariant.renamedStep, .changedOutputType] {
        let store = InMemoryWorkflowPersistence()
        let runner = RunEngine(persistence: store)
        let log = EventLog()
        let shipping = ShippingAction()
        let original = OrderWorkflow(log: log, shipping: shipping, variant: .original)
        let id = try await suspendOrderRun(
            runner: runner,
            workflow: original,
            input: OrderInput(orderID: "B-8", amount: 50)
        )
        let originalOutput = try #require(await store.loadRun(id: id)?.steps[0].output)
        let changed = OrderWorkflow(log: log, shipping: shipping, variant: variant)
        let resumed = try await runner.resume(changed, id: id)
        let error = try requireFailure(await capture { try await resumed.value() })
        switch variant {
        case .renamedStep:
            #expect(error as? FlowRunError == .stepOrderChanged(expected: "charge", actual: "renamed-charge"))
        case .changedOutputType:
            if let flowError = error as? FlowRunError, case .decodingFailed = flowError {
                break
            }
            Issue.record("Expected checkpoint decoding failure, got \(error)")
        case .original:
            Issue.record("Unexpected variant")
        case .omitsPendingStep:
            Issue.record("Unexpected variant")
        }
        #expect(await log.all() == ["body:B-8:50", "charge", "ship:1", "body:B-8:50"])
        let failed = try #require(await store.loadRun(id: id))
        #expect(failed.status == .failed)
        #expect(failed.failure?.kind == .runtime)
        #expect(failed.steps[0].output == originalOutput)
        #expect(failed.steps.map(\.attempts) == [1, 1])
    }
}

@Test func resumedBodyCannotSilentlySkipAnUnfinishedCheckpoint() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let log = EventLog()
    let shipping = ShippingAction()
    let original = OrderWorkflow(log: log, shipping: shipping, variant: .original)
    let id = try await suspendOrderRun(
        runner: runner,
        workflow: original,
        input: OrderInput(orderID: "E-4", amount: 90)
    )
    let changed = OrderWorkflow(log: log, shipping: shipping, variant: .omitsPendingStep)
    let resumed = try await runner.resume(changed, id: id)

    let error = try requireFailure(await capture { try await resumed.value() })
    #expect(error as? FlowRunError == .unfinishedCheckpoint("ship"))
    #expect(await shipping.startCount() == 1)
    #expect(await log.all() == ["body:E-4:90", "charge", "ship:1", "body:E-4:90"])
    let record = try #require(await store.loadRun(id: id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .runtime)
    #expect(record.steps.map(\.attempts) == [1, 1])
}

@Test func resumeWithAnIncompatibleInputSchemaFailsWithoutCallingTheWorkflow() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let workflow = OrderWorkflow(log: EventLog(), shipping: ShippingAction(), variant: .original)
    let id = try await suspendOrderRun(
        runner: runner,
        workflow: workflow,
        input: OrderInput(orderID: "C-1", amount: 10)
    )
    let calls = InvocationCounter()
    let error = try requireFailure(await capture {
        try await runner.resume(WrongInputWorkflow(calls: calls), id: id)
    })
    guard case FlowRunError.decodingFailed = error else {
        Issue.record("Expected decode failure, got \(error)")
        return
    }
    #expect(await calls.value() == 0)
    let record = try #require(await store.loadRun(id: id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .runtime)
    #expect(record.steps.map(\.attempts) == [1, 1])
}

@Test func failedAttemptBudgetPersistsAcrossCancellationDuringBackoff() async throws {
    let store = InMemoryWorkflowPersistence()
    let sleeper = BlockingSleeper()
    let runner = RunEngine(persistence: store, retrySleeper: sleeper)
    let calls = InvocationCounter()
    let workflow = FailingStepWorkflow(calls: calls)
    let first = try await runner.start(workflow, input: 0)
    do {
        try await waitUntil { await sleeper.delays().count == 1 }
    } catch {
        first.cancel()
        _ = try? await first.value()
        throw error
    }
    first.cancel()
    let cancellation = try requireFailure(await capture { try await first.value() })
    #expect(cancellation is CancellationError)
    let suspended = try #require(await store.loadRun(id: first.id))
    #expect(suspended.status == .suspended)
    #expect(suspended.steps[0].failures == 1)
    #expect(suspended.steps[0].attempts == 1)

    let resumed = try await RunEngine(persistence: store, retrySleeper: sleeper).resume(workflow, id: first.id)
    let error = try requireFailure(await capture { try await resumed.value() })
    let stepError = try #require(error as? StepExecutionError)
    #expect(stepError.attempts == 2)
    #expect(await calls.value() == 2)
    #expect(await sleeper.delays() == [.seconds(5)])
    let failed = try #require(await store.loadRun(id: first.id))
    #expect(failed.status == .failed)
    #expect(failed.steps[0].failures == 2)
    #expect(failed.steps[0].attempts == 2)
}

@Test func concurrentResumeCallsGrantExactlyOneHandle() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let shipping = ShippingAction(pauseEveryTime: true)
    let workflow = OrderWorkflow(log: EventLog(), shipping: shipping, variant: .original)
    let id = try await suspendOrderRun(
        runner: runner,
        workflow: workflow,
        input: OrderInput(orderID: "D-2", amount: 25)
    )

    let contender = RunEngine(persistence: store)
    async let first = capture { try await runner.resume(workflow, id: id) }
    async let second = capture { try await contender.resume(workflow, id: id) }
    let outcomes = await [first, second]
    let winners = outcomes.compactMap { try? $0.get() }
    let losers = outcomes.compactMap { result -> (any Error)? in
        if case .failure(let error) = result { return error }
        return nil
    }
    #expect(winners.count == 1)
    #expect(losers.count == 1)
    #expect(losers.first as? FlowRunError == .runNotSuspended(id))
    #expect(try await runner.snapshot(id: id)?.status == .running)

    let winner = try #require(winners.first)
    winner.cancel()
    let cancellation = try requireFailure(await capture { try await winner.value() })
    #expect(cancellation is CancellationError)
    #expect(try await runner.snapshot(id: id)?.status == .suspended)
}
