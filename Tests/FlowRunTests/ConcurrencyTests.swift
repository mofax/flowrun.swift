import Foundation
import Testing
@testable import FlowRun

private actor TwoRunBarrier {
    private var entered: Set<Int> = []
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var released = false

    func enter(_ value: Int) async -> Int {
        entered.insert(value)
        if !released {
            await withCheckedContinuation { waiters.append($0) }
        }
        return value * 10
    }

    func arrivalCount() -> Int { entered.count }

    func release() {
        released = true
        for waiter in waiters { waiter.resume() }
        waiters.removeAll()
    }
}

private struct BarrierWorkflow: Workflow {
    static let identifier = "tests.barrier.v1"
    let barrier: TwoRunBarrier

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        try await context.step(id: "blocked") { await barrier.enter(input) }
    }
}

private actor StepGate {
    private var entered = false
    private var releaseWaiter: CheckedContinuation<Void, Never>?
    private var released = false

    func enter() async -> Int {
        entered = true
        if !released {
            await withCheckedContinuation { releaseWaiter = $0 }
        }
        return 1
    }

    func hasEntered() -> Bool { entered }

    func release() {
        released = true
        releaseWaiter?.resume()
        releaseWaiter = nil
    }
}

private struct OverlapWorkflow: Workflow {
    static let identifier = "tests.overlap.v1"
    let gate: StepGate
    let secondCalls: InvocationCounter

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        async let first: Int = context.step(id: "first") { await gate.enter() }
        do {
            try await waitUntil { await gate.hasEntered() }
        } catch {
            await gate.release()
            _ = try? await first
            throw error
        }
        do {
            let _: Int = try await context.step(id: "second") {
                await secondCalls.next()
            }
            await gate.release()
            return try await first
        } catch {
            await gate.release()
            _ = try await first
            throw error
        }
    }
}

private struct DuplicateStepWorkflow: Workflow {
    static let identifier = "tests.duplicate-step.v1"
    let calls: InvocationCounter

    func run(input: Int, context: WorkflowContext) async throws -> Int {
        let first: Int = try await context.step(id: "same") { await calls.next() }
        let second: Int = try await context.step(id: "same") { await calls.next() }
        return first + second
    }
}

@Test func independentRunsCanBothEnterBlockedStepsBeforeEitherCompletes() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let barrier = TwoRunBarrier()
    let workflow = BarrierWorkflow(barrier: barrier)
    let first = try await runner.start(workflow, input: 1)
    let second = try await runner.start(workflow, input: 2)

    do {
        try await waitUntil { await barrier.arrivalCount() == 2 }
    } catch {
        await barrier.release()
        first.cancel()
        second.cancel()
        throw error
    }
    let firstRecord = try #require(await store.loadRun(id: first.id))
    let secondRecord = try #require(await store.loadRun(id: second.id))
    #expect(firstRecord.status == .running)
    #expect(secondRecord.status == .running)
    #expect(firstRecord.steps.map(\.status) == [.running])
    #expect(secondRecord.steps.map(\.status) == [.running])
    #expect(firstRecord.steps[0].output == nil)
    #expect(secondRecord.steps[0].output == nil)

    await barrier.release()
    #expect(try await first.value() == 10)
    #expect(try await second.value() == 20)
    #expect(try await runner.snapshot(id: first.id)?.status == .succeeded)
    #expect(try await runner.snapshot(id: second.id)?.status == .succeeded)
}

@Test func overlappingStepCallIsRejectedBeforeItsOperationOrCheckpoint() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let secondCalls = InvocationCounter()
    let handle = try await runner.start(
        OverlapWorkflow(gate: StepGate(), secondCalls: secondCalls),
        input: 0
    )

    let error = try requireFailure(await capture { try await handle.value() })
    #expect(error as? FlowRunError == .concurrentStep)
    #expect(await secondCalls.value() == 0)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.failure?.kind == .runtime)
    #expect(record.steps.map(\.id) == ["first"])
    #expect(record.steps[0].status == .succeeded)
}

@Test func duplicateStepIDDoesNotExecuteTheSecondAction() async throws {
    let store = InMemoryWorkflowPersistence()
    let runner = RunEngine(persistence: store)
    let calls = InvocationCounter()
    let handle = try await runner.start(DuplicateStepWorkflow(calls: calls), input: 0)

    let error = try requireFailure(await capture { try await handle.value() })
    #expect(error as? FlowRunError == .duplicateStepID("same"))
    #expect(await calls.value() == 1)
    let record = try #require(await store.loadRun(id: handle.id))
    #expect(record.status == .failed)
    #expect(record.steps.count == 1)
    #expect(record.steps[0].attempts == 1)
    #expect(record.steps[0].status == .succeeded)
}
