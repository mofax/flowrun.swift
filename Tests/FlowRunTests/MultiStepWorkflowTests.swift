import Foundation
import Testing
@testable import FlowRun

private struct PurchaseInput: Codable, Sendable {
    let orderID: String
    let quantity: Int
    let unitPrice: Int
}

private struct Reservation: Codable, Sendable, Equatable {
    let reference: String
    let quantity: Int
}

private struct Payment: Codable, Sendable, Equatable {
    let reference: String
    let amount: Int
}

private struct Shipment: Codable, Sendable, Equatable {
    let trackingNumber: String
}

private struct PurchaseResult: Codable, Sendable, Equatable {
    let reservation: Reservation
    let payment: Payment
    let shipment: Shipment
    let notified: Bool
}

private enum ShippingBehavior: Sendable {
    case pauseFirstAttempt
    case alwaysFail
}

private struct ActionTrace: Sendable {
    let events: [String]
    let reserveCalls: Int
    let paymentCalls: Int
    let shippingCalls: Int
    let notificationCalls: Int
}

private actor PurchaseActions {
    let shippingBehavior: ShippingBehavior
    private var events: [String] = []
    private var reserveCalls = 0
    private var paymentCalls = 0
    private var shippingCalls = 0
    private var notificationCalls = 0

    init(shippingBehavior: ShippingBehavior) {
        self.shippingBehavior = shippingBehavior
    }

    func bodyStarted(orderID: String) {
        events.append("body:\(orderID)")
    }

    func reserve(orderID: String, quantity: Int) -> Reservation {
        reserveCalls += 1
        events.append("reserve:\(reserveCalls)")
        return Reservation(reference: "stock:\(orderID):\(quantity)", quantity: quantity)
    }

    func charge(reservation: Reservation, unitPrice: Int) throws -> Payment {
        paymentCalls += 1
        events.append("charge:\(paymentCalls)")
        if paymentCalls == 1 { throw FixtureError.transient }
        let amount = reservation.quantity * unitPrice
        return Payment(reference: "payment:\(reservation.reference):\(amount)", amount: amount)
    }

    func ship(payment: Payment) async throws -> Shipment {
        shippingCalls += 1
        let attempt = shippingCalls
        events.append("ship:\(attempt)")
        switch shippingBehavior {
        case .pauseFirstAttempt:
            if attempt == 1 { try await Task.sleep(for: .seconds(30)) }
        case .alwaysFail:
            throw FixtureError.transient
        }
        return Shipment(trackingNumber: "tracking:\(payment.reference)")
    }

    func notify(shipment: Shipment) -> Bool {
        notificationCalls += 1
        events.append("notify:\(shipment.trackingNumber)")
        return true
    }

    func trace() -> ActionTrace {
        ActionTrace(
            events: events,
            reserveCalls: reserveCalls,
            paymentCalls: paymentCalls,
            shippingCalls: shippingCalls,
            notificationCalls: notificationCalls
        )
    }
}

private struct PurchaseWorkflow: Workflow {
    static let identifier = "tests.purchase.v1"
    let actions: PurchaseActions

    func run(input: PurchaseInput, context: WorkflowContext) async throws -> PurchaseResult {
        await actions.bodyStarted(orderID: input.orderID)

        let reservation: Reservation = try await context.step(id: "reserve") {
            await actions.reserve(orderID: input.orderID, quantity: input.quantity)
        }
        let payment: Payment = try await context.step(
            id: "charge",
            retry: RetryPolicy(retries: 2, backoff: .fixed(.milliseconds(7)))
        ) {
            try await actions.charge(reservation: reservation, unitPrice: input.unitPrice)
        }
        let shipment: Shipment = try await context.step(
            id: "ship",
            retry: RetryPolicy(
                retries: 1,
                backoff: .exponential(
                    initial: .milliseconds(5),
                    multiplier: 2,
                    maximum: .milliseconds(10)
                )
            )
        ) {
            try await actions.ship(payment: payment)
        }
        let notified: Bool = try await context.step(id: "notify") {
            await actions.notify(shipment: shipment)
        }
        return PurchaseResult(
            reservation: reservation,
            payment: payment,
            shipment: shipment,
            notified: notified
        )
    }
}

@Test func fourStepWorkflowReplaysUpstreamOutputsAndContinuesAfterInterruptedThirdStep() async throws {
    let store = InMemoryWorkflowPersistence()
    let sleeper = RecordingSleeper()
    let actions = PurchaseActions(shippingBehavior: .pauseFirstAttempt)
    let workflow = PurchaseWorkflow(actions: actions)
    let input = PurchaseInput(orderID: "O-42", quantity: 2, unitPrice: 75)
    let firstRunner = RunEngine(persistence: store, retrySleeper: sleeper)
    let first = try await firstRunner.start(workflow, input: input)

    do {
        try await waitUntil { await actions.trace().shippingCalls == 1 }
    } catch {
        first.cancel()
        _ = try? await first.value()
        throw error
    }
    let beforeCancellation = try #require(await store.loadRun(id: first.id))
    #expect(beforeCancellation.steps.map(\.id) == ["reserve", "charge", "ship"])
    #expect(beforeCancellation.steps.map(\.status) == [.succeeded, .succeeded, .running])
    #expect(beforeCancellation.steps.map(\.attempts) == [1, 2, 1])
    #expect(try JSONDecoder().decode(Reservation.self, from: #require(beforeCancellation.steps[0].output)) ==
        Reservation(reference: "stock:O-42:2", quantity: 2))
    #expect(try JSONDecoder().decode(Payment.self, from: #require(beforeCancellation.steps[1].output)) ==
        Payment(reference: "payment:stock:O-42:2:150", amount: 150))
    #expect(beforeCancellation.steps[2].output == nil)

    first.cancel()
    let cancelled = try requireFailure(await capture { try await first.value() })
    #expect(cancelled is CancellationError)
    #expect(await store.loadRun(id: first.id)?.status == .suspended)
    let suspendedTrace = await actions.trace()
    #expect(suspendedTrace.events == ["body:O-42", "reserve:1", "charge:1", "charge:2", "ship:1"])
    #expect(suspendedTrace.notificationCalls == 0)

    let resumed = try await RunEngine(persistence: store, retrySleeper: sleeper).resume(workflow, id: first.id)
    let result = try await resumed.value()
    #expect(result == PurchaseResult(
        reservation: Reservation(reference: "stock:O-42:2", quantity: 2),
        payment: Payment(reference: "payment:stock:O-42:2:150", amount: 150),
        shipment: Shipment(trackingNumber: "tracking:payment:stock:O-42:2:150"),
        notified: true
    ))
    let trace = await actions.trace()
    #expect(trace.events == [
        "body:O-42", "reserve:1", "charge:1", "charge:2", "ship:1",
        "body:O-42", "ship:2", "notify:tracking:payment:stock:O-42:2:150"
    ])
    #expect(trace.reserveCalls == 1)
    #expect(trace.paymentCalls == 2)
    #expect(trace.shippingCalls == 2)
    #expect(trace.notificationCalls == 1)
    #expect(await sleeper.delays() == [.milliseconds(7)])

    let completed = try #require(await store.loadRun(id: first.id))
    #expect(completed.status == .succeeded)
    #expect(completed.steps.map(\.id) == ["reserve", "charge", "ship", "notify"])
    #expect(completed.steps.map(\.status) == [.succeeded, .succeeded, .succeeded, .succeeded])
    #expect(completed.steps.map(\.attempts) == [1, 2, 2, 1])
    #expect(completed.steps.map(\.failures) == [0, 1, 0, 0])
    #expect(try JSONDecoder().decode(Shipment.self, from: #require(completed.steps[2].output)) == result.shipment)
    #expect(try JSONDecoder().decode(Bool.self, from: #require(completed.steps[3].output)))
    #expect(try JSONDecoder().decode(PurchaseResult.self, from: #require(completed.output)) == result)
}

@Test func exhaustedThirdStepStopsFourStepWorkflowBeforeNotification() async throws {
    let store = InMemoryWorkflowPersistence()
    let sleeper = RecordingSleeper()
    let actions = PurchaseActions(shippingBehavior: .alwaysFail)
    let workflow = PurchaseWorkflow(actions: actions)
    let runner = RunEngine(persistence: store, retrySleeper: sleeper)
    let handle = try await runner.start(
        workflow,
        input: PurchaseInput(orderID: "O-99", quantity: 3, unitPrice: 20)
    )

    let error = try requireFailure(await capture { try await handle.value() })
    let stepError = try #require(error as? StepExecutionError)
    #expect(stepError.stepID == "ship")
    #expect(stepError.attempts == 2)
    #expect(stepError.kind == .step)
    let trace = await actions.trace()
    #expect(trace.events == [
        "body:O-99", "reserve:1", "charge:1", "charge:2", "ship:1", "ship:2"
    ])
    #expect(trace.reserveCalls == 1)
    #expect(trace.paymentCalls == 2)
    #expect(trace.shippingCalls == 2)
    #expect(trace.notificationCalls == 0)
    #expect(await sleeper.delays() == [.milliseconds(7), .milliseconds(5)])

    let failed = try #require(await store.loadRun(id: handle.id))
    #expect(failed.status == .failed)
    #expect(failed.failure?.stepID == "ship")
    #expect(failed.output == nil)
    #expect(failed.steps.map(\.id) == ["reserve", "charge", "ship"])
    #expect(failed.steps.map(\.status) == [.succeeded, .succeeded, .failed])
    #expect(failed.steps.map(\.attempts) == [1, 2, 2])
    #expect(failed.steps.map(\.failures) == [0, 1, 2])
    #expect(try JSONDecoder().decode(Payment.self, from: #require(failed.steps[1].output)) ==
        Payment(reference: "payment:stock:O-99:3:60", amount: 60))
    #expect(failed.steps[2].output == nil)
    let resume = await capture { try await runner.resume(workflow, id: handle.id) }
    #expect(try requireFailure(resume) as? FlowRunError == .runNotSuspended(handle.id))
}
