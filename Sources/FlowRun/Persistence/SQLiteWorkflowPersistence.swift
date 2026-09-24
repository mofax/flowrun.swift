import Foundation
import SQLite3

private final class SQLiteConnection: @unchecked Sendable {
    let pointer: OpaquePointer
    init(_ pointer: OpaquePointer) { self.pointer = pointer }
    deinit { sqlite3_close(pointer) }
}

/// File-backed persistence. Separate instances and processes may open the same database.
public actor SQLiteWorkflowPersistence: WorkflowPersistence {
    private let connection: SQLiteConnection
    private var database: OpaquePointer { connection.pointer }

    public init(url: URL) throws {
        guard url.isFileURL else { throw FlowRunError.persistenceFailed("SQLite requires a file URL") }
        var pointer: OpaquePointer?
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        guard sqlite3_open_v2(url.path, &pointer, flags, nil) == SQLITE_OK, let pointer else {
            let message = pointer.map { String(cString: sqlite3_errmsg($0)) } ?? "Cannot open database"
            if let pointer { sqlite3_close(pointer) }
            throw FlowRunError.persistenceFailed(message)
        }
        do {
            sqlite3_busy_timeout(pointer, 5_000)
            try Self.execute(pointer, sql: "PRAGMA journal_mode=WAL")
            try Self.execute(pointer, sql: "PRAGMA foreign_keys=ON")
            try Self.execute(pointer, sql: """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY NOT NULL,
                    workflow_id TEXT NOT NULL,
                    input BLOB NOT NULL,
                    status TEXT NOT NULL,
                    output BLOB,
                    failure_kind TEXT,
                    failure_message TEXT,
                    failure_step_id TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    owner_generation TEXT NOT NULL,
                    heartbeat_at REAL NOT NULL
                )
                """)
            try Self.execute(pointer, sql: """
                CREATE TABLE IF NOT EXISTS run_steps (
                    run_id TEXT NOT NULL,
                    step_index INTEGER NOT NULL,
                    step_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    output BLOB,
                    attempts INTEGER NOT NULL,
                    failures INTEGER NOT NULL,
                    failure_kind TEXT,
                    failure_message TEXT,
                    failure_step_id TEXT,
                    PRIMARY KEY (run_id, step_index),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                )
                """)
            try Self.execute(pointer, sql: "CREATE INDEX IF NOT EXISTS runs_status_heartbeat ON runs(status, heartbeat_at)")
        } catch {
            sqlite3_close(pointer)
            throw error
        }
        connection = SQLiteConnection(pointer)
    }

    public func createRun(_ record: RunRecord) throws -> RunRecord {
        try transaction {
            guard !(try exists(id: record.id)) else { throw FlowRunError.runAlreadyExists(record.id) }
            try RunTransition.validateNew(record)
            try insertRun(record)
            return record
        }
    }

    public func loadRun(id: RunID) throws -> RunRecord? { try load(id: id, includePayloads: true) }

    public func claimSuspendedRun(id: RunID, workflowID: String) throws -> RunRecord {
        try transaction {
            guard var record = try load(id: id, includePayloads: false) else {
                throw FlowRunError.runNotFound(id)
            }
            try RunTransition.claim(&record, workflowID: workflowID, now: Date())
            try updateClaim(record)
            guard let claimed = try load(id: id, includePayloads: true) else {
                throw FlowRunError.runNotFound(id)
            }
            return claimed
        }
    }

    public func heartbeat(runID: RunID, generation: UInt64) throws {
        try transaction {
            guard var record = try load(id: runID, includePayloads: false) else {
                throw FlowRunError.runNotFound(runID)
            }
            try RunTransition.heartbeat(&record, generation: generation, now: Date())
            try updateHeartbeat(record)
        }
    }

    public func timeoutStaleRuns(before cutoff: Date) throws -> [RunID] {
        try transaction {
            let statement = try prepare("SELECT id FROM runs WHERE status = ? AND heartbeat_at <= ?")
            defer { sqlite3_finalize(statement) }
            try bind(RunStatus.running.rawValue, to: statement, at: 1)
            try bind(cutoff.timeIntervalSince1970, to: statement, at: 2)
            var ids: [RunID] = []
            var result = sqlite3_step(statement)
            while result == SQLITE_ROW {
                guard let chars = sqlite3_column_text(statement, 0), let uuid = UUID(uuidString: String(cString: chars)) else {
                    throw FlowRunError.persistenceFailed("Invalid stored run ID")
                }
                ids.append(RunID(uuid))
                result = sqlite3_step(statement)
            }
            guard result == SQLITE_DONE else { throw databaseError() }

            var timedOut: [RunID] = []
            for id in ids {
                guard var record = try load(id: id, includePayloads: false) else { throw FlowRunError.runNotFound(id) }
                let previousLastStep = record.steps.last
                if try RunTransition.timeout(&record, before: cutoff, now: Date()) {
                    try updateTerminalRun(record)
                    try updateLastStepIfChanged(previous: previousLastStep, record: record)
                    timedOut.append(id)
                }
            }
            return timedOut
        }
    }

    public func startStep(runID: RunID, generation: UInt64, index: Int, stepID: String) throws -> StepRecord {
        try transaction {
            guard var record = try load(id: runID, includePayloads: false) else { throw FlowRunError.runNotFound(runID) }
            let existingStepCount = record.steps.count
            let step = try RunTransition.startStep(&record, generation: generation, index: index, stepID: stepID, now: Date())
            try touchRun(record)
            if record.steps.count > existingStepCount { try insertStep(step, runID: runID) }
            else { try updateStartedStep(step, runID: runID) }
            return step
        }
    }

    public func recordStepFailure(runID: RunID, generation: UInt64, index: Int, failure: FailureRecord) throws -> StepRecord {
        try transaction {
            guard var record = try load(id: runID, includePayloads: false) else { throw FlowRunError.runNotFound(runID) }
            let step = try RunTransition.recordFailure(&record, generation: generation, index: index, failure: failure, now: Date())
            try touchRun(record)
            try updateFailedStep(step, runID: runID)
            return step
        }
    }

    public func completeStep(runID: RunID, generation: UInt64, index: Int, output: Data) throws -> StepRecord {
        try transaction {
            guard var record = try load(id: runID, includePayloads: false) else { throw FlowRunError.runNotFound(runID) }
            let step = try RunTransition.completeStep(&record, generation: generation, index: index, output: output, now: Date())
            try touchRun(record)
            try updateCompletedStep(step, runID: runID)
            return step
        }
    }

    public func finishRun(runID: RunID, generation: UInt64, status: RunStatus, output: Data?, failure: FailureRecord?) throws {
        try transaction {
            guard var record = try load(id: runID, includePayloads: false) else { throw FlowRunError.runNotFound(runID) }
            let previousLastStep = record.steps.last
            try RunTransition.finish(&record, generation: generation, status: status, output: output, failure: failure, now: Date())
            try updateTerminalRun(record)
            try updateLastStepIfChanged(previous: previousLastStep, record: record)
        }
    }

    private func transaction<T>(_ body: () throws -> T) throws -> T {
        try Self.execute(database, sql: "BEGIN IMMEDIATE")
        do {
            let result = try body()
            try Self.execute(database, sql: "COMMIT")
            return result
        } catch {
            try? Self.execute(database, sql: "ROLLBACK")
            throw error
        }
    }

    private func exists(id: RunID) throws -> Bool {
        let statement = try prepare("SELECT 1 FROM runs WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bind(id.description, to: statement, at: 1)
        let result = sqlite3_step(statement)
        guard result == SQLITE_ROW || result == SQLITE_DONE else { throw databaseError() }
        return result == SQLITE_ROW
    }

    private func load(id: RunID, includePayloads: Bool) throws -> RunRecord? {
        let runStatement = try prepare("""
            SELECT workflow_id, \(includePayloads ? "input" : "NULL"), status,
                   \(includePayloads ? "output" : "NULL"), failure_kind, failure_message, failure_step_id,
                   created_at, updated_at, owner_generation, heartbeat_at
            FROM runs WHERE id = ?
            """)
        defer { sqlite3_finalize(runStatement) }
        try bind(id.description, to: runStatement, at: 1)
        let result = sqlite3_step(runStatement)
        if result == SQLITE_DONE { return nil }
        guard result == SQLITE_ROW else { throw databaseError() }

        let workflowID = try requiredString(runStatement, at: 0, field: "workflow ID")
        let status = try runStatus(runStatement, at: 2)
        let input = includePayloads ? try requiredData(runStatement, at: 1, field: "run input") : Data()
        let output = includePayloads ? data(runStatement, at: 3) : nil
        let runFailure = try failure(runStatement, startingAt: 4)
        let createdAt = Date(timeIntervalSince1970: sqlite3_column_double(runStatement, 7))
        let updatedAt = Date(timeIntervalSince1970: sqlite3_column_double(runStatement, 8))
        let ownerGeneration = try generation(runStatement, at: 9)
        let heartbeatAt = Date(timeIntervalSince1970: sqlite3_column_double(runStatement, 10))

        let stepStatement = try prepare("""
            SELECT step_id, step_index, status, \(includePayloads ? "output" : "NULL"), attempts, failures,
                   failure_kind, failure_message, failure_step_id
            FROM run_steps WHERE run_id = ? ORDER BY step_index ASC
            """)
        defer { sqlite3_finalize(stepStatement) }
        try bind(id.description, to: stepStatement, at: 1)
        var steps: [StepRecord] = []
        var stepResult = sqlite3_step(stepStatement)
        while stepResult == SQLITE_ROW {
            steps.append(StepRecord(
                id: try requiredString(stepStatement, at: 0, field: "step ID"),
                index: try integer(stepStatement, at: 1, field: "step index"),
                status: try stepStatus(stepStatement, at: 2),
                output: includePayloads ? data(stepStatement, at: 3) : nil,
                attempts: try integer(stepStatement, at: 4, field: "attempts"),
                failures: try integer(stepStatement, at: 5, field: "failures"),
                lastFailure: try failure(stepStatement, startingAt: 6)
            ))
            stepResult = sqlite3_step(stepStatement)
        }
        guard stepResult == SQLITE_DONE else { throw databaseError() }

        var record = RunRecord(id: id, workflowID: workflowID, input: input, createdAt: createdAt)
        record.status = status
        record.steps = steps
        record.output = output
        record.failure = runFailure
        record.updatedAt = updatedAt
        record.ownerGeneration = ownerGeneration
        record.heartbeatAt = heartbeatAt
        return record
    }

    private func insertRun(_ record: RunRecord) throws {
        let statement = try prepare("""
            INSERT INTO runs (id, workflow_id, input, status, output, failure_kind, failure_message, failure_step_id,
                              created_at, updated_at, owner_generation, heartbeat_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
        defer { sqlite3_finalize(statement) }
        try bind(record.id.description, to: statement, at: 1)
        try bind(record.workflowID, to: statement, at: 2)
        try bind(record.input, to: statement, at: 3)
        try bind(record.status.rawValue, to: statement, at: 4)
        try bind(record.output, to: statement, at: 5)
        try bind(record.failure, to: statement, startingAt: 6)
        try bind(record.createdAt.timeIntervalSince1970, to: statement, at: 9)
        try bind(record.updatedAt.timeIntervalSince1970, to: statement, at: 10)
        try bind(String(record.ownerGeneration), to: statement, at: 11)
        try bind(record.heartbeatAt.timeIntervalSince1970, to: statement, at: 12)
        try requireOneChange(statement)
    }

    private func insertStep(_ step: StepRecord, runID: RunID) throws {
        let statement = try prepare("""
            INSERT INTO run_steps (run_id, step_index, step_id, status, output, attempts, failures,
                                   failure_kind, failure_message, failure_step_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
        defer { sqlite3_finalize(statement) }
        try bind(runID.description, to: statement, at: 1)
        try bind(step.index, to: statement, at: 2)
        try bind(step.id, to: statement, at: 3)
        try bind(step.status.rawValue, to: statement, at: 4)
        try bind(step.output, to: statement, at: 5)
        try bind(step.attempts, to: statement, at: 6)
        try bind(step.failures, to: statement, at: 7)
        try bind(step.lastFailure, to: statement, startingAt: 8)
        try requireOneChange(statement)
    }

    private func touchRun(_ record: RunRecord) throws {
        let statement = try prepare("UPDATE runs SET updated_at = ? WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bind(record.updatedAt.timeIntervalSince1970, to: statement, at: 1)
        try bind(record.id.description, to: statement, at: 2)
        try requireOneChange(statement)
    }

    private func updateHeartbeat(_ record: RunRecord) throws {
        let statement = try prepare("UPDATE runs SET updated_at = ?, heartbeat_at = ? WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bind(record.updatedAt.timeIntervalSince1970, to: statement, at: 1)
        try bind(record.heartbeatAt.timeIntervalSince1970, to: statement, at: 2)
        try bind(record.id.description, to: statement, at: 3)
        try requireOneChange(statement)
    }

    private func updateClaim(_ record: RunRecord) throws {
        let statement = try prepare("UPDATE runs SET status = ?, updated_at = ?, owner_generation = ?, heartbeat_at = ? WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bind(record.status.rawValue, to: statement, at: 1)
        try bind(record.updatedAt.timeIntervalSince1970, to: statement, at: 2)
        try bind(String(record.ownerGeneration), to: statement, at: 3)
        try bind(record.heartbeatAt.timeIntervalSince1970, to: statement, at: 4)
        try bind(record.id.description, to: statement, at: 5)
        try requireOneChange(statement)
    }

    private func updateStartedStep(_ step: StepRecord, runID: RunID) throws {
        let statement = try prepare("UPDATE run_steps SET status = ?, attempts = ? WHERE run_id = ? AND step_index = ?")
        defer { sqlite3_finalize(statement) }
        try bind(step.status.rawValue, to: statement, at: 1)
        try bind(step.attempts, to: statement, at: 2)
        try bind(runID.description, to: statement, at: 3)
        try bind(step.index, to: statement, at: 4)
        try requireOneChange(statement)
    }

    private func updateFailedStep(_ step: StepRecord, runID: RunID) throws {
        let statement = try prepare("""
            UPDATE run_steps SET status = ?, failures = ?, failure_kind = ?, failure_message = ?, failure_step_id = ?
            WHERE run_id = ? AND step_index = ?
            """)
        defer { sqlite3_finalize(statement) }
        try bind(step.status.rawValue, to: statement, at: 1)
        try bind(step.failures, to: statement, at: 2)
        try bind(step.lastFailure, to: statement, startingAt: 3)
        try bind(runID.description, to: statement, at: 6)
        try bind(step.index, to: statement, at: 7)
        try requireOneChange(statement)
    }

    private func updateCompletedStep(_ step: StepRecord, runID: RunID) throws {
        let statement = try prepare("UPDATE run_steps SET status = ?, output = ? WHERE run_id = ? AND step_index = ?")
        defer { sqlite3_finalize(statement) }
        try bind(step.status.rawValue, to: statement, at: 1)
        try bind(step.output, to: statement, at: 2)
        try bind(runID.description, to: statement, at: 3)
        try bind(step.index, to: statement, at: 4)
        try requireOneChange(statement)
    }

    private func updateTerminalRun(_ record: RunRecord) throws {
        let statement = try prepare("""
            UPDATE runs SET status = ?, output = ?, failure_kind = ?, failure_message = ?, failure_step_id = ?,
                            updated_at = ?, owner_generation = ? WHERE id = ?
            """)
        defer { sqlite3_finalize(statement) }
        try bind(record.status.rawValue, to: statement, at: 1)
        try bind(record.output, to: statement, at: 2)
        try bind(record.failure, to: statement, startingAt: 3)
        try bind(record.updatedAt.timeIntervalSince1970, to: statement, at: 6)
        try bind(String(record.ownerGeneration), to: statement, at: 7)
        try bind(record.id.description, to: statement, at: 8)
        try requireOneChange(statement)
    }

    private func updateLastStepIfChanged(previous: StepRecord?, record: RunRecord) throws {
        guard let current = record.steps.last,
              previous?.status != current.status || previous?.lastFailure != current.lastFailure else { return }
        let statement = try prepare("""
            UPDATE run_steps SET status = ?, failure_kind = ?, failure_message = ?, failure_step_id = ?
            WHERE run_id = ? AND step_index = ?
            """)
        defer { sqlite3_finalize(statement) }
        try bind(current.status.rawValue, to: statement, at: 1)
        try bind(current.lastFailure, to: statement, startingAt: 2)
        try bind(record.id.description, to: statement, at: 5)
        try bind(current.index, to: statement, at: 6)
        try requireOneChange(statement)
    }

    private func requireOneChange(_ statement: OpaquePointer) throws {
        guard sqlite3_step(statement) == SQLITE_DONE, sqlite3_changes(database) == 1 else { throw databaseError() }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK, let statement else { throw databaseError() }
        return statement
    }

    private func bind(_ value: String, to statement: OpaquePointer, at index: Int32) throws {
        let result = value.withCString { sqlite3_bind_text(statement, index, $0, -1, Self.transient) }
        guard result == SQLITE_OK else { throw databaseError() }
    }

    private func bind(_ value: String?, to statement: OpaquePointer, at index: Int32) throws {
        if let value { try bind(value, to: statement, at: index) } else { try bindNull(to: statement, at: index) }
    }

    private func bind(_ value: Double, to statement: OpaquePointer, at index: Int32) throws {
        guard sqlite3_bind_double(statement, index, value) == SQLITE_OK else { throw databaseError() }
    }

    private func bind(_ value: Int, to statement: OpaquePointer, at index: Int32) throws {
        guard sqlite3_bind_int64(statement, index, Int64(value)) == SQLITE_OK else { throw databaseError() }
    }

    private func bind(_ value: Data, to statement: OpaquePointer, at index: Int32) throws {
        let result = value.withUnsafeBytes { sqlite3_bind_blob(statement, index, $0.baseAddress, Int32(value.count), Self.transient) }
        guard result == SQLITE_OK else { throw databaseError() }
    }

    private func bind(_ value: Data?, to statement: OpaquePointer, at index: Int32) throws {
        if let value { try bind(value, to: statement, at: index) } else { try bindNull(to: statement, at: index) }
    }

    private func bind(_ failure: FailureRecord?, to statement: OpaquePointer, startingAt index: Int32) throws {
        try bind(failure?.kind.rawValue, to: statement, at: index)
        try bind(failure?.message, to: statement, at: index + 1)
        try bind(failure?.stepID, to: statement, at: index + 2)
    }

    private func bindNull(to statement: OpaquePointer, at index: Int32) throws {
        guard sqlite3_bind_null(statement, index) == SQLITE_OK else { throw databaseError() }
    }

    private func requiredString(_ statement: OpaquePointer, at index: Int32, field: String) throws -> String {
        guard let chars = sqlite3_column_text(statement, index) else { throw FlowRunError.persistenceFailed("Stored \(field) is missing") }
        return String(cString: chars)
    }

    private func requiredData(_ statement: OpaquePointer, at index: Int32, field: String) throws -> Data {
        guard let value = data(statement, at: index) else { throw FlowRunError.persistenceFailed("Stored \(field) is missing") }
        return value
    }

    private func data(_ statement: OpaquePointer, at index: Int32) -> Data? {
        guard sqlite3_column_type(statement, index) != SQLITE_NULL else { return nil }
        let count = Int(sqlite3_column_bytes(statement, index))
        if count == 0 { return Data() }
        guard let bytes = sqlite3_column_blob(statement, index) else { return nil }
        return Data(bytes: bytes, count: count)
    }

    private func failure(_ statement: OpaquePointer, startingAt index: Int32) throws -> FailureRecord? {
        guard sqlite3_column_type(statement, index) != SQLITE_NULL else {
            guard sqlite3_column_type(statement, index + 1) == SQLITE_NULL, sqlite3_column_type(statement, index + 2) == SQLITE_NULL else {
                throw FlowRunError.persistenceFailed("Stored failure is incomplete")
            }
            return nil
        }
        guard let kind = FailureKind(rawValue: try requiredString(statement, at: index, field: "failure kind")) else {
            throw FlowRunError.persistenceFailed("Invalid stored failure kind")
        }
        let message = try requiredString(statement, at: index + 1, field: "failure message")
        let stepID = sqlite3_column_type(statement, index + 2) == SQLITE_NULL ? nil : try requiredString(statement, at: index + 2, field: "failure step ID")
        return FailureRecord(kind: kind, message: message, stepID: stepID)
    }

    private func runStatus(_ statement: OpaquePointer, at index: Int32) throws -> RunStatus {
        guard let value = RunStatus(rawValue: try requiredString(statement, at: index, field: "run status")) else {
            throw FlowRunError.persistenceFailed("Invalid stored run status")
        }
        return value
    }

    private func stepStatus(_ statement: OpaquePointer, at index: Int32) throws -> StepStatus {
        guard let value = StepStatus(rawValue: try requiredString(statement, at: index, field: "step status")) else {
            throw FlowRunError.persistenceFailed("Invalid stored step status")
        }
        return value
    }

    private func generation(_ statement: OpaquePointer, at index: Int32) throws -> UInt64 {
        let value = try requiredString(statement, at: index, field: "owner generation")
        guard let generation = UInt64(value) else { throw FlowRunError.persistenceFailed("Invalid stored owner generation") }
        return generation
    }

    private func integer(_ statement: OpaquePointer, at index: Int32, field: String) throws -> Int {
        let value = sqlite3_column_int64(statement, index)
        guard value >= Int64(Int.min), value <= Int64(Int.max) else { throw FlowRunError.persistenceFailed("Invalid stored \(field)") }
        return Int(value)
    }

    private func databaseError() -> FlowRunError {
        FlowRunError.persistenceFailed(String(cString: sqlite3_errmsg(database)))
    }

    private static let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    private static func execute(_ database: OpaquePointer, sql: String) throws {
        guard sqlite3_exec(database, sql, nil, nil, nil) == SQLITE_OK else {
            throw FlowRunError.persistenceFailed(String(cString: sqlite3_errmsg(database)))
        }
    }
}
