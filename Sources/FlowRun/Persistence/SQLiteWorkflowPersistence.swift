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
        guard url.isFileURL else {
            throw FlowRunError.persistenceFailed("SQLite requires a file URL")
        }
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
            try Self.execute(pointer, sql: """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    heartbeat_at REAL NOT NULL,
                    record BLOB NOT NULL
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
            guard try load(id: record.id) == nil else { throw FlowRunError.runAlreadyExists(record.id) }
            try RunTransition.validateNew(record)
            try insert(record)
            return record
        }
    }

    public func loadRun(id: RunID) throws -> RunRecord? { try load(id: id) }

    public func claimSuspendedRun(id: RunID, workflowID: String) throws -> RunRecord {
        try mutate(id: id) { record in
            try RunTransition.claim(&record, workflowID: workflowID, now: Date())
            return record
        }
    }

    public func heartbeat(runID: RunID, generation: UInt64) throws {
        try mutate(id: runID) { record in
            try RunTransition.heartbeat(&record, generation: generation, now: Date())
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
                guard let chars = sqlite3_column_text(statement, 0),
                      let uuid = UUID(uuidString: String(cString: chars)) else {
                    throw FlowRunError.persistenceFailed("Invalid stored run ID")
                }
                ids.append(RunID(uuid))
                result = sqlite3_step(statement)
            }
            guard result == SQLITE_DONE else { throw databaseError() }
            var timedOut: [RunID] = []
            for id in ids {
                guard var record = try load(id: id) else { throw FlowRunError.runNotFound(id) }
                if try RunTransition.timeout(&record, before: cutoff, now: Date()) {
                    try update(record)
                    timedOut.append(id)
                }
            }
            return timedOut
        }
    }

    public func startStep(
        runID: RunID, generation: UInt64, index: Int, stepID: String
    ) throws -> StepRecord {
        try mutate(id: runID) { record in
            try RunTransition.startStep(
                &record, generation: generation, index: index, stepID: stepID, now: Date()
            )
        }
    }

    public func recordStepFailure(
        runID: RunID, generation: UInt64, index: Int, failure: FailureRecord
    ) throws -> StepRecord {
        try mutate(id: runID) { record in
            try RunTransition.recordFailure(
                &record, generation: generation, index: index, failure: failure, now: Date()
            )
        }
    }

    public func completeStep(runID: RunID, generation: UInt64, index: Int, output: Data) throws -> StepRecord {
        try mutate(id: runID) { record in
            try RunTransition.completeStep(
                &record, generation: generation, index: index, output: output, now: Date()
            )
        }
    }

    public func finishRun(
        runID: RunID, generation: UInt64, status: RunStatus, output: Data?, failure: FailureRecord?
    ) throws {
        try mutate(id: runID) { record in
            try RunTransition.finish(
                &record, generation: generation, status: status, output: output, failure: failure, now: Date()
            )
        }
    }

    private func mutate<T>(id: RunID, _ operation: (inout RunRecord) throws -> T) throws -> T {
        try transaction {
            guard var record = try load(id: id) else { throw FlowRunError.runNotFound(id) }
            let result = try operation(&record)
            try update(record)
            return result
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

    private func load(id: RunID) throws -> RunRecord? {
        let statement = try prepare("SELECT record FROM runs WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bind(id.description, to: statement, at: 1)
        let result = sqlite3_step(statement)
        if result == SQLITE_DONE { return nil }
        guard result == SQLITE_ROW else { throw databaseError() }
        let count = Int(sqlite3_column_bytes(statement, 0))
        guard let bytes = sqlite3_column_blob(statement, 0) else {
            throw FlowRunError.persistenceFailed("Stored run has no record")
        }
        do {
            let record = try JSONDecoder().decode(RunRecord.self, from: Data(bytes: bytes, count: count))
            guard record.id == id else { throw FlowRunError.persistenceFailed("Stored run ID mismatch") }
            return record
        } catch let error as FlowRunError {
            throw error
        } catch {
            throw FlowRunError.persistenceFailed("Cannot decode stored run: \(error)")
        }
    }

    private func insert(_ record: RunRecord) throws {
        let statement = try prepare("INSERT INTO runs (id, status, heartbeat_at, record) VALUES (?, ?, ?, ?)")
        defer { sqlite3_finalize(statement) }
        try bind(record.id.description, to: statement, at: 1)
        try bind(record.status.rawValue, to: statement, at: 2)
        try bind(record.heartbeatAt.timeIntervalSince1970, to: statement, at: 3)
        try bind(try JSONEncoder().encode(record), to: statement, at: 4)
        guard sqlite3_step(statement) == SQLITE_DONE else { throw databaseError() }
    }

    private func update(_ record: RunRecord) throws {
        let statement = try prepare("UPDATE runs SET status = ?, heartbeat_at = ?, record = ? WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bind(record.status.rawValue, to: statement, at: 1)
        try bind(record.heartbeatAt.timeIntervalSince1970, to: statement, at: 2)
        try bind(try JSONEncoder().encode(record), to: statement, at: 3)
        try bind(record.id.description, to: statement, at: 4)
        guard sqlite3_step(statement) == SQLITE_DONE, sqlite3_changes(database) == 1 else {
            throw databaseError()
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw databaseError()
        }
        return statement
    }

    private func bind(_ value: String, to statement: OpaquePointer, at index: Int32) throws {
        let result = value.withCString {
            sqlite3_bind_text(statement, index, $0, -1, Self.transient)
        }
        guard result == SQLITE_OK else { throw databaseError() }
    }

    private func bind(_ value: Double, to statement: OpaquePointer, at index: Int32) throws {
        guard sqlite3_bind_double(statement, index, value) == SQLITE_OK else { throw databaseError() }
    }

    private func bind(_ value: Data, to statement: OpaquePointer, at index: Int32) throws {
        let result = value.withUnsafeBytes {
            sqlite3_bind_blob(statement, index, $0.baseAddress, Int32(value.count), Self.transient)
        }
        guard result == SQLITE_OK else { throw databaseError() }
    }

    private func databaseError() -> FlowRunError {
        FlowRunError.persistenceFailed(String(cString: sqlite3_errmsg(database)))
    }

    private static let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    private static func execute(_ database: OpaquePointer, sql: String) throws {
        let result = sqlite3_exec(database, sql, nil, nil, nil)
        guard result == SQLITE_OK else {
            throw FlowRunError.persistenceFailed(String(cString: sqlite3_errmsg(database)))
        }
    }
}
