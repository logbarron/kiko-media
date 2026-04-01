import Foundation
import GRDB
import Testing
@testable import KikoMediaCore

@Suite("Database migration")
struct DatabaseMigrationTests {
    @Test("v2 migration upgrades legacy rows with completedAt NULL and retryCount 0")
    func migrationForwardCompatibility() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-db-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tempDir) }
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        let dbPath = tempDir.appendingPathComponent("legacy.db").path

        try seedLegacyV1Database(path: dbPath)

        let database = try Database(
            path: dbPath,
            busyTimeout: 5000,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 500
        )
        let asset = try await database.getAsset(id: "legacy-001")

        #expect(asset != nil)
        #expect(asset?.completedAt == nil)
        #expect(asset?.retryCount == 0)
        #expect(asset?.createdAt.timeIntervalSince1970 == 1_700_000_000)
    }

    @Test("migration is idempotent across repeated Database init")
    func migrationIdempotent() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-db-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tempDir) }
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        let dbPath = tempDir.appendingPathComponent("legacy.db").path

        try seedLegacyV1Database(path: dbPath)

        let db1 = try Database(
            path: dbPath,
            busyTimeout: 5000,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 500
        )
        _ = try await db1.getAsset(id: "legacy-001")

        let db2 = try Database(
            path: dbPath,
            busyTimeout: 5000,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 500
        )
        let asset = try await db2.getAsset(id: "legacy-001")

        #expect(asset != nil)
        #expect(asset?.retryCount == 0)
        #expect(asset?.completedAt == nil)
    }

    @Test("v3 migration upgrades legacy rows with heartCount defaulting to 0")
    func migrationV3BackfillsHeartCount() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-db-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tempDir) }
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        let dbPath = tempDir.appendingPathComponent("legacy.db").path

        try seedLegacyV2Database(path: dbPath)

        let database = try Database(
            path: dbPath,
            busyTimeout: 5000,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 500
        )
        let asset = try await database.getAsset(id: "legacy-v2-001")

        #expect(asset != nil)
        #expect(asset?.id == "legacy-v2-001")
        #expect(asset?.status == .failed)
        #expect(asset?.retryCount == 3)
        #expect(asset?.heartCount == 0)
    }

    @Test("markComplete writes completedAt on success path")
    func markCompleteSetsCompletedAt() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "mark-v2-001", type: .video, originalName: "clip.mov")
        try await env.database.markComplete(id: "mark-v2-001", timestamp: "2026:02:27 10:00:00")
        let asset = try await env.database.getAsset(id: "mark-v2-001")

        #expect(asset?.status == .complete)
        #expect(asset?.completedAt != nil)
        #expect(asset?.retryCount == 0)
    }

    @Test("updateStatus to complete backfills completedAt when missing")
    func updateStatusCompleteBackfillsCompletedAt() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "status-v2-complete-001", type: .video, originalName: "clip.mov")
        try await env.database.updateStatus(id: "status-v2-complete-001", status: .complete)
        let asset = try await env.database.getAsset(id: "status-v2-complete-001")

        #expect(asset?.status == .complete)
        #expect(asset?.completedAt != nil)
    }

    @Test("updateStatus to moderated backfills completedAt when missing")
    func updateStatusModeratedBackfillsCompletedAt() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "status-v2-moderated-001", type: .video, originalName: "clip.mov")
        try await env.database.updateStatus(id: "status-v2-moderated-001", status: .moderated)
        let asset = try await env.database.getAsset(id: "status-v2-moderated-001")

        #expect(asset?.status == .moderated)
        #expect(asset?.completedAt != nil)
    }

    @Test("reEnqueueForRetry increments retryCount and returns queued status")
    func reEnqueueForRetryIncrementsCounter() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "retry-v2-001", type: .video, originalName: "clip.mov")
        try await env.database.updateStatus(id: "retry-v2-001", status: .failed)

        let retryCount = try await env.database.reEnqueueForRetry(id: "retry-v2-001")
        let asset = try await env.database.getAsset(id: "retry-v2-001")

        #expect(retryCount == 1)
        #expect(asset?.status == .queued)
        #expect(asset?.retryCount == 1)
    }

    @Test("incrementRetryCount persists durable retry counter without status mutation")
    func incrementRetryCountPersistsDurably() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "retry-v2-002", type: .video, originalName: "clip.mov")
        try await env.database.updateStatus(id: "retry-v2-002", status: .processing)

        let retryCount = try await env.database.incrementRetryCount(id: "retry-v2-002", by: 2)
        let asset = try await env.database.getAsset(id: "retry-v2-002")

        #expect(retryCount == 2)
        #expect(asset?.status == .processing)
        #expect(asset?.retryCount == 2)
    }
}

private func seedLegacyV1Database(path: String) throws {
    let queue = try DatabaseQueue(path: path)
    try queue.write { db in
        try db.execute(sql: """
        CREATE TABLE assets (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            originalName TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            createdAt DATETIME NOT NULL
        );
        """)
        try db.execute(
            sql: """
            INSERT INTO assets (id, type, timestamp, originalName, status, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            arguments: [
                "legacy-001",
                "video",
                "",
                "legacy.mov",
                "queued",
                Date(timeIntervalSince1970: 1_700_000_000)
            ]
        )
    }
}

private func seedLegacyV2Database(path: String) throws {
    let queue = try DatabaseQueue(path: path)
    try queue.write { db in
        try db.execute(sql: """
        CREATE TABLE assets (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            originalName TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            createdAt DATETIME NOT NULL,
            completedAt DATETIME,
            retryCount INTEGER NOT NULL DEFAULT 0
        );
        """)
        try db.execute(sql: """
        CREATE TABLE grdb_migrations (identifier TEXT NOT NULL PRIMARY KEY);
        """)
        try db.execute(
            sql: "INSERT INTO grdb_migrations (identifier) VALUES (?)",
            arguments: ["v1_create_assets"]
        )
        try db.execute(
            sql: "INSERT INTO grdb_migrations (identifier) VALUES (?)",
            arguments: ["v2_walltime_columns"]
        )
        try db.execute(
            sql: """
            INSERT INTO assets (id, type, timestamp, originalName, status, createdAt, completedAt, retryCount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            arguments: [
                "legacy-v2-001",
                "video",
                "2026:01:02 03:04:05",
                "legacy-v2.mov",
                "failed",
                Date(timeIntervalSince1970: 1_700_000_123),
                Date(timeIntervalSince1970: 1_700_000_456),
                3
            ]
        )
    }
}
