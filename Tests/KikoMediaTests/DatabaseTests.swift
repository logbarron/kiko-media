import Testing
import Foundation
import class GRDB.DatabaseQueue
import struct GRDB.DatabaseError
@testable import KikoMediaCore

// MARK: - Status Lifecycle

@Suite("Database Status Lifecycle")
struct DatabaseLifecycleTests {

    @Test("Queued → processing → complete lifecycle")
    func completeLifecycle() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "lifecycle-001"
        let inserted = try await env.database.insertQueued(id: id, type: .image, originalName: "photo.jpg")
        #expect(inserted)

        var asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .queued)

        try await env.database.updateStatus(id: id, status: .processing)
        asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .processing)

        try await env.database.markComplete(id: id, timestamp: "2025:02:05 14:30:00")
        asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .complete)
        #expect(asset?.timestamp == "2025:02:05 14:30:00")
    }

    @Test("Queued → processing → failed lifecycle")
    func failedLifecycle() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "lifecycle-fail-001"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "clip.mov")
        try await env.database.updateStatus(id: id, status: .processing)
        try await env.database.updateStatus(id: id, status: .failed)

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .failed)
    }

    @Test("Complete → moderated → complete (moderation toggle)")
    func moderationToggle() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mod-toggle-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "photo.jpg")
        try await env.database.markComplete(id: id, timestamp: "2025:02:05 12:00:00")

        try await env.database.updateStatus(id: id, status: .moderated)
        var asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .moderated)

        try await env.database.updateStatus(id: id, status: .complete)
        asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .complete)
    }

    @Test("markComplete updates both status and timestamp atomically")
    func markCompleteUpdates() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mark-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "photo.jpg")

        // Initially timestamp is empty (set by insertQueued)
        var asset = try await env.database.getAsset(id: id)
        #expect(asset?.timestamp == "")

        try await env.database.markComplete(id: id, timestamp: "2025:06:15 09:45:00")
        asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .complete)
        #expect(asset?.timestamp == "2025:06:15 09:45:00")
    }

    @Test("markComplete rolls back cleanly when completion update aborts")
    func markCompleteRollbackOnAbort() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mark-rollback-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "photo.jpg")

        let triggerDb = try DatabaseQueue(path: env.config.databasePath)
        try await triggerDb.write { db in
            try db.execute(
                sql: """
                CREATE TRIGGER fail_mark_complete_rollback
                BEFORE UPDATE OF status ON assets
                FOR EACH ROW
                WHEN NEW.id = '\(id)' AND NEW.status = 'complete'
                BEGIN
                    SELECT RAISE(ABORT, 'forced markComplete rollback');
                END;
                """
            )
        }

        do {
            try await env.database.markComplete(id: id, timestamp: "2025:06:15 09:45:00")
            Issue.record("Expected markComplete to fail due to trigger abort")
        } catch {
            // Expected.
        }

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .queued)
        #expect(asset?.timestamp == "")
    }

    @Test("updateType changes the asset type")
    func updateType() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "type-001"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "file.mov")

        var asset = try await env.database.getAsset(id: id)
        #expect(asset?.type == .video)

        try await env.database.updateType(id: id, type: .image)
        asset = try await env.database.getAsset(id: id)
        #expect(asset?.type == .image)
    }
}

// MARK: - Idempotency & Uniqueness

@Suite("Database Idempotency")
struct DatabaseIdempotencyTests {

    @Test("insertQueued is idempotent (duplicate returns false)")
    func insertQueuedIdempotent() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "idempotent-001"
        let first = try await env.database.insertQueued(id: id, type: .image, originalName: "photo.jpg")
        #expect(first == true)

        let second = try await env.database.insertQueued(id: id, type: .image, originalName: "photo.jpg")
        #expect(second == false)

        // Only one row exists
        let count = try await env.database.getTotalAssetCount()
        #expect(count == 1)
    }

    @Test("insertComplete is idempotent (duplicate returns false)")
    func insertCompleteIdempotent() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "rebuild-001"
        let first = try await env.database.insertComplete(
            id: id, type: .image, timestamp: "2025:02:05 12:00:00",
            originalName: "photo.jpg", status: .complete
        )
        #expect(first == true)

        let second = try await env.database.insertComplete(
            id: id, type: .image, timestamp: "2025:02:05 12:00:00",
            originalName: "photo.jpg", status: .complete
        )
        #expect(second == false)
    }

    @Test("getAsset returns nil for nonexistent ID")
    func getAssetMissing() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let asset = try await env.database.getAsset(id: "does-not-exist")
        #expect(asset == nil)
    }

    @Test("updateStatus throws when asset row does not exist")
    func updateStatusMissingRowThrows() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        do {
            try await env.database.updateStatus(id: "missing-status-row", status: .failed)
            Issue.record("Expected updateStatus to throw for missing row")
        } catch let error as DatabaseWriteError {
            #expect(error == .assetNotFound("missing-status-row"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("markComplete throws when asset row does not exist")
    func markCompleteMissingRowThrows() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        do {
            try await env.database.markComplete(id: "missing-complete-row", timestamp: "2026:02:28 12:00:00")
            Issue.record("Expected markComplete to throw for missing row")
        } catch let error as DatabaseWriteError {
            #expect(error == .assetNotFound("missing-complete-row"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("updateType throws when asset row does not exist")
    func updateTypeMissingRowThrows() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        do {
            try await env.database.updateType(id: "missing-type-row", type: .video)
            Issue.record("Expected updateType to throw for missing row")
        } catch let error as DatabaseWriteError {
            #expect(error == .assetNotFound("missing-type-row"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("incrementRetryCount throws when asset row does not exist")
    func incrementRetryCountMissingRowThrows() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        do {
            _ = try await env.database.incrementRetryCount(id: "missing-retry-row")
            Issue.record("Expected incrementRetryCount to throw for missing row")
        } catch let error as DatabaseWriteError {
            #expect(error == .assetNotFound("missing-retry-row"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("reEnqueueForRetry throws when asset row does not exist")
    func reEnqueueForRetryMissingRowThrows() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        do {
            _ = try await env.database.reEnqueueForRetry(id: "missing-reenqueue-row")
            Issue.record("Expected reEnqueueForRetry to throw for missing row")
        } catch let error as DatabaseWriteError {
            #expect(error == .assetNotFound("missing-reenqueue-row"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("assetExists returns correct values")
    func assetExists() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        #expect(try await env.database.assetExists(id: "nope") == false)

        _ = try await env.database.insertQueued(id: "exists-001", type: .image, originalName: "photo.jpg")
        #expect(try await env.database.assetExists(id: "exists-001") == true)
    }
}

// MARK: - Query Filtering

@Suite("Database Query Filtering")
struct DatabaseFilterTests {

    /// Seed a database with assets in every status
    private func seedAllStatuses(_ db: Database) async throws {
        // Complete assets (visible in public gallery)
        for i in 0..<3 {
            _ = try await db.insertQueued(id: "complete-\(i)", type: .image, originalName: "c\(i).jpg")
            try await db.markComplete(id: "complete-\(i)", timestamp: "2025:02:05 \(String(format: "%02d", i)):00:00")
        }
        // Moderated asset (hidden from public, visible in moderation)
        _ = try await db.insertQueued(id: "moderated-0", type: .image, originalName: "m0.jpg")
        try await db.markComplete(id: "moderated-0", timestamp: "2025:02:05 10:00:00")
        try await db.updateStatus(id: "moderated-0", status: .moderated)
        // Queued asset
        _ = try await db.insertQueued(id: "queued-0", type: .video, originalName: "q0.mov")
        // Processing asset
        _ = try await db.insertQueued(id: "processing-0", type: .video, originalName: "p0.mov")
        try await db.updateStatus(id: "processing-0", status: .processing)
        // Failed asset
        _ = try await db.insertQueued(id: "failed-0", type: .image, originalName: "f0.jpg")
        try await db.updateStatus(id: "failed-0", status: .failed)
    }

    @Test("getAllAssets returns ONLY complete assets")
    func publicGalleryFilter() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await seedAllStatuses(env.database)

        let assets = try await env.database.getAllAssets()
        #expect(assets.count == 3)
        #expect(assets.allSatisfy { $0.status == .complete })
    }

    @Test("getAssetCount counts only complete assets")
    func publicCount() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await seedAllStatuses(env.database)

        let count = try await env.database.getAssetCount()
        #expect(count == 3)
    }

    @Test("getModerationAssets returns complete + moderated")
    func moderationFilter() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await seedAllStatuses(env.database)

        let assets = try await env.database.getModerationAssets()
        #expect(assets.count == 4) // 3 complete + 1 moderated
        let statuses = Set(assets.map { $0.status })
        #expect(statuses == [.complete, .moderated])
    }

    @Test("getModerationAssetCount counts complete + moderated")
    func moderationCount() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await seedAllStatuses(env.database)

        let count = try await env.database.getModerationAssetCount()
        #expect(count == 4)
    }

    @Test("getTotalAssetCount counts ALL statuses")
    func totalCount() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await seedAllStatuses(env.database)

        let count = try await env.database.getTotalAssetCount()
        #expect(count == 7) // 3 complete + 1 moderated + 1 queued + 1 processing + 1 failed
    }

    @Test("getDashboardCounts separates complete totals from non-complete states")
    func dashboardCounts() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await seedAllStatuses(env.database)

        let counts = try await env.database.getDashboardCounts()
        #expect(counts.queuedImages == 0)
        #expect(counts.queuedVideos == 1)
        #expect(counts.processingImages == 0)
        #expect(counts.processingVideos == 1)
        #expect(counts.completeImages == 3)
        #expect(counts.completeVideos == 0)
        #expect(counts.moderated == 1)
        #expect(counts.failed == 1)
    }

    @Test("getVerifiableAssets returns complete, moderated, and processing")
    func verifiableAssets() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await seedAllStatuses(env.database)

        let assets = try await env.database.getVerifiableAssets()
        let statuses = Set(assets.map { $0.status })
        #expect(statuses == [.complete, .moderated, .processing])
        #expect(assets.count == 5) // 3 complete + 1 moderated + 1 processing
    }

    @Test("getAllAssets orders by timestamp descending")
    func sortOrder() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let timestamps = [
            ("asset-early", "2025:02:05 08:00:00"),
            ("asset-late", "2025:02:05 20:00:00"),
            ("asset-mid", "2025:02:05 12:00:00"),
        ]
        for (id, ts) in timestamps {
            _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
            try await env.database.markComplete(id: id, timestamp: ts)
        }

        let assets = try await env.database.getAllAssets()
        #expect(assets.map(\.id) == ["asset-late", "asset-mid", "asset-early"])
    }
}

// MARK: - Batch Queries

@Suite("Database Batch Queries")
struct DatabaseBatchTests {

    @Test("getExistingIds returns matching IDs")
    func existingIds() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "a", type: .image, originalName: "a.jpg")
        _ = try await env.database.insertQueued(id: "b", type: .image, originalName: "b.jpg")

        let result = try await env.database.getExistingIds(from: ["a", "b", "c"])
        #expect(result == Set(["a", "b"]))
    }

    @Test("getExistingIds with empty input returns empty set")
    func existingIdsEmpty() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let result = try await env.database.getExistingIds(from: [])
        #expect(result.isEmpty)
    }

    @Test("getExistingIds handles large ID lists (avoids SQLite parameter limit)")
    func existingIdsBatching() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        // Insert 10 assets
        for i in 0..<10 {
            _ = try await env.database.insertQueued(id: "batch-\(i)", type: .image, originalName: "\(i).jpg")
        }

        // Query with >999 candidate IDs (SQLite parameter limit is ~999)
        var candidates: [String] = []
        for i in 0..<1_200 {
            candidates.append("batch-\(i)")
        }

        let result = try await env.database.getExistingIds(from: candidates)
        #expect(result.count == 10)
    }

    @Test("Database init rejects non-positive SQL batch size")
    func rejectsNonPositiveBatchSize() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-dbtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let dbPath = tempDir.appendingPathComponent("test.db").path

        do {
            _ = try Database(
                path: dbPath,
                busyTimeout: 5000,
                cacheSize: -20000,
                defaultPageSize: 100,
                maxPageSize: 500,
                maxPageOffset: 10_000,
                sqlBatchSize: 0
            )
            Issue.record("Expected Database init to reject sqlBatchSize <= 0")
        } catch let error as DatabaseInitError {
            #expect(error == .invalidSQLBatchSize(0))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

}

// MARK: - PRAGMA Safety

@Suite("Database PRAGMA Safety")
struct DatabasePragmaSafetyTests {
    @Test("sqliteIntegerLiteral emits strict integer tokens")
    func sqliteIntegerLiteralFormatting() {
        let samples = [Int.min, -20_000, -1, 0, 1, 20_000, Int.max]
        for value in samples {
            let literal = Database.sqliteIntegerLiteral(value)
            #expect(Int(literal) == value)
            #expect(!literal.isEmpty)
            #expect(literal.allSatisfy { $0 == "-" || $0.isNumber })
            #expect(!literal.hasPrefix("+"))
            if literal.count > 1 {
                #expect(!literal.dropFirst().contains("-"))
            }
        }
    }

    @Test("Database init applies busy timeout during write contention")
    func busyTimeoutAppliesUnderWriteContention() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-db-busy-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let dbPath = tempDir.appendingPathComponent("test.db").path
        let database = try Database(
            path: dbPath,
            busyTimeout: 200,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 500
        )

        let lockHolder = try DatabaseQueue(path: dbPath)
        let lockAcquired = DispatchSemaphore(value: 0)
        let releaseLock = DispatchSemaphore(value: 0)
        final class HolderErrorBox: @unchecked Sendable { var error: Error? }
        let holderError = HolderErrorBox()
        let holderQueue = DispatchQueue(label: "DatabaseTests.busyTimeout.lockHolder")

        holderQueue.async {
            do {
                try lockHolder.writeWithoutTransaction { db in
                    try db.execute(sql: "BEGIN IMMEDIATE TRANSACTION")
                    lockAcquired.signal()
                    releaseLock.wait()
                    try db.execute(sql: "ROLLBACK")
                }
            } catch {
                holderError.error = error
                lockAcquired.signal()
            }
        }
        defer {
            releaseLock.signal()
            holderQueue.sync {}
        }

        #expect(waitForSemaphore(lockAcquired, timeout: .now() + 5) == .success)
        if let error = holderError.error {
            throw error
        }

        let clock = ContinuousClock()
        let start = clock.now

        do {
            _ = try await database.insertQueued(id: "busy-timeout-001", type: .image, originalName: "photo.jpg")
            Issue.record("Expected write contention to fail with SQLITE_BUSY")
        } catch let error as DatabaseError {
            let elapsed = start.duration(to: clock.now)
            #expect(error.resultCode == DatabaseError.SQLITE_BUSY)
            #expect(elapsed >= .milliseconds(150))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}

private func waitForSemaphore(
    _ semaphore: DispatchSemaphore,
    timeout: DispatchTime
) -> DispatchTimeoutResult {
    semaphore.wait(timeout: timeout)
}

// MARK: - Pagination

@Suite("Database Pagination")
struct DatabasePaginationTests {

    private func timestamp(for index: Int) -> String {
        let hours = index / 3600
        let minutes = (index / 60) % 60
        let seconds = index % 60
        return String(format: "2025:02:05 %02d:%02d:%02d", hours, minutes, seconds)
    }

    private func makeDB(assetCount: Int) async throws -> TestEnv {
        let env = try TestEnv()
        let seedQueue = try DatabaseQueue(path: env.config.databasePath)
        try await seedQueue.write { db in
            let now = Date()
            for i in 0..<assetCount {
                try db.execute(
                    sql: """
                    INSERT INTO assets
                    (id, type, timestamp, originalName, status, createdAt, completedAt, retryCount, heartCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
                    """,
                    arguments: [
                        "asset-\(String(format: "%05d", i))",
                        Asset.AssetType.image.rawValue,
                        self.timestamp(for: i),
                        "photo\(i).jpg",
                        Asset.AssetStatus.complete.rawValue,
                        now,
                        now
                    ]
                )
            }
        }
        return env
    }

    @Test("Default limit returns at most 100 rows")
    func defaultLimit() async throws {
        let env = try await makeDB(assetCount: 150)
        defer { env.cleanup() }

        let results = try await env.database.getAllAssets()
        #expect(results.count == 100)
    }

    @Test("Explicit limit is respected")
    func explicitLimit() async throws {
        let env = try await makeDB(assetCount: 20)
        defer { env.cleanup() }

        let results = try await env.database.getAllAssets(limit: 5)
        #expect(results.count == 5)
    }

    @Test("Limit is capped at 500")
    func limitClamping() async throws {
        let env = try await makeDB(assetCount: 520)
        defer { env.cleanup() }

        let results = try await env.database.getAllAssets(limit: 9999)
        #expect(results.count == 500)
    }

    @Test("Offset skips rows")
    func offsetWorks() async throws {
        let env = try await makeDB(assetCount: 10)
        defer { env.cleanup() }

        let all = try await env.database.getAllAssets(limit: 10)
        let offset5 = try await env.database.getAllAssets(limit: 10, offset: 5)
        #expect(offset5.count == 5)
        #expect(offset5.first?.id == all[5].id)
    }

    @Test("Negative offset is treated as zero")
    func negativeOffset() async throws {
        let env = try await makeDB(assetCount: 5)
        defer { env.cleanup() }

        let fromZero = try await env.database.getAllAssets(offset: 0)
        let fromNegative = try await env.database.getAllAssets(offset: -5)
        #expect(fromZero.map(\.id) == fromNegative.map(\.id))
    }

    @Test("Large offset is capped at 10,000 (DoS protection)")
    func offsetClamping() async throws {
        let env = try await makeDB(assetCount: 10_005)
        defer { env.cleanup() }

        let fromMax = try await env.database.getAllAssets(offset: 10_000)
        let fromHuge = try await env.database.getAllAssets(offset: 999_999_999)
        #expect(fromMax.count == 5)
        #expect(fromMax.count == fromHuge.count)
        #expect(fromMax.map(\.id) == fromHuge.map(\.id))
    }

    @Test("Moderation query has same pagination clamping")
    func moderationPaginationClamping() async throws {
        let env = try await makeDB(assetCount: 10_005)
        defer { env.cleanup() }

        let fromHuge = try await env.database.getModerationAssets(offset: 999_999_999)
        let fromMax = try await env.database.getModerationAssets(offset: 10_000)
        #expect(fromMax.count == 5)
        #expect(fromMax.count == fromHuge.count)
        #expect(fromMax.map(\.id) == fromHuge.map(\.id))
    }

    @Test("Empty database returns empty results")
    func emptyDatabase() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let assets = try await env.database.getAllAssets()
        #expect(assets.isEmpty)

        let count = try await env.database.getAssetCount()
        #expect(count == 0)
    }
}

// MARK: - Integrity

@Suite("Database Integrity")
struct DatabaseIntegrityTests {

    @Test("integrityCheck returns true on healthy database")
    func healthyDB() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "health-001", type: .image, originalName: "photo.jpg")
        let healthy = try await env.database.integrityCheck()
        #expect(healthy)
    }

    @Test("integrityCheck detects on-disk corruption")
    func corruptedDBDetected() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-db-corrupt-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let dbPath = tempDir.appendingPathComponent("corrupt.db").path
        do {
            let db = try Database(
                path: dbPath,
                busyTimeout: 5000,
                cacheSize: -20000,
                defaultPageSize: 100,
                maxPageSize: 500,
                maxPageOffset: 10_000,
                sqlBatchSize: 500
            )
            _ = try await db.insertQueued(id: "health-corrupt-001", type: .image, originalName: "photo.jpg")
        }

        let dbURL = URL(fileURLWithPath: dbPath)
        let handle = try FileHandle(forWritingTo: dbURL)
        defer { try? handle.close() }
        try handle.seek(toOffset: 0)
        handle.write(Data(repeating: 0, count: 200))

        var detectedCorruption = false
        do {
            let reopened = try Database(
                path: dbPath,
                busyTimeout: 5000,
                cacheSize: -20000,
                defaultPageSize: 100,
                maxPageSize: 500,
                maxPageOffset: 10_000,
                sqlBatchSize: 500
            )
            let healthy = try await reopened.integrityCheck()
            detectedCorruption = !healthy
        } catch {
            detectedCorruption = true
        }
        #expect(detectedCorruption)
    }
}

// MARK: - Heart Count

@Suite("Database Heart Count")
struct DatabaseHeartCountTests {

    @Test("incrementHeartCount increments and returns new count")
    func incrementReturnsNewCount() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "heart-001", type: .image, originalName: "photo.jpg")
        try await env.database.markComplete(id: "heart-001", timestamp: "2025:02:05 12:00:00")

        let first = try await env.database.incrementHeartCount(id: "heart-001")
        #expect(first == 1)
        let second = try await env.database.incrementHeartCount(id: "heart-001")
        #expect(second == 2)
    }

    @Test("incrementHeartCount remains exact under concurrent updates")
    func incrementHeartCountConcurrentAccuracy() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "heart-concurrent-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "photo.jpg")
        try await env.database.markComplete(id: id, timestamp: "2025:02:05 12:00:00")

        let taskCount = 100
        var returnedCounts: [Int] = []
        returnedCounts.reserveCapacity(taskCount)

        try await withThrowingTaskGroup(of: Int.self) { group in
            for _ in 0..<taskCount {
                group.addTask {
                    try await env.database.incrementHeartCount(id: id)
                }
            }

            for try await count in group {
                returnedCounts.append(count)
            }
        }

        #expect(returnedCounts.count == taskCount)
        #expect(Set(returnedCounts).count == taskCount)
        #expect(returnedCounts.min() == 1)
        #expect(returnedCounts.max() == taskCount)

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.heartCount == taskCount)
    }

    @Test("incrementHeartCount on non-existent ID throws")
    func incrementNonExistentThrows() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        do {
            _ = try await env.database.incrementHeartCount(id: "no-such-asset")
            Issue.record("Expected incrementHeartCount to throw for missing asset")
        } catch let error as DatabaseWriteError {
            #expect(error == .assetNotFound("no-such-asset"))
        }
    }

    @Test("incrementHeartCount on non-complete asset throws")
    func incrementNonCompleteThrows() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "heart-queued", type: .image, originalName: "photo.jpg")

        do {
            _ = try await env.database.incrementHeartCount(id: "heart-queued")
            Issue.record("Expected incrementHeartCount to throw for queued asset")
        } catch let error as DatabaseWriteError {
            #expect(error == .assetNotFound("heart-queued"))
        }
    }

    @Test("getHeartCounts returns complete and moderated assets only")
    func getHeartCountsVisibleAndModeratedOnly() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "heart-complete", type: .image, originalName: "complete.jpg")
        try await env.database.markComplete(id: "heart-complete", timestamp: "2025:02:05 12:00:00")
        _ = try await env.database.incrementHeartCount(id: "heart-complete")
        _ = try await env.database.incrementHeartCount(id: "heart-complete")

        _ = try await env.database.insertQueued(id: "heart-moderated", type: .image, originalName: "moderated.jpg")
        try await env.database.markComplete(id: "heart-moderated", timestamp: "2025:02:05 12:00:00")
        _ = try await env.database.incrementHeartCount(id: "heart-moderated")
        try await env.database.updateStatus(id: "heart-moderated", status: .moderated)

        _ = try await env.database.insertQueued(id: "heart-queued", type: .image, originalName: "queued.jpg")

        let counts = try await env.database.getHeartCounts(
            ids: ["heart-complete", "heart-moderated", "heart-queued", "missing-heart"]
        )

        #expect(counts["heart-complete"] == 2)
        #expect(counts["heart-moderated"] == 1)
        #expect(counts["heart-queued"] == nil)
        #expect(counts["missing-heart"] == nil)
        #expect(counts.count == 2)
    }

    @Test("getHeartCounts batches across configured SQL batch size")
    func getHeartCountsBatchesAcrossConfiguredSQLBatchSize() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-heart-batch-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let database = try Database(
            path: tempDir.appendingPathComponent("test.db").path,
            busyTimeout: 5000,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 2
        )

        for id in ["heart-batch-1", "heart-batch-2", "heart-batch-3"] {
            _ = try await database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
            try await database.markComplete(id: id, timestamp: "2025:02:05 12:00:00")
            _ = try await database.incrementHeartCount(id: id)
        }

        let counts = try await database.getHeartCounts(ids: ["heart-batch-1", "heart-batch-2", "heart-batch-3"])

        #expect(counts["heart-batch-1"] == 1)
        #expect(counts["heart-batch-2"] == 1)
        #expect(counts["heart-batch-3"] == 1)
        #expect(counts.count == 3)
    }

    @Test("Gallery query with sortByHearts returns correct order")
    func gallerySortByHearts() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "popular", type: .image, originalName: "popular.jpg")
        try await env.database.markComplete(id: "popular", timestamp: "2025:02:05 10:00:00")
        _ = try await env.database.insertQueued(id: "recent", type: .image, originalName: "recent.jpg")
        try await env.database.markComplete(id: "recent", timestamp: "2025:02:05 14:00:00")

        _ = try await env.database.incrementHeartCount(id: "popular")
        _ = try await env.database.incrementHeartCount(id: "popular")
        _ = try await env.database.incrementHeartCount(id: "recent")

        let byTime = try await env.database.getGalleryAssetsAndCount(sortByHearts: false)
        #expect(byTime.assets.map(\.id) == ["recent", "popular"])

        let byHearts = try await env.database.getGalleryAssetsAndCount(sortByHearts: true)
        #expect(byHearts.assets.map(\.id) == ["popular", "recent"])
    }
}
