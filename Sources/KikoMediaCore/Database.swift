import Foundation
import GRDB

package struct Asset: Codable, Sendable {
    package var id: String
    package var type: AssetType
    package var timestamp: String
    package var originalName: String
    package var status: AssetStatus
    package var createdAt: Date
    package var completedAt: Date?
    package var retryCount: Int
    package var heartCount: Int

    package enum AssetType: String, Codable, Sendable {
        case image
        case video
    }

    package enum AssetStatus: String, Codable, Sendable {
        case queued
        case processing
        case complete
        case moderated
        case failed
    }

    package static func isValidId(_ id: String) -> Bool {
        !id.isEmpty
            && id.utf8.count <= 128
            && !id.hasPrefix(".")
            && !id.contains("/")
            && !id.contains("\\")
            && !id.contains("..")
            && !id.contains("\0")
            && !id.unicodeScalars.contains(where: {
                CharacterSet.whitespacesAndNewlines.contains($0)
                    || CharacterSet.controlCharacters.contains($0)
            })
    }

    package static func sanitizedOriginalName(_ rawName: String?) -> String {
        let maxNameLength = 255
        let fallbackName = "unknown"

        guard let rawName else { return fallbackName }

        var sanitized = ""
        sanitized.reserveCapacity(min(rawName.count, maxNameLength))

        for scalar in rawName.unicodeScalars.prefix(maxNameLength) {
            switch scalar.value {
            case 0x2F, 0x5C, 0x3C, 0x3E, 0x26, 0x22, 0x27, 0x60:
                sanitized.append("_")
            default:
                if CharacterSet.controlCharacters.contains(scalar) {
                    sanitized.append(" ")
                } else {
                    sanitized.unicodeScalars.append(scalar)
                }
            }
        }

        let trimmed = sanitized.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? fallbackName : trimmed
    }
}

extension Asset: FetchableRecord, PersistableRecord {
    package static var databaseTableName: String { "assets" }

    package enum Columns: String, ColumnExpression {
        case id, type, timestamp, originalName, status, createdAt, completedAt, retryCount, heartCount
    }
}

enum DatabaseInitError: Error, Equatable {
    case invalidSQLBatchSize(Int)
}

package enum DatabaseWriteError: Error, Equatable {
    case assetNotFound(String)
}

package final class Database: Sendable {
    package struct ModerationState: Sendable {
        package let id: String
        package let status: Asset.AssetStatus
    }

    private let dbQueue: DatabaseQueue
    package let defaultLimit: Int
    package let maxLimit: Int
    package let maxOffset: Int
    package let batchSize: Int

    package init(
        path: String,
        busyTimeout: Int,
        cacheSize: Int,
        defaultPageSize: Int,
        maxPageSize: Int,
        maxPageOffset: Int,
        sqlBatchSize: Int
    ) throws {
        guard sqlBatchSize > 0 else {
            throw DatabaseInitError.invalidSQLBatchSize(sqlBatchSize)
        }

        self.defaultLimit = defaultPageSize
        self.maxLimit = maxPageSize
        self.maxOffset = maxPageOffset
        self.batchSize = sqlBatchSize

        let directory = (path as NSString).deletingLastPathComponent
        try FileManager.default.createDirectory(
            atPath: directory,
            withIntermediateDirectories: true
        )

        var config = Configuration()
        config.busyMode = .timeout(TimeInterval(busyTimeout) / 1000.0)
        // SQLite PRAGMA assignments do not accept bound parameters.
        let cacheSizeLiteral = Self.sqliteIntegerLiteral(cacheSize)
        config.prepareDatabase { db in
            try db.execute(sql: "PRAGMA journal_mode = WAL")
            try db.execute(sql: "PRAGMA synchronous = NORMAL")
            try db.execute(sql: "PRAGMA foreign_keys = ON")
            try db.execute(sql: "PRAGMA cache_size = " + cacheSizeLiteral)
        }

        dbQueue = try DatabaseQueue(path: path, configuration: config)
        try migrate()
    }

    @inline(__always)
    package static func sqliteIntegerLiteral(_ value: Int) -> String {
        String(value)
    }

    private func migrate() throws {
        var migrator = DatabaseMigrator()

        migrator.registerMigration("v1_create_assets") { db in
            try db.create(table: "assets", ifNotExists: true) { t in
                t.column("id", .text).primaryKey()
                t.column("type", .text).notNull()
                t.column("timestamp", .text).notNull()
                t.column("originalName", .text).notNull()
                t.column("status", .text).notNull().defaults(to: "queued")
                t.column("createdAt", .datetime).notNull()
            }

            try db.create(
                index: "idx_assets_status_timestamp",
                on: "assets",
                columns: ["status", "timestamp"],
                ifNotExists: true
            )
        }

        migrator.registerMigration("v2_walltime_columns") { db in
            try db.alter(table: "assets") { t in
                t.add(column: "completedAt", .datetime)
                t.add(column: "retryCount", .integer).notNull().defaults(to: 0)
            }
        }

        migrator.registerMigration("v3_heart_count") { db in
            try db.alter(table: "assets") { t in
                t.add(column: "heartCount", .integer).notNull().defaults(to: 0)
            }
        }

        try migrator.migrate(dbQueue)
    }

    package func assetExists(id: String) async throws -> Bool {
        try await dbQueue.read { db in
            try Asset.filter(Asset.Columns.id == id).fetchCount(db) > 0
        }
    }

    package func getAsset(id: String) async throws -> Asset? {
        try await dbQueue.read { db in
            try Asset.filter(Asset.Columns.id == id).fetchOne(db)
        }
    }

    private func normalizedPagination(limit: Int?, offset: Int?) -> (limit: Int, offset: Int) {
        let effectiveLimit = max(min(limit ?? self.defaultLimit, self.maxLimit), 1)
        let effectiveOffset = min(max(offset ?? 0, 0), self.maxOffset)
        return (limit: effectiveLimit, offset: effectiveOffset)
    }

    private func fetchModerationAssets(_ db: GRDB.Database, limit: Int, offset: Int) throws -> [Asset] {
        try Asset.fetchAll(
            db,
            sql: """
            SELECT * FROM assets
            WHERE status IN (?, ?)
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            """,
            arguments: [
                Asset.AssetStatus.complete.rawValue,
                Asset.AssetStatus.moderated.rawValue,
                limit,
                offset
            ]
        )
    }

    private func fetchModerationAssetCount(_ db: GRDB.Database) throws -> Int {
        try Int.fetchOne(
            db,
            sql: "SELECT COUNT(*) FROM assets WHERE status IN (?, ?)",
            arguments: [
                Asset.AssetStatus.complete.rawValue,
                Asset.AssetStatus.moderated.rawValue
            ]
        ) ?? 0
    }

    package func getAllAssets(limit: Int? = nil, offset: Int? = nil) async throws -> [Asset] {
        try await dbQueue.read { db in
            let paging = self.normalizedPagination(limit: limit, offset: offset)

            return try Asset
                .filter(Asset.Columns.status == Asset.AssetStatus.complete.rawValue)
                .order(Asset.Columns.timestamp.desc)
                .limit(paging.limit, offset: paging.offset)
                .fetchAll(db)
        }
    }

    package func getGalleryAssetsAndCount(limit: Int? = nil, offset: Int? = nil, sortByHearts: Bool = false) async throws -> (assets: [Asset], total: Int) {
        try await dbQueue.read { db in
            let paging = self.normalizedPagination(limit: limit, offset: offset)

            let base = Asset
                .filter(Asset.Columns.status == Asset.AssetStatus.complete.rawValue)
            let ordered = sortByHearts
                ? base.order(Asset.Columns.heartCount.desc, Asset.Columns.timestamp.desc)
                : base.order(Asset.Columns.timestamp.desc)
            let assets = try ordered
                .limit(paging.limit, offset: paging.offset)
                .fetchAll(db)
            let total = try base.fetchCount(db)
            return (assets: assets, total: total)
        }
    }

    package func getAssetCount() async throws -> Int {
        try await dbQueue.read { db in
            try Asset
                .filter(Asset.Columns.status == Asset.AssetStatus.complete.rawValue)
                .fetchCount(db)
        }
    }

    package func getModerationAssets(limit: Int? = nil, offset: Int? = nil) async throws -> [Asset] {
        try await dbQueue.read { db in
            let paging = self.normalizedPagination(limit: limit, offset: offset)
            return try self.fetchModerationAssets(db, limit: paging.limit, offset: paging.offset)
        }
    }

    package func getModerationAssetCount() async throws -> Int {
        try await dbQueue.read { db in
            try self.fetchModerationAssetCount(db)
        }
    }

    package func getModerationAssetsAndCount(limit: Int? = nil, offset: Int? = nil) async throws -> (assets: [Asset], total: Int) {
        try await dbQueue.read { db in
            let paging = self.normalizedPagination(limit: limit, offset: offset)
            let assets = try self.fetchModerationAssets(db, limit: paging.limit, offset: paging.offset)
            let total = try self.fetchModerationAssetCount(db)
            return (assets: assets, total: total)
        }
    }

    package func getTerminalModerationStates() async throws -> [ModerationState] {
        try await dbQueue.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT id, status FROM assets
                WHERE status IN (?, ?)
                """,
                arguments: [
                    Asset.AssetStatus.complete.rawValue,
                    Asset.AssetStatus.moderated.rawValue
                ]
            )
            return rows.compactMap { row in
                let id: String = row["id"]
                let rawStatus: String = row["status"]
                guard let status = Asset.AssetStatus(rawValue: rawStatus) else {
                    return nil
                }
                return ModerationState(id: id, status: status)
            }
        }
    }

    /// Returns complete, moderated, and processing (stranded) assets for artifact verification
    package func getVerifiableAssets() async throws -> [Asset] {
        try await dbQueue.read { db in
            try Asset.fetchAll(
                db,
                sql: "SELECT * FROM assets WHERE status IN (?, ?, ?)",
                arguments: [
                    Asset.AssetStatus.complete.rawValue,
                    Asset.AssetStatus.moderated.rawValue,
                    Asset.AssetStatus.processing.rawValue
                ]
            )
        }
    }

    package func getUnfinishedAssets() async throws -> [Asset] {
        try await dbQueue.read { db in
            try Asset.fetchAll(
                db,
                sql: "SELECT * FROM assets WHERE status IN (?, ?)",
                arguments: [
                    Asset.AssetStatus.queued.rawValue,
                    Asset.AssetStatus.processing.rawValue
                ]
            )
        }
    }

    package func getExistingIds(from candidates: [String]) async throws -> Set<String> {
        guard !candidates.isEmpty else { return [] }

        return try await dbQueue.read { db in
            var result = Set<String>()

            for batch in chunked(candidates, into: self.batchSize) {
                let placeholders = batch.map { _ in "?" }.joined(separator: ",")
                let sql = "SELECT id FROM assets WHERE id IN (\(placeholders))"
                let ids = try String.fetchAll(db, sql: sql, arguments: StatementArguments(batch))
                result.formUnion(ids)
            }

            return result
        }
    }

    package func insertQueued(id: String, type: Asset.AssetType, originalName: String) async throws -> Bool {
        let asset = Asset(
            id: id,
            type: type,
            timestamp: "",
            originalName: originalName,
            status: .queued,
            createdAt: Date(),
            completedAt: nil,
            retryCount: 0,
            heartCount: 0
        )
        return try await dbQueue.write { db in
            try asset.insert(db, onConflict: .ignore)
            return db.changesCount > 0
        }
    }

    package func deleteQueued(id: String) async throws -> Bool {
        try await dbQueue.write { db in
            try db.execute(
                sql: "DELETE FROM assets WHERE id = ? AND status = ?",
                arguments: [id, Asset.AssetStatus.queued.rawValue]
            )
            return db.changesCount > 0
        }
    }

    package func updateStatus(id: String, status: Asset.AssetStatus) async throws {
        let statusRaw = status.rawValue
        let completeRaw = Asset.AssetStatus.complete.rawValue
        let moderatedRaw = Asset.AssetStatus.moderated.rawValue
        let now = Date()
        try await dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE assets
                SET status = ?,
                    completedAt = CASE
                        WHEN ? IN (?, ?) THEN COALESCE(completedAt, ?)
                        ELSE completedAt
                    END
                WHERE id = ?
                """,
                arguments: [statusRaw, statusRaw, completeRaw, moderatedRaw, now, id]
            )
            guard db.changesCount > 0 else {
                throw DatabaseWriteError.assetNotFound(id)
            }
        }
    }

    package func updateType(id: String, type: Asset.AssetType) async throws {
        try await dbQueue.write { db in
            try db.execute(
                sql: "UPDATE assets SET type = ? WHERE id = ?",
                arguments: [type.rawValue, id]
            )
            guard db.changesCount > 0 else {
                throw DatabaseWriteError.assetNotFound(id)
            }
        }
    }

    package func markComplete(id: String, timestamp: String) async throws {
        try await dbQueue.write { db in
            try db.execute(
                sql: "UPDATE assets SET status = ?, timestamp = ?, completedAt = ? WHERE id = ?",
                arguments: [Asset.AssetStatus.complete.rawValue, timestamp, Date(), id]
            )
            guard db.changesCount > 0 else {
                throw DatabaseWriteError.assetNotFound(id)
            }
        }
    }

    package func reEnqueueForRetry(id: String) async throws -> Int {
        try await dbQueue.write { db in
            try db.execute(
                sql: "UPDATE assets SET status = ?, retryCount = retryCount + 1 WHERE id = ?",
                arguments: [Asset.AssetStatus.queued.rawValue, id]
            )
            guard db.changesCount > 0 else {
                throw DatabaseWriteError.assetNotFound(id)
            }
            guard let retryCount = try Int.fetchOne(
                db,
                sql: "SELECT retryCount FROM assets WHERE id = ?",
                arguments: [id]
            ) else {
                throw DatabaseWriteError.assetNotFound(id)
            }
            return retryCount
        }
    }

    package func incrementRetryCount(id: String, by amount: Int = 1) async throws -> Int {
        let safeAmount = max(0, amount)
        guard safeAmount > 0 else {
            return try await dbQueue.read { db in
                guard let retryCount = try Int.fetchOne(
                    db,
                    sql: "SELECT retryCount FROM assets WHERE id = ?",
                    arguments: [id]
                ) else {
                    throw DatabaseWriteError.assetNotFound(id)
                }
                return retryCount
            }
        }

        return try await dbQueue.write { db in
            try db.execute(
                sql: "UPDATE assets SET retryCount = retryCount + ? WHERE id = ?",
                arguments: [safeAmount, id]
            )
            guard db.changesCount > 0 else {
                throw DatabaseWriteError.assetNotFound(id)
            }
            guard let retryCount = try Int.fetchOne(
                db,
                sql: "SELECT retryCount FROM assets WHERE id = ?",
                arguments: [id]
            ) else {
                throw DatabaseWriteError.assetNotFound(id)
            }
            return retryCount
        }
    }

    package func getTotalAssetCount() async throws -> Int {
        try await dbQueue.read { db in
            try Asset.fetchCount(db)
        }
    }

    package func getHeartCounts(ids: [String]) async throws -> [String: Int] {
        guard !ids.isEmpty else { return [:] }

        return try await dbQueue.read { db in
            var result: [String: Int] = [:]
            result.reserveCapacity(ids.count)

            for batch in chunked(ids, into: self.batchSize) {
                let placeholders = batch.map { _ in "?" }.joined(separator: ",")
                let sql = """
                SELECT id, heartCount FROM assets
                WHERE id IN (\(placeholders))
                  AND status IN (?, ?)
                """
                let arguments = StatementArguments(
                    batch + [
                        Asset.AssetStatus.complete.rawValue,
                        Asset.AssetStatus.moderated.rawValue,
                    ]
                )
                let rows = try Row.fetchAll(db, sql: sql, arguments: arguments)
                for row in rows {
                    let id: String = row["id"]
                    let heartCount: Int = row["heartCount"]
                    result[id] = heartCount
                }
            }

            return result
        }
    }

    package func getAllAssetIds() async throws -> Set<String> {
        try await dbQueue.read { db in
            Set(try String.fetchAll(db, sql: "SELECT id FROM assets"))
        }
    }

    package func insertComplete(
        id: String,
        type: Asset.AssetType,
        timestamp: String,
        originalName: String,
        status: Asset.AssetStatus
    ) async throws -> Bool {
        let asset = Asset(
            id: id,
            type: type,
            timestamp: timestamp,
            originalName: originalName,
            status: status,
            createdAt: Date(),
            completedAt: status == .complete || status == .moderated ? Date() : nil,
            retryCount: 0,
            heartCount: 0
        )
        return try await dbQueue.write { db in
            try asset.insert(db, onConflict: .ignore)
            return db.changesCount > 0
        }
    }

    package struct DashboardCounts: Sendable {
        package let queuedImages: Int
        package let queuedVideos: Int
        package let processingImages: Int
        package let processingVideos: Int
        package let completeImages: Int
        package let completeVideos: Int
        package let moderated: Int
        package let failed: Int
    }

    package func getDashboardCountsSync() throws -> DashboardCounts {
        try dbQueue.read { db in
            try Self.fetchDashboardCounts(db)
        }
    }

    package func getDashboardCounts() async throws -> DashboardCounts {
        try await dbQueue.read { db in
            try Self.fetchDashboardCounts(db)
        }
    }

    private static func fetchDashboardCounts(_ db: GRDB.Database) throws -> DashboardCounts {
        var qi = 0, qv = 0, pi = 0, pv = 0, ci = 0, cv = 0, mod = 0, fail = 0
        let rows = try Row.fetchAll(db, sql: """
            SELECT status, type, COUNT(*) AS cnt FROM assets GROUP BY status, type
            """)
        for row in rows {
            let status: String = row["status"]
            let type: String? = row["type"]
            let cnt: Int = row["cnt"]
            switch (status, type) {
            case ("queued", "image"):    qi = cnt
            case ("queued", "video"):    qv = cnt
            case ("processing", "image"): pi = cnt
            case ("processing", "video"): pv = cnt
            case ("complete", "image"):  ci = cnt
            case ("complete", "video"):  cv = cnt
            case ("moderated", _):       mod += cnt
            case ("failed", _):          fail += cnt
            default: break
            }
        }
        return DashboardCounts(
            queuedImages: qi, queuedVideos: qv,
            processingImages: pi, processingVideos: pv,
            completeImages: ci, completeVideos: cv,
            moderated: mod, failed: fail
        )
    }

    package func incrementHeartCount(id: String) async throws -> Int {
        try await dbQueue.write { db in
            try db.execute(
                sql: "UPDATE assets SET heartCount = heartCount + 1 WHERE id = ? AND status = ?",
                arguments: [id, Asset.AssetStatus.complete.rawValue]
            )
            guard db.changesCount > 0 else {
                throw DatabaseWriteError.assetNotFound(id)
            }
            guard let count = try Int.fetchOne(
                db,
                sql: "SELECT heartCount FROM assets WHERE id = ?",
                arguments: [id]
            ) else {
                throw DatabaseWriteError.assetNotFound(id)
            }
            return count
        }
    }

    package func integrityCheck() async throws -> Bool {
        try await dbQueue.read { db in
            let result = try String.fetchOne(db, sql: "PRAGMA quick_check")
            return result == "ok"
        }
    }
}

private func chunked<Element>(_ values: [Element], into size: Int) -> [[Element]] {
    guard size > 0 else {
        return values.isEmpty ? [] : [values]
    }

    return stride(from: 0, to: values.count, by: size).map {
        Array(values[$0..<Swift.min($0 + size, values.count)])
    }
}
