import Darwin
import Foundation
import KikoMediaCore
import OSLog

extension KikoMediaAppRuntime {
    static func initializeDatabase(config: Config) async throws -> (Database, Bool) {
        var needsRebuild = false
        let fm = FileManager.default

        func openDB() throws -> Database {
            try Database(
                path: config.databasePath,
                busyTimeout: config.sqliteBusyTimeout,
                cacheSize: config.sqliteCacheSize,
                defaultPageSize: config.defaultPageSize,
                maxPageSize: config.maxPageSize,
                maxPageOffset: config.maxPageOffset,
                sqlBatchSize: config.sqlBatchSize
            )
        }

        func recoverFreshDatabase(createFailureMessage: String) -> Database {
            guard moveAsideCorruptDB(config: config) else {
                Logger.kiko.error("Cannot proceed: failed to move aside corrupt database")
                exit(1)
            }
            do {
                needsRebuild = true
                return try openDB()
            } catch {
                Logger.kiko.error("\(createFailureMessage): \(error)")
                exit(1)
            }
        }

        let dbExists = fm.fileExists(atPath: config.databasePath)
        if !dbExists {
            Logger.kiko.warning("Database file missing, will rebuild from SSD")
            needsRebuild = true
        }

        var database: Database
        do {
            database = try openDB()
        } catch {
            Logger.kiko.error("Failed to open database: \(error)")
            database = recoverFreshDatabase(createFailureMessage: "Failed to create fresh database")
        }

        if dbExists && !needsRebuild {
            do {
                let isHealthy = try await database.integrityCheck()
                if !isHealthy {
                    Logger.kiko.error("Database integrity check failed")
                    database = recoverFreshDatabase(
                        createFailureMessage: "Failed to create fresh database after integrity failure"
                    )
                }
            } catch {
                Logger.kiko.error("Database integrity check threw: \(error)")
                database = recoverFreshDatabase(
                    createFailureMessage: "Failed to create fresh database after integrity failure"
                )
            }
        }

        let ssdMounted = VolumeUtils.isMounted(volumeContainingPath: config.externalSSDPath)
        if ssdMounted {
            let ssdCount = countSSDFiles(path: config.externalSSDPath)
            let dbCount = (try? await database.getTotalAssetCount()) ?? 0

            if ssdCount > dbCount {
                Logger.kiko.warning("SSD has \(ssdCount) files but DB has \(dbCount) records, will rebuild")
                needsRebuild = true
            }
        } else if needsRebuild {
            Logger.kiko.error("SSD not mounted and rebuild required, cannot proceed")
            exit(1)
        }

        return (database, needsRebuild)
    }

    static func ensureDirectories(config: Config) throws {
        let fm = FileManager.default
        let dirs = [config.uploadDir, config.thumbsDir, config.previewsDir, config.logsDir, config.moderatedDir]
        for dir in dirs {
            if !fm.fileExists(atPath: dir) {
                try fm.createDirectory(atPath: dir, withIntermediateDirectories: true)
            }
        }
    }

    static func moveAsideCorruptDB(config: Config) -> Bool {
        let fm = FileManager.default
        let timestamp = ISO8601DateFormatter().string(from: Date())
            .replacingOccurrences(of: ":", with: "-")

        let basePath = config.databasePath

        if fm.fileExists(atPath: basePath) {
            let destPath = "\(basePath).corrupt-\(timestamp)"
            do {
                try fm.moveItem(atPath: basePath, toPath: destPath)
                Logger.kiko.info("Moved corrupt DB file to \(destPath)")
            } catch {
                Logger.kiko.error("CRITICAL: Failed to move aside corrupt DB \(basePath): \(error)")
                return false
            }
        }

        for ext in ["-wal", "-shm"] {
            let sourcePath = basePath + ext
            if fm.fileExists(atPath: sourcePath) {
                let destPath = "\(basePath).corrupt-\(timestamp)\(ext)"
                do {
                    try fm.moveItem(atPath: sourcePath, toPath: destPath)
                    Logger.kiko.info("Moved corrupt DB file to \(destPath)")
                } catch {
                    Logger.kiko.warning("Failed to move \(sourcePath), deleting: \(error)")
                    do {
                        try fm.removeItem(atPath: sourcePath)
                    } catch {
                        Logger.kiko.error("CRITICAL: Cannot remove stale \(ext) file: \(error)")
                        return false
                    }
                }
            }
        }

        return true
    }

    static func countSSDFiles(path: String) -> Int {
        let fm = FileManager.default
        guard let fileEnumerator = fm.enumerator(
            at: URL(fileURLWithPath: path, isDirectory: true),
            includingPropertiesForKeys: nil,
            options: [.skipsSubdirectoryDescendants],
            errorHandler: { url, error in
                Logger.kiko.warning("SSD count: skipping unreadable path \(url.path): \(error.localizedDescription)")
                return true
            }
        ) else {
            return 0
        }

        var count = 0
        while let fileURL = fileEnumerator.nextObject() as? URL {
            let filename = fileURL.lastPathComponent
            guard !filename.hasPrefix("."),
                  !filename.contains(".partial-") else { continue }
            let fullPath = fileURL.path
            var isDirectory: ObjCBool = false
            if fm.fileExists(atPath: fullPath, isDirectory: &isDirectory),
               !isDirectory.boolValue {
                count += 1
            }
        }
        return count
    }
}
