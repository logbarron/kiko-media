import Foundation
import OSLog

enum ModerationMarkerError: Error {
    case invalidId(String)
    case writeFailed(String)
}

package struct ModerationMarkers: Sendable {
    package let baseDir: URL

    package init(baseDir: URL) {
        self.baseDir = baseDir
    }

    package func mark(_ id: String) throws {
        guard let path = safePath(for: id) else {
            Logger.kiko.warning("ModerationMarkers: rejected invalid id for mark: \(id)")
            throw ModerationMarkerError.invalidId(id)
        }

        try ensureDirectory()

        let fm = FileManager.default
        if fm.fileExists(atPath: path) {
            return
        }

        if !fm.createFile(atPath: path, contents: nil) {
            Logger.kiko.warning("ModerationMarkers: failed to create marker for \(id)")
            throw ModerationMarkerError.writeFailed(id)
        }
    }

    package func unmark(_ id: String) throws {
        guard let path = safePath(for: id) else {
            Logger.kiko.warning("ModerationMarkers: rejected invalid id for unmark: \(id)")
            throw ModerationMarkerError.invalidId(id)
        }

        do {
            try FileManager.default.removeItem(atPath: path)
        } catch CocoaError.fileNoSuchFile {
            // Idempotent
        }
    }

    package func allMarked() throws -> Set<String> {
        do {
            try ensureDirectory()
            let files = try FileManager.default.contentsOfDirectory(atPath: baseDir.path)
            return Set(files.filter { !$0.hasPrefix(".") })
        } catch {
            Logger.kiko.warning("ModerationMarkers: failed to list markers: \(error)")
            throw error
        }
    }

    package func pruneUntracked(keeping keepIds: Set<String>) throws -> (removed: Int, failed: Int) {
        do {
            try ensureDirectory()
            let entries = try FileManager.default.contentsOfDirectory(atPath: baseDir.path)
            var removed = 0
            var failed = 0

            for entry in entries where !entry.hasPrefix(".") && !keepIds.contains(entry) {
                guard let path = containedPath(forEntry: entry) else {
                    Logger.kiko.warning("ModerationMarkers: failed to prune unsafe marker entry \(entry)")
                    failed += 1
                    continue
                }
                do {
                    try FileManager.default.removeItem(atPath: path)
                    removed += 1
                } catch CocoaError.fileNoSuchFile {
                    continue
                } catch {
                    Logger.kiko.warning("ModerationMarkers: failed to prune marker \(entry): \(error)")
                    failed += 1
                }
            }

            return (removed: removed, failed: failed)
        } catch {
            Logger.kiko.warning("ModerationMarkers: failed to prune markers: \(error)")
            throw error
        }
    }

    package func safePath(for id: String) -> String? {
        guard !id.isEmpty,
              id.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "-" || $0 == "_" || $0 == ".") }) else {
            return nil
        }
        return containedPath(forEntry: id)
    }

    private func ensureDirectory() throws {
        let fm = FileManager.default
        if !fm.fileExists(atPath: baseDir.path) {
            try fm.createDirectory(at: baseDir, withIntermediateDirectories: true)
        }
    }

    private func containedPath(forEntry entry: String) -> String? {
        guard !entry.isEmpty else {
            return nil
        }

        let candidatePath = baseDir.appendingPathComponent(entry).path
        let resolvedPath = URL(fileURLWithPath: candidatePath).standardized.path
        let resolvedDir = baseDir.standardized.path

        guard resolvedPath.hasPrefix(resolvedDir + "/") else {
            return nil
        }

        return resolvedPath
    }
}
