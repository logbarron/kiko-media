import Testing
import Foundation
@testable import KikoMediaCore
@testable import KikoMediaApp

@Suite("Path Safety")
struct PathSafetyTests {

    // MARK: - FileServer.isPathWithinDirectory

    // FileServer requires a Database for init, but isPathWithinDirectory is pure path logic.
    // We construct a minimal FileServer with a temp DB to access the method.

    @Test("Path within directory returns true")
    func pathWithinDirectory() throws {
        try withFileServer { fs in
            #expect(fs.isPathWithinDirectory(path: "/tmp/thumbs/abc.jpg", directory: "/tmp/thumbs"))
        }
    }

    @Test("Path traversal is blocked", arguments: [
        "/tmp/thumbs/../../etc/passwd",
        "/etc/passwd",
        "/tmp/thumbs/../../../secret",
        "/tmp/thumbs/./../../etc/shadow",
    ])
    func traversalBlocked(path: String) throws {
        try withFileServer { fs in
            #expect(!fs.isPathWithinDirectory(path: path, directory: "/tmp/thumbs"))
        }
    }

    @Test("Sibling directory is blocked", arguments: [
        "/tmp/thumbs/../previews/file.jpg",
        "/tmp/thumbs/../uploads/secret",
    ])
    func siblingBlocked(path: String) throws {
        try withFileServer { fs in
            #expect(!fs.isPathWithinDirectory(path: path, directory: "/tmp/thumbs"))
        }
    }

    @Test("Directory itself (without trailing slash child) is blocked")
    func directoryItself() throws {
        try withFileServer { fs in
            // The exact directory path should NOT match (needs to be a child)
            #expect(!fs.isPathWithinDirectory(path: "/tmp/thumbs", directory: "/tmp/thumbs"))
        }
    }

    @Test("Symlink inside allowed directory that points outside is blocked")
    func symlinkEscapeBlocked() throws {
        try withFileServer { fs in
            let root = FileManager.default.temporaryDirectory
                .appendingPathComponent("kiko-symlink-\(UUID().uuidString)")
            try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: root) }

            let allowedDir = root.appendingPathComponent("allowed")
            let outsideDir = root.appendingPathComponent("outside")
            try FileManager.default.createDirectory(at: allowedDir, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: outsideDir, withIntermediateDirectories: true)

            let symlink = allowedDir.appendingPathComponent("escape")
            try FileManager.default.createSymbolicLink(
                atPath: symlink.path,
                withDestinationPath: outsideDir.path
            )

            let outsideFile = outsideDir.appendingPathComponent("secret.jpg")
            try Data("x".utf8).write(to: outsideFile)

            let escapedPath = symlink.appendingPathComponent("secret.jpg").path
            #expect(!fs.isPathWithinDirectory(path: escapedPath, directory: allowedDir.path))
        }
    }

    // MARK: - ModerationMarkers.safePath

    private var markers: ModerationMarkers {
        ModerationMarkers(baseDir: URL(fileURLWithPath: "/tmp/kiko-test-markers"))
    }

    @Test("Valid ID returns a path")
    func safePathValid() {
        #expect(markers.safePath(for: "abc123") != nil)
        #expect(markers.safePath(for: "abc-123_file") != nil)
        #expect(markers.safePath(for: "file.txt") != nil)
    }

    @Test("Empty ID returns nil")
    func safePathEmpty() {
        #expect(markers.safePath(for: "") == nil)
    }

    @Test("Slash in ID returns nil")
    func safePathSlash() {
        #expect(markers.safePath(for: "path/to/file") == nil)
        #expect(markers.safePath(for: "/absolute") == nil)
    }

    @Test("Unicode in ID returns nil")
    func safePathUnicode() {
        #expect(markers.safePath(for: "café") == nil)
        #expect(markers.safePath(for: "file🎉") == nil)
    }

    @Test("Space in ID returns nil")
    func safePathSpace() {
        #expect(markers.safePath(for: "has space") == nil)
        #expect(markers.safePath(for: " ") == nil)
    }

    @Test("Backslash in ID returns nil")
    func safePathBackslash() {
        #expect(markers.safePath(for: "path\\file") == nil)
    }

    @Test("Safe path result is within base directory")
    func safePathContainment() {
        if let path = markers.safePath(for: "test-id") {
            #expect(path.hasPrefix("/tmp/kiko-test-markers/"))
        }
    }

    // MARK: - Helpers

    private func withFileServer<T>(_ body: (FileServer) throws -> T) throws -> T {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-test-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }
        let dbPath = tempDir.appendingPathComponent("test.db").path
        let db = try Database(
            path: dbPath,
            busyTimeout: 5000,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 500
        )
        return try body(FileServer(thumbsDir: "/tmp/thumbs", previewsDir: "/tmp/previews", database: db, cacheControl: "public, max-age=31536000, immutable"))
    }
}

// MARK: - ModerationMarkers Direct Tests

@Suite("ModerationMarkers")
struct ModerationMarkersTests {

    @Test("mark then unmark roundtrip")
    func markUnmarkRoundtrip() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-markers-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let markers = ModerationMarkers(baseDir: tempDir)
        try markers.mark("test-id")
        #expect(try markers.allMarked().contains("test-id"))

        try markers.unmark("test-id")
        #expect(try !markers.allMarked().contains("test-id"))
    }

    @Test("unmark nonexistent ID does not crash")
    func unmarkNonexistent() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-markers-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let markers = ModerationMarkers(baseDir: tempDir)
        try markers.unmark("never-marked")
        #expect(try markers.allMarked().isEmpty)
    }

    @Test("mark with path traversal ID throws invalidId")
    func markInvalidId() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-markers-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let markers = ModerationMarkers(baseDir: tempDir)
        #expect(throws: ModerationMarkerError.self) { try markers.mark("../evil") }
        #expect(throws: ModerationMarkerError.self) { try markers.mark("") }
        #expect(throws: ModerationMarkerError.self) { try markers.mark("has space") }

        #expect(try markers.allMarked().isEmpty)
    }
}
