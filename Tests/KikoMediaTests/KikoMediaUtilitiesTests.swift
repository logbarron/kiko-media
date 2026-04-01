import Foundation
import Testing
@testable import KikoMediaApp

@Suite("KikoMedia utilities")
struct KikoMediaUtilitiesTests {
    @Test("countSSDFiles ignores partial archive artifacts")
    func countSSDFilesIgnoresPartialArtifacts() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-ssd-count-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let finalized = [
            root.appendingPathComponent("asset-a.jpg"),
            root.appendingPathComponent("asset-b.mp4"),
        ]
        for path in finalized {
            try Data("ok".utf8).write(to: path)
        }

        try Data("ignore".utf8).write(to: root.appendingPathComponent(".hidden"))
        try Data("ignore".utf8).write(to: root.appendingPathComponent("asset-c.jpg.partial-1234"))

        let nestedDir = root.appendingPathComponent("nested")
        try FileManager.default.createDirectory(at: nestedDir, withIntermediateDirectories: true)
        try Data("ignore".utf8).write(to: nestedDir.appendingPathComponent("nested.jpg"))

        let count = KikoMediaAppRuntime.countSSDFiles(path: root.path)
        #expect(count == 2)
    }
}
