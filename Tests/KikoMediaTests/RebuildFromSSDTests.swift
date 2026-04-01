import Testing
import Foundation
@testable import KikoMediaCore

@Suite("SSD Rebuild Filtering")
struct RebuildFromSSDTests {

    private func makeTempDirectory() throws -> URL {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-rebuild-test-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    @Test("rebuild candidate rejects invalid ID")
    func rejectsInvalidId() async throws {
        let dir = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }

        let filename = "bad..id.jpg"
        let path = dir.appendingPathComponent(filename).path
        try TestImage.writeJPEG(to: path, width: 100, height: 100)

        let candidate = await MediaProcessor.rebuildCandidate(fullPath: path)
        #expect(candidate == nil)
    }

    @Test("rebuild candidate rejects unsupported extension")
    func rejectsUnsupportedExtension() async throws {
        let dir = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }

        let filename = "rebuild-valid-001.txt"
        let path = dir.appendingPathComponent(filename).path
        try Data("not media".utf8).write(to: URL(fileURLWithPath: path))

        let candidate = await MediaProcessor.rebuildCandidate(fullPath: path)
        #expect(candidate == nil)
    }

    @Test("rebuild candidate rejects fake video file")
    func rejectsFakeVideoFile() async throws {
        let dir = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }

        let filename = "rebuild-valid-002.mp4"
        let path = dir.appendingPathComponent(filename).path
        try Data("definitely not a real mp4".utf8).write(to: URL(fileURLWithPath: path))

        let candidate = await MediaProcessor.rebuildCandidate(fullPath: path)
        #expect(candidate == nil)
    }

    @Test("rebuild candidate accepts valid image file")
    func acceptsValidImage() async throws {
        let dir = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }

        let filename = "rebuild-valid-003.jpg"
        let path = dir.appendingPathComponent(filename).path
        try TestImage.writeJPEG(to: path, width: 100, height: 100)

        let candidate = await MediaProcessor.rebuildCandidate(fullPath: path)
        #expect(candidate != nil)
        #expect(candidate?.id == "rebuild-valid-003")
        #expect(candidate?.originalName == filename)
        #expect(candidate?.assetType == .image)
    }

    @Test("rebuildFromSSD keeps in-flight probes bounded by maxConcurrentRebuildProbes")
    func rebuildFromSSDKeepsInflightBounded() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let fm = FileManager.default
        let ssdPath = env.tempDir.appendingPathComponent("ssd-rebuild-bounded").path
        try fm.createDirectory(atPath: ssdPath, withIntermediateDirectories: true)

        let fixtureCount = 8
        for index in 0..<fixtureCount {
            let id = String(format: "rebuild-bounded-%03d", index + 1)
            let imagePath = "\(ssdPath)/\(id).jpg"
            try TestImage.writeJPEG(to: imagePath, width: 640, height: 480)
        }

        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: env.uploadDir,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            logsDir: env.tempDir.appendingPathComponent("logs").path,
            externalSSDPath: ssdPath,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil,
            maxConcurrentRebuildProbes: 2
        )

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-rebuild-bounded"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)
        let recorder = RebuildProbeRecorder()

        await MediaProcessor.withRebuildMountCheckOverride({ _ in true }) {
            await MediaProcessor.withRebuildProbeConcurrencyObserver({ event in
            recorder.record(event)
            }) {
                await processor.rebuildFromSSD()
            }
        }

        let maxInFlight = recorder.maxInFlight()
        #expect(maxInFlight == 2, "Expected runtime probe concurrency to reach configured cap")
        #expect(maxInFlight <= 2, "Runtime probe concurrency must never exceed configured cap")
        #expect(recorder.count(phase: .scheduled) >= fixtureCount)
        #expect(recorder.count(phase: .completed) >= fixtureCount)

        let total = try await env.database.getTotalAssetCount()
        #expect(total == fixtureCount)
    }

    @Test("rebuildFromSSD skips IDs already present in database")
    func rebuildFromSSDSkipsExistingIds() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let fm = FileManager.default
        let ssdPath = env.tempDir.appendingPathComponent("ssd-rebuild-dedup").path
        try fm.createDirectory(atPath: ssdPath, withIntermediateDirectories: true)

        let id = "rebuild-dedup-001"
        _ = try await env.database.insertComplete(
            id: id,
            type: .image,
            timestamp: "2025:02:10 08:00:00",
            originalName: "existing-\(id).jpg",
            status: .complete
        )

        try TestImage.writeJPEG(to: "\(ssdPath)/\(id).jpg", width: 800, height: 600)

        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: env.uploadDir,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            logsDir: env.tempDir.appendingPathComponent("logs").path,
            externalSSDPath: ssdPath,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-rebuild-dedup"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)

        await MediaProcessor.withRebuildMountCheckOverride({ _ in true }) {
            await processor.rebuildFromSSD()
        }

        let total = try await env.database.getTotalAssetCount()
        #expect(total == 1)

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .complete)
        #expect(asset?.timestamp == "2025:02:10 08:00:00")
        #expect(asset?.originalName == "existing-\(id).jpg")
    }

    @Test("rebuildFromSSD reconciles DB moderation state and prunes stale markers")
    func rebuildFromSSDReconcilesModerationAndPrunesStaleMarkers() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let fm = FileManager.default
        let ssdPath = env.tempDir.appendingPathComponent("ssd-rebuild-reconcile").path
        try fm.createDirectory(atPath: ssdPath, withIntermediateDirectories: true)

        let rebuiltModeratedId = "rebuild-reconcile-mod-001"
        try TestImage.writeJPEG(to: "\(ssdPath)/\(rebuiltModeratedId).jpg", width: 900, height: 700)

        let existingUnmoderatedId = "rebuild-reconcile-unmod-001"
        _ = try await env.database.insertComplete(
            id: existingUnmoderatedId,
            type: .image,
            timestamp: "2025:02:10 09:00:00",
            originalName: "\(existingUnmoderatedId).jpg",
            status: .moderated
        )

        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: env.uploadDir,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            logsDir: env.tempDir.appendingPathComponent("logs").path,
            externalSSDPath: ssdPath,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-rebuild-reconcile"))
        let staleMarkerId = "rebuild-reconcile-stale-001"
        try markers.mark(rebuiltModeratedId)
        try markers.mark(staleMarkerId)

        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)

        await MediaProcessor.withRebuildMountCheckOverride({ _ in true }) {
            await processor.rebuildFromSSD()
        }

        let rebuiltModeratedAsset = try await env.database.getAsset(id: rebuiltModeratedId)
        #expect(rebuiltModeratedAsset?.status == .moderated)

        let existingUnmoderatedAsset = try await env.database.getAsset(id: existingUnmoderatedId)
        #expect(existingUnmoderatedAsset?.status == .complete)

        let markerIds = try markers.allMarked()
        #expect(markerIds.contains(rebuiltModeratedId))
        #expect(!markerIds.contains(existingUnmoderatedId))
        #expect(!markerIds.contains(staleMarkerId))
    }

    @Test("rebuildFromSSD retains markers when rebuild inserts fail")
    func rebuildFromSSDRetainsMarkersWhenRebuildInsertsFail() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let fm = FileManager.default
        let ssdPath = env.tempDir.appendingPathComponent("ssd-rebuild-insert-fail").path
        try fm.createDirectory(atPath: ssdPath, withIntermediateDirectories: true)

        let failingModeratedId = "rebuild-insert-fail-mod-001"
        try TestImage.writeJPEG(to: "\(ssdPath)/\(failingModeratedId).jpg", width: 900, height: 700)

        let successfulModeratedId = "rebuild-insert-fail-mod-002"
        try TestImage.writeJPEG(to: "\(ssdPath)/\(successfulModeratedId).jpg", width: 900, height: 700)

        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: env.uploadDir,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            logsDir: env.tempDir.appendingPathComponent("logs").path,
            externalSSDPath: ssdPath,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-rebuild-insert-fail"))
        let staleMarkerId = "rebuild-insert-fail-stale-001"
        try markers.mark(failingModeratedId)
        try markers.mark(successfulModeratedId)
        try markers.mark(staleMarkerId)

        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)

        await MediaProcessor.withRebuildMountCheckOverride({ _ in true }) {
            await MediaProcessor.withRebuildInsertFailureOverride({ id in id == failingModeratedId }) {
                await processor.rebuildFromSSD()
            }
        }

        let failedAsset = try await env.database.getAsset(id: failingModeratedId)
        #expect(failedAsset == nil)

        let successfulAsset = try await env.database.getAsset(id: successfulModeratedId)
        #expect(successfulAsset?.status == .moderated)

        let markerIds = try markers.allMarked()
        #expect(markerIds.contains(failingModeratedId))
        #expect(markerIds.contains(successfulModeratedId))
        #expect(markerIds.contains(staleMarkerId))
    }

    @Test("rebuildFromSSD fails closed when moderation markers are unreadable")
    func rebuildFromSSDFailsClosedWhenMarkersUnreadable() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let fm = FileManager.default
        let ssdPath = env.tempDir.appendingPathComponent("ssd-rebuild-marker-fail").path
        try fm.createDirectory(atPath: ssdPath, withIntermediateDirectories: true)

        let id = "rebuild-marker-fail-001"
        try TestImage.writeJPEG(to: "\(ssdPath)/\(id).jpg", width: 400, height: 300)

        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: env.uploadDir,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            logsDir: env.tempDir.appendingPathComponent("logs").path,
            externalSSDPath: ssdPath,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )

        let brokenMarkersPath = env.tempDir.appendingPathComponent("moderated-rebuild-broken")
        try Data("not-a-directory".utf8).write(to: brokenMarkersPath)

        let markers = ModerationMarkers(baseDir: brokenMarkersPath)
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)

        await MediaProcessor.withRebuildMountCheckOverride({ _ in true }) {
            await processor.rebuildFromSSD()
        }

        let total = try await env.database.getTotalAssetCount()
        #expect(total == 0)
    }
}

private final class RebuildProbeRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var events: [MediaProcessor.RebuildProbeConcurrencyEvent] = []

    func record(_ event: MediaProcessor.RebuildProbeConcurrencyEvent) {
        lock.lock()
        events.append(event)
        lock.unlock()
    }

    func maxInFlight() -> Int {
        lock.lock()
        let maxValue = events.map(\.inFlight).max() ?? 0
        lock.unlock()
        return maxValue
    }

    func count(phase: MediaProcessor.RebuildProbeConcurrencyEvent.Phase) -> Int {
        lock.lock()
        let count = events.filter { $0.phase == phase }.count
        lock.unlock()
        return count
    }
}
