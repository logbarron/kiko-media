import Testing
import Foundation
import GRDB
@testable import KikoMediaCore

@Suite("Crash Recovery")
struct CrashRecoveryTests {

    @Test("Complete asset has its upload file cleaned up during recovery")
    func completeAssetCleanedUp() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-complete-001"
        // Simulate: asset completed, but upload file was left behind (e.g., crash before cleanup)
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
        try await env.database.markComplete(id: id, timestamp: "2025:02:05 12:00:00")

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)
        #expect(FileManager.default.fileExists(atPath: uploadPath), "Setup: upload file must exist")

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        // Upload file should be deleted (complete assets don't need the upload anymore)
        #expect(!FileManager.default.fileExists(atPath: uploadPath))
    }

    @Test("Moderated asset has its upload file cleaned up during recovery")
    func moderatedAssetCleanedUp() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-moderated-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
        try await env.database.markComplete(id: id, timestamp: "2025:02:05 12:00:00")
        try await env.database.updateStatus(id: id, status: .moderated)

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath)
        #expect(FileManager.default.fileExists(atPath: uploadPath))

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        #expect(!FileManager.default.fileExists(atPath: uploadPath))
    }

    @Test("Unknown upload file with matching .info size is inserted into database as queued")
    func unknownFileInsertedWithValidInfo() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-unknown-001"
        // File exists in uploads but NO database record
        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)
        let attrs = try FileManager.default.attributesOfItem(atPath: uploadPath)
        let size = (attrs[.size] as? NSNumber)?.int64Value ?? 0
        let infoPath = "\(env.uploadDir)/\(id).info"
        try """
        {"ID":"\(id)","Size":\(size),"SizeIsDeferred":false,"Offset":0,"MetaData":{"filename":"\(id).jpg"}}
        """.write(toFile: infoPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        // Should now exist in database
        let exists = try await env.database.assetExists(id: id)
        #expect(exists, "Unknown upload file with valid .info metadata should be inserted during recovery")
    }

    @Test("Recovery sanitizes filename from .info metadata before insert")
    func recoverySanitizesOriginalName() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-sanitize-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)
        let attrs = try FileManager.default.attributesOfItem(atPath: uploadPath)
        let size = (attrs[.size] as? NSNumber)?.int64Value ?? 0
        let infoPath = "\(env.uploadDir)/\(id).info"
        try """
        {"ID":"\(id)","Size":\(size),"SizeIsDeferred":false,"Offset":0,"MetaData":{"filename":"<img src=x onerror=alert(1)>/bad.jpg"}}
        """.write(toFile: infoPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.originalName == "_img src=x onerror=alert(1)__bad.jpg")
    }

    @Test("Unknown upload file without .info is skipped")
    func unknownFileWithoutInfoSkipped() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-unknown-noinfo-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let exists = try await env.database.assetExists(id: id)
        #expect(!exists, "Unknown upload file without .info should be skipped")
    }

    @Test("Unknown upload file with malformed .info metadata is skipped")
    func unknownFileWithMalformedInfoSkipped() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-unknown-malformed-info-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)
        let infoPath = "\(env.uploadDir)/\(id).info"
        try "{not-valid-json".write(toFile: infoPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let exists = try await env.database.assetExists(id: id)
        #expect(!exists, "Unknown upload file with malformed .info should be skipped")
        #expect(FileManager.default.fileExists(atPath: uploadPath), "Skipped malformed metadata should not delete the upload file")
    }

    @Test("Unknown upload file with unsupported media type is skipped")
    func unknownFileWithUnsupportedTypeSkipped() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-unknown-unsupported-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try "not-a-media-file".write(toFile: uploadPath, atomically: true, encoding: .utf8)
        let attrs = try FileManager.default.attributesOfItem(atPath: uploadPath)
        let size = (attrs[.size] as? NSNumber)?.int64Value ?? 0
        let infoPath = "\(env.uploadDir)/\(id).info"
        try """
        {"ID":"\(id)","Size":\(size),"SizeIsDeferred":false,"Offset":0,"MetaData":{"filename":"\(id).txt"}}
        """.write(toFile: infoPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let exists = try await env.database.assetExists(id: id)
        #expect(!exists, "Unknown upload file with unsupported type should be skipped")
    }

    @Test("Known interrupted upload with unsupported media type is marked failed")
    func knownInterruptedUnsupportedTypeMarkedFailed() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-known-unsupported-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try "not-a-media-file".write(toFile: uploadPath, atomically: true, encoding: .utf8)

        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).txt")

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .failed)
    }

    @Test("Known upload with per-row DB read failure is marked failed during recovery")
    func knownUploadDbReadFailureMarkedFailed() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-known-db-read-fail-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)

        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")

        let corruptDb = try DatabaseQueue(path: env.config.databasePath)
        try await corruptDb.write { db in
            try db.execute(
                sql: "UPDATE assets SET status = ? WHERE id = ?",
                arguments: ["corrupt-status", id]
            )
        }

        let knownIds = try await env.database.getExistingIds(from: [id])
        #expect(knownIds.contains(id), "Setup: existing-ID scan must still report the upload as known")

        do {
            _ = try await env.database.getAsset(id: id)
            Issue.record("Expected getAsset(id:) to throw for the corrupted row")
        } catch {
            // Expected.
        }

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .failed)
        #expect(
            FileManager.default.fileExists(atPath: uploadPath),
            "Fail-closed recovery should retain the upload for manual review"
        )
    }

    @Test("Unknown upload file with .info missing Size is skipped")
    func unknownFileWithInfoMissingSizeSkipped() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-unknown-missing-size-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)
        let infoPath = "\(env.uploadDir)/\(id).info"
        try """
        {"ID":"\(id)","SizeIsDeferred":false,"Offset":0,"MetaData":{"filename":"\(id).jpg"}}
        """.write(toFile: infoPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let exists = try await env.database.assetExists(id: id)
        #expect(!exists, "Unknown upload file with .info missing Size should be skipped")
        #expect(FileManager.default.fileExists(atPath: uploadPath))
    }

    @Test("Unknown upload file with mismatched .info size is skipped")
    func unknownFileWithMismatchedSizeSkipped() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-unknown-size-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)
        let attrs = try FileManager.default.attributesOfItem(atPath: uploadPath)
        let size = (attrs[.size] as? NSNumber)?.int64Value ?? 0
        let infoPath = "\(env.uploadDir)/\(id).info"
        try """
        {"ID":"\(id)","Size":\(size + 1),"SizeIsDeferred":false,"Offset":0,"MetaData":{"filename":"\(id).jpg"}}
        """.write(toFile: infoPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let exists = try await env.database.assetExists(id: id)
        #expect(!exists, "Unknown upload file with mismatched size should be skipped")
    }

    @Test("Failed asset upload file is left alone during recovery")
    func failedAssetSkipped() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-failed-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
        try await env.database.updateStatus(id: id, status: .processing)
        try await env.database.updateStatus(id: id, status: .failed)

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        // Failed assets should be left for manual review
        #expect(FileManager.default.fileExists(atPath: uploadPath), "Failed asset upload file should not be deleted")
    }

    @Test("Interrupted (queued/processing) asset upload file is preserved for re-processing")
    func interruptedAssetPreserved() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-queued-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 100, height: 100)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        // File should NOT be deleted — it's needed for re-processing
        #expect(FileManager.default.fileExists(atPath: uploadPath))
    }

    @Test("Queued asset missing upload payload is marked failed during recovery orphan sweep")
    func queuedAssetWithoutUploadPayloadMarkedFailed() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-orphan-queued-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
        #expect(!FileManager.default.fileExists(atPath: "\(env.uploadDir)/\(id)"))

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-orphan-queued"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .failed)
    }

    @Test("Recovery still runs orphan sweep when uploads directory listing fails")
    func orphanSweepStillRunsWhenUploadListingFails() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-orphan-listing-fail-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")

        let missingUploadDir = env.tempDir.appendingPathComponent("missing-uploads-\(UUID().uuidString)").path
        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            uploadDir: missingUploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            moderatedDir: env.config.moderatedDir,
            externalSSDPath: env.config.externalSSDPath,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-orphan-listing"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .failed)
    }

    @Test("Recovery with empty uploads directory completes without error")
    func emptyUploadsDir() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        // Should not crash or throw
        await processor.recoverIncomplete()
    }

    @Test("Recovery ignores .info metadata files")
    func ignoresInfoFiles() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        // Create a .info file (tusd metadata) — should be ignored by recovery
        let infoPath = "\(env.uploadDir)/someid.info"
        try "metadata".write(toFile: infoPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        // .info file should not be treated as an upload
        let exists = try await env.database.assetExists(id: "someid.info")
        #expect(!exists, ".info file should not be inserted into database")
    }

    @Test("Recovery ignores hidden files")
    func ignoresHiddenFiles() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let hiddenPath = "\(env.uploadDir)/.DS_Store"
        try "junk".write(toFile: hiddenPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.recoverIncomplete()

        let exists = try await env.database.assetExists(id: ".DS_Store")
        #expect(!exists, "Hidden files should not be inserted into database")
    }

    @Test("Artifact verification repairs corrupt JPEG-derived files from SSD original")
    func verifyRepairsCorruptDerivedImageFiles() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ssdDir = env.tempDir.appendingPathComponent("ssd")
        try FileManager.default.createDirectory(at: ssdDir, withIntermediateDirectories: true)

        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            uploadDir: env.config.uploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            moderatedDir: env.config.moderatedDir,
            externalSSDPath: ssdDir.path,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil,
            maxConcurrentImages: 0
        )

        let id = "recovery-corrupt-derived-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
        try await env.database.markComplete(id: id, timestamp: "2025:02:05 12:00:00")

        let thumbPath = "\(env.thumbsDir)/\(id).jpg"
        let previewPath = "\(env.previewsDir)/\(id).jpg"
        try Data("corrupt-thumb".utf8).write(to: URL(fileURLWithPath: thumbPath))
        try Data("corrupt-preview".utf8).write(to: URL(fileURLWithPath: previewPath))

        #expect(TestImage.dimensions(at: thumbPath) == nil, "Setup: thumb must be corrupt")
        #expect(TestImage.dimensions(at: previewPath) == nil, "Setup: preview must be corrupt")

        let originalPath = ssdDir.appendingPathComponent("\(id).jpg").path
        try TestImage.writeJPEG(to: originalPath, width: 1200, height: 900)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)
        await processor.verifyDerivedArtifacts()

        let queuedForRepair = try await env.database.getAsset(id: id)
        #expect(queuedForRepair?.status == .processing, "verifyDerivedArtifacts should mark broken derivatives for repair")

        await processor.process(job: ProcessingJob(
            uploadId: id,
            originalName: "\(id).jpg",
            filePath: originalPath,
            assetType: .image,
            isRepair: true,
            restoreStatus: .complete
        ))

        #expect(TestImage.dimensions(at: thumbPath) != nil, "Repair should regenerate valid thumbnail from SSD original")
        #expect(TestImage.dimensions(at: previewPath) != nil, "Repair should regenerate valid preview from SSD original")
        let repaired = try await env.database.getAsset(id: id)
        #expect(repaired?.status == .complete, "Repair should restore complete status")
    }

    @Test("Artifact verification rejects invalid video preview files during startup verification")
    func verifyRejectsInvalidDerivedVideoPreview() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ssdDir = env.tempDir.appendingPathComponent("ssd")
        try FileManager.default.createDirectory(at: ssdDir, withIntermediateDirectories: true)

        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            uploadDir: env.config.uploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            moderatedDir: env.config.moderatedDir,
            externalSSDPath: ssdDir.path,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil,
            maxConcurrentVideos: 0
        )

        let id = "recovery-invalid-video-preview-001"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
        try await env.database.markComplete(id: id, timestamp: "2025:02:05 12:00:00")

        let thumbPath = "\(env.thumbsDir)/\(id).jpg"
        let previewPath = "\(env.previewsDir)/\(id).mp4"
        try TestImage.writeJPEG(to: thumbPath, width: 512, height: 512)
        try Data("corrupt-video-preview".utf8).write(to: URL(fileURLWithPath: previewPath))

        #expect(TestImage.dimensions(at: thumbPath) != nil, "Setup: thumb must be valid")
        #expect(FileManager.default.fileExists(atPath: previewPath), "Setup: preview file must exist")
        #expect(!(await VideoProcessor.isVideo(sourcePath: previewPath)), "Setup: preview must be an invalid video")

        let originalPath = ssdDir.appendingPathComponent("\(id).mov").path
        try Data("ssd-original-video".utf8).write(to: URL(fileURLWithPath: originalPath))

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: markers)
        await processor.verifyDerivedArtifacts()

        let queuedForRepair = try await env.database.getAsset(id: id)
        #expect(
            queuedForRepair?.status == .processing,
            "verifyDerivedArtifacts should not keep a bad video preview marked as healthy"
        )
    }

    @Test("Artifact verification reconciles complete -> moderated when marker exists")
    func verifyReconcilesModerationFromMarkers() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-reconcile-mod-001"
        try await env.insertCompleteImageAsset(id: id)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        try markers.mark(id)

        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.verifyDerivedArtifacts()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .moderated)
    }

    @Test("Artifact verification reconciles moderated -> complete when marker is absent")
    func verifyReconcilesUnmoderationFromMarkers() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-reconcile-unmod-001"
        try await env.insertCompleteImageAsset(id: id)
        try await env.database.updateStatus(id: id, status: .moderated)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.verifyDerivedArtifacts()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .complete)
    }

    @Test("Artifact verification fails closed when moderation markers are unreadable")
    func verifyModerationFailClosedWhenMarkersUnreadable() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "recovery-reconcile-fail-closed-001"
        try await env.insertCompleteImageAsset(id: id)
        try await env.database.updateStatus(id: id, status: .moderated)

        let markerPath = env.tempDir.appendingPathComponent("broken-moderated")
        try "not-a-directory".write(to: markerPath, atomically: true, encoding: .utf8)

        let markers = ModerationMarkers(baseDir: markerPath)
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        await processor.verifyDerivedArtifacts()

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .moderated)
    }

    @Test("CA recovery startup backlog: recoverIncomplete feeds multiple complexity-aware picks in one pass")
    func caRecoveryStartupBacklogFeedsMultiplePicksInOnePass() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ids = ["recovery-ca-backlog-a", "recovery-ca-backlog-b", "recovery-ca-backlog-c"]
        for id in ids {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            let uploadPath = "\(env.uploadDir)/\(id)"
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: uploadPath))
        }

        let config = makeCARecoveryConfig(env: env, maxConcurrentVideos: 2)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)

        let launchGate = BlockingGate()
        let routeRecorder = CARecoveryRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-recovery-backlog"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId)
                await launchGate.wait()
                return (thumb: true, preview: true, timestamp: "2026:03:09 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        for id in ids {
            await processor.enqueueJob(
                ProcessingJob(
                    uploadId: id,
                    originalName: "\(id).mov",
                    filePath: "\(env.uploadDir)/\(id)",
                    assetType: .video,
                    estimatedVideoRuntimeSeconds: 60,
                    frameCount: 500,
                    probedDurationSeconds: 60,
                    videoEstimateConfidence: .high
                )
            )
        }

        await processor.recoverIncomplete()

        let multiLaunched = try await waitUntil(timeoutSeconds: 4) {
            await routeRecorder.count() >= 2
        }
        #expect(multiLaunched, "Recovery with all slots idle and N>1 pending should pick multiple jobs in one processQueues pass")

        await launchGate.open()

        let completed = try await waitUntil(timeoutSeconds: 6) {
            for id in ids {
                let asset = try await env.database.getAsset(id: id)
                if asset?.status != .complete { return false }
            }
            return true
        }
        #expect(completed, "All recovered video jobs should complete")

        let recoveredIDs = await routeRecorder.allIDs()
        for id in ids {
            #expect(recoveredIDs.contains(id), "Job \(id) should have been processed after recovery")
        }

        await processor.shutdown()
    }

    @Test("Processing retains upload and enforces bounded completion retry when completion DB update fails")
    func processRetainsUploadWithBoundedCompletionRetryOnDbFailure() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "ds006-mark-complete-fail-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 1200, height: 900)

        let archiveDir = "/tmp/kiko-ds006-\(UUID().uuidString)"
        try FileManager.default.createDirectory(atPath: archiveDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: archiveDir) }

        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            bindAddress: env.config.bindAddress,
            uploadDir: env.config.uploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            moderatedDir: env.config.moderatedDir,
            externalSSDPath: archiveDir,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )

        let triggerDb = try DatabaseQueue(path: env.config.databasePath)
        try await triggerDb.write { db in
            try db.execute(
                sql: """
                CREATE TRIGGER fail_mark_complete_ds006
                BEFORE UPDATE OF status ON assets
                FOR EACH ROW
                WHEN NEW.id = '\(id)' AND NEW.status = 'complete'
                BEGIN
                    SELECT RAISE(ABORT, 'forced markComplete failure');
                END;
                """
            )
        }

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let testArchiveOriginal: @Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult = {
            sourcePath, assetId, originalName in
            let ext = (originalName as NSString).pathExtension.lowercased()
            let filename = ext.isEmpty ? assetId : "\(assetId).\(ext)"
            let externalPath = "\(archiveDir)/\(filename)"
            do {
                try FileManager.default.createDirectory(atPath: archiveDir, withIntermediateDirectories: true)
                try? FileManager.default.removeItem(atPath: externalPath)
                try FileManager.default.copyItem(atPath: sourcePath, toPath: externalPath)
                return .success(externalPath: externalPath, checksum: "test-checksum")
            } catch {
                return .failed("Test archive failed: \(error.localizedDescription)")
            }
        }
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: testArchiveOriginal
        )
        await processor.process(job: ProcessingJob(
            uploadId: id,
            originalName: "\(id).jpg",
            filePath: uploadPath,
            assetType: .image
        ))

        let thumbPath = "\(env.thumbsDir)/\(id).jpg"
        let previewPath = "\(env.previewsDir)/\(id).jpg"
        let archivedPath = "\(archiveDir)/\(id).jpg"

        #expect(FileManager.default.fileExists(atPath: thumbPath), "Derived thumbnail should be generated before completion write")
        #expect(FileManager.default.fileExists(atPath: previewPath), "Derived preview should be generated before completion write")
        #expect(FileManager.default.fileExists(atPath: archivedPath), "Archive copy should succeed before completion write")
        #expect(FileManager.default.fileExists(atPath: uploadPath), "Upload must be preserved when completion DB write fails")
        var asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .queued)
        #expect(asset?.retryCount == 1)
        #expect(asset?.timestamp == "")

        await processor.process(job: ProcessingJob(
            uploadId: id,
            originalName: "\(id).jpg",
            filePath: uploadPath,
            assetType: .image
        ))

        asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .failed)
        #expect(asset?.retryCount == 2)
        #expect(
            FileManager.default.fileExists(atPath: uploadPath),
            "Upload must be retained for manual review once completion retry budget is exhausted"
        )
    }
}

private func makeCARecoveryConfig(env: TestEnv, maxConcurrentVideos: Int) -> Config {
    Config(
        publicPort: env.config.publicPort,
        internalPort: env.config.internalPort,
        bindAddress: env.config.bindAddress,
        uploadDir: env.config.uploadDir,
        thumbsDir: env.config.thumbsDir,
        previewsDir: env.config.previewsDir,
        logsDir: env.config.logsDir,
        moderatedDir: env.config.moderatedDir,
        externalSSDPath: env.config.externalSSDPath,
        databasePath: env.config.databasePath,
        turnstileSecret: env.config.turnstileSecret,
        sessionHmacSecret: env.config.sessionHmacSecret,
        maxConcurrentVideos: maxConcurrentVideos,
        videoTranscodePreset: env.config.videoTranscodePreset,
        tbWorkers: "127.0.0.1:1"
    )
}

private actor CARecoveryRouteRecorder {
    private var launchedIDs: [String] = []

    func record(_ uploadId: String) {
        launchedIDs.append(uploadId)
    }

    func count() -> Int {
        launchedIDs.count
    }

    func allIDs() -> [String] {
        launchedIDs
    }
}

private actor BlockingGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        if isOpen { return }
        await withCheckedContinuation { continuation in
            if isOpen {
                continuation.resume()
                return
            }
            waiters.append(continuation)
        }
    }

    func open() {
        guard !isOpen else { return }
        isOpen = true
        let pending = waiters
        waiters.removeAll(keepingCapacity: false)
        for waiter in pending {
            waiter.resume()
        }
    }
}

private func waitUntil(
    timeoutSeconds: TimeInterval,
    pollEveryMillis: UInt64 = 25,
    condition: @escaping @Sendable () async throws -> Bool
) async throws -> Bool {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        if try await condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(Int(pollEveryMillis)))
    }
    return try await condition()
}
