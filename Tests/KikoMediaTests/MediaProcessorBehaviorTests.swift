import Testing
import Foundation
import GRDB
import CryptoKit
import Darwin
@testable import KikoMediaCore

@Suite("Media Processor Behavior", Testing.ParallelizationTrait.serialized)
struct MediaProcessorBehaviorTests {

    @Test("routing directive consumption is single-use")
    func routingDirectiveConsumptionIsSingleUse() {
        var dispatchState = CADispatchState()
        let uploadID = "mp-behavior-routing-directive-single-use"
        dispatchState.registerActiveVideo(
            job: ProcessingJob(
                uploadId: uploadID,
                originalName: "\(uploadID).mov",
                filePath: "/tmp/\(uploadID)",
                assetType: .video
            ),
            routing: .remote(
                workerIndex: 2,
                slotIndex: 1,
                slotID: "worker-2#s2"
            )
        )

        let first = dispatchState.consumeRoutingDirective(for: uploadID)
        guard case .remote(let workerIndex, let slotIndex, let slotID) = first else {
            Issue.record("First routing handoff should return the selected remote directive")
            return
        }
        #expect(workerIndex == 2)
        #expect(slotIndex == 1)
        #expect(slotID == "worker-2#s2")

        let second = dispatchState.consumeRoutingDirective(for: uploadID)
        guard case .local(let localSlotIndex) = second else {
            Issue.record("Second routing handoff should fall back to the default local directive")
            return
        }
        #expect(localSlotIndex == 0)
    }

    @Test("enqueue uses caller-provided asset type")
    func enqueueUsesCallerProvidedAssetType() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mp-behavior-enqueue-type-001"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).txt")

        let uploadPath = "\(env.uploadDir)/\(id)"
        try Data("plain text, not an image".utf8).write(to: URL(fileURLWithPath: uploadPath))

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-enqueue-type"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)

        let accepted = await processor.enqueue(
            uploadId: id,
            originalName: "\(id).txt",
            filePath: uploadPath,
            assetType: .image
        )
        #expect(accepted)

        let reachedFailed = try await waitUntil(timeoutSeconds: 3) {
            let asset = try await env.database.getAsset(id: id)
            return asset?.status == .failed
        }
        #expect(reachedFailed, "Expected quick image-path failure for non-image upload")

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.type == .image, "enqueue should not re-detect and override caller-provided type")
    }

    @Test("shutdown cancels and drains in-flight work")
    func shutdownCancelsAndDrainsInFlightWork() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mp-behavior-shutdown-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 1600, height: 1200)

        let archiveProbe = ArchiveProbe()
        let archiveOriginal: @Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult = { _, _, _ in
            await archiveProbe.markStarted()
            try? await Task.sleep(for: .milliseconds(600))
            await archiveProbe.recordCancellationState(Task.isCancelled)
            return .success(externalPath: "/tmp/\(UUID().uuidString)", checksum: "test-checksum")
        }

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-shutdown"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveOriginal
        )

        let accepted = await processor.enqueue(
            uploadId: id,
            originalName: "\(id).jpg",
            filePath: uploadPath,
            assetType: .image
        )
        #expect(accepted)

        let archiveStarted = try await waitUntil(timeoutSeconds: 3) {
            await archiveProbe.hasStarted()
        }
        #expect(archiveStarted, "Setup: archive stage should start before shutdown")

        await processor.shutdown()
        #expect(
            await archiveProbe.sawCancellation() == false,
            "Archive offload should run detached from parent cancellation"
        )

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .processing, "Canceled in-flight job should not mark complete")

        let thumbPath = "\(env.thumbsDir)/\(id).jpg"
        let previewPath = "\(env.previewsDir)/\(id).jpg"
        #expect(!FileManager.default.fileExists(atPath: thumbPath))
        #expect(!FileManager.default.fileExists(atPath: previewPath))
    }

    @Test("held video job is not launched before its wake time")
    func heldVideoJobIsNotLaunchedBeforeItsWakeTime() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mp-behavior-held-early-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: uploadPath))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-held-early"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: id,
                originalName: "\(id).mov",
                filePath: uploadPath,
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 120,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        #expect(await processor.videoJobState(uploadId: id) == .queued)

        let hold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(4.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: id, hold: hold))
        #expect(await processor.videoJobState(uploadId: id) == .held(hold))

        await processor.processQueues()
        try? await Task.sleep(for: .milliseconds(250))

        #expect(await routeRecorder.route(uploadId: id) == nil)
        #expect(await processor.videoJobState(uploadId: id) == .held(hold))

        await processor.shutdown()
    }

    @Test("held video job wakes and is reconsidered")
    func heldVideoJobWakesAndIsReconsidered() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mp-behavior-held-wake-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: uploadPath))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let launchGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-held-wake"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                await launchGate.wait()
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: id,
                originalName: "\(id).mov",
                filePath: uploadPath,
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 120,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        let hold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(5.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: id, hold: hold))
        await processor.processQueues()

        #expect(await processor.videoJobState(uploadId: id) == .held(hold))

        let launchedAfterWake = try await waitUntil(timeoutSeconds: 8) {
            await routeRecorder.route(uploadId: id) == .local
        }
        #expect(launchedAfterWake)
        let launchedAt = try #require(await routeRecorder.recordedAt(uploadId: id))
        #expect(launchedAt >= hold.wakeAt)
        #expect(await processor.videoJobState(uploadId: id) == .active)

        await launchGate.open()
        let completed = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: id)
            return asset?.status == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("arrival recompute keeps a stable held video job before wake")
    func arrivalRecomputeKeepsStableHeldVideoJobBeforeWake() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-held-arrival-first"
        let heldID = "mp-behavior-held-arrival-held"
        let arrivalID = "mp-behavior-held-arrival-new"
        let firstPath = "\(env.uploadDir)/\(firstID)"
        let heldPath = "\(env.uploadDir)/\(heldID)"
        let arrivalPath = "\(env.uploadDir)/\(arrivalID)"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: heldID, type: .video, originalName: "\(heldID).mov")
        _ = try await env.database.insertQueued(id: arrivalID, type: .video, originalName: "\(arrivalID).mov")
        try Data(repeating: 0x60, count: 512).write(to: URL(fileURLWithPath: firstPath))
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: heldPath))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: arrivalPath))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-held-arrival"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    try? await Task.sleep(for: .seconds(10))
                }
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: firstPath,
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 20_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStarted)
        #expect(await processor.videoJobState(uploadId: firstID) == .active)
        let firstStartedAt = try #require(await routeRecorder.recordedAt(uploadId: firstID))

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: heldID,
                originalName: "\(heldID).mov",
                filePath: heldPath,
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 120,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        let hold = MediaProcessor.VideoHoldMetadata(
            wakeAt: firstStartedAt.addingTimeInterval(20.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: heldID, hold: hold))
        #expect(await processor.videoJobState(uploadId: heldID) == .held(hold))

        #expect(await processor.enqueue(
            uploadId: arrivalID,
            originalName: "\(arrivalID).mov",
            filePath: arrivalPath,
            assetType: .video
        ))

        try? await Task.sleep(for: .milliseconds(300))

        #expect(
            await routeRecorder.route(uploadId: heldID) == nil,
            "Irrelevant arrival recomputes should not dump a stable hold back into the queue"
        )
        #expect(await processor.videoJobState(uploadId: heldID) == .held(hold))
        #expect(await processor.videoJobState(uploadId: arrivalID) == .queued)

        await processor.shutdown()
    }

    @Test("completion refill reconsiders a held video job before wake")
    func completionRefillReconsidersHeldVideoJobBeforeWake() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-held-finish-first"
        let heldID = "mp-behavior-held-finish-held"
        let firstPath = "\(env.uploadDir)/\(firstID)"
        let heldPath = "\(env.uploadDir)/\(heldID)"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: heldID, type: .video, originalName: "\(heldID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: firstPath))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: heldPath))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let firstGate = BlockingGate()
        let heldGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-held-finish"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                }
                if uploadId == heldID {
                    await heldGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: firstPath,
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 120,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStarted)
        #expect(await processor.videoJobState(uploadId: firstID) == .active)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: heldID,
                originalName: "\(heldID).mov",
                filePath: heldPath,
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 120,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        let hold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(5.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: heldID, hold: hold))
        #expect(await processor.videoJobState(uploadId: heldID) == .held(hold))

        await firstGate.open()

        let heldStartedAfterFinish = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: heldID) == .local
        }
        #expect(heldStartedAfterFinish)
        let heldStartedAt = try #require(await routeRecorder.recordedAt(uploadId: heldID))
        #expect(heldStartedAt < hold.wakeAt)
        #expect(await processor.videoJobState(uploadId: heldID) == .active)

        await heldGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let first = try await env.database.getAsset(id: firstID)?.status
            let held = try await env.database.getAsset(id: heldID)?.status
            return first == .complete && held == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("recompute can replace or cancel a stale hold")
    func recomputeCanReplaceOrCancelAStaleHold() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mp-behavior-held-recompute-001"
        let uploadPath = "\(env.uploadDir)/\(id)"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: uploadPath))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let launchGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-held-recompute"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                await launchGate.wait()
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: id,
                originalName: "\(id).mov",
                filePath: uploadPath,
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 120,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        let initialHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(5.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: id, hold: initialHold))
        await processor.processQueues()

        let replacementHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(15.0),
            targetSlotID: "local#s2"
        )
        #expect(await processor.setVideoHold(uploadId: id, hold: replacementHold))
        #expect(await processor.videoJobState(uploadId: id) == .held(replacementHold))

        try? await Task.sleep(for: .seconds(6))

        #expect(
            await routeRecorder.route(uploadId: id) == nil,
            "Replacing the hold should cancel the stale wake deadline"
        )
        #expect(await processor.videoJobState(uploadId: id) == .held(replacementHold))

        let recomputeRequestedAt = Date()
        await processor.requestRecomputeFromDispatcher(trigger: .slotDownBatch)

        let launchedAfterRecompute = try await waitUntil(timeoutSeconds: 5) {
            await routeRecorder.route(uploadId: id) == .local
        }
        #expect(launchedAfterRecompute)
        let launchedAt = try #require(await routeRecorder.recordedAt(uploadId: id))
        #expect(launchedAt >= recomputeRequestedAt)
        #expect(await processor.videoJobState(uploadId: id) == .active)

        await launchGate.open()
        let completed = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: id)
            return asset?.status == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("production recompute only invalidates impacted holds")
    func productionRecomputeOnlyInvalidatesImpactedHolds() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-held-targeted-first"
        let impactedID = "mp-behavior-held-targeted-impacted"
        let stableID = "mp-behavior-held-targeted-stable"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: impactedID, type: .video, originalName: "\(impactedID).mov")
        _ = try await env.database.insertQueued(id: stableID, type: .video, originalName: "\(stableID).mov")
        try Data(repeating: 0x60, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(impactedID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(stableID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let remoteSignature = "mp-behavior-held-targeted-remote-signature"
        let priorTable = makeRemoteComplexityAwarePriorTable(
            config: config,
            localMSPerFrameC1: 1.0,
            remoteSignature: remoteSignature,
            remoteMSPerFrameC1: 5.0
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for targeted hold invalidation")

        await dispatcher.testHookSeedRunningEstimate(workerIndex: 0, slotIndex: 0, estRemainingMS: 12_000)
        let seededSnapshot = await dispatcher.complexityAwareSnapshot()
        let seededWorker = try #require(seededSnapshot.first)
        let impactedRemainingMS = try #require(seededWorker.slots[0].estimatedRemainingMS)

        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-held-targeted"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    try? await Task.sleep(for: .seconds(30))
                } else {
                    try? await Task.sleep(for: .milliseconds(50))
                }
                return (thumb: true, preview: true, timestamp: "2026:03:25 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 30_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStarted)
        let firstStartedAt = try #require(await routeRecorder.recordedAt(uploadId: firstID))

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: impactedID,
                originalName: "\(impactedID).mov",
                filePath: "\(env.uploadDir)/\(impactedID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 400,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: stableID,
                originalName: "\(stableID).mov",
                filePath: "\(env.uploadDir)/\(stableID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 400,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        let impactedHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(impactedRemainingMS / 1_000.0),
            targetSlotID: "127.0.0.1#s1"
        )
        let stableHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: firstStartedAt.addingTimeInterval(30.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: impactedID, hold: impactedHold))
        #expect(await processor.setVideoHold(uploadId: stableID, hold: stableHold))
        #expect(await processor.videoJobState(uploadId: impactedID) == .held(impactedHold))
        #expect(await processor.videoJobState(uploadId: stableID) == .held(stableHold))

        await processor.recordTransientRemoteExclusion(
            uploadId: impactedID,
            workerIndex: 0,
            slotIndex: 0
        )
        await processor.requestRecomputeFromDispatcher(trigger: .fail)

        let impactedStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: impactedID) == .local
        }
        #expect(impactedStartedLocal)
        let stableState = await processor.videoJobState(uploadId: stableID)
        #expect(await routeRecorder.route(uploadId: impactedID) == .local)
        #expect(await routeRecorder.route(uploadId: stableID) == nil)
        if case .held(let hold)? = stableState {
            #expect(hold.targetSlotID == stableHold.targetSlotID)
        } else {
            Issue.record("Stable hold should stay held while only the impacted hold is reconsidered")
        }
        #expect(await routeRecorder.route(uploadId: stableID) == nil)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("completion refill bypasses slot-down batching and keeps an unrelated stable hold")
    func completionRefillBypassesSlotDownBatchingAndKeepsUnrelatedStableHold() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-completion-refill-first"
        let secondID = "mp-behavior-completion-refill-second"
        let heldID = "mp-behavior-completion-refill-held"
        let refillID = "mp-behavior-completion-refill-queued"
        for id in [firstID, secondID, heldID, refillID] {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
        let secondGate = BlockingGate()
        let refillGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-completion-refill"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    try? await Task.sleep(for: .seconds(2))
                }
                if uploadId == secondID {
                    await secondGate.wait()
                }
                if uploadId == refillID {
                    await refillGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:25 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 2_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_500,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let initialLaunchesStarted = try await waitUntil(timeoutSeconds: 2) {
            let firstRoute = await routeRecorder.route(uploadId: firstID)
            let secondRoute = await routeRecorder.route(uploadId: secondID)
            return firstRoute == .local && secondRoute == .local
        }
        #expect(initialLaunchesStarted)
        let firstStartedAt = try #require(await routeRecorder.recordedAt(uploadId: firstID))

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: heldID,
                originalName: "\(heldID).mov",
                filePath: "\(env.uploadDir)/\(heldID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        let stableHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: firstStartedAt.addingTimeInterval(2.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: heldID, hold: stableHold))
        #expect(await processor.videoJobState(uploadId: heldID) == .held(stableHold))

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: refillID,
                originalName: "\(refillID).mov",
                filePath: "\(env.uploadDir)/\(refillID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 900,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        await processor.requestRecomputeFromDispatcher(trigger: .slotDownBatch)
        let completionTriggeredAt = Date()
        await secondGate.open()

        let refillStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: refillID) == .local
        }
        #expect(refillStarted)
        let refillStartedAt = try #require(await routeRecorder.recordedAt(uploadId: refillID))
        #expect(refillStartedAt >= completionTriggeredAt)
        #expect(refillStartedAt < stableHold.wakeAt)

        let stableState = await processor.videoJobState(uploadId: heldID)
        if case .held(let hold)? = stableState {
            #expect(hold == stableHold)
        } else {
            Issue.record("Stable hold should remain held while completion refill consumes the newly free slot")
        }
        #expect(await routeRecorder.route(uploadId: heldID) == nil)

        await refillGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            let refillStatus = try await env.database.getAsset(id: refillID)?.status
            return firstStatus == .complete && secondStatus == .complete && refillStatus == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("completion refill reconsiders a stable hold when it is the only work left for a newly free slot")
    func completionRefillReconsidersStableHoldWhenOnlyHeldWorkRemains() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-completion-idle-first"
        let secondID = "mp-behavior-completion-idle-second"
        let heldID = "mp-behavior-completion-idle-held"
        for id in [firstID, secondID, heldID] {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
        let secondGate = BlockingGate()
        let heldGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-completion-idle"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    try? await Task.sleep(for: .seconds(2))
                }
                if uploadId == secondID {
                    await secondGate.wait()
                }
                if uploadId == heldID {
                    await heldGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:25 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 2_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_500,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let initialLaunchesStarted = try await waitUntil(timeoutSeconds: 2) {
            let firstRoute = await routeRecorder.route(uploadId: firstID)
            let secondRoute = await routeRecorder.route(uploadId: secondID)
            return firstRoute == .local && secondRoute == .local
        }
        #expect(initialLaunchesStarted)
        let firstStartedAt = try #require(await routeRecorder.recordedAt(uploadId: firstID))

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: heldID,
                originalName: "\(heldID).mov",
                filePath: "\(env.uploadDir)/\(heldID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        let stableHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: firstStartedAt.addingTimeInterval(2.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: heldID, hold: stableHold))
        #expect(await processor.videoJobState(uploadId: heldID) == .held(stableHold))
        #expect(await routeRecorder.route(uploadId: heldID) == nil)

        let completionTriggeredAt = Date()
        await secondGate.open()

        let heldStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: heldID) == .local
        }
        #expect(heldStarted)
        let heldStartedAt = try #require(await routeRecorder.recordedAt(uploadId: heldID))
        #expect(heldStartedAt >= completionTriggeredAt)
        #expect(heldStartedAt < stableHold.wakeAt)
        #expect(await processor.videoJobState(uploadId: heldID) == .active)

        await heldGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            let heldStatus = try await env.database.getAsset(id: heldID)?.status
            return firstStatus == .complete && secondStatus == .complete && heldStatus == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("coalesced completion refill reconsiders held-only idle capacity in an in-flight generic run")
    func coalescedCompletionRefillReconsidersHeldOnlyIdleCapacityInAnInFlightGenericRun() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-coalesced-finish-first"
        let heldID = "mp-behavior-coalesced-finish-held"
        for id in [firstID, heldID] {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
        let firstGate = BlockingGate()
        let heldGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-coalesced-finish"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                }
                if uploadId == heldID {
                    await heldGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:25 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 2_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStarted)
        let firstStartedAt = try #require(await routeRecorder.recordedAt(uploadId: firstID))

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: heldID,
                originalName: "\(heldID).mov",
                filePath: "\(env.uploadDir)/\(heldID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        let stableHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: firstStartedAt.addingTimeInterval(2.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: heldID, hold: stableHold))
        #expect(await processor.videoJobState(uploadId: heldID) == .held(stableHold))

        let finishTriggerTask = Task {
            await Task.yield()
            await processor.requestRecomputeFromDispatcher(trigger: .finish)
        }
        await processor.processQueues()
        await finishTriggerTask.value

        let heldStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: heldID) == .local
        }
        #expect(heldStarted)
        let heldStartedAt = try #require(await routeRecorder.recordedAt(uploadId: heldID))
        #expect(heldStartedAt < stableHold.wakeAt)
        #expect(await processor.videoJobState(uploadId: heldID) == .active)

        await heldGate.open()
        await firstGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let heldStatus = try await env.database.getAsset(id: heldID)?.status
            return firstStatus == .complete && heldStatus == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("image completion does not take the completion refill path")
    func imageCompletionDoesNotTakeTheCompletionRefillPath() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstImageID = "mp-behavior-image-finish-first"
        let secondImageID = "mp-behavior-image-finish-second"
        let heldVideoID = "mp-behavior-image-finish-held"
        _ = try await env.database.insertQueued(id: firstImageID, type: .image, originalName: "\(firstImageID).jpg")
        _ = try await env.database.insertQueued(id: secondImageID, type: .image, originalName: "\(secondImageID).jpg")
        _ = try await env.database.insertQueued(id: heldVideoID, type: .video, originalName: "\(heldVideoID).mov")

        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(firstImageID)", width: 1600, height: 1200)
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(secondImageID)", width: 1600, height: 1200)
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(heldVideoID)"))

        let config = makeComplexityAwareConfig(
            env: env,
            maxConcurrentImages: 1,
            maxConcurrentVideos: 1
        )
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
        let firstArchiveGate = BlockingGate()
        let recomputeRecorder = MediaProcessorRecomputeTriggerRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-image-finish"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                if assetId == firstImageID {
                    await firstArchiveGate.wait()
                }
                return .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true
        )

        await processor.setRecomputeSignal { trigger in
            Task { await recomputeRecorder.record(trigger) }
        }

        #expect(await processor.enqueue(
            uploadId: firstImageID,
            originalName: "\(firstImageID).jpg",
            filePath: "\(env.uploadDir)/\(firstImageID)",
            assetType: .image
        ))
        #expect(await processor.enqueue(
            uploadId: secondImageID,
            originalName: "\(secondImageID).jpg",
            filePath: "\(env.uploadDir)/\(secondImageID)",
            assetType: .image
        ))

        let imageBacklogEstablished = try await waitUntil(timeoutSeconds: 3) {
            let firstStatus = try await env.database.getAsset(id: firstImageID)?.status
            let secondStatus = try await env.database.getAsset(id: secondImageID)?.status
            return firstStatus == .processing && secondStatus == .queued
        }
        #expect(imageBacklogEstablished)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: heldVideoID,
                originalName: "\(heldVideoID).mov",
                filePath: "\(env.uploadDir)/\(heldVideoID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 500,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        let stableHold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(10.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: heldVideoID, hold: stableHold))
        #expect(await processor.videoJobState(uploadId: heldVideoID) == .held(stableHold))

        await firstArchiveGate.open()

        let secondImageCompleted = try await waitUntil(timeoutSeconds: 6) {
            let secondStatus = try await env.database.getAsset(id: secondImageID)?.status
            return secondStatus == .complete
        }
        #expect(secondImageCompleted)

        try? await Task.sleep(for: .milliseconds(100))
        #expect(await processor.videoJobState(uploadId: heldVideoID) == .held(stableHold))
        #expect(await recomputeRecorder.count(.finish) == 0)

        await processor.shutdown()
    }

    @Test("production dispatches now to the currently free remote slot before future local reservation")
    func productionDispatchesNowToCurrentlyFreeRemoteSlotBeforeFutureLocalReservation() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-production-hold-fast-first"
        let heldID = "mp-behavior-production-hold-fast-held"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: heldID, type: .video, originalName: "\(heldID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(heldID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let remoteSignature = "mp-behavior-production-hold-fast-remote-signature"
        let priorTable = makeRemoteComplexityAwarePriorTable(
            config: config,
            localMSPerFrameC1: 1.0,
            remoteSignature: remoteSignature,
            remoteMSPerFrameC1: 10.0
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for future-aware production routing")

        let firstGate = BlockingGate()
        let remoteGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-production-hold-fast"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                if uploadId == heldID {
                    await remoteGate.wait()
                }
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 4_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStartedLocal)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: heldID,
                originalName: "\(heldID).mov",
                filePath: "\(env.uploadDir)/\(heldID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let heldStartedRemote = try await waitUntil(timeoutSeconds: 5) {
            await routeRecorder.route(uploadId: heldID) == .remote
        }
        #expect(heldStartedRemote)
        #expect(await processor.videoJobState(uploadId: heldID) == .active)

        await firstGate.open()
        await remoteGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let heldStatus = try await env.database.getAsset(id: heldID)?.status
            return firstStatus == .complete && heldStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("production dispatches now to the slower ready slot when waiting loses")
    func productionDispatchesNowToTheSlowerReadySlotWhenWaitingLoses() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-production-send-slow-first"
        let secondID = "mp-behavior-production-send-slow-second"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: secondID, type: .video, originalName: "\(secondID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let remoteSignature = "mp-behavior-production-send-slow-remote-signature"
        let priorTable = makeRemoteComplexityAwarePriorTable(
            config: config,
            localMSPerFrameC1: 1.0,
            remoteSignature: remoteSignature,
            remoteMSPerFrameC1: 2.0
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for future-aware production routing")

        let firstGate = BlockingGate()
        let remoteGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-production-send-slow"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                if uploadId == secondID {
                    await remoteGate.wait()
                }
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 10_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStartedLocal)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let secondStartedRemote = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: secondID) == .remote
        }
        #expect(secondStartedRemote)
        #expect(await processor.videoJobState(uploadId: secondID) == .active)

        await remoteGate.open()
        await firstGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            return firstStatus == .complete && secondStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("stale production hold wakes, revalidates, and recomputes instead of blindly dispatching")
    func staleProductionHoldRevalidatesAndRecomputesInsteadOfBlindDispatch() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let dispatchID = "mp-behavior-production-stale-hold-dispatch"
        let uploadID = "mp-behavior-production-stale-hold-001"
        for id in [dispatchID, uploadID] {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
        }
        try Data(repeating: 0x60, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(dispatchID)"))
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(uploadID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let remoteSignature = "mp-behavior-production-stale-hold-remote-signature"
        let priorTable = makeRemoteComplexityAwarePriorTable(
            config: config,
            localMSPerFrameC1: 5.0,
            remoteSignature: remoteSignature,
            remoteMSPerFrameC1: 1.0
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for future-aware production routing")

        await dispatcher.testHookSeedRunningEstimate(workerIndex: 0, slotIndex: 0, estRemainingMS: 150)

        let launchGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-production-stale-hold"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == dispatchID {
                    await launchGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        for id in [dispatchID, uploadID] {
            await processor.enqueueJob(
                ProcessingJob(
                    uploadId: id,
                    originalName: "\(id).mov",
                    filePath: "\(env.uploadDir)/\(id)",
                    assetType: .video,
                    estimatedVideoRuntimeSeconds: 60,
                    frameCount: 100,
                    probedDurationSeconds: 60,
                    videoEstimateConfidence: .high
                )
            )
        }
        await processor.processQueues()

        let dispatchStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: dispatchID) == .local
        }
        #expect(dispatchStartedLocal)

        let heldForRemote = try await waitUntil(timeoutSeconds: 2) {
            guard case .held(let hold)? = await processor.videoJobState(uploadId: uploadID) else {
                return false
            }
            return hold.targetSlotID == "127.0.0.1#s1"
        }
        #expect(heldForRemote)

        try? await Task.sleep(for: .milliseconds(50))
        await dispatcher.testHookSeedRunningEstimate(workerIndex: 0, slotIndex: 0, estRemainingMS: 2_000)
        await launchGate.open()

        let reroutedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: uploadID) == .local
        }
        #expect(reroutedLocal)
        #expect(await dispatcher.testHookPreflightUnavailableDispatchCount() == 0)

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let status = try await env.database.getAsset(id: uploadID)?.status
            return status == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("updateType failure aborts processing before derived outputs")
    func updateTypeFailureAbortsProcessing() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mp-behavior-update-type-fail-001"
        _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).jpg")

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 1200, height: 900)

        let triggerDb = try DatabaseQueue(path: env.config.databasePath)
        try await triggerDb.write { db in
            try db.execute(
                sql: """
                CREATE TRIGGER fail_update_type_mp_behavior
                BEFORE UPDATE OF type ON assets
                FOR EACH ROW
                WHEN NEW.id = '\(id)'
                BEGIN
                    SELECT RAISE(ABORT, 'forced updateType failure');
                END;
                """
            )
        }

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-update-type"))
        let processor = MediaProcessor(config: env.config, database: env.database, moderationMarkers: markers)
        let accepted = await processor.enqueue(
            uploadId: id,
            originalName: "\(id).jpg",
            filePath: uploadPath,
            assetType: .image
        )
        #expect(accepted)

        let reachedProcessing = try await waitUntil(timeoutSeconds: 2) {
            let asset = try await env.database.getAsset(id: id)
            return asset?.status == .processing
        }
        #expect(reachedProcessing, "Setup: job should enter processing before updateType failure short-circuits")

        try? await Task.sleep(for: .milliseconds(200))

        let thumbPath = "\(env.thumbsDir)/\(id).jpg"
        let previewPath = "\(env.previewsDir)/\(id).jpg"
        #expect(!FileManager.default.fileExists(atPath: thumbPath))
        #expect(!FileManager.default.fileExists(atPath: previewPath))
        #expect(FileManager.default.fileExists(atPath: uploadPath))

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .processing)
        #expect(asset?.type == .video)
    }

    @Test("duplicate enqueue for same upload while in-flight does not duplicate processing work")
    func duplicateEnqueueInFlightIsIdempotent() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "mp-behavior-idem-001"
        _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")

        let uploadPath = "\(env.uploadDir)/\(id)"
        try TestImage.writeJPEG(to: uploadPath, width: 1400, height: 1000)

        let archiveOriginal: @Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult = { _, _, _ in
            try? await Task.sleep(for: .milliseconds(350))
            return .success(externalPath: "/tmp/\(UUID().uuidString)", checksum: "test-checksum")
        }

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-idempotent"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveOriginal
        )

        let launchEvents = ProcessingLaunchRecorder()
        let completed = try await MediaProcessor.withProcessingLaunchObserver({ event in
            launchEvents.record(event)
        }) {
            async let firstAccepted = processor.enqueue(
                uploadId: id,
                originalName: "\(id).jpg",
                filePath: uploadPath,
                assetType: .image
            )
            async let secondAccepted = processor.enqueue(
                uploadId: id,
                originalName: "\(id).jpg",
                filePath: uploadPath,
                assetType: .image
            )

            let (first, second) = await (firstAccepted, secondAccepted)
            #expect(first)
            #expect(second)

            return try await waitUntil(timeoutSeconds: 4) {
                let asset = try await env.database.getAsset(id: id)
                return asset?.status == .complete
            }
        }
        #expect(completed)

        #expect(launchEvents.count(site: .imageThumbnail, inheritsContext: nil) == 1)
        #expect(launchEvents.count(site: .imagePreview, inheritsContext: nil) == 1)
        #expect(launchEvents.count(site: .imageTimestamp, inheritsContext: nil) == 1)
        #expect(launchEvents.count(site: .archiveDetached, inheritsContext: nil) == 1)
    }

    @Test("concurrent unique enqueues complete without losing jobs")
    func concurrentUniqueEnqueuesComplete() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ids = (0..<10).map { "mp-behavior-concurrent-\(String(format: "%03d", $0))" }
        for id in ids {
            _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
            try TestImage.writeJPEG(to: "\(env.uploadDir)/\(id)", width: 1200, height: 900)
        }

        let archiveOriginal: @Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult = { _, _, _ in
            .success(externalPath: "/tmp/\(UUID().uuidString)", checksum: "test-checksum")
        }

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-concurrent"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveOriginal
        )

        var acceptedCount = 0
        try await withThrowingTaskGroup(of: Bool.self) { group in
            for id in ids {
                let uploadPath = "\(env.uploadDir)/\(id)"
                group.addTask {
                    await processor.enqueue(
                        uploadId: id,
                        originalName: "\(id).jpg",
                        filePath: uploadPath,
                        assetType: .image
                    )
                }
            }

            for try await accepted in group where accepted {
                acceptedCount += 1
            }
        }
        #expect(acceptedCount == ids.count)

        let allComplete = try await waitUntil(timeoutSeconds: 6) {
            for id in ids {
                let asset = try await env.database.getAsset(id: id)
                if asset?.status != .complete {
                    return false
                }
            }
            return true
        }
        #expect(allComplete)

        for id in ids {
            let thumbPath = "\(env.thumbsDir)/\(id).jpg"
            let previewPath = "\(env.previewsDir)/\(id).jpg"
            #expect(FileManager.default.fileExists(atPath: thumbPath))
            #expect(FileManager.default.fileExists(atPath: previewPath))
        }
    }

    @Test("scheduling metrics collect sumW, failed_count, and latency distribution in production path")
    func schedulingMetricsCollectInProductionPath() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let successID = "mp-behavior-scheduling-metrics-success"
        let failedID = "mp-behavior-scheduling-metrics-failed"
        _ = try await env.database.insertQueued(id: successID, type: .video, originalName: "\(successID).mov")
        _ = try await env.database.insertQueued(id: failedID, type: .video, originalName: "\(failedID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(successID)"))
        try Data(repeating: 0x62, count: 768).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(failedID)"))

        let config = makeComplexityAwareConfig(env: env)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-scheduling-metrics"))
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
                if uploadId == failedID {
                    return (thumb: false, preview: false, timestamp: "2026:02:28 10:00:00")
                }
                return (thumb: true, preview: true, timestamp: "2026:02:28 10:00:00")
            }
        )

        #expect(await processor.enqueue(uploadId: successID, originalName: "\(successID).mov", filePath: "\(env.uploadDir)/\(successID)", assetType: .video))
        #expect(await processor.enqueue(uploadId: failedID, originalName: "\(failedID).mov", filePath: "\(env.uploadDir)/\(failedID)", assetType: .video))

        let terminal = try await waitUntil(timeoutSeconds: 4) {
            let success = try await env.database.getAsset(id: successID)?.status
            let failed = try await env.database.getAsset(id: failedID)?.status
            return success == .complete && failed == .failed
        }
        #expect(terminal)

        guard let metrics = await processor.schedulingMetricsSnapshot() else {
            Issue.record("Expected scheduling metrics snapshot")
            return
        }
        #expect(metrics.sumWSeconds >= 0)
        #expect(metrics.p95Seconds >= 0)
        #expect(metrics.makespanSeconds >= 0)
        #expect(metrics.failedCount == 1)
    }

    @Test("scheduling metrics are video-only and exclude image failures")
    func schedulingMetricsExcludeImageFailures() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let videoID = "mp-behavior-scheduling-video-only-video"
        let imageID = "mp-behavior-scheduling-video-only-image"
        _ = try await env.database.insertQueued(id: videoID, type: .video, originalName: "\(videoID).mov")
        _ = try await env.database.insertQueued(id: imageID, type: .image, originalName: "\(imageID).jpg")

        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(videoID)"))
        try Data(repeating: 0x62, count: 256).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(imageID)"))

        let config = makeComplexityAwareConfig(env: env)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-scheduling-video-only"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            localVideoProcessingOverride: { _, _, _ in
                (thumb: true, preview: true, timestamp: "2026:02:28 10:00:00")
            }
        )

        #expect(await processor.enqueue(uploadId: videoID, originalName: "\(videoID).mov", filePath: "\(env.uploadDir)/\(videoID)", assetType: .video))
        #expect(await processor.enqueue(uploadId: imageID, originalName: "\(imageID).jpg", filePath: "\(env.uploadDir)/\(imageID)", assetType: .image))

        let terminal = try await waitUntil(timeoutSeconds: 4) {
            let videoStatus = try await env.database.getAsset(id: videoID)?.status
            let imageStatus = try await env.database.getAsset(id: imageID)?.status
            return videoStatus == .complete && imageStatus == .failed
        }
        #expect(terminal)

        guard let metrics = await processor.schedulingMetricsSnapshot() else {
            Issue.record("Expected scheduling metrics snapshot")
            return
        }
        #expect(metrics.failedCount == 0)
        #expect(metrics.sumWSeconds >= 0)
        #expect(metrics.p95Seconds >= 0)
        #expect(metrics.makespanSeconds >= 0)
    }

    @Test("non-zero remote tail estimate can flip complexity-aware routing")
    func nonZeroRemoteTailEstimateCanFlipComplexityAwareRouting() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-ca-tail-first"
        let secondID = "mp-behavior-ca-tail-second"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: secondID, type: .video, originalName: "\(secondID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env)
        let remoteSignature = "mp-behavior-ca-tail-remote-signature"
        var priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 4.0)
        priorTable.merge(
            remoteMachine: BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 0.5,
                avgCorpusFrameCount: 60 * 24,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 90,
                        msPerVideoP50: 800,
                        msPerVideoP95: 1_000,
                        degradationRatio: 1.0
                    ),
                    BenchmarkPriorCell(
                        concurrency: 2,
                        videosPerMin: 140,
                        msPerVideoP50: 1_100,
                        msPerVideoP95: 1_400,
                        degradationRatio: 1.2
                    ),
                ]
            )
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let connectFDSource = LockedFDSource()
        let monotonicClock = MonotonicNowSequence()

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            monotonicNowNanosOverride: { monotonicClock.next() },
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                connectFDSource.take()
            },
            queryCapabilitiesOverride: { _, _, _, _ in
                workerCaps
            }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: dispatcher should expose a profiled remote worker for CA selection")

        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-tail-routing"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                return (thumb: true, preview: true, timestamp: "2026:03:04 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_200,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstComplete = try await waitUntil(timeoutSeconds: 4) {
            let status = try await env.database.getAsset(id: firstID)?.status
            return status == .complete
        }
        #expect(firstComplete)
        #expect(await routeRecorder.route(uploadId: firstID) == .remote)

        let seedUploadID = "mp-behavior-ca-tail-seed"
        let seedUploadPath = "\(env.uploadDir)/\(seedUploadID)"
        try Data(repeating: 0x7A, count: 128).write(to: URL(fileURLWithPath: seedUploadPath))
        monotonicClock.installScriptedValues([
            1_000_000_000,
            2_000_000_000,
            3_000_000_000,
            35_000_000_000,
            70_000_000_000,
            100_000_000_000,
        ])
        let seededTailTelemetry = try await seedRemoteTailTelemetrySample(
            dispatcher: dispatcher,
            connectFDSource: connectFDSource,
            uploadId: seedUploadID,
            uploadPath: seedUploadPath
        )
        #expect(seededTailTelemetry == .success)

        let tailSnapshot = await dispatcher.complexityAwareSnapshot()
        #expect((tailSnapshot.first?.txOutEstimateMS ?? 0) > 0)
        #expect((tailSnapshot.first?.publishOverheadEstimateMS ?? 0) > 0)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_200,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let secondComplete = try await waitUntil(timeoutSeconds: 4) {
            let status = try await env.database.getAsset(id: secondID)?.status
            return status == .complete
        }
        #expect(secondComplete)
        #expect(await routeRecorder.route(uploadId: secondID) == .local)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("active local readiness uses frame-count runtime before stale stored probe estimate")
    func activeLocalReadinessUsesFrameCountBeforeStoredProbeEstimate() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-ready-active-frame-first"
        let secondID = "mp-behavior-ready-active-frame-second"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: secondID, type: .video, originalName: "\(secondID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let remoteSignature = "mp-behavior-ready-active-frame-remote-signature"
        let priorTable = makeSlowRemoteComplexityAwarePriorTable(config: config, remoteSignature: remoteSignature)

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for CA routing choices")

        let firstGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ready-active-frame"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:04 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStartedLocal)

        try? await Task.sleep(for: .milliseconds(180))

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let secondRouted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: secondID) != nil
        }
        #expect(secondRouted)
        #expect(await routeRecorder.route(uploadId: secondID) == .local)

        await firstGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            return firstStatus == .complete && secondStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("active local readiness falls back to stored estimate when frame count is missing")
    func activeLocalReadinessFallsBackToStoredEstimateWhenFrameCountIsMissing() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-ready-active-fallback-first"
        let secondID = "mp-behavior-ready-active-fallback-second"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: secondID, type: .video, originalName: "\(secondID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let remoteSignature = "mp-behavior-ready-active-fallback-remote-signature"
        let priorTable = makeSlowRemoteComplexityAwarePriorTable(config: config, remoteSignature: remoteSignature)

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for CA routing choices")

        let firstGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ready-active-fallback"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:04 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: nil,
                probedDurationSeconds: nil,
                videoEstimateConfidence: .low
            )
        )
        await processor.processQueues()

        let firstStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStartedLocal)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let secondRouted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: secondID) != nil
        }
        #expect(secondRouted)
        #expect(await routeRecorder.route(uploadId: secondID) == .remote)

        await firstGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            return firstStatus == .complete && secondStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("active scheduling under multi-slot load still dispatches queued work")
    func activeSchedulingUnderMultiSlotLoadStillDispatchesQueuedWork() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-ready-active-multiload-first"
        let secondID = "mp-behavior-ready-active-multiload-second"
        let thirdID = "mp-behavior-ready-active-multiload-third"
        for id in [firstID, secondID, thirdID] {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let localContext = try makeLocalMediaProcessorPriorContext(preset: config.videoTranscodePreset)
        let remoteSignature = "mp-behavior-ready-active-multiload-remote-signature"
        var priorTable = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: localContext.signature,
                chipName: localContext.machine.chipName,
                performanceCores: localContext.machine.performanceCores,
                efficiencyCores: localContext.machine.efficiencyCores,
                videoEncodeEngines: localContext.machine.videoEncodeEngines,
                osVersion: localContext.machine.osVersion,
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 1.0,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 600,
                        msPerVideoP50: 100,
                        msPerVideoP95: 120,
                        degradationRatio: 1.0
                    ),
                    BenchmarkPriorCell(
                        concurrency: 2,
                        videosPerMin: 150,
                        msPerVideoP50: 400,
                        msPerVideoP95: 460,
                        degradationRatio: 4.0
                    ),
                ]
            ),
        ])
        priorTable.merge(
            remoteMachine: BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 2.0,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 300,
                        msPerVideoP50: 200,
                        msPerVideoP95: 240,
                        degradationRatio: 1.0
                    ),
                ]
            )
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for CA routing choices")

        let localGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ready-active-multiload"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID || uploadId == secondID {
                    await localGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:04 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        for id in [firstID, secondID] {
            await processor.enqueueJob(
                ProcessingJob(
                    uploadId: id,
                    originalName: "\(id).mov",
                    filePath: "\(env.uploadDir)/\(id)",
                    assetType: .video,
                    estimatedVideoRuntimeSeconds: 60,
                    frameCount: 100,
                    probedDurationSeconds: 60,
                    videoEstimateConfidence: .high
                )
            )
        }
        await processor.processQueues()

        let firstTwoStarted = try await waitUntil(timeoutSeconds: 2) {
            let firstRoute = await routeRecorder.route(uploadId: firstID)
            let secondRoute = await routeRecorder.route(uploadId: secondID)
            return firstRoute != nil && secondRoute != nil
        }
        #expect(firstTwoStarted)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: thirdID,
                originalName: "\(thirdID).mov",
                filePath: "\(env.uploadDir)/\(thirdID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let thirdRouted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: thirdID) != nil
        }
        #expect(thirdRouted)

        await localGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            let thirdStatus = try await env.database.getAsset(id: thirdID)?.status
            return firstStatus == .complete && secondStatus == .complete && thirdStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("active local runtime projection repairs sparse degradation points before routing")
    func activeLocalRuntimeProjectionRepairsSparseDegradationPointsBeforeRouting() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-sparse-active-runtime-first"
        let secondID = "mp-behavior-sparse-active-runtime-second"
        let thirdID = "mp-behavior-sparse-active-runtime-third"
        for id in [firstID, secondID, thirdID] {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let localContext = try makeLocalMediaProcessorPriorContext(preset: config.videoTranscodePreset)
        let remoteSignature = "mp-behavior-sparse-active-runtime-remote-signature"
        var priorTable = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: localContext.signature,
                chipName: localContext.machine.chipName,
                performanceCores: localContext.machine.performanceCores,
                efficiencyCores: localContext.machine.efficiencyCores,
                videoEncodeEngines: localContext.machine.videoEncodeEngines,
                osVersion: localContext.machine.osVersion,
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 1.0,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 600,
                        msPerVideoP50: 100,
                        msPerVideoP95: 120,
                        degradationRatio: 1.0
                    ),
                    BenchmarkPriorCell(
                        concurrency: 3,
                        videosPerMin: 150,
                        msPerVideoP50: 400,
                        msPerVideoP95: 460,
                        degradationRatio: 4.0
                    ),
                ]
            ),
        ])
        priorTable.merge(
            remoteMachine: BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 3.0,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 200,
                        msPerVideoP50: 300,
                        msPerVideoP95: 340,
                        degradationRatio: 1.0
                    ),
                ]
            )
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for CA routing choices")

        let localGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-sparse-active-runtime"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID || uploadId == secondID {
                    await localGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:04 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        for id in [firstID, secondID] {
            await processor.enqueueJob(
                ProcessingJob(
                    uploadId: id,
                    originalName: "\(id).mov",
                    filePath: "\(env.uploadDir)/\(id)",
                    assetType: .video,
                    estimatedVideoRuntimeSeconds: 60,
                    frameCount: 100,
                    probedDurationSeconds: 60,
                    videoEstimateConfidence: .high
                )
            )
        }
        await processor.processQueues()

        let firstTwoStartedLocally = try await waitUntil(timeoutSeconds: 2) {
            let firstRoute = await routeRecorder.route(uploadId: firstID)
            let secondRoute = await routeRecorder.route(uploadId: secondID)
            return firstRoute == .local && secondRoute == .local
        }
        #expect(firstTwoStartedLocally)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: thirdID,
                originalName: "\(thirdID).mov",
                filePath: "\(env.uploadDir)/\(thirdID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let thirdStartedRemotely = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: thirdID) == .remote
        }
        #expect(
            thirdStartedRemotely,
            "Sparse local degradation should be repaired before active-runtime projection so waiting behind two local jobs does not look artificially free"
        )

        await localGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            let thirdStatus = try await env.database.getAsset(id: thirdID)?.status
            return firstStatus == .complete && secondStatus == .complete && thirdStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("affine remote prior fixed overhead can flip complexity-aware routing")
    func affineRemotePriorFixedOverheadCanFlipComplexityAwareRouting() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "mp-behavior-ca-affine-fixed-001"
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: "\(uploadID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(uploadID)"))

        let config = makeComplexityAwareConfig(env: env)
        let remoteSignature = "mp-behavior-ca-affine-fixed-remote-signature"
        var priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
        priorTable.merge(
            remoteMachine: BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 0.5,
                fixedOverheadMS: 200,
                avgCorpusFrameCount: 60 * 24,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 10,
                        msPerVideoP50: 250,
                        msPerVideoP95: 350,
                        degradationRatio: 1.0
                    ),
                ]
            )
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for CA routing choices")

        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-affine-fixed"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                return (thumb: true, preview: true, timestamp: "2026:03:07 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: uploadID,
                originalName: "\(uploadID).mov",
                filePath: "\(env.uploadDir)/\(uploadID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let routed = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: uploadID) != nil
        }
        #expect(routed)
        #expect(await routeRecorder.route(uploadId: uploadID) == .local)

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: uploadID)
            return asset?.status == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("affine local prior fixed overhead is not double counted after live update")
    func affineLocalPriorFixedOverheadIsNotDoubleCountedAfterLiveUpdate() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-ca-affine-local-first"
        let secondID = "mp-behavior-ca-affine-local-second"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: secondID, type: .video, originalName: "\(secondID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env)
        let localContext = try makeLocalMediaProcessorPriorContext(preset: config.videoTranscodePreset)
        let remoteSignature = "mp-behavior-ca-affine-local-remote-signature"
        var priorTable = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: localContext.signature,
                chipName: localContext.machine.chipName,
                performanceCores: localContext.machine.performanceCores,
                efficiencyCores: localContext.machine.efficiencyCores,
                videoEncodeEngines: localContext.machine.videoEncodeEngines,
                osVersion: localContext.machine.osVersion,
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 1.0,
                fixedOverheadMS: 200,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 200,
                        msPerVideoP50: 300,
                        msPerVideoP95: 360,
                        degradationRatio: 1.0
                    ),
                ]
            ),
        ])
        priorTable.merge(
            remoteMachine: BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 3.1,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 190,
                        msPerVideoP50: 310,
                        msPerVideoP95: 360,
                        degradationRatio: 1.0
                    ),
                ]
            )
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior, "Setup: profiled remote worker is required for CA routing choices")

        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-affine-local"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    try? await Task.sleep(for: .milliseconds(300))
                }
                return (thumb: true, preview: true, timestamp: "2026:03:07 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStartedLocal)

        let firstCompleted = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            return firstStatus == .complete
        }
        #expect(firstCompleted)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 100,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let secondRouted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: secondID) != nil
        }
        #expect(secondRouted)
        #expect(await routeRecorder.route(uploadId: secondID) == .local)

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            return firstStatus == .complete && secondStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("active runtime estimate prioritizes frame count over stored probe estimate in routing")
    func activeRuntimeEstimatePrioritizesFrameCountOverStoredProbeEstimateInRouting() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-behavior-active-runtime-first"
        let remoteFillID = "mp-behavior-active-runtime-remote-fill"
        let secondID = "mp-behavior-active-runtime-second"
        for id in [firstID, remoteFillID, secondID] {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
        }
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x63, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(remoteFillID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let remoteSignature = "mp-behavior-active-runtime-remote-signature"
        let priorTable = makeRemoteComplexityAwarePriorTable(
            config: config,
            localMSPerFrameC1: 1.0,
            remoteSignature: remoteSignature,
            remoteMSPerFrameC1: 100.0
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6567,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior)

        let firstGate = BlockingGate()
        let remoteGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-active-runtime-priority"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                if uploadId == remoteFillID {
                    await remoteGate.wait()
                }
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:06 10:00:00")
            }
        )

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 1_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: remoteFillID,
                originalName: "\(remoteFillID).mov",
                filePath: "\(env.uploadDir)/\(remoteFillID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 600,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStartedLocal = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStartedLocal)
        let remoteFillStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: remoteFillID) == .remote
        }
        #expect(remoteFillStarted)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 50,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        try? await Task.sleep(for: .milliseconds(100))
        #expect(await routeRecorder.route(uploadId: secondID) == nil)

        await firstGate.open()

        let secondStartedLocal = try await waitUntil(timeoutSeconds: 8) {
            await routeRecorder.route(uploadId: secondID) == .local
        }
        #expect(secondStartedLocal)

        await remoteGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let firstStatus = try await env.database.getAsset(id: firstID)?.status
            let remoteFillStatus = try await env.database.getAsset(id: remoteFillID)?.status
            let secondStatus = try await env.database.getAsset(id: secondID)?.status
            return firstStatus == .complete && remoteFillStatus == .complete && secondStatus == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("local live msPerFrameC1 tracks sustained runtime shifts without exact math pinning")
    func localLiveMSPerFrameC1UsesLockedAdaptiveResponseRateUpdates() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeComplexityAwareConfig(env: env)
        let localContext = try makeLocalMediaProcessorPriorContext(preset: config.videoTranscodePreset)
        let startingEstimate = 9.135
        let observations = Array(repeating: 4.9, count: 10) + [7.0]

        let priorTable = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: localContext.signature,
                chipName: localContext.machine.chipName,
                performanceCores: localContext.machine.performanceCores,
                efficiencyCores: localContext.machine.efficiencyCores,
                videoEncodeEngines: localContext.machine.videoEncodeEngines,
                osVersion: localContext.machine.osVersion,
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: startingEstimate,
                fixedOverheadMS: 0,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 100,
                        msPerVideoP50: 914,
                        msPerVideoP95: 1_000,
                        degradationRatio: 1.0
                    ),
                ]
            ),
        ])

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-local-adaptive-learning"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true
        )

        let initialEstimate = try #require(await processor.localLiveMSPerFrameC1Estimate())
        #expect(abs(initialEstimate - startingEstimate) < 0.000_001)

        let frameCount = 100.0
        var estimates: [Double] = []
        estimates.reserveCapacity(observations.count)

        for observation in observations {
            let processNanos = UInt64((observation * frameCount * 1_000_000).rounded())
            await processor.updateLocalLiveMSPerFrame(
                processNanos: processNanos,
                frameCount: frameCount
            )

            let estimate = try #require(await processor.localLiveMSPerFrameC1Estimate())
            estimates.append(estimate)
        }

        let fastPhase = Array(estimates.prefix(10))
        let firstFastEstimate = try #require(fastPhase.first)
        let stabilizedFastEstimate = try #require(fastPhase.last)
        let postSlowEstimate = try #require(estimates.last)

        #expect(
            firstFastEstimate < startingEstimate,
            "A faster first observation should immediately reduce the live estimate"
        )

        for index in 1..<fastPhase.count {
            #expect(
                fastPhase[index] <= fastPhase[index - 1] + 0.000_001,
                "Repeated faster observations should not make the live estimate climb"
            )
        }

        #expect(
            fastPhase[4] <= 5.5,
            "Sustained faster observations should pull the estimate down quickly enough to affect routing"
        )
        #expect(
            stabilizedFastEstimate >= 4.8 && stabilizedFastEstimate <= 5.1,
            "After sustained 4.9 ms/frame observations, the estimate should stay near that steady state"
        )
        #expect(
            postSlowEstimate > stabilizedFastEstimate,
            "A slower observation should raise the live estimate after a fast steady state"
        )
        #expect(
            postSlowEstimate < 7.0,
            "A single slower observation should not completely replace the learned steady-state runtime"
        )

        await processor.shutdown()
    }

    @Test("transient remote requeue aborts when queued status persistence fails")
    func transientRemoteRequeueRequiresDurableQueuedStatus() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "mp-behavior-transient-requeue-durable-001"
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: "\(uploadID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: uploadPath))

        let connectAttempts = LockedIntCounter()
        let queuedWriteAttempts = LockedIntCounter()

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6554,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                connectAttempts.increment()
                return nil
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        enum ForcedQueuedWriteError: Error { case failed }
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-transient-requeue"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            thunderboltDispatcher: dispatcher,
            statusUpdateOverride: { id, status in
                if status == .queued {
                    queuedWriteAttempts.increment()
                    throw ForcedQueuedWriteError.failed
                }
                try await env.database.updateStatus(id: id, status: status)
            }
        )

        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath,
            assetType: .video
        ))

        let sawInitialConnectAttempt = try await waitUntil(timeoutSeconds: 3) {
            connectAttempts.value() >= 1
        }
        #expect(sawInitialConnectAttempt)

        let sawQueuedWriteAttempt = try await waitUntil(timeoutSeconds: 3) {
            queuedWriteAttempts.value() >= 1
        }
        #expect(sawQueuedWriteAttempt)

        // Hold long enough for an incorrect re-enqueue to attempt a second queued write.
        try? await Task.sleep(for: .milliseconds(2200))

        #expect(queuedWriteAttempts.value() == 1, "Failed queued-status persistence must prevent re-enqueue")
        #expect(connectAttempts.value() >= 1)
        let asset = try await env.database.getAsset(id: uploadID)
        #expect(asset?.status == .processing)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    // MARK: - Multi-pick backlog fixtures

    @Test("startup backlog with multiple idle slots picks multiple jobs in one processQueues pass")
    func startupBacklogMultiPickFromIdleSlots() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ids = ["mp-backlog-startup-a", "mp-backlog-startup-b", "mp-backlog-startup-c"]
        for id in ids {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let launchGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backlog-startup"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
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

        await processor.processQueues()

        let multiLaunched = try await waitUntil(timeoutSeconds: 3) {
            var launchCount = 0
            for id in ids {
                if await routeRecorder.route(uploadId: id) != nil {
                    launchCount += 1
                }
            }
            return launchCount >= 2
        }
        #expect(multiLaunched, "Multiple idle local slots should pick multiple jobs in one pass")

        await launchGate.open()
        let completed = try await waitUntil(timeoutSeconds: 6) {
            for id in ids {
                let asset = try await env.database.getAsset(id: id)
                if asset?.status != .complete { return false }
            }
            return true
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("mixed dispatch-now plus hold in one CA batch")
    func mixedDispatchNowPlusHoldInOneBatch() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let dispatchID = "mp-backlog-mixed-dispatch"
        let holdID = "mp-backlog-mixed-hold"
        _ = try await env.database.insertQueued(id: dispatchID, type: .video, originalName: "\(dispatchID).mov")
        _ = try await env.database.insertQueued(id: holdID, type: .video, originalName: "\(holdID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(dispatchID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(holdID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let remoteSignature = "mp-backlog-mixed-remote-sig"
        let priorTable = makeRemoteComplexityAwarePriorTable(
            config: config,
            localMSPerFrameC1: 1.0,
            remoteSignature: remoteSignature,
            remoteMSPerFrameC1: 10.0
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior)

        await dispatcher.testHookSeedRunningEstimate(workerIndex: 0, slotIndex: 0, estRemainingMS: 3_000)

        let firstGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backlog-mixed"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                await firstGate.wait()
                return (thumb: true, preview: true, timestamp: "2026:03:09 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: dispatchID,
                originalName: "\(dispatchID).mov",
                filePath: "\(env.uploadDir)/\(dispatchID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 4_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: holdID,
                originalName: "\(holdID).mov",
                filePath: "\(env.uploadDir)/\(holdID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 4_000,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let dispatchRouted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: dispatchID) == .local
        }
        #expect(dispatchRouted, "One job should dispatch immediately to the idle local slot")

        let holdJobRouted = try await waitUntil(timeoutSeconds: 6) {
            await routeRecorder.route(uploadId: holdID) != nil
        }
        #expect(holdJobRouted, "Held job should eventually launch after the hold wake fires")

        let dispatchRecordedAt = await routeRecorder.recordedAt(uploadId: dispatchID)
        let holdRecordedAt = await routeRecorder.recordedAt(uploadId: holdID)
        if let dr = dispatchRecordedAt, let hr = holdRecordedAt {
            #expect(dr <= hr, "Dispatch-now job must launch before the held job")
        }

        await firstGate.open()

        let completed = try await waitUntil(timeoutSeconds: 6) {
            let d = try await env.database.getAsset(id: dispatchID)?.status
            let h = try await env.database.getAsset(id: holdID)?.status
            return d == .complete && h == .complete
        }
        #expect(completed)

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("multi-pick revalidation: held work is recomputed against new projected state")
    func multiPickRevalidationRecomputesAgainstProjectedState() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-backlog-reval-first"
        let secondID = "mp-backlog-reval-second"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: secondID, type: .video, originalName: "\(secondID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)

        let firstGate = BlockingGate()
        let secondGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backlog-reval"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID {
                    await firstGate.wait()
                } else if uploadId == secondID {
                    await secondGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:09 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 500,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStarted)
        #expect(await processor.videoJobState(uploadId: firstID) == .active)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 500,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        let hold = MediaProcessor.VideoHoldMetadata(
            wakeAt: Date().addingTimeInterval(10.0),
            targetSlotID: "local#s1"
        )
        #expect(await processor.setVideoHold(uploadId: secondID, hold: hold))
        #expect(await processor.videoJobState(uploadId: secondID) == .held(hold))

        await firstGate.open()

        let secondStarted = try await waitUntil(timeoutSeconds: 5) {
            await routeRecorder.route(uploadId: secondID) == .local
        }
        #expect(secondStarted, "Completion of first job should trigger recompute that launches held second job")
        #expect(await processor.videoJobState(uploadId: secondID) == .active)

        await secondGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let f = try await env.database.getAsset(id: firstID)?.status
            let s = try await env.database.getAsset(id: secondID)?.status
            return f == .complete && s == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("multi-pick with repeated same-slot reuse across picks")
    func multiPickRepeatedSameSlotReuse() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ids = ["mp-backlog-slot-reuse-a", "mp-backlog-slot-reuse-b"]
        for id in ids {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)

        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backlog-slot-reuse"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
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
                    frameCount: 100,
                    probedDurationSeconds: 60,
                    videoEstimateConfidence: .high
                )
            )
        }

        await processor.processQueues()

        let completed = try await waitUntil(timeoutSeconds: 6) {
            for id in ids {
                let asset = try await env.database.getAsset(id: id)
                if asset?.status != .complete { return false }
            }
            return true
        }
        #expect(completed, "Both jobs should complete even when assigned to the same local slot sequentially")

        for id in ids {
            #expect(await routeRecorder.route(uploadId: id) == .local)
        }

        await processor.shutdown()
    }

    @Test("multi-pick exclusion clearing near transient requeue path")
    func multiPickExclusionClearingNearTransientRequeue() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let excludedID = "mp-backlog-excl-clear-excluded"
        let normalID = "mp-backlog-excl-clear-normal"
        _ = try await env.database.insertQueued(id: excludedID, type: .video, originalName: "\(excludedID).mov")
        _ = try await env.database.insertQueued(id: normalID, type: .video, originalName: "\(normalID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(excludedID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(normalID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let remoteSignature = "mp-backlog-excl-clear-remote-sig"
        let priorTable = makeRemoteComplexityAwarePriorTable(
            config: config,
            localMSPerFrameC1: 1.0,
            remoteSignature: remoteSignature,
            remoteMSPerFrameC1: 0.5
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior)

        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backlog-excl-clear"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
                return (thumb: true, preview: true, timestamp: "2026:03:09 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: excludedID,
                originalName: "\(excludedID).mov",
                filePath: "\(env.uploadDir)/\(excludedID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 500,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: normalID,
                originalName: "\(normalID).mov",
                filePath: "\(env.uploadDir)/\(normalID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 500,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        await processor.recordTransientRemoteExclusion(
            uploadId: excludedID,
            workerIndex: 0,
            slotIndex: 0
        )

        await processor.processQueues()

        let completed = try await waitUntil(timeoutSeconds: 6) {
            let e = try await env.database.getAsset(id: excludedID)?.status
            let n = try await env.database.getAsset(id: normalID)?.status
            return e == .complete && n == .complete
        }
        #expect(completed, "Both jobs should complete despite one having a transient exclusion")

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("multi-pick backlog with affine fixed-overhead routing")
    func multiPickBacklogWithAffineFixedOverhead() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ids = ["mp-backlog-affine-a", "mp-backlog-affine-b"]
        for id in ids {
            _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
            try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(id)"))
        }

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 2)
        let remoteSignature = "mp-backlog-affine-remote-sig"
        var priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
        priorTable.merge(
            remoteMachine: BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: config.videoTranscodePreset,
                msPerFrameC1: 0.5,
                fixedOverheadMS: 5_000,
                avgCorpusFrameCount: 60 * 24,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 10,
                        msPerVideoP50: 250,
                        msPerVideoP95: 350,
                        degradationRatio: 1.0
                    ),
                ]
            )
        )

        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let workerCaps = makeWorkerCaps(signature: remoteSignature)
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: config.videoTranscodePreset,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in workerCaps }
        )
        await dispatcher.warmupPrior()

        let mergedRemotePrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedRemotePrior)

        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backlog-affine"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetId, _ in
                .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
            },
            thunderboltDispatcher: dispatcher,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { uploadId, _, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .remote)
                return (thumb: true, preview: true)
            },
            localVideoProcessingOverride: { uploadId, _, _ in
                await routeRecorder.record(uploadId: uploadId, route: .local)
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
                    frameCount: 100,
                    probedDurationSeconds: 60,
                    videoEstimateConfidence: .high
                )
            )
        }
        await processor.processQueues()

        let completed = try await waitUntil(timeoutSeconds: 6) {
            for id in ids {
                let asset = try await env.database.getAsset(id: id)
                if asset?.status != .complete { return false }
            }
            return true
        }
        #expect(completed)

        var routedLocal = 0
        for id in ids {
            if await routeRecorder.route(uploadId: id) == .local {
                routedLocal += 1
            }
        }
        #expect(routedLocal == 2, "High fixed overhead on remote should route both small jobs locally")

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("projected-state carryover: second processQueues call recomputes against post-first-pass state")
    func projectedStateCarryoverThroughProductionBehavior() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "mp-backlog-carryover-first"
        let secondID = "mp-backlog-carryover-second"
        _ = try await env.database.insertQueued(id: firstID, type: .video, originalName: "\(firstID).mov")
        _ = try await env.database.insertQueued(id: secondID, type: .video, originalName: "\(secondID).mov")
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(firstID)"))
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: "\(env.uploadDir)/\(secondID)"))

        let config = makeComplexityAwareConfig(env: env, maxConcurrentVideos: 1)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)

        let firstGate = BlockingGate()
        let secondGate = BlockingGate()
        let routeRecorder = VideoRouteRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backlog-carryover"))
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
                await routeRecorder.record(uploadId: uploadId, route: .local)
                if uploadId == firstID { await firstGate.wait() }
                if uploadId == secondID { await secondGate.wait() }
                return (thumb: true, preview: true, timestamp: "2026:03:09 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: "\(env.uploadDir)/\(firstID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 200,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )
        await processor.processQueues()

        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await routeRecorder.route(uploadId: firstID) == .local
        }
        #expect(firstStarted)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: "\(env.uploadDir)/\(secondID)",
                assetType: .video,
                estimatedVideoRuntimeSeconds: 60,
                frameCount: 200,
                probedDurationSeconds: 60,
                videoEstimateConfidence: .high
            )
        )

        await firstGate.open()

        let secondStarted = try await waitUntil(timeoutSeconds: 5) {
            await routeRecorder.route(uploadId: secondID) == .local
        }
        #expect(secondStarted, "Completion recompute should pick second job with fresh projected state")

        await secondGate.open()

        let completed = try await waitUntil(timeoutSeconds: 4) {
            let f = try await env.database.getAsset(id: firstID)?.status
            let s = try await env.database.getAsset(id: secondID)?.status
            return f == .complete && s == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }
}

private actor ArchiveProbe {
    private var started = false
    private var sawCancelled = false

    func markStarted() {
        started = true
    }

    func recordCancellationState(_ isCancelled: Bool) {
        sawCancelled = isCancelled
    }

    func hasStarted() -> Bool {
        started
    }

    func sawCancellation() -> Bool {
        sawCancelled
    }
}

private final class ProcessingLaunchRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var events: [MediaProcessor.ProcessingLaunchEvent] = []

    func record(_ event: MediaProcessor.ProcessingLaunchEvent) {
        lock.lock()
        events.append(event)
        lock.unlock()
    }

    func count(
        site: MediaProcessor.ProcessingLaunchEvent.Site,
        inheritsContext: Bool?
    ) -> Int {
        lock.lock()
        let value = events.filter { event in
            guard event.site == site else { return false }
            if let inheritsContext {
                return event.inheritsProcessingContext == inheritsContext
            }
            return true
        }.count
        lock.unlock()
        return value
    }
}

private final class LockedIntCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    func increment() {
        lock.lock()
        count += 1
        lock.unlock()
    }

    func value() -> Int {
        lock.lock()
        let current = count
        lock.unlock()
        return current
    }
}

private enum VideoRoute: Sendable, Equatable {
    case local
    case remote
}

private actor VideoRouteRecorder {
    private var routeByUploadId: [String: VideoRoute] = [:]
    private var recordedAtByUploadId: [String: Date] = [:]

    func record(uploadId: String, route: VideoRoute) {
        routeByUploadId[uploadId] = route
        if recordedAtByUploadId[uploadId] == nil {
            recordedAtByUploadId[uploadId] = Date()
        }
    }

    func route(uploadId: String) -> VideoRoute? {
        routeByUploadId[uploadId]
    }

    func recordedAt(uploadId: String) -> Date? {
        recordedAtByUploadId[uploadId]
    }
}

private actor MediaProcessorRecomputeTriggerRecorder {
    private var values: [ThunderboltDispatcher.RecomputeTrigger] = []

    func record(_ trigger: ThunderboltDispatcher.RecomputeTrigger) {
        values.append(trigger)
    }

    func count(_ trigger: ThunderboltDispatcher.RecomputeTrigger) -> Int {
        values.filter { $0 == trigger }.count
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

private final class LockedFDSource: @unchecked Sendable {
    private let lock = NSLock()
    private var pendingFD: Int32?

    func store(_ fd: Int32) {
        lock.lock()
        pendingFD = fd
        lock.unlock()
    }

    func take() -> Int32? {
        lock.lock()
        let fd = pendingFD
        pendingFD = nil
        lock.unlock()
        return fd
    }
}

private final class MonotonicNowSequence: @unchecked Sendable {
    private let lock = NSLock()
    private var scriptedValues: [UInt64] = []
    private var lastValue: UInt64 = 0

    init(scriptedValues: [UInt64] = []) {
        self.scriptedValues = scriptedValues
        self.lastValue = scriptedValues.last ?? 0
    }

    func installScriptedValues(_ values: [UInt64]) {
        lock.lock()
        scriptedValues = values
        if let last = values.last {
            lastValue = last
        }
        lock.unlock()
    }

    func next() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        if !scriptedValues.isEmpty {
            let value = scriptedValues.removeFirst()
            lastValue = value
            return value
        }
        lastValue = lastValue &+ 1_000_000
        return lastValue
    }
}

private struct SocketPair {
    let clientFD: Int32
    let serverFD: Int32
}

private func makeSocketPair() throws -> SocketPair {
    var descriptors = [Int32](repeating: -1, count: 2)
    guard socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0 else {
        throw POSIXError(.ENOTSOCK)
    }

    var one: Int32 = 1
    for fd in descriptors {
        _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
    }
    var readTimeout = timeval(tv_sec: 5, tv_usec: 0)
    _ = withUnsafePointer(to: &readTimeout) { timeoutPointer in
        setsockopt(
            descriptors[1],
            SOL_SOCKET,
            SO_RCVTIMEO,
            timeoutPointer,
            socklen_t(MemoryLayout<timeval>.size)
        )
    }
    return SocketPair(clientFD: descriptors[0], serverFD: descriptors[1])
}

private func seedRemoteTailTelemetrySample(
    dispatcher: ThunderboltDispatcher,
    connectFDSource: LockedFDSource,
    uploadId: String,
    uploadPath: String
) async throws -> ThunderboltDispatcher.DispatchResult {
    let pair = try makeSocketPair()
    connectFDSource.store(pair.clientFD)

    let workerTask = Task.detached(priority: .utility) {
        defer { Darwin.close(pair.serverFD) }
        guard consumeDispatchRequest(fd: pair.serverFD) else { return }

        let previewPayload = Data("preview".utf8)
        let thumbPayload = Data("thumb".utf8)
        let header = makeResponseHeader(
            status: 0x01,
            processNanos: 42_000_000,
            previewPayload: previewPayload,
            thumbPayload: thumbPayload
        )
        _ = writeAll(fd: pair.serverFD, data: header)
        _ = writeAll(fd: pair.serverFD, data: previewPayload)
        _ = writeAll(fd: pair.serverFD, data: thumbPayload)
    }

    let result = await dispatcher.dispatch(
        uploadId: uploadId,
        filePath: uploadPath,
        originalName: "\(uploadId).mov",
        mimeType: "video/quicktime",
        targetWorkerIndex: 0,
        targetSlotIndex: 0
    )

    _ = await workerTask.result
    return result
}

private func consumeDispatchRequest(fd: Int32) -> Bool {
    // Header prefix: fileSize(8) + sha(64) + nameLen(2)
    guard let prefix = readExactly(fd: fd, count: 74) else { return false }
    let fileSize = prefix.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: 0, as: UInt64.self).bigEndian)
    }
    guard fileSize >= 0 else { return false }

    let nameLen = prefix.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: 72, as: UInt16.self).bigEndian)
    }
    guard nameLen >= 0 else { return false }
    guard let nameAndMimeLen = readExactly(fd: fd, count: nameLen + 2) else { return false }

    let mimeLen = nameAndMimeLen.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: nameLen, as: UInt16.self).bigEndian)
    }
    guard mimeLen >= 0 else { return false }
    guard readExactly(fd: fd, count: mimeLen) != nil else { return false }
    guard readExactly(fd: fd, count: fileSize) != nil else { return false }
    return true
}

private func makeResponseHeader(
    status: UInt8,
    processNanos: UInt64,
    previewPayload: Data,
    thumbPayload: Data
) -> Data {
    let previewSHA = SHA256.hash(data: previewPayload).map { String(format: "%02x", $0) }.joined()
    let thumbSHA = SHA256.hash(data: thumbPayload).map { String(format: "%02x", $0) }.joined()

    var header = Data(capacity: 145)
    header.append(status)

    var processNanosBE = processNanos.bigEndian
    header.append(Data(bytes: &processNanosBE, count: MemoryLayout<UInt64>.size))

    var previewSizeBE = UInt32(previewPayload.count).bigEndian
    header.append(Data(bytes: &previewSizeBE, count: MemoryLayout<UInt32>.size))
    header.append(Data(previewSHA.utf8))

    var thumbSizeBE = UInt32(thumbPayload.count).bigEndian
    header.append(Data(bytes: &thumbSizeBE, count: MemoryLayout<UInt32>.size))
    header.append(Data(thumbSHA.utf8))

    return header
}

private func readExactly(fd: Int32, count: Int) -> Data? {
    guard count >= 0 else { return nil }
    guard count > 0 else { return Data() }

    var data = Data(count: count)
    let ok = data.withUnsafeMutableBytes { raw -> Bool in
        guard let base = raw.baseAddress else { return false }
        var offset = 0
        while offset < count {
            let rc = Darwin.read(fd, base.advanced(by: offset), count - offset)
            if rc < 0 {
                if errno == EINTR { continue }
                return false
            }
            if rc == 0 {
                return false
            }
            offset += rc
        }
        return true
    }
    return ok ? data : nil
}

@discardableResult
private func writeAll(fd: Int32, data: Data) -> Bool {
    var offset = 0
    return data.withUnsafeBytes { raw -> Bool in
        guard let base = raw.baseAddress else { return data.isEmpty }
        while offset < data.count {
            let rc = Darwin.write(fd, base.advanced(by: offset), data.count - offset)
            if rc < 0 {
                if errno == EINTR { continue }
                return false
            }
            if rc == 0 {
                return false
            }
            offset += rc
        }
        return true
    }
}

private func makeWorkerCaps(signature: String) -> WorkerCaps {
    let payload: [String: Any] = [
        "worker_signature": signature,
    ]
    let data = try! JSONSerialization.data(withJSONObject: payload, options: [])
    return try! JSONDecoder().decode(WorkerCaps.self, from: data)
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

private func makeComplexityAwareConfig(
    env: TestEnv,
    maxConcurrentImages: Int? = nil,
    maxConcurrentVideos: Int? = nil
) -> Config {
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
        maxConcurrentImages: maxConcurrentImages ?? env.config.maxConcurrentImages,
        maxConcurrentVideos: maxConcurrentVideos ?? env.config.maxConcurrentVideos,
        videoTranscodePreset: env.config.videoTranscodePreset,
        tbWorkers: "127.0.0.1:1"
    )
}

private func makeSlowRemoteComplexityAwarePriorTable(
    config: Config,
    remoteSignature: String
) -> BenchmarkPriorTable {
    var priorTable = makeLocalComplexityAwarePriorTable(config: config, msPerFrameC1: 1.0)
    priorTable.merge(
        remoteMachine: BenchmarkPriorMachine(
            signature: remoteSignature,
            chipName: "remote-test",
            performanceCores: 4,
            efficiencyCores: 0,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: config.videoTranscodePreset,
            msPerFrameC1: 10.0,
            avgCorpusFrameCount: 60 * 24,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 1,
                    videosPerMin: 6,
                    msPerVideoP50: 14_400,
                    msPerVideoP95: 16_500,
                    degradationRatio: 1.0
                ),
                BenchmarkPriorCell(
                    concurrency: 2,
                    videosPerMin: 9,
                    msPerVideoP50: 18_000,
                    msPerVideoP95: 21_000,
                    degradationRatio: 1.3
                ),
            ]
        )
    )
    return priorTable
}

private func makeRemoteComplexityAwarePriorTable(
    config: Config,
    localMSPerFrameC1: Double,
    remoteSignature: String,
    remoteMSPerFrameC1: Double
) -> BenchmarkPriorTable {
    let avgCorpusFrameCount = Double(60 * 24)
    let baseP50 = max(1, Int(Double(avgCorpusFrameCount) * remoteMSPerFrameC1))
    let degradedP50 = max(baseP50 + 1, Int(Double(baseP50) * 1.25))

    var priorTable = makeLocalComplexityAwarePriorTable(
        config: config,
        msPerFrameC1: localMSPerFrameC1
    )
    priorTable.merge(
        remoteMachine: BenchmarkPriorMachine(
            signature: remoteSignature,
            chipName: "remote-test",
            performanceCores: 4,
            efficiencyCores: 0,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: config.videoTranscodePreset,
            msPerFrameC1: remoteMSPerFrameC1,
            avgCorpusFrameCount: avgCorpusFrameCount,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 1,
                    videosPerMin: Double(max(1, Int(60_000 / max(1, baseP50)))),
                    msPerVideoP50: baseP50,
                    msPerVideoP95: max(baseP50 + 1, Int(Double(baseP50) * 1.15)),
                    degradationRatio: 1.0
                ),
                BenchmarkPriorCell(
                    concurrency: 2,
                    videosPerMin: Double(max(1, Int(60_000 / max(1, degradedP50)))),
                    msPerVideoP50: degradedP50,
                    msPerVideoP95: max(degradedP50 + 1, Int(Double(degradedP50) * 1.15)),
                    degradationRatio: 1.25
                ),
            ]
        )
    )
    return priorTable
}

private struct LocalMediaProcessorPriorContext {
    let machine: BenchmarkPriorMachine
    let signature: String
}

private func makeLocalMediaProcessorPriorContext(preset: String) throws -> LocalMediaProcessorPriorContext {
    let caps = WorkerCaps.detectLocal()
    let chipName = try #require(caps.chipName)
    let performanceCores = try #require(caps.performanceCores)
    let efficiencyCores = try #require(caps.efficiencyCores)
    let videoEncodeEngines = try #require(caps.videoEncodeEngines)
    let osVersion = WorkerSignatureBuilder.normalizedOS(ProcessInfo.processInfo.operatingSystemVersion)
    let signature = WorkerSignatureBuilder.make(
        chipName: chipName,
        performanceCores: performanceCores,
        efficiencyCores: efficiencyCores,
        videoEncodeEngines: videoEncodeEngines,
        preset: preset,
        osVersion: ProcessInfo.processInfo.operatingSystemVersion
    )
    let machine = BenchmarkPriorMachine(
        signature: signature,
        chipName: chipName,
        performanceCores: performanceCores,
        efficiencyCores: efficiencyCores,
        videoEncodeEngines: videoEncodeEngines,
        osVersion: osVersion,
        transcodePreset: preset,
        msPerFrameC1: 1.0,
        fixedOverheadMS: 200,
        avgCorpusFrameCount: 100,
        cells: [
            BenchmarkPriorCell(
                concurrency: 1,
                videosPerMin: 200,
                msPerVideoP50: 300,
                msPerVideoP95: 360,
                degradationRatio: 1.0
            ),
        ]
    )
    return LocalMediaProcessorPriorContext(machine: machine, signature: signature)
}
