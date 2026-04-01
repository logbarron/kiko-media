import Foundation
import Testing
@testable import KikoMediaCore

@Suite("CA fallback policy")
struct CAFallbackPolicyTests {
    @Test("all remote slots down falls back to local processing")
    func allRemoteSlotsDown_fallsBackToLocalProcessing() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "ca-fallback-down-001"
        try await seedQueuedVideo(env: env, id: uploadID, bytes: 8_000)

        let attempts = AttemptRecorder()
        let recorder = EventRecorder()
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
            complexityAwareSchedulingEnabled: false,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                Task { await attempts.increment() }
                return nil
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let config = makeVideoConfig(env: env, tbWorkers: "127.0.0.1:1")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-fallback-down"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            thunderboltDispatcher: dispatcher,
            complexityAwareSchedulingEnabled: false,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath(env: env, id: uploadID),
            assetType: .video
        ))

        let completed = try await waitForCompletion(database: env.database, ids: [uploadID], timeoutSeconds: 6)
        #expect(completed)

        let hasRemoteAttempt = try await waitUntil(timeoutSeconds: 2) {
            await attempts.snapshot() >= 1
        }
        #expect(hasRemoteAttempt)
        #expect(await recorder.localOrder() == [uploadID])

        await dispatcher.shutdown()
    }

    @Test("permanent remote failure falls back to local processing")
    func permanentRemoteFailure_fallsBackToLocal() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "perm-fail-fallback-001"
        try await seedQueuedVideo(env: env, id: uploadID, bytes: 8_000)

        let attempts = AttemptRecorder()
        let recorder = EventRecorder()
        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6555,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                Task { await attempts.increment() }
                return nil
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil },
            onRetrySeed: { _ in 3 }
        )

        let config = makeVideoConfig(env: env, tbWorkers: "127.0.0.1:1")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-perm-fail"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            thunderboltDispatcher: dispatcher,
            complexityAwareSchedulingEnabled: false,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath(env: env, id: uploadID),
            assetType: .video
        ))

        let completed = try await waitForCompletion(database: env.database, ids: [uploadID], timeoutSeconds: 6)
        #expect(completed, "Video should complete via local fallback, not be marked failed")

        let hasRemoteAttempt = try await waitUntil(timeoutSeconds: 2) {
            await attempts.snapshot() >= 1
        }
        #expect(hasRemoteAttempt)
        #expect(await recorder.localOrder() == [uploadID])

        let asset = try await env.database.getAsset(id: uploadID)
        #expect(asset?.status == .complete)

        await dispatcher.shutdown()
    }

    @Test("remote dispatch nil return falls back to local with no queue corruption")
    func remoteDispatchNilReturn_fallsBackToLocal_noQueueCorruption() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "ca-fallback-nil-first"
        let secondID = "ca-fallback-nil-second"
        try await seedQueuedVideo(env: env, id: firstID, bytes: 8_000)
        try await seedQueuedVideo(env: env, id: secondID, bytes: 4_000)

        let recorder = EventRecorder()
        let config = makeVideoConfig(env: env, tbWorkers: "10.0.0.2:1")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-fallback-nil"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            remoteVideoDispatchOverride: { id, _, _, _ in
                await recorder.recordRemote(id)
                return nil
            },
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.enqueue(
            uploadId: firstID,
            originalName: "\(firstID).mov",
            filePath: uploadPath(env: env, id: firstID),
            assetType: .video
        ))
        #expect(await processor.enqueue(
            uploadId: secondID,
            originalName: "\(secondID).mov",
            filePath: uploadPath(env: env, id: secondID),
            assetType: .video
        ))

        let completed = try await waitForCompletion(database: env.database, ids: [firstID, secondID], timeoutSeconds: 4)
        #expect(completed)

        let events = await recorder.events()
        let remoteFirst = events.firstIndex(of: "remote:\(firstID)")
        let localFirst = events.firstIndex(of: "local:\(firstID)")
        #expect(remoteFirst != nil)
        #expect(localFirst != nil)
        if let remoteFirst, let localFirst {
            #expect(remoteFirst < localFirst)
        }
        #expect(events.filter { $0 == "local:\(firstID)" }.count == 1)
        #expect(events.filter { $0 == "local:\(secondID)" }.count == 1)
        #expect(events.filter { $0.hasPrefix("local:") }.count == 2)
    }

    @Test("terminal remote result skips local fallback")
    func terminalRemoteResult_skipsLocalFallback() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "ca-fallback-terminal-001"
        try await seedQueuedVideo(env: env, id: uploadID, bytes: 8_000)

        let recorder = EventRecorder()
        let config = makeVideoConfig(env: env, tbWorkers: "10.0.0.2:1")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-fallback-terminal"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            remoteVideoDispatchOverride: { id, _, _, _ in
                await recorder.recordRemote(id)
                return (thumb: false, preview: false)
            },
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath(env: env, id: uploadID),
            assetType: .video
        ))

        let failed = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: uploadID)
            return asset?.status == .failed
        }
        #expect(failed)

        let events = await recorder.events()
        #expect(events.contains("remote:\(uploadID)"))
        #expect(!events.contains("local:\(uploadID)"))
    }

    @Test("scheduler policy none ignores configured dispatcher and stays local")
    func schedulerPolicyNone_ignoresConfiguredDispatcherAndStaysLocal() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "ca-fallback-policy-none-local"
        try await seedQueuedVideo(env: env, id: uploadID, bytes: 8_000)

        let attempts = AttemptRecorder()
        let recorder = EventRecorder()
        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6556,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: false,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                Task { await attempts.increment() }
                return nil
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let config = makeVideoConfig(
            env: env,
            tbWorkers: "127.0.0.1:1",
            videoSchedulerPolicy: .none
        )
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-policy-none-local"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            thunderboltDispatcher: dispatcher,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath(env: env, id: uploadID),
            assetType: .video
        ))

        let completed = try await waitForCompletion(database: env.database, ids: [uploadID], timeoutSeconds: 6)
        #expect(completed)
        #expect(await recorder.localOrder() == [uploadID])
        #expect(await attempts.snapshot() == 0)

        await dispatcher.shutdown()
    }

    @Test("frameCount nil uses fallback duration times 24")
    func frameCountNil_usesFallbackDurationTimes24() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let slowID = "ca-fallback-framecount-slow"
        let fastID = "ca-fallback-framecount-fast"
        try await seedQueuedVideo(env: env, id: slowID, bytes: 3_000)
        try await seedQueuedVideo(env: env, id: fastID, bytes: 3_000)

        let recorder = EventRecorder()
        let config = makeVideoConfig(env: env, tbWorkers: "127.0.0.1:1")
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-fallback-framecount"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: slowID,
                originalName: "\(slowID).mov",
                filePath: uploadPath(env: env, id: slowID),
                assetType: .video,
                estimatedVideoRuntimeSeconds: nil,
                frameCount: nil,
                probedDurationSeconds: 120,
                videoEstimateConfidence: .low
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: fastID,
                originalName: "\(fastID).mov",
                filePath: uploadPath(env: env, id: fastID),
                assetType: .video,
                estimatedVideoRuntimeSeconds: nil,
                frameCount: nil,
                probedDurationSeconds: 10,
                videoEstimateConfidence: .low
            )
        )
        await processor.processQueues()

        let completed = try await waitForCompletion(database: env.database, ids: [slowID, fastID], timeoutSeconds: 4)
        #expect(completed)
        #expect(await recorder.localOrder() == [fastID, slowID])
    }

    @Test("frameCount and duration nil ignore runtime estimate for CA fallback ordering")
    func frameCountAndDurationNil_ignoreRuntimeEstimateForCAFallbackOrdering() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let firstID = "ca-fallback-both-nil-first"
        let secondID = "ca-fallback-both-nil-second"
        try await seedQueuedVideo(env: env, id: firstID, bytes: 3_000)
        try await seedQueuedVideo(env: env, id: secondID, bytes: 3_000)

        let recorder = EventRecorder()
        let config = makeVideoConfig(env: env, tbWorkers: "127.0.0.1:1")
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-fallback-both-nil"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)

        await processor.enqueueJob(
            ProcessingJob(
                uploadId: firstID,
                originalName: "\(firstID).mov",
                filePath: uploadPath(env: env, id: firstID),
                assetType: .video,
                arrivalAtSeconds: 0,
                estimatedVideoRuntimeSeconds: 120,
                frameCount: nil,
                probedDurationSeconds: nil,
                videoEstimateConfidence: .low
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: secondID,
                originalName: "\(secondID).mov",
                filePath: uploadPath(env: env, id: secondID),
                assetType: .video,
                arrivalAtSeconds: 0,
                estimatedVideoRuntimeSeconds: 10,
                frameCount: nil,
                probedDurationSeconds: nil,
                videoEstimateConfidence: .low
            )
        )
        await processor.processQueues()

        let completed = try await waitForCompletion(database: env.database, ids: [firstID, secondID], timeoutSeconds: 4)
        #expect(completed)
        #expect(await recorder.localOrder() == [firstID, secondID])
    }

    @Test("prior missing keeps FIFO policy and skips CA scoring")
    func priorMissing_policyStaysFIFO_noCAScoring() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let blockerID = "ca-fallback-prior-missing-blocker"
        let largeID = "ca-fallback-prior-missing-large"
        let smallID = "ca-fallback-prior-missing-small"
        try await seedQueuedVideo(env: env, id: blockerID, bytes: 512)
        try await seedQueuedVideo(env: env, id: largeID, bytes: 64_000)
        try await seedQueuedVideo(env: env, id: smallID, bytes: 256)

        let gate = AsyncGate()
        let recorder = EventRecorder()
        let config = makeVideoConfig(env: env, tbWorkers: "10.0.0.2:1")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-fallback-prior-missing"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            complexityAwareSchedulingEnabled: true,
            remoteVideoDispatchOverride: { id, _, _, _ in
                await recorder.recordRemote(id)
                return nil
            },
            localVideoProcessingOverride: { id, _, _ in
                await recorder.recordLocal(id)
                if id == blockerID {
                    await gate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.videoSelectionPolicy == .fifo)

        #expect(await processor.enqueue(
            uploadId: blockerID,
            originalName: "\(blockerID).mov",
            filePath: uploadPath(env: env, id: blockerID),
            assetType: .video
        ))
        let blockerStarted = try await waitUntil(timeoutSeconds: 2) {
            await recorder.localOrder().contains(blockerID)
        }
        #expect(blockerStarted)

        #expect(await processor.enqueue(
            uploadId: largeID,
            originalName: "\(largeID).mov",
            filePath: uploadPath(env: env, id: largeID),
            assetType: .video
        ))
        #expect(await processor.enqueue(
            uploadId: smallID,
            originalName: "\(smallID).mov",
            filePath: uploadPath(env: env, id: smallID),
            assetType: .video
        ))

        await gate.open()

        let completed = try await waitForCompletion(database: env.database, ids: [blockerID, largeID, smallID], timeoutSeconds: 4)
        #expect(completed)
        #expect(await recorder.localOrder() == [blockerID, largeID, smallID])
    }
}

private let archiveSuccess: @Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult = { _, assetId, _ in
    .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
}

private func makeVideoConfig(
    env: TestEnv,
    tbWorkers: String,
    videoSchedulerPolicy: VideoSchedulerPolicy = .auto
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
        healthCheckInterval: env.config.healthCheckInterval,
        jsonMaxBodyBytes: env.config.jsonMaxBodyBytes,
        webhookRetryAfterSeconds: env.config.webhookRetryAfterSeconds,
        turnstileSecret: env.config.turnstileSecret,
        sessionHmacSecret: env.config.sessionHmacSecret,
        gateSecret: env.config.gateSecret,
        maxConcurrentImages: env.config.maxConcurrentImages,
        maxConcurrentVideos: 1,
        videoSchedulerPolicy: videoSchedulerPolicy,
        maxConcurrentRebuildProbes: env.config.maxConcurrentRebuildProbes,
        thumbnailSize: env.config.thumbnailSize,
        thumbnailQuality: env.config.thumbnailQuality,
        previewSize: env.config.previewSize,
        previewQuality: env.config.previewQuality,
        videoThumbnailSize: env.config.videoThumbnailSize,
        videoThumbnailTime: env.config.videoThumbnailTime,
        videoThumbnailQuality: env.config.videoThumbnailQuality,
        videoTranscodeTimeout: env.config.videoTranscodeTimeout,
        videoTranscodePreset: env.config.videoTranscodePreset,
        tbWorkers: tbWorkers,
        tbPort: env.config.tbPort,
        tbConnectTimeout: env.config.tbConnectTimeout,
        maxImagePixels: env.config.maxImagePixels,
        maxImageDimension: env.config.maxImageDimension,
        maxCompressionRatio: env.config.maxCompressionRatio,
        sqliteBusyTimeout: env.config.sqliteBusyTimeout,
        sqliteCacheSize: env.config.sqliteCacheSize,
        defaultPageSize: env.config.defaultPageSize,
        maxPageSize: env.config.maxPageSize,
        maxPageOffset: env.config.maxPageOffset,
        sqlBatchSize: env.config.sqlBatchSize,
        sessionCookieTTL: env.config.sessionCookieTTL,
        sessionCookieName: env.config.sessionCookieName,
        turnstileVerifyTimeout: env.config.turnstileVerifyTimeout,
        turnstileMaxResponse: env.config.turnstileMaxResponse,
        turnstileMaxInFlightVerifications: env.config.turnstileMaxInFlightVerifications,
        turnstileOverloadRetryAfterSeconds: env.config.turnstileOverloadRetryAfterSeconds,
        turnstileExpectedHostname: env.config.turnstileExpectedHostname,
        turnstileExpectedAction: env.config.turnstileExpectedAction,
        turnstileExpectedCData: env.config.turnstileExpectedCData,
        cacheControl: env.config.cacheControl,
        eventTimezone: env.config.eventTimezone,
        maxPendingWebhookJobs: env.config.maxPendingWebhookJobs,
        queueCompactionThreshold: env.config.queueCompactionThreshold,
        sha256BufferSize: env.config.sha256BufferSize
    )
}

private func seedQueuedVideo(env: TestEnv, id: String, bytes: Int) async throws {
    _ = try await env.database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
    let data = Data(repeating: 0x61, count: max(bytes, 1))
    try data.write(to: URL(fileURLWithPath: uploadPath(env: env, id: id)))
}

private func uploadPath(env: TestEnv, id: String) -> String {
    "\(env.uploadDir)/\(id)"
}

private func waitForCompletion(
    database: Database,
    ids: [String],
    timeoutSeconds: TimeInterval
) async throws -> Bool {
    try await waitUntil(timeoutSeconds: timeoutSeconds) {
        for id in ids {
            let asset = try await database.getAsset(id: id)
            if asset?.status != .complete {
                return false
            }
        }
        return true
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

private actor AsyncGate {
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

private actor EventRecorder {
    private var orderedLocals: [String] = []
    private var allEvents: [String] = []

    func recordRemote(_ uploadId: String) {
        allEvents.append("remote:\(uploadId)")
    }

    func recordLocal(_ uploadId: String) {
        orderedLocals.append(uploadId)
        allEvents.append("local:\(uploadId)")
    }

    func localOrder() -> [String] {
        orderedLocals
    }

    func events() -> [String] {
        allEvents
    }
}

private actor AttemptRecorder {
    private var count = 0

    func increment() {
        count += 1
    }

    func snapshot() -> Int {
        count
    }
}
