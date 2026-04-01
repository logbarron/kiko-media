import Foundation
import Testing
@testable import KikoMediaCore

@Suite("CA local FIFO invariants")
struct CALocalFIFOInvariantTests {
    @Test("single local-only video is processed immediately")
    func singleJob_localOnly_processedImmediately() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "ca-local-fifo-single"
        try await seedQueuedVideo(env: env, id: uploadID, bytes: 1_000)

        let recorder = LocalOrderRecorder()
        let config = makeLocalConfig(env: env, tbWorkers: "")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-local-fifo-single"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.record(id)
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

        let completed = try await waitForCompletion(database: env.database, ids: [uploadID], timeoutSeconds: 4)
        #expect(completed)
        #expect(await recorder.order() == [uploadID])
    }

    @Test("multiple local-only jobs dequeue in arrival order")
    func multipleJobs_noTBWorkers_dequeuedInArrivalOrder() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let first = "ca-local-fifo-first"
        let second = "ca-local-fifo-second"
        let third = "ca-local-fifo-third"
        try await seedQueuedVideo(env: env, id: first, bytes: 512)
        try await seedQueuedVideo(env: env, id: second, bytes: 4_096)
        try await seedQueuedVideo(env: env, id: third, bytes: 256)

        let gate = AsyncGate()
        let recorder = LocalOrderRecorder()
        let config = makeLocalConfig(env: env, tbWorkers: "")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-local-fifo-multiple"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.record(id)
                if id == first {
                    await gate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        #expect(await processor.enqueue(
            uploadId: first,
            originalName: "\(first).mov",
            filePath: uploadPath(env: env, id: first),
            assetType: .video
        ))
        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await recorder.order().contains(first)
        }
        #expect(firstStarted)

        #expect(await processor.enqueue(
            uploadId: second,
            originalName: "\(second).mov",
            filePath: uploadPath(env: env, id: second),
            assetType: .video
        ))
        #expect(await processor.enqueue(
            uploadId: third,
            originalName: "\(third).mov",
            filePath: uploadPath(env: env, id: third),
            assetType: .video
        ))

        await gate.open()

        let completed = try await waitForCompletion(database: env.database, ids: [first, second, third], timeoutSeconds: 4)
        #expect(completed)
        #expect(await recorder.order() == [first, second, third])
    }

    @Test("multiple held local-only jobs reinsert in arrival order after wake")
    func multipleHeldJobs_reinsertInArrivalOrderAfterWake() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let first = "ca-local-held-first"
        let second = "ca-local-held-second"
        let third = "ca-local-held-third"
        try await seedQueuedVideo(env: env, id: first, bytes: 512)
        try await seedQueuedVideo(env: env, id: second, bytes: 4_096)
        try await seedQueuedVideo(env: env, id: third, bytes: 256)

        let firstGate = AsyncGate()
        let recorder = LocalOrderRecorder()
        let config = makeLocalConfig(env: env, tbWorkers: "")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-local-held-order"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: archiveSuccess,
            localVideoProcessingOverride: { id, _, _ in
                await recorder.record(id)
                if id == first {
                    await firstGate.wait()
                }
                return (thumb: true, preview: true, timestamp: "2026:03:01 10:00:00")
            }
        )

        let baseArrival = Date().timeIntervalSince1970
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: first,
                originalName: "\(first).mov",
                filePath: uploadPath(env: env, id: first),
                assetType: .video,
                arrivalAtSeconds: baseArrival
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: second,
                originalName: "\(second).mov",
                filePath: uploadPath(env: env, id: second),
                assetType: .video,
                arrivalAtSeconds: baseArrival + 1
            )
        )
        await processor.enqueueJob(
            ProcessingJob(
                uploadId: third,
                originalName: "\(third).mov",
                filePath: uploadPath(env: env, id: third),
                assetType: .video,
                arrivalAtSeconds: baseArrival + 2
            )
        )
        await processor.processQueues()

        let firstStarted = try await waitUntil(timeoutSeconds: 2) {
            await recorder.order().contains(first)
        }
        #expect(firstStarted)

        let wakeAt = Date().addingTimeInterval(0.2)
        let secondHold = MediaProcessor.VideoHoldMetadata(wakeAt: wakeAt, targetSlotID: "local#s1")
        let thirdHold = MediaProcessor.VideoHoldMetadata(wakeAt: wakeAt, targetSlotID: "local#s1")
        #expect(await processor.setVideoHold(uploadId: second, hold: secondHold))
        #expect(await processor.setVideoHold(uploadId: third, hold: thirdHold))

        let reinsertedInQueue = try await waitUntil(timeoutSeconds: 2) {
            let secondState = await processor.videoJobState(uploadId: second)
            let thirdState = await processor.videoJobState(uploadId: third)
            return secondState == .queued && thirdState == .queued
        }
        #expect(reinsertedInQueue)

        await firstGate.open()

        let completed = try await waitForCompletion(database: env.database, ids: [first, second, third], timeoutSeconds: 4)
        #expect(completed)
        #expect(await recorder.order() == [first, second, third])

        await processor.shutdown()
    }

    @Test("policy is FIFO when no TB workers are configured")
    func policyIsFIFO_whenNoTBWorkersConfigured() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeLocalConfig(env: env, tbWorkers: "")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-local-fifo-policy"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }

    @Test("policy is FIFO when prior data is missing")
    func policyIsFIFO_whenPriorDataMissing() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeLocalConfig(env: env, tbWorkers: "10.0.0.2:1")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-local-fifo-no-prior"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            complexityAwareSchedulingEnabled: true
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }
}

private let archiveSuccess: @Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult = { _, assetId, _ in
    .success(externalPath: "/tmp/\(assetId)", checksum: "test-checksum")
}

private func makeLocalConfig(env: TestEnv, tbWorkers: String) -> Config {
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
        maxConcurrentVideos: 1,
        videoTranscodePreset: env.config.videoTranscodePreset,
        tbWorkers: tbWorkers
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

private actor LocalOrderRecorder {
    private var values: [String] = []

    func record(_ uploadID: String) {
        values.append(uploadID)
    }

    func order() -> [String] {
        values
    }
}
