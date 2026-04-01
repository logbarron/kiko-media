import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("CA resolved video cost", Testing.ParallelizationTrait.serialized)
struct CAResolvedVideoCostTests {
    @Test("normal enqueue carries one resolved video cost through solver dispatch and benchmark runtime")
    func normalEnqueueCarriesOneResolvedVideoCostThroughSolverDispatchAndBenchmarkRuntime() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "ws1-normal-001"
        let originalName = "\(uploadID).mov"
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data(repeating: 0x61, count: 2_048).write(to: URL(fileURLWithPath: uploadPath), options: .atomic)
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: originalName)

        let config = makeWS1CAConfig(env: env)
        let benchmarkContext = try makeWS1BenchmarkContext(preset: config.videoTranscodePreset)
        let priorTable = makeWS1LocalPriorTable(
            context: benchmarkContext,
            preset: config.videoTranscodePreset,
            msPerFrameC1: 2.0,
            fixedOverheadMS: 1_000
        )
        let gate = WS1BlockingGate()
        let dispatchRecorder = WS1DispatchRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ws1-normal"))
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
                await gate.wait()
                return (thumb: true, preview: true, timestamp: "2026:03:25 10:00:00")
            }
        )

        let accepted = await MediaProcessor.withVideoDispatchObserver(
            { uploadId, frameCount, videoCost in
                Task {
                    await dispatchRecorder.record(
                        uploadId: uploadId,
                        frameCount: frameCount,
                        videoCost: videoCost
                    )
                }
            },
            operation: {
                await processor.enqueue(
                    uploadId: uploadID,
                    originalName: originalName,
                    filePath: uploadPath,
                    assetType: .video
                )
            }
        )
        #expect(accepted)

        let sawJob = try await waitUntil(timeoutSeconds: 4) {
            await processor.processingJobSnapshot(uploadId: uploadID) != nil
        }
        #expect(sawJob)

        let productionJob = try #require(await processor.processingJobSnapshot(uploadId: uploadID))
        let queueCost = try #require(productionJob.resolvedVideoCost)
        let expectedCost = CAProfileAndFallbackMath.resolveVideoCost(
            frameCount: nil,
            durationSeconds: nil,
            runtimeSeconds: nil,
            confidence: .low,
            runtimeSourceWhenPresent: .probeEstimate,
            localMSPerFrameC1: 2.0,
            localFixedOverheadMS: 1_000
        )

        #expect(queueCost == expectedCost)
        #expect(queueCost.derivation.frameCountSource == .defaultFallback)
        #expect(queueCost.derivation.runtimeSource == .modeledFromFrameCount)

        let solverJob = MediaProcessor.makeComplexityAwareJob(productionJob, enqueueOrder: 0)
        #expect(solverJob.frameCount == queueCost.frameCount)
        #expect(MediaProcessor.dispatchFrameCount(for: productionJob) == queueCost.frameCount)

        let sawDispatch = try await waitUntil(timeoutSeconds: 2) {
            await dispatchRecorder.observation(uploadId: uploadID) != nil
        }
        #expect(sawDispatch)

        let dispatchObservation = try #require(await dispatchRecorder.observation(uploadId: uploadID))
        #expect(dispatchObservation.frameCount == queueCost.frameCount)
        #expect(dispatchObservation.videoCost == queueCost)

        let benchmarkVideo = MediaFile(
            path: uploadPath,
            name: originalName,
            type: .video,
            sizeBytes: 2_048
        )
        let setup = try await prepareThunderboltCARunSetup(
            corpus: [benchmarkVideo],
            preset: config.videoTranscodePreset,
            hardware: benchmarkContext.hardware,
            slotOverrides: ThunderboltCASlotOverrides(localSlots: 1, remoteSlotsByHost: [:]),
            mode: .strict,
            priorTableOverride: priorTable,
            settingsOverride: makeWS1BenchmarkSettings()
        )
        let benchmarkCost = try #require(setup.videoCosts.first)
        #expect(benchmarkCost == queueCost)

        let observed = try await runThunderboltCA(
            corpus: [benchmarkVideo],
            preset: config.videoTranscodePreset,
            timeout: 1,
            hardware: benchmarkContext.hardware,
            policy: .complexityAware,
            profile: .allAtOnce,
            preparedSetup: setup,
            localVideoRunner: { _, _, _, _, _, _ in true }
        )
        let benchmarkJob = try #require(observed.result.jobs.first)
        #expect(benchmarkJob.frameCount == queueCost.frameCount)
        #expect(benchmarkJob.frameCount == dispatchObservation.frameCount)

        await gate.open()
        let completed = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: uploadID)
            return asset?.status == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("recovery enqueue uses the same canonical queue-time resolver as normal enqueue")
    func recoveryEnqueueUsesTheSameCanonicalQueueTimeResolverAsNormalEnqueue() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let normalID = "ws1-recovery-normal"
        let recoveryID = "ws1-recovery-recovered"
        let normalPath = "\(env.uploadDir)/\(normalID)"
        let recoveryPath = "\(env.uploadDir)/\(recoveryID)"
        try Data(repeating: 0x62, count: 1_024).write(to: URL(fileURLWithPath: normalPath), options: .atomic)
        try Data(repeating: 0x62, count: 1_024).write(to: URL(fileURLWithPath: recoveryPath), options: .atomic)
        _ = try await env.database.insertQueued(id: normalID, type: .video, originalName: "\(normalID).mov")
        _ = try await env.database.insertQueued(id: recoveryID, type: .video, originalName: "\(recoveryID).mov")

        let config = makeWS1CAConfig(env: env)
        let benchmarkContext = try makeWS1BenchmarkContext(preset: config.videoTranscodePreset)
        let priorTable = makeWS1LocalPriorTable(
            context: benchmarkContext,
            preset: config.videoTranscodePreset,
            msPerFrameC1: 2.0,
            fixedOverheadMS: 1_000
        )
        let gate = WS1BlockingGate()
        let dispatchRecorder = WS1DispatchRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ws1-recovery"))
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
                await gate.wait()
                return (thumb: true, preview: true, timestamp: "2026:03:25 10:00:00")
            }
        )

        let accepted = await MediaProcessor.withVideoDispatchObserver(
            { uploadId, frameCount, videoCost in
                Task {
                    await dispatchRecorder.record(
                        uploadId: uploadId,
                        frameCount: frameCount,
                        videoCost: videoCost
                    )
                }
            },
            operation: {
                await processor.enqueue(
                    uploadId: normalID,
                    originalName: "\(normalID).mov",
                    filePath: normalPath,
                    assetType: .video
                )
            }
        )
        #expect(accepted)

        let normalDispatched = try await waitUntil(timeoutSeconds: 2) {
            await dispatchRecorder.observation(uploadId: normalID) != nil
        }
        #expect(normalDispatched)

        await processor.enqueueRecoveryJob(
            uploadId: recoveryID,
            originalName: "\(recoveryID).mov",
            filePath: recoveryPath,
            assetType: .video
        )

        let sawRecoveryJob = try await waitUntil(timeoutSeconds: 2) {
            await processor.processingJobSnapshot(uploadId: recoveryID) != nil
        }
        #expect(sawRecoveryJob)

        let normalCost = try #require(await processor.processingJobSnapshot(uploadId: normalID)?.resolvedVideoCost)
        let recoveryCost = try #require(await processor.processingJobSnapshot(uploadId: recoveryID)?.resolvedVideoCost)
        let expectedCost = CAProfileAndFallbackMath.resolveVideoCost(
            frameCount: nil,
            durationSeconds: nil,
            runtimeSeconds: nil,
            confidence: .low,
            runtimeSourceWhenPresent: .probeEstimate,
            localMSPerFrameC1: 2.0,
            localFixedOverheadMS: 1_000
        )

        #expect(normalCost == expectedCost)
        #expect(recoveryCost == expectedCost)
        #expect(recoveryCost == normalCost)

        await gate.open()
        let completed = try await waitUntil(timeoutSeconds: 4) {
            let normalAsset = try await env.database.getAsset(id: normalID)
            let recoveryAsset = try await env.database.getAsset(id: recoveryID)
            return normalAsset?.status == .complete && recoveryAsset?.status == .complete
        }
        #expect(completed)

        await processor.shutdown()
    }

    @Test("extensionless successful probes carry the same MIME-aware resolved video cost through production and benchmark")
    func extensionlessSuccessfulProbesCarryTheSameMIMEAwareResolvedVideoCostThroughProductionAndBenchmark() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "ws1-extensionless-001"
        let originalName = "\(uploadID).mp4"
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try FileManager.default.copyItem(
            at: try ws1FixtureVideoURL(),
            to: URL(fileURLWithPath: uploadPath)
        )
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: originalName)

        let config = makeWS1CAConfig(env: env)
        let benchmarkContext = try makeWS1BenchmarkContext(preset: config.videoTranscodePreset)
        let priorTable = makeWS1LocalPriorTable(
            context: benchmarkContext,
            preset: config.videoTranscodePreset,
            msPerFrameC1: 2.0,
            fixedOverheadMS: 1_000
        )
        let gate = WS1BlockingGate()
        let dispatchRecorder = WS1DispatchRecorder()
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ws1-extensionless"))
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
                await gate.wait()
                return (thumb: true, preview: true, timestamp: "2026:03:25 10:00:00")
            }
        )
        try await MediaProcessor.$metadataProbeTimeoutOverrideNanos.withValue(30_000_000_000) {
            let accepted = await MediaProcessor.withVideoDispatchObserver(
                { uploadId, frameCount, videoCost in
                    Task {
                        await dispatchRecorder.record(
                            uploadId: uploadId,
                            frameCount: frameCount,
                            videoCost: videoCost
                        )
                    }
                },
                operation: {
                    await processor.enqueue(
                        uploadId: uploadID,
                        originalName: originalName,
                        filePath: uploadPath,
                        assetType: .video
                    )
                }
            )
            #expect(accepted)

            let sawJob = try await waitUntil(timeoutSeconds: 4) {
                await processor.processingJobSnapshot(uploadId: uploadID) != nil
            }
            #expect(sawJob)

            let productionJob = try #require(await processor.processingJobSnapshot(uploadId: uploadID))
            let queueCost = try #require(productionJob.resolvedVideoCost)
            #expect(queueCost.confidence == .high)
            #expect(queueCost.derivation.frameCountSource == .measuredFrameCount)
            #expect(queueCost.derivation.durationSource == .measuredDuration)
            #expect(queueCost.derivation.runtimeSource == .probeEstimate)

            let solverJob = MediaProcessor.makeComplexityAwareJob(productionJob, enqueueOrder: 0)
            #expect(solverJob.frameCount == queueCost.frameCount)
            #expect(MediaProcessor.dispatchFrameCount(for: productionJob) == queueCost.frameCount)

            let sawDispatch = try await waitUntil(timeoutSeconds: 4) {
                await dispatchRecorder.observation(uploadId: uploadID) != nil
            }
            #expect(sawDispatch)

            let dispatchObservation = try #require(await dispatchRecorder.observation(uploadId: uploadID))
            #expect(dispatchObservation.frameCount == queueCost.frameCount)
            #expect(dispatchObservation.videoCost == queueCost)

            let benchmarkVideoSize = (try FileManager.default.attributesOfItem(atPath: uploadPath)[.size] as? NSNumber)?.intValue ?? 0
            let benchmarkVideo = MediaFile(
                path: uploadPath,
                name: originalName,
                type: .video,
                sizeBytes: benchmarkVideoSize
            )
            let setup = try await prepareThunderboltCARunSetup(
                corpus: [benchmarkVideo],
                preset: config.videoTranscodePreset,
                hardware: benchmarkContext.hardware,
                slotOverrides: ThunderboltCASlotOverrides(localSlots: 1, remoteSlotsByHost: [:]),
                mode: .strict,
                priorTableOverride: priorTable,
                settingsOverride: makeWS1BenchmarkSettings()
            )
            let benchmarkCost = try #require(setup.videoCosts.first)
            #expect(benchmarkCost == queueCost)

            let observed = try await runThunderboltCA(
                corpus: [benchmarkVideo],
                preset: config.videoTranscodePreset,
                timeout: 1,
                hardware: benchmarkContext.hardware,
                policy: .complexityAware,
                profile: .allAtOnce,
                preparedSetup: setup,
                localVideoRunner: { _, _, _, _, _, _ in true }
            )
            let benchmarkJob = try #require(observed.result.jobs.first)
            #expect(benchmarkJob.frameCount == queueCost.frameCount)
            #expect(benchmarkJob.frameCount == dispatchObservation.frameCount)

            await gate.open()
            let completed = try await waitUntil(timeoutSeconds: 4) {
                let asset = try await env.database.getAsset(id: uploadID)
                return asset?.status == .complete
            }
            #expect(completed)

            await processor.shutdown()
        }
    }

    @Test("fallback frames come only from true content duration or true frame count inputs")
    func fallbackFramesComeOnlyFromTrueContentDurationOrTrueFrameCountInputs() {
        let durationFallback = CAProfileAndFallbackMath.resolveVideoCost(
            frameCount: nil,
            durationSeconds: 15,
            runtimeSeconds: 999,
            confidence: .low,
            runtimeSourceWhenPresent: .estimatedProcessingRuntime,
            localMSPerFrameC1: 2.0
        )
        let measuredFrameCount = CAProfileAndFallbackMath.resolveVideoCost(
            frameCount: 333,
            durationSeconds: nil,
            runtimeSeconds: nil,
            confidence: .high,
            runtimeSourceWhenPresent: .probeEstimate,
            localMSPerFrameC1: 2.0
        )

        #expect(durationFallback.frameCount == CAProfileAndFallbackMath.fallbackFrameCount(durationSeconds: 15, frameCount: nil))
        #expect(durationFallback.durationSeconds == 15)
        #expect(durationFallback.runtimeSeconds == 999)
        #expect(durationFallback.derivation.frameCountSource == .contentDurationFallback)
        #expect(durationFallback.derivation.durationSource == .measuredDuration)

        #expect(measuredFrameCount.frameCount == 333)
        #expect(measuredFrameCount.derivation.frameCountSource == .measuredFrameCount)
    }

    @Test("estimated processing runtime is never treated as video duration")
    func estimatedProcessingRuntimeIsNeverTreatedAsVideoDuration() {
        let shortRuntime = CAProfileAndFallbackMath.resolveVideoCost(
            frameCount: nil,
            durationSeconds: nil,
            runtimeSeconds: 10,
            confidence: .low,
            runtimeSourceWhenPresent: .estimatedProcessingRuntime,
            localMSPerFrameC1: nil
        )
        let longRuntime = CAProfileAndFallbackMath.resolveVideoCost(
            frameCount: nil,
            durationSeconds: nil,
            runtimeSeconds: 120,
            confidence: .low,
            runtimeSourceWhenPresent: .estimatedProcessingRuntime,
            localMSPerFrameC1: nil
        )

        let defaultFrameCount = CAProfileAndFallbackMath.fallbackFrameCount(
            durationSeconds: nil,
            frameCount: nil
        )

        #expect(shortRuntime.frameCount == defaultFrameCount)
        #expect(longRuntime.frameCount == defaultFrameCount)
        #expect(shortRuntime.frameCount == longRuntime.frameCount)
        #expect(shortRuntime.durationSeconds == nil)
        #expect(longRuntime.durationSeconds == nil)
        #expect(shortRuntime.runtimeSeconds == 10)
        #expect(longRuntime.runtimeSeconds == 120)
        #expect(shortRuntime.derivation.frameCountSource == .defaultFallback)
        #expect(longRuntime.derivation.frameCountSource == .defaultFallback)
        #expect(shortRuntime.derivation.durationSource == .missing)
        #expect(longRuntime.derivation.durationSource == .missing)
        #expect(shortRuntime.derivation.runtimeSource == .estimatedProcessingRuntime)
        #expect(longRuntime.derivation.runtimeSource == .estimatedProcessingRuntime)
    }
}

private actor WS1BlockingGate {
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
        let pendingWaiters = waiters
        waiters.removeAll(keepingCapacity: false)
        for waiter in pendingWaiters {
            waiter.resume()
        }
    }
}

private struct WS1DispatchObservation: Equatable {
    let frameCount: Double
    let videoCost: CAResolvedVideoCost
}

private actor WS1DispatchRecorder {
    private var observations: [String: WS1DispatchObservation] = [:]

    func record(uploadId: String, frameCount: Double, videoCost: CAResolvedVideoCost) {
        observations[uploadId] = WS1DispatchObservation(
            frameCount: frameCount,
            videoCost: videoCost
        )
    }

    func observation(uploadId: String) -> WS1DispatchObservation? {
        observations[uploadId]
    }
}

private struct WS1BenchmarkContext {
    let hardware: HardwareProfile
    let signature: String
}

private func makeWS1BenchmarkContext(preset: String) throws -> WS1BenchmarkContext {
    let caps = WorkerCaps.detectLocal()
    let chipName = try #require(caps.chipName)
    let performanceCores = try #require(caps.performanceCores)
    let efficiencyCores = try #require(caps.efficiencyCores)
    let videoEncodeEngines = try #require(caps.videoEncodeEngines)
    let hardware = HardwareProfile(
        chipName: chipName,
        performanceCores: performanceCores,
        efficiencyCores: efficiencyCores,
        totalCores: max(1, performanceCores + efficiencyCores),
        memoryGB: 16,
        videoEncodeEngines: videoEncodeEngines,
        hwEncoderNames: []
    )
    let signature = WorkerSignatureBuilder.make(
        chipName: chipName,
        performanceCores: performanceCores,
        efficiencyCores: efficiencyCores,
        videoEncodeEngines: videoEncodeEngines,
        preset: preset,
        osVersion: ProcessInfo.processInfo.operatingSystemVersion
    )
    return WS1BenchmarkContext(hardware: hardware, signature: signature)
}

private func makeWS1LocalPriorTable(
    context: WS1BenchmarkContext,
    preset: String,
    msPerFrameC1: Double,
    fixedOverheadMS: Double
) -> BenchmarkPriorTable {
    BenchmarkPriorTable(
        machines: [
            BenchmarkPriorMachine(
                signature: context.signature,
                chipName: context.hardware.chipName,
                performanceCores: context.hardware.performanceCores,
                efficiencyCores: context.hardware.efficiencyCores,
                videoEncodeEngines: context.hardware.videoEncodeEngines,
                osVersion: WorkerSignatureBuilder.normalizedOS(ProcessInfo.processInfo.operatingSystemVersion),
                transcodePreset: preset,
                msPerFrameC1: msPerFrameC1,
                fixedOverheadMS: fixedOverheadMS,
                avgCorpusFrameCount: 1_440,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 60,
                        msPerVideoP50: 4_000,
                        msPerVideoP95: 4_500,
                        degradationRatio: 1.0
                    ),
                ]
            ),
        ]
    )
}

private func makeWS1CAConfig(env: TestEnv) -> Config {
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
        tbWorkers: "127.0.0.1:1"
    )
}

private func makeWS1BenchmarkSettings() -> ThunderboltSettingsResolution {
    ThunderboltSettingsResolution(
        workersRaw: nil,
        workersSource: "test",
        port: 7_000,
        portSource: "test",
        connectTimeout: 100,
        connectTimeoutSource: "test",
        warnings: []
    )
}

private func ws1FixtureVideoURL() throws -> URL {
    let testFileURL = URL(fileURLWithPath: #filePath)
    let repoRoot = testFileURL
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let fixtureURL = repoRoot.appendingPathComponent(
        "docs/tui/2026-03-20_kiko-media_03-demo-tui-orchestrator-help.mp4"
    )
    guard FileManager.default.fileExists(atPath: fixtureURL.path) else {
        throw NSError(
            domain: "CAResolvedVideoCostTests",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "Missing WS1 fixture video at \(fixtureURL.path)"]
        )
    }
    return fixtureURL
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
