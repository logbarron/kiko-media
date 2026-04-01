import CryptoKit
import Darwin
import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Thunderbolt dispatcher tail telemetry", .serialized)
struct ThunderboltDispatcherTailTelemetryTests {
    @Test("shared transfer overhead learner seeds, degrades, and recovers behaviorally")
    func sharedTransferOverheadLearner_seedsDegradesAndRecoversBehaviorally() {
        let seeded = TransferOverheadEstimator.next(
            previousBaseline: nil,
            previousEstimate: nil,
            sample: 100
        )
        #expect(seeded.baseline == 100)
        #expect(seeded.estimate == 100)

        let belowThreshold = TransferOverheadEstimator.next(
            previousBaseline: 100,
            previousEstimate: 100,
            sample: 124.9
        )
        #expect(belowThreshold.baseline > 100)
        #expect(belowThreshold.baseline < 124.9)
        #expect(belowThreshold.estimate > 100)
        #expect(belowThreshold.estimate < 124.9)

        let degraded = TransferOverheadEstimator.next(
            previousBaseline: 100,
            previousEstimate: 100,
            sample: 125
        )
        #expect(degraded.baseline > 100)
        #expect(degraded.baseline < 125)
        #expect(degraded.estimate > belowThreshold.estimate)
        #expect(degraded.estimate > degraded.baseline)

        let recovered = TransferOverheadEstimator.next(
            previousBaseline: 110,
            previousEstimate: 130,
            sample: 90
        )
        #expect(recovered.baseline < 110)
        #expect(recovered.baseline > 90)
        #expect(recovered.estimate < 130)
        #expect(recovered.estimate > recovered.baseline)
    }

    @Test("shared transfer overhead reducer accepts zero and rejects invalid positive-only samples")
    func sharedTransferOverheadReducer_acceptsZeroAndRejectsInvalidPositiveOnlySamples() throws {
        let seeded = try #require(
            TransferOverheadEstimator.reduce(
                previousBaseline: nil,
                previousEstimate: nil,
                sampleMS: UInt32(0)
            )
        )
        #expect(seeded.baseline == 0)
        #expect(seeded.estimate == 0)

        let reduced = try #require(
            TransferOverheadEstimator.reduce(
                previousBaseline: 10,
                previousEstimate: 15,
                sampleMS: UInt32(0)
            )
        )
        #expect(reduced.baseline >= 0)
        #expect(reduced.baseline < 10)
        #expect(reduced.estimate >= 0)
        #expect(reduced.estimate < 15)

        #expect(
            TransferOverheadEstimator.reduce(
                previousBaseline: 10,
                previousEstimate: 15,
                sampleMS: Double.nan
            ) == nil
        )
        #expect(
            TransferOverheadEstimator.reduce(
                previousBaseline: 10,
                previousEstimate: 15,
                sampleMS: -1.0
            ) == nil
        )
        #expect(
            TransferOverheadEstimator.reducePositive(
                previousBaseline: 10,
                previousEstimate: 15,
                sampleMS: 0
            ) == nil
        )
    }

    @Test("captures txOut and publishOverhead samples on success")
    func capturesTailTelemetrySamplesOnSuccess() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "tb-tail-telemetry-capture-001"
        let uploadPath = try writeUpload(env: env, uploadID: uploadID)

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(values: [
            900_000_000, // slot dispatch start
            1_000_000_000, // dispatch sent
            1_050_000_000, // first running tick observed
            2_000_000_000, // complete/tick boundary
            2_050_000_000, // payload read completion
            2_054_000_000 // local validation completion
        ])

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6560,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }
            guard let request = readDispatchRequest(fd: pair.serverFD),
                  let context = parseTickContext(fromWorkerMime: request.mime) else {
                return
            }
            sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: context)
        }

        let result = await dispatcher.dispatch(
            uploadId: uploadID,
            filePath: uploadPath,
            originalName: "\(uploadID).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )

        #expect(result == .success)
        let snapshot = await dispatcher.complexityAwareSnapshot()
        let worker = try #require(snapshot.first)
        let txOutEstimate = try #require(worker.txOutEstimateMS)
        let publishEstimate = try #require(worker.publishOverheadEstimateMS)
        #expect(txOutEstimate.isFinite)
        #expect(publishEstimate.isFinite)
        #expect(txOutEstimate > 0)
        #expect(publishEstimate > 0)
        #expect(txOutEstimate > publishEstimate)

        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("updates tail telemetry EMA using successive successful samples")
    func updatesTailTelemetryEMAAfterMultipleSuccesses() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID1 = "tb-tail-telemetry-ema-001"
        let uploadID2 = "tb-tail-telemetry-ema-002"
        let uploadPath1 = try writeUpload(env: env, uploadID: uploadID1)
        let uploadPath2 = try writeUpload(env: env, uploadID: uploadID2)

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(values: [
            900_000_000, // dispatch1 slot dispatch start
            1_000_000_000, // dispatch1 sent
            1_000_000_000, // dispatch1 first running tick observed
            2_000_000_000, // dispatch1 complete/tick boundary
            2_040_000_000, // dispatch1 payload read completion (40ms)
            2_060_000_000, // dispatch1 validation completion (20ms)
            2_061_000_000, // snapshot after dispatch1
            2_961_000_000, // dispatch2 slot dispatch start
            3_061_000_000, // dispatch2 sent
            3_061_000_000, // dispatch2 first running tick observed
            4_061_000_000, // dispatch2 complete/tick boundary
            4_161_000_000, // dispatch2 payload read completion (100ms)
            4_241_000_000, // dispatch2 validation completion (80ms)
            4_242_000_000 // snapshot after dispatch2
        ])

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6561,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }
            for _ in 0 ..< 2 {
                guard let request = readDispatchRequest(fd: pair.serverFD),
                      let context = parseTickContext(fromWorkerMime: request.mime) else {
                    return
                }
                sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: context)
            }
        }

        let secondTxOutSampleMS = 100.0
        let secondPublishSampleMS = 80.0

        let firstResult = await dispatcher.dispatch(
            uploadId: uploadID1,
            filePath: uploadPath1,
            originalName: "\(uploadID1).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )
        #expect(firstResult == .success)

        let firstSnapshot = await dispatcher.complexityAwareSnapshot()
        let firstWorker = try #require(firstSnapshot.first)
        let firstTxOutEstimate = try #require(firstWorker.txOutEstimateMS)
        let firstPublishEstimate = try #require(firstWorker.publishOverheadEstimateMS)

        let secondResult = await dispatcher.dispatch(
            uploadId: uploadID2,
            filePath: uploadPath2,
            originalName: "\(uploadID2).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )
        #expect(secondResult == .success)

        let secondSnapshot = await dispatcher.complexityAwareSnapshot()
        let secondWorker = try #require(secondSnapshot.first)
        let secondTxOutEstimate = try #require(secondWorker.txOutEstimateMS)
        let secondPublishEstimate = try #require(secondWorker.publishOverheadEstimateMS)

        #expect(secondTxOutEstimate > firstTxOutEstimate)
        #expect(secondPublishEstimate > firstPublishEstimate)

        let firstTxOutDistanceToNewSample = secondTxOutSampleMS - firstTxOutEstimate
        let secondTxOutDistanceToNewSample = secondTxOutSampleMS - secondTxOutEstimate
        #expect(firstTxOutDistanceToNewSample > 0)
        #expect(secondTxOutDistanceToNewSample > 0)
        #expect(secondTxOutDistanceToNewSample < firstTxOutDistanceToNewSample)

        let firstPublishDistanceToNewSample = secondPublishSampleMS - firstPublishEstimate
        let secondPublishDistanceToNewSample = secondPublishSampleMS - secondPublishEstimate
        #expect(firstPublishDistanceToNewSample > 0)
        #expect(secondPublishDistanceToNewSample > 0)
        #expect(secondPublishDistanceToNewSample < firstPublishDistanceToNewSample)

        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("tail telemetry updates only on success")
    func tailTelemetryUpdatesOnlyOnSuccess() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID1 = "tb-tail-telemetry-success-only-001"
        let uploadID2 = "tb-tail-telemetry-success-only-002"
        let uploadPath1 = try writeUpload(env: env, uploadID: uploadID1)
        let uploadPath2 = try writeUpload(env: env, uploadID: uploadID2)

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(values: [
            900_000_000, // dispatch1 slot dispatch start
            1_000_000_000, // dispatch1 sent
            1_000_000_000, // dispatch1 first running tick observed
            2_000_000_000, // dispatch1 complete/tick boundary
            2_025_000_000, // dispatch1 payload read completion (25ms)
            2_040_000_000, // dispatch1 validation completion (15ms)
            2_900_000_000, // dispatch2 slot dispatch start
            3_000_000_000, // dispatch2 sent
            3_000_000_000 // dispatch2 first running tick observed (fails before completion boundary)
        ])

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6562,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }

            guard let firstRequest = readDispatchRequest(fd: pair.serverFD),
                  let firstContext = parseTickContext(fromWorkerMime: firstRequest.mime) else {
                return
            }
            sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: firstContext)

            guard let secondRequest = readDispatchRequest(fd: pair.serverFD),
                  let secondContext = parseTickContext(fromWorkerMime: secondRequest.mime) else {
                return
            }
            sendRunningAndFailedTickStream(fd: pair.serverFD, context: secondContext)
        }

        let firstResult = await dispatcher.dispatch(
            uploadId: uploadID1,
            filePath: uploadPath1,
            originalName: "\(uploadID1).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )
        let secondResult = await dispatcher.dispatch(
            uploadId: uploadID2,
            filePath: uploadPath2,
            originalName: "\(uploadID2).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )

        #expect(firstResult == .success)

        let firstSnapshot = await dispatcher.complexityAwareSnapshot()
        let firstWorker = try #require(firstSnapshot.first)
        let firstTxOutEstimate = try #require(firstWorker.txOutEstimateMS)
        let firstPublishEstimate = try #require(firstWorker.publishOverheadEstimateMS)

        #expect(secondResult == .transientRetry(slotHealthDown: false))

        let secondSnapshot = await dispatcher.complexityAwareSnapshot()
        let secondWorker = try #require(secondSnapshot.first)
        let secondTxOutEstimate = try #require(secondWorker.txOutEstimateMS)
        let secondPublishEstimate = try #require(secondWorker.publishOverheadEstimateMS)
        #expect(secondTxOutEstimate == firstTxOutEstimate)
        #expect(secondPublishEstimate == firstPublishEstimate)

        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("tail telemetry samples are clamped to bounded non-negative EMA values")
    func clampsTailTelemetryToBounds() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "tb-tail-telemetry-clamp-001"
        let uploadPath = try writeUpload(env: env, uploadID: uploadID)

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(values: [
            900_000_000, // slot dispatch start
            1_000_000_000, // dispatch sent
            1_000_000_000, // first running tick observed
            2_000_000_000, // complete/tick boundary
            900_002_000_000_000, // payload read completion (very large delta)
            1_800_002_000_000_000 // validation completion (very large delta)
        ])

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6563,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }
            guard let request = readDispatchRequest(fd: pair.serverFD),
                  let context = parseTickContext(fromWorkerMime: request.mime) else {
                return
            }
            sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: context)
        }

        let result = await dispatcher.dispatch(
            uploadId: uploadID,
            filePath: uploadPath,
            originalName: "\(uploadID).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )

        #expect(result == .success)
        let snapshot = await dispatcher.complexityAwareSnapshot()
        let worker = try #require(snapshot.first)
        let txOutEstimate = try #require(worker.txOutEstimateMS)
        let publishEstimate = try #require(worker.publishOverheadEstimateMS)
        #expect(txOutEstimate.isFinite)
        #expect(publishEstimate.isFinite)
        #expect(txOutEstimate >= 0)
        #expect(publishEstimate >= 0)
        #expect(txOutEstimate <= 1_000_000)
        #expect(publishEstimate <= 1_000_000)

        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("remote live msPerFrameC1 converges toward stable observations and smooths spikes")
    func remoteLiveMSPerFrameC1ConvergesTowardStableObservationsAndSmoothsSpikes() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let remoteSignature = "tb-live-adaptive-remote-signature"
        let startingEstimate = 9.135
        let stableObservation = 4.9
        let spikeObservation = 7.0
        let observations = Array(repeating: stableObservation, count: 10) + [spikeObservation]

        let priorTable = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: "",
                msPerFrameC1: startingEstimate,
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

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(
            values: makeSuccessfulDispatchNanosSequence(
                firstRunningLatencyMS: Array(repeating: 0, count: observations.count),
                txOutMS: 10,
                publishOverheadMS: 5,
                includeSnapshotReadbacks: true
            )
        )

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6565,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in
                makeWorkerCaps(signature: remoteSignature)
            }
        )

        await dispatcher.warmupPrior()
        let mergedPrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.complexityAwareSnapshot()
            return snapshot.first?.workerSignature == remoteSignature
        }
        #expect(mergedPrior, "Setup: remote prior merge must complete before adaptive updates")

        let frameCount = 100.0
        let startingDistanceToStable = abs(startingEstimate - stableObservation)
        var previousEstimate = startingEstimate
        var stableEstimates: [Double] = []
        stableEstimates.reserveCapacity(observations.count - 1)

        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }
            for observation in observations {
                guard let request = readDispatchRequest(fd: pair.serverFD),
                      let context = parseTickContext(fromWorkerMime: request.mime) else {
                    return
                }
                let processNanos = UInt64((observation * frameCount * 1_000_000).rounded())
                sendSuccessfulTickStreamAndResponse(
                    fd: pair.serverFD,
                    context: context,
                    processNanos: processNanos,
                    runningElapsedMS: 0
                )
            }
        }

        for index in observations.indices {
            let uploadID = "tb-live-adaptive-\(index)"
            let uploadPath = try writeUpload(env: env, uploadID: uploadID)
            let result = await dispatcher.dispatch(
                uploadId: uploadID,
                filePath: uploadPath,
                originalName: "\(uploadID).mov",
                mimeType: "video/quicktime",
                targetWorkerIndex: 0,
                targetSlotIndex: 0,
                frameCount: frameCount
            )

            #expect(result == .success)
            let snapshot = await dispatcher.complexityAwareSnapshot()
            let estimate = try #require(snapshot.first?.liveMSPerFrameC1)
            #expect(estimate.isFinite)
            #expect(estimate > 0)

            if index < observations.count - 1 {
                let previousDistanceToStable = abs(previousEstimate - stableObservation)
                let currentDistanceToStable = abs(estimate - stableObservation)
                #expect(currentDistanceToStable <= previousDistanceToStable)
                stableEstimates.append(estimate)
            } else {
                let stableEstimate = try #require(stableEstimates.last)
                #expect(estimate > stableEstimate)
                #expect(estimate > stableObservation)
                #expect(estimate < spikeObservation)
            }

            previousEstimate = estimate
        }

        let finalStableEstimate = try #require(stableEstimates.last)
        #expect(abs(finalStableEstimate - stableObservation) < startingDistanceToStable * 0.10)

        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("warmup prior merge preserves existing affine fields when capability cells arrive")
    func warmupPriorMergePreservesExistingAffineFieldsWhenCapabilityCellsArrive() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let remoteSignature = "tb-remote-prior-affine-preserve"
        let incomingCells = [
            BenchmarkPriorCell(
                concurrency: 1,
                videosPerMin: 55,
                msPerVideoP50: 1_200,
                msPerVideoP95: 1_500,
                degradationRatio: 1.0
            ),
            BenchmarkPriorCell(
                concurrency: 2,
                videosPerMin: 90,
                msPerVideoP50: 1_600,
                msPerVideoP95: 1_900,
                degradationRatio: 1.2
            ),
        ]
        let priorTable = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 4,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: "",
                msPerFrameC1: 0.5,
                fixedOverheadMS: 200,
                avgCorpusFrameCount: 100,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 40,
                        msPerVideoP50: 1_400,
                        msPerVideoP95: 1_700,
                        degradationRatio: 1.0
                    ),
                ]
            ),
        ])

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6565,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            benchmarkPriorTable: priorTable,
            videoTranscodePreset: "",
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in nil },
            queryCapabilitiesOverride: { _, _, _, _ in
                makeWorkerCaps(
                    signature: remoteSignature,
                    osVersion: "26.0",
                    priorCells: incomingCells
                )
            }
        )

        await dispatcher.warmupPrior()
        let mergedPrior = try await waitUntil(timeoutSeconds: 2) {
            let snapshot = await dispatcher.benchmarkPriorSnapshot()
            return snapshot.machines.first { $0.signature == remoteSignature }?.cells == incomingCells
        }
        #expect(mergedPrior, "Setup: remote prior merge must complete before snapshot assertions")

        let snapshot = await dispatcher.benchmarkPriorSnapshot()
        let machine = try #require(snapshot.machines.first { $0.signature == remoteSignature })
        #expect(machine.msPerFrameC1 == 0.5)
        #expect(machine.fixedOverheadMS == 200)
        #expect(machine.avgCorpusFrameCount == 100)
        #expect(machine.affineModelSource == .explicit)
        #expect(machine.cells == incomingCells)

        await dispatcher.shutdown()
    }

    @Test("transfer overhead rises on degraded samples and recovers conservatively")
    func transferOverheadRisesOnDegradedSamplesAndRecoversConservatively() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadIDs = [
            "tb-transfer-adaptive-001",
            "tb-transfer-adaptive-002",
            "tb-transfer-adaptive-003",
        ]
        let firstRunningLatencySamples = [100.0, 200.0, 90.0]

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(
            values: makeSuccessfulDispatchNanosSequence(
                firstRunningLatencyMS: [100, 200, 90],
                txOutMS: 10,
                publishOverheadMS: 5,
                includeSnapshotReadbacks: true
            )
        )

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6566,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }
            for _ in uploadIDs {
                guard let request = readDispatchRequest(fd: pair.serverFD),
                      let context = parseTickContext(fromWorkerMime: request.mime) else {
                    return
                }
                sendSuccessfulTickStreamAndResponse(
                    fd: pair.serverFD,
                    context: context,
                    processNanos: 150_000_000,
                    runningElapsedMS: 0
                )
            }
        }

        var estimates: [Double] = []
        estimates.reserveCapacity(uploadIDs.count)

        for uploadID in uploadIDs {
            let uploadPath = try writeUpload(env: env, uploadID: uploadID)
            let result = await dispatcher.dispatch(
                uploadId: uploadID,
                filePath: uploadPath,
                originalName: "\(uploadID).mov",
                mimeType: "video/quicktime",
                targetWorkerIndex: 0,
                targetSlotIndex: 0,
                frameCount: 100
            )

            #expect(result == .success)
            let snapshot = await dispatcher.complexityAwareSnapshot()
            let estimate = try #require(snapshot.first?.transferOverheadEstimateMS)
            #expect(estimate.isFinite)
            #expect(estimate > 0)
            estimates.append(estimate)
        }

        let firstEstimate = estimates[0]
        let secondEstimate = estimates[1]
        let thirdEstimate = estimates[2]
        #expect(secondEstimate > firstEstimate)
        #expect(secondEstimate < firstRunningLatencySamples[1])
        #expect(thirdEstimate < secondEstimate)
        #expect(thirdEstimate > firstEstimate)
        #expect(thirdEstimate > firstRunningLatencySamples[2])

        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("complexityAwareSnapshot fallback remaining estimate subtracts elapsed runtime before first running tick")
    func complexityAwareFallbackRemainingEstimateSubtractsElapsedBeforeFirstTick() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let learnedRuntimeMS = 150.0
        let uploadID1 = "tb-eta-fallback-elapsed-001"
        let uploadID2 = "tb-eta-fallback-elapsed-002"
        let uploadPath1 = try writeUpload(env: env, uploadID: uploadID1)
        let uploadPath2 = try writeUpload(env: env, uploadID: uploadID2)

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(values: [
            1_000_000_000, // dispatch1 slot dispatch start
            1_100_000_000, // dispatch1 sent
            1_150_000_000, // dispatch1 first running tick observed
            1_300_000_000, // dispatch1 complete/tick boundary (150ms process)
            1_320_000_000, // dispatch1 payload read completion
            1_340_000_000, // dispatch1 validation completion
            2_000_000_000, // dispatch2 slot dispatch start
            2_050_000_000, // dispatch2 sent
            2_600_000_000, // CA snapshot now (600ms elapsed on dispatch2)
            2_800_000_000, // dispatch2 first running tick observed
            2_900_000_000, // dispatch2 complete/tick boundary
            2_920_000_000, // dispatch2 payload read completion
            2_940_000_000 // dispatch2 validation completion
        ])

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6564,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let secondDispatchGate = SecondDispatchGate()
        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }

            guard let firstRequest = readDispatchRequest(fd: pair.serverFD),
                  let firstContext = parseTickContext(fromWorkerMime: firstRequest.mime) else {
                return
            }
            sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: firstContext)

            guard let secondRequest = readDispatchRequest(fd: pair.serverFD),
                  let secondContext = parseTickContext(fromWorkerMime: secondRequest.mime) else {
                return
            }
            await secondDispatchGate.signalRequestReady()
            await secondDispatchGate.waitForRelease()
            sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: secondContext)
        }

        let firstResult = await dispatcher.dispatch(
            uploadId: uploadID1,
            filePath: uploadPath1,
            originalName: "\(uploadID1).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )
        #expect(firstResult == .success)

        let secondDispatchTask = Task {
            await dispatcher.dispatch(
                uploadId: uploadID2,
                filePath: uploadPath2,
                originalName: "\(uploadID2).mov",
                mimeType: "video/quicktime",
                targetWorkerIndex: 0,
                targetSlotIndex: 0
            )
        }

        await secondDispatchGate.waitUntilRequestReady()

        let caSnapshot = await dispatcher.complexityAwareSnapshot()
        let caWorker = try #require(caSnapshot.first)
        let caSlot = try #require(caWorker.slots.first)
        #expect(caSlot.isBusy)
        let caRemainingMS = try #require(caSlot.estimatedRemainingMS)
        #expect(caRemainingMS.isFinite)
        #expect(caRemainingMS >= 0)
        #expect(caRemainingMS <= learnedRuntimeMS * 0.10)

        await secondDispatchGate.release()
        let secondResult = await secondDispatchTask.value
        #expect(secondResult == .success)

        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("complexityAwareSnapshot fallback remaining estimate decays before first running tick")
    func complexityAwareFallbackRemainingEstimateDecaysBeforeFirstTick() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let learnedRuntimeMS = 150.0
        let uploadID1 = "tb-eta-fallback-decay-001"
        let uploadID2 = "tb-eta-fallback-decay-002"
        let uploadPath1 = try writeUpload(env: env, uploadID: uploadID1)
        let uploadPath2 = try writeUpload(env: env, uploadID: uploadID2)

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let nanos = NanosSequence(values: [
            1_000_000_000, // dispatch1 slot dispatch start
            1_100_000_000, // dispatch1 sent
            1_150_000_000, // dispatch1 first running tick observed
            1_300_000_000, // dispatch1 complete/tick boundary (150ms process)
            1_320_000_000, // dispatch1 payload read completion
            1_340_000_000, // dispatch1 validation completion
            2_000_000_000, // dispatch2 slot dispatch start
            2_050_000_000, // dispatch2 sent
            2_100_000_000, // early CA snapshot now (100ms elapsed on dispatch2)
            2_600_000_000, // late CA snapshot now (600ms elapsed on dispatch2)
            2_800_000_000, // dispatch2 first running tick observed
            2_900_000_000, // dispatch2 complete/tick boundary
            2_920_000_000, // dispatch2 payload read completion
            2_940_000_000 // dispatch2 validation completion
        ])

        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6565,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            videoTranscodePreset: "",
            monotonicNowNanosOverride: { nanos.next() },
            bridgeSourcesOverride: [loopbackBridgeSource()],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let secondDispatchGate = SecondDispatchGate()
        let workerTask = Task.detached(priority: .userInitiated) {
            defer { Darwin.close(pair.serverFD) }

            guard let firstRequest = readDispatchRequest(fd: pair.serverFD),
                  let firstContext = parseTickContext(fromWorkerMime: firstRequest.mime) else {
                return
            }
            sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: firstContext)

            guard let secondRequest = readDispatchRequest(fd: pair.serverFD),
                  let secondContext = parseTickContext(fromWorkerMime: secondRequest.mime) else {
                return
            }
            await secondDispatchGate.signalRequestReady()
            await secondDispatchGate.waitForRelease()
            sendSuccessfulTickStreamAndResponse(fd: pair.serverFD, context: secondContext)
        }

        let firstResult = await dispatcher.dispatch(
            uploadId: uploadID1,
            filePath: uploadPath1,
            originalName: "\(uploadID1).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )
        #expect(firstResult == .success)

        let secondDispatchTask = Task {
            await dispatcher.dispatch(
                uploadId: uploadID2,
                filePath: uploadPath2,
                originalName: "\(uploadID2).mov",
                mimeType: "video/quicktime",
                targetWorkerIndex: 0,
                targetSlotIndex: 0
            )
        }

        await secondDispatchGate.waitUntilRequestReady()

        let earlySnapshot = await dispatcher.complexityAwareSnapshot()
        let earlyWorker = try #require(earlySnapshot.first)
        let earlySlot = try #require(earlyWorker.slots.first)
        #expect(earlySlot.isBusy)
        let earlyRemainingMS = try #require(earlySlot.estimatedRemainingMS)
        #expect(earlyRemainingMS.isFinite)
        #expect(earlyRemainingMS > 0)
        #expect(earlyRemainingMS < learnedRuntimeMS)

        let lateSnapshot = await dispatcher.complexityAwareSnapshot()
        let lateWorker = try #require(lateSnapshot.first)
        let lateSlot = try #require(lateWorker.slots.first)
        #expect(lateSlot.isBusy)
        let lateRemainingMS = try #require(lateSlot.estimatedRemainingMS)
        #expect(lateRemainingMS.isFinite)
        #expect(lateRemainingMS >= 0)
        #expect(lateRemainingMS < earlyRemainingMS)
        #expect(lateRemainingMS <= learnedRuntimeMS * 0.10)

        await secondDispatchGate.release()
        let secondResult = await secondDispatchTask.value
        #expect(secondResult == .success)

        await dispatcher.shutdown()
        _ = await workerTask.result
    }
}

private final class NanosSequence: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [UInt64]
    private var fallback: UInt64

    init(values: [UInt64]) {
        self.values = values
        self.fallback = values.last ?? 0
    }

    func next() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        if !values.isEmpty {
            fallback = values.removeFirst()
            return fallback
        }
        return fallback
    }
}

private actor SecondDispatchGate {
    private var requestReady = false
    private var requestContinuation: CheckedContinuation<Void, Never>?
    private var released = false
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func signalRequestReady() {
        requestReady = true
        requestContinuation?.resume()
        requestContinuation = nil
    }

    func waitUntilRequestReady() async {
        if requestReady { return }
        await withCheckedContinuation { continuation in
            requestContinuation = continuation
        }
    }

    func release() {
        released = true
        releaseContinuation?.resume()
        releaseContinuation = nil
    }

    func waitForRelease() async {
        if released { return }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }
}

private func writeUpload(env: TestEnv, uploadID: String) throws -> String {
    let path = "\(env.uploadDir)/\(uploadID)"
    try Data(repeating: 0x6B, count: 128).write(to: URL(fileURLWithPath: path))
    return path
}

private struct SocketPair {
    let clientFD: Int32
    let serverFD: Int32
}

private struct DispatchRequest {
    let fileSize: Int
    let mime: String
}

private struct TickContext {
    let jobHandle: UInt32
    let sessionID: UInt32
}

private final class FDSequence: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [Int32?]

    init(values: [Int32?]) {
        self.values = values
    }

    func next() -> Int32? {
        lock.lock()
        defer { lock.unlock() }
        guard !values.isEmpty else { return nil }
        return values.removeFirst()
    }
}

private func loopbackBridgeSource() -> ThunderboltDispatcher.BridgeSource {
    ThunderboltDispatcher.BridgeSource(
        name: "bridge-test",
        ip: "127.0.0.1",
        network: 0x7F00_0000,
        mask: 0xFF00_0000
    )
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

private func readDispatchRequest(fd: Int32) -> DispatchRequest? {
    guard let prefix = readExactly(fd: fd, count: 74) else { return nil }
    let fileSize = prefix.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: 0, as: UInt64.self).bigEndian)
    }
    guard fileSize >= 0 else { return nil }

    let nameLen = prefix.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: 72, as: UInt16.self).bigEndian)
    }
    guard nameLen >= 0 else { return nil }
    guard let nameAndMimeLen = readExactly(fd: fd, count: nameLen + 2) else { return nil }
    let mimeLen = nameAndMimeLen.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: nameLen, as: UInt16.self).bigEndian)
    }
    guard mimeLen >= 0 else { return nil }
    guard let mimeData = readExactly(fd: fd, count: mimeLen),
          let mime = String(data: mimeData, encoding: .utf8) else {
        return nil
    }
    if fileSize > 0 {
        guard readExactly(fd: fd, count: fileSize) != nil else { return nil }
    }
    return DispatchRequest(fileSize: fileSize, mime: mime)
}

private func parseTickContext(fromWorkerMime mime: String) -> TickContext? {
    let tag = "#kiko-v2:"
    guard let tagRange = mime.range(of: tag) else { return nil }
    let metadata = mime[tagRange.upperBound...]
    var parsedHandle: UInt32?
    var parsedSession: UInt32?

    for pair in metadata.split(separator: ",") {
        let parts = pair.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
        guard parts.count == 2 else { continue }
        let key = parts[0].trimmingCharacters(in: .whitespacesAndNewlines)
        let value = parts[1].trimmingCharacters(in: .whitespacesAndNewlines)
        if key == "h", let handle = UInt32(value) {
            parsedHandle = handle
        } else if key == "s", let session = UInt32(value) {
            parsedSession = session
        }
    }

    guard let parsedHandle, let parsedSession else { return nil }
    return TickContext(jobHandle: parsedHandle, sessionID: parsedSession)
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
            let bytesRead = Darwin.read(fd, base.advanced(by: offset), count - offset)
            if bytesRead > 0 {
                offset += bytesRead
                continue
            }
            if bytesRead == 0 { return false }
            if bytesRead < 0, errno == EINTR { continue }
            return false
        }
        return true
    }
    return ok ? data : nil
}

private func writeAll(fd: Int32, data: Data) -> Bool {
    data.withUnsafeBytes { raw -> Bool in
        guard let base = raw.baseAddress else { return true }
        var sent = 0
        while sent < data.count {
            let bytesWritten = Darwin.write(fd, base.advanced(by: sent), data.count - sent)
            if bytesWritten > 0 {
                sent += bytesWritten
                continue
            }
            if bytesWritten < 0, errno == EINTR { continue }
            return false
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

private func makeSuccessfulDispatchNanosSequence(
    firstRunningLatencyMS: [UInt64],
    txOutMS: UInt64 = 40,
    publishOverheadMS: UInt64 = 20,
    includeSnapshotReadbacks: Bool = false
) -> [UInt64] {
    var values: [UInt64] = []
    values.reserveCapacity(firstRunningLatencyMS.count * (includeSnapshotReadbacks ? 7 : 6))

    var dispatchStartNanos: UInt64 = 1_000_000_000
    for latencyMS in firstRunningLatencyMS {
        let dispatchSentAtNanos = dispatchStartNanos + 100_000_000
        let firstRunningObservedAtNanos = dispatchSentAtNanos + (latencyMS * 1_000_000)
        let tickCompletionBoundaryNanos = firstRunningObservedAtNanos + 1_000_000_000
        let payloadReadCompletedAtNanos = tickCompletionBoundaryNanos + (txOutMS * 1_000_000)
        let finalizationCompletedAtNanos = payloadReadCompletedAtNanos + (publishOverheadMS * 1_000_000)
        values.append(dispatchStartNanos)
        values.append(dispatchSentAtNanos)
        values.append(firstRunningObservedAtNanos)
        values.append(tickCompletionBoundaryNanos)
        values.append(payloadReadCompletedAtNanos)
        values.append(finalizationCompletedAtNanos)
        if includeSnapshotReadbacks {
            let snapshotObservedAtNanos = finalizationCompletedAtNanos + 1_000_000
            values.append(snapshotObservedAtNanos)
            dispatchStartNanos = snapshotObservedAtNanos + 900_000_000
        } else {
            dispatchStartNanos = finalizationCompletedAtNanos + 900_000_000
        }
    }

    return values
}

private func makeWorkerCaps(
    signature: String,
    osVersion: String? = nil,
    msPerFrameC1: Double? = nil,
    priorCells: [BenchmarkPriorCell]? = nil
) -> WorkerCaps {
    var payload: [String: Any] = [
        "worker_signature": signature,
    ]
    if let osVersion {
        payload["os_version"] = osVersion
    }
    if let msPerFrameC1 {
        payload["ms_per_frame_c1"] = msPerFrameC1
    }
    if let priorCells {
        payload["prior_cells"] = priorCells.map { cell in
            [
                "concurrency": cell.concurrency,
                "videos_per_min": cell.videosPerMin,
                "ms_per_video_p50": cell.msPerVideoP50,
                "ms_per_video_p95": cell.msPerVideoP95,
                "degradation_ratio": cell.degradationRatio,
            ]
        }
    }
    let data = try! JSONSerialization.data(withJSONObject: payload, options: [])
    return try! JSONDecoder().decode(WorkerCaps.self, from: data)
}

private func sendSuccessfulTickStreamAndResponse(
    fd: Int32,
    context: TickContext,
    processNanos: UInt64 = 150_000_000,
    runningElapsedMS: UInt32 = 100
) {
    let running = ProgressTickV2(
        status: .running,
        jobHandle: context.jobHandle,
        sessionID: context.sessionID,
        errorClass: .none,
        progress: 0.30,
        elapsedMS: runningElapsedMS,
        estRemainingMS: 750
    ).encode()
    let complete = ProgressTickV2(
        status: .complete,
        jobHandle: context.jobHandle,
        sessionID: context.sessionID,
        errorClass: .none,
        progress: 1.0,
        elapsedMS: 1_000,
        estRemainingMS: 0
    ).encode()
    _ = writeAll(fd: fd, data: running)
    _ = writeAll(fd: fd, data: complete)

    let previewPayload = Data("preview".utf8)
    let thumbPayload = Data("thumb".utf8)
    let header = makeResponseHeader(
        status: 0x01,
        processNanos: processNanos,
        previewPayload: previewPayload,
        thumbPayload: thumbPayload
    )
    _ = writeAll(fd: fd, data: header)
    _ = writeAll(fd: fd, data: previewPayload)
    _ = writeAll(fd: fd, data: thumbPayload)
}

private func sendRunningAndFailedTickStream(fd: Int32, context: TickContext) {
    let running = ProgressTickV2(
        status: .running,
        jobHandle: context.jobHandle,
        sessionID: context.sessionID,
        errorClass: .none,
        progress: 0.25,
        elapsedMS: 120,
        estRemainingMS: 880
    ).encode()
    let failed = ProgressTickV2(
        status: .failed,
        jobHandle: context.jobHandle,
        sessionID: context.sessionID,
        errorClass: .transient,
        progress: 0.30,
        elapsedMS: 200,
        estRemainingMS: 0
    ).encode()
    _ = writeAll(fd: fd, data: running)
    _ = writeAll(fd: fd, data: failed)
}
