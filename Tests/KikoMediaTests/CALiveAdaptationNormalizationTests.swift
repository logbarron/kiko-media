import Foundation
import Testing
@testable import KikoMediaCore

@Suite("CA live adaptation normalization")
struct CALiveAdaptationNormalizationTests {
    @Test("shared successful execution normalizer subtracts fixed overhead and repairs sparse curves before c1 normalization")
    func sharedSuccessfulExecutionNormalizer_subtractsFixedOverheadAndRepairsSparseCurves() throws {
        let model = CASuccessfulExecutionSampleModel(
            msPerFrameC1: 5.0,
            fixedOverheadMS: 200,
            degradationCurve: [
                CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
                CADegradationPoint(concurrency: 3, ratioToC1: 2.0),
            ]
        )

        let normalized = try #require(
            ThunderboltAdaptiveTelemetryReducer.normalizedMSPerFrameC1(
                processNanos: 500_000_000,
                frameCount: 100,
                model: model,
                concurrency: 2
            )
        )

        #expect(abs(normalized - 2.0) < 0.000_001)
    }

    @Test("the same successful sample yields the same update in production local production remote benchmark local and benchmark remote")
    func successfulSample_yieldsTheSameUpdateAcrossProductionAndBenchmarkCallsites() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let model = CASuccessfulExecutionSampleModel(
            msPerFrameC1: 5.0,
            fixedOverheadMS: 200,
            degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)]
        )
        let processNanos: UInt64 = 500_000_000
        let frameCount = 100.0
        let expectedEstimate = try expectedAdaptiveEstimate(
            model: model,
            processNanos: processNanos,
            frameCount: frameCount,
            concurrency: 1
        )

        let processor = try makeNormalizationProcessor(
            env: env,
            model: model
        )

        await processor.updateLocalLiveMSPerFrame(
            processNanos: processNanos,
            frameCount: frameCount
        )
        let productionLocalEstimate = try #require(await processor.localLiveMSPerFrameC1Estimate())

        let dispatcher = makeNormalizationDispatcher(env: env)

        await dispatcher.recordSuccessfulExecutionSampleForTesting(
            workerIndex: 0,
            processNanos: processNanos,
            frameCount: frameCount,
            concurrency: 1,
            successfulExecutionSampleModel: model
        )
        let productionRemoteEstimate = try #require(
            await dispatcher.complexityAwareSnapshot().first?.liveMSPerFrameC1
        )

        let benchmarkLocalRuntime = makeNormalizationRuntimeEngine(
            machineID: "benchmark-local",
            model: model
        )
        await benchmarkLocalRuntime.recordCompletion(
            machineIndex: 0,
            frameCount: frameCount,
            processNanos: processNanos,
            concurrencyHint: 1
        )
        let benchmarkLocalEstimate = try #require(
            await benchmarkLocalRuntime.adaptationRows().first?.finalMSPerFrameC1
        )

        let benchmarkRemoteRuntime = makeNormalizationRuntimeEngine(
            machineID: "benchmark-remote",
            model: model
        )
        await benchmarkRemoteRuntime.recordCompletion(
            machineIndex: 0,
            frameCount: frameCount,
            processNanos: processNanos,
            concurrencyHint: 1
        )
        let benchmarkRemoteEstimate = try #require(
            await benchmarkRemoteRuntime.adaptationRows().first?.finalMSPerFrameC1
        )

        for estimate in [
            productionLocalEstimate,
            productionRemoteEstimate,
            benchmarkLocalEstimate,
            benchmarkRemoteEstimate,
        ] {
            #expect(abs(estimate - expectedEstimate) < 0.000_001)
        }

        await dispatcher.shutdown()
        await processor.shutdown()
    }

    @Test("benchmark and production transfer-overhead reduction use the same shared reducer path")
    func benchmarkAndProductionTransferOverheadReduction_useTheSameSharedReducerPath() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let samples: [UInt32?] = [100, 200, 0]
        var expectedUpdate: TransferOverheadEstimator.Update?
        for sample in samples {
            expectedUpdate = TransferOverheadEstimator.reduce(
                previousBaseline: expectedUpdate?.baseline,
                previousEstimate: expectedUpdate?.estimate,
                sampleMS: sample
            )
        }
        let expectedEstimate = try #require(expectedUpdate?.estimate)

        let dispatcher = makeNormalizationDispatcher(env: env)
        for sample in samples {
            await dispatcher.recordTransferOverheadSampleForTesting(
                workerIndex: 0,
                sampleMS: sample
            )
        }
        let productionEstimate = try #require(
            await dispatcher.complexityAwareSnapshot().first?.transferOverheadEstimateMS
        )

        let runtime = makeNormalizationRuntimeEngine(
            machineID: "benchmark-transfer",
            model: CASuccessfulExecutionSampleModel(
                msPerFrameC1: 1.0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)]
            )
        )
        for sample in samples {
            await runtime.recordTransferOverhead(machineIndex: 0, sampleMS: sample.map(Double.init))
        }
        let benchmarkEstimate = try #require(
            await runtime.machineSnapshotForTesting().first?.txInMS
        )

        #expect(abs(productionEstimate - expectedEstimate) < 0.000_001)
        #expect(abs(benchmarkEstimate - expectedEstimate) < 0.000_001)

        await dispatcher.shutdown()
    }
}

private func makeNormalizationProcessor(
    env: TestEnv,
    model: CASuccessfulExecutionSampleModel
) throws -> MediaProcessor {
    let priorTable = try makeNormalizationLocalPriorTable(
        config: env.config,
        model: model
    )
    return MediaProcessor(
        config: env.config,
        database: env.database,
        moderationMarkers: ModerationMarkers(
            baseDir: env.tempDir.appendingPathComponent("moderated-normalization")
        ),
        benchmarkPriorTable: priorTable,
        complexityAwareSchedulingEnabled: true
    )
}

private func makeNormalizationDispatcher(env: TestEnv) -> ThunderboltDispatcher {
    ThunderboltDispatcher(
        workers: [.init(host: "127.0.0.1", slots: 1)],
        port: 7000,
        connectTimeout: 100,
        thumbsDir: env.thumbsDir,
        previewsDir: env.previewsDir,
        sha256BufferSize: env.config.sha256BufferSize,
        complexityAwareSchedulingEnabled: true,
        bridgeSourcesOverride: [],
        connectOverride: { _, _, _, _ in nil },
        queryCapabilitiesOverride: { _, _, _, _ in nil }
    )
}

private func makeNormalizationRuntimeEngine(
    machineID: String,
    model: CASuccessfulExecutionSampleModel
) -> CABenchmarkRuntimeEngine {
    CABenchmarkRuntimeEngine(
        policy: .fifo,
        videoCosts: makeResolvedVideoCosts(frameCounts: [100]),
        machineProfiles: [
            CABenchmarkRuntimeMachineProfile(
                id: machineID,
                msPerFrameC1: model.msPerFrameC1,
                fixedOverheadMS: model.fixedOverheadMS,
                degradationCurve: model.degradationCurve,
                txInMS: 0
            ),
        ],
        slotBindings: [
            CABenchmarkRuntimeSlotBinding(machineIndex: 0, slotID: "\(machineID)-s1"),
        ]
    )
}

private func makeNormalizationLocalPriorTable(
    config: Config,
    model: CASuccessfulExecutionSampleModel
) throws -> BenchmarkPriorTable {
    let caps = WorkerCaps.detectLocal()
    let signature = try #require(
        WorkerSignatureBuilder.make(
            chipName: caps.chipName,
            performanceCores: caps.performanceCores,
            efficiencyCores: caps.efficiencyCores,
            videoEncodeEngines: caps.videoEncodeEngines,
            preset: config.videoTranscodePreset,
            osVersion: caps.osVersion
        )
    )
    let repairedCurve = CAProfileAndFallbackMath.repairedDenseDegradationCurve(
        from: model.degradationCurve
    )
    let cells = repairedCurve.map { point in
        let p50 = Int(
            (
                model.fixedOverheadMS
                    + (100.0 * model.msPerFrameC1 * point.ratioToC1)
            ).rounded()
        )
        return BenchmarkPriorCell(
            concurrency: point.concurrency,
            videosPerMin: 10,
            msPerVideoP50: max(1, p50),
            msPerVideoP95: max(1, p50 + 100),
            degradationRatio: point.ratioToC1
        )
    }
    let machine = BenchmarkPriorMachine(
        signature: signature,
        chipName: caps.chipName ?? "test-chip",
        performanceCores: caps.performanceCores ?? 1,
        efficiencyCores: caps.efficiencyCores ?? 0,
        videoEncodeEngines: caps.videoEncodeEngines ?? 1,
        osVersion: caps.osVersion ?? "0.0",
        transcodePreset: config.videoTranscodePreset,
        msPerFrameC1: model.msPerFrameC1,
        fixedOverheadMS: model.fixedOverheadMS,
        avgCorpusFrameCount: 100,
        cells: cells
    )
    return BenchmarkPriorTable(machines: [machine])
}

private func expectedAdaptiveEstimate(
    model: CASuccessfulExecutionSampleModel,
    processNanos: UInt64,
    frameCount: Double,
    concurrency: Int
) throws -> Double {
    let observed = try #require(
        ThunderboltAdaptiveTelemetryReducer.normalizedMSPerFrameC1(
            processNanos: processNanos,
            frameCount: frameCount,
            model: model,
            concurrency: concurrency
        )
    )
    let update = try #require(
        LiveAdaptiveMSPerFrameC1Estimator.next(
            previousEstimate: nil,
            previousSmoothedError: nil,
            previousSmoothedAbsoluteError: nil,
            initialEstimate: model.msPerFrameC1,
            observed: observed
        )
    )
    return update.estimate
}
