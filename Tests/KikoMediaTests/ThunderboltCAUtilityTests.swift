import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt CA utilities")
struct ThunderboltCAUtilityTests {
    @Test("ms-per-frame helper rejects nil non-finite and non-positive values")
    func msPerFrameHelperRejectsInvalidValues() {
        #expect(validMSPerFrameC1(nil) == nil)
        #expect(validMSPerFrameC1(.nan) == nil)
        #expect(validMSPerFrameC1(0) == nil)
        #expect(validMSPerFrameC1(8.5) == 8.5)
    }

    @Test("degradation curve repair uses dense monotone interpolation with upward clamp")
    func degradationCurveRepair_usesDenseMonotoneInterpolationWithUpwardClamp() {
        let curve = caDegradationCurve(from: [3: 1.4, 1: 1.0, 0: 2.0, 2: .nan])

        #expect(CAProfileAndFallbackMath.denseDegradationRepairRuleName == "monotone-interpolation-with-upward-clamp")
        #expect(curve.map(\.concurrency) == [1, 2, 3])
        #expect(curve.map(\.ratioToC1) == [1.0, 1.2, 1.4])
    }

    @Test("slot enumeration keeps local slots first and skips zero-slot workers")
    func slotEnumerationKeepsLocalSlotsFirstAndSkipsZeroSlotWorkers() {
        let workers = [
            ThunderboltBoundWorkerSpec(
                host: "worker-a",
                connectHost: "10.0.0.2",
                slots: 2,
                sourceIP: "10.0.0.1",
                bridgeName: "bridge0"
            ),
            ThunderboltBoundWorkerSpec(
                host: "worker-b",
                connectHost: "10.0.0.3",
                slots: 0,
                sourceIP: "10.0.0.1",
                bridgeName: "bridge0"
            ),
        ]

        let slots = caSlots(localSlots: 2, reachableWorkers: workers)
        #expect(slots.map(\.label) == ["local-1", "local-2", "worker-a-1", "worker-a-2"])
    }

    @Test("burst arrival profile uses configured phase boundaries")
    func burstArrivalProfileUsesConfiguredPhaseBoundaries() {
        let offsets = caArrivalOffsets(profile: .burst_1_20_5_5_1, count: 30)

        #expect(offsets.count == 30)
        #expect(offsets[0] == 0)
        #expect(offsets[1] == 5)
        #expect(offsets[20] == 5)
        #expect(offsets[21] == 10)
        #expect(offsets[29] == 15)
    }

    @Test("seed runtime falls back to concurrency one and returns nil when prior is missing")
    func seedRuntimeFallsBackToConcurrencyOneAndReturnsNilWhenPriorIsMissing() {
        let machine = BenchmarkPriorMachine(
            signature: "local-signature",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: "p",
            msPerFrameC1: 8.0,
            avgCorpusFrameCount: 1_440,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 1,
                    videosPerMin: 20,
                    msPerVideoP50: 1_500,
                    msPerVideoP95: 2_000,
                    degradationRatio: 1.0
                ),
                BenchmarkPriorCell(
                    concurrency: 3,
                    videosPerMin: 10,
                    msPerVideoP50: 3_000,
                    msPerVideoP95: 3_600,
                    degradationRatio: 1.6
                ),
            ]
        )
        let table = BenchmarkPriorTable(machines: [machine])

        #expect(CAProfileAndFallbackMath.seedRuntimeSeconds(priorTable: table, localSignature: "local-signature", localConcurrency: 3) == 3.0)
        #expect(CAProfileAndFallbackMath.seedRuntimeSeconds(priorTable: table, localSignature: "local-signature", localConcurrency: 2) == 1.5)
        #expect(CAProfileAndFallbackMath.seedRuntimeSeconds(priorTable: BenchmarkPriorTable(), localSignature: "missing", localConcurrency: 1) == nil)
    }

    @Test("shared local prior shaping preserves independent ms and curve fallbacks")
    func sharedLocalPriorShapingPreservesIndependentMSAndCurveFallbacks() {
        let invalidMSMachine = BenchmarkPriorMachine(
            signature: "invalid-ms",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: "p",
            msPerFrameC1: .nan,
            fixedOverheadMS: -20,
            avgCorpusFrameCount: 1_440,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 2,
                    videosPerMin: 10,
                    msPerVideoP50: 3_000,
                    msPerVideoP95: 3_600,
                    degradationRatio: 1.6
                )
            ]
        )
        let invalidCurveMachine = BenchmarkPriorMachine(
            signature: "invalid-curve",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: "p",
            msPerFrameC1: 8.0,
            fixedOverheadMS: 120,
            avgCorpusFrameCount: 1_440,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 0,
                    videosPerMin: 10,
                    msPerVideoP50: 3_000,
                    msPerVideoP95: 3_600,
                    degradationRatio: 1.6
                )
            ]
        )

        let invalidMSProfile = CAProfileAndFallbackMath.localPriorProfileShaping(from: invalidMSMachine)
        #expect(invalidMSProfile.msPerFrameC1 == 1.0)
        #expect(invalidMSProfile.fixedOverheadMS == 0)
        #expect(invalidMSProfile.degradationCurve == [
            CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
            CADegradationPoint(concurrency: 2, ratioToC1: 1.6),
        ])
        #expect(invalidMSProfile.usedFallbackMSPerFrameC1)
        #expect(!invalidMSProfile.usedFallbackDegradationCurve)

        let invalidCurveProfile = CAProfileAndFallbackMath.localPriorProfileShaping(from: invalidCurveMachine)
        #expect(invalidCurveProfile.msPerFrameC1 == 8.0)
        #expect(invalidCurveProfile.fixedOverheadMS == 120)
        #expect(invalidCurveProfile.degradationCurve == [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)])
        #expect(!invalidCurveProfile.usedFallbackMSPerFrameC1)
        #expect(invalidCurveProfile.usedFallbackDegradationCurve)
    }

    @Test("shared validated prior profile requires both valid ms and degradation curve")
    func sharedValidatedPriorProfileRequiresBothValidMSAndDegradationCurve() {
        let validMachine = BenchmarkPriorMachine(
            signature: "valid",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: "p",
            msPerFrameC1: 8.0,
            fixedOverheadMS: 150,
            avgCorpusFrameCount: 1_440,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 1,
                    videosPerMin: 20,
                    msPerVideoP50: 1_500,
                    msPerVideoP95: 2_000,
                    degradationRatio: 1.0
                )
            ]
        )
        let invalidCurveMachine = BenchmarkPriorMachine(
            signature: "invalid-curve",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: "p",
            msPerFrameC1: 9.0,
            fixedOverheadMS: 75,
            avgCorpusFrameCount: 1_440,
            cells: []
        )
        let table = BenchmarkPriorTable(machines: [validMachine, invalidCurveMachine])

        let validProfile = CAProfileAndFallbackMath.priorProfile(forSignature: "valid", in: table)
        #expect(validProfile?.msPerFrameC1 == 8.0)
        #expect(validProfile?.fixedOverheadMS == 150)
        #expect(validProfile?.degradationCurve == [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)])
        #expect(CAProfileAndFallbackMath.priorProfile(forSignature: "invalid-curve", in: table) == nil)
        #expect(CAProfileAndFallbackMath.priorProfile(forSignature: "missing", in: table) == nil)
    }

    @Test("sparse persisted prior curves are repaired conservatively before scoring")
    func sparsePersistedPriorCurves_areRepairedConservativelyBeforeScoring() throws {
        let sparseMachine = BenchmarkPriorMachine(
            signature: "sparse",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: "p",
            msPerFrameC1: 8.0,
            fixedOverheadMS: 150,
            avgCorpusFrameCount: 1_440,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 1,
                    videosPerMin: 20,
                    msPerVideoP50: 1_500,
                    msPerVideoP95: 2_000,
                    degradationRatio: 1.0
                ),
                BenchmarkPriorCell(
                    concurrency: 3,
                    videosPerMin: 10,
                    msPerVideoP50: 3_000,
                    msPerVideoP95: 3_600,
                    degradationRatio: 1.6
                ),
            ]
        )

        let profile = try #require(CAProfileAndFallbackMath.validatedPriorProfile(from: sparseMachine))
        #expect(profile.degradationCurve == [
            CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
            CADegradationPoint(concurrency: 2, ratioToC1: 1.3),
            CADegradationPoint(concurrency: 3, ratioToC1: 1.6),
        ])
    }

    @Test("acceptance corpus signature is order-independent and fixed-width")
    func acceptanceCorpusSignatureIsOrderIndependentAndFixedWidth() {
        let videoA = MediaFile(path: "/tmp/video-b.mp4", name: "video-b.mp4", type: .video, sizeBytes: 20_000)
        let videoB = MediaFile(path: "/tmp/video-a.mp4", name: "video-a.mp4", type: .video, sizeBytes: 10_000)

        let signatureAB = caAcceptanceCorpusSignature(videos: [videoA, videoB])
        let signatureBA = caAcceptanceCorpusSignature(videos: [videoB, videoA])

        #expect(signatureAB == signatureBA)
        #expect(signatureAB.count == 16)
    }

    @Test("frame estimate fallback uses the shared default when probe is missing")
    func frameEstimateFallbackUsesTheSharedDefaultWhenProbeIsMissing() async {
        let smallEstimates = await caEstimates(
            videos: [
                MediaFile(
                    path: "/tmp/does-not-exist.mp4",
                    name: "does-not-exist.mp4",
                    type: .video,
                    sizeBytes: 1_000
                )
            ],
            localMSPerFrameC1: 10.0
        )
        let largeEstimates = await caEstimates(
            videos: [
                MediaFile(
                    path: "/tmp/does-not-exist.mp4",
                    name: "does-not-exist.mp4",
                    type: .video,
                    sizeBytes: 250_000_000
                )
            ],
            localMSPerFrameC1: 10.0
        )

        let defaultFallbackFrameCount = CAProfileAndFallbackMath.fallbackFrameCount(
            durationSeconds: nil,
            frameCount: nil
        )

        #expect(smallEstimates.count == 1)
        #expect(largeEstimates.count == 1)
        #expect(smallEstimates[0] == defaultFallbackFrameCount)
        #expect(largeEstimates[0] == defaultFallbackFrameCount)
    }

    @Test("shared runtime fallback uses shared frame fallback and clamps invalid inputs")
    func sharedRuntimeFallbackUsesSharedFrameFallbackAndClampsInvalidInputs() {
        #expect(
            CAProfileAndFallbackMath.runtimeSecondsFallback(
                durationSeconds: nil,
                frameCount: nil,
                localMSPerFrameC1: 10.0,
                localFixedOverheadMS: 100
            ) == 14.5
        )
        #expect(
            CAProfileAndFallbackMath.runtimeSeconds(
                frameCount: 200,
                localMSPerFrameC1: 10.0,
                localFixedOverheadMS: 50,
                degradationFactor: 1.5
            ) == 3.05
        )
        #expect(
            CAProfileAndFallbackMath.runtimeSeconds(
                frameCount: 200,
                localMSPerFrameC1: .nan,
                localFixedOverheadMS: 50,
                degradationFactor: 1.5
            ) == nil
        )
    }
}
