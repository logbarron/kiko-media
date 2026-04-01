import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt CA model mode")
struct ThunderboltCAModelModeTests {
    private let localCurve = [
        CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
        CADegradationPoint(concurrency: 2, ratioToC1: 1.25),
    ]

    @Test("shared remote model uses capability-backed tier when exact prior is missing")
    func strictUsesCapabilityBackedTierWhenPriorMissing() throws {
        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .strict,
            caps: makeCaps(signature: "sig-remote", msPerFrameC1: 0.9, degradationCurve: [1: 1.0, 2: 1.15]),
            priorTable: BenchmarkPriorTable(),
            localMSPerFrameC1: 1.4,
            localCurve: localCurve
        )

        #expect(decision.machineID == "worker-a:7000")
        #expect(decision.msSource == "caps(remote)")
        #expect(decision.curveSource == "caps(remote)")
        #expect(decision.confidenceTier == CAMachineConfidenceTier.capabilityBacked.rawValue)
        #expect(decision.confidenceMultiplier == 1.25)
        #expect(decision.concurrencyCap == 1)
        #expect(decision.exclusionReason == nil)
    }

    @Test("strict includes remote when prior is complete")
    func strictIncludesWhenPriorPresent() {
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "sig-remote",
                msPerFrameC1: 0.82,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
                    BenchmarkPriorCell(concurrency: 2, videosPerMin: 16, msPerVideoP50: 5_200, msPerVideoP95: 6_400, degradationRatio: 1.30),
                ]
            ),
        ])

        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .strict,
            caps: makeCaps(signature: "sig-remote", msPerFrameC1: 0.91, degradationCurve: [1: 1.0, 2: 1.2]),
            priorTable: prior,
            localMSPerFrameC1: 1.4,
            localCurve: localCurve
        )

        #expect(decision.machineID == "worker-a:7000")
        #expect(decision.msPerFrameC1 == 0.82)
        #expect(decision.msSource == "prior(remote)")
        #expect(decision.curveSource == "prior(remote)")
        #expect(decision.confidenceTier == CAMachineConfidenceTier.exactPrior.rawValue)
        #expect(decision.confidenceMultiplier == 1.0)
        #expect(decision.concurrencyCap == nil)
        #expect(decision.exclusionReason == nil)
        #expect(!decision.fallbackActive)
    }

    @Test("production and benchmark remote scoring use explicit affine priors directly when present")
    func productionAndBenchmarkRemoteScoringUseExplicitAffinePriorsDirectlyWhenPresent() throws {
        let rawMSPerFrameC1 = 1.0
        let avgCorpusFrameCount = 400.0
        let c1P50MS = 400
        let remoteSignature = "sig-explicit-direct"
        let adjustedLegacyMS = try #require(
            CAProfileAndFallbackMath.adjustedRemotePriorEstimates(
                msPerFrameC1: rawMSPerFrameC1,
                fixedOverheadMS: 0,
                avgCorpusFrameCount: avgCorpusFrameCount,
                c1P50MS: Double(c1P50MS)
            )?.msPerFrameC1
        )
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: remoteSignature,
                msPerFrameC1: rawMSPerFrameC1,
                fixedOverheadMS: 0,
                avgCorpusFrameCount: avgCorpusFrameCount,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 60_000.0 / Double(c1P50MS),
                        msPerVideoP50: c1P50MS,
                        msPerVideoP95: c1P50MS + 80,
                        degradationRatio: 1.0
                    ),
                ]
            ),
        ])
        let localProfile = try #require(
            CAProfileAndFallbackMath.validatedPriorProfile(
                from: makePriorMachine(
                    signature: "sig-local",
                    msPerFrameC1: 8.0,
                    fixedOverheadMS: 0,
                    cells: [
                        BenchmarkPriorCell(
                            concurrency: 1,
                            videosPerMin: 10,
                            msPerVideoP50: 1_200,
                            msPerVideoP95: 1_400,
                            degradationRatio: 1.0
                        ),
                    ]
                )
            )
        )

        let benchmarkResult = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: [.local(index: 0), .remote(worker: makeWorker(host: "worker-a", slots: 1), index: 0)],
            reachableWorkers: [makeWorker(host: "worker-a", slots: 1)],
            workerCaps: [makeCaps(signature: remoteSignature, msPerFrameC1: rawMSPerFrameC1, degradationCurve: [1: 1.0])],
            priorTable: prior,
            remoteTxInEstimateMSByHost: [:],
            localMSPerFrameC1: localProfile.msPerFrameC1,
            localFixedOverheadMS: localProfile.fixedOverheadMS,
            localMSSource: "prior(local)",
            localCurve: localProfile.degradationCurve,
            localCurveSource: "prior(local)"
        )
        let productionResult = MediaProcessor.buildProductionTopologyModel(
            localSlotCount: 1,
            localProfile: localProfile,
            priorTable: prior,
            remoteSnapshot: [
                makeProductionWorkerSnapshot(
                    host: "worker-a",
                    port: 7000,
                    workerSignature: remoteSignature,
                    caps: makeCaps(signature: remoteSignature, msPerFrameC1: rawMSPerFrameC1, degradationCurve: [1: 1.0]),
                    liveMSPerFrameC1: nil,
                    transferOverheadEstimateMS: nil,
                    txOutEstimateMS: nil,
                    publishOverheadEstimateMS: nil,
                    slots: [(isBusy: false, isDown: false, estimatedRemainingMS: nil)]
                ),
            ]
        )

        let benchmarkRemote = try #require(benchmarkResult.machineProfiles.first { $0.id == "worker-a:7000" })
        let productionRemote = try #require(productionResult.machineProfiles.first { $0.id == "worker-a:7000" })
        #expect(benchmarkRemote.msPerFrameC1 == rawMSPerFrameC1)
        #expect(productionRemote.msPerFrameC1 == rawMSPerFrameC1)
        #expect(benchmarkRemote.fixedOverheadMS == 0)
        #expect(productionRemote.fixedOverheadMS == 0)
        #expect(adjustedLegacyMS < rawMSPerFrameC1)
    }

    @Test("auto uses capabilities when prior is missing")
    func autoUsesCapsWithoutPrior() {
        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .auto,
            caps: makeCaps(signature: "sig-remote", msPerFrameC1: 0.93, degradationCurve: [1: 1.0, 2: 1.2]),
            priorTable: BenchmarkPriorTable(),
            localMSPerFrameC1: 1.4,
            localCurve: localCurve
        )

        #expect(decision.machineID == "worker-a:7000")
        #expect(decision.msSource == "caps(remote)")
        #expect(decision.curveSource == "caps(remote)")
        #expect(decision.msPerFrameC1 == 0.93 * 1.25)
        #expect(decision.degradationCurve.count == 2)
        #expect(decision.confidenceTier == CAMachineConfidenceTier.capabilityBacked.rawValue)
        #expect(decision.confidenceMultiplier == 1.25)
        #expect(decision.concurrencyCap == 1)
        #expect(decision.fallbackActive)
    }

    @Test("auto falls back to local model when prior and caps are missing")
    func autoFallsBackToLocalModel() {
        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .auto,
            caps: makeCaps(signature: nil, msPerFrameC1: nil, degradationCurve: nil),
            priorTable: BenchmarkPriorTable(),
            localMSPerFrameC1: 1.35,
            localCurve: localCurve
        )

        #expect(decision.machineID == "worker-a:7000")
        #expect(decision.msSource == "fallback(local-c1)")
        #expect(decision.curveSource == "fallback(local-curve)")
        #expect(decision.msPerFrameC1 == 1.35 * 1.35)
        #expect(decision.degradationCurve == localCurve)
        #expect(decision.confidenceTier == CAMachineConfidenceTier.localFallback.rawValue)
        #expect(decision.confidenceMultiplier == 1.35)
        #expect(decision.concurrencyCap == 1)
        #expect(decision.fallbackActive)
    }

    @Test("partial prior falls through to capability-backed tier")
    func partialPriorFallsThroughToCapabilityBackedTier() {
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "sig-remote",
                msPerFrameC1: 0.78,
                cells: []
            ),
        ])

        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .auto,
            caps: makeCaps(signature: "sig-remote", msPerFrameC1: 0.95, degradationCurve: [1: 1.0, 2: 1.22]),
            priorTable: prior,
            localMSPerFrameC1: 1.35,
            localCurve: localCurve
        )

        #expect(decision.machineID == "worker-a:7000")
        #expect(decision.msPerFrameC1 == 0.95 * 1.25)
        #expect(decision.msSource == "caps(remote)")
        #expect(decision.curveSource == "caps(remote)")
        #expect(decision.degradationCurve.count == 2)
        #expect(decision.confidenceTier == CAMachineConfidenceTier.capabilityBacked.rawValue)
        #expect(decision.fallbackActive)
    }

    @Test("legacy remote priors keep the heuristic path until maintenance rewrites them")
    func legacyRemotePriorsKeepTheHeuristicPathUntilMaintenanceRewritesThem() throws {
        let rawMSPerFrameC1 = 1.0
        let avgCorpusFrameCount = 400.0
        let c1P50MS = 400
        let remoteSignature = "sig-legacy-heuristic"
        let adjustedLegacyMS = try #require(
            CAProfileAndFallbackMath.adjustedRemotePriorEstimates(
                msPerFrameC1: rawMSPerFrameC1,
                fixedOverheadMS: 0,
                avgCorpusFrameCount: avgCorpusFrameCount,
                c1P50MS: Double(c1P50MS)
            )?.msPerFrameC1
        )
        let result = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: [.local(index: 0), .remote(worker: makeWorker(host: "worker-a", slots: 1), index: 0)],
            reachableWorkers: [makeWorker(host: "worker-a", slots: 1)],
            workerCaps: [makeCaps(signature: remoteSignature, msPerFrameC1: rawMSPerFrameC1, degradationCurve: [1: 1.0])],
            priorTable: BenchmarkPriorTable(machines: [
                makePriorMachine(
                    signature: remoteSignature,
                    msPerFrameC1: rawMSPerFrameC1,
                    fixedOverheadMS: 0,
                    avgCorpusFrameCount: avgCorpusFrameCount,
                    affineModelSource: .legacyHeuristic,
                    cells: [
                        BenchmarkPriorCell(
                            concurrency: 1,
                            videosPerMin: 60_000.0 / Double(c1P50MS),
                            msPerVideoP50: c1P50MS,
                            msPerVideoP95: c1P50MS + 80,
                            degradationRatio: 1.0
                        ),
                    ]
                ),
            ]),
            remoteTxInEstimateMSByHost: [:],
            localMSPerFrameC1: 8.0,
            localMSSource: "prior(local)",
            localCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
            localCurveSource: "prior(local)"
        )

        let remoteMachine = try #require(result.machineProfiles.first { $0.id == "worker-a:7000" })
        #expect(remoteMachine.msPerFrameC1 == adjustedLegacyMS)
        #expect(result.diagnostics.remotePriorGap)
        #expect(result.diagnostics.exactPriorSlotCount == 1)
    }

    @Test("legacy remote priors with fixed overhead still require migration until source is explicit")
    func legacyRemotePriorsWithFixedOverheadStillRequireMigrationUntilSourceIsExplicit() {
        let remoteSignature = "sig-legacy-fixed-overhead"
        let result = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: [.local(index: 0), .remote(worker: makeWorker(host: "worker-a", slots: 1), index: 0)],
            reachableWorkers: [makeWorker(host: "worker-a", slots: 1)],
            workerCaps: [makeCaps(signature: remoteSignature, msPerFrameC1: 0.9, degradationCurve: [1: 1.0])],
            priorTable: BenchmarkPriorTable(machines: [
                makePriorMachine(
                    signature: remoteSignature,
                    msPerFrameC1: 0.9,
                    fixedOverheadMS: 120,
                    avgCorpusFrameCount: 400,
                    affineModelSource: .legacyHeuristic,
                    cells: [
                        BenchmarkPriorCell(
                            concurrency: 1,
                            videosPerMin: 10,
                            msPerVideoP50: 600,
                            msPerVideoP95: 700,
                            degradationRatio: 1.0
                        ),
                    ]
                ),
            ]),
            remoteTxInEstimateMSByHost: [:],
            localMSPerFrameC1: 8.0,
            localMSSource: "prior(local)",
            localCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
            localCurveSource: "prior(local)"
        )

        let remoteMachine = result.machineProfiles.first { $0.id == "worker-a:7000" }
        #expect(remoteMachine?.fixedOverheadMS == 120)
        #expect(result.diagnostics.remotePriorGap)
        #expect(result.diagnostics.exactPriorSlotCount == 1)
    }

    @Test("os-only signature drift uses hardware-compatible prior tier")
    func osOnlySignatureDriftUsesHardwareCompatiblePriorTier() {
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=\(defaultVideoPreset)",
                msPerFrameC1: 0.82,
                fixedOverheadMS: 120,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
                    BenchmarkPriorCell(concurrency: 2, videosPerMin: 16, msPerVideoP50: 5_200, msPerVideoP95: 6_400, degradationRatio: 1.30),
                    BenchmarkPriorCell(concurrency: 3, videosPerMin: 18, msPerVideoP50: 6_400, msPerVideoP95: 7_800, degradationRatio: 1.55),
                ]
            ),
        ])

        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .auto,
            caps: makeCaps(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.4;pcores=4;preset=\(defaultVideoPreset)",
                msPerFrameC1: 0.91,
                degradationCurve: [1: 1.0, 2: 1.2, 3: 1.4]
            ),
            priorTable: prior,
            localMSPerFrameC1: 1.4,
            localCurve: localCurve
        )

        #expect(decision.machineID == "worker-a:7000")
        #expect(decision.msSource == "prior(hardware-compatible)")
        #expect(decision.curveSource == "prior(hardware-compatible)")
        #expect(decision.confidenceTier == CAMachineConfidenceTier.hardwareCompatiblePrior.rawValue)
        #expect(decision.confidenceMultiplier == 1.15)
        #expect(decision.concurrencyCap == 1)
        #expect(decision.msPerFrameC1 == 0.82 * 1.15)
        #expect(decision.fixedOverheadMS == 120 * 1.15)
    }

    @Test("lower-confidence tiers clamp curves conservatively and keep multipliers visible")
    func lowerConfidenceTiersClampCurvesConservativelyAndKeepMultipliersVisible() {
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=\(defaultVideoPreset)",
                msPerFrameC1: 1.0,
                fixedOverheadMS: 100,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
                    BenchmarkPriorCell(concurrency: 2, videosPerMin: 8, msPerVideoP50: 5_200, msPerVideoP95: 6_400, degradationRatio: 1.25),
                    BenchmarkPriorCell(concurrency: 3, videosPerMin: 6, msPerVideoP50: 6_700, msPerVideoP95: 8_100, degradationRatio: 1.60),
                ]
            ),
        ])

        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .auto,
            caps: makeCaps(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.9;pcores=4;preset=\(defaultVideoPreset)",
                msPerFrameC1: 0.8,
                degradationCurve: [1: 1.0, 2: 1.1, 3: 1.2]
            ),
            priorTable: prior,
            localMSPerFrameC1: 1.4,
            localCurve: [
                CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
                CADegradationPoint(concurrency: 2, ratioToC1: 1.2),
                CADegradationPoint(concurrency: 3, ratioToC1: 1.4),
            ]
        )

        #expect(decision.confidenceTier == CAMachineConfidenceTier.hardwareCompatiblePrior.rawValue)
        #expect(decision.confidenceMultiplier == 1.15)
        #expect(decision.concurrencyCap == 1)
        #expect(decision.degradationCurve == [
            CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
            CADegradationPoint(concurrency: 2, ratioToC1: 1.60),
            CADegradationPoint(concurrency: 3, ratioToC1: 1.60),
        ])
    }

    @Test("lower-confidence modeled slots remain scoreable but dispatch behind a one-lane cap")
    func lowerConfidenceModeledSlotsRemainScoreableButDispatchBehindOneLaneCap() {
        let topology = CATopologyModelBuilder.build(
            mode: .auto,
            localSlotCount: 1,
            localProfile: CATopologyModelLocalProfile(
                machineID: "local",
                msPerFrameC1: 10.0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                msSource: "prior(local)",
                curveSource: "prior(local)"
            ),
            priorTable: BenchmarkPriorTable(),
            remoteWorkers: [
                CATopologyModelRemoteWorkerInput(
                    host: "worker-caps",
                    port: 7000,
                    workerSignature: "sig-caps",
                    caps: makeCaps(signature: "sig-caps", msPerFrameC1: 0.9, degradationCurve: [1: 1.0, 2: 1.1]),
                    transferOverheadEstimateMS: 0,
                    txOutEstimateMS: 0,
                    publishOverheadEstimateMS: 0,
                    slots: [
                        CATopologyModelRemoteSlotInput(slotIndex: 1, isExecutable: true),
                        CATopologyModelRemoteSlotInput(slotIndex: 2, isExecutable: true),
                    ]
                ),
            ]
        )

        #expect(topology.diagnostics.remoteExecutableSlotCount == 2)
        #expect(topology.diagnostics.totalExecutableSlotCount == 3)
        #expect(topology.diagnostics.capabilityBackedSlotCount == 2)

        let remoteRow = topology.diagnostics.coverageRows.first { $0.host == "worker-caps" }
        #expect(remoteRow?.modeledSlots == 2)
        #expect(remoteRow?.confidenceTier == .capabilityBacked)

        let remoteMachine = topology.machineProfiles.first { $0.id == "worker-caps:7000" }
        #expect(remoteMachine?.modeledConcurrencyCap == 1)

        let assembly = CAProjectedSlotSelection.assemble(
            nowMS: 0,
            localRemainingMS: [10_000],
            topology: topology,
            remoteWorkers: [
                makeProductionWorkerSnapshot(
                    host: "worker-caps",
                    port: 7000,
                    workerSignature: "sig-caps",
                    caps: makeCaps(signature: "sig-caps", msPerFrameC1: 0.9, degradationCurve: [1: 1.0, 2: 1.1]),
                    liveMSPerFrameC1: nil,
                    transferOverheadEstimateMS: 0,
                    txOutEstimateMS: 0,
                    publishOverheadEstimateMS: 0,
                    slots: [
                        (isBusy: false, isDown: false, estimatedRemainingMS: nil),
                        (isBusy: false, isDown: false, estimatedRemainingMS: nil),
                    ]
                ),
            ]
        )
        let plan = assembly.plan(
            pendingJobs: [
                .init(token: 0, job: CAJob(id: "job-0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 120)),
                .init(token: 1, job: CAJob(id: "job-1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 120)),
            ],
            nowMS: 0,
            maxCount: 2
        )
        let scoredBatch = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: 0, job: CAJob(id: "job-0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 120)),
                .init(token: 1, job: CAJob(id: "job-1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 120)),
            ],
            machines: assembly.machineContexts.map(\.machine),
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .includeFutureReady
        )

        #expect(plan.dispatches.count == 1)
        #expect(plan.dispatches.first?.route == .remote(workerIndex: 0))
        #expect(scoredBatch.picks.count == 2)
        #expect(scoredBatch.picks.filter { $0.slot.machineIndex == 1 }.count == 2)
        #expect(scoredBatch.picks.allSatisfy { $0.score.clampedConcurrency == 1 })
        let readyTimes = scoredBatch.picks.map(\.score.tReadySlotMS).sorted()
        #expect(readyTimes.first == 0)
        #expect((readyTimes.last ?? 0) > 0)
    }

    @Test("shared builder keeps production and benchmark modeled machines aligned")
    func sharedBuilderKeepsProductionAndBenchmarkModeledMachinesAligned() throws {
        let localProfile = try #require(
            CAProfileAndFallbackMath.validatedPriorProfile(
                from: makePriorMachine(
                    signature: "sig-local",
                    msPerFrameC1: 1.10,
                    fixedOverheadMS: 95,
                    cells: [
                        BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_800, msPerVideoP95: 5_800, degradationRatio: 1.0),
                        BenchmarkPriorCell(concurrency: 2, videosPerMin: 16, msPerVideoP50: 6_000, msPerVideoP95: 7_200, degradationRatio: 1.28),
                    ]
                )
            )
        )
        let priorTable = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "sig-exact",
                msPerFrameC1: 0.78,
                fixedOverheadMS: 60,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 11, msPerVideoP50: 3_200, msPerVideoP95: 4_100, degradationRatio: 1.0),
                    BenchmarkPriorCell(concurrency: 2, videosPerMin: 18, msPerVideoP50: 4_100, msPerVideoP95: 5_000, degradationRatio: 1.18),
                ]
            ),
            makePriorMachine(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=\(defaultVideoPreset)",
                msPerFrameC1: 0.84,
                fixedOverheadMS: 70,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 3_500, msPerVideoP95: 4_300, degradationRatio: 1.0),
                    BenchmarkPriorCell(concurrency: 2, videosPerMin: 15, msPerVideoP50: 4_500, msPerVideoP95: 5_500, degradationRatio: 1.24),
                ]
            ),
        ])
        let reachableWorkers = [
            makeWorker(host: "worker-exact", slots: 2),
            makeWorker(host: "worker-compatible", slots: 1),
            makeWorker(host: "worker-fallback", slots: 1),
        ]
        let workerCaps: [WorkerCaps?] = [
            makeCaps(signature: "sig-exact", msPerFrameC1: 0.74, degradationCurve: [1: 1.0, 2: 1.12]),
            makeCaps(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.4;pcores=4;preset=\(defaultVideoPreset)",
                msPerFrameC1: 0.80,
                degradationCurve: [1: 1.0, 2: 1.18]
            ),
            nil,
        ]
        let remoteLiveMSPerFrameC1ByHost = [
            "worker-exact": 0.72,
            "worker-compatible": 0.82,
        ]
        let remoteTxInEstimateMSByHost = [
            "worker-exact": 12.0,
            "worker-compatible": 18.0,
            "worker-fallback": 24.0,
        ]
        let remoteTxOutEstimateMSByHost = [
            "worker-exact": 6.0,
            "worker-compatible": 7.5,
            "worker-fallback": 8.0,
        ]
        let remotePublishOverheadEstimateMSByHost = [
            "worker-exact": 2.0,
            "worker-compatible": 3.0,
            "worker-fallback": 4.0,
        ]

        let benchmarkResult = buildThunderboltCAModelProfiles(
            mode: .auto,
            port: 7000,
            slots: caSlots(localSlots: 2, reachableWorkers: reachableWorkers),
            reachableWorkers: reachableWorkers,
            workerCaps: workerCaps,
            priorTable: priorTable,
            remoteLiveMSPerFrameC1ByHost: remoteLiveMSPerFrameC1ByHost,
            remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost,
            remoteTxOutEstimateMSByHost: remoteTxOutEstimateMSByHost,
            remotePublishOverheadEstimateMSByHost: remotePublishOverheadEstimateMSByHost,
            localMSPerFrameC1: localProfile.msPerFrameC1,
            localFixedOverheadMS: localProfile.fixedOverheadMS,
            localMSSource: "prior(local)",
            localCurve: localProfile.degradationCurve,
            localCurveSource: "prior(local)"
        )
        let productionResult = MediaProcessor.buildProductionTopologyModel(
            localSlotCount: 2,
            localProfile: localProfile,
            priorTable: priorTable,
            remoteSnapshot: [
                makeProductionWorkerSnapshot(
                    host: "worker-exact",
                    port: 7000,
                    workerSignature: "sig-exact",
                    caps: workerCaps[0],
                    liveMSPerFrameC1: remoteLiveMSPerFrameC1ByHost["worker-exact"],
                    transferOverheadEstimateMS: remoteTxInEstimateMSByHost["worker-exact"],
                    txOutEstimateMS: remoteTxOutEstimateMSByHost["worker-exact"],
                    publishOverheadEstimateMS: remotePublishOverheadEstimateMSByHost["worker-exact"],
                    slots: [(isBusy: false, isDown: false, estimatedRemainingMS: nil), (isBusy: false, isDown: false, estimatedRemainingMS: nil)]
                ),
                makeProductionWorkerSnapshot(
                    host: "worker-compatible",
                    port: 7000,
                    workerSignature: "chip=Apple M4;ecores=6;encoders=1;os=26.4;pcores=4;preset=\(defaultVideoPreset)",
                    caps: workerCaps[1],
                    liveMSPerFrameC1: remoteLiveMSPerFrameC1ByHost["worker-compatible"],
                    transferOverheadEstimateMS: remoteTxInEstimateMSByHost["worker-compatible"],
                    txOutEstimateMS: remoteTxOutEstimateMSByHost["worker-compatible"],
                    publishOverheadEstimateMS: remotePublishOverheadEstimateMSByHost["worker-compatible"],
                    slots: [(isBusy: false, isDown: false, estimatedRemainingMS: nil)]
                ),
                makeProductionWorkerSnapshot(
                    host: "worker-fallback",
                    port: 7000,
                    workerSignature: nil,
                    caps: nil,
                    liveMSPerFrameC1: nil,
                    transferOverheadEstimateMS: remoteTxInEstimateMSByHost["worker-fallback"],
                    txOutEstimateMS: remoteTxOutEstimateMSByHost["worker-fallback"],
                    publishOverheadEstimateMS: remotePublishOverheadEstimateMSByHost["worker-fallback"],
                    slots: [(isBusy: false, isDown: false, estimatedRemainingMS: nil)]
                ),
            ]
        )

        #expect(sharedMachineProfileTuples(productionResult.machineProfiles) == thunderboltMachineProfileTuples(benchmarkResult.machineProfiles))
        #expect(sharedSlotBindingTuples(productionResult.slotBindings) == thunderboltSlotBindingTuples(benchmarkResult.slotBindings))
        #expect(productionResult.machineIndexByHost == benchmarkResult.machineIndexByHost)
        #expect(sharedModelInputTuples(productionResult.modelInputs) == thunderboltModelInputTuples(benchmarkResult.modelInputs))
        #expect(sharedCoverageTuples(productionResult.diagnostics.coverageRows) == thunderboltCoverageTuples(benchmarkResult.diagnostics.coverageRows))
        #expect(sharedDiagnosticSummary(productionResult.diagnostics) == thunderboltDiagnosticSummary(benchmarkResult.diagnostics))
    }

    @Test("production executable capacity matches FIFO executable capacity")
    func productionExecutableCapacityMatchesFIFOExecutableCapacity() throws {
        let localProfile = try #require(
            CAProfileAndFallbackMath.validatedPriorProfile(
                from: makePriorMachine(
                    signature: "sig-local",
                    msPerFrameC1: 1.15,
                    fixedOverheadMS: 80,
                    cells: [
                        BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_400, msPerVideoP95: 5_200, degradationRatio: 1.0),
                    ]
                )
            )
        )
        let remoteSnapshot = [
            makeProductionWorkerSnapshot(
                host: "worker-a",
                port: 7000,
                workerSignature: nil,
                caps: nil,
                liveMSPerFrameC1: nil,
                transferOverheadEstimateMS: 16,
                txOutEstimateMS: 7,
                publishOverheadEstimateMS: 3,
                slots: [
                    (isBusy: false, isDown: false, estimatedRemainingMS: nil),
                    (isBusy: false, isDown: true, estimatedRemainingMS: nil),
                    (isBusy: true, isDown: false, estimatedRemainingMS: 120),
                ]
            ),
            makeProductionWorkerSnapshot(
                host: "worker-b",
                port: 7000,
                workerSignature: "sig-caps",
                caps: makeCaps(signature: "sig-caps", msPerFrameC1: 0.92, degradationCurve: [1: 1.0, 2: 1.10]),
                liveMSPerFrameC1: nil,
                transferOverheadEstimateMS: 18,
                txOutEstimateMS: 8,
                publishOverheadEstimateMS: 3,
                slots: [
                    (isBusy: false, isDown: false, estimatedRemainingMS: nil),
                    (isBusy: false, isDown: false, estimatedRemainingMS: nil),
                ]
            ),
            makeProductionWorkerSnapshot(
                host: "worker-c",
                port: 7000,
                workerSignature: nil,
                caps: nil,
                liveMSPerFrameC1: nil,
                transferOverheadEstimateMS: 22,
                txOutEstimateMS: 9,
                publishOverheadEstimateMS: 4,
                slots: [
                    (isBusy: false, isDown: true, estimatedRemainingMS: nil),
                ]
            ),
        ]

        let result = MediaProcessor.buildProductionTopologyModel(
            localSlotCount: 2,
            localProfile: localProfile,
            priorTable: BenchmarkPriorTable(),
            remoteSnapshot: remoteSnapshot
        )
        let fifoExecutableCapacity = 2 + remoteSnapshot.reduce(into: 0) { partial, worker in
            partial += worker.slots.reduce(into: 0) { workerTotal, slot in
                if !slot.isDown {
                    workerTotal += 1
                }
            }
        }

        #expect(result.diagnostics.localExecutableSlotCount == 2)
        #expect(result.diagnostics.remoteExecutableSlotCount == 4)
        #expect(result.diagnostics.totalExecutableSlotCount == fifoExecutableCapacity)
        #expect(result.slotBindings.count == fifoExecutableCapacity)
        #expect(result.diagnostics.hardwareCompatiblePriorSlotCount == 0)
        #expect(result.diagnostics.capabilityBackedSlotCount == 2)
        #expect(result.diagnostics.localFallbackSlotCount == 2)

        let coverage = Dictionary(uniqueKeysWithValues: result.diagnostics.coverageRows.map { ($0.host, $0) })
        #expect(coverage["worker-a"]?.reachableSlots == 3)
        #expect(coverage["worker-a"]?.executableSlots == 2)
        #expect(coverage["worker-a"]?.modeledSlots == 2)
        #expect(coverage["worker-a"]?.confidenceTier == .localFallback)
        #expect(coverage["worker-b"]?.reachableSlots == 2)
        #expect(coverage["worker-b"]?.executableSlots == 2)
        #expect(coverage["worker-b"]?.modeledSlots == 2)
        #expect(coverage["worker-b"]?.confidenceTier == .capabilityBacked)
        #expect(coverage["worker-c"]?.reachableSlots == 1)
        #expect(coverage["worker-c"]?.executableSlots == 0)
        #expect(coverage["worker-c"]?.modeledSlots == 0)
    }

    @Test("benchmark reporting exposes executable capacity and confidence tiers")
    func benchmarkReportingExposesExecutableCapacityAndConfidenceTiers() async throws {
        let result = buildThunderboltCAModelProfiles(
            mode: .auto,
            port: 7000,
            slots: caSlots(
                localSlots: 1,
                reachableWorkers: [
                    makeWorker(host: "worker-exact", slots: 1),
                    makeWorker(host: "worker-compatible", slots: 1),
                    makeWorker(host: "worker-caps", slots: 1),
                    makeWorker(host: "worker-local", slots: 1),
                ]
            ),
            reachableWorkers: [
                makeWorker(host: "worker-exact", slots: 1),
                makeWorker(host: "worker-compatible", slots: 1),
                makeWorker(host: "worker-caps", slots: 1),
                makeWorker(host: "worker-local", slots: 1),
            ],
            workerCaps: [
                makeCaps(signature: "sig-exact", msPerFrameC1: 0.7, degradationCurve: [1: 1.0]),
                makeCaps(
                    signature: "chip=Apple M4;ecores=6;encoders=1;os=26.5;pcores=4;preset=\(defaultVideoPreset)",
                    msPerFrameC1: 0.8,
                    degradationCurve: [1: 1.0]
                ),
                makeCaps(signature: "sig-caps", msPerFrameC1: 0.9, degradationCurve: [1: 1.0]),
                nil,
            ],
            priorTable: BenchmarkPriorTable(machines: [
                makePriorMachine(
                    signature: "sig-exact",
                    msPerFrameC1: 0.75,
                    cells: [
                        BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 3_000, msPerVideoP95: 3_800, degradationRatio: 1.0),
                    ]
                ),
                makePriorMachine(
                    signature: "chip=Apple M4;ecores=6;encoders=1;os=26.4;pcores=4;preset=\(defaultVideoPreset)",
                    msPerFrameC1: 0.82,
                    cells: [
                        BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 3_200, msPerVideoP95: 3_900, degradationRatio: 1.0),
                    ]
                ),
            ]),
            remoteTxInEstimateMSByHost: [
                "worker-exact": 10,
                "worker-compatible": 12,
                "worker-caps": 14,
                "worker-local": 16,
            ],
            localMSPerFrameC1: 1.2,
            localMSSource: "prior(local)",
            localCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
            localCurveSource: "prior(local)"
        )

        let report = try await captureBenchmarkReport {
            printThunderboltCAModelEligibilitySummary(
                diagnostics: result.diagnostics,
                modelInputs: result.modelInputs
            )
        }

        #expect(report.contains("Executable capacity"))
        #expect(report.contains("5 total (1 local + 4 remote)"))
        #expect(report.contains("Confidence tiers"))
        #expect(report.contains("exact=1, compatible=1, caps=1, local=1"))
        #expect(report.contains(" exec "))
        #expect(report.contains(" tier "))
        #expect(report.contains(" mult "))
        #expect(report.contains(" cap "))
    }

    @Test("remote telemetry uses provided estimates")
    func remoteTelemetryUsesProvidedEstimates() {
        let decision = resolveThunderboltCARemoteModelDecision(
            host: "worker-a",
            port: 7000,
            mode: .auto,
            caps: makeCaps(signature: "sig-remote", msPerFrameC1: 0.93, degradationCurve: [1: 1.0, 2: 1.2]),
            priorTable: BenchmarkPriorTable(),
            remoteTxInEstimateMS: 87.5,
            remoteTxOutEstimateMS: 21.0,
            remotePublishOverheadEstimateMS: 9.5,
            localMSPerFrameC1: 1.4,
            localCurve: localCurve
        )

        #expect(decision.txInMS == 87.5)
        #expect(decision.txOutMS == 21.0)
        #expect(decision.publishOverheadMS == 9.5)
    }

    @Test("tail telemetry probe uses measured round-trip estimates when fields are missing")
    func tailTelemetryProbeUsesMeasuredRoundTripEstimatesWhenMissing() throws {
        let sample = try makeTailProbeSampleVideo()
        defer { try? FileManager.default.removeItem(atPath: sample.path) }

        let workerA = makeWorker(host: "worker-a", slots: 1)
        let workerB = makeWorker(host: "worker-b", slots: 1)
        let estimates = measureThunderboltCATailTelemetryEstimates(
            workers: [workerA, workerB],
            videos: [sample],
            port: 7000,
            connectTimeout: 100,
            providedTxOutEstimateMSByHost: ["worker-a": 42],
            providedPublishOverheadEstimateMSByHost: [:],
            roundTripRunner: { worker, _, _, _, _, _ in
                switch worker.host {
                case "worker-a":
                    return ThunderboltRoundTripResult(
                        success: true,
                        sendSeconds: 0.01,
                        processNanos: 5_000_000,
                        receiveSeconds: 0.01,
                        totalSeconds: 0.02,
                        txOutMS: 999,
                        publishOverheadMS: 12
                    )
                case "worker-b":
                    return ThunderboltRoundTripResult(
                        success: true,
                        sendSeconds: 0.01,
                        processNanos: 5_000_000,
                        receiveSeconds: 0.01,
                        totalSeconds: 0.02,
                        txOutMS: 27,
                        publishOverheadMS: 8
                    )
                default:
                    return ThunderboltRoundTripResult(
                        success: false,
                        sendSeconds: 0,
                        processNanos: 0,
                        receiveSeconds: 0,
                        totalSeconds: 0
                    )
                }
            }
        )

        #expect(estimates.txOutMSByHost["worker-a"] == nil)
        #expect(estimates.publishOverheadMSByHost["worker-a"] == 12)
        #expect(estimates.txOutMSByHost["worker-b"] == 27)
        #expect(estimates.publishOverheadMSByHost["worker-b"] == 8)
    }

    @Test("tail telemetry probe ignores failed samples even when telemetry fields are populated")
    func tailTelemetryProbeIgnoresFailedSamplesEvenWhenTelemetryFieldsArePopulated() throws {
        let sample = try makeTailProbeSampleVideo()
        defer { try? FileManager.default.removeItem(atPath: sample.path) }

        let workerA = makeWorker(host: "worker-a", slots: 1)
        let workerB = makeWorker(host: "worker-b", slots: 1)
        let estimates = measureThunderboltCATailTelemetryEstimates(
            workers: [workerA, workerB],
            videos: [sample],
            port: 7000,
            connectTimeout: 100,
            roundTripRunner: { worker, _, _, _, _, _ in
                switch worker.host {
                case "worker-a":
                    return ThunderboltRoundTripResult(
                        success: false,
                        sendSeconds: 0.01,
                        processNanos: 5_000_000,
                        receiveSeconds: 0.01,
                        totalSeconds: 0.02,
                        txOutMS: 321,
                        publishOverheadMS: 123
                    )
                case "worker-b":
                    return ThunderboltRoundTripResult(
                        success: true,
                        sendSeconds: 0.01,
                        processNanos: 5_000_000,
                        receiveSeconds: 0.01,
                        totalSeconds: 0.02,
                        txOutMS: 27,
                        publishOverheadMS: 8
                    )
                default:
                    return ThunderboltRoundTripResult(
                        success: false,
                        sendSeconds: 0,
                        processNanos: 0,
                        receiveSeconds: 0,
                        totalSeconds: 0
                    )
                }
            }
        )

        #expect(estimates.txOutMSByHost["worker-a"] == nil)
        #expect(estimates.publishOverheadMSByHost["worker-a"] == nil)
        #expect(estimates.txOutMSByHost["worker-b"] == 27)
        #expect(estimates.publishOverheadMSByHost["worker-b"] == 8)
    }

    @Test("ca run json artifact includes decision timing fields")
    func caRunJSONArtifactIncludesDecisionTimingFields() async throws {
        let sample = try makeTailProbeSampleVideo()
        defer { try? FileManager.default.removeItem(atPath: sample.path) }

        let run = try await runThunderboltCA(
            corpus: [sample],
            preset: defaultVideoPreset,
            timeout: 1,
            hardware: makeHardwareProfile(),
            policy: .complexityAware,
            profile: .allAtOnce,
            preparedSetup: makePreparedCARunSetup(videos: [sample]),
            localVideoRunner: { _, _, _, _, _, _ in
                try? await Task.sleep(nanoseconds: 5_000_000)
                return true
            },
            roundTripRunner: { _, _, _, _, _, _ in
                ThunderboltRoundTripResult(
                    success: false,
                    sendSeconds: 0,
                    processNanos: 0,
                    receiveSeconds: 0,
                    totalSeconds: 0
                )
            }
        )

        let prediction = try #require(run.observability.predictions.first)
        #expect(prediction.decisionAtSeconds >= 0)
        #expect(prediction.decisionAtSeconds.isFinite)
        #expect(prediction.predictedSlotReadyMS != nil)
        #expect(prediction.predictedStartMS != nil)
        #expect(prediction.predictedDoneMS != nil)
        #expect(prediction.actualStartMS != nil)
        #expect(prediction.actualDoneMS != nil)
        #expect(prediction.waited == false)
        let job = try #require(run.result.jobs.first)
        #expect(job.actualExecutor == "local")

        let artifactURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-observability-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: artifactURL) }

        try writeThunderboltCAJSON(run.result, toPath: artifactURL.path)

        let data = try Data(contentsOf: artifactURL)
        let json = try #require(JSONSerialization.jsonObject(with: data) as? [String: Any])
        let jobs = try #require(json["jobs"] as? [[String: Any]])
        #expect(jobs.count == 1)
        #expect(jobs.first?["actualExecutor"] as? String == "local")
        let observability = try #require(json["observability"] as? [String: Any])
        let predictions = try #require(observability["predictions"] as? [[String: Any]])
        let sampleJSON = try #require(predictions.first)

        let decisionAtSeconds = try #require((sampleJSON["decision_at_seconds"] as? NSNumber)?.doubleValue)
        let predictedSlotReadyMS = try #require((sampleJSON["predicted_slot_ready_ms"] as? NSNumber)?.doubleValue)
        let predictedStartMS = try #require((sampleJSON["predicted_start_ms"] as? NSNumber)?.doubleValue)
        let predictedDoneMS = try #require((sampleJSON["predicted_done_ms"] as? NSNumber)?.doubleValue)
        let actualStartMS = try #require((sampleJSON["actual_start_ms"] as? NSNumber)?.doubleValue)
        let actualDoneMS = try #require((sampleJSON["actual_done_ms"] as? NSNumber)?.doubleValue)

        #expect(decisionAtSeconds >= 0)
        #expect(decisionAtSeconds.isFinite)
        #expect(predictedSlotReadyMS >= 0)
        #expect(predictedStartMS >= predictedSlotReadyMS)
        #expect(predictedDoneMS >= predictedStartMS)
        #expect(actualStartMS >= 0)
        #expect(actualDoneMS >= actualStartMS)
        #expect(sampleJSON["waited"] as? Bool == false)
        #expect(sampleJSON["predicted_saved_vs_ready_now_ms"] == nil)
        #expect(sampleJSON["actual_saved_vs_ready_now_ms"] == nil)

        let solverTelemetryJSON = try #require(observability["solver_telemetry"] as? [[String: Any]])
        #expect(!solverTelemetryJSON.isEmpty)
        let firstTelemetry = try #require(solverTelemetryJSON.first)
        #expect(firstTelemetry["nodes_visited"] as? Int != nil)
        #expect(firstTelemetry["pruned_by_pick_count"] as? Int != nil)
        #expect(firstTelemetry["pruned_by_makespan"] as? Int != nil)
        #expect(firstTelemetry["pruned_by_completion_sum"] as? Int != nil)
        #expect(firstTelemetry["incumbent_updates"] as? Int != nil)
        #expect(firstTelemetry["max_depth"] as? Int != nil)
        #expect((firstTelemetry["solver_wall_ms"] as? NSNumber)?.doubleValue != nil)
    }

    @Test("prediction audit uses end-to-end completion quantity")
    func predictionAuditUsesEndToEndCompletionQuantity() {
        let summary = aggregateCAPredictionBuckets([
            makeObservedRun(
                predictions: [
                    ThunderboltCAPredictionSample(
                        machineID: "worker-a:7000",
                        decisionAtSeconds: 0,
                        predictedSlotReadyMS: 60,
                        predictedStartMS: 80,
                        predictedDoneMS: 100,
                        actualStartMS: 180,
                        actualDoneMS: 200,
                        waited: true,
                        success: true,
                        executorMismatch: false
                    ),
                ]
            ),
        ])

        #expect(summary.total == 1)
        #expect(summary.included == 1)
        #expect(summary.failed == 0)
        #expect(summary.mismatch == 0)
        #expect(summary.noModel == 0)
        #expect(summary.rows.count == 1)
        #expect(summary.rows.first?.label == ">1.20x")
        #expect(abs((summary.rows.first?.meanRatio ?? 0) - 2.0) < 0.0001)
        #expect(abs((summary.rows.first?.meanAbsErrorPercent ?? 0) - 100.0) < 0.0001)
    }

    @Test("build model propagates remote telemetry estimates")
    func buildModelPropagatesRemoteTelemetryEstimates() {
        let worker = makeWorker(host: "worker-a", slots: 1)
        let slots: [ThunderboltCASlot] = [
            .local(index: 0),
            .remote(worker: worker, index: 0),
        ]
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "sig-remote",
                msPerFrameC1: 0.82,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
                    BenchmarkPriorCell(concurrency: 2, videosPerMin: 16, msPerVideoP50: 5_200, msPerVideoP95: 6_400, degradationRatio: 1.30),
                ]
            ),
        ])

        let result = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: slots,
            reachableWorkers: [worker],
            workerCaps: [makeCaps(signature: "sig-remote", msPerFrameC1: 0.91, degradationCurve: [1: 1.0, 2: 1.2])],
            priorTable: prior,
            remoteTxInEstimateMSByHost: ["worker-a": 64],
            remoteTxOutEstimateMSByHost: ["worker-a": 23],
            remotePublishOverheadEstimateMSByHost: ["worker-a": 11],
            localMSPerFrameC1: 1.4,
            localMSSource: "prior(local)",
            localCurve: localCurve,
            localCurveSource: "prior(local)"
        )

        let remoteMachine = result.machineProfiles.first { $0.id == "worker-a:7000" }
        let remoteInput = result.modelInputs.first { $0.machineID == "worker-a:7000" }
        #expect(remoteMachine?.txInMS == 64)
        #expect(remoteMachine?.txOutMS == 23)
        #expect(remoteMachine?.publishOverheadMS == 11)
        #expect(remoteInput?.txInMS == 64)
        #expect(remoteInput?.txOutMS == 23)
        #expect(remoteInput?.publishOverheadMS == 11)
    }

    @Test("build model falls back to zero telemetry when estimates are unavailable")
    func buildModelFallsBackToZeroRemoteTelemetryWithoutEstimate() {
        let worker = makeWorker(host: "worker-a", slots: 1)
        let slots: [ThunderboltCASlot] = [
            .local(index: 0),
            .remote(worker: worker, index: 0),
        ]
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "sig-remote",
                msPerFrameC1: 0.82,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
                ]
            ),
        ])

        let result = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: slots,
            reachableWorkers: [worker],
            workerCaps: [makeCaps(signature: "sig-remote", msPerFrameC1: 0.91, degradationCurve: [1: 1.0, 2: 1.2])],
            priorTable: prior,
            remoteTxInEstimateMSByHost: [:],
            localMSPerFrameC1: 1.4,
            localMSSource: "prior(local)",
            localCurve: localCurve,
            localCurveSource: "prior(local)"
        )

        let remoteMachine = result.machineProfiles.first { $0.id == "worker-a:7000" }
        let remoteInput = result.modelInputs.first { $0.machineID == "worker-a:7000" }
        #expect(remoteMachine?.txInMS == 0)
        #expect(remoteMachine?.txOutMS == 0)
        #expect(remoteMachine?.publishOverheadMS == 0)
        #expect(remoteInput?.txInMS == 0)
        #expect(remoteInput?.txOutMS == 0)
        #expect(remoteInput?.publishOverheadMS == 0)
    }

    @Test("non-zero tail telemetry changes CA pick outcome")
    func nonZeroTailTelemetry_changesCAPickOutcome() {
        let worker = makeWorker(host: "worker-a", slots: 1)
        let slots: [ThunderboltCASlot] = [
            .local(index: 0),
            .remote(worker: worker, index: 0),
        ]
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "sig-remote",
                msPerFrameC1: 0.70,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
                ]
            ),
        ])

        let zeroTailModel = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: slots,
            reachableWorkers: [worker],
            workerCaps: [makeCaps(signature: "sig-remote", msPerFrameC1: 0.70, degradationCurve: [1: 1.0])],
            priorTable: prior,
            remoteTxInEstimateMSByHost: [:],
            localMSPerFrameC1: 1.0,
            localMSSource: "prior(local)",
            localCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
            localCurveSource: "prior(local)"
        )
        let heavyTailModel = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: slots,
            reachableWorkers: [worker],
            workerCaps: [makeCaps(signature: "sig-remote", msPerFrameC1: 0.70, degradationCurve: [1: 1.0])],
            priorTable: prior,
            remoteTxInEstimateMSByHost: [:],
            remoteTxOutEstimateMSByHost: ["worker-a": 120],
            remotePublishOverheadEstimateMSByHost: ["worker-a": 80],
            localMSPerFrameC1: 1.0,
            localMSSource: "prior(local)",
            localCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
            localCurveSource: "prior(local)"
        )

        let job = CAPendingPickJob(
            token: 0,
            job: CAJob(id: "job-1", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)
        )
        let zeroTailPick = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [job],
            machines: makeSingleSlotMachines(zeroTailModel.machineProfiles),
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        let heavyTailPick = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [job],
            machines: makeSingleSlotMachines(heavyTailModel.machineProfiles),
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )

        #expect(zeroTailPick.picks.first?.slot.machineIndex == 1)
        #expect(heavyTailPick.picks.first?.slot.machineIndex == 0)
    }

    @Test("benchmark runtime consumes affine prior fixed overhead")
    func benchmarkRuntimeConsumesAffinePriorFixedOverhead() async throws {
        let worker = makeWorker(host: "worker-a", slots: 1)
        let slots: [ThunderboltCASlot] = [
            .local(index: 0),
            .remote(worker: worker, index: 0),
        ]
        let prior = BenchmarkPriorTable(machines: [
            makePriorMachine(
                signature: "sig-remote",
                msPerFrameC1: 0.50,
                fixedOverheadMS: 200,
                cells: [
                    BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 250, msPerVideoP95: 350, degradationRatio: 1.0),
                ]
            ),
        ])

        let result = buildThunderboltCAModelProfiles(
            mode: .strict,
            port: 7000,
            slots: slots,
            reachableWorkers: [worker],
            workerCaps: [makeCaps(signature: "sig-remote", msPerFrameC1: 0.50, degradationCurve: [1: 1.0])],
            priorTable: prior,
            remoteTxInEstimateMSByHost: [:],
            localMSPerFrameC1: 1.0,
            localMSSource: "prior(local)",
            localCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
            localCurveSource: "prior(local)"
        )

        let remoteMachine = result.machineProfiles.first { $0.id == "worker-a:7000" }
        let remoteInput = result.modelInputs.first { $0.machineID == "worker-a:7000" }
        #expect(remoteMachine?.fixedOverheadMS == 200)
        #expect(remoteInput?.fixedOverheadMS == 200)

        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .complexityAware,
            videoCosts: makeResolvedVideoCosts(frameCounts: [100]),
            machineProfiles: result.machineProfiles,
            slotBindings: result.slotBindings
        )
        let localSlotOrdinal = try #require(result.slotBindings.firstIndex { $0.machineIndex == 0 })
        let remoteSlotOrdinal = try #require(result.slotBindings.firstIndex { $0.machineIndex == 1 })

        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await runtimeEngine.finishArrivals()

        let batch = await runtimeEngine.scheduleBatch(
            freeSlotOrdinals: Set([localSlotOrdinal, remoteSlotOrdinal]),
            nowSeconds: 0
        )
        let localDispatch = batch.first(where: { $0.slotOrdinal == localSlotOrdinal })?.item
        let remoteDispatch = batch.first(where: { $0.slotOrdinal == remoteSlotOrdinal })?.item

        #expect(localDispatch?.index == 0)
        #expect(localDispatch?.dispatchMachineIndex == 0)
        #expect(remoteDispatch == nil)
    }

    @Test("benchmark local setup carries fixed overhead while keeping default frame fallback when probe is missing")
    func benchmarkLocalSetupKeepsDefaultFrameFallbackWhenProbeIsMissing() async throws {
        let localContext = try makeLocalBenchmarkContext(preset: defaultVideoPreset)
        let videoURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-local-affine-\(UUID().uuidString).mov")
        defer { try? FileManager.default.removeItem(at: videoURL) }
        try Data(repeating: 0x61, count: 2_048).write(to: videoURL, options: .atomic)

        let video = MediaFile(
            path: videoURL.path,
            name: videoURL.lastPathComponent,
            type: .video,
            sizeBytes: 25_000_000
        )
        let fixedOverheadMS = 200.0
        let prior = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: localContext.signature,
                chipName: localContext.hardware.chipName,
                performanceCores: localContext.hardware.performanceCores,
                efficiencyCores: localContext.hardware.efficiencyCores,
                videoEncodeEngines: localContext.hardware.videoEncodeEngines,
                osVersion: WorkerSignatureBuilder.normalizedOS(ProcessInfo.processInfo.operatingSystemVersion),
                transcodePreset: defaultVideoPreset,
                msPerFrameC1: 1.0,
                fixedOverheadMS: fixedOverheadMS,
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
        let settings = ThunderboltSettingsResolution(
            workersRaw: nil,
            workersSource: "test",
            port: 7000,
            portSource: "test",
            connectTimeout: 500,
            connectTimeoutSource: "test",
            warnings: []
        )

        let setup = try await prepareThunderboltCARunSetup(
            corpus: [video],
            preset: defaultVideoPreset,
            hardware: localContext.hardware,
            slotOverrides: ThunderboltCASlotOverrides(localSlots: 1, remoteSlotsByHost: [:]),
            mode: .strict,
            priorTableOverride: prior,
            settingsOverride: settings
        )

        #expect(setup.localFixedOverheadMS == fixedOverheadMS)
        #expect(setup.machineProfiles.first?.id == "local")
        #expect(setup.machineProfiles.first?.fixedOverheadMS == fixedOverheadMS)
        #expect(setup.modelInputs.first?.machineID == "local")
        #expect(setup.modelInputs.first?.fixedOverheadMS == fixedOverheadMS)

        let videoCost = try #require(setup.videoCosts.first)
        #expect(videoCost.frameCount == CAProfileAndFallbackMath.fallbackFrameCount(durationSeconds: nil, frameCount: nil))
        #expect(videoCost.derivation.frameCountSource == .defaultFallback)
        #expect(videoCost.derivation.runtimeSource == .modeledFromFrameCount)
    }

    @Test("benchmark remote setup preserves persisted affine prior into real selection path")
    func benchmarkRemoteSetupPreservesPersistedAffinePriorIntoRealSelectionPath() async throws {
        let localContext = try makeLocalBenchmarkContext(preset: defaultVideoPreset)
        let videoURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-remote-affine-\(UUID().uuidString).mov")
        defer { try? FileManager.default.removeItem(at: videoURL) }
        try Data(repeating: 0x62, count: 2_048).write(to: videoURL, options: .atomic)

        let video = MediaFile(
            path: videoURL.path,
            name: videoURL.lastPathComponent,
            type: .video,
            sizeBytes: 25_000_000
        )
        let worker = makeWorker(host: "127.0.0.1", slots: 1)
        let remoteSignature = "sig-remote-real-setup"
        let remoteCells = [
            BenchmarkPriorCell(
                concurrency: 1,
                videosPerMin: 10,
                msPerVideoP50: 250,
                msPerVideoP95: 350,
                degradationRatio: 1.0
            ),
        ]
        let prior = BenchmarkPriorTable(machines: [
            BenchmarkPriorMachine(
                signature: remoteSignature,
                chipName: "remote-test",
                performanceCores: 4,
                efficiencyCores: 0,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: defaultVideoPreset,
                msPerFrameC1: 0.5,
                fixedOverheadMS: 200,
                avgCorpusFrameCount: 100,
                cells: remoteCells
            ),
        ])
        let workerCaps = makeCaps(
            signature: remoteSignature,
            msPerFrameC1: 0.5,
            degradationCurve: nil,
            priorCells: remoteCells,
            chipName: "remote-test",
            performanceCores: 4,
            efficiencyCores: 0,
            videoEncodeEngines: 1,
            osVersion: "26.0"
        )
        let settings = ThunderboltSettingsResolution(
            workersRaw: nil,
            workersSource: "test",
            port: 7000,
            portSource: "test",
            connectTimeout: 500,
            connectTimeoutSource: "test",
            warnings: []
        )
        let setup = try await prepareThunderboltCARunSetup(
            corpus: [video],
            preset: defaultVideoPreset,
            hardware: localContext.hardware,
            slotOverrides: ThunderboltCASlotOverrides(localSlots: 1, remoteSlotsByHost: [:]),
            mode: .strict,
            priorTableOverride: prior,
            settingsOverride: settings,
            reachableWorkersOverride: [worker],
            workerCapsOverride: [workerCaps],
            remoteTxInEstimateMSByHost: [worker.host: 1],
            remoteTxOutEstimateMSByHost: [worker.host: 1],
            remotePublishOverheadEstimateMSByHost: [worker.host: 1]
        )

        let remoteMachine = setup.machineProfiles.first { $0.id == "\(worker.host):\(settings.port)" }
        let remoteInput = setup.modelInputs.first { $0.machineID == "\(worker.host):\(settings.port)" }
        #expect(remoteMachine?.fixedOverheadMS == 200)
        #expect(remoteInput?.fixedOverheadMS == 200)

        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .complexityAware,
            videoCosts: makeResolvedVideoCosts(frameCounts: [100]),
            machineProfiles: setup.machineProfiles,
            slotBindings: setup.slotBindings
        )
        let localSlotOrdinal = try #require(setup.slotBindings.firstIndex { $0.machineIndex == 0 })
        let remoteSlotOrdinal = try #require(setup.slotBindings.firstIndex { $0.machineIndex != 0 })

        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await runtimeEngine.finishArrivals()

        let batch = await runtimeEngine.scheduleBatch(
            freeSlotOrdinals: Set([localSlotOrdinal, remoteSlotOrdinal]),
            nowSeconds: 0
        )
        let localDispatch = batch.first(where: { $0.slotOrdinal == localSlotOrdinal })?.item
        let remoteDispatch = batch.first(where: { $0.slotOrdinal == remoteSlotOrdinal })?.item

        #expect(localDispatch?.index == 0)
        #expect(localDispatch?.dispatchMachineIndex == 0)
        #expect(remoteDispatch == nil)
    }

    @Test("benchmark runtime keeps affine fixed overhead out of adapted slope")
    func benchmarkRuntimeKeepsAffineFixedOverheadOutOfAdaptedSlope() async {
        let tolerance = 0.000_001
        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .complexityAware,
            videoCosts: makeResolvedVideoCosts(frameCounts: [100]),
            machineProfiles: [
                ThunderboltCAMachineProfile(
                    id: "local",
                    msPerFrameC1: 1.0,
                    fixedOverheadMS: 200,
                    degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                    txInMS: 0,
                    txOutMS: 0,
                    publishOverheadMS: 0
                ),
                ThunderboltCAMachineProfile(
                    id: "remote",
                    msPerFrameC1: 4.0,
                    degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                    txInMS: 0,
                    txOutMS: 0,
                    publishOverheadMS: 0
                ),
            ],
            slotBindings: [
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-slot"),
                ThunderboltCASlotBinding(machineIndex: 1, slotID: "remote-slot"),
            ]
        )

        await runtimeEngine.recordCompletion(
            machineIndex: 0,
            frameCount: 100,
            processNanos: 300_000_000,
            concurrencyHint: 1
        )
        let localAdaptation = await runtimeEngine.adaptationRows().first(where: { $0.machineID == "local" })
        #expect(abs((localAdaptation?.finalMSPerFrameC1 ?? 0) - 1.0) < tolerance)

        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await runtimeEngine.finishArrivals()

        let batch = await runtimeEngine.scheduleBatch(freeSlotOrdinals: [0, 1], nowSeconds: 0.025)
        let localDispatch = batch.first(where: { $0.slotOrdinal == 0 })?.item
        let remoteDispatch = batch.first(where: { $0.slotOrdinal == 1 })?.item

        #expect(localDispatch?.index == 0)
        #expect(localDispatch?.dispatchMachineIndex == 0)
        #expect(remoteDispatch == nil)
    }

    @Test("benchmark runtime adaptive slope uses shared locked estimator sequence")
    func benchmarkRuntimeAdaptiveSlopeUsesSharedLockedEstimatorSequence() async throws {
        let startingEstimate = 9.135
        let observations: [Double] = [4.9, 7.2, 5.4, 4.9]
        let fixedOverheadMS = 120.0
        let concurrency = 2
        let concurrencyRatio = 1.5
        let runtimeEngine = makeAdaptiveBenchmarkRuntimeEngine(
            initialMSPerFrameC1: startingEstimate,
            fixedOverheadMS: fixedOverheadMS,
            concurrencyTwoRatio: concurrencyRatio
        )

        var previousLiveEstimate: Double?
        var previousLiveSmoothedError: Double?
        var previousLiveSmoothedAbsoluteError: Double?

        for (index, observation) in observations.enumerated() {
            await recordBenchmarkSlopeObservation(
                runtimeEngine: runtimeEngine,
                observedC1MSPerFrame: observation,
                fixedOverheadMS: fixedOverheadMS,
                concurrency: concurrency,
                concurrencyRatio: concurrencyRatio
            )

            let benchmarkEstimate = try #require(await runtimeEngine.adaptationRows().first?.finalMSPerFrameC1)
            let liveUpdate = try #require(
                LiveAdaptiveMSPerFrameC1Estimator.next(
                    previousEstimate: previousLiveEstimate,
                    previousSmoothedError: previousLiveSmoothedError,
                    previousSmoothedAbsoluteError: previousLiveSmoothedAbsoluteError,
                    initialEstimate: startingEstimate,
                    observed: observation
                )
            )

            #expect(abs(benchmarkEstimate - liveUpdate.estimate) < 0.000_001)

            if index == 0 {
                #expect(abs(benchmarkEstimate - observation) < 0.000_001)
                #expect(abs(liveUpdate.smoothedError - (observation - startingEstimate)) < 0.000_001)
                #expect(abs(liveUpdate.smoothedAbsoluteError - abs(observation - startingEstimate)) < 0.000_001)
            }

            previousLiveEstimate = liveUpdate.estimate
            previousLiveSmoothedError = liveUpdate.smoothedError
            previousLiveSmoothedAbsoluteError = liveUpdate.smoothedAbsoluteError
        }
    }

    @Test("benchmark runtime adaptive slope converges toward repeated faster completions without overshoot")
    func benchmarkRuntimeAdaptiveSlopeConvergesTowardRepeatedFasterCompletionsWithoutOvershoot() async throws {
        let initialEstimate = 9.135
        let observedEstimate = 4.9
        let tolerance = 0.000_001
        let runtimeEngine = makeAdaptiveBenchmarkRuntimeEngine(
            initialMSPerFrameC1: initialEstimate,
            fixedOverheadMS: 120,
            concurrencyTwoRatio: 1.5
        )
        var estimates: [Double] = []
        estimates.reserveCapacity(10)

        for _ in 0..<10 {
            await recordBenchmarkSlopeObservation(
                runtimeEngine: runtimeEngine,
                observedC1MSPerFrame: observedEstimate,
                fixedOverheadMS: 120,
                concurrency: 2,
                concurrencyRatio: 1.5
            )
            let estimate = try #require(await runtimeEngine.adaptationRows().first?.finalMSPerFrameC1)
            estimates.append(estimate)
        }

        #expect(estimates.count == 10)

        var previousEstimate = initialEstimate
        for estimate in estimates {
            #expect(estimate.isFinite)
            #expect(estimate <= previousEstimate + tolerance)
            #expect(estimate >= observedEstimate - tolerance)
            #expect(estimate <= initialEstimate + tolerance)
            previousEstimate = estimate
        }

        let firstEstimate = try #require(estimates.first)
        let penultimateEstimate = try #require(estimates.dropLast().last)
        let lastEstimate = try #require(estimates.last)
        let firstAdjustment = abs(initialEstimate - firstEstimate)
        let lastAdjustment = abs(lastEstimate - penultimateEstimate)

        #expect(abs(lastEstimate - observedEstimate) <= abs(firstEstimate - observedEstimate) + tolerance)
        #expect(lastAdjustment <= firstAdjustment + tolerance)
    }

    @Test("benchmark runtime adaptive slope moves with the signal and reacts more to larger bias")
    func benchmarkRuntimeAdaptiveSlopeMovesWithTheSignalAndReactsMoreToLargerBias() async throws {
        let initialEstimate = 9.135
        let fasterObservedEstimate = 4.9
        let tolerance = 0.000_001

        let fasterRuntimeEngine = makeAdaptiveBenchmarkRuntimeEngine(
            initialMSPerFrameC1: initialEstimate,
            fixedOverheadMS: 120,
            concurrencyTwoRatio: 1.5
        )
        await recordBenchmarkSlopeObservation(
            runtimeEngine: fasterRuntimeEngine,
            observedC1MSPerFrame: fasterObservedEstimate,
            fixedOverheadMS: 120,
            concurrency: 2,
            concurrencyRatio: 1.5
        )
        let fasterEstimate = try #require(await fasterRuntimeEngine.adaptationRows().first?.finalMSPerFrameC1)
        #expect(fasterEstimate < initialEstimate)
        #expect(fasterEstimate >= fasterObservedEstimate - tolerance)

        let smallerBiasRuntimeEngine = makeAdaptiveBenchmarkRuntimeEngine(
            initialMSPerFrameC1: initialEstimate,
            fixedOverheadMS: 120,
            concurrencyTwoRatio: 1.5
        )
        await recordBenchmarkSlopeObservation(
            runtimeEngine: smallerBiasRuntimeEngine,
            observedC1MSPerFrame: fasterObservedEstimate,
            fixedOverheadMS: 120,
            concurrency: 2,
            concurrencyRatio: 1.5
        )
        let smallerBiasBaseline = try #require(await smallerBiasRuntimeEngine.adaptationRows().first?.finalMSPerFrameC1)
        let smallerSlowObservedEstimate = smallerBiasBaseline + 0.3
        await recordBenchmarkSlopeObservation(
            runtimeEngine: smallerBiasRuntimeEngine,
            observedC1MSPerFrame: smallerSlowObservedEstimate,
            fixedOverheadMS: 120,
            concurrency: 2,
            concurrencyRatio: 1.5
        )
        let smallerBiasEstimate = try #require(await smallerBiasRuntimeEngine.adaptationRows().first?.finalMSPerFrameC1)
        #expect(smallerBiasEstimate > smallerBiasBaseline)
        #expect(smallerBiasEstimate <= smallerSlowObservedEstimate + tolerance)

        let largerBiasRuntimeEngine = makeAdaptiveBenchmarkRuntimeEngine(
            initialMSPerFrameC1: initialEstimate,
            fixedOverheadMS: 120,
            concurrencyTwoRatio: 1.5
        )
        await recordBenchmarkSlopeObservation(
            runtimeEngine: largerBiasRuntimeEngine,
            observedC1MSPerFrame: fasterObservedEstimate,
            fixedOverheadMS: 120,
            concurrency: 2,
            concurrencyRatio: 1.5
        )
        let largerBiasBaseline = try #require(await largerBiasRuntimeEngine.adaptationRows().first?.finalMSPerFrameC1)
        let largerSlowObservedEstimate = largerBiasBaseline + 3.1
        await recordBenchmarkSlopeObservation(
            runtimeEngine: largerBiasRuntimeEngine,
            observedC1MSPerFrame: largerSlowObservedEstimate,
            fixedOverheadMS: 120,
            concurrency: 2,
            concurrencyRatio: 1.5
        )
        let largerBiasEstimate = try #require(await largerBiasRuntimeEngine.adaptationRows().first?.finalMSPerFrameC1)
        #expect(largerBiasEstimate > largerBiasBaseline)
        #expect(largerBiasEstimate <= largerSlowObservedEstimate + tolerance)

        let smallerBiasAdjustment = smallerBiasEstimate - smallerBiasBaseline
        let largerBiasAdjustment = largerBiasEstimate - largerBiasBaseline

        #expect(largerBiasEstimate > smallerBiasEstimate)
        #expect(largerBiasAdjustment > smallerBiasAdjustment)
    }

    @Test("benchmark runtime adaptive slope flips dispatch once repeated completions make local competitive")
    func benchmarkRuntimeAdaptiveSlopeFlipsDispatchOnceRepeatedCompletionsMakeLocalCompetitive() async throws {
        let initialEstimate = 9.135
        let observedEstimate = 4.9
        let remoteEstimate = 8.0
        let tolerance = 0.000_001

        let baselineRuntimeEngine = makeAdaptiveRoutingBenchmarkRuntimeEngine(
            initialLocalMSPerFrameC1: initialEstimate,
            remoteMSPerFrameC1: remoteEstimate,
            fixedOverheadMS: 120,
            concurrencyTwoRatio: 1.5
        )
        await baselineRuntimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await baselineRuntimeEngine.finishArrivals()

        let baselineBatch = await baselineRuntimeEngine.scheduleBatch(freeSlotOrdinals: [0, 1], nowSeconds: 0)
        #expect(baselineBatch.count == 1)
        #expect(baselineBatch.first?.item.dispatchMachineIndex == 1)

        let adaptedRuntimeEngine = makeAdaptiveRoutingBenchmarkRuntimeEngine(
            initialLocalMSPerFrameC1: initialEstimate,
            remoteMSPerFrameC1: remoteEstimate,
            fixedOverheadMS: 120,
            concurrencyTwoRatio: 1.5
        )
        var previousEstimate = initialEstimate
        let maxAdaptiveObservations = 25

        for _ in 0..<maxAdaptiveObservations {
            await recordBenchmarkSlopeObservation(
                runtimeEngine: adaptedRuntimeEngine,
                observedC1MSPerFrame: observedEstimate,
                fixedOverheadMS: 120,
                concurrency: 2,
                concurrencyRatio: 1.5
            )
            let estimate = try #require(await adaptedRuntimeEngine.adaptationRows().first?.finalMSPerFrameC1)
            #expect(estimate.isFinite)
            #expect(estimate <= previousEstimate + tolerance)
            #expect(estimate >= observedEstimate - tolerance)
            previousEstimate = estimate
        }

        await adaptedRuntimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await adaptedRuntimeEngine.finishArrivals()

        let adaptedBatch = await adaptedRuntimeEngine.scheduleBatch(freeSlotOrdinals: [0, 1], nowSeconds: 0)
        #expect(adaptedBatch.count == 1)
        #expect(adaptedBatch.first?.item.dispatchMachineIndex == 0)
    }

    @Test("benchmark runtime matches production two-stage ready-now and reservation split")
    func benchmarkRuntime_matchesProductionTwoStageReadyNowAndReservationSplit() async {
        let machineProfiles = [
            ThunderboltCAMachineProfile(
                id: "slow",
                msPerFrameC1: 2.5,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
            ThunderboltCAMachineProfile(
                id: "fast",
                msPerFrameC1: 1.0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
        ]
        let productionAssembly = CAProjectedSlotSelection.Assembly(
            machineContexts: [
                .init(
                    machine: CAMachine(
                        id: "slow",
                        slots: [CASlot(id: "slow-slot", readyAtMS: 0)],
                        msPerFrameC1: 2.5,
                        degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                        txInMS: 0
                    ),
                    route: .local
                ),
                .init(
                    machine: CAMachine(
                        id: "fast",
                        slots: [CASlot(id: "fast-slot", readyAtMS: 20)],
                        msPerFrameC1: 1.0,
                        degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                        txInMS: 0
                    ),
                    route: .remote(workerIndex: 0)
                ),
            ]
        )
        let productionPlan = productionAssembly.plan(
            pendingJobs: [
                .init(token: 0, job: CAJob(id: "job-0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 20)),
                .init(token: 1, job: CAJob(id: "job-1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 20)),
            ],
            nowMS: 0,
            maxCount: 1
        )

        #expect(productionPlan.dispatches.count == 1)
        #expect(productionPlan.holds.count == 1)
        #expect(productionPlan.dispatches[0].token == 0)
        #expect(productionPlan.dispatches[0].slotID == "slow-slot")
        #expect(productionPlan.holds[0].token == 1)
        #expect(productionPlan.holds[0].targetSlotID == "fast-slot")
        #expect(productionPlan.holds[0].wakeAtMS == 20)

        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .complexityAware,
            videoCosts: makeResolvedVideoCosts(frameCounts: [20, 20]),
            machineProfiles: machineProfiles,
            slotBindings: [
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "slow-slot"),
                ThunderboltCASlotBinding(machineIndex: 1, slotID: "fast-slot"),
            ]
        )

        await runtimeEngine.markSlotReady(slotOrdinal: 1, nowSeconds: 0.02)
        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await runtimeEngine.enqueue(index: 1, arrivalAtSeconds: 0)
        await runtimeEngine.finishArrivals()

        let batch = await runtimeEngine.scheduleRuntimeBatch(
            freeSlotOrdinals: [0],
            nowSeconds: 0,
            totalJobCount: 2
        )

        #expect(batch.dispatches.count == 1)
        #expect(batch.dispatches[0].item.index == productionPlan.dispatches[0].token)
        #expect(batch.dispatches[0].slotOrdinal == 0)
        #expect(await runtimeEngine.pendingIndicesSnapshot().isEmpty)

        let held = try! #require(await runtimeEngine.heldDispatchSnapshot().first)
        #expect(held.index == productionPlan.holds[0].token)
        #expect(held.slotOrdinal == 1)
        #expect(held.targetReadyAtSeconds == productionPlan.holds[0].wakeAtMS / 1_000.0)
        #expect(held.wakeAtSeconds == held.targetReadyAtSeconds)
    }

    @Test("benchmark runtime matches production dispatch-now decision")
    func benchmarkRuntime_matchesProductionDispatchNowDecision() async {
        let machineProfiles = [
            ThunderboltCAMachineProfile(
                id: "slow",
                msPerFrameC1: 2.5,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
            ThunderboltCAMachineProfile(
                id: "fast",
                msPerFrameC1: 1.0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
        ]
        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .complexityAware,
            videoCosts: makeResolvedVideoCosts(frameCounts: [10]),
            machineProfiles: machineProfiles,
            slotBindings: [
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "slow-slot"),
                ThunderboltCASlotBinding(machineIndex: 1, slotID: "fast-slot"),
            ]
        )

        await runtimeEngine.markSlotReady(slotOrdinal: 1, nowSeconds: 0.03)
        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await runtimeEngine.finishArrivals()

        let productionPick = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                CAPendingPickJob(
                    token: 0,
                    job: CAJob(id: "job-0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
                ),
            ],
            machines: makeSingleSlotMachines(machineProfiles, readyAtMSByMachine: [0, 30]),
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )
        #expect(productionPick.picks.count == 1)
        #expect(productionPick.picks[0].slot.machineIndex == 0)
        #expect(productionPick.picks[0].score.tReadySlotMS <= 0)

        let dispatchBatch = await runtimeEngine.scheduleBatch(freeSlotOrdinals: [0], nowSeconds: 0)
        let slowDispatch = dispatchBatch.first(where: { $0.slotOrdinal == 0 })?.item
        let fastDispatch = dispatchBatch.first(where: { $0.slotOrdinal == 1 })?.item

        #expect(slowDispatch?.index == 0)
        #expect(slowDispatch?.dispatchMachineIndex == productionPick.picks[0].slot.machineIndex)
        #expect(fastDispatch == nil)
    }

    @Test("ca coordinator reconsiders held jobs after earlier slot completion")
    func caCoordinator_reconsidersHeldJobsAfterEarlierSlotCompletion() async throws {
        let firstURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-held-first-\(UUID().uuidString).mov")
        let secondURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-held-local-\(UUID().uuidString).mov")
        let thirdURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-held-second-\(UUID().uuidString).mov")
        try Data(repeating: 0x41, count: 2_048).write(to: firstURL, options: .atomic)
        try Data(repeating: 0x42, count: 2_048).write(to: secondURL, options: .atomic)
        try Data(repeating: 0x43, count: 2_048).write(to: thirdURL, options: .atomic)
        defer {
            try? FileManager.default.removeItem(at: firstURL)
            try? FileManager.default.removeItem(at: secondURL)
            try? FileManager.default.removeItem(at: thirdURL)
        }

        let first = MediaFile(
            path: firstURL.path,
            name: firstURL.lastPathComponent,
            type: .video,
            sizeBytes: 25_000_000
        )
        let second = MediaFile(
            path: secondURL.path,
            name: secondURL.lastPathComponent,
            type: .video,
            sizeBytes: 5_000_000
        )
        let held = MediaFile(
            path: thirdURL.path,
            name: thirdURL.lastPathComponent,
            type: .video,
            sizeBytes: 1_000_000
        )
        let remoteWorker = makeWorker(host: "worker-held", slots: 1)
        let setup = makeHeldRecomputePreparedCARunSetup(
            videos: [first, second, held],
            remoteWorker: remoteWorker
        )

        let observed = try await runThunderboltCA(
            corpus: [first, second, held],
            preset: defaultVideoPreset,
            timeout: 1,
            hardware: makeHardwareProfile(),
            policy: .complexityAware,
            profile: .allAtOnce,
            preparedSetup: setup,
            localVideoRunner: { _, _, _, _, _, _ in
                try? await Task.sleep(for: .milliseconds(400))
                return true
            },
            roundTripRunner: { _, video, _, _, _, _ in
                if video.name == first.name {
                    Thread.sleep(forTimeInterval: 1.2)
                    return ThunderboltRoundTripResult(
                        success: true,
                        sendSeconds: 0.001,
                        processNanos: 1_200_000_000,
                        receiveSeconds: 0.001,
                        totalSeconds: 1.202
                    )
                }
                Thread.sleep(forTimeInterval: 0.02)
                return ThunderboltRoundTripResult(
                    success: true,
                    sendSeconds: 0.001,
                    processNanos: 20_000_000,
                    receiveSeconds: 0.001,
                    totalSeconds: 0.022
                )
            }
        )

        let firstJob = try #require(observed.result.jobs.first(where: { $0.videoName == first.name }))
        let heldJob = try #require(observed.result.jobs.first(where: { $0.videoName == held.name }))
        let firstCompletedAt = try #require(firstJob.completedAtSeconds)
        #expect(!heldJob.actualExecutor.isEmpty)

        let predictionsByVideoName = Dictionary(
            uniqueKeysWithValues: zip(observed.result.jobs, observed.observability.predictions).map { pair in
                (pair.0.videoName, pair.1)
            }
        )
        let heldPrediction = try #require(predictionsByVideoName[held.name])
        let heldPredictedSlotReadyMS = try #require(heldPrediction.predictedSlotReadyMS)
        let heldActualStartMS = try #require(heldPrediction.actualStartMS)
        let heldActualDoneMS = try #require(heldPrediction.actualDoneMS)
        #expect(heldPrediction.waited == true)
        #expect(heldPrediction.decisionAtSeconds >= heldJob.arrivalAtSeconds)
        #expect(heldPrediction.decisionAtSeconds < firstCompletedAt)
        #expect(heldPredictedSlotReadyMS > 0)
        #expect((heldPrediction.predictedStartMS ?? 0) >= heldPredictedSlotReadyMS)
        #expect((heldPrediction.predictedDoneMS ?? 0) >= (heldPrediction.predictedStartMS ?? 0))
        #expect(heldActualStartMS >= heldPredictedSlotReadyMS)
        #expect(heldActualDoneMS > heldActualStartMS)

        let artifactURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-held-observability-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: artifactURL) }

        try writeThunderboltCAJSON(observed.result, toPath: artifactURL.path)

        let data = try Data(contentsOf: artifactURL)
        let json = try #require(JSONSerialization.jsonObject(with: data) as? [String: Any])
        let jobsJSON = try #require(json["jobs"] as? [[String: Any]])
        let observabilityJSON = try #require(json["observability"] as? [String: Any])
        let predictionsJSON = try #require(observabilityJSON["predictions"] as? [[String: Any]])
        #expect(jobsJSON.count == predictionsJSON.count)

        let jobsByVideoName: [String: [String: Any]] = Dictionary(
            uniqueKeysWithValues: jobsJSON.compactMap { jobJSON in
                guard let videoName = jobJSON["videoName"] as? String else { return nil }
                return (videoName, jobJSON)
            }
        )
        let predictionJSONByVideoName: [String: [String: Any]] = Dictionary(
            uniqueKeysWithValues: zip(jobsJSON, predictionsJSON).compactMap { jobJSON, predictionJSON in
                guard let videoName = jobJSON["videoName"] as? String else { return nil }
                return (videoName, predictionJSON)
            }
        )

        let heldJobJSON = try #require(jobsByVideoName[held.name])
        #expect((heldJobJSON["actualExecutor"] as? String)?.isEmpty == false)

        let heldPredictionJSON = try #require(predictionJSONByVideoName[held.name])
        let decisionAtSeconds = try #require((heldPredictionJSON["decision_at_seconds"] as? NSNumber)?.doubleValue)
        let predictedSlotReadyMS = try #require((heldPredictionJSON["predicted_slot_ready_ms"] as? NSNumber)?.doubleValue)
        let predictedStartMS = try #require((heldPredictionJSON["predicted_start_ms"] as? NSNumber)?.doubleValue)
        let predictedDoneMS = try #require((heldPredictionJSON["predicted_done_ms"] as? NSNumber)?.doubleValue)
        let actualStartMS = try #require((heldPredictionJSON["actual_start_ms"] as? NSNumber)?.doubleValue)
        let actualDoneMS = try #require((heldPredictionJSON["actual_done_ms"] as? NSNumber)?.doubleValue)

        #expect(heldPredictionJSON["waited"] as? Bool == true)
        #expect(decisionAtSeconds >= heldJob.arrivalAtSeconds)
        #expect(decisionAtSeconds < firstCompletedAt)
        #expect(predictedSlotReadyMS > 0)
        #expect(predictedStartMS >= predictedSlotReadyMS)
        #expect(predictedDoneMS >= predictedStartMS)
        #expect(actualStartMS >= predictedSlotReadyMS)
        #expect(actualDoneMS > actualStartMS)
    }

    @Test("solver telemetry is collected through benchmark runtime")
    func solverTelemetry_collectedThroughBenchmarkRuntime() async {
        let machineProfiles = [
            ThunderboltCAMachineProfile(
                id: "local",
                msPerFrameC1: 1.0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
        ]
        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .complexityAware,
            videoCosts: makeResolvedVideoCosts(frameCounts: [100, 50]),
            machineProfiles: machineProfiles,
            slotBindings: [
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-s0"),
            ]
        )

        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await runtimeEngine.enqueue(index: 1, arrivalAtSeconds: 0)
        await runtimeEngine.finishArrivals()

        _ = await runtimeEngine.scheduleBatch(freeSlotOrdinals: [0], nowSeconds: 0)
        _ = await runtimeEngine.scheduleBatch(freeSlotOrdinals: [0], nowSeconds: 0.200)

        let telemetry = await runtimeEngine.solverTelemetrySnapshot()
        #expect(!telemetry.isEmpty)

        for snapshot in telemetry {
            #expect(snapshot.nodesVisited >= 0)
            #expect(snapshot.incumbentUpdates >= 0)
            #expect(snapshot.maxDepth >= 0)
            #expect(snapshot.solverWallMS >= 0)
            #expect(snapshot.prunedByPickCount >= 0)
            #expect(snapshot.prunedByMakespan >= 0)
            #expect(snapshot.prunedByCompletionSum >= 0)
        }
    }
}

private func makePriorMachine(
    signature: String,
    msPerFrameC1: Double,
    fixedOverheadMS: Double = 0,
    avgCorpusFrameCount: Double = 4_000,
    affineModelSource: BenchmarkPriorAffineModelSource = .explicit,
    cells: [BenchmarkPriorCell]
) -> BenchmarkPriorMachine {
    BenchmarkPriorMachine(
        signature: signature,
        chipName: "Apple M4",
        performanceCores: 4,
        efficiencyCores: 6,
        videoEncodeEngines: 1,
        osVersion: "26.0",
        transcodePreset: defaultVideoPreset,
        msPerFrameC1: msPerFrameC1,
        fixedOverheadMS: fixedOverheadMS,
        avgCorpusFrameCount: avgCorpusFrameCount,
        affineModelSource: affineModelSource,
        cells: cells
    )
}

private func makeCaps(
    signature: String?,
    msPerFrameC1: Double?,
    degradationCurve: [Int: Double]?,
    priorCells: [BenchmarkPriorCell]? = nil,
    chipName: String? = nil,
    performanceCores: Int? = nil,
    efficiencyCores: Int? = nil,
    videoEncodeEngines: Int? = nil,
    osVersion: String? = nil
) -> WorkerCaps {
    var json: [String: Any] = [:]
    if let signature {
        json["worker_signature"] = signature
    }
    if let chipName {
        json["chip_name"] = chipName
    }
    if let performanceCores {
        json["performance_cores"] = performanceCores
    }
    if let efficiencyCores {
        json["efficiency_cores"] = efficiencyCores
    }
    if let videoEncodeEngines {
        json["video_encode_engines"] = videoEncodeEngines
    }
    if let osVersion {
        json["os_version"] = osVersion
    }
    if let msPerFrameC1 {
        json["ms_per_frame_c1"] = msPerFrameC1
    }
    if let degradationCurve {
        json["degradation_curve"] = Dictionary(
            uniqueKeysWithValues: degradationCurve.map { (String($0.key), $0.value) }
        )
    }
    if let priorCells {
        json["prior_cells"] = priorCells.map { cell in
            [
                "concurrency": cell.concurrency,
                "videos_per_min": cell.videosPerMin,
                "ms_per_video_p50": cell.msPerVideoP50,
                "ms_per_video_p95": cell.msPerVideoP95,
                "degradation_ratio": cell.degradationRatio,
            ]
        }
    }
    json["tick_version"] = 2
    let data = try! JSONSerialization.data(withJSONObject: json, options: [])
    return try! JSONDecoder().decode(WorkerCaps.self, from: data)
}

private func makeWorker(host: String, slots: Int) -> ThunderboltBoundWorkerSpec {
    ThunderboltBoundWorkerSpec(
        host: host,
        connectHost: host,
        slots: slots,
        sourceIP: "10.0.0.10",
        bridgeName: "bridge0"
    )
}

private func makeTailProbeSampleVideo() throws -> MediaFile {
    let tempURL = FileManager.default.temporaryDirectory
        .appendingPathComponent("tb-ca-tail-probe-\(UUID().uuidString).mov")
    try Data(repeating: 0x42, count: 2_048).write(to: tempURL, options: .atomic)
    return MediaFile(
        path: tempURL.path,
        name: tempURL.lastPathComponent,
        type: .video,
        sizeBytes: 2_048
    )
}

private func makeProductionWorkerSnapshot(
    host: String,
    port: Int,
    workerSignature: String?,
    caps: WorkerCaps?,
    liveMSPerFrameC1: Double?,
    transferOverheadEstimateMS: Double?,
    txOutEstimateMS: Double?,
    publishOverheadEstimateMS: Double?,
    slots: [(isBusy: Bool, isDown: Bool, estimatedRemainingMS: Double?)]
) -> ThunderboltDispatcher.CAWorkerSnapshot {
    ThunderboltDispatcher.CAWorkerSnapshot(
        workerIndex: 0,
        host: host,
        port: port,
        workerSignature: workerSignature,
        caps: caps,
        liveMSPerFrameC1: liveMSPerFrameC1,
        transferOverheadEstimateMS: transferOverheadEstimateMS,
        txOutEstimateMS: txOutEstimateMS,
        publishOverheadEstimateMS: publishOverheadEstimateMS,
        slots: slots.enumerated().map { index, slot in
            ThunderboltDispatcher.CASlotSnapshot(
                slotIndex: index,
                id: "\(host)#s\(index + 1)",
                isBusy: slot.isBusy,
                isDown: slot.isDown,
                estimatedRemainingMS: slot.estimatedRemainingMS
            )
        }
    )
}

private func sharedMachineProfileTuples(
    _ profiles: [CATopologyModelMachineProfile]
) -> [String] {
    profiles.map { profile in
        [
            profile.id,
            formattedDouble(profile.msPerFrameC1),
            formattedDouble(profile.fixedOverheadMS),
            profile.degradationCurve.map { "c\($0.concurrency)=\(formattedDouble($0.ratioToC1))" }.joined(separator: ","),
            formattedDouble(profile.txInMS),
            formattedDouble(profile.txOutMS),
            formattedDouble(profile.publishOverheadMS),
            profile.modeledConcurrencyCap.map(String.init) ?? "-",
        ].joined(separator: "|")
    }
}

private func thunderboltMachineProfileTuples(
    _ profiles: [ThunderboltCAMachineProfile]
) -> [String] {
    profiles.map { profile in
        [
            profile.id,
            formattedDouble(profile.msPerFrameC1),
            formattedDouble(profile.fixedOverheadMS),
            profile.degradationCurve.map { "c\($0.concurrency)=\(formattedDouble($0.ratioToC1))" }.joined(separator: ","),
            formattedDouble(profile.txInMS),
            formattedDouble(profile.txOutMS),
            formattedDouble(profile.publishOverheadMS),
            profile.modeledConcurrencyCap.map(String.init) ?? "-",
        ].joined(separator: "|")
    }
}

private func sharedSlotBindingTuples(
    _ bindings: [CATopologyModelSlotBinding]
) -> [String] {
    bindings.map { "\($0.machineIndex)|\($0.slotID)" }
}

private func thunderboltSlotBindingTuples(
    _ bindings: [ThunderboltCASlotBinding]
) -> [String] {
    bindings.map { "\($0.machineIndex)|\($0.slotID)" }
}

private func sharedModelInputTuples(
    _ rows: [CATopologyModelInputRow]
) -> [String] {
    rows.map { row in
        [
            row.machineID,
            "\(row.slotCount)",
            formattedDouble(row.msPerFrameC1),
            formattedDouble(row.fixedOverheadMS),
            row.msSource,
            row.curveSource,
            formattedDouble(row.txInMS),
            formattedDouble(row.txOutMS),
            formattedDouble(row.publishOverheadMS),
            row.confidenceTier?.rawValue ?? "-",
            formattedDouble(row.confidenceMultiplier),
            row.concurrencyCap.map(String.init) ?? "-",
        ].joined(separator: "|")
    }
}

private func thunderboltModelInputTuples(
    _ rows: [ThunderboltCAModelInputRow]
) -> [String] {
    rows.map { row in
        [
            row.machineID,
            "\(row.slotCount)",
            formattedDouble(row.msPerFrameC1),
            formattedDouble(row.fixedOverheadMS),
            row.msSource,
            row.curveSource,
            formattedDouble(row.txInMS),
            formattedDouble(row.txOutMS),
            formattedDouble(row.publishOverheadMS),
            row.confidenceTier ?? "-",
            formattedDouble(row.confidenceMultiplier),
            row.concurrencyCap.map(String.init) ?? "-",
        ].joined(separator: "|")
    }
}

private func sharedCoverageTuples(
    _ rows: [CATopologyModelCoverageRow]
) -> [String] {
    rows.map { row in
        [
            row.host,
            "\(row.reachableSlots)",
            "\(row.executableSlots)",
            "\(row.modeledSlots)",
            row.msSource,
            row.curveSource,
            row.confidenceTier?.rawValue ?? "-",
            formattedDouble(row.confidenceMultiplier),
            row.concurrencyCap.map(String.init) ?? "-",
            row.note,
        ].joined(separator: "|")
    }
}

private func thunderboltCoverageTuples(
    _ rows: [ThunderboltCARemoteCoverageRow]
) -> [String] {
    rows.map { row in
        [
            row.host,
            "\(row.reachableSlots)",
            "\(row.executableSlots)",
            "\(row.modeledSlots)",
            row.msSource,
            row.curveSource,
            row.confidenceTier ?? "-",
            formattedDouble(row.confidenceMultiplier),
            row.concurrencyCap.map(String.init) ?? "-",
            row.note,
        ].joined(separator: "|")
    }
}

private func sharedDiagnosticSummary(
    _ diagnostics: CATopologyModelDiagnostics
) -> String {
    [
        diagnostics.mode.rawValue,
        "\(diagnostics.reachableWorkerCount)",
        "\(diagnostics.reachableSlotCount)",
        "\(diagnostics.modeledWorkerCount)",
        "\(diagnostics.modeledSlotCount)",
        "\(diagnostics.fallbackActive)",
        "\(diagnostics.localPriorGap)",
        "\(diagnostics.remotePriorGap)",
        "\(diagnostics.localExecutableSlotCount)",
        "\(diagnostics.remoteExecutableSlotCount)",
        "\(diagnostics.totalExecutableSlotCount)",
        "\(diagnostics.exactPriorSlotCount)",
        "\(diagnostics.hardwareCompatiblePriorSlotCount)",
        "\(diagnostics.capabilityBackedSlotCount)",
        "\(diagnostics.localFallbackSlotCount)",
    ].joined(separator: "|")
}

private func thunderboltDiagnosticSummary(
    _ diagnostics: ThunderboltCAModelDiagnostics
) -> String {
    [
        diagnostics.mode.rawValue,
        "\(diagnostics.reachableWorkerCount)",
        "\(diagnostics.reachableSlotCount)",
        "\(diagnostics.modeledWorkerCount)",
        "\(diagnostics.modeledSlotCount)",
        "\(diagnostics.fallbackActive)",
        "\(diagnostics.localPriorGap)",
        "\(diagnostics.remotePriorGap)",
        "\(diagnostics.localExecutableSlotCount)",
        "\(diagnostics.remoteExecutableSlotCount)",
        "\(diagnostics.totalExecutableSlotCount)",
        "\(diagnostics.exactPriorSlotCount)",
        "\(diagnostics.hardwareCompatiblePriorSlotCount)",
        "\(diagnostics.capabilityBackedSlotCount)",
        "\(diagnostics.localFallbackSlotCount)",
    ].joined(separator: "|")
}

private func formattedDouble(_ value: Double) -> String {
    String(format: "%.6f", value)
}

private func captureBenchmarkReport(
    _ writeBody: @escaping @Sendable () throws -> Void
) async throws -> String {
    try await BenchOutputCaptureGate.shared.withExclusive {
        let reportDirectory = ".build/tb-ca-model-report-\(UUID().uuidString)"
        let reportURL = try BenchOutput.startReport(reportDirectory: reportDirectory)
        let reportDirectoryURL = reportURL.deletingLastPathComponent()

        defer {
            BenchOutput.finishReport()
            try? FileManager.default.removeItem(at: reportDirectoryURL)
        }

        try writeBody()
        BenchOutput.finishReport()
        return try String(contentsOf: reportURL, encoding: .utf8)
    }
}

private func makeObservedRun(
    predictions: [ThunderboltCAPredictionSample]
) -> ThunderboltCAObservedRun {
    let observability = ThunderboltCAObservability(
        policy: .complexityAware,
        modelInputs: [],
        adaptation: [],
        predictions: predictions
    )
    return ThunderboltCAObservedRun(
        result: ThunderboltCARunResult(
            schedulerPolicy: CASchedulerPolicy.complexityAware.rawValue,
            arrivalProfile: "test-profile",
            totalJobs: predictions.count,
            successfulJobs: predictions.filter(\.success).count,
            failedCount: predictions.filter { !$0.success }.count,
            metrics: ThunderboltCAMetricsJSON(
                sumWSeconds: 0,
                p95Seconds: 0,
                makespanSeconds: 0,
                failedCount: predictions.filter { !$0.success }.count
            ),
            jobs: [],
            observability: observability
        ),
        observability: observability
    )
}

private struct LocalBenchmarkContext {
    let hardware: HardwareProfile
    let signature: String
}

private func makeLocalBenchmarkContext(preset: String) throws -> LocalBenchmarkContext {
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
    return LocalBenchmarkContext(hardware: hardware, signature: signature)
}

private func makeHardwareProfile() -> HardwareProfile {
    HardwareProfile(
        chipName: "Test Chip",
        performanceCores: 4,
        efficiencyCores: 4,
        totalCores: 8,
        memoryGB: 16,
        videoEncodeEngines: 1,
        hwEncoderNames: []
    )
}

private func makePreparedCARunSetup(videos: [MediaFile]) -> ThunderboltCARunSetup {
    let machineProfile = ThunderboltCAMachineProfile(
        id: "local",
        msPerFrameC1: 1.0,
        degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
        txInMS: 0,
        txOutMS: 0,
        publishOverheadMS: 0
    )
    return ThunderboltCARunSetup(
        port: 7000,
        connectTimeout: 100,
        videos: videos,
        videoCosts: makeDefaultResolvedVideoCosts(count: videos.count),
        priorTable: BenchmarkPriorTable(),
        localSignature: "sig-local",
        localSlotCount: 1,
        localMSPerFrameC1: 1.0,
        localFixedOverheadMS: 0,
        sourceHashes: [:],
        slots: [.local(index: 0)],
        machineProfiles: [machineProfile],
        slotBindings: [ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-slot")],
        machineIndexByHost: [:],
        modelInputs: [
            ThunderboltCAModelInputRow(
                machineID: "local",
                slotCount: 1,
                msPerFrameC1: 1.0,
                msSource: "test(local)",
                curveSource: "test(local)",
                txInMS: 0
            ),
        ],
        diagnostics: ThunderboltCAModelDiagnostics(
            mode: .strict,
            coverageRows: [],
            strictExclusions: [],
            reachableWorkerCount: 0,
            reachableSlotCount: 0,
            modeledWorkerCount: 0,
            modeledSlotCount: 0,
            fallbackActive: false,
            localPriorGap: false,
            remotePriorGap: false
        ),
        reachableWorkers: [],
        workerCapsByHost: [:]
    )
}

private func makeHeldRecomputePreparedCARunSetup(
    videos: [MediaFile],
    remoteWorker: ThunderboltBoundWorkerSpec
) -> ThunderboltCARunSetup {
    let localMachine = ThunderboltCAMachineProfile(
        id: "local",
        msPerFrameC1: 50.0,
        degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
        txInMS: 0,
        txOutMS: 0,
        publishOverheadMS: 0
    )
    let remoteMachine = ThunderboltCAMachineProfile(
        id: "\(remoteWorker.host):7000",
        msPerFrameC1: 1.0,
        degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
        txInMS: 0,
        txOutMS: 0,
        publishOverheadMS: 0
    )

    let modelInputs = [
        ThunderboltCAModelInputRow(
            machineID: localMachine.id,
            slotCount: 1,
            msPerFrameC1: localMachine.msPerFrameC1,
            msSource: "test(local)",
            curveSource: "test(local)",
            txInMS: 0
        ),
        ThunderboltCAModelInputRow(
            machineID: remoteMachine.id,
            slotCount: 1,
            msPerFrameC1: remoteMachine.msPerFrameC1,
            msSource: "test(remote)",
            curveSource: "test(remote)",
            txInMS: 0
        ),
    ]

    return ThunderboltCARunSetup(
        port: 7000,
        connectTimeout: 100,
        videos: videos,
        videoCosts: makeDefaultResolvedVideoCosts(count: videos.count),
        priorTable: BenchmarkPriorTable(),
        localSignature: "sig-local-held",
        localSlotCount: 1,
        localMSPerFrameC1: localMachine.msPerFrameC1,
        localFixedOverheadMS: 0,
        sourceHashes: Dictionary(uniqueKeysWithValues: videos.map { ($0.path, "sha-\($0.name)") }),
        slots: [
            .local(index: 0),
            .remote(worker: remoteWorker, index: 0),
        ],
        machineProfiles: [localMachine, remoteMachine],
        slotBindings: [
            ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-slot"),
            ThunderboltCASlotBinding(machineIndex: 1, slotID: "remote-slot"),
        ],
        machineIndexByHost: [remoteWorker.host: 1],
        modelInputs: modelInputs,
        diagnostics: ThunderboltCAModelDiagnostics(
            mode: .strict,
            coverageRows: [],
            strictExclusions: [],
            reachableWorkerCount: 1,
            reachableSlotCount: 1,
            modeledWorkerCount: 1,
            modeledSlotCount: 1,
            fallbackActive: false,
            localPriorGap: false,
            remotePriorGap: false
        ),
        reachableWorkers: [remoteWorker],
        workerCapsByHost: [:]
    )
}

private func makeSingleSlotMachines(
    _ profiles: [ThunderboltCAMachineProfile],
    readyAtMSByMachine: [Double]? = nil
) -> [CAMachine] {
    profiles.enumerated().map { machineIndex, profile in
        CAMachine(
            id: profile.id,
            slots: [
                CASlot(
                    id: "\(profile.id)#s0",
                    readyAtMS: readyAtMSByMachine?[machineIndex] ?? 0
                ),
            ],
            msPerFrameC1: profile.msPerFrameC1,
            fixedOverheadMS: profile.fixedOverheadMS,
            degradationCurve: profile.degradationCurve,
            txInMS: profile.txInMS,
            txOutMS: profile.txOutMS,
            publishOverheadMS: profile.publishOverheadMS
        )
    }
}

private func makeMultiSlotMachines(
    _ profiles: [ThunderboltCAMachineProfile],
    slotsPerMachine: [Int],
    readyAtMS: Double = 0
) -> [CAMachine] {
    profiles.enumerated().map { machineIndex, profile in
        let slotCount = slotsPerMachine.indices.contains(machineIndex) ? slotsPerMachine[machineIndex] : 1
        return CAMachine(
            id: profile.id,
            slots: (0..<slotCount).map { slotIndex in
                CASlot(id: "\(profile.id)#s\(slotIndex)", readyAtMS: readyAtMS)
            },
            msPerFrameC1: profile.msPerFrameC1,
            fixedOverheadMS: profile.fixedOverheadMS,
            degradationCurve: profile.degradationCurve,
            txInMS: profile.txInMS,
            txOutMS: profile.txOutMS,
            publishOverheadMS: profile.publishOverheadMS
        )
    }
}

private func makeAdaptiveBenchmarkRuntimeEngine(
    initialMSPerFrameC1: Double,
    fixedOverheadMS: Double,
    concurrencyTwoRatio: Double
) -> CABenchmarkRuntimeEngine {
    makeThunderboltCABenchmarkRuntimeEngine(
        policy: .complexityAware,
        videoCosts: makeResolvedVideoCosts(frameCounts: [100]),
        machineProfiles: [
            ThunderboltCAMachineProfile(
                id: "local",
                msPerFrameC1: initialMSPerFrameC1,
                fixedOverheadMS: fixedOverheadMS,
                degradationCurve: [
                    CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
                    CADegradationPoint(concurrency: 2, ratioToC1: concurrencyTwoRatio),
                ],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
        ],
        slotBindings: [
            ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-slot"),
        ]
    )
}

private func makeAdaptiveRoutingBenchmarkRuntimeEngine(
    initialLocalMSPerFrameC1: Double,
    remoteMSPerFrameC1: Double,
    fixedOverheadMS: Double,
    concurrencyTwoRatio: Double
) -> CABenchmarkRuntimeEngine {
    makeThunderboltCABenchmarkRuntimeEngine(
        policy: .complexityAware,
        videoCosts: makeResolvedVideoCosts(frameCounts: [100]),
        machineProfiles: [
            ThunderboltCAMachineProfile(
                id: "local",
                msPerFrameC1: initialLocalMSPerFrameC1,
                fixedOverheadMS: fixedOverheadMS,
                degradationCurve: [
                    CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
                    CADegradationPoint(concurrency: 2, ratioToC1: concurrencyTwoRatio),
                ],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
            ThunderboltCAMachineProfile(
                id: "remote",
                msPerFrameC1: remoteMSPerFrameC1,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0
            ),
        ],
        slotBindings: [
            ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-slot"),
            ThunderboltCASlotBinding(machineIndex: 1, slotID: "remote-slot"),
        ]
    )
}

private func recordBenchmarkSlopeObservation(
    runtimeEngine: CABenchmarkRuntimeEngine,
    observedC1MSPerFrame: Double,
    fixedOverheadMS: Double,
    frameCount: Double = 100,
    concurrency: Int,
    concurrencyRatio: Double
) async {
    let runtimeMS = fixedOverheadMS + (observedC1MSPerFrame * concurrencyRatio * frameCount)
    await runtimeEngine.recordCompletion(
        machineIndex: 0,
        frameCount: frameCount,
        processNanos: UInt64((runtimeMS * 1_000_000).rounded()),
        concurrencyHint: concurrency
    )
}
