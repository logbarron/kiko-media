import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt showdown prior promotion")
struct ThunderboltShowdownPriorPromotionTests {
    private let localSignature = "sig-local"
    private let remoteSignature = "sig-remote-a"
    private let secondRemoteSignature = "sig-remote-b"

    @Test("preflight classifier covers all combinations")
    func preflightClassificationCombos() {
        #expect(classifyThunderboltShowdownPreflight(localPriorGap: false, remotePriorGap: false) == .healthy)
        #expect(classifyThunderboltShowdownPreflight(localPriorGap: true, remotePriorGap: false) == .localPriorGap)
        #expect(classifyThunderboltShowdownPreflight(localPriorGap: false, remotePriorGap: true) == .remotePriorGap)
        #expect(classifyThunderboltShowdownPreflight(localPriorGap: true, remotePriorGap: true) == .localAndRemotePriorGap)
    }

    @Test("showdown blocks CA selection and guidance when wall clock regresses")
    func showdownBlocksCASelectionAndGuidanceOnWallRegression() {
        let fifoMetrics = ThunderboltShowdownComparatorMetrics(
            failedCount: 0,
            sumWSeconds: 12.0,
            p95Seconds: 10.0,
            makespanSeconds: 20.0
        )
        let caMetrics = ThunderboltShowdownComparatorMetrics(
            failedCount: 0,
            sumWSeconds: 10.0,
            p95Seconds: 9.0,
            makespanSeconds: 25.0
        )
        let winner = showdownWinnerPolicy(
            fifoMetrics: fifoMetrics,
            caMetrics: caMetrics
        )
        let wallScore = ShowdownScore(fifo: 1, ca: 0, ties: 0)

        #expect(winner == .fifo)
        #expect(showdownWinnerLabel(wallScore) == "FIFO (1/1 profiles)")

        let guidance = showdownGuidanceLines(
            sumWScore: ShowdownScore(fifo: 0, ca: 1, ties: 0),
            p95Score: ShowdownScore(fifo: 0, ca: 1, ties: 0),
            wallScore: wallScore,
            profileWins: ShowdownScore(fifo: 0, ca: 1, ties: 0),
            totalFailedAcrossRuns: 0,
            preflight: .healthy
        )

        #expect(guidance.first == "Keep FIFO in production for now (set VIDEO_SCHEDULER_POLICY=fifo).")
        #expect(!guidance.contains("Enable CA in production (set VIDEO_SCHEDULER_POLICY=auto; keep FIFO fallback available)."))
    }

    @Test("acceptance fails when CA improves sumW but regresses wall clock")
    func acceptanceFailsOnWallClockRegression() {
        let decision = evaluateThunderboltCAAcceptanceGate(
            fifoMetrics: ThunderboltShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 12.0,
                p95Seconds: 10.0,
                makespanSeconds: 20.0
            ),
            caMetrics: ThunderboltShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 10.0,
                p95Seconds: 9.0,
                makespanSeconds: 25.0
            )
        )

        #expect(!decision.pass)
        #expect(decision.sumWImproved)
        #expect(decision.failedCountNonRegression)
        #expect(decision.makespanRegressed)
    }

    @Test("acceptance fails when CA regresses p95 even if wall clock improves")
    func acceptanceFailsOnP95Regression() {
        let decision = evaluateThunderboltCAAcceptanceGate(
            fifoMetrics: ThunderboltShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 12.0,
                p95Seconds: 10.0,
                makespanSeconds: 20.0
            ),
            caMetrics: ThunderboltShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 10.0,
                p95Seconds: 11.0,
                makespanSeconds: 19.0
            )
        )

        #expect(!decision.pass)
        #expect(decision.sumWImproved)
        #expect(decision.failedCountNonRegression)
        #expect(decision.p95Regressed)
        #expect(!decision.makespanRegressed)
    }

    @Test("force can promote when executable remote coverage is preserved without the exact prior")
    func forceCanPromoteWhenExecutableRemoteCoverageIsPreservedWithoutTheExactPrior() {
        let current = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let candidate = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.1),
        ])
        let workers = [makeWorker(host: "worker-a", slots: 2)]
        let caps = ["worker-a": makeCaps(signature: remoteSignature)]

        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: current,
            candidatePriorTable: candidate,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: caps,
            port: 7000,
            force: true
        )

        #expect(decision.shouldPromote)
        #expect(decision.currentRemoteWorkerCoverage == 1)
        #expect(decision.candidateRemoteWorkerCoverage == 1)
        #expect(decision.currentRemoteSlotCoverage == 2)
        #expect(decision.candidateRemoteSlotCoverage == 2)
        #expect(decision.reason == "force promote enabled")
        #expect(decision.forceApplied)
    }

    @Test("promotion requires a real executable coverage gain once capability-backed baseline exists")
    func promotionRequiresARealExecutableCoverageGainOnceCapabilityBackedBaselineExists() {
        let current = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
        ])
        let candidate = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.1),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let workers = [makeWorker(host: "worker-a", slots: 2)]
        let caps = ["worker-a": makeCaps(signature: remoteSignature)]

        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: current,
            candidatePriorTable: candidate,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: caps,
            port: 7000,
            force: false
        )

        #expect(!decision.shouldPromote)
        #expect(decision.currentRemoteWorkerCoverage == 1)
        #expect(decision.candidateRemoteWorkerCoverage == 1)
        #expect(decision.candidateRemoteSlotCoverage == 2)
        #expect(decision.reason == "candidate does not improve strict coverage")
        #expect(!decision.forceApplied)
    }

    @Test("force can promote equal coverage when no-regression holds")
    func forcePromotesEqualCoverage() {
        let current = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let candidate = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let workers = [makeWorker(host: "worker-a", slots: 2)]
        let caps = ["worker-a": makeCaps(signature: remoteSignature)]

        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: current,
            candidatePriorTable: candidate,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: caps,
            port: 7000,
            force: true
        )

        #expect(decision.shouldPromote)
        #expect(decision.forceApplied)
        #expect(decision.missingModeledSignatures.isEmpty)
    }

    @Test("promotion rejects weaker corpus even when force is enabled")
    func promotionRejectsWeakerCorpusEvenWithForce() {
        let current = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let candidate = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let workers = [makeWorker(host: "worker-a", slots: 2)]
        let caps = ["worker-a": makeCaps(signature: remoteSignature)]

        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: current,
            candidatePriorTable: candidate,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: caps,
            port: 7000,
            currentCorpusSummary: BenchmarkPriorCorpusSummary(videoCount: 4, totalBytes: 4_096),
            candidateCorpusSummary: BenchmarkPriorCorpusSummary(videoCount: 1, totalBytes: 1_024),
            force: true
        )

        #expect(!decision.shouldPromote)
        #expect(!decision.candidateCorpusAtLeastAsStrong)
        #expect(decision.reason == "weaker corpus than canonical")
    }

    @Test("missing exact prior does not count as remote worker coverage regression when fallback modeling preserves coverage")
    func missingExactPriorDoesNotCountAsRemoteWorkerCoverageRegressionWhenFallbackModelingPreservesCoverage() {
        let current = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
            makeMachine(signature: secondRemoteSignature, msPerFrameC1: 0.88),
        ])
        let candidate = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let workers = [
            makeWorker(host: "worker-a", slots: 2),
            makeWorker(host: "worker-b", slots: 2),
        ]
        let caps = [
            "worker-a": makeCaps(signature: remoteSignature),
            "worker-b": makeCaps(signature: secondRemoteSignature),
        ]

        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: current,
            candidatePriorTable: candidate,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: caps,
            port: 7000,
            force: true
        )

        #expect(decision.shouldPromote)
        #expect(decision.currentRemoteWorkerCoverage == 2)
        #expect(decision.candidateRemoteWorkerCoverage == 2)
        #expect(decision.currentRemoteSlotCoverage == 4)
        #expect(decision.candidateRemoteSlotCoverage == 4)
        #expect(decision.reason == "force promote enabled")
        #expect(decision.forceApplied)
    }

    @Test("missing exact priors do not regress worker or slot coverage when executable modeling remains available")
    func missingExactPriorsDoNotRegressWorkerOrSlotCoverageWhenExecutableModelingRemainsAvailable() {
        let current = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
            makeMachine(signature: secondRemoteSignature, msPerFrameC1: 0.88),
        ])
        let candidate = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let workers = [
            makeWorker(host: "worker-a", slots: 4),
            makeWorker(host: "worker-b", slots: 1),
        ]
        let caps = [
            "worker-a": makeCaps(signature: remoteSignature),
            "worker-b": makeCaps(signature: secondRemoteSignature),
        ]

        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: current,
            candidatePriorTable: candidate,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: caps,
            port: 7000,
            force: true
        )

        #expect(decision.shouldPromote)
        #expect(decision.currentRemoteWorkerCoverage == 2)
        #expect(decision.candidateRemoteWorkerCoverage == 2)
        #expect(decision.currentRemoteSlotCoverage == 5)
        #expect(decision.candidateRemoteSlotCoverage == 5)
        #expect(decision.reason == "force promote enabled")
        #expect(decision.forceApplied)
    }

    @Test("guarded promotion rejects a wall-regressing showdown candidate")
    func guardedPromotionRejectsWallRegressingCandidate() {
        let showdownWinner = showdownWinnerPolicy(
            fifoMetrics: ThunderboltShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 12.0,
                p95Seconds: 10.0,
                makespanSeconds: 20.0
            ),
            caMetrics: ThunderboltShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 10.0,
                p95Seconds: 9.0,
                makespanSeconds: 25.0
            )
        )
        let current = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let candidate = BenchmarkPriorTable(machines: [
            makeMachine(signature: localSignature, msPerFrameC1: 1.2),
            makeMachine(signature: remoteSignature, msPerFrameC1: 0.9),
        ])
        let workers = [makeWorker(host: "worker-a", slots: 2)]
        let caps = ["worker-a": makeCaps(signature: remoteSignature)]

        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: current,
            candidatePriorTable: candidate,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: caps,
            port: 7000,
            showdownComparatorPass: showdownWinner == .complexityAware,
            requireComparator: true,
            force: false
        )

        #expect(showdownWinner == .fifo)
        #expect(!decision.shouldPromote)
        #expect(decision.reason == "showdown comparator rejected candidate")
    }

    @Test("shared prior policy engine defers then finalizes guarded promotion for remote coverage gains")
    func sharedPriorPolicyEngineDefersThenFinalizesGuardedPromotion() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let paths = resolveThunderboltCAPriorPaths()
            let currentArtifact = makeArtifact(localSignature: localSignature)
            let candidateArtifact = makeArtifact(
                localSignature: localSignature,
                remoteSignatures: [remoteSignature]
            )
            let workers = [makeWorker(host: "worker-a", slots: 2)]
            let caps = ["worker-a": makeCaps(signature: remoteSignature)]

            try currentArtifact.write(toPath: paths.canonicalPath)

            let deferred = try applyThunderboltShowdownPriorUpdatePolicy(
                candidateArtifact: candidateArtifact,
                currentCanonicalArtifact: currentArtifact,
                localSignature: localSignature,
                reachableWorkers: workers,
                workerCapsByHost: caps,
                port: 7000,
                policy: .promoteGuarded,
                paths: paths,
                allowExistingCanonicalSkip: false,
                deferPromotion: true
            )

            assertOutcome(deferred.outcome, matches: .candidateWritten(paths.candidatePath))
            #expect(deferred.deferredPromotion)
            #expect(FileManager.default.fileExists(atPath: paths.candidatePath))

            let finalized = try applyThunderboltShowdownPriorUpdatePolicy(
                candidateArtifact: candidateArtifact,
                currentCanonicalArtifact: currentArtifact,
                localSignature: localSignature,
                reachableWorkers: workers,
                workerCapsByHost: caps,
                port: 7000,
                policy: .promoteGuarded,
                paths: paths,
                allowExistingCanonicalSkip: false,
                showdownComparatorPass: true,
                requireComparator: true,
                candidateAlreadyWritten: true
            )

            assertOutcome(finalized.outcome, matches: .promoted(paths.canonicalPath))
            #expect(!finalized.deferredPromotion)
            #expect(!FileManager.default.fileExists(atPath: paths.candidatePath))

            let canonical = try #require(BenchmarkPriorArtifact.load(fromPath: paths.canonicalPath))
            #expect(canonical.machines.contains { $0.signature == remoteSignature })
        }
    }
}

private func makeMachine(signature: String, msPerFrameC1: Double) -> BenchmarkPriorMachine {
    BenchmarkPriorMachine(
        signature: signature,
        chipName: "Apple M4",
        performanceCores: 4,
        efficiencyCores: 6,
        videoEncodeEngines: 1,
        osVersion: "26.0",
        transcodePreset: defaultVideoPreset,
        msPerFrameC1: msPerFrameC1,
        avgCorpusFrameCount: 4_000,
        cells: [
            BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
            BenchmarkPriorCell(concurrency: 2, videosPerMin: 16, msPerVideoP50: 5_200, msPerVideoP95: 6_300, degradationRatio: 1.30),
        ]
    )
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

private func makeCaps(signature: String) -> WorkerCaps {
    let payload: [String: Any] = [
        "worker_signature": signature,
        "ms_per_frame_c1": 0.9,
        "degradation_curve": [
            "1": 1.0,
            "2": 1.2,
        ],
    ]
    let data = try! JSONSerialization.data(withJSONObject: payload, options: [])
    return try! JSONDecoder().decode(WorkerCaps.self, from: data)
}

private func makeArtifact(
    localSignature: String,
    remoteSignatures: [String] = []
) -> BenchmarkPriorArtifact {
    let localMachine = makeMachine(signature: localSignature, msPerFrameC1: 1.2)
    let remoteMachines = remoteSignatures.enumerated().map { index, signature in
        makeMachine(signature: signature, msPerFrameC1: 0.9 - (Double(index) * 0.05))
    }
    return BenchmarkPriorArtifact(
        generatedAt: Date(timeIntervalSince1970: 0),
        corpusHash: "sha256:test",
        corpusSummary: BenchmarkPriorCorpusSummary(videoCount: 4, totalBytes: 4_096),
        machines: [localMachine] + remoteMachines
    )
}

private func makeTempDirectory() -> String {
    let path = FileManager.default.temporaryDirectory
        .appendingPathComponent("tb-showdown-policy-\(UUID().uuidString)")
        .path
    try! FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true)
    return path
}

private func assertOutcome(
    _ outcome: ThunderboltPriorWriteOutcome,
    matches expected: ThunderboltPriorWriteOutcome
) {
    switch (outcome, expected) {
    case (.skippedPolicyOff, .skippedPolicyOff),
         (.skippedInsufficientSignal, .skippedInsufficientSignal),
         (.skippedExistingCanonical, .skippedExistingCanonical):
        break
    case let (.candidateWritten(actual), .candidateWritten(expected)),
         let (.canonicalWritten(actual), .canonicalWritten(expected)),
         let (.promoted(actual), .promoted(expected)):
        #expect(actual == expected)
    case let (.candidateRejected(actualReason, actualPath), .candidateRejected(expectedReason, expectedPath)):
        #expect(actualReason == expectedReason)
        #expect(actualPath == expectedPath)
    default:
        Issue.record("Unexpected outcome: \(outcome)")
    }
}
