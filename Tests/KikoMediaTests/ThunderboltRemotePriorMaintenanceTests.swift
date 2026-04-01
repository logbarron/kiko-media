import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt remote prior maintenance", Testing.ParallelizationTrait.serialized)
struct ThunderboltRemotePriorMaintenanceTests {
    @Test("remote-prior-gap no longer takes the old skip path when policy is on")
    func remotePriorGapRunsMaintenanceWhenPolicyIsOn() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let setup = makeShowdownSetup(reachableWorkers: [makeWorker(host: "worker-a", slots: 2)])
            let candidateArtifact = makePriorArtifact(
                localSignature: setup.localSignature,
                remoteSignatures: ["sig-remote-a"]
            )
            var builderCalled = false
            var outputLines: [String] = []

            let result = try await runThunderboltShowdownPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                setup: setup,
                slotOverrides: nil,
                modelMode: .auto,
                preflight: .remotePriorGap,
                priorUpdatePolicy: .candidateOnly,
                candidateBuilder: { _, _, _, _, builderSetup, _, preflight in
                    builderCalled = true
                    #expect(preflight == .remotePriorGap)
                    #expect(builderSetup.reachableWorkers.map(\.host) == ["worker-a"])
                    return ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: candidateArtifact,
                        contributedRemoteHosts: ["worker-a"],
                        skippedRemoteHosts: [:]
                    )
                },
                prepareSetup: { _, _, _, _, _, priorTableOverride in
                    makeShowdownSetup(
                        reachableWorkers: [makeWorker(host: "worker-a", slots: 2)],
                        priorTable: priorTableOverride ?? BenchmarkPriorTable()
                    )
                },
                outputLine: { outputLines.append($0) }
            )

            let maintenanceResult = try #require(result)
            #expect(builderCalled)
            #expect(maintenanceResult.candidateArtifact.machines.contains { $0.signature == "sig-remote-a" })
            #expect(FileManager.default.fileExists(atPath: maintenanceResult.paths.canonicalPath))
            #expect(lineContainsAllMarkers(outputLines, markers: ["worker-a"]))
            #expect(lineContainsAllMarkers(outputLines, markers: [maintenanceResult.paths.canonicalPath]))
        }
    }

    @Test("unreachable workers are excluded without corrupting candidate generation")
    func unreachableWorkersExcludedWithoutCorruptingCandidateGeneration() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let reachableWorker = makeWorker(host: "worker-a", slots: 2)
            let setup = makeShowdownSetup(
                reachableWorkers: [reachableWorker],
                workerCapsByHost: [
                    "worker-a": makeCaps(signature: "sig-remote-a"),
                    "worker-b": makeCaps(signature: "sig-remote-b"),
                ]
            )

            let result = try await runThunderboltShowdownPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                setup: setup,
                slotOverrides: nil,
                modelMode: .auto,
                preflight: .remotePriorGap,
                priorUpdatePolicy: .candidateOnly,
                candidateBuilder: { _, _, _, _, builderSetup, _, _ in
                    #expect(builderSetup.reachableWorkers.map(\.host) == ["worker-a"])
                    return ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(
                            localSignature: builderSetup.localSignature,
                            remoteSignatures: ["sig-remote-a"]
                        ),
                        contributedRemoteHosts: ["worker-a"],
                        skippedRemoteHosts: [:]
                    )
                },
                prepareSetup: { _, _, _, _, _, priorTableOverride in
                    makeShowdownSetup(
                        reachableWorkers: [reachableWorker],
                        priorTable: priorTableOverride ?? BenchmarkPriorTable(),
                        workerCapsByHost: ["worker-a": makeCaps(signature: "sig-remote-a")]
                    )
                }
            )

            let maintenanceResult = try #require(result)
            #expect(maintenanceResult.candidateArtifact.machines.contains { $0.signature == "sig-remote-a" })
            #expect(!maintenanceResult.candidateArtifact.machines.contains { $0.signature == "sig-remote-b" })
        }
    }

    @Test("reachable worker with insufficient isolated data is skipped with truthful outcome")
    func insufficientRemoteDataIsReportedTruthfully() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let setup = makeShowdownSetup(reachableWorkers: [makeWorker(host: "worker-a", slots: 2)])
            var outputLines: [String] = []

            let result = try await runThunderboltShowdownPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                setup: setup,
                slotOverrides: nil,
                modelMode: .auto,
                preflight: .remotePriorGap,
                priorUpdatePolicy: .candidateOnly,
                candidateBuilder: { _, _, _, _, builderSetup, _, _ in
                    ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(localSignature: builderSetup.localSignature),
                        contributedRemoteHosts: [],
                        skippedRemoteHosts: ["worker-a": "insufficient valid isolated data"]
                    )
                },
                prepareSetup: { _, _, _, _, _, priorTableOverride in
                    makeShowdownSetup(
                        reachableWorkers: [makeWorker(host: "worker-a", slots: 2)],
                        priorTable: priorTableOverride ?? BenchmarkPriorTable()
                    )
                },
                outputLine: { outputLines.append($0) }
            )

            let maintenanceResult = try #require(result)
            #expect(maintenanceResult.candidateArtifact.machines.map(\.signature) == [setup.localSignature])
            #expect(lineContainsAllMarkers(outputLines, markers: ["worker-a", "insufficient valid isolated data"]))
        }
    }

    @Test("reachable worker with valid isolated data contributes a remote machine profile to the candidate overlay")
    func validRemoteDataContributesToOverlay() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let worker = makeWorker(host: "worker-a", slots: 2)
            let setup = makeShowdownSetup(reachableWorkers: [worker])
            let remoteSignature = "sig-remote-a"

            let result = try await runThunderboltShowdownPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                setup: setup,
                slotOverrides: nil,
                modelMode: .auto,
                preflight: .remotePriorGap,
                priorUpdatePolicy: .candidateOnly,
                candidateBuilder: { _, _, _, _, builderSetup, _, _ in
                    ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(
                            localSignature: builderSetup.localSignature,
                            remoteSignatures: [remoteSignature]
                        ),
                        contributedRemoteHosts: [worker.host],
                        skippedRemoteHosts: [:]
                    )
                },
                prepareSetup: { _, _, _, _, _, priorTableOverride in
                    let priorTable = try #require(priorTableOverride)
                    #expect(priorTable.lookup(signature: remoteSignature, concurrency: 1) != nil)
                    return makeShowdownSetup(
                        reachableWorkers: [worker],
                        priorTable: priorTable
                    )
                }
            )

            let maintenanceResult = try #require(result)
            #expect(maintenanceResult.setup.priorTable.lookup(signature: remoteSignature, concurrency: 1) != nil)
        }
    }

    @Test("maintenance candidate rewrites legacy remote prior into explicit affine form")
    func maintenanceCandidateRewritesLegacyRemotePriorIntoExplicitAffineForm() throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }

        let localSignature = "sig-local"
        let remoteSignature = "sig-remote-a"
        let corpus = makeRepresentativeCorpus(frameCounts: [100, 200, 400], directory: baseDirectory)
        let rewrittenRemote = try #require(
            buildThunderboltRemoteMaintenanceMachine(
                worker: makeWorker(host: "worker-a", slots: 2),
                caps: makeCaps(signature: remoteSignature),
                preset: defaultVideoPreset,
                telemetry: [
                    makeRemoteMaintenanceTelemetry(videoID: "video-100", frameCount: 100, concurrency: 1, processMS: 300),
                    makeRemoteMaintenanceTelemetry(videoID: "video-200", frameCount: 200, concurrency: 1, processMS: 500),
                    makeRemoteMaintenanceTelemetry(videoID: "video-400", frameCount: 400, concurrency: 1, processMS: 900),
                    makeRemoteMaintenanceTelemetry(videoID: "video-100", frameCount: 100, concurrency: 2, processMS: 450),
                    makeRemoteMaintenanceTelemetry(videoID: "video-200", frameCount: 200, concurrency: 2, processMS: 750),
                    makeRemoteMaintenanceTelemetry(videoID: "video-400", frameCount: 400, concurrency: 2, processMS: 1_350),
                ]
            ).machine
        )
        let legacyArtifact = BenchmarkPriorArtifact(
            generatedAt: Date(timeIntervalSince1970: 0),
            corpusHash: "sha256:legacy",
            corpusSummary: BenchmarkPriorCorpusSummary(videoCount: corpus.count, totalBytes: Int64(corpus.reduce(0) { $0 + $1.sizeBytes })),
            machines: [
                makeMachine(signature: localSignature),
                BenchmarkPriorMachine(
                    signature: remoteSignature,
                    chipName: "Apple M4",
                    performanceCores: 4,
                    efficiencyCores: 6,
                    videoEncodeEngines: 1,
                    osVersion: "26.0",
                    transcodePreset: defaultVideoPreset,
                    msPerFrameC1: 1.0,
                    fixedOverheadMS: 0,
                    avgCorpusFrameCount: 400,
                    affineModelSource: .legacyHeuristic,
                    cells: [
                        BenchmarkPriorCell(
                            concurrency: 1,
                            videosPerMin: 150,
                            msPerVideoP50: 400,
                            msPerVideoP95: 480,
                            degradationRatio: 1.0
                        ),
                    ]
                ),
            ]
        )
        let localInputs = ThunderboltLocalPriorCandidateInputs(
            videoSweep: [
                ConcurrencySweepPoint(
                    concurrency: 1,
                    throughputPerMinute: 200,
                    p50Seconds: 0.3,
                    p95Seconds: 0.36,
                    peakMemoryMB: 128
                ),
            ],
            frameCounts: [100, 200, 400],
            localAffineSamples: [
                LocalVideoAffineSample(sourcePath: corpus[0].path, frameCount: 100, processMS: 500),
                LocalVideoAffineSample(sourcePath: corpus[1].path, frameCount: 200, processMS: 700),
                LocalVideoAffineSample(sourcePath: corpus[2].path, frameCount: 400, processMS: 1_100),
            ]
        )

        let builtCandidateArtifact = try buildThunderboltLocalPriorCandidateArtifact(
            corpus: corpus,
            hardware: sampleHardware(),
            preset: defaultVideoPreset,
            baseArtifact: legacyArtifact,
            mergedMachines: [rewrittenRemote],
            inputs: localInputs
        )
        let candidateArtifact = try #require(builtCandidateArtifact)
        let migratedRemote = try #require(candidateArtifact.machines.first { $0.signature == remoteSignature })
        #expect(migratedRemote.affineModelSource == .explicit)
        #expect(abs(migratedRemote.fixedOverheadMS - 100.0) < 0.001)
        #expect(abs(migratedRemote.msPerFrameC1 - 2.0) < 0.001)
    }

    @Test("showdown setup after maintenance uses the overlay even when canonical is unchanged")
    func showdownMaintenanceUsesOverlayWhenCanonicalIsUnchanged() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let worker = makeWorker(host: "worker-a", slots: 2)
            let setup = makeShowdownSetup(reachableWorkers: [worker])
            let localOnlyCanonical = makePriorArtifact(localSignature: setup.localSignature)
            try localOnlyCanonical.write(toPath: resolveThunderboltCAPriorPaths().canonicalPath)

            let remoteSignature = "sig-remote-a"
            let result = try await runThunderboltShowdownPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                setup: setup,
                slotOverrides: nil,
                modelMode: .auto,
                preflight: .remotePriorGap,
                priorUpdatePolicy: .promoteGuarded,
                candidateBuilder: { _, _, _, _, builderSetup, _, _ in
                    ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(
                            localSignature: builderSetup.localSignature,
                            remoteSignatures: [remoteSignature]
                        ),
                        contributedRemoteHosts: [worker.host],
                        skippedRemoteHosts: [:]
                    )
                },
                prepareSetup: { _, _, _, _, _, priorTableOverride in
                    makeShowdownSetup(
                        reachableWorkers: [worker],
                        priorTable: try #require(priorTableOverride)
                    )
                }
            )

            let maintenanceResult = try #require(result)
            let canonicalArtifact = try #require(BenchmarkPriorArtifact.load(fromPath: maintenanceResult.paths.canonicalPath))
            let candidateArtifact = try #require(BenchmarkPriorArtifact.load(fromPath: maintenanceResult.paths.candidatePath))
            #expect(maintenanceResult.deferredPromotion)
            #expect(maintenanceResult.setup.priorTable.lookup(signature: remoteSignature, concurrency: 1) != nil)
            #expect(!canonicalArtifact.machines.contains { $0.signature == remoteSignature })
            #expect(candidateArtifact.machines.contains { $0.signature == remoteSignature })
        }
    }

    @Test("representative remote sample-set follows the locked frame-count quantile rule deterministically")
    func representativeSampleSetUsesLockedQuantilesDeterministically() {
        let largeFrameCounts: [Double] = [400, 100, 700, 300, 600, 200, 500]
        let largeCorpus = makeRepresentativeCorpus(frameCounts: largeFrameCounts)
        let largeSelected = selectThunderboltRemoteMaintenanceRepresentativeVideos(
            from: largeCorpus,
            frameCounts: largeFrameCounts
        )

        #expect(largeSelected.map { $0.video.name } == [
            "video-1.mov",
            "video-5.mov",
            "video-0.mov",
            "video-6.mov",
            "video-2.mov",
        ])
        #expect(largeSelected.map(\.frameCount) == [100, 200, 400, 500, 700])

        let smallFrameCounts: [Double] = [300, 100, 200, 50]
        let smallCorpus = makeRepresentativeCorpus(frameCounts: smallFrameCounts)
        let smallSelected = selectThunderboltRemoteMaintenanceRepresentativeVideos(
            from: smallCorpus,
            frameCounts: smallFrameCounts
        )

        #expect(smallSelected.map { $0.video.name } == [
            "video-3.mov",
            "video-1.mov",
            "video-2.mov",
            "video-0.mov",
        ])
        #expect(smallSelected.map(\.frameCount) == [50, 100, 200, 300])
    }

    @Test("remote maintenance no longer stops after one selected video when enough corpus videos exist")
    func remoteMaintenanceNoLongerStopsAfterOneSelectedVideo() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }

        let worker = makeWorker(host: "worker-a", slots: 2)
        let frameCounts: [Double] = [400, 100, 700, 300, 600, 200, 500]
        let corpus = makeRepresentativeCorpus(frameCounts: frameCounts, directory: baseDirectory)
        let preparedSamples = try #require(
            prepareThunderboltRemoteMaintenanceRepresentativeSamples(
                from: corpus,
                frameCounts: frameCounts
            )
        )

        #expect(preparedSamples.map { $0.video.name } == [
            "video-1.mov",
            "video-5.mov",
            "video-0.mov",
            "video-6.mov",
            "video-2.mov",
        ])

        let selectedPaths = Set(preparedSamples.map(\.video.path))
        let selectedNames = Set(preparedSamples.map { $0.video.name })
        let selected = selectThunderboltRemoteMaintenanceRepresentativeVideos(
            from: corpus,
            frameCounts: frameCounts
        )
        let probedVideoNames = ConcurrentStringCollector()

        let telemetry = await collectThunderboltRemoteMaintenanceTelemetry(
            worker: worker,
            workerSignature: "sig-remote-a",
            samples: preparedSamples,
            port: 7_000,
            connectTimeout: 100,
            roundTripRunner: { _, video, _, _, _, _ in
                probedVideoNames.append(video.name)
                return ThunderboltRoundTripResult(
                    success: true,
                    sendSeconds: 0.01,
                    processNanos: 20_000_000,
                    receiveSeconds: 0.01,
                    totalSeconds: 0.02
                )
            }
        )

        let recordedVideoNames = probedVideoNames.snapshot()
        #expect(Set(recordedVideoNames) == selectedNames)
        #expect(Set(recordedVideoNames).count == 5)
        #expect(Set(telemetry.map(\.videoPath)) == selectedPaths)
        #expect(preparedSamples.map { $0.video.path } == selected.map { $0.video.path })
        #expect(telemetry.count == selected.count * 3)
    }

    @Test("remote representative prior emits fixed overhead and median-ratio degradation")
    func remoteRepresentativePriorEmitsFixedOverheadAndMedianRatio() throws {
        let telemetry = [
            makeRemoteMaintenanceTelemetry(videoID: "video-100", frameCount: 100, concurrency: 1, processMS: 300),
            makeRemoteMaintenanceTelemetry(videoID: "video-200", frameCount: 200, concurrency: 1, processMS: 500),
            makeRemoteMaintenanceTelemetry(videoID: "video-400", frameCount: 400, concurrency: 1, processMS: 900),
            makeRemoteMaintenanceTelemetry(videoID: "video-100", frameCount: 100, concurrency: 2, processMS: 450),
            makeRemoteMaintenanceTelemetry(videoID: "video-200", frameCount: 200, concurrency: 2, processMS: 750),
            makeRemoteMaintenanceTelemetry(videoID: "video-400", frameCount: 400, concurrency: 2, processMS: 1_350),
        ]

        let result = buildThunderboltRemoteMaintenanceMachine(
            worker: makeWorker(host: "worker-a", slots: 2),
            caps: makeCaps(signature: "sig-remote-a"),
            preset: defaultVideoPreset,
            telemetry: telemetry
        )
        let machine = try #require(result.machine)
        let c1Cell = try #require(machine.cells.first { $0.concurrency == 1 })
        let c2Cell = try #require(machine.cells.first { $0.concurrency == 2 })

        #expect(machine.affineModelSource == .explicit)
        #expect(abs(machine.fixedOverheadMS - 100.0) < 0.001)
        #expect(abs(machine.msPerFrameC1 - 2.0) < 0.001)
        #expect(abs(c1Cell.degradationRatio - 1.0) < 0.001)
        #expect(abs(c2Cell.degradationRatio - 1.5) < 0.001)
    }

    @Test("remote maintenance reports bounded telemetry progress with truthful skip output")
    func remoteMaintenanceReportsBoundedTelemetryProgressWithTruthfulSkipOutput() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }

        let worker = makeWorker(host: "worker-a", slots: 2)
        let frameCounts: [Double] = [400, 100, 700, 300, 600, 200, 500]
        let corpus = makeRepresentativeCorpus(frameCounts: frameCounts, directory: baseDirectory)
        let setup = makeShowdownSetup(
            videos: corpus,
            reachableWorkers: [worker],
            workerCapsByHost: ["worker-a": makeCaps(signature: "sig-remote-a")]
        )
        var outputLines: [String] = []

        let progressReporter = ThunderboltPriorMaintenanceProgressReporter(transientWriter: { _ in })
        let result = try await buildThunderboltShowdownPriorCandidateArtifact(
            corpus: corpus,
            preset: defaultVideoPreset,
            timeout: 120,
            hardware: sampleHardware(),
            setup: setup,
            currentCanonicalArtifact: nil,
            preflight: .remotePriorGap,
            burstConfigRunner: { config, _, _, _, _, _, _ in
                let wallSeconds = 0.4 + (Double(config.localSlots) * 0.1)
                return ThunderboltBurstResult(
                    wallSeconds: wallSeconds,
                    completed: config.localSlots,
                    failed: 0,
                    completionSeconds: Array(repeating: wallSeconds, count: max(1, config.localSlots))
                )
            },
            frameCountEstimator: { _ in frameCounts },
            localAffineSampleCollector: { emittedVideos, _, _, _ in
                [
                    LocalVideoAffineSample(sourcePath: emittedVideos[0].path, frameCount: 100, processMS: 500),
                    LocalVideoAffineSample(sourcePath: emittedVideos[1].path, frameCount: 200, processMS: 700),
                    LocalVideoAffineSample(sourcePath: emittedVideos[2].path, frameCount: 400, processMS: 1_100),
                ]
            },
            progressReporter: progressReporter,
            roundTripRunner: { _, _, _, _, _, _ in
                ThunderboltRoundTripResult(
                    success: false,
                    sendSeconds: 0.01,
                    processNanos: 0,
                    receiveSeconds: 0.01,
                    totalSeconds: 0.02
                )
            },
            outputLine: { outputLines.append($0) }
        )
        let boardLines = await progressReporter.finish(terminalFinalize: { _ in })
        outputLines.append(contentsOf: boardLines)

        let buildResult = try #require(result)
        #expect(buildResult.contributedRemoteHosts.isEmpty)
        #expect(buildResult.skippedRemoteHosts["worker-a"] == "insufficient valid isolated data")
        #expect(progressBoardContainsStage(
            boardLines,
            stage: .remoteSamplePreparation,
            status: .done,
            detailMarkers: ["5 samples"]
        ))
        #expect(progressBoardContainsStage(
            boardLines,
            stage: .remoteTelemetry,
            status: .done,
            detailMarkers: ["1/1 workers", "15/15 probes"]
        ))
    }
}

private func sampleCorpus() -> [MediaFile] {
    [
        MediaFile(
            path: "/tmp/sample.mov",
            name: "sample.mov",
            type: .video,
            sizeBytes: 1_024
        )
    ]
}

private func sampleHardware() -> HardwareProfile {
    HardwareProfile(
        chipName: "Apple M4",
        performanceCores: 4,
        efficiencyCores: 6,
        totalCores: 10,
        memoryGB: 16,
        videoEncodeEngines: 1,
        hwEncoderNames: []
    )
}

private func makeRepresentativeCorpus(
    frameCounts: [Double],
    directory: String? = nil
) -> [MediaFile] {
    frameCounts.enumerated().map { index, frameCount in
        let path: String
        if let directory {
            let url = URL(fileURLWithPath: directory)
                .appendingPathComponent("video-\(index).mov")
            try! Data("video-\(index)-\(Int(frameCount))".utf8).write(to: url)
            path = url.path
        } else {
            path = "/tmp/video-\(index).mov"
        }
        return MediaFile(
            path: path,
            name: "video-\(index).mov",
            type: .video,
            sizeBytes: max(1, Int(frameCount))
        )
    }
}

private final class ConcurrentStringCollector: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String] = []

    func append(_ value: String) {
        lock.lock()
        values.append(value)
        lock.unlock()
    }

    func snapshot() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return values
    }
}

private func lineContainsAllMarkers(
    _ lines: [String],
    markers: [String]
) -> Bool {
    lines.contains { line in
        markers.allSatisfy(line.contains)
    }
}

private func progressBoardContainsStage(
    _ lines: [String],
    stage: ThunderboltPriorMaintenanceStage,
    status: ThunderboltPriorMaintenanceStageStatus,
    detailMarkers: [String]
) -> Bool {
    lineContainsAllMarkers(
        lines,
        markers: [stage.label, status.rawValue] + detailMarkers
    )
}

private func makeRemoteMaintenanceTelemetry(
    videoID: String,
    frameCount: Double,
    concurrency: Int,
    processMS: Double,
    txInMS: Double = 0,
    txOutMS: Double = 0,
    publishOverheadMS: Double = 0
) -> ThunderboltRemoteMaintenanceTelemetrySample {
    makeThunderboltRemoteMaintenanceTelemetrySample(
        host: "worker-a",
        workerSignature: "sig-remote-a",
        concurrency: concurrency,
        isolated: true,
        success: true,
        actualExecutor: "worker-a",
        processNanos: UInt64(processMS * 1_000_000.0),
        txInMS: txInMS,
        txOutMS: txOutMS,
        publishOverheadMS: publishOverheadMS,
        videoPath: "/tmp/\(videoID).mov",
        frameCount: frameCount
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

private func makeShowdownSetup(
    videos: [MediaFile] = sampleCorpus(),
    reachableWorkers: [ThunderboltBoundWorkerSpec],
    priorTable: BenchmarkPriorTable = BenchmarkPriorTable(),
    workerCapsByHost: [String: WorkerCaps] = [:]
) -> ThunderboltCARunSetup {
    let localSignature = "sig-local"
    let modelInputs = [
        ThunderboltCAModelInputRow(
            machineID: "local",
            slotCount: 1,
            msPerFrameC1: 1.0,
            msSource: "test",
            curveSource: "test",
            txInMS: 0,
            txOutMS: 0,
            publishOverheadMS: 0
        )
    ]
    let diagnostics = ThunderboltCAModelDiagnostics(
        mode: .auto,
        coverageRows: reachableWorkers.map { worker in
            ThunderboltCARemoteCoverageRow(
                host: worker.host,
                reachableSlots: worker.slots,
                modeledSlots: 0,
                msSource: "fallback(local-c1)",
                curveSource: "fallback(local-curve)",
                note: "gap"
            )
        },
        strictExclusions: [],
        reachableWorkerCount: reachableWorkers.count,
        reachableSlotCount: reachableWorkers.reduce(0) { $0 + $1.slots },
        modeledWorkerCount: 0,
        modeledSlotCount: 0,
        fallbackActive: true,
        localPriorGap: false,
        remotePriorGap: !reachableWorkers.isEmpty
    )

    return ThunderboltCARunSetup(
        port: 7_000,
        connectTimeout: 100,
        videos: videos,
        videoCosts: makeDefaultResolvedVideoCosts(count: videos.count),
        priorTable: priorTable,
        localSignature: localSignature,
        localSlotCount: 1,
        localMSPerFrameC1: 1.0,
        sourceHashes: [:],
        slots: [.local(index: 1)],
        machineProfiles: [
            ThunderboltCAMachineProfile(
                id: "local",
                msPerFrameC1: 1.0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0
            )
        ],
        slotBindings: [
            ThunderboltCASlotBinding(machineIndex: 0, slotID: "local#s1")
        ],
        machineIndexByHost: [:],
        modelInputs: modelInputs,
        diagnostics: diagnostics,
        reachableWorkers: reachableWorkers,
        workerCapsByHost: workerCapsByHost
    )
}

private func makePriorArtifact(
    localSignature: String,
    remoteSignatures: [String] = []
) -> BenchmarkPriorArtifact {
    let machines = [makeMachine(signature: localSignature)] + remoteSignatures.map {
        makeMachine(signature: $0, msPerFrameC1: 0.8)
    }
    return BenchmarkPriorArtifact(
        generatedAt: Date(timeIntervalSince1970: 0),
        corpusHash: "sha256:test",
        corpusSummary: BenchmarkPriorCorpusSummary(videoCount: 1, totalBytes: 1_024),
        machines: machines
    )
}

private func makeMachine(
    signature: String,
    msPerFrameC1: Double = 1.0
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
        avgCorpusFrameCount: 2_400,
        cells: [
            BenchmarkPriorCell(concurrency: 1, videosPerMin: 10, msPerVideoP50: 4_000, msPerVideoP95: 5_000, degradationRatio: 1.0),
            BenchmarkPriorCell(concurrency: 2, videosPerMin: 16, msPerVideoP50: 5_200, msPerVideoP95: 6_300, degradationRatio: 1.30),
        ]
    )
}

private func makeTempDirectory() -> String {
    let path = FileManager.default.temporaryDirectory
        .appendingPathComponent("tb-remote-prior-\(UUID().uuidString)")
        .path
    try! FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true)
    return path
}
