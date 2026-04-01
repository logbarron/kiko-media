import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt full prior maintenance", Testing.ParallelizationTrait.serialized)
struct ThunderboltFullPriorMaintenanceTests {
    @Test("full with policy off does not run the builder")
    func fullWithPolicyOffDoesNotRunBuilder() async throws {
        var setupBuilderCalled = false
        var candidateBuilderCalled = false

        let result = try await runThunderboltFullPriorMaintenance(
            corpus: sampleCorpus(),
            preset: defaultVideoPreset,
            timeout: 120,
            hardware: sampleHardware(),
            modelMode: .auto,
            priorUpdatePolicy: .off,
            priorTableOverlay: nil,
            remoteTxInEstimateMSByHost: [:],
            setupBuilder: { _, _, _, _, _, _ in
                setupBuilderCalled = true
                return makeShowdownSetup(reachableWorkers: [makeWorker(host: "worker-a", slots: 2)])
            },
            candidateBuilder: { _, _, _, _, _, _, _ in
                candidateBuilderCalled = true
                return nil
            }
        )

        #expect(result == nil)
        #expect(!setupBuilderCalled)
        #expect(!candidateBuilderCalled)
        #expect(!shouldRunThunderboltFullPriorMaintenance(includeShowdown: true, priorUpdatePolicy: .off))
    }

    @Test("full with policy on runs the builder before showdown")
    func fullWithPolicyOnRunsBuilder() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            var candidateBuilderCalled = false
            var outputLines: [String] = []
            let worker = makeWorker(host: "worker-a", slots: 2)

            let result = try await runThunderboltFullPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                modelMode: .auto,
                priorUpdatePolicy: .candidateOnly,
                priorTableOverlay: nil,
                remoteTxInEstimateMSByHost: [:],
                setupBuilder: { _, _, _, _, _, _ in
                    makeShowdownSetup(reachableWorkers: [worker])
                },
                candidateBuilder: { _, _, _, _, builderSetup, _, _ in
                    candidateBuilderCalled = true
                    return ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(
                            localSignature: builderSetup.localSignature,
                            remoteSignatures: ["sig-remote-a"]
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
                },
                outputLine: { outputLines.append($0) }
            )

            let maintenanceResult = try #require(result)
            #expect(candidateBuilderCalled)
            #expect(shouldRunThunderboltFullPriorMaintenance(includeShowdown: true, priorUpdatePolicy: .candidateOnly))
            #expect(maintenanceResult.candidateArtifact.machines.contains { $0.signature == "sig-remote-a" })
            #expect(FileManager.default.fileExists(atPath: maintenanceResult.paths.canonicalPath))
            #expect(lineContainsAllMarkers(outputLines, markers: [worker.host]))
            #expect(lineContainsAllMarkers(outputLines, markers: [maintenanceResult.paths.canonicalPath]))
        }
    }

    @Test("full builder can produce a local plus remote candidate set")
    func fullBuilderProducesLocalPlusRemoteCandidateSet() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let worker = makeWorker(host: "worker-a", slots: 2)
            let result = try await runThunderboltFullPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                modelMode: .auto,
                priorUpdatePolicy: .candidateOnly,
                priorTableOverlay: nil,
                remoteTxInEstimateMSByHost: [:],
                setupBuilder: { _, _, _, _, _, _ in
                    makeShowdownSetup(reachableWorkers: [worker])
                },
                candidateBuilder: { _, _, _, _, builderSetup, _, _ in
                    ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(
                            localSignature: builderSetup.localSignature,
                            remoteSignatures: ["sig-remote-a"]
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
            #expect(maintenanceResult.setup.priorTable.lookup(signature: "sig-remote-a", concurrency: 1) != nil)
        }
    }

    @Test("full showdown uses the in-memory candidate overlay even without canonical promotion")
    func fullShowdownUsesInMemoryOverlayWithoutPromotion() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let worker = makeWorker(host: "worker-a", slots: 2)
            let canonical = makePriorArtifact(localSignature: "sig-local")
            try canonical.write(toPath: resolveThunderboltCAPriorPaths().canonicalPath)
            var outputLines: [String] = []

            let result = try await runThunderboltFullPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                modelMode: .auto,
                priorUpdatePolicy: .promoteGuarded,
                priorTableOverlay: nil,
                remoteTxInEstimateMSByHost: [:],
                setupBuilder: { _, _, _, _, _, _ in
                    makeShowdownSetup(reachableWorkers: [worker])
                },
                candidateBuilder: { _, _, _, _, builderSetup, _, _ in
                    ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(
                            localSignature: builderSetup.localSignature,
                            remoteSignatures: ["sig-remote-a"]
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
                },
                outputLine: { outputLines.append($0) }
            )

            let maintenanceResult = try #require(result)
            let canonicalArtifact = try #require(BenchmarkPriorArtifact.load(fromPath: maintenanceResult.paths.canonicalPath))
            let candidateArtifact = try #require(BenchmarkPriorArtifact.load(fromPath: maintenanceResult.paths.candidatePath))
            #expect(maintenanceResult.deferredPromotion)
            #expect(maintenanceResult.setup.priorTable.lookup(signature: "sig-remote-a", concurrency: 1) != nil)
            #expect(lineContainsAllMarkers(outputLines, markers: [worker.host]))
            #expect(!canonicalArtifact.machines.contains { $0.signature == "sig-remote-a" })
            #expect(candidateArtifact.machines.contains { $0.signature == "sig-remote-a" })
        }
    }

    @Test("full prior maintenance forces healthy-preflight maintenance instead of skipping")
    func fullPriorMaintenanceForcesHealthyPreflightMaintenanceInsteadOfSkipping() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        let localSignature = "sig-local-healthy-preflight"

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            var candidateBuilderCalled = false
            var prepareSetupCalled = false
            var outputLines: [String] = []

            let result = try await runThunderboltFullPriorMaintenance(
                corpus: sampleCorpus(),
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: sampleHardware(),
                modelMode: .auto,
                priorUpdatePolicy: .candidateOnly,
                priorTableOverlay: nil,
                remoteTxInEstimateMSByHost: [:],
                setupBuilder: { _, _, _, _, _, _ in
                    makeShowdownSetup(
                        reachableWorkers: [],
                        localSignature: localSignature
                    )
                },
                candidateBuilder: { _, _, _, _, builderSetup, _, preflight in
                    candidateBuilderCalled = true
                    #expect(preflight.rawValue == ThunderboltShowdownPreflightClassification.healthy.rawValue)
                    return ThunderboltShowdownPriorCandidateBuildResult(
                        artifact: makePriorArtifact(localSignature: builderSetup.localSignature),
                        contributedRemoteHosts: [],
                        skippedRemoteHosts: [:]
                    )
                },
                prepareSetup: { _, _, _, _, _, priorTableOverride in
                    prepareSetupCalled = true
                    let priorTable = try #require(priorTableOverride)
                    #expect(priorTable.lookup(signature: localSignature, concurrency: 1) != nil)
                    return makeShowdownSetup(
                        reachableWorkers: [],
                        priorTable: priorTable,
                        localSignature: localSignature
                    )
                },
                outputLine: { outputLines.append($0) }
            )

            let maintenanceResult = try #require(result)
            #expect(candidateBuilderCalled)
            #expect(prepareSetupCalled)
            #expect(maintenanceResult.setup.diagnostics.localPriorGap == false)
            #expect(maintenanceResult.setup.diagnostics.remotePriorGap == false)
            #expect(maintenanceResult.candidateArtifact.machines.map(\.signature) == [localSignature])
            #expect(FileManager.default.fileExists(atPath: maintenanceResult.paths.canonicalPath))
            #expect(lineContainsAllMarkers(outputLines, markers: [maintenanceResult.paths.canonicalPath]))
        }
    }

    @Test("full prior maintenance preserves shared affine local prior output")
    func fullPriorMaintenancePreservesSharedAffineLocalPriorOutput() async throws {
        let baseDirectory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: baseDirectory) }
        let corpusRoot = URL(fileURLWithPath: baseDirectory, isDirectory: true)
        let corpus = try makeAffineCorpus(root: corpusRoot)
        let hardware = sampleHardware()
        let localSignature = makeLocalSignature(hardware: hardware)
        let expectedFrameCounts = [100.0, 200.0, 400.0]
        let expectedFrameCountByPath = Dictionary(uniqueKeysWithValues: zip(corpus.map(\.path), expectedFrameCounts))

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            var burstRunnerCalled = false
            var frameCountEstimatorCalled = false
            var localAffineCollectorCalled = false
            var builderOutputLines: [String] = []
            var maintenanceOutputLines: [String] = []
            let result = try await runThunderboltFullPriorMaintenance(
                corpus: corpus,
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: hardware,
                modelMode: .auto,
                priorUpdatePolicy: .candidateOnly,
                priorTableOverlay: nil,
                remoteTxInEstimateMSByHost: [:],
                setupBuilder: { _, _, _, _, _, _ in
                    makeShowdownSetup(
                        videos: corpus,
                        reachableWorkers: [],
                        localSignature: localSignature
                    )
                },
                candidateBuilder: { builderCorpus, preset, timeout, hardware, builderSetup, currentCanonicalArtifact, preflight in
                    let progressReporter = ThunderboltPriorMaintenanceProgressReporter(transientWriter: { _ in })
                    let result = try await buildThunderboltShowdownPriorCandidateArtifact(
                        corpus: builderCorpus,
                        preset: preset,
                        timeout: timeout,
                        hardware: hardware,
                        setup: builderSetup,
                        currentCanonicalArtifact: currentCanonicalArtifact,
                        preflight: preflight,
                        probeAllReachableRemoteWorkers: true,
                        burstConfigRunner: { config, workers, videos, _, _, emittedPreset, emittedTimeout in
                            burstRunnerCalled = true
                            #expect(workers.isEmpty)
                            #expect(videos.map(\.path) == corpus.map(\.path))
                            #expect(emittedPreset == defaultVideoPreset)
                            #expect(emittedTimeout == 120)
                            let wallSeconds = 0.5 + (Double(config.localSlots) * 0.2)
                            return ThunderboltBurstResult(
                                wallSeconds: wallSeconds,
                                completed: config.localSlots,
                                failed: 0,
                                completionSeconds: Array(repeating: wallSeconds, count: max(1, config.localSlots))
                            )
                        },
                        frameCountEstimator: { videos in
                            frameCountEstimatorCalled = true
                            return Array(expectedFrameCounts.prefix(videos.count))
                        },
                        localAffineSampleCollector: { emittedVideos, emittedPreset, emittedTimeout, frameCountByPath in
                            localAffineCollectorCalled = true
                            #expect(emittedVideos.map(\.path) == corpus.map(\.path))
                            #expect(emittedPreset == defaultVideoPreset)
                            #expect(emittedTimeout == 120)
                            #expect(frameCountByPath == expectedFrameCountByPath)
                            return sampleLocalAffineSamples(corpus: corpus)
                        },
                        progressReporter: progressReporter,
                        outputLine: { builderOutputLines.append($0) }
                    )
                    let boardLines = await progressReporter.finish(terminalFinalize: { _ in })
                    builderOutputLines.append(contentsOf: boardLines)
                    return result
                },
                prepareSetup: { _, _, _, _, _, priorTableOverride in
                    makeShowdownSetup(
                        videos: corpus,
                        reachableWorkers: [],
                        priorTable: try #require(priorTableOverride),
                        localSignature: localSignature
                    )
                },
                outputLine: { maintenanceOutputLines.append($0) }
            )

            let maintenanceResult = try #require(result)
            let localMachine = try #require(
                maintenanceResult.candidateArtifact.machines.first(where: { $0.signature == localSignature })
            )
            #expect(burstRunnerCalled)
            #expect(frameCountEstimatorCalled)
            #expect(localAffineCollectorCalled)
            #expect(abs(localMachine.fixedOverheadMS - 300) < 0.001)
            #expect(abs(localMachine.msPerFrameC1 - 2) < 0.001)
            #expect(maintenanceResult.setup.priorTable.lookup(signature: localSignature, concurrency: 1) != nil)
            #expect(builderOutputLines.count >= ThunderboltPriorMaintenanceStage.allCases.count + 2)
            #expect(progressBoardContainsStage(
                builderOutputLines,
                stage: .localSweep,
                status: .done,
                detailMarkers: ["3", "configs"]
            ))
            #expect(progressBoardContainsStage(
                builderOutputLines,
                stage: .frameCounting,
                status: .done,
                detailMarkers: ["3", "videos"]
            ))
            #expect(progressBoardContainsStage(
                builderOutputLines,
                stage: .affineSampleCollection,
                status: .done,
                detailMarkers: ["3", "samples"]
            ))
            #expect(progressBoardContainsStage(
                builderOutputLines,
                stage: .remoteSamplePreparation,
                status: .skipped,
                detailMarkers: []
            ))
            #expect(progressBoardContainsStage(
                builderOutputLines,
                stage: .remoteTelemetry,
                status: .skipped,
                detailMarkers: []
            ))
            #expect(maintenanceOutputLines.filter { $0.contains(ThunderboltPriorMaintenanceStage.setupRebuild.label) }.count >= 2)
        }
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

private func makeWorker(host: String, slots: Int) -> ThunderboltBoundWorkerSpec {
    ThunderboltBoundWorkerSpec(
        host: host,
        connectHost: host,
        slots: slots,
        sourceIP: "10.0.0.10",
        bridgeName: "bridge0"
    )
}

private func makeShowdownSetup(
    videos: [MediaFile] = sampleCorpus(),
    reachableWorkers: [ThunderboltBoundWorkerSpec],
    priorTable: BenchmarkPriorTable = BenchmarkPriorTable(),
    localSignature: String = "sig-local"
) -> ThunderboltCARunSetup {
    ThunderboltCARunSetup(
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
        modelInputs: [
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
        ],
        diagnostics: ThunderboltCAModelDiagnostics(
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
        ),
        reachableWorkers: reachableWorkers,
        workerCapsByHost: [:]
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
        .appendingPathComponent("tb-full-prior-\(UUID().uuidString)")
        .path
    try! FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true)
    return path
}

private func sampleVideoSweep(c1P50Seconds: Double) -> [ConcurrencySweepPoint] {
    [
        ConcurrencySweepPoint(
            concurrency: 1,
            throughputPerMinute: 60_000.0 / (c1P50Seconds * 1_000.0),
            p50Seconds: c1P50Seconds,
            p95Seconds: c1P50Seconds + 0.2,
            peakMemoryMB: 128
        )
    ]
}

private func sampleLocalAffineSamples(corpus: [MediaFile]) -> [LocalVideoAffineSample] {
    [
        LocalVideoAffineSample(sourcePath: corpus[0].path, frameCount: 100, processMS: 500),
        LocalVideoAffineSample(sourcePath: corpus[1].path, frameCount: 200, processMS: 700),
        LocalVideoAffineSample(sourcePath: corpus[2].path, frameCount: 400, processMS: 1_100),
    ]
}

private func makeAffineCorpus(root: URL) throws -> [MediaFile] {
    let names = ["a.mov", "b.mov", "c.mov"]
    return try names.enumerated().map { index, name in
        let url = root.appendingPathComponent(name)
        let data = Data("full-\(index)".utf8)
        try data.write(to: url, options: .atomic)
        return MediaFile(
            path: url.path,
            name: name,
            type: .video,
            sizeBytes: data.count
        )
    }
}

private func makeLocalSignature(hardware: HardwareProfile) -> String {
    WorkerSignatureBuilder.make(
        chipName: hardware.chipName,
        performanceCores: hardware.performanceCores,
        efficiencyCores: hardware.efficiencyCores,
        videoEncodeEngines: hardware.videoEncodeEngines,
        preset: defaultVideoPreset,
        osVersion: ProcessInfo.processInfo.operatingSystemVersion
    )
}
