import Foundation
import Synchronization
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt non-JSON prior emission")
struct ThunderboltNonJSONPriorEmissionTests {
    @Test("measurement-only burst/full prior emission defaults to non-mutating policy")
    func measurementOnlyBurstDefaultsToNonMutatingPolicy() async throws {
        let corpus = sampleCorpus()
        let videos = corpus.filter { $0.type == .video }
        let runs = [
            (
                ThunderboltBurstConfig(localSlots: 1, remoteSlots: [0]),
                ThunderboltBurstResult(
                    wallSeconds: 12.0,
                    completed: 1,
                    failed: 0,
                    completionSeconds: [12.0]
                )
            )
        ]
        let workers = [sampleWorker()]
        let hardware = sampleHardware()

        var emitterCalled = false
        var outputLines: [String] = []

        _ = try await emitThunderboltPriorAfterBurstSweepIfNeeded(
            corpus: corpus,
            videos: videos,
            runs: runs,
            workers: workers,
            port: 7300,
            connectTimeout: 900,
            hardware: hardware,
            preset: defaultVideoPreset,
            timeout: 120,
            emitPrior: {
                emittedCorpus,
                emittedVideos,
                emittedRuns,
                emittedWorkers,
                emittedPort,
                emittedConnectTimeout,
                emittedHardware,
                emittedPreset,
                emittedTimeout,
                emittedPolicy,
                emittedWorkerCapsByHost in
                emitterCalled = true
                #expect(emittedCorpus.count == corpus.count)
                #expect(emittedVideos.count == videos.count)
                #expect(emittedRuns.count == runs.count)
                #expect(emittedWorkers.count == workers.count)
                #expect(emittedPort == 7300)
                #expect(emittedConnectTimeout == 900)
                #expect(emittedHardware.chipName == hardware.chipName)
                #expect(emittedPreset == defaultVideoPreset)
                #expect(emittedTimeout == 120)
                #expect(emittedPolicy == .off)
                #expect(emittedWorkerCapsByHost.isEmpty)
                return (nil, .skippedInsufficientSignal)
            },
            outputLine: { line in
                outputLines.append(line)
            }
        )

        #expect(emitterCalled)
        expectOutputLines(
            outputLines,
            count: 3,
            containing: [
                ["prior generation", "insufficient successful isolated data"],
                ["Canonical prior:", "unchanged"],
                ["Recommendation:", "keep current"],
            ]
        )
    }

    @Test("non-showdown burst/full path forces prior updates off")
    func nonShowdownBurstSweepForcesPriorUpdatesOff() {
        #expect(
            effectiveThunderboltBurstSweepPriorUpdatePolicy(
                includeShowdown: false,
                showdownPriorUpdatePolicy: .candidateOnly
            ) == .off
        )
        #expect(
            effectiveThunderboltBurstSweepPriorUpdatePolicy(
                includeShowdown: false,
                showdownPriorUpdatePolicy: .promoteForce
            ) == .off
        )
        #expect(
            effectiveThunderboltBurstSweepPriorUpdatePolicy(
                includeShowdown: true,
                showdownPriorUpdatePolicy: .candidateOnly
            ) == .candidateOnly
        )
    }

    @Test("non-JSON burst/full path reports in-memory candidate when policy is off")
    func reportsInMemoryCandidateWhenPolicyIsOff() async throws {
        let corpus = sampleCorpus()
        let videos = corpus.filter { $0.type == .video }
        let workers = [sampleWorker()]
        var outputLines: [String] = []

        _ = try await emitThunderboltPriorAfterBurstSweepIfNeeded(
            corpus: corpus,
            videos: videos,
            runs: [],
            workers: workers,
            port: 7300,
            connectTimeout: 900,
            hardware: sampleHardware(),
            preset: defaultVideoPreset,
            timeout: 120,
            priorUpdatePolicy: .off,
            emitPrior: { _, _, _, _, _, _, _, _, _, emittedPolicy, _ in
                #expect(emittedPolicy == .off)
                return (sampleCandidateArtifact(), .skippedPolicyOff)
            },
            outputLine: { line in
                outputLines.append(line)
            }
        )

        expectOutputLines(
            outputLines,
            count: 4,
            containing: [
                ["benchmark prior candidate", "in-memory only"],
                ["prior update", "policy off"],
                ["Canonical prior:", "unchanged"],
                ["Recommendation:", "keep current"],
            ]
        )
    }

    @Test("non-JSON burst/full path reports prior emission failure truthfully")
    func reportsPriorEmissionFailureTruthfully() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempRoot) }

        let corpus = try makeAffineCorpus(root: tempRoot)
        let videos = corpus.filter { $0.type == .video }
        let workers = [sampleWorker()]
        let outputLines = Mutex<[String]>([])
        let expectedFrameCountByPath = defaultBurstSweepFrameCountByPath(videos: videos)
        let blockedBaseDirectory = tempRoot.appendingPathComponent("benchmark-prior-blocker")
        try Data("blocked".utf8).write(to: blockedBaseDirectory, options: .atomic)

        let candidateArtifact = try await TestEnvironment.withEnvironment([
            "BASE_DIRECTORY": blockedBaseDirectory.path,
        ]) {
            let priorPaths = resolveThunderboltCAPriorPaths()
            let candidateArtifact = try await emitThunderboltPriorAfterBurstSweepIfNeeded(
                corpus: corpus,
                videos: videos,
                runs: [
                    (
                        ThunderboltBurstConfig(localSlots: 1, remoteSlots: [0]),
                        ThunderboltBurstResult(
                            wallSeconds: 0.7,
                            completed: 1,
                            failed: 0,
                            completionSeconds: [0.7]
                        )
                    )
                ],
                workers: workers,
                port: 7300,
                connectTimeout: 900,
                hardware: sampleHardware(),
                preset: defaultVideoPreset,
                timeout: 120,
                priorUpdatePolicy: .candidateOnly,
                emitPrior: {
                    emittedCorpus,
                    emittedVideos,
                    emittedRuns,
                    emittedWorkers,
                    emittedPort,
                    emittedConnectTimeout,
                    emittedHardware,
                    emittedPreset,
                    emittedTimeout,
                    emittedPolicy,
                    emittedWorkerCapsByHost in
                    try await updateThunderboltPriorFromBurstSweep(
                        corpus: emittedCorpus,
                        videos: emittedVideos,
                        runs: emittedRuns,
                        workers: emittedWorkers,
                        port: emittedPort,
                        connectTimeout: emittedConnectTimeout,
                        hardware: emittedHardware,
                        preset: emittedPreset,
                        timeout: emittedTimeout,
                        priorUpdatePolicy: emittedPolicy,
                        workerCapsByHost: emittedWorkerCapsByHost,
                        localAffineSampleCollector: { sampledVideos, _, _, frameCountByPath in
                            #expect(sampledVideos.count == videos.count)
                            #expect(frameCountByPath == expectedFrameCountByPath)
                            return sampleLocalAffineSamples(corpus: corpus)
                        }
                    )
                },
                outputLine: { line in
                    outputLines.withLock { $0.append(line) }
                }
            )
            #expect(!FileManager.default.fileExists(atPath: priorPaths.canonicalPath))
            #expect(!FileManager.default.fileExists(atPath: priorPaths.candidatePath))
            return candidateArtifact
        }

        let artifact = try #require(candidateArtifact)
        #expect(!artifact.machines.isEmpty)

        expectOutputLines(
            outputLines.withLock { $0 },
            count: 4,
            containing: [
                ["benchmark prior candidate", "in-memory only"],
                ["Failed prior update:"],
                ["Canonical prior:", "unchanged"],
                ["Recommendation:", "keep current"],
            ]
        )
    }

    @Test("shared search-space gate only blocks brute-force mode")
    func sharedSearchSpaceGateOnlyBlocksBruteForceMode() throws {
        try validateThunderboltBurstSearchSpace(
            maxLocal: 5,
            maxRemoteSlots: [5, 5],
            sweepMode: .optimized(topK: 3),
            limit: 10
        )

        let exactLimit = thunderboltBurstConfigCount(
            maxSlots: [1],
            localSlotsRange: 0...1
        )
        #expect(exactLimit == 3)
        try validateThunderboltBurstSearchSpace(
            maxLocal: 1,
            maxRemoteSlots: [1],
            sweepMode: .bruteForce,
            limit: exactLimit
        )

        do {
            try validateThunderboltBurstSearchSpace(
                maxLocal: 5,
                maxRemoteSlots: [5, 5],
                sweepMode: .bruteForce,
                limit: 10
            )
            Issue.record("Expected brute-force search-space validation to fail")
        } catch let error as ThunderboltBenchmarkJSONError {
            guard case .noBurstConfigs = error else {
                Issue.record("Unexpected error: \(error)")
                return
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("non-JSON burst path uses shared affine local prior builder")
    func nonJSONBurstPathUsesSharedAffineLocalPriorBuilder() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempRoot) }

        let corpus = try makeAffineCorpus(root: tempRoot)
        let videos = corpus.filter { $0.type == .video }
        let workers = [sampleWorker()]
        let expectedFrameCountByPath = defaultBurstSweepFrameCountByPath(videos: videos)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            let (candidateArtifact, outcome) = try await updateThunderboltPriorFromBurstSweep(
                corpus: corpus,
                videos: videos,
                runs: [
                    (
                        ThunderboltBurstConfig(localSlots: 1, remoteSlots: [0]),
                        ThunderboltBurstResult(
                            wallSeconds: 0.7,
                            completed: 1,
                            failed: 0,
                            completionSeconds: [0.7]
                        )
                    )
                ],
                workers: workers,
                port: 7300,
                connectTimeout: 900,
                hardware: sampleHardware(),
                preset: defaultVideoPreset,
                timeout: 120,
                priorUpdatePolicy: .candidateOnly,
                localAffineSampleCollector: { emittedVideos, _, _, frameCountByPath in
                    #expect(emittedVideos.count == videos.count)
                    #expect(frameCountByPath == expectedFrameCountByPath)
                    return sampleLocalAffineSamples(corpus: corpus)
                }
            )

            let machine = try #require(candidateArtifact?.machines.first)
            assertOutcome(outcome, matches: .canonicalWritten(resolveThunderboltCAPriorPaths().canonicalPath))
            #expect(abs(machine.fixedOverheadMS - 300) < 0.001)
            #expect(abs(machine.msPerFrameC1 - 2) < 0.001)
        }
    }

    @Test("JSON burst path keeps prior updates in-memory by default")
    func jsonBurstPathKeepsPriorUpdatesInMemoryByDefault() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempRoot) }

        let corpus = try makeAffineCorpus(root: tempRoot)
        let videos = corpus.filter { $0.type == .video }
        let workers = [sampleWorker()]
        let expectedFrameCountByPath = defaultBurstSweepFrameCountByPath(videos: videos)
        let runs = [
            (
                ThunderboltBurstConfig(localSlots: 1, remoteSlots: [0]),
                ThunderboltBurstResult(
                    wallSeconds: 0.7,
                    completed: 1,
                    failed: 0,
                    completionSeconds: [0.7]
                )
            )
        ]
        let outputLines = Mutex<[String]>([])
        let emitPriorCalled = Mutex(false)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            let priorPaths = resolveThunderboltCAPriorPaths()
            let candidateArtifact = try await emitThunderboltJSONPriorAfterBurstSweepIfNeeded(
                corpus: corpus,
                videos: videos,
                runs: runs,
                workers: workers,
                port: 7300,
                connectTimeout: 900,
                hardware: sampleHardware(),
                preset: defaultVideoPreset,
                timeout: 120,
                emitPrior: {
                    emittedCorpus,
                    emittedVideos,
                    emittedRuns,
                    emittedWorkers,
                    emittedPort,
                    emittedConnectTimeout,
                    emittedHardware,
                    emittedPreset,
                    emittedTimeout,
                    emittedPolicy,
                    emittedWorkerCapsByHost in
                    emitPriorCalled.withLock { $0 = true }
                    #expect(emittedPolicy == .off)
                    return try await updateThunderboltPriorFromBurstSweep(
                        corpus: emittedCorpus,
                        videos: emittedVideos,
                        runs: emittedRuns,
                        workers: emittedWorkers,
                        port: emittedPort,
                        connectTimeout: emittedConnectTimeout,
                        hardware: emittedHardware,
                        preset: emittedPreset,
                        timeout: emittedTimeout,
                        priorUpdatePolicy: emittedPolicy,
                        workerCapsByHost: emittedWorkerCapsByHost,
                        localAffineSampleCollector: { sampledVideos, _, _, frameCountByPath in
                            #expect(sampledVideos.count == videos.count)
                            #expect(frameCountByPath == expectedFrameCountByPath)
                            return sampleLocalAffineSamples(corpus: corpus)
                        }
                    )
                },
                outputLine: { line in
                    outputLines.withLock { $0.append(line) }
                }
            )

            let artifact = try #require(candidateArtifact)
            #expect(!artifact.machines.isEmpty)
            #expect(emitPriorCalled.withLock { $0 })
            #expect(!FileManager.default.fileExists(atPath: priorPaths.canonicalPath))
            #expect(!FileManager.default.fileExists(atPath: priorPaths.candidatePath))
        }

        expectOutputLines(
            outputLines.withLock { $0 },
            count: 4,
            containing: [
                ["benchmark prior candidate", "in-memory only"],
                ["prior update", "policy off"],
                ["Canonical prior:", "unchanged"],
                ["Recommendation:", "keep current"],
            ]
        )
    }

    @Test("non-JSON thunderbolt entry throws when all configured workers are excluded")
    func nonJSONThunderboltEntryThrowsWhenAllConfiguredWorkersAreExcluded() async throws {
        let connectivityCalled = Mutex(false)

        do {
            try await TestEnvironment.withEnvironment([
                "TB_WORKERS": "worker-a:2",
                "TB_PORT": "7300",
            ]) {
                try await benchmarkThunderbolt(
                    corpus: sampleCorpus(),
                    hardware: sampleHardware(),
                    includeShowdown: false,
                    resolveBoundWorkers: { workers in
                        (
                            [],
                            [
                                ThunderboltWorkerBindingIssue(
                                    worker: workers[0],
                                    reason: "no local bridge route to 10.0.0.2"
                                )
                            ]
                        )
                    },
                    benchmarkConnectivity: { _, _, _ in
                        connectivityCalled.withLock { $0 = true }
                        return []
                    }
                )
            }
            Issue.record("Expected worker binding issues to be surfaced")
        } catch let error as ThunderboltBenchmarkJSONError {
            guard case .workerBindingIssues(let issues) = error else {
                Issue.record("Unexpected error: \(error)")
                return
            }
            #expect(issues.count == 1)
            #expect(issues.first?.worker.host == "worker-a")
            #expect(issues.first?.reason == "no local bridge route to 10.0.0.2")
        }

        #expect(!connectivityCalled.withLock { $0 })
    }

    @Test("shared worker selection keeps only reachable configured workers")
    func sharedWorkerSelectionKeepsOnlyReachableConfiguredWorkers() {
        let configuredWorkers = [
            ThunderboltWorkerSpec(host: "worker-a", slots: 2),
            ThunderboltWorkerSpec(host: "worker-b", slots: 3),
            ThunderboltWorkerSpec(host: "worker-c", slots: 4),
            ThunderboltWorkerSpec(host: "worker-d", slots: 1),
        ]
        let boundWorkers = [
            ThunderboltBoundWorkerSpec(
                host: "worker-a",
                connectHost: "10.0.0.2",
                slots: 2,
                sourceIP: "10.0.0.1",
                bridgeName: "bridge0"
            ),
            ThunderboltBoundWorkerSpec(
                host: "worker-c",
                connectHost: "10.0.0.4",
                slots: 4,
                sourceIP: "10.0.0.1",
                bridgeName: "bridge0"
            ),
            ThunderboltBoundWorkerSpec(
                host: "worker-d",
                connectHost: "10.0.0.5",
                slots: 1,
                sourceIP: "10.0.0.1",
                bridgeName: "bridge0"
            ),
        ]
        let bindingIssues = [
            ThunderboltWorkerBindingIssue(
                worker: configuredWorkers[1],
                reason: "no local bridge route to 10.0.0.3"
            )
        ]
        let connectivity = [
            ThunderboltConnectivityResult(worker: boundWorkers[0], reachable: true, connectMillis: 1),
            ThunderboltConnectivityResult(worker: boundWorkers[1], reachable: false, connectMillis: 2),
            ThunderboltConnectivityResult(worker: boundWorkers[2], reachable: true, connectMillis: 3),
        ]

        let selection = resolveThunderboltReachableWorkerSelection(
            configuredWorkers: configuredWorkers,
            boundWorkers: boundWorkers,
            bindingIssues: bindingIssues,
            connectivity: connectivity
        )

        #expect(selection.reachableWorkers.map(\.host) == ["worker-a", "worker-d"])
        #expect(selection.reachableConfiguredIndices == [0, 3])
    }

    @Test("shared zero-reachable worker error surfaces no-bridge state")
    func sharedZeroReachableWorkerErrorSurfacesNoBridgeState() {
        let error = thunderboltZeroReachableWorkerError(
            bindingIssues: [],
            connectivity: [],
            port: 7300
        )

        guard case .noBridgeSources = error else {
            Issue.record("Unexpected error: \(error)")
            return
        }
    }

    @Test("shared zero-reachable worker error surfaces binding issues before fallback")
    func sharedZeroReachableWorkerErrorSurfacesBindingIssuesBeforeFallback() {
        let configuredWorker = ThunderboltWorkerSpec(host: "worker-a", slots: 2)
        let error = thunderboltZeroReachableWorkerError(
            bindingIssues: [
                ThunderboltWorkerBindingIssue(
                    worker: configuredWorker,
                    reason: "no local bridge route to 10.0.0.2"
                )
            ],
            connectivity: [],
            port: 7300
        )

        guard case .workerBindingIssues(let issues) = error else {
            Issue.record("Unexpected error: \(error)")
            return
        }
        #expect(issues.count == 1)
        #expect(issues[0].worker == configuredWorker)
    }

    @Test("shared worker selection throws worker unreachable when all bound workers are excluded")
    func sharedWorkerSelectionThrowsWorkerUnreachableWhenAllBoundWorkersAreExcluded() {
        let configuredWorkers = [
            ThunderboltWorkerSpec(host: "worker-a", slots: 2)
        ]
        let boundWorkers = [
            ThunderboltBoundWorkerSpec(
                host: "worker-a",
                connectHost: "10.0.0.2",
                slots: 2,
                sourceIP: "10.0.0.1",
                bridgeName: "bridge0"
            )
        ]
        let connectivity = [
            ThunderboltConnectivityResult(worker: boundWorkers[0], reachable: false, connectMillis: 1)
        ]

        do {
            _ = try requireThunderboltReachableWorkerSelection(
                configuredWorkers: configuredWorkers,
                boundWorkers: boundWorkers,
                bindingIssues: [],
                connectivity: connectivity,
                port: 7300
            )
            Issue.record("Expected workerUnreachable when no reachable workers remain")
        } catch let error as ThunderboltBenchmarkJSONError {
            guard case .workerUnreachable(let host, let port) = error else {
                Issue.record("Unexpected error: \(error)")
                return
            }
            #expect(host == "worker-a")
            #expect(port == 7300)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("shared exhaustive burst config traversal preserves legacy sort order")
    func sharedExhaustiveBurstConfigTraversalPreservesLegacySortOrder() async throws {
        let maxSlots = [2, 1]
        let localSlotsRange = 0...2
        let expected = legacyThunderboltBurstConfigs(
            maxSlots: maxSlots,
            localSlotsRange: localSlotsRange
        )
        var actual: [ThunderboltBurstConfig] = []

        await withEachThunderboltBurstConfig(
            maxSlots: maxSlots,
            localSlotsRange: localSlotsRange
        ) { config in
            actual.append(config)
        }

        #expect(actual == expected)
        #expect(
            actual.count == thunderboltBurstConfigCount(
                maxSlots: maxSlots,
                localSlotsRange: localSlotsRange
            )
        )
    }

    @Test("shared exhaustive burst config traversal keeps baseline first")
    func sharedExhaustiveBurstConfigTraversalKeepsBaselineFirst() async throws {
        let maxSlots = [1, 1]
        let localSlotsRange = 0...2
        let expectedTotal = thunderboltBurstConfigCount(
            maxSlots: maxSlots,
            localSlotsRange: localSlotsRange
        )
        let baseline = ThunderboltBurstConfig(localSlots: 1, remoteSlots: [0, 0])
        let expectedConfigs = [baseline] + legacyThunderboltBurstConfigs(
            maxSlots: maxSlots,
            localSlotsRange: localSlotsRange
        ).filter { $0 != baseline }

        var visitedRuns: [(run: Int, total: Int, config: ThunderboltBurstConfig)] = []
        await withEachThunderboltBurstConfigBaselineFirst(
            maxSlots: maxSlots,
            localSlotsRange: localSlotsRange
        ) { run, total, config in
            visitedRuns.append((run: run, total: total, config: config))
        }

        #expect(visitedRuns.count == expectedTotal)
        #expect(visitedRuns.map { $0.run } == Array(1...expectedTotal))
        #expect(visitedRuns.allSatisfy { $0.total == expectedTotal })
        #expect(visitedRuns.first?.config == baseline)
        #expect(visitedRuns.map { $0.config } == expectedConfigs)
    }

    @Test("shared burst execution lets callers layer output formatting on top")
    func sharedBurstExecutionLetsCallersLayerOutputFormattingOnTop() async throws {
        let videos = sampleCorpus().filter { $0.type == .video }
        let maxLocal = burstSweepLocalSlotsMax(
            videoEncodeEngines: sampleHardware().videoEncodeEngines,
            totalCores: sampleHardware().totalCores
        )
        let worker = ThunderboltBoundWorkerSpec(
            host: "worker-a",
            connectHost: "worker-a",
            slots: 0,
            sourceIP: "10.0.0.2",
            bridgeName: "bridge0"
        )
        let seenHeaders = Mutex<[[String]]>([])
        let seenRows = Mutex<[[String]]>([])

        let execution = try await executeThunderboltBurstSweep(
            videos: videos,
            workers: [worker],
            port: 7300,
            connectTimeout: 900,
            preset: defaultVideoPreset,
            timeout: 120,
            hardware: sampleHardware(),
            sweepMode: .bruteForce,
            configExecutor: { config, workers, emittedVideos, _, _, _, _, onProgress in
                #expect(workers.map(\.host) == [worker.host])
                #expect(emittedVideos.map(\.path) == videos.map(\.path))
                onProgress?(1, 1)
                let wallSeconds = 10.0 - Double(config.localSlots)
                return ThunderboltBurstResult(
                    wallSeconds: wallSeconds,
                    completed: 1,
                    failed: 0,
                    completionSeconds: [wallSeconds]
                )
            },
            headerPrinter: { columns in
                seenHeaders.withLock { $0.append(columns.map(\.header)) }
            },
            rowPrinter: { values, columns, semantics in
                seenRows.withLock { $0.append(values) }
                #expect(columns.map(\.header) == seenHeaders.withLock { $0[0] })
                #expect(semantics.isEmpty)
            }
        )

        let headers = seenHeaders.withLock { $0 }
        let rows = seenRows.withLock { $0 }
        let bestRun = try #require(execution.bestRun)

        #expect(headers.count == 1)
        let header = try #require(headers.first)
        expectBurstSweepHeaderStructure(header, workerCount: 1, includePrediction: false)
        #expect(rows.allSatisfy { $0.count == header.count })
        #expect(rows.count == execution.displayedRuns.count)
        #expect(execution.evaluatedRuns.count == execution.displayedRuns.count)
        #expect(execution.displayedRuns.first?.0 == ThunderboltBurstConfig(localSlots: 1, remoteSlots: [0]))
        #expect(bestRun.0.localSlots == maxLocal)
        #expect(bestRun.1.wallSeconds == 10.0 - Double(maxLocal))
    }
}

private func sampleCorpus() -> [MediaFile] {
    [
        MediaFile(
            path: "/tmp/sample.mov",
            name: "sample.mov",
            type: .video,
            sizeBytes: 1_024
        ),
        MediaFile(
            path: "/tmp/sample.jpg",
            name: "sample.jpg",
            type: .image,
            sizeBytes: 512
        ),
    ]
}

private func sampleWorker() -> ThunderboltBoundWorkerSpec {
    ThunderboltBoundWorkerSpec(
        host: "worker-a",
        connectHost: "worker-a",
        slots: 2,
        sourceIP: "10.0.0.2",
        bridgeName: "bridge0"
    )
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

private func sampleCandidateArtifact() -> BenchmarkPriorArtifact {
    BenchmarkPriorArtifact(
        generatedAt: Date(timeIntervalSince1970: 0),
        corpusHash: "sha256:test",
        corpusSummary: BenchmarkPriorCorpusSummary(videoCount: 1, totalBytes: 1_024),
        machines: [
            BenchmarkPriorMachine(
                signature: "local-signature",
                chipName: "Apple M4",
                performanceCores: 4,
                efficiencyCores: 6,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: defaultVideoPreset,
                msPerFrameC1: 1.0,
                fixedOverheadMS: 300,
                avgCorpusFrameCount: 2_400,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 10.0,
                        msPerVideoP50: 4_000,
                        msPerVideoP95: 5_000,
                        degradationRatio: 1.0
                    )
                ]
            )
        ]
    )
}

private func legacyThunderboltBurstConfigs(
    maxSlots: [Int],
    localSlotsRange: ClosedRange<Int>
) -> [ThunderboltBurstConfig] {
    var configs: [ThunderboltBurstConfig] = []
    var remoteSlots = Array(repeating: 0, count: maxSlots.count)

    func appendRemoteVectors(index: Int, localSlots: Int) {
        if index == maxSlots.count {
            let total = localSlots + remoteSlots.reduce(0, +)
            if total > 0 {
                configs.append(
                    ThunderboltBurstConfig(localSlots: localSlots, remoteSlots: remoteSlots)
                )
            }
            return
        }

        for value in 0...maxSlots[index] {
            remoteSlots[index] = value
            appendRemoteVectors(index: index + 1, localSlots: localSlots)
        }
    }

    for localSlots in localSlotsRange {
        appendRemoteVectors(index: 0, localSlots: localSlots)
    }

    return configs.sorted(by: thunderboltBurstConfigPrecedes)
}

private func thunderboltBurstConfigPrecedes(
    _ lhs: ThunderboltBurstConfig,
    _ rhs: ThunderboltBurstConfig
) -> Bool {
    let lhsTotal = lhs.remoteSlots.reduce(lhs.localSlots, +)
    let rhsTotal = rhs.remoteSlots.reduce(rhs.localSlots, +)
    if lhsTotal != rhsTotal { return lhsTotal < rhsTotal }
    if lhs.localSlots != rhs.localSlots { return lhs.localSlots < rhs.localSlots }
    return lhs.remoteSlots.lexicographicallyPrecedes(rhs.remoteSlots)
}

private func sampleLocalAffineSamples(corpus: [MediaFile]) -> [LocalVideoAffineSample] {
    [
        LocalVideoAffineSample(sourcePath: corpus[0].path, frameCount: 100, processMS: 500),
        LocalVideoAffineSample(sourcePath: corpus[1].path, frameCount: 200, processMS: 700),
        LocalVideoAffineSample(sourcePath: corpus[2].path, frameCount: 400, processMS: 1_100),
    ]
}

private func defaultBurstSweepFrameCountByPath(videos: [MediaFile]) -> [String: Double] {
    let fallbackFrames = CAProfileAndFallbackMath.fallbackFrameCount(durationSeconds: nil, frameCount: nil)
    return Dictionary(uniqueKeysWithValues: videos.map { video in
        (video.path, fallbackFrames)
    })
}

private func makeAffineCorpus(root: URL) throws -> [MediaFile] {
    let names = ["a.mov", "b.mov", "c.mov"]
    return try names.enumerated().map { index, name in
        let url = root.appendingPathComponent(name)
        let data = Data("burst-\(index)".utf8)
        try data.write(to: url, options: .atomic)
        return MediaFile(
            path: url.path,
            name: name,
            type: .video,
            sizeBytes: data.count
        )
    }
}

private func makeTempDirectory() -> URL {
    let url = FileManager.default.temporaryDirectory
        .appendingPathComponent("tb-nonjson-prior-\(UUID().uuidString)")
    try! FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

private func expectOutputLines(
    _ lines: [String],
    count expectedCount: Int,
    containing expectedFragments: [[String]]
) {
    #expect(lines.count == expectedCount)
    for fragments in expectedFragments {
        #expect(lines.contains { line in
            fragments.allSatisfy { line.contains($0) }
        })
    }
}

private func expectBurstSweepHeaderStructure(
    _ header: [String],
    workerCount: Int,
    includePrediction: Bool
) {
    let expectedCount = 5 + workerCount + (includePrediction ? 1 : 0)
    let workerHeaders = header.filter { candidate in
        guard candidate.first == "W" else { return false }
        let suffix = candidate.dropFirst()
        return !suffix.isEmpty && suffix.allSatisfy(\.isNumber)
    }
    #expect(header.count == expectedCount)
    #expect(header.contains("Run"))
    #expect(header.contains("Local"))
    #expect(workerHeaders.count == workerCount)
    #expect(header.contains { $0.localizedCaseInsensitiveContains("video") })
    #expect(header.contains { $0.localizedCaseInsensitiveContains("wall") })
    #expect(header.contains { $0.localizedCaseInsensitiveContains("fail") })
    #expect(header.contains("Prediction") == includePrediction)
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
