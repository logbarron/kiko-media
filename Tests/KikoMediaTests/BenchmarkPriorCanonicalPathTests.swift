import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Benchmark prior canonical path")
struct BenchmarkPriorCanonicalPathTests {
    @Test("default emitter writes canonical prior path under BASE_DIRECTORY")
    func defaultEmitterWritesCanonicalPath() async throws {
        let fm = FileManager.default
        let tempRoot = fm.temporaryDirectory.appendingPathComponent("prior-canonical-\(UUID().uuidString)")
        let baseDirectory = tempRoot.appendingPathComponent("base")
        let videoURL = tempRoot.appendingPathComponent("sample.mov")

        try fm.createDirectory(at: tempRoot, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: tempRoot) }

        let videoData = Data("test-video".utf8)
        try videoData.write(to: videoURL)

        let expectedPath = baseDirectory.appendingPathComponent("benchmark-prior.json").path

        let emittedPath = try await TestEnvironment.withEnvironment(
            ["BASE_DIRECTORY": baseDirectory.path]
        ) {
            try emitBenchmarkPriorArtifact(
                corpus: [
                    MediaFile(
                        path: videoURL.path,
                        name: videoURL.lastPathComponent,
                        type: .video,
                        sizeBytes: videoData.count
                    )
                ],
                videoSweep: [
                    ConcurrencySweepPoint(
                        concurrency: 1,
                        throughputPerMinute: 12.0,
                        p50Seconds: 4.0,
                        p95Seconds: 5.0,
                        peakMemoryMB: 128
                    )
                ],
                corpusFrameCounts: [2_400],
                hardware: HardwareProfile(
                    chipName: "Apple M4",
                    performanceCores: 4,
                    efficiencyCores: 6,
                    totalCores: 10,
                    memoryGB: 16,
                    videoEncodeEngines: 1,
                    hwEncoderNames: []
                ),
                preset: defaultVideoPreset
            )
        }

        #expect(emittedPath == expectedPath)
        #expect(fm.fileExists(atPath: expectedPath))
        #expect(BenchmarkPriorArtifact.load(fromPath: expectedPath) != nil)
    }

    @Test("canonical path uses Config BASE_DIRECTORY fallback when env is unset")
    func canonicalPathUsesConfigFallbackWhenEnvUnset() async throws {
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": nil]) {
            let resolved = resolveCanonicalBenchmarkPriorPath()
            let expectedBaseDirectory = NSString(string: Config.envString("BASE_DIRECTORY")).expandingTildeInPath
            let expectedPath = URL(fileURLWithPath: expectedBaseDirectory)
                .appendingPathComponent("benchmark-prior.json")
                .path
            #expect(resolved == expectedPath)
        }
    }

    @Test("CA prior loader reads canonical path when canonical file exists")
    func caPriorLoaderReadsCanonicalPathWhenPresent() async throws {
        let fm = FileManager.default
        let tempRoot = fm.temporaryDirectory.appendingPathComponent("ca-prior-canonical-only-\(UUID().uuidString)")
        let baseDirectory = tempRoot.appendingPathComponent("base")
        defer { try? fm.removeItem(at: tempRoot) }
        try fm.createDirectory(at: baseDirectory, withIntermediateDirectories: true)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory.path]) {
            let canonicalPath = resolveCanonicalBenchmarkPriorPath()
            try self.writePriorArtifact(
                atPath: canonicalPath,
                signature: "canonical-machine",
                msPerVideoP50: 1_111
            )

            let table = loadCAPriorTable()
            #expect(table.lookup(signature: "canonical-machine", concurrency: 1)?.msPerVideoP50 == 1_111)
        }
    }

    @Test("CA run setup prefers in-memory prior override over canonical file")
    func caRunSetupPrefersInMemoryPriorOverrideOverCanonicalFile() async throws {
        let fm = FileManager.default
        let tempRoot = fm.temporaryDirectory.appendingPathComponent("ca-prior-override-\(UUID().uuidString)")
        let baseDirectory = tempRoot.appendingPathComponent("base")
        let videoURL = tempRoot.appendingPathComponent("sample.mov")
        defer { try? fm.removeItem(at: tempRoot) }

        try fm.createDirectory(at: baseDirectory, withIntermediateDirectories: true)
        try Data("test-video".utf8).write(to: videoURL)

        let hardware = HardwareProfile(
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            totalCores: 10,
            memoryGB: 16,
            videoEncodeEngines: 1,
            hwEncoderNames: []
        )
        let localSignature = WorkerSignatureBuilder.make(
            chipName: hardware.chipName,
            performanceCores: hardware.performanceCores,
            efficiencyCores: hardware.efficiencyCores,
            videoEncodeEngines: hardware.videoEncodeEngines,
            preset: defaultVideoPreset,
            osVersion: ProcessInfo.processInfo.operatingSystemVersion
        )

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory.path, "TB_WORKERS": nil]) {
            let canonicalPath = resolveCanonicalBenchmarkPriorPath()
            try self.writePriorArtifact(
                atPath: canonicalPath,
                signature: localSignature,
                msPerVideoP50: 1_111
            )

            let overrideMachine = BenchmarkPriorMachine(
                signature: localSignature,
                chipName: "Apple M4",
                performanceCores: 4,
                efficiencyCores: 6,
                videoEncodeEngines: 1,
                osVersion: "26.0",
                transcodePreset: defaultVideoPreset,
                msPerFrameC1: 0.25,
                avgCorpusFrameCount: 2_400,
                cells: [
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 24,
                        msPerVideoP50: 777,
                        msPerVideoP95: 900,
                        degradationRatio: 1.0
                    )
                ]
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
                corpus: [
                    MediaFile(
                        path: videoURL.path,
                        name: videoURL.lastPathComponent,
                        type: .video,
                        sizeBytes: 1_024
                    )
                ],
                preset: defaultVideoPreset,
                hardware: hardware,
                slotOverrides: nil,
                mode: .auto,
                priorTableOverride: BenchmarkPriorTable(machines: [overrideMachine]),
                settingsOverride: settings
            )

            #expect(setup.localMSPerFrameC1 == 0.25)
            #expect(setup.priorTable.lookup(signature: localSignature, concurrency: 1)?.msPerVideoP50 == 777)
        }
    }

    @Test("CA prior loader returns empty table when canonical file is absent")
    func caPriorLoaderReturnsEmptyTableWhenCanonicalFileIsAbsent() async throws {
        let fm = FileManager.default
        let tempRoot = fm.temporaryDirectory.appendingPathComponent("ca-prior-missing-canonical-\(UUID().uuidString)")
        let baseDirectory = tempRoot.appendingPathComponent("base")
        defer { try? fm.removeItem(at: tempRoot) }

        try fm.createDirectory(at: baseDirectory, withIntermediateDirectories: true)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory.path]) {
            let canonicalPath = resolveCanonicalBenchmarkPriorPath()
            #expect(!FileManager.default.fileExists(atPath: canonicalPath))

            let table = loadCAPriorTable()
            #expect(table.machines.isEmpty)
        }
    }

    @Test("benchmark canonical path uses shared Config benchmark-prior resolver")
    func canonicalPathUsesSharedConfigResolver() async throws {
        let baseDirectory = "~/tmp/benchmark-prior-shared-\(UUID().uuidString)"
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": baseDirectory]) {
            let resolved = resolveCanonicalBenchmarkPriorPath()
            let expected = Config.benchmarkPriorPath(
                baseDirectoryPath: NSString(string: baseDirectory).expandingTildeInPath
            )
            #expect(resolved == expected)
        }
    }

    @Test("canonical path uses Config BASE_DIRECTORY fallback when env is whitespace")
    func canonicalPathUsesConfigFallbackWhenBaseDirectoryWhitespace() async throws {
        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": "   "]) {
            let expectedBaseDirectory = NSString(
                string: Config.stringDefaults["BASE_DIRECTORY"] ?? ""
            ).expandingTildeInPath
            let expected = Config.benchmarkPriorPath(baseDirectoryPath: expectedBaseDirectory)
            #expect(resolveCanonicalBenchmarkPriorPath() == expected)
        }
    }

    @Test("showdown canonical path tracks benchmark emitter canonical path")
    func showdownPathMatchesEmitterCanonicalPath() async throws {
        let fm = FileManager.default
        let tempRoot = fm.temporaryDirectory.appendingPathComponent("prior-path-match-\(UUID().uuidString)")
        defer { try? fm.removeItem(at: tempRoot) }
        try fm.createDirectory(at: tempRoot, withIntermediateDirectories: true)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            let emitterPath = resolveCanonicalBenchmarkPriorPath()
            let showdownPaths = resolveThunderboltCAPriorPaths()
            #expect(showdownPaths.canonicalPath == emitterPath)
            #expect(showdownPaths.candidatePath.hasSuffix("/benchmark-prior.candidate.json"))
        }
    }

    private func writePriorArtifact(
        atPath path: String,
        signature: String,
        msPerVideoP50: Int
    ) throws {
        let machine = BenchmarkPriorMachine(
            signature: signature,
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.0",
            transcodePreset: "test-preset",
            msPerFrameC1: 1.0,
            avgCorpusFrameCount: 1_440,
            cells: [
                BenchmarkPriorCell(
                    concurrency: 1,
                    videosPerMin: 12,
                    msPerVideoP50: msPerVideoP50,
                    msPerVideoP95: msPerVideoP50 + 100,
                    degradationRatio: 1.0
                )
            ]
        )
        let artifact = BenchmarkPriorArtifact(
            generatedAt: Date(timeIntervalSince1970: 0),
            corpusHash: "sha256:test",
            corpusSummary: BenchmarkPriorCorpusSummary(videoCount: 1, totalBytes: 1),
            machines: [machine]
        )
        try artifact.write(toPath: path)
    }
}
