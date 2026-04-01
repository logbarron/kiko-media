import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Benchmark prior")
struct BenchmarkPriorTests {
    @Test("worker signature includes os major.minor")
    func signatureIncludesNormalizedOS() {
        let signature = WorkerSignatureBuilder.make(
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            preset: "AVAssetExportPreset1920x1080",
            osVersion: OperatingSystemVersion(majorVersion: 26, minorVersion: 3, patchVersion: 1)
        )

        #expect(signature == "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=AVAssetExportPreset1920x1080")
    }

    @Test("prior lookup is exact and does not fuzzy-match signatures")
    func lookupExactMatchOnly() {
        let machine = BenchmarkPriorMachine(
            signature: "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=p",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.3",
            transcodePreset: "p",
            msPerFrameC1: 8.75,
            avgCorpusFrameCount: 1_440,
            cells: [
                .init(concurrency: 2, videosPerMin: 9.5, msPerVideoP50: 12_600, msPerVideoP95: 15_800, degradationRatio: 1.17)
            ]
        )
        let artifact = BenchmarkPriorArtifact(
            version: 2,
            generatedAt: Date(timeIntervalSince1970: 0),
            corpusHash: "sha256:abc",
            corpusSummary: .init(videoCount: 77, totalBytes: 123),
            machines: [machine]
        )

        let table = BenchmarkPriorTable(artifact: artifact)
        let exact = table.lookup(signature: machine.signature, concurrency: 2)
        #expect(exact?.videosPerMin == 9.5)

        let mismatch = table.lookup(signature: machine.signature.replacingOccurrences(of: "26.3", with: "26.4"), concurrency: 2)
        #expect(mismatch == nil)
    }

    @Test("prior table keeps existing machines when merging a new remote signature")
    func mergeRemoteCells() {
        let local = BenchmarkPriorMachine(
            signature: "local",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.3",
            transcodePreset: "p",
            msPerFrameC1: 10.0,
            avgCorpusFrameCount: 1_440,
            cells: [.init(concurrency: 1, videosPerMin: 5.0, msPerVideoP50: 11_000, msPerVideoP95: 14_000, degradationRatio: 1.0)]
        )
        let remote = BenchmarkPriorMachine(
            signature: "remote",
            chipName: "Apple M4 Max",
            performanceCores: 12,
            efficiencyCores: 4,
            videoEncodeEngines: 2,
            osVersion: "26.3",
            transcodePreset: "p",
            msPerFrameC1: 6.5,
            avgCorpusFrameCount: 1_440,
            cells: [.init(concurrency: 3, videosPerMin: 20.0, msPerVideoP50: 9_000, msPerVideoP95: 11_000, degradationRatio: 1.2)]
        )

        var table = BenchmarkPriorTable(machines: [local])
        table.merge(remoteMachine: remote)

        #expect(table.lookup(signature: "local", concurrency: 1)?.videosPerMin == 5.0)
        #expect(table.lookup(signature: "remote", concurrency: 3)?.videosPerMin == 20.0)
    }

    @Test("dispatcher can load persisted prior cells by worker signature")
    func dispatcherLoadsPersistedPriorBySignature() {
        let signature = "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=p"
        let machine = BenchmarkPriorMachine(
            signature: signature,
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.3",
            transcodePreset: "p",
            msPerFrameC1: 8.75,
            avgCorpusFrameCount: 1_440,
            cells: [
                .init(concurrency: 1, videosPerMin: 6.0, msPerVideoP50: 12_000, msPerVideoP95: 14_000, degradationRatio: 1.0),
                .init(concurrency: 2, videosPerMin: 9.0, msPerVideoP50: 9_500, msPerVideoP95: 11_800, degradationRatio: 1.2)
            ]
        )
        let table = BenchmarkPriorTable(machines: [machine])

        let cells = ThunderboltDispatcher.priorCells(forSignature: signature, table: table)
        #expect(cells.count == 2)
        #expect(cells.map(\.concurrency) == [1, 2])
        #expect(cells.map(\.msPerVideoP50) == [12_000, 9_500])
    }

    @Test("hardware-compatible lookup prefers the closest OS match for the same hardware preset")
    func hardwareCompatibleLookupPrefersTheClosestOSMatchForTheSameHardwarePreset() throws {
        let older = BenchmarkPriorMachine(
            signature: "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=p",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.3",
            transcodePreset: "p",
            msPerFrameC1: 8.75,
            avgCorpusFrameCount: 1_440,
            cells: [
                .init(concurrency: 1, videosPerMin: 6.0, msPerVideoP50: 12_000, msPerVideoP95: 14_000, degradationRatio: 1.0),
            ]
        )
        let newer = BenchmarkPriorMachine(
            signature: "chip=Apple M4;ecores=6;encoders=1;os=26.6;pcores=4;preset=p",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.6",
            transcodePreset: "p",
            msPerFrameC1: 8.50,
            avgCorpusFrameCount: 1_440,
            cells: [
                .init(concurrency: 1, videosPerMin: 6.2, msPerVideoP50: 11_800, msPerVideoP95: 13_900, degradationRatio: 1.0),
            ]
        )
        let table = BenchmarkPriorTable(machines: [older, newer])

        let match = try #require(
            table.hardwareCompatibleMachine(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.5;pcores=4;preset=p"
            )
        )
        #expect(match.signature == newer.signature)
    }

    @Test("hardware-compatible lookup still works after loading explicit affine artifact shape")
    func hardwareCompatibleLookupStillWorksAfterLoadingExplicitAffineArtifactShape() throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("benchmark-prior-compatible-explicit-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let artifact = BenchmarkPriorArtifact(
            generatedAt: Date(timeIntervalSince1970: 0),
            corpusHash: "sha256:compatible",
            corpusSummary: .init(videoCount: 1, totalBytes: 1_024),
            machines: [
                BenchmarkPriorMachine(
                    signature: "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=p",
                    chipName: "Apple M4",
                    performanceCores: 4,
                    efficiencyCores: 6,
                    videoEncodeEngines: 1,
                    osVersion: "26.3",
                    transcodePreset: "p",
                    msPerFrameC1: 0.9,
                    fixedOverheadMS: 120,
                    avgCorpusFrameCount: 500,
                    affineModelSource: .explicit,
                    cells: [
                        .init(concurrency: 1, videosPerMin: 10, msPerVideoP50: 800, msPerVideoP95: 900, degradationRatio: 1.0),
                    ]
                ),
            ]
        )
        try artifact.write(toPath: tempURL.path)

        let loaded = try #require(BenchmarkPriorArtifact.load(fromPath: tempURL.path))
        let table = BenchmarkPriorTable(artifact: loaded)
        let match = try #require(
            table.hardwareCompatibleMachine(
                signature: "chip=Apple M4;ecores=6;encoders=1;os=26.4;pcores=4;preset=p"
            )
        )
        #expect(match.affineModelSource == .explicit)
        #expect(match.signature == "chip=Apple M4;ecores=6;encoders=1;os=26.3;pcores=4;preset=p")
    }

    @Test("activation gate accepts a hardware-compatible local prior when only OS version drifted")
    func activationGateAcceptsAHardwareCompatibleLocalPriorWhenOnlyOSVersionDrifted() throws {
        let caps = WorkerCaps.detectLocal()
        let chipName = try #require(caps.chipName)
        let performanceCores = try #require(caps.performanceCores)
        let efficiencyCores = try #require(caps.efficiencyCores)
        let videoEncodeEngines = try #require(caps.videoEncodeEngines)
        let currentOS = try #require(caps.osVersion)
        let currentVersion = WorkerSignatureComponents.parse(
            signature: WorkerSignatureBuilder.make(
                chipName: chipName,
                performanceCores: performanceCores,
                efficiencyCores: efficiencyCores,
                videoEncodeEngines: videoEncodeEngines,
                preset: defaultVideoPreset,
                osVersion: currentOS
            )
        )
        let driftedOS = "\(currentVersion?.osVersion.split(separator: ".").first.flatMap { Int($0) } ?? 0).\(max(0, (currentVersion?.osVersion.split(separator: ".").dropFirst().first.flatMap { Int($0) } ?? 0) + 1))"
        let driftedSignature = try #require(
            WorkerSignatureBuilder.make(
                chipName: chipName,
                performanceCores: performanceCores,
                efficiencyCores: efficiencyCores,
                videoEncodeEngines: videoEncodeEngines,
                preset: defaultVideoPreset,
                osVersion: driftedOS
            )
        )
        let priorMachine = BenchmarkPriorMachine(
            signature: driftedSignature,
            chipName: chipName,
            performanceCores: performanceCores,
            efficiencyCores: efficiencyCores,
            videoEncodeEngines: videoEncodeEngines,
            osVersion: driftedOS,
            transcodePreset: defaultVideoPreset,
            msPerFrameC1: 2.0,
            avgCorpusFrameCount: 1_440,
            cells: [
                .init(concurrency: 1, videosPerMin: 6.0, msPerVideoP50: 3_500, msPerVideoP95: 4_200, degradationRatio: 1.0),
            ]
        )

        let profile = CAActivationGate.resolveLocalPriorProfile(
            priorTable: BenchmarkPriorTable(machines: [priorMachine]),
            videoTranscodePreset: defaultVideoPreset
        )
        #expect(profile != nil)
        #expect(profile?.degradationCurve.isEmpty == false)
    }

    @Test("explicit affine prior fields persist and load with explicit source")
    func explicitAffinePriorFieldsPersistAndLoadWithExplicitSource() throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("benchmark-prior-explicit-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let machine = BenchmarkPriorMachine(
            signature: "sig-explicit",
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            videoEncodeEngines: 1,
            osVersion: "26.3",
            transcodePreset: "p",
            msPerFrameC1: 1.0,
            fixedOverheadMS: 0,
            avgCorpusFrameCount: 400,
            affineModelSource: .explicit,
            cells: [
                .init(concurrency: 1, videosPerMin: 10, msPerVideoP50: 400, msPerVideoP95: 450, degradationRatio: 1.0),
            ]
        )
        let artifact = BenchmarkPriorArtifact(
            generatedAt: Date(timeIntervalSince1970: 0),
            corpusHash: "sha256:explicit",
            corpusSummary: .init(videoCount: 1, totalBytes: 1_024),
            machines: [machine]
        )

        try artifact.write(toPath: tempURL.path)

        let loadedArtifact = try #require(BenchmarkPriorArtifact.load(fromPath: tempURL.path))
        let loadedMachine = try #require(loadedArtifact.machines.first)
        #expect(loadedMachine.msPerFrameC1 == 1.0)
        #expect(loadedMachine.fixedOverheadMS == 0)
        #expect(loadedMachine.avgCorpusFrameCount == 400)
        #expect(loadedMachine.affineModelSource == .explicit)

        let json = try String(contentsOf: tempURL, encoding: .utf8)
        #expect(json.contains("\"affine_model_source\""))
    }

    @Test("legacy v2 prior payload missing fixed overhead defaults to zero")
    func legacyPriorPayloadMissingFixedOverheadDefaultsToZero() throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("benchmark-prior-legacy-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let json = """
        {
          "version": 2,
          "generated_at": "2026-03-06T00:00:00Z",
          "corpus_hash": "sha256:test",
          "corpus_summary": {
            "video_count": 3,
            "total_bytes": 12345
          },
          "machines": [
            {
              "signature": "sig-legacy",
              "chip_name": "Apple M4",
              "performance_cores": 4,
              "efficiency_cores": 6,
              "video_encode_engines": 1,
              "os_version": "26.3",
              "transcode_preset": "p",
              "ms_per_frame_c1": 8.75,
              "avg_corpus_frame_count": 1440,
              "cells": [
                {
                  "concurrency": 1,
                  "videos_per_min": 6.0,
                  "ms_per_video_p50": 12000,
                  "ms_per_video_p95": 14000,
                  "degradation_ratio": 1.0
                }
              ]
            }
          ]
        }
        """
        try Data(json.utf8).write(to: tempURL, options: .atomic)

        let artifact = try #require(BenchmarkPriorArtifact.load(fromPath: tempURL.path))
        let machine = try #require(artifact.machines.first)
        #expect(machine.fixedOverheadMS == 0)
        #expect(machine.affineModelSource == .legacyHeuristic)
    }

    @Test("legacy v2 prior payload with fixed overhead but no affine source stays legacy")
    func legacyPriorPayloadWithFixedOverheadButNoAffineSourceStaysLegacy() throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("benchmark-prior-legacy-fixed-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let json = """
        {
          "version": 2,
          "generated_at": "2026-03-06T00:00:00Z",
          "corpus_hash": "sha256:test",
          "corpus_summary": {
            "video_count": 3,
            "total_bytes": 12345
          },
          "machines": [
            {
              "signature": "sig-legacy-fixed",
              "chip_name": "Apple M4",
              "performance_cores": 4,
              "efficiency_cores": 6,
              "video_encode_engines": 1,
              "os_version": "26.3",
              "transcode_preset": "p",
              "ms_per_frame_c1": 0.9,
              "fixed_overhead_ms": 250,
              "avg_corpus_frame_count": 1440,
              "cells": [
                {
                  "concurrency": 1,
                  "videos_per_min": 6.0,
                  "ms_per_video_p50": 12000,
                  "ms_per_video_p95": 14000,
                  "degradation_ratio": 1.0
                }
              ]
            }
          ]
        }
        """
        try Data(json.utf8).write(to: tempURL, options: .atomic)

        let artifact = try #require(BenchmarkPriorArtifact.load(fromPath: tempURL.path))
        let machine = try #require(artifact.machines.first)
        #expect(machine.fixedOverheadMS == 250)
        #expect(machine.affineModelSource == .legacyHeuristic)
        #expect(CAProfileAndFallbackMath.requiresRemoteAffineMigration(machine))
    }

    @Test("prior loader rejects unsupported version envelope")
    func priorLoaderRejectsUnsupportedVersionEnvelope() throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("benchmark-prior-unsupported-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        try Data(#"{"version":999}"#.utf8).write(to: tempURL, options: .atomic)

        #expect(BenchmarkPriorArtifact.loadResult(fromPath: tempURL.path) == .unsupportedVersion(999))
        #expect(BenchmarkPriorArtifact.load(fromPath: tempURL.path) == nil)
    }

    @Test("prior loader distinguishes missing and invalid artifacts")
    func priorLoaderDistinguishesMissingAndInvalidArtifacts() throws {
        let missingPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("benchmark-prior-missing-\(UUID().uuidString).json")
            .path
        #expect(BenchmarkPriorArtifact.loadResult(fromPath: missingPath) == .missing)

        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("benchmark-prior-invalid-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        try Data(#"{"version":"oops"}"#.utf8).write(to: tempURL, options: .atomic)

        #expect(BenchmarkPriorArtifact.loadResult(fromPath: tempURL.path) == .invalid)
        #expect(BenchmarkPriorArtifact.load(fromPath: tempURL.path) == nil)
    }

    @Test("local prior artifact emits explicit fixed overhead from affine telemetry")
    func localPriorArtifactEmitsExplicitFixedOverheadFromAffineTelemetry() throws {
        let tempRoot = makeTempDirectory(prefix: "benchmark-prior-artifact")
        defer { try? FileManager.default.removeItem(at: tempRoot) }

        let corpus = try makeVideoCorpus(root: tempRoot)
        let builtArtifact = try buildBenchmarkPriorArtifact(
            corpus: corpus,
            videoSweep: sampleVideoSweep(c1P50Seconds: 0.7),
            corpusFrameCounts: [100, 200, 400],
            localAffineSamples: sampleLocalAffineSamples(corpus: corpus),
            hardware: sampleHardware(),
            preset: "AVAssetExportPreset1920x1080"
        )
        let artifact = try #require(builtArtifact)
        let machine = try #require(artifact.machines.first)
        #expect(machine.affineModelSource == .explicit)
        #expect(abs(machine.fixedOverheadMS - 300) < 0.001)
        #expect(abs(machine.msPerFrameC1 - 2) < 0.001)

        let outputPath = tempRoot.appendingPathComponent("prior.json").path
        try artifact.write(toPath: outputPath)
        let json = try String(contentsOfFile: outputPath, encoding: .utf8)
        #expect(json.contains("\"fixed_overhead_ms\""))
    }
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

private func sampleVideoSweep(c1P50Seconds: Double) -> [ConcurrencySweepPoint] {
    [
        ConcurrencySweepPoint(
            concurrency: 1,
            throughputPerMinute: 60_000.0 / (c1P50Seconds * 1_000.0),
            p50Seconds: c1P50Seconds,
            p95Seconds: c1P50Seconds + 0.2,
            peakMemoryMB: 128
        ),
        ConcurrencySweepPoint(
            concurrency: 2,
            throughputPerMinute: 12.0,
            p50Seconds: c1P50Seconds * 1.3,
            p95Seconds: c1P50Seconds * 1.5,
            peakMemoryMB: 160
        ),
    ]
}

private func sampleLocalAffineSamples(corpus: [MediaFile]) -> [LocalVideoAffineSample] {
    [
        LocalVideoAffineSample(sourcePath: corpus[0].path, frameCount: 100, processMS: 500),
        LocalVideoAffineSample(sourcePath: corpus[1].path, frameCount: 200, processMS: 700),
        LocalVideoAffineSample(sourcePath: corpus[2].path, frameCount: 400, processMS: 1_100),
    ]
}

private func makeVideoCorpus(root: URL) throws -> [MediaFile] {
    let names = ["a.mov", "b.mov", "c.mov"]
    return try names.enumerated().map { index, name in
        let url = root.appendingPathComponent(name)
        let data = Data("video-\(index)".utf8)
        try data.write(to: url, options: .atomic)
        return MediaFile(
            path: url.path,
            name: name,
            type: .video,
            sizeBytes: data.count
        )
    }
}

private func makeTempDirectory(prefix: String) -> URL {
    let url = FileManager.default.temporaryDirectory
        .appendingPathComponent("\(prefix)-\(UUID().uuidString)")
    try! FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}
