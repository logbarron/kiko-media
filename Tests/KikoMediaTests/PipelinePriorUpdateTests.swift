import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Pipeline prior update", Testing.ParallelizationTrait.serialized)
struct PipelinePriorUpdateTests {
    @Test("pipeline update disabled reports policy off truthfully")
    func pipelineUpdateDisabledReportsPolicyOffTruthfully() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: tempRoot.path) }
        let videoURL = tempRoot.appendingPathComponent("sample.mov")
        try Data("test-video".utf8).write(to: videoURL)

        let result = try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            try updatePipelineBenchmarkPriorFromRun(
                corpus: sampleCorpus(videoURL: videoURL),
                videoSweep: sampleVideoSweep(),
                corpusFrameCounts: [2_400],
                hardware: sampleHardware(),
                preset: defaultVideoPreset,
                shouldAttemptUpdate: false
            )
        }

        #expect(result.0 == nil)
        assertOutcome(result.1, matches: .skippedPolicyOff)
        assertEmitsOperatorOutput(renderPriorUpdateLines(candidateArtifact: result.0, outcome: result.1))
    }

    @Test("pipeline update enabled promotes only when guards allow")
    func pipelineUpdateEnabledPromotesOnlyWhenGuardsAllow() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: tempRoot.path) }
        let videoURL = tempRoot.appendingPathComponent("sample.mov")
        let videoData = Data("test-video".utf8)
        try videoData.write(to: videoURL)

        let hardware = sampleHardware()
        let localSignature = makeLocalSignature(hardware: hardware)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            let canonicalPath = resolveThunderboltCAPriorPaths().canonicalPath
            try makePriorArtifact(
                signature: localSignature,
                corpusSummary: BenchmarkPriorCorpusSummary(videoCount: 1, totalBytes: 1_024),
                msPerFrameC1: 2.0,
                msPerVideoP50: 6_000
            ).write(toPath: canonicalPath)

            let (candidateArtifact, outcome) = try updatePipelineBenchmarkPriorFromRun(
                corpus: sampleCorpus(videoURL: videoURL),
                videoSweep: sampleVideoSweep(),
                corpusFrameCounts: [2_400],
                hardware: hardware,
                preset: defaultVideoPreset,
                shouldAttemptUpdate: true
            )

            assertOutcome(outcome, matches: .promoted(resolveThunderboltCAPriorPaths().canonicalPath))
            #expect(candidateArtifact != nil)
            #expect(BenchmarkPriorArtifact.load(fromPath: canonicalPath)?.machines.first(where: { $0.signature == localSignature })?.msPerFrameC1 == 1.6666666666666667)
            #expect(!FileManager.default.fileExists(atPath: resolveThunderboltCAPriorPaths().candidatePath))
        }
    }

    @Test("existing matching or stronger canonical causes a truthful skipped write")
    func strongerCanonicalCausesTruthfulSkippedWrite() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: tempRoot.path) }
        let videoURL = tempRoot.appendingPathComponent("sample.mov")
        let videoData = Data("test-video".utf8)
        try videoData.write(to: videoURL)

        let hardware = sampleHardware()
        let localSignature = makeLocalSignature(hardware: hardware)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            let paths = resolveThunderboltCAPriorPaths()
            try makePriorArtifact(
                signature: localSignature,
                corpusSummary: BenchmarkPriorCorpusSummary(videoCount: 2, totalBytes: 2_048),
                msPerFrameC1: 1.0,
                msPerVideoP50: 3_000
            ).write(toPath: paths.canonicalPath)

            let (candidateArtifact, outcome) = try updatePipelineBenchmarkPriorFromRun(
                corpus: sampleCorpus(videoURL: videoURL),
                videoSweep: sampleVideoSweep(),
                corpusFrameCounts: [2_400],
                hardware: hardware,
                preset: defaultVideoPreset,
                shouldAttemptUpdate: true
            )

            #expect(candidateArtifact == nil)
            assertOutcome(outcome, matches: .skippedExistingCanonical)
            #expect(!FileManager.default.fileExists(atPath: paths.candidatePath))
            assertEmitsOperatorOutput(renderPriorUpdateLines(candidateArtifact: candidateArtifact, outcome: outcome))
        }
    }

    @Test("nil default preserves update intent while switching to guarded candidate-first semantics")
    func nilDefaultPreservesUpdateIntentWithGuardedSemantics() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: tempRoot.path) }
        let videoURL = tempRoot.appendingPathComponent("sample.mov")
        let videoData = Data("test-video".utf8)
        try videoData.write(to: videoURL)

        let hardware = sampleHardware()
        let localSignature = makeLocalSignature(hardware: hardware)

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            let paths = resolveThunderboltCAPriorPaths()
            try makePriorArtifact(
                signature: localSignature,
                corpusSummary: BenchmarkPriorCorpusSummary(videoCount: 2, totalBytes: 2_048),
                msPerFrameC1: 1.0,
                msPerVideoP50: 3_000
            ).write(toPath: paths.canonicalPath)

            #expect(shouldAttemptPipelinePriorUpdate(nil))
            let (_, outcome) = try updatePipelineBenchmarkPriorFromRun(
                corpus: sampleCorpus(videoURL: videoURL),
                videoSweep: sampleVideoSweep(),
                corpusFrameCounts: [2_400],
                hardware: hardware,
                preset: defaultVideoPreset,
                shouldAttemptUpdate: shouldAttemptPipelinePriorUpdate(nil)
            )

            assertOutcome(outcome, matches: .skippedExistingCanonical)
        }
    }

    @Test("pipeline prior update preserves affine decomposition instead of forcing slope only")
    func pipelinePriorUpdatePreservesAffineDecompositionInsteadOfForcingSlopeOnly() async throws {
        let tempRoot = makeTempDirectory()
        defer { try? FileManager.default.removeItem(atPath: tempRoot.path) }
        let corpus = try makeAffineCorpus(root: tempRoot)
        let hardware = sampleHardware()

        try await TestEnvironment.withEnvironment(["BASE_DIRECTORY": tempRoot.path]) {
            let (candidateArtifact, outcome) = try updatePipelineBenchmarkPriorFromRun(
                corpus: corpus,
                videoSweep: sampleVideoSweep(c1P50Seconds: 0.7),
                corpusFrameCounts: [100, 200, 400],
                localAffineSamples: sampleLocalAffineSamples(corpus: corpus),
                hardware: hardware,
                preset: defaultVideoPreset,
                shouldAttemptUpdate: true
            )

            let machine = try #require(candidateArtifact?.machines.first)
            let slopeOnlyFallback = 700.0 / ((100.0 + 200.0 + 400.0) / 3.0)
            assertOutcome(outcome, matches: .canonicalWritten(resolveThunderboltCAPriorPaths().canonicalPath))
            #expect(abs(machine.fixedOverheadMS - 300) < 0.001)
            #expect(abs(machine.msPerFrameC1 - 2) < 0.001)
            #expect(abs(machine.msPerFrameC1 - slopeOnlyFallback) > 0.5)
        }
    }
}

private func sampleCorpus(videoURL: URL) -> [MediaFile] {
    [
        MediaFile(
            path: videoURL.path,
            name: videoURL.lastPathComponent,
            type: .video,
            sizeBytes: 1_024
        )
    ]
}

private func sampleVideoSweep(c1P50Seconds: Double = 4.0) -> [ConcurrencySweepPoint] {
    [
        ConcurrencySweepPoint(
            concurrency: 1,
            throughputPerMinute: 15.0,
            p50Seconds: c1P50Seconds,
            p95Seconds: c1P50Seconds + 1.0,
            peakMemoryMB: 128
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

private func makePriorArtifact(
    signature: String,
    corpusSummary: BenchmarkPriorCorpusSummary,
    msPerFrameC1: Double,
    msPerVideoP50: Int
) -> BenchmarkPriorArtifact {
    BenchmarkPriorArtifact(
        generatedAt: Date(timeIntervalSince1970: 0),
        corpusHash: "sha256:test",
        corpusSummary: corpusSummary,
        machines: [
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
                    BenchmarkPriorCell(
                        concurrency: 1,
                        videosPerMin: 60_000.0 / Double(msPerVideoP50),
                        msPerVideoP50: msPerVideoP50,
                        msPerVideoP95: msPerVideoP50 + 1_000,
                        degradationRatio: 1.0
                    )
                ]
            )
        ]
    )
}

private func renderPriorUpdateLines(
    candidateArtifact: BenchmarkPriorArtifact?,
    outcome: ThunderboltPriorWriteOutcome
) -> [String] {
    var lines: [String] = []
    reportThunderboltPriorUpdate(
        candidateArtifact: candidateArtifact,
        outcome: outcome,
        outputLine: { lines.append($0) }
    )
    return lines
}

private func assertEmitsOperatorOutput(_ lines: [String]) {
    #expect(!lines.isEmpty)
    #expect(lines.allSatisfy { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty })
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
    case let (.canonicalWritten(actual), .canonicalWritten(expected)),
         let (.promoted(actual), .promoted(expected)):
        #expect(actual == expected)
    case let (.candidateRejected(actualReason, actualPath), .candidateRejected(expectedReason, expectedPath)):
        #expect(actualReason == expectedReason)
        #expect(actualPath == expectedPath)
    default:
        Issue.record("Unexpected outcome: \(outcome)")
    }
}

private func makeTempDirectory() -> URL {
    let path = FileManager.default.temporaryDirectory
        .appendingPathComponent("pipeline-prior-\(UUID().uuidString)")
    try! FileManager.default.createDirectory(at: path, withIntermediateDirectories: true)
    return path
}

private func makeAffineCorpus(root: URL) throws -> [MediaFile] {
    let names = ["a.mov", "b.mov", "c.mov"]
    return try names.enumerated().map { index, name in
        let url = root.appendingPathComponent(name)
        let data = Data("affine-\(index)".utf8)
        try data.write(to: url, options: .atomic)
        return MediaFile(
            path: url.path,
            name: name,
            type: .video,
            sizeBytes: data.count
        )
    }
}

private func sampleLocalAffineSamples(corpus: [MediaFile]) -> [LocalVideoAffineSample] {
    [
        LocalVideoAffineSample(sourcePath: corpus[0].path, frameCount: 100, processMS: 500),
        LocalVideoAffineSample(sourcePath: corpus[1].path, frameCount: 200, processMS: 700),
        LocalVideoAffineSample(sourcePath: corpus[2].path, frameCount: 400, processMS: 1_100),
    ]
}
