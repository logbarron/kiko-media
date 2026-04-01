import CryptoKit
import Foundation
import KikoMediaCore

typealias LocalVideoAffineSampleCollector = (
    [MediaFile],
    String,
    Int,
    [String: Double]
) async throws -> [LocalVideoAffineSample]

func resolveCanonicalBenchmarkPriorPath() -> String {
    let configuredBaseDirectory = Config.envString("BASE_DIRECTORY")
        .trimmingCharacters(in: .whitespacesAndNewlines)
    let baseDirectory = configuredBaseDirectory.isEmpty
        ? (Config.stringDefaults["BASE_DIRECTORY"] ?? "")
        : configuredBaseDirectory
    let expandedBaseDirectory = NSString(string: baseDirectory).expandingTildeInPath
    return Config.benchmarkPriorPath(baseDirectoryPath: expandedBaseDirectory)
}

func emitBenchmarkPriorArtifact(
    corpus: [MediaFile],
    videoSweep: [ConcurrencySweepPoint],
    corpusFrameCounts: [Double],
    localAffineSamples: [LocalVideoAffineSample] = [],
    hardware: HardwareProfile,
    preset: String,
    outputPath: String = resolveCanonicalBenchmarkPriorPath(),
    baseArtifact: BenchmarkPriorArtifact? = nil,
    mergedMachines: [BenchmarkPriorMachine] = []
) throws -> String? {
    guard let artifact = try buildBenchmarkPriorArtifact(
        corpus: corpus,
        videoSweep: videoSweep,
        corpusFrameCounts: corpusFrameCounts,
        localAffineSamples: localAffineSamples,
        hardware: hardware,
        preset: preset,
        baseArtifact: baseArtifact,
        mergedMachines: mergedMachines
    ) else {
        return nil
    }
    try artifact.write(toPath: outputPath)
    return outputPath
}

func shouldAttemptPipelinePriorUpdate(_ updateProductionPriorFromRun: Bool?) -> Bool {
    updateProductionPriorFromRun != false
}

func updatePipelineBenchmarkPriorFromRun(
    corpus: [MediaFile],
    videoSweep: [ConcurrencySweepPoint],
    corpusFrameCounts: [Double],
    localAffineSamples: [LocalVideoAffineSample] = [],
    hardware: HardwareProfile,
    preset: String,
    shouldAttemptUpdate: Bool
) throws -> (BenchmarkPriorArtifact?, ThunderboltPriorWriteOutcome) {
    guard shouldAttemptUpdate else {
        return (nil, .skippedPolicyOff)
    }

    let paths = resolveThunderboltCAPriorPaths()
    let existingCanonical = BenchmarkPriorArtifact.load(fromPath: paths.canonicalPath)
    guard let candidateArtifact = try buildBenchmarkPriorArtifact(
        corpus: corpus,
        videoSweep: videoSweep,
        corpusFrameCounts: corpusFrameCounts,
        localAffineSamples: localAffineSamples,
        hardware: hardware,
        preset: preset,
        baseArtifact: existingCanonical
    ) else {
        return (nil, .skippedInsufficientSignal)
    }

    let localSignature = WorkerSignatureBuilder.make(
        chipName: hardware.chipName,
        performanceCores: hardware.performanceCores,
        efficiencyCores: hardware.efficiencyCores,
        videoEncodeEngines: hardware.videoEncodeEngines,
        preset: preset,
        osVersion: ProcessInfo.processInfo.operatingSystemVersion
    )
    guard let existingCanonical else {
        try candidateArtifact.write(toPath: paths.canonicalPath)
        return (candidateArtifact, .canonicalWritten(paths.canonicalPath))
    }

    if pipelineCanonicalContainsMatchingOrStrongerLocalProfile(
        current: existingCanonical,
        candidate: candidateArtifact,
        localSignature: localSignature
    ) {
        return (nil, .skippedExistingCanonical)
    }

    try candidateArtifact.write(toPath: paths.candidatePath)
    guard candidateArtifact.corpusSummary.videoCount >= existingCanonical.corpusSummary.videoCount,
          candidateArtifact.corpusSummary.totalBytes >= existingCanonical.corpusSummary.totalBytes else {
        return (candidateArtifact, .candidateRejected("weaker corpus than canonical", paths.candidatePath))
    }
    guard pipelineCandidateImprovesLocalProfile(
        current: existingCanonical,
        candidate: candidateArtifact,
        localSignature: localSignature
    ) else {
        return (candidateArtifact, .candidateRejected("candidate does not improve local machine profile", paths.candidatePath))
    }

    try candidateArtifact.write(toPath: paths.canonicalPath)
    try? FileManager.default.removeItem(atPath: paths.candidatePath)
    return (candidateArtifact, .promoted(paths.canonicalPath))
}

func buildBenchmarkPriorArtifact(
    corpus: [MediaFile],
    videoSweep: [ConcurrencySweepPoint],
    corpusFrameCounts: [Double],
    localAffineSamples: [LocalVideoAffineSample] = [],
    hardware: HardwareProfile,
    preset: String,
    baseArtifact: BenchmarkPriorArtifact? = nil,
    mergedMachines: [BenchmarkPriorMachine] = []
) throws -> BenchmarkPriorArtifact? {
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty, !videoSweep.isEmpty else { return nil }
    let validFrameCounts = corpusFrameCounts.filter { $0.isFinite && $0 > 0 }
    guard !validFrameCounts.isEmpty else { return nil }
    let avgCorpusFrameCount = validFrameCounts.reduce(0, +) / Double(validFrameCounts.count)

    let osVersion = WorkerSignatureBuilder.normalizedOS(ProcessInfo.processInfo.operatingSystemVersion)
    let signature = WorkerSignatureBuilder.make(
        chipName: hardware.chipName,
        performanceCores: hardware.performanceCores,
        efficiencyCores: hardware.efficiencyCores,
        videoEncodeEngines: hardware.videoEncodeEngines,
        preset: preset,
        osVersion: ProcessInfo.processInfo.operatingSystemVersion
    )

    let baseCells = videoSweep
        .sorted { $0.concurrency < $1.concurrency }
        .map { point in
            BenchmarkPriorCell(
                concurrency: point.concurrency,
                videosPerMin: point.throughputPerMinute,
                msPerVideoP50: max(1, Int((point.p50Seconds * 1_000.0).rounded())),
                msPerVideoP95: max(1, Int((point.p95Seconds * 1_000.0).rounded()))
            )
        }
    let c1Milliseconds = baseCells.first(where: { $0.concurrency == 1 })?.msPerVideoP50 ?? baseCells[0].msPerVideoP50
    let c1 = max(1, c1Milliseconds)
    let fallbackMSPerFrameC1 = Double(c1) / avgCorpusFrameCount
    let affineFit = fitLocalAffinePrior(
        samples: localAffineSamples,
        fallbackMSPerFrameC1: fallbackMSPerFrameC1
    )
    let cells = baseCells.map { cell in
        BenchmarkPriorCell(
            concurrency: cell.concurrency,
            videosPerMin: cell.videosPerMin,
            msPerVideoP50: cell.msPerVideoP50,
            msPerVideoP95: cell.msPerVideoP95,
            degradationRatio: Double(cell.msPerVideoP50) / Double(c1)
        )
    }

    let machine = BenchmarkPriorMachine(
        signature: signature,
        chipName: hardware.chipName,
        performanceCores: hardware.performanceCores,
        efficiencyCores: hardware.efficiencyCores,
        videoEncodeEngines: hardware.videoEncodeEngines,
        osVersion: osVersion,
        transcodePreset: preset,
        msPerFrameC1: affineFit.msPerFrameC1,
        fixedOverheadMS: affineFit.fixedOverheadMS,
        avgCorpusFrameCount: avgCorpusFrameCount,
        cells: cells
    )

    var machinesBySignature: [String: BenchmarkPriorMachine] = [:]
    if let baseArtifact {
        for machine in baseArtifact.machines {
            machinesBySignature[machine.signature] = machine
        }
    }
    for machine in mergedMachines {
        machinesBySignature[machine.signature] = machine
    }
    machinesBySignature[machine.signature] = machine
    let merged = machinesBySignature.values.sorted { lhs, rhs in
        lhs.signature < rhs.signature
    }

    let artifact = BenchmarkPriorArtifact(
        generatedAt: Date(),
        corpusHash: try computeCorpusHash(videos: videos),
        corpusSummary: BenchmarkPriorCorpusSummary(
            videoCount: videos.count,
            totalBytes: videos.reduce(into: Int64(0)) { $0 += Int64($1.sizeBytes) }
        ),
        machines: merged
    )

    return artifact
}

func pipelineCanonicalContainsMatchingOrStrongerLocalProfile(
    current: BenchmarkPriorArtifact,
    candidate: BenchmarkPriorArtifact,
    localSignature: String
) -> Bool {
    guard let currentMachine = current.machines.first(where: { $0.signature == localSignature }),
          let candidateMachine = candidate.machines.first(where: { $0.signature == localSignature }) else {
        return false
    }
    guard current.corpusSummary.videoCount >= candidate.corpusSummary.videoCount,
          current.corpusSummary.totalBytes >= candidate.corpusSummary.totalBytes else {
        return false
    }
    if affineParametersDiffer(currentMachine, candidateMachine) {
        return false
    }
    if let currentMS = validMSPerFrameC1(currentMachine.msPerFrameC1),
       let candidateMS = validMSPerFrameC1(candidateMachine.msPerFrameC1),
       currentMS > candidateMS {
        return false
    }

    let currentCells = Dictionary(uniqueKeysWithValues: currentMachine.cells.map { ($0.concurrency, $0) })
    for candidateCell in candidateMachine.cells {
        guard let currentCell = currentCells[candidateCell.concurrency] else {
            return false
        }
        if currentCell.msPerVideoP50 > candidateCell.msPerVideoP50 {
            return false
        }
        if currentCell.msPerVideoP95 > candidateCell.msPerVideoP95 {
            return false
        }
        if currentCell.videosPerMin < candidateCell.videosPerMin {
            return false
        }
        if currentCell.degradationRatio > candidateCell.degradationRatio {
            return false
        }
    }
    return true
}

func pipelineCandidateImprovesLocalProfile(
    current: BenchmarkPriorArtifact,
    candidate: BenchmarkPriorArtifact,
    localSignature: String
) -> Bool {
    guard let currentMachine = current.machines.first(where: { $0.signature == localSignature }),
          let candidateMachine = candidate.machines.first(where: { $0.signature == localSignature }) else {
        return false
    }
    if affineParametersDiffer(currentMachine, candidateMachine) {
        return true
    }
    if candidate.corpusSummary.videoCount > current.corpusSummary.videoCount
        || candidate.corpusSummary.totalBytes > current.corpusSummary.totalBytes {
        return true
    }
    if let currentMS = validMSPerFrameC1(currentMachine.msPerFrameC1),
       let candidateMS = validMSPerFrameC1(candidateMachine.msPerFrameC1),
       candidateMS < currentMS {
        return true
    }

    let currentCells = Dictionary(uniqueKeysWithValues: currentMachine.cells.map { ($0.concurrency, $0) })
    for candidateCell in candidateMachine.cells {
        guard let currentCell = currentCells[candidateCell.concurrency] else {
            return true
        }
        if candidateCell.msPerVideoP50 < currentCell.msPerVideoP50 {
            return true
        }
        if candidateCell.msPerVideoP95 < currentCell.msPerVideoP95 {
            return true
        }
        if candidateCell.videosPerMin > currentCell.videosPerMin {
            return true
        }
        if candidateCell.degradationRatio < currentCell.degradationRatio {
            return true
        }
    }
    return false
}

func frameCountLookup(
    videos: [MediaFile],
    frameCounts: [Double]
) -> [String: Double] {
    var lookup: [String: Double] = [:]
    lookup.reserveCapacity(min(videos.count, frameCounts.count))
    for (video, frameCount) in zip(videos, frameCounts) where frameCount.isFinite && frameCount > 0 {
        lookup[video.path] = frameCount
    }
    return lookup
}

private func fitLocalAffinePrior(
    samples: [LocalVideoAffineSample],
    fallbackMSPerFrameC1: Double
) -> (fixedOverheadMS: Double, msPerFrameC1: Double) {
    let validSamples = samples.filter {
        $0.frameCount.isFinite
            && $0.frameCount > 0
            && $0.processMS.isFinite
            && $0.processMS > 0
    }
    guard validSamples.count >= 2 else {
        return (0, fallbackMSPerFrameC1)
    }

    let meanFrameCount = validSamples.reduce(0.0) { $0 + $1.frameCount } / Double(validSamples.count)
    let meanProcessMS = validSamples.reduce(0.0) { $0 + $1.processMS } / Double(validSamples.count)
    let denominator = validSamples.reduce(0.0) { partial, sample in
        let deltaX = sample.frameCount - meanFrameCount
        return partial + (deltaX * deltaX)
    }
    guard denominator > 0 else {
        return (0, fallbackMSPerFrameC1)
    }

    let numerator = validSamples.reduce(0.0) { partial, sample in
        let deltaX = sample.frameCount - meanFrameCount
        let deltaY = sample.processMS - meanProcessMS
        return partial + (deltaX * deltaY)
    }
    let slope = numerator / denominator
    guard slope.isFinite, slope > 0 else {
        return (0, fallbackMSPerFrameC1)
    }

    let fixedOverheadMS = meanProcessMS - (slope * meanFrameCount)
    guard fixedOverheadMS.isFinite else {
        return (0, fallbackMSPerFrameC1)
    }

    return (fixedOverheadMS, slope)
}

private func affineParametersDiffer(
    _ lhs: BenchmarkPriorMachine,
    _ rhs: BenchmarkPriorMachine
) -> Bool {
    !doublesEquivalent(lhs.fixedOverheadMS, rhs.fixedOverheadMS)
}

private func doublesEquivalent(
    _ lhs: Double,
    _ rhs: Double,
    tolerance: Double = 0.000_001
) -> Bool {
    abs(lhs - rhs) <= tolerance
}

private func computeCorpusHash(videos: [MediaFile]) throws -> String {
    let hashes = try videos.map { video in
        try SHA256Utility.calculateSHA256(path: video.path)
    }.sorted()

    let combined = hashes.joined()
    let digest = SHA256.hash(data: Data(combined.utf8))
    let digestHex = digest.map { String(format: "%02x", $0) }.joined()
    return "sha256:\(digestHex)"
}
