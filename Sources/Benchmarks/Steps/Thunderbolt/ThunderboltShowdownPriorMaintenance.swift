import Foundation
import KikoMediaCore

typealias ThunderboltPriorUpdateFunction = (
    [MediaFile],
    [MediaFile],
    [(ThunderboltBurstConfig, ThunderboltBurstResult)],
    [ThunderboltBoundWorkerSpec],
    Int,
    Int,
    HardwareProfile,
    String,
    Int,
    ThunderboltPriorUpdatePolicy,
    [String: WorkerCaps]
) async throws -> (BenchmarkPriorArtifact?, ThunderboltPriorWriteOutcome)

typealias ThunderboltOutputLineFunction = (String) -> Void

@discardableResult
func emitThunderboltJSONPriorAfterBurstSweepIfNeeded(
    corpus: [MediaFile],
    videos: [MediaFile],
    runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)],
    workers: [ThunderboltBoundWorkerSpec],
    port: Int,
    connectTimeout: Int,
    hardware: HardwareProfile,
    preset: String,
    timeout: Int,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy = .off,
    workerCapsByHost: [String: WorkerCaps] = [:],
    emitPrior: ThunderboltPriorUpdateFunction = {
        corpus,
        videos,
        runs,
        workers,
        port,
        connectTimeout,
        hardware,
        preset,
        timeout,
        priorUpdatePolicy,
        workerCapsByHost in
        try await updateThunderboltPriorFromBurstSweep(
            corpus: corpus,
            videos: videos,
            runs: runs,
            workers: workers,
            port: port,
            connectTimeout: connectTimeout,
            hardware: hardware,
            preset: preset,
            timeout: timeout,
            priorUpdatePolicy: priorUpdatePolicy,
            workerCapsByHost: workerCapsByHost
        )
    },
    outputLine: ThunderboltOutputLineFunction = BenchOutput.line
) async throws -> BenchmarkPriorArtifact? {
    try await emitThunderboltPriorAfterBurstSweepIfNeeded(
        corpus: corpus,
        videos: videos,
        runs: runs,
        workers: workers,
        port: port,
        connectTimeout: connectTimeout,
        hardware: hardware,
        preset: preset,
        timeout: timeout,
        priorUpdatePolicy: priorUpdatePolicy,
        workerCapsByHost: workerCapsByHost,
        emitPrior: emitPrior,
        outputLine: outputLine
    )
}

func thunderboltPriorUpdatePolicyLabel(_ policy: ThunderboltPriorUpdatePolicy) -> String {
    switch policy {
    case .off:
        return "don't update"
    case .candidateOnly:
        return "save for review"
    case .promoteForce:
        return "promote if safe"
    case .promoteGuarded:
        return "promote only if CA wins"
    }
}

func reportThunderboltPriorUpdate(
    candidateArtifact: BenchmarkPriorArtifact?,
    outcome: ThunderboltPriorWriteOutcome,
    outputLine: ThunderboltOutputLineFunction
) {
    switch outcome {
    case .skippedPolicyOff:
        if candidateArtifact != nil {
            outputLine("  Built benchmark prior candidate: in-memory only")
        }
        outputLine("  Skipped prior update: policy off")
        outputLine("  Canonical prior: unchanged")
        outputLine("  Recommendation: keep current")

    case .skippedInsufficientSignal:
        outputLine("  Skipped prior generation: insufficient successful isolated data")
        outputLine("  Canonical prior: unchanged")
        outputLine("  Recommendation: keep current")

    case .skippedExistingCanonical:
        if candidateArtifact != nil {
            outputLine("  Built benchmark prior candidate: matches canonical local profile")
        }
        outputLine("  Skipped write: canonical already contains matching machine profile")
        outputLine("  Canonical prior: unchanged")
        outputLine("  Recommendation: keep current")

    case .candidateWritten(let path):
        outputLine("  Wrote benchmark prior candidate: \(path)")
        outputLine("  Canonical prior: unchanged")
        outputLine("  Recommendation: review candidate")

    case .canonicalWritten(let path):
        outputLine("  Wrote benchmark prior: \(path)")
        outputLine("  Canonical prior: updated")
        outputLine("  Recommendation: promoted")

    case .promoted(let path):
        outputLine("  Promoted prior candidate to canonical: \(path)")
        outputLine("  Canonical prior: updated")
        outputLine("  Recommendation: promoted")

    case .candidateRejected(let reason, let candidatePath):
        outputLine("  Wrote benchmark prior candidate: \(candidatePath)")
        outputLine("  Skipped promotion: \(reason)")
        outputLine("  Canonical prior: unchanged")
        outputLine("  Recommendation: review candidate")

    case .failed(let error):
        if candidateArtifact != nil {
            outputLine("  Built benchmark prior candidate: in-memory only")
        }
        outputLine("  Failed prior update: \(error)")
        outputLine("  Canonical prior: unchanged")
        outputLine("  Recommendation: keep current")
    }
}

func canonicalContainsMatchingMachineProfile(
    current: BenchmarkPriorArtifact,
    candidate: BenchmarkPriorArtifact,
    localSignature: String
) -> Bool {
    guard let currentMachine = current.machines.first(where: { $0.signature == localSignature }),
          let candidateMachine = candidate.machines.first(where: { $0.signature == localSignature }) else {
        return false
    }
    guard currentMachine == candidateMachine else {
        return false
    }
    return current.corpusSummary.videoCount >= candidate.corpusSummary.videoCount
        && current.corpusSummary.totalBytes >= candidate.corpusSummary.totalBytes
}

func updateThunderboltPriorFromBurstSweep(
    corpus: [MediaFile],
    videos: [MediaFile],
    runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)],
    workers: [ThunderboltBoundWorkerSpec],
    port: Int,
    connectTimeout: Int,
    hardware: HardwareProfile,
    preset: String,
    timeout: Int,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy = .off,
    workerCapsByHost: [String: WorkerCaps] = [:],
    localAffineSampleCollector: @escaping LocalVideoAffineSampleCollector = { videos, preset, timeout, frameCountByPath in
        try await collectLocalVideoAffineSamples(
            videos: videos,
            preset: preset,
            timeout: timeout,
            frameCountByPath: frameCountByPath
        )
    }
) async throws -> (BenchmarkPriorArtifact?, ThunderboltPriorWriteOutcome) {
    let paths = resolveThunderboltCAPriorPaths()
    let existingCanonical = BenchmarkPriorArtifact.load(fromPath: paths.canonicalPath)
    let localSignature = WorkerSignatureBuilder.make(
        chipName: hardware.chipName,
        performanceCores: hardware.performanceCores,
        efficiencyCores: hardware.efficiencyCores,
        videoEncodeEngines: hardware.videoEncodeEngines,
        preset: preset,
        osVersion: ProcessInfo.processInfo.operatingSystemVersion
    )
    guard let localInputs = try await collectThunderboltLocalPriorCandidateInputs(
        videos: videos,
        preset: preset,
        timeout: timeout,
        localRunsProvider: {
            var localRuns = runs
            if !successfulLocalOnlyBurstRuns(from: localRuns).contains(where: { $0.0.localSlots == 1 }) {
                let baseline = ThunderboltBurstConfig(
                    localSlots: 1,
                    remoteSlots: Array(repeating: 0, count: workers.count)
                )
                let baselineResult = try await runThunderboltBurstConfig(
                    config: baseline,
                    workers: workers,
                    videos: videos,
                    port: port,
                    connectTimeout: connectTimeout,
                    preset: preset,
                    timeout: timeout
                )
                if baselineResult.failed == 0,
                   baselineResult.completed > 0,
                   baselineResult.wallSeconds > 0 {
                    localRuns.append((baseline, baselineResult))
                }
            }
            return localRuns
        },
        localAffineSampleCollector: localAffineSampleCollector
    ) else {
        return (nil, .skippedInsufficientSignal)
    }

    guard let candidateArtifact = try buildThunderboltLocalPriorCandidateArtifact(
        corpus: corpus,
        hardware: hardware,
        preset: preset,
        baseArtifact: existingCanonical,
        inputs: localInputs
    ) else {
        return (nil, .skippedInsufficientSignal)
    }

    do {
        let application = try applyThunderboltShowdownPriorUpdatePolicy(
            candidateArtifact: candidateArtifact,
            currentCanonicalArtifact: existingCanonical,
            localSignature: localSignature,
            reachableWorkers: workers,
            workerCapsByHost: workerCapsByHost,
            port: port,
            policy: priorUpdatePolicy,
            paths: paths
        )
        return (candidateArtifact, application.outcome)
    } catch {
        return (candidateArtifact, .failed(error))
    }
}

func emitThunderboltPriorAfterBurstSweepIfNeeded(
    corpus: [MediaFile],
    videos: [MediaFile],
    runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)],
    workers: [ThunderboltBoundWorkerSpec],
    port: Int,
    connectTimeout: Int,
    hardware: HardwareProfile,
    preset: String,
    timeout: Int,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy = .off,
    workerCapsByHost: [String: WorkerCaps] = [:],
    emitPrior: ThunderboltPriorUpdateFunction = {
        corpus,
        videos,
        runs,
        workers,
        port,
        connectTimeout,
        hardware,
        preset,
        timeout,
        priorUpdatePolicy,
        workerCapsByHost in
        try await updateThunderboltPriorFromBurstSweep(
            corpus: corpus,
            videos: videos,
            runs: runs,
            workers: workers,
            port: port,
            connectTimeout: connectTimeout,
            hardware: hardware,
            preset: preset,
            timeout: timeout,
            priorUpdatePolicy: priorUpdatePolicy,
            workerCapsByHost: workerCapsByHost
        )
    },
    outputLine: ThunderboltOutputLineFunction = BenchOutput.line
) async throws -> BenchmarkPriorArtifact? {
    do {
        let (candidateArtifact, outcome) = try await emitPrior(
            corpus,
            videos,
            runs,
            workers,
            port,
            connectTimeout,
            hardware,
            preset,
            timeout,
            priorUpdatePolicy,
            workerCapsByHost
        )
        reportThunderboltPriorUpdate(
            candidateArtifact: candidateArtifact,
            outcome: outcome,
            outputLine: outputLine
        )
        return candidateArtifact
    } catch {
        if isBenchmarkInterrupted(error, interruptState: nil) {
            throw error
        }
        reportThunderboltPriorUpdate(
            candidateArtifact: nil,
            outcome: .failed(error),
            outputLine: outputLine
        )
        return nil
    }
}

func effectiveThunderboltBurstSweepPriorUpdatePolicy(
    includeShowdown: Bool,
    showdownPriorUpdatePolicy: ThunderboltPriorUpdatePolicy
) -> ThunderboltPriorUpdatePolicy {
    includeShowdown ? showdownPriorUpdatePolicy : .off
}

func successfulLocalOnlyBurstRuns(
    from runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)]
) -> [(ThunderboltBurstConfig, ThunderboltBurstResult)] {
    runs.filter { config, result in
        config.localSlots > 0 &&
            config.remoteSlots.allSatisfy { $0 == 0 } &&
            result.failed == 0 &&
            result.completed > 0 &&
            result.wallSeconds > 0
    }
}

func syntheticVideoSweepFromLocalBurstRuns(
    _ runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)]
) -> [ConcurrencySweepPoint] {
    var bestByConcurrency: [Int: ConcurrencySweepPoint] = [:]

    for (config, result) in runs {
        let concurrency = max(1, config.localSlots)
        let completed = Double(result.completed)
        guard completed > 0, result.wallSeconds > 0 else { continue }

        let serviceSeconds = (result.wallSeconds * Double(concurrency)) / completed
        guard serviceSeconds.isFinite, serviceSeconds > 0 else { continue }

        let candidate = ConcurrencySweepPoint(
            concurrency: concurrency,
            throughputPerMinute: completed / result.wallSeconds * 60.0,
            p50Seconds: serviceSeconds,
            p95Seconds: serviceSeconds,
            peakMemoryMB: 0
        )

        if let existing = bestByConcurrency[concurrency] {
            if candidate.p50Seconds < existing.p50Seconds {
                bestByConcurrency[concurrency] = candidate
            }
        } else {
            bestByConcurrency[concurrency] = candidate
        }
    }

    return bestByConcurrency.values.sorted { lhs, rhs in
        lhs.concurrency < rhs.concurrency
    }
}

struct ThunderboltShowdownPriorCandidateBuildResult: Sendable {
    let artifact: BenchmarkPriorArtifact
    let contributedRemoteHosts: [String]
    let skippedRemoteHosts: [String: String]
}

struct ThunderboltShowdownPriorMaintenanceResult: Sendable {
    let setup: ThunderboltCARunSetup
    let candidateArtifact: BenchmarkPriorArtifact
    let currentCanonicalArtifact: BenchmarkPriorArtifact?
    let paths: ThunderboltCAPriorPaths
    let policy: ThunderboltPriorUpdatePolicy
    let allowExistingCanonicalSkip: Bool
    let deferredPromotion: Bool
}

struct ThunderboltLocalPriorCandidateInputs: Sendable {
    let videoSweep: [ConcurrencySweepPoint]
    let frameCounts: [Double]
    let localAffineSamples: [LocalVideoAffineSample]
}

struct ThunderboltPriorPolicyApplicationResult: Sendable {
    let outcome: ThunderboltPriorWriteOutcome
    let deferredPromotion: Bool
}

typealias ThunderboltShowdownPriorCandidateBuilder = (
    [MediaFile],
    String,
    Int,
    HardwareProfile,
    ThunderboltCARunSetup,
    BenchmarkPriorArtifact?,
    ThunderboltShowdownPreflightClassification
) async throws -> ThunderboltShowdownPriorCandidateBuildResult?

typealias ThunderboltBurstConfigRunner = (
    ThunderboltBurstConfig,
    [ThunderboltBoundWorkerSpec],
    [MediaFile],
    Int,
    Int,
    String,
    Int
) async throws -> ThunderboltBurstResult

typealias ThunderboltFrameCountEstimator = ([MediaFile]) async -> [Double]

typealias ThunderboltLocalBurstRunProvider = () async throws -> [(ThunderboltBurstConfig, ThunderboltBurstResult)]

typealias ThunderboltShowdownSetupPreparer = (
    [MediaFile],
    String,
    HardwareProfile,
    ThunderboltCASlotOverrides?,
    ThunderboltCAModelMode,
    BenchmarkPriorTable?
) async throws -> ThunderboltCARunSetup

private func emitThunderboltPriorMaintenanceProgress(
    _ outputLine: ThunderboltOutputLineFunction,
    stage: String,
    detail: String
) {
    outputLine("  Progress: \(stage): \(detail)")
}

func collectThunderboltLocalPriorCandidateInputs(
    videos: [MediaFile],
    preset: String,
    timeout: Int,
    localRunsProvider: ThunderboltLocalBurstRunProvider,
    frameCountEstimator: ThunderboltFrameCountEstimator? = nil,
    localAffineSampleCollector: LocalVideoAffineSampleCollector? = nil,
    progressReporter: ThunderboltPriorMaintenanceProgressReporter? = nil,
    emitProgressWhenNoBoard: Bool = false,
    outputLine: @escaping ThunderboltOutputLineFunction = BenchOutput.line,
    loopCancellationCheck: @escaping @Sendable (ThunderboltLoopCancellationPoint) throws -> Void = { _ in
        try Task.checkCancellation()
    }
) async throws -> ThunderboltLocalPriorCandidateInputs? {
    let localRuns = successfulLocalOnlyBurstRuns(from: try await localRunsProvider())
    let syntheticSweep = syntheticVideoSweepFromLocalBurstRuns(localRuns)
    guard !syntheticSweep.isEmpty else {
        return nil
    }

    let emitProgress: (String, String) -> Void = { stage, detail in
        guard emitProgressWhenNoBoard else {
            return
        }
        emitThunderboltPriorMaintenanceProgress(
            outputLine,
            stage: stage,
            detail: detail
        )
    }

    if let progressReporter {
        await progressReporter.startStage(.frameCounting, detail: "0/\(videos.count) videos")
    } else {
        emitProgress("frame counting", "starting")
    }
    try loopCancellationCheck(.priorFrameCounting)
    let frameCounts: [Double]
    if let frameCountEstimator {
        frameCounts = await frameCountEstimator(videos)
    } else {
        frameCounts = await caEstimates(
            videos: videos,
            localMSPerFrameC1: 0,
            onProgress: { done, total in
                guard let progressReporter else { return }
                Task {
                    await progressReporter.updateStage(
                        .frameCounting,
                        detail: "\(done)/\(total) videos"
                    )
                }
            }
        )
    }
    if let progressReporter {
        await progressReporter.completeStage(.frameCounting, detail: "\(frameCounts.count) videos")
        await progressReporter.startStage(.affineSampleCollection, detail: "0/\(videos.count) videos")
    } else {
        emitProgress("frame counting", "complete (\(frameCounts.count) videos)")
        emitProgress("affine sample collection", "starting")
    }

    try loopCancellationCheck(.priorAffineSampleCollection)
    let frameCountByPath = frameCountLookup(videos: videos, frameCounts: frameCounts)
    let localAffineSamples: [LocalVideoAffineSample]
    if let localAffineSampleCollector {
        localAffineSamples = try await localAffineSampleCollector(
            videos,
            preset,
            timeout,
            frameCountByPath
        )
    } else {
        localAffineSamples = try await collectLocalVideoAffineSamples(
            videos: videos,
            preset: preset,
            timeout: timeout,
            frameCountByPath: frameCountByPath,
            onProgress: { done, total in
                guard let progressReporter else { return }
                Task {
                    await progressReporter.updateStage(
                        .affineSampleCollection,
                        detail: "\(done)/\(total) videos"
                    )
                }
            }
        )
    }
    if let progressReporter {
        await progressReporter.completeStage(
            .affineSampleCollection,
            detail: "\(localAffineSamples.count) samples"
        )
    } else {
        emitProgress(
            "affine sample collection",
            "complete (\(localAffineSamples.count) samples)"
        )
    }

    return ThunderboltLocalPriorCandidateInputs(
        videoSweep: syntheticSweep,
        frameCounts: frameCounts,
        localAffineSamples: localAffineSamples
    )
}

func buildThunderboltLocalPriorCandidateArtifact(
    corpus: [MediaFile],
    hardware: HardwareProfile,
    preset: String,
    baseArtifact: BenchmarkPriorArtifact? = nil,
    mergedMachines: [BenchmarkPriorMachine] = [],
    inputs: ThunderboltLocalPriorCandidateInputs
) throws -> BenchmarkPriorArtifact? {
    try buildBenchmarkPriorArtifact(
        corpus: corpus,
        videoSweep: inputs.videoSweep,
        corpusFrameCounts: inputs.frameCounts,
        localAffineSamples: inputs.localAffineSamples,
        hardware: hardware,
        preset: preset,
        baseArtifact: baseArtifact,
        mergedMachines: mergedMachines
    )
}

func makeThunderboltRemoteMaintenanceTelemetrySample(
    host: String,
    workerSignature: String?,
    concurrency: Int,
    isolated: Bool,
    success: Bool,
    actualExecutor: String,
    processNanos: UInt64,
    txInMS: Double? = nil,
    txOutMS: Double? = nil,
    publishOverheadMS: Double? = nil,
    videoPath: String = "",
    frameCount: Double = 0
) -> ThunderboltRemoteMaintenanceTelemetrySample {
    let invalidationReason: ThunderboltRemoteMaintenanceSampleInvalidationReason? = {
        if !isolated {
            return .nonIsolatedProbe
        }
        if !success {
            return .unsuccessfulRemoteProbe
        }
        if concurrency <= 0 {
            return .invalidConcurrency
        }
        if workerSignature?.isEmpty != false {
            return .missingWorkerSignature
        }
        if actualExecutor == "local-fallback" {
            return .localFallback
        }
        if actualExecutor != host {
            return .executorMismatch
        }
        if processNanos == 0 {
            return .missingProcessTime
        }
        return nil
    }()

    return ThunderboltRemoteMaintenanceTelemetrySample(
        host: host,
        workerSignature: workerSignature,
        concurrency: concurrency,
        isolated: isolated,
        actualExecutor: actualExecutor,
        processNanos: processNanos,
        txInMS: txInMS,
        txOutMS: txOutMS,
        publishOverheadMS: publishOverheadMS,
        videoPath: videoPath,
        frameCount: frameCount,
        invalidationReason: invalidationReason
    )
}

func evaluateThunderboltRemoteMaintenancePriorEligibility(
    samples: [ThunderboltRemoteMaintenanceTelemetrySample]
) -> ThunderboltRemoteMaintenancePriorEligibility {
    let validConcurrencies = Set(samples.filter(\.validForPriorGeneration).map(\.concurrency))
    guard validConcurrencies.contains(1) else {
        return ThunderboltRemoteMaintenancePriorEligibility(
            workerEligible: false,
            eligibleConcurrencies: []
        )
    }
    return ThunderboltRemoteMaintenancePriorEligibility(
        workerEligible: true,
        eligibleConcurrencies: validConcurrencies
    )
}

func classifyThunderboltShowdownPreflight(
    localPriorGap: Bool,
    remotePriorGap: Bool
) -> ThunderboltShowdownPreflightClassification {
    ThunderboltShowdownPreflightClassification(
        sharedClassification: BenchmarkPriorPreflightClassification.classify(
            localPriorGap: localPriorGap,
            remotePriorGap: remotePriorGap
        )
    )
}

func runThunderboltShowdownPriorMaintenance(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    setup: ThunderboltCARunSetup,
    slotOverrides: ThunderboltCASlotOverrides?,
    modelMode: ThunderboltCAModelMode,
    preflight: ThunderboltShowdownPreflightClassification,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy,
    allowHealthyPreflight: Bool = false,
    candidateBuilder: ThunderboltShowdownPriorCandidateBuilder? = nil,
    useProgressBoard: Bool? = nil,
    progressReporter: ThunderboltPriorMaintenanceProgressReporter? = nil,
    prepareSetup: ThunderboltShowdownSetupPreparer = { corpus, preset, hardware, slotOverrides, mode, priorTableOverride in
        try await prepareThunderboltCARunSetup(
            corpus: corpus,
            preset: preset,
            hardware: hardware,
            slotOverrides: slotOverrides,
            mode: mode,
            priorTableOverride: priorTableOverride
        )
    },
    outputLine: @escaping ThunderboltOutputLineFunction = BenchOutput.line
) async throws -> ThunderboltShowdownPriorMaintenanceResult? {
    guard priorUpdatePolicy != .off else {
        return nil
    }

    switch preflight {
    case .healthy:
        guard !allowHealthyPreflight else { break }
        outputLine("  Skipped prior maintenance: preflight is healthy.")
        return nil
    case .localPriorGap, .localAndRemotePriorGap:
        break
    case .remotePriorGap:
        break
    }

    let activeProgressReporter = progressReporter ?? ((useProgressBoard ?? (candidateBuilder == nil))
        ? ThunderboltPriorMaintenanceProgressReporter()
        : nil)
    var deferredSummaryLines: [String] = []
    let summaryOutputLine: ThunderboltOutputLineFunction = { line in
        if activeProgressReporter == nil {
            outputLine(line)
        } else {
            deferredSummaryLines.append(line)
        }
    }

    let paths = resolveThunderboltCAPriorPaths()
    let existingCanonical = BenchmarkPriorArtifact.load(fromPath: paths.canonicalPath)
    let resolvedCandidateBuilder = candidateBuilder ?? { corpus, preset, timeout, hardware, setup, currentCanonicalArtifact, preflight in
        try await buildThunderboltShowdownPriorCandidateArtifact(
            corpus: corpus,
            preset: preset,
            timeout: timeout,
            hardware: hardware,
            setup: setup,
            currentCanonicalArtifact: currentCanonicalArtifact,
            preflight: preflight,
            progressReporter: activeProgressReporter,
            outputLine: outputLine
        )
    }
    do {
        guard let buildResult = try await resolvedCandidateBuilder(
            corpus,
            preset,
            timeout,
            hardware,
            setup,
            existingCanonical,
            preflight
        ) else {
            _ = await activeProgressReporter?.finish()
            reportThunderboltPriorUpdate(
                candidateArtifact: nil,
                outcome: .skippedInsufficientSignal,
                outputLine: outputLine
            )
            return nil
        }

        let remoteScope = buildResult.contributedRemoteHosts.sorted()
        if remoteScope.isEmpty {
            summaryOutputLine("  Built benchmark prior candidate: local only")
        } else {
            summaryOutputLine("  Built benchmark prior candidate: local + \(remoteScope.joined(separator: ", "))")
        }
        for (host, reason) in buildResult.skippedRemoteHosts.sorted(by: { lhs, rhs in
            lhs.key < rhs.key
        }) {
            summaryOutputLine("  Skipped remote prior generation for \(host): \(reason)")
        }

        let candidateArtifact = buildResult.artifact
        let policyApplication = try applyThunderboltShowdownPriorUpdatePolicy(
            candidateArtifact: candidateArtifact,
            currentCanonicalArtifact: existingCanonical,
            localSignature: setup.localSignature,
            reachableWorkers: setup.reachableWorkers,
            workerCapsByHost: setup.workerCapsByHost,
            port: setup.port,
            policy: priorUpdatePolicy,
            paths: paths,
            allowExistingCanonicalSkip: remoteScope.isEmpty,
            deferPromotion: existingCanonical != nil,
            emitPromotionCoverage: false
        )

        if !policyApplication.deferredPromotion {
            reportThunderboltPriorUpdate(
                candidateArtifact: candidateArtifact,
                outcome: policyApplication.outcome,
                outputLine: summaryOutputLine
            )
        }

        if let activeProgressReporter {
            await activeProgressReporter.startStage(.setupRebuild, detail: "rebuilding")
        } else {
            emitThunderboltPriorMaintenanceProgress(
                outputLine,
                stage: "setup rebuild",
                detail: "starting"
            )
        }
        let refreshedSetup = try await prepareSetup(
            corpus,
            preset,
            hardware,
            slotOverrides,
            modelMode,
            BenchmarkPriorTable(artifact: candidateArtifact)
        )
        if let activeProgressReporter {
            await activeProgressReporter.completeStage(.setupRebuild, detail: "complete")
            _ = await activeProgressReporter.finish()
        } else {
            emitThunderboltPriorMaintenanceProgress(
                outputLine,
                stage: "setup rebuild",
                detail: "complete"
            )
        }
        for line in deferredSummaryLines {
            outputLine(line)
        }
        return ThunderboltShowdownPriorMaintenanceResult(
            setup: refreshedSetup,
            candidateArtifact: candidateArtifact,
            currentCanonicalArtifact: existingCanonical,
            paths: paths,
            policy: priorUpdatePolicy,
            allowExistingCanonicalSkip: remoteScope.isEmpty,
            deferredPromotion: policyApplication.deferredPromotion
        )
    } catch {
        if let activeProgressReporter {
            _ = await activeProgressReporter.finish()
        }
        for line in deferredSummaryLines {
            outputLine(line)
        }
        throw error
    }
}

func buildThunderboltShowdownPriorCandidateArtifact(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    setup: ThunderboltCARunSetup,
    currentCanonicalArtifact: BenchmarkPriorArtifact?,
    preflight: ThunderboltShowdownPreflightClassification,
    probeAllReachableRemoteWorkers: Bool = false,
    burstConfigRunner: ThunderboltBurstConfigRunner? = nil,
    frameCountEstimator: ThunderboltFrameCountEstimator? = nil,
    localAffineSampleCollector: LocalVideoAffineSampleCollector? = nil,
    progressReporter: ThunderboltPriorMaintenanceProgressReporter? = nil,
    roundTripRunner: @escaping ThunderboltCARoundTripRunner = runThunderboltRoundTrip,
    outputLine: @escaping ThunderboltOutputLineFunction = BenchOutput.line,
    loopCancellationCheck: @escaping @Sendable (ThunderboltLoopCancellationPoint) throws -> Void = { _ in
        try Task.checkCancellation()
    }
) async throws -> ThunderboltShowdownPriorCandidateBuildResult? {
    let maxLocal = max(
        1,
        burstSweepLocalSlotsMax(
            videoEncodeEngines: hardware.videoEncodeEngines,
            totalCores: hardware.totalCores
        )
    )
    var localRuns: [(ThunderboltBurstConfig, ThunderboltBurstResult)] = []
    localRuns.reserveCapacity(maxLocal)

    if let progressReporter {
        await progressReporter.startStage(.localSweep, detail: "0/\(maxLocal) configs")
    } else {
        emitThunderboltPriorMaintenanceProgress(
            outputLine,
            stage: "local sweep",
            detail: "0/\(maxLocal)"
        )
    }
    for localSlots in 1...maxLocal {
        try loopCancellationCheck(.priorLocalSweep)
        let config = ThunderboltBurstConfig(
            localSlots: localSlots,
            remoteSlots: Array(repeating: 0, count: setup.reachableWorkers.count)
        )
        let completedConfigs = localSlots - 1
        let result: ThunderboltBurstResult
        if let burstConfigRunner {
            result = try await burstConfigRunner(
                config,
                setup.reachableWorkers,
                setup.videos,
                setup.port,
                setup.connectTimeout,
                preset,
                timeout
            )
        } else {
            result = try await runThunderboltBurstConfig(
                config: config,
                workers: setup.reachableWorkers,
                videos: setup.videos,
                port: setup.port,
                connectTimeout: setup.connectTimeout,
                preset: preset,
                timeout: timeout,
                onProgress: { done, total in
                    guard let progressReporter else { return }
                    Task {
                        await progressReporter.updateStage(
                            .localSweep,
                            detail: "\(completedConfigs)/\(maxLocal) configs, \(done)/\(total) videos"
                        )
                    }
                },
                loopCancellationCheck: loopCancellationCheck
            )
        }
        if result.failed == 0, result.completed > 0, result.wallSeconds > 0 {
            localRuns.append((config, result))
        }
        if let progressReporter {
            if localSlots == maxLocal {
                await progressReporter.completeStage(.localSweep, detail: "\(localSlots)/\(maxLocal) configs")
            } else {
                await progressReporter.updateStage(.localSweep, detail: "\(localSlots)/\(maxLocal) configs")
            }
        } else {
            emitThunderboltPriorMaintenanceProgress(
                outputLine,
                stage: "local sweep",
                detail: "\(localSlots)/\(maxLocal)"
            )
        }
    }

    guard let localInputs = try await collectThunderboltLocalPriorCandidateInputs(
        videos: setup.videos,
        preset: preset,
        timeout: timeout,
        localRunsProvider: { localRuns },
        frameCountEstimator: frameCountEstimator,
        localAffineSampleCollector: localAffineSampleCollector,
        progressReporter: progressReporter,
        emitProgressWhenNoBoard: true,
        outputLine: outputLine,
        loopCancellationCheck: loopCancellationCheck
    ) else {
        throw ThunderboltShowdownPriorMaintenanceError.noLocalBurstData
    }
    var contributedRemoteHosts: [String] = []
    var skippedRemoteHosts: [String: String] = [:]
    var remoteMachines: [BenchmarkPriorMachine] = []

    let shouldProbeRemoteWorkers =
        probeAllReachableRemoteWorkers
        || preflight == .remotePriorGap
        || preflight == .localAndRemotePriorGap
    if shouldProbeRemoteWorkers {
        let gapHosts = if probeAllReachableRemoteWorkers {
            Set(setup.reachableWorkers.map(\.host))
        } else {
            Set(setup.diagnostics.coverageRows.compactMap { row -> String? in
                guard row.reachableSlots > 0 else { return nil }
                let needsMaintenance =
                    row.modeledSlots < row.reachableSlots
                    || row.msSource != "prior(remote)"
                    || row.curveSource != "prior(remote)"
                    || thunderboltRemoteWorkerNeedsAffineMigration(
                        host: row.host,
                        setup: setup
                    )
                return needsMaintenance ? row.host : nil
            })
        }
        if !gapHosts.isEmpty {
            if let progressReporter {
                await progressReporter.startStage(.remoteSamplePreparation, detail: "preparing samples")
            } else {
                emitThunderboltPriorMaintenanceProgress(
                    outputLine,
                    stage: "remote sample preparation",
                    detail: "starting"
                )
            }
            try loopCancellationCheck(.priorRemoteSamplePreparation)
            if let representativeSamples = prepareThunderboltRemoteMaintenanceRepresentativeSamples(
                from: setup.videos,
                frameCounts: localInputs.frameCounts,
                onProgress: { done, total in
                    guard let progressReporter else { return }
                    Task { await progressReporter.updateStage(.remoteSamplePreparation, detail: "\(done)/\(total) samples") }
                }
            ), !representativeSamples.isEmpty {
                if let progressReporter {
                    await progressReporter.completeStage(.remoteSamplePreparation, detail: "\(representativeSamples.count) samples")
                } else {
                    emitThunderboltPriorMaintenanceProgress(
                        outputLine,
                        stage: "remote sample preparation",
                        detail: "complete (\(representativeSamples.count) samples)"
                    )
                }
                let telemetryWorkers = setup.reachableWorkers.filter { gapHosts.contains($0.host) }
                let totalTelemetryProbes = telemetryWorkers.reduce(into: 0) { partial, worker in
                    partial += representativeSamples.count * (1...worker.slots).reduce(0, +)
                }
                if let progressReporter {
                    await progressReporter.startStage(
                        .remoteTelemetry,
                        detail: "0/\(telemetryWorkers.count) workers, 0/\(totalTelemetryProbes) probes"
                    )
                } else {
                    emitThunderboltPriorMaintenanceProgress(
                        outputLine,
                        stage: "remote telemetry",
                        detail: "0/\(telemetryWorkers.count) workers"
                    )
                }
                var completedWorkers = 0
                var completedProbes = 0
                for (index, worker) in telemetryWorkers.enumerated() {
                    try loopCancellationCheck(.priorRemoteTelemetry)
                    let telemetry: [ThunderboltRemoteMaintenanceTelemetrySample]
                    if progressReporter == nil {
                        telemetry = try await collectThunderboltRemoteMaintenanceTelemetryInterruptibly(
                            worker: worker,
                            workerSignature: setup.workerCapsByHost[worker.host]?.workerSignature,
                            samples: representativeSamples,
                            port: setup.port,
                            connectTimeout: setup.connectTimeout,
                            roundTripRunner: roundTripRunner,
                            loopCancellationCheck: loopCancellationCheck
                        )
                    } else {
                        let completedWorkersSnapshot = completedWorkers
                        let completedProbesSnapshot = completedProbes
                        telemetry = try await collectThunderboltRemoteMaintenanceTelemetryInterruptibly(
                            worker: worker,
                            workerSignature: setup.workerCapsByHost[worker.host]?.workerSignature,
                            samples: representativeSamples,
                            port: setup.port,
                            connectTimeout: setup.connectTimeout,
                            roundTripRunner: roundTripRunner,
                            onProgress: { workerDone, _ in
                                guard let progressReporter else { return }
                                Task {
                                    await progressReporter.updateStage(
                                        .remoteTelemetry,
                                        detail: "\(completedWorkersSnapshot)/\(telemetryWorkers.count) workers, \(completedProbesSnapshot + workerDone)/\(totalTelemetryProbes) probes"
                                    )
                                }
                            },
                            loopCancellationCheck: loopCancellationCheck
                        )
                    }
                    let machineResult = buildThunderboltRemoteMaintenanceMachine(
                        worker: worker,
                        caps: setup.workerCapsByHost[worker.host],
                        preset: preset,
                        telemetry: telemetry
                    )
                    completedWorkers += 1
                    completedProbes += telemetry.count
                    if let machine = machineResult.machine {
                        remoteMachines.append(machine)
                        contributedRemoteHosts.append(worker.host)
                        if let progressReporter {
                            if index + 1 == telemetryWorkers.count {
                                await progressReporter.completeStage(
                                    .remoteTelemetry,
                                    detail: "\(completedWorkers)/\(telemetryWorkers.count) workers, \(completedProbes)/\(totalTelemetryProbes) probes"
                                )
                            } else {
                                await progressReporter.updateStage(
                                    .remoteTelemetry,
                                    detail: "\(completedWorkers)/\(telemetryWorkers.count) workers, \(completedProbes)/\(totalTelemetryProbes) probes"
                                )
                            }
                        } else {
                            emitThunderboltPriorMaintenanceProgress(
                                outputLine,
                                stage: "remote telemetry",
                                detail: "\(index + 1)/\(telemetryWorkers.count) workers (\(worker.host): contributed \(telemetry.count) probes)"
                            )
                        }
                    } else {
                        skippedRemoteHosts[worker.host] = machineResult.reason
                        if let progressReporter {
                            if index + 1 == telemetryWorkers.count {
                                await progressReporter.completeStage(
                                    .remoteTelemetry,
                                    detail: "\(completedWorkers)/\(telemetryWorkers.count) workers, \(completedProbes)/\(totalTelemetryProbes) probes"
                                )
                            } else {
                                await progressReporter.updateStage(
                                    .remoteTelemetry,
                                    detail: "\(completedWorkers)/\(telemetryWorkers.count) workers, \(completedProbes)/\(totalTelemetryProbes) probes"
                                )
                            }
                        } else {
                            emitThunderboltPriorMaintenanceProgress(
                                outputLine,
                                stage: "remote telemetry",
                                detail: "\(index + 1)/\(telemetryWorkers.count) workers (\(worker.host): skipped - \(machineResult.reason))"
                            )
                        }
                    }
                }
            } else {
                if let progressReporter {
                    await progressReporter.failStage(
                        .remoteSamplePreparation,
                        detail: "could not prepare isolated remote maintenance sample set"
                    )
                    await progressReporter.skipStage(.remoteTelemetry, detail: "sample preparation failed")
                } else {
                    emitThunderboltPriorMaintenanceProgress(
                        outputLine,
                        stage: "remote sample preparation",
                        detail: "failed (could not prepare isolated remote maintenance sample set)"
                    )
                    emitThunderboltPriorMaintenanceProgress(
                        outputLine,
                        stage: "remote telemetry",
                        detail: "skipped (sample preparation failed)"
                    )
                }
                for host in gapHosts.sorted() {
                    skippedRemoteHosts[host] = "could not prepare isolated remote maintenance sample set"
                }
            }
        } else {
            if let progressReporter {
                await progressReporter.skipStage(.remoteSamplePreparation, detail: "no remote maintenance gaps")
                await progressReporter.skipStage(.remoteTelemetry, detail: "no remote maintenance gaps")
            } else {
                emitThunderboltPriorMaintenanceProgress(
                    outputLine,
                    stage: "remote sample preparation",
                    detail: "skipped (no remote maintenance gaps)"
                )
                emitThunderboltPriorMaintenanceProgress(
                    outputLine,
                    stage: "remote telemetry",
                    detail: "skipped (no remote maintenance gaps)"
                )
            }
        }
    } else {
        if let progressReporter {
            await progressReporter.skipStage(.remoteSamplePreparation, detail: "preflight does not require remote maintenance")
            await progressReporter.skipStage(.remoteTelemetry, detail: "preflight does not require remote maintenance")
        } else {
            emitThunderboltPriorMaintenanceProgress(
                outputLine,
                stage: "remote sample preparation",
                detail: "skipped (preflight does not require remote maintenance)"
            )
            emitThunderboltPriorMaintenanceProgress(
                outputLine,
                stage: "remote telemetry",
                detail: "skipped (preflight does not require remote maintenance)"
            )
        }
    }

    guard let artifact = try buildThunderboltLocalPriorCandidateArtifact(
        corpus: corpus,
        hardware: hardware,
        preset: preset,
        baseArtifact: currentCanonicalArtifact,
        mergedMachines: remoteMachines,
        inputs: localInputs
    ) else {
        return nil
    }
    return ThunderboltShowdownPriorCandidateBuildResult(
        artifact: artifact,
        contributedRemoteHosts: contributedRemoteHosts.sorted(),
        skippedRemoteHosts: skippedRemoteHosts
    )
}

func shouldRunThunderboltFullPriorMaintenance(
    includeShowdown: Bool,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy
) -> Bool {
    includeShowdown && priorUpdatePolicy != .off
}

func runThunderboltFullPriorMaintenance(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    modelMode: ThunderboltCAModelMode,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy,
    priorTableOverlay: BenchmarkPriorTable?,
    remoteTxInEstimateMSByHost: [String: Double],
    useProgressBoard: Bool? = nil,
    setupBuilder: (
        [MediaFile],
        String,
        HardwareProfile,
        ThunderboltCAModelMode,
        BenchmarkPriorTable?,
        [String: Double]
    ) async throws -> ThunderboltCARunSetup = { corpus, preset, hardware, modelMode, priorTableOverride, remoteTxInEstimateMSByHost in
        try await prepareThunderboltCARunSetup(
            corpus: corpus,
            preset: preset,
            hardware: hardware,
            slotOverrides: nil,
            mode: modelMode,
            priorTableOverride: priorTableOverride,
            remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost
        )
    },
    candidateBuilder: ThunderboltShowdownPriorCandidateBuilder? = nil,
    prepareSetup: ThunderboltShowdownSetupPreparer = { corpus, preset, hardware, slotOverrides, mode, priorTableOverride in
        try await prepareThunderboltCARunSetup(
            corpus: corpus,
            preset: preset,
            hardware: hardware,
            slotOverrides: slotOverrides,
            mode: mode,
            priorTableOverride: priorTableOverride
        )
    },
    outputLine: @escaping ThunderboltOutputLineFunction = BenchOutput.line
) async throws -> ThunderboltShowdownPriorMaintenanceResult? {
    guard priorUpdatePolicy != .off else {
        return nil
    }

    let setup = try await setupBuilder(
        corpus,
        preset,
        hardware,
        modelMode,
        priorTableOverlay,
        remoteTxInEstimateMSByHost
    )
    let preflight = classifyThunderboltShowdownPreflight(
        localPriorGap: setup.diagnostics.localPriorGap,
        remotePriorGap: setup.diagnostics.remotePriorGap
    )
    let shouldUseProgressBoard = useProgressBoard ?? (candidateBuilder == nil)
    let progressReporter = shouldUseProgressBoard ? ThunderboltPriorMaintenanceProgressReporter() : nil
    let resolvedCandidateBuilder = candidateBuilder ?? { corpus, preset, timeout, hardware, setup, currentCanonicalArtifact, preflight in
        try await buildThunderboltShowdownPriorCandidateArtifact(
            corpus: corpus,
            preset: preset,
            timeout: timeout,
            hardware: hardware,
            setup: setup,
            currentCanonicalArtifact: currentCanonicalArtifact,
            preflight: preflight,
            probeAllReachableRemoteWorkers: true,
            progressReporter: progressReporter,
            outputLine: outputLine
        )
    }
    return try await runThunderboltShowdownPriorMaintenance(
        corpus: corpus,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        setup: setup,
        slotOverrides: nil,
        modelMode: modelMode,
        preflight: preflight,
        priorUpdatePolicy: priorUpdatePolicy,
        allowHealthyPreflight: true,
        candidateBuilder: resolvedCandidateBuilder,
        useProgressBoard: shouldUseProgressBoard,
        progressReporter: progressReporter,
        prepareSetup: prepareSetup,
        outputLine: outputLine
    )
}

func collectThunderboltRemoteMaintenanceTelemetry(
    worker: ThunderboltBoundWorkerSpec,
    workerSignature: String?,
    samples representativeSamples: [ThunderboltRemoteMaintenancePreparedSample],
    port: Int,
    connectTimeout: Int,
    roundTripRunner: @escaping ThunderboltCARoundTripRunner = runThunderboltRoundTrip,
    onProgress: (@Sendable (Int, Int) -> Void)? = nil
) async -> [ThunderboltRemoteMaintenanceTelemetrySample] {
    do {
        return try await collectThunderboltRemoteMaintenanceTelemetryInterruptibly(
            worker: worker,
            workerSignature: workerSignature,
            samples: representativeSamples,
            port: port,
            connectTimeout: connectTimeout,
            roundTripRunner: roundTripRunner,
            onProgress: onProgress
        )
    } catch {
        return []
    }
}

func collectThunderboltRemoteMaintenanceTelemetryInterruptibly(
    worker: ThunderboltBoundWorkerSpec,
    workerSignature: String?,
    samples representativeSamples: [ThunderboltRemoteMaintenancePreparedSample],
    port: Int,
    connectTimeout: Int,
    roundTripRunner: @escaping ThunderboltCARoundTripRunner = runThunderboltRoundTrip,
    onProgress: (@Sendable (Int, Int) -> Void)? = nil,
    loopCancellationCheck: @escaping @Sendable (ThunderboltLoopCancellationPoint) throws -> Void = { _ in
        try Task.checkCancellation()
    }
) async throws -> [ThunderboltRemoteMaintenanceTelemetrySample] {
    guard worker.slots > 0, !representativeSamples.isEmpty else { return [] }

    let tempDir = makeTempDir("tb-remote-maintenance")
    defer { cleanup(tempDir) }

    var samples: [ThunderboltRemoteMaintenanceTelemetrySample] = []
    samples.reserveCapacity(representativeSamples.count * (1...worker.slots).reduce(0, +))
    onProgress?(0, representativeSamples.count * (1...worker.slots).reduce(0, +))

    for representativeSample in representativeSamples {
        try loopCancellationCheck(.priorRemoteTelemetry)
        let sampleVideo = representativeSample.video
        let sampleSHA256 = representativeSample.sha256
        let sampleFrameCount = representativeSample.frameCount
        let sampleVideoPath = representativeSample.video.path

        for concurrency in 1...worker.slots {
            try loopCancellationCheck(.priorRemoteTelemetry)
            let batch = try await withThrowingTaskGroup(
                of: ThunderboltRemoteMaintenanceTelemetrySample.self
            ) { group in
                for _ in 0..<concurrency {
                    try loopCancellationCheck(.priorRemoteTelemetry)
                    group.addTask {
                        try loopCancellationCheck(.priorRemoteTelemetry)
                        let result = roundTripRunner(
                            worker,
                            sampleVideo,
                            sampleSHA256,
                            port,
                            connectTimeout,
                            tempDir
                        )
                        try loopCancellationCheck(.priorRemoteTelemetry)
                        return makeThunderboltRemoteMaintenanceTelemetrySample(
                            host: worker.host,
                            workerSignature: workerSignature,
                            concurrency: concurrency,
                            isolated: true,
                            success: result.success,
                            actualExecutor: worker.host,
                            processNanos: result.processNanos,
                            txInMS: result.firstRunningLatencySecondsEstimate.map { max(0, $0 * 1_000.0) },
                            txOutMS: result.txOutMS,
                            publishOverheadMS: result.publishOverheadMS,
                            videoPath: sampleVideoPath,
                            frameCount: sampleFrameCount
                        )
                    }
                }

                var batchSamples: [ThunderboltRemoteMaintenanceTelemetrySample] = []
                batchSamples.reserveCapacity(concurrency)
                for try await sample in group {
                    batchSamples.append(sample)
                }
                return batchSamples
            }
            samples.append(contentsOf: batch)
            onProgress?(samples.count, representativeSamples.count * (1...worker.slots).reduce(0, +))
        }
    }

    return samples
}

func buildThunderboltRemoteMaintenanceMachine(
    worker _: ThunderboltBoundWorkerSpec,
    caps: WorkerCaps?,
    preset: String,
    telemetry: [ThunderboltRemoteMaintenanceTelemetrySample]
) -> (machine: BenchmarkPriorMachine?, reason: String) {
    guard let caps,
          let signature = caps.workerSignature,
          !signature.isEmpty else {
        return (nil, "missing worker signature from capabilities")
    }

    let eligibility = evaluateThunderboltRemoteMaintenancePriorEligibility(samples: telemetry)
    guard eligibility.workerEligible else {
        return (nil, "insufficient valid isolated data")
    }

    let validSamples = telemetry.filter(\.validForPriorGeneration)
    let affineSamples = telemetry.filter(\.validForAffinePriorGeneration).filter { $0.concurrency == 1 }
    let eligibleConcurrencies = eligibility.eligibleConcurrencies.sorted()
    guard let affineFit = fitThunderboltRemoteMaintenanceAffineProcessModel(affineSamples),
          let avgFrameCount = thunderboltRemoteMaintenanceAverage(affineSamples.map(\.frameCount)),
          let c1MedianMS = thunderboltRemoteMaintenanceMedian(
              affineSamples.map { Double($0.processNanos) / 1_000_000.0 }
          ) else {
        return (nil, "insufficient valid isolated data")
    }

    var cells: [BenchmarkPriorCell] = []
    cells.reserveCapacity(eligibleConcurrencies.count)

    for concurrency in eligibleConcurrencies {
        let processTimesMS = validSamples
            .filter { $0.concurrency == concurrency }
            .map { Double($0.processNanos) / 1_000_000.0 }
            .sorted()
        guard let medianMS = thunderboltRemoteMaintenanceMedian(processTimesMS),
              c1MedianMS > 0 else {
            continue
        }
        guard let p50MS = thunderboltRemoteMaintenancePercentile(processTimesMS, percentile: 0.50),
              let p95MS = thunderboltRemoteMaintenancePercentile(processTimesMS, percentile: 0.95) else {
            continue
        }
        let roundedP50 = max(1, Int(p50MS.rounded()))
        let roundedP95 = max(1, Int(p95MS.rounded()))
        cells.append(
            BenchmarkPriorCell(
                concurrency: concurrency,
                videosPerMin: (Double(concurrency) * 60_000.0) / Double(roundedP50),
                msPerVideoP50: roundedP50,
                msPerVideoP95: roundedP95,
                degradationRatio: medianMS / c1MedianMS
            )
        )
    }

    guard cells.contains(where: { $0.concurrency == 1 }) else {
        return (nil, "insufficient valid isolated data")
    }

    return (
        BenchmarkPriorMachine(
            signature: signature,
            chipName: caps.chipName ?? "unknown",
            performanceCores: caps.performanceCores ?? 0,
            efficiencyCores: caps.efficiencyCores ?? 0,
            videoEncodeEngines: caps.videoEncodeEngines ?? 0,
            osVersion: caps.osVersion ?? "unknown",
            transcodePreset: preset,
            msPerFrameC1: affineFit.msPerFrameC1,
            fixedOverheadMS: affineFit.fixedOverheadMS,
            avgCorpusFrameCount: avgFrameCount,
            cells: cells
        ),
        "ok"
    )
}

func selectThunderboltRemoteMaintenanceRepresentativeVideos(
    from videos: [MediaFile],
    frameCounts: [Double]
) -> [ThunderboltRemoteMaintenanceRepresentativeVideo] {
    let rankedVideos = zip(videos, frameCounts).enumerated().map { offset, pair in
        (
            offset: offset,
            video: pair.0,
            frameCount: max(1, pair.1)
        )
    }.sorted { lhs, rhs in
        if lhs.frameCount != rhs.frameCount {
            return lhs.frameCount < rhs.frameCount
        }
        if lhs.video.path != rhs.video.path {
            return lhs.video.path < rhs.video.path
        }
        return lhs.offset < rhs.offset
    }

    guard !rankedVideos.isEmpty else { return [] }
    if rankedVideos.count < 5 {
        return rankedVideos.map { ThunderboltRemoteMaintenanceRepresentativeVideo(video: $0.video, frameCount: $0.frameCount) }
    }

    let lastIndex = rankedVideos.count - 1
    let quantileIndices = [
        0,
        Int(floor(Double(lastIndex) * 0.25)),
        Int(floor(Double(lastIndex) * 0.50)),
        Int(floor(Double(lastIndex) * 0.75)),
        lastIndex,
    ]

    var selectedIndices: [Int] = []
    var seenIndices = Set<Int>()
    for index in quantileIndices where seenIndices.insert(index).inserted {
        selectedIndices.append(index)
    }
    if selectedIndices.count < 5 {
        for index in rankedVideos.indices where seenIndices.insert(index).inserted {
            selectedIndices.append(index)
            if selectedIndices.count == 5 {
                break
            }
        }
    }

    return selectedIndices.map { index in
        let ranked = rankedVideos[index]
        return ThunderboltRemoteMaintenanceRepresentativeVideo(
            video: ranked.video,
            frameCount: ranked.frameCount
        )
    }
}

func prepareThunderboltRemoteMaintenanceRepresentativeSamples(
    from videos: [MediaFile],
    frameCounts: [Double],
    onProgress: (@Sendable (Int, Int) -> Void)? = nil
) -> [ThunderboltRemoteMaintenancePreparedSample]? {
    let representativeVideos = selectThunderboltRemoteMaintenanceRepresentativeVideos(
        from: videos,
        frameCounts: frameCounts
    )
    guard !representativeVideos.isEmpty else { return nil }

    var samples: [ThunderboltRemoteMaintenancePreparedSample] = []
    samples.reserveCapacity(representativeVideos.count)
    onProgress?(0, representativeVideos.count)
    for representativeVideo in representativeVideos {
        guard let sha256 = try? SHA256Utility.calculateSHA256(
            path: representativeVideo.video.path,
            bufferSize: BenchDefaults.sha256BufferSize
        ) else {
            return nil
        }
        samples.append(
            ThunderboltRemoteMaintenancePreparedSample(
                video: representativeVideo.video,
                frameCount: representativeVideo.frameCount,
                sha256: sha256
            )
        )
        onProgress?(samples.count, representativeVideos.count)
    }
    return samples
}

private func thunderboltRemoteMaintenancePercentile(
    _ values: [Double],
    percentile: Double
) -> Double? {
    guard !values.isEmpty else { return nil }
    let sorted = values.sorted()
    let rank = Int((Double(sorted.count - 1) * percentile).rounded(.up))
    return sorted[min(sorted.count - 1, max(0, rank))]
}

private struct ThunderboltRemoteMaintenanceAffineFit {
    let fixedOverheadMS: Double
    let msPerFrameC1: Double
}

private func fitThunderboltRemoteMaintenanceAffineProcessModel(
    _ samples: [ThunderboltRemoteMaintenanceTelemetrySample]
) -> ThunderboltRemoteMaintenanceAffineFit? {
    guard samples.count >= 2 else { return nil }

    let points = samples.map { sample in
        (
            frameCount: sample.frameCount,
            processMS: Double(sample.processNanos) / 1_000_000.0
        )
    }
    let sampleCount = Double(points.count)
    let sumFrameCount = points.reduce(0.0) { $0 + $1.frameCount }
    let sumProcessMS = points.reduce(0.0) { $0 + $1.processMS }
    let meanFrameCount = sumFrameCount / sampleCount
    let meanProcessMS = sumProcessMS / sampleCount
    let centeredFrameVariance = points.reduce(0.0) { partial, point in
        let delta = point.frameCount - meanFrameCount
        return partial + (delta * delta)
    }
    guard centeredFrameVariance > 0 else { return nil }

    let covariance = points.reduce(0.0) { partial, point in
        partial + ((point.frameCount - meanFrameCount) * (point.processMS - meanProcessMS))
    }
    let msPerFrameC1 = covariance / centeredFrameVariance
    let fixedOverheadMS = meanProcessMS - (msPerFrameC1 * meanFrameCount)
    return ThunderboltRemoteMaintenanceAffineFit(
        fixedOverheadMS: fixedOverheadMS,
        msPerFrameC1: msPerFrameC1
    )
}

private func thunderboltRemoteMaintenanceMedian(_ values: [Double]) -> Double? {
    guard !values.isEmpty else { return nil }
    let sorted = values.sorted()
    let middleIndex = sorted.count / 2
    if sorted.count.isMultiple(of: 2) {
        return (sorted[middleIndex - 1] + sorted[middleIndex]) / 2.0
    }
    return sorted[middleIndex]
}

private func thunderboltRemoteMaintenanceAverage(_ values: [Double]) -> Double? {
    guard !values.isEmpty else { return nil }
    return values.reduce(0.0, +) / Double(values.count)
}

private func thunderboltRemoteWorkerNeedsAffineMigration(
    host: String,
    setup: ThunderboltCARunSetup
) -> Bool {
    guard let signature = setup.workerCapsByHost[host]?.workerSignature else {
        return false
    }
    let matchedMachine = setup.priorTable.exactMachine(signature: signature)
        ?? setup.priorTable.hardwareCompatibleMachine(signature: signature)
    return CAProfileAndFallbackMath.requiresRemoteAffineMigration(matchedMachine)
}

func finalizeThunderboltShowdownPriorPromotionIfNeeded(
    maintenanceResult: ThunderboltShowdownPriorMaintenanceResult?,
    showdownComparatorPass: Bool,
    outputLine: ThunderboltOutputLineFunction = BenchOutput.line
) throws {
    guard let maintenanceResult,
          maintenanceResult.deferredPromotion,
          let currentCanonicalArtifact = maintenanceResult.currentCanonicalArtifact else {
        return
    }

    let application = try applyThunderboltShowdownPriorUpdatePolicy(
        candidateArtifact: maintenanceResult.candidateArtifact,
        currentCanonicalArtifact: currentCanonicalArtifact,
        localSignature: maintenanceResult.setup.localSignature,
        reachableWorkers: maintenanceResult.setup.reachableWorkers,
        workerCapsByHost: maintenanceResult.setup.workerCapsByHost,
        port: maintenanceResult.setup.port,
        policy: maintenanceResult.policy,
        paths: maintenanceResult.paths,
        allowExistingCanonicalSkip: maintenanceResult.allowExistingCanonicalSkip,
        showdownComparatorPass: showdownComparatorPass,
        requireComparator: maintenanceResult.policy == .promoteGuarded,
        candidateAlreadyWritten: true,
        emitPromotionCoverage: true
    )
    reportThunderboltPriorUpdate(
        candidateArtifact: maintenanceResult.candidateArtifact,
        outcome: application.outcome,
        outputLine: outputLine
    )
}

func applyThunderboltShowdownPriorUpdatePolicy(
    candidateArtifact: BenchmarkPriorArtifact,
    currentCanonicalArtifact: BenchmarkPriorArtifact?,
    localSignature: String,
    reachableWorkers: [ThunderboltBoundWorkerSpec],
    workerCapsByHost: [String: WorkerCaps],
    port: Int,
    policy: ThunderboltPriorUpdatePolicy,
    paths: ThunderboltCAPriorPaths,
    allowExistingCanonicalSkip: Bool = true,
    showdownComparatorPass: Bool? = nil,
    requireComparator: Bool = false,
    deferPromotion: Bool = false,
    candidateAlreadyWritten: Bool = false,
    emitPromotionCoverage: Bool = false
) throws -> ThunderboltPriorPolicyApplicationResult {
    if policy == .off {
        return ThunderboltPriorPolicyApplicationResult(
            outcome: .skippedPolicyOff,
            deferredPromotion: false
        )
    }
    if allowExistingCanonicalSkip,
       let currentCanonicalArtifact,
       canonicalContainsMatchingMachineProfile(
        current: currentCanonicalArtifact,
        candidate: candidateArtifact,
        localSignature: localSignature
       ) {
        return ThunderboltPriorPolicyApplicationResult(
            outcome: .skippedExistingCanonical,
            deferredPromotion: false
        )
    }
    guard let currentCanonicalArtifact else {
        try candidateArtifact.write(toPath: paths.canonicalPath)
        return ThunderboltPriorPolicyApplicationResult(
            outcome: .canonicalWritten(paths.canonicalPath),
            deferredPromotion: false
        )
    }

    switch policy {
    case .off:
        return ThunderboltPriorPolicyApplicationResult(
            outcome: .skippedPolicyOff,
            deferredPromotion: false
        )
    case .candidateOnly:
        if !candidateAlreadyWritten {
            try candidateArtifact.write(toPath: paths.candidatePath)
        }
        return ThunderboltPriorPolicyApplicationResult(
            outcome: .candidateWritten(paths.candidatePath),
            deferredPromotion: false
        )
    case .promoteGuarded, .promoteForce:
        if !candidateAlreadyWritten {
            try candidateArtifact.write(toPath: paths.candidatePath)
        }
        if deferPromotion {
            return ThunderboltPriorPolicyApplicationResult(
                outcome: .candidateWritten(paths.candidatePath),
                deferredPromotion: true
            )
        }
        let decision = evaluateThunderboltShowdownPriorPromotion(
            currentPriorTable: BenchmarkPriorTable(artifact: currentCanonicalArtifact),
            candidatePriorTable: BenchmarkPriorTable(artifact: candidateArtifact),
            localSignature: localSignature,
            reachableWorkers: reachableWorkers,
            workerCapsByHost: workerCapsByHost,
            port: port,
            currentCorpusSummary: currentCanonicalArtifact.corpusSummary,
            candidateCorpusSummary: candidateArtifact.corpusSummary,
            showdownComparatorPass: showdownComparatorPass,
            requireComparator: requireComparator,
            force: policy == .promoteForce
        )
        if emitPromotionCoverage {
            BenchmarkRuntimeRenderer.printField(
                "Promotion coverage",
                "workers \(decision.currentRemoteWorkerCoverage)->\(decision.candidateRemoteWorkerCoverage), " +
                    "slots \(decision.currentRemoteSlotCoverage)->\(decision.candidateRemoteSlotCoverage)"
            )
        }
        guard decision.shouldPromote else {
            return ThunderboltPriorPolicyApplicationResult(
                outcome: .candidateRejected(decision.reason, paths.candidatePath),
                deferredPromotion: false
            )
        }
        try candidateArtifact.write(toPath: paths.canonicalPath)
        try? FileManager.default.removeItem(atPath: paths.candidatePath)
        return ThunderboltPriorPolicyApplicationResult(
            outcome: .promoted(paths.canonicalPath),
            deferredPromotion: false
        )
    }
}

func evaluateThunderboltShowdownPriorPromotion(
    currentPriorTable: BenchmarkPriorTable,
    candidatePriorTable: BenchmarkPriorTable,
    localSignature: String,
    reachableWorkers: [ThunderboltBoundWorkerSpec],
    workerCapsByHost: [String: WorkerCaps],
    port: Int,
    currentCorpusSummary: BenchmarkPriorCorpusSummary? = nil,
    candidateCorpusSummary: BenchmarkPriorCorpusSummary? = nil,
    showdownComparatorPass: Bool? = nil,
    requireComparator: Bool = false,
    force: Bool
) -> ThunderboltShowdownPriorPromotionDecision {
    let currentLocalPriorValid = strictLocalPriorValid(
        priorTable: currentPriorTable,
        localSignature: localSignature
    )
    let candidateLocalPriorValid = strictLocalPriorValid(
        priorTable: candidatePriorTable,
        localSignature: localSignature
    )

    let currentModeledWorkers = strictModeledWorkers(
        priorTable: currentPriorTable,
        localSignature: localSignature,
        reachableWorkers: reachableWorkers,
        workerCapsByHost: workerCapsByHost,
        port: port
    )
    let candidateModeledWorkers = strictModeledWorkers(
        priorTable: candidatePriorTable,
        localSignature: localSignature,
        reachableWorkers: reachableWorkers,
        workerCapsByHost: workerCapsByHost,
        port: port
    )
    return ThunderboltShowdownPriorPromotionDecision(
        sharedDecision: BenchmarkPriorPolicyKernel.evaluatePromotion(
            localSignature: localSignature,
            currentLocalPriorValid: currentLocalPriorValid,
            candidateLocalPriorValid: candidateLocalPriorValid,
            currentModeledWorkers: currentModeledWorkers.map(\.sharedPriorModeledWorker),
            candidateModeledWorkers: candidateModeledWorkers.map(\.sharedPriorModeledWorker),
            currentCorpusSummary: currentCorpusSummary,
            candidateCorpusSummary: candidateCorpusSummary,
            showdownComparatorPass: showdownComparatorPass,
            requireComparator: requireComparator,
            force: force
        )
    )
}

func strictLocalPriorValid(
    priorTable: BenchmarkPriorTable,
    localSignature: String
) -> Bool {
    guard let machine = priorTable.machines.first(where: { $0.signature == localSignature }) else {
        return false
    }
    guard validMSPerFrameC1(machine.msPerFrameC1) != nil else {
        return false
    }
    return !caDegradationCurve(from: machine).isEmpty
}

func strictModeledWorkers(
    priorTable: BenchmarkPriorTable,
    localSignature: String,
    reachableWorkers: [ThunderboltBoundWorkerSpec],
    workerCapsByHost: [String: WorkerCaps],
    port: Int
) -> [ThunderboltShowdownModeledWorker] {
    let localMachine = priorTable.machines.first(where: { $0.signature == localSignature })
    let localCurve: [CADegradationPoint]
    if let localMachine {
        let curve = caDegradationCurve(from: localMachine)
        localCurve = curve.isEmpty ? [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)] : curve
    } else {
        localCurve = [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)]
    }
    let localMS = localMachine?.msPerFrameC1 ?? 1.0
    return reachableWorkers.compactMap { worker -> ThunderboltShowdownModeledWorker? in
        guard worker.slots > 0 else { return nil }
        let caps = workerCapsByHost[worker.host]
        guard let signature = caps?.workerSignature else { return nil }
        let decision = resolveThunderboltCARemoteModelDecision(
            host: worker.host,
            port: port,
            mode: .strict,
            caps: caps,
            priorTable: priorTable,
            localMSPerFrameC1: localMS,
            localCurve: localCurve
        )
        guard decision.machineID != nil else { return nil }
        return ThunderboltShowdownModeledWorker(signature: signature, slots: worker.slots)
    }
}
