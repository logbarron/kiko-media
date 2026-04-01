import Foundation

enum LimitWorkload: String, Sendable {
    case image
    case video
    case mixed
}

struct LimitFinderConfig: Sendable {
    var workload: LimitWorkload = .mixed
    var startLoad: Int = 1
    var stepLoad: Int = 1
    /// When 0 or less, max load is derived from hardware.
    var maxLoad: Int = 0
    var imageJobs: Int?
    var videoJobs: Int?
    var errorRateThreshold: Double = 0.05
    var timeoutThresholdSeconds: Double = 30.0
    var memoryCapMB: Int = 4096
    var thermalThreshold: String = "serious"
    var refinementSteps: Int = 4
    var soakSeconds: Int = 0
    var jsonOutputPath: String?
}

private struct LimitStepExecution {
    let summary: LimitStepSummary
    let isBad: Bool
}

private struct LimitWorkloadRunResult {
    let pipeline: PipelineResult
    let imageConcurrency: Int
    let videoConcurrency: Int
}

private struct LimitRunMetadata: Codable {
    let timestamp: String
    let host: String
    let osVersion: String
    let cpuCores: Int
    let physicalMemoryMB: Int
    let corpusPath: String
    let gitCommit: String?
}

private struct LimitRunConfigJSON: Codable {
    let workload: String
    let startLoad: Int
    let stepLoad: Int
    let maxLoad: Int
    let imageJobs: Int
    let videoJobs: Int
    let errorRateThreshold: Double
    let timeoutThresholdSeconds: Double
    let memoryCapMB: Int
    let thermalThreshold: String
    let refinementSteps: Int
    let soakSeconds: Int
    let videoPreset: String
    let videoTimeout: Int
}

private struct LimitLatencySummary: Codable {
    let p50Seconds: Double
    let p95Seconds: Double
    let minSeconds: Double
    let maxSeconds: Double
    let count: Int
}

private struct LimitStepSummary: Codable {
    let phase: String
    let workload: String
    let load: Int
    let imageConcurrency: Int
    let videoConcurrency: Int
    let completed: Int
    let failed: Int
    let failureRate: Double
    let throughputPerMinute: Double
    let latency: LimitLatencySummary
    let totalSeconds: Double
    let peakMemoryMB: Int
    let thermalState: String
    let stopReason: String?
}

private struct LimitRecommendation: Codable {
    let knee: Int?
    let maxStable: Int?
    let firstUnstable: Int?
    let confidenceNotes: [String]
}

private struct LimitRunSummary: Codable {
    let metadata: LimitRunMetadata
    let config: LimitRunConfigJSON
    let steps: [LimitStepSummary]
    let recommendation: LimitRecommendation
}

func benchmarkLimitFinder(
    corpus: [MediaFile],
    corpusPath: String,
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    config rawConfig: LimitFinderConfig
) async throws {
    let images = corpus.filter { $0.type == .image }
    let videos = corpus.filter { $0.type == .video }

    switch rawConfig.workload {
    case .image:
        guard !images.isEmpty else {
            BenchmarkRuntimeRenderer.printSubsectionTitle("Limit Finder")
            BenchOutput.line("  No image files in media folder, skipping")
            return
        }
    case .video:
        guard !videos.isEmpty else {
            BenchmarkRuntimeRenderer.printSubsectionTitle("Limit Finder")
            BenchOutput.line("  No video files in media folder, skipping")
            return
        }
    case .mixed:
        guard !images.isEmpty, !videos.isEmpty else {
            BenchmarkRuntimeRenderer.printSubsectionTitle("Limit Finder")
            BenchOutput.line("  Mixed mode needs both images and videos in media folder, skipping")
            return
        }
    }

    let config = normalizedLimitConfig(rawConfig, hardware: hardware)
    let imageJobs = max(1, config.imageJobs ?? max(images.count * 3, 24))
    let videoJobs = max(0, config.videoJobs ?? videos.count)

    BenchmarkRuntimeRenderer.printSubsectionTitle("Limit Finder (\(config.workload.rawValue))")
    BenchmarkRuntimeRenderer.printField(
        "Ramp start/step/max",
        "\(config.startLoad) / \(config.stepLoad) / \(config.maxLoad)"
    )
    BenchmarkRuntimeRenderer.printField(
        "Jobs per step",
        "images: \(imageJobs), videos: \(videoJobs)"
    )
    BenchmarkRuntimeRenderer.printField(
        "Stop thresholds",
        "failure>\(String(format: "%.3f", config.errorRateThreshold)), p95>\(String(format: "%.2f", config.timeoutThresholdSeconds))s, memory>=\(config.memoryCapMB)MB, thermal>=\(config.thermalThreshold)"
    )
    BenchmarkRuntimeRenderer.printField(
        "Refine/soak",
        "\(config.refinementSteps) steps, soak \(config.soakSeconds)s"
    )
    BenchmarkRuntimeRenderer.printField(
        "Video preset/timeout",
        "\(preset), \(timeout)s"
    )
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printSubsectionTitle("Step Results")
    let stepColumns = limitStepColumns
    BenchmarkRuntimeRenderer.printTableHeader(stepColumns)
    BenchOutput.line("")

    var steps: [LimitStepSummary] = []
    var lastGood: Int? = nil
    var firstBad: Int? = nil

    var load = config.startLoad
    while load <= config.maxLoad {
        let step = try await runLimitStep(
            phase: "ramp",
            workload: config.workload,
            load: load,
            hardware: hardware,
            images: images,
            videos: videos,
            imageJobs: imageJobs,
            videoJobs: videoJobs,
            preset: preset,
            timeout: timeout,
            config: config
        )
        steps.append(step.summary)
        printLimitStep(step.summary, columns: stepColumns)

        if step.isBad {
            firstBad = load
            break
        }

        lastGood = load
        load += config.stepLoad
    }

    if let good = lastGood, let bad = firstBad, bad - good > 1, config.refinementSteps > 0 {
        var low = good
        var high = bad

        for _ in 0..<config.refinementSteps {
            if high - low <= 1 { break }
            let mid = (low + high) / 2
            let step = try await runLimitStep(
                phase: "refine",
                workload: config.workload,
                load: mid,
                hardware: hardware,
                images: images,
                videos: videos,
                imageJobs: imageJobs,
                videoJobs: videoJobs,
                preset: preset,
                timeout: timeout,
                config: config
            )
            steps.append(step.summary)
            printLimitStep(step.summary, columns: stepColumns)

            if step.isBad {
                high = mid
            } else {
                low = mid
            }
        }

        lastGood = low
        firstBad = high
    }

    if config.soakSeconds > 0, let knee = lastGood {
        let soakAtKnee = try await runLimitSoakStep(
            phase: "soak-knee",
            workload: config.workload,
            load: knee,
            durationSeconds: config.soakSeconds,
            hardware: hardware,
            images: images,
            videos: videos,
            imageJobs: imageJobs,
            videoJobs: videoJobs,
            preset: preset,
            timeout: timeout,
            config: config
        )
        steps.append(soakAtKnee.summary)
        printLimitStep(soakAtKnee.summary, columns: stepColumns)

        let nextLoad = knee + 1
        if nextLoad <= config.maxLoad {
            let soakAtNext = try await runLimitSoakStep(
                phase: "soak-knee+1",
                workload: config.workload,
                load: nextLoad,
                durationSeconds: config.soakSeconds,
                hardware: hardware,
                images: images,
                videos: videos,
                imageJobs: imageJobs,
                videoJobs: videoJobs,
                preset: preset,
                timeout: timeout,
                config: config
            )
            steps.append(soakAtNext.summary)
            printLimitStep(soakAtNext.summary, columns: stepColumns)

            if firstBad == nil, soakAtNext.isBad {
                firstBad = nextLoad
            }
            if !soakAtNext.isBad {
                lastGood = nextLoad
            }
        }
    }

    let confidenceNotes = makeConfidenceNotes(
        workload: config.workload,
        maxLoad: config.maxLoad,
        lastGood: lastGood,
        firstBad: firstBad,
        refinementSteps: config.refinementSteps,
        soakSeconds: config.soakSeconds
    )
    let recommendation = LimitRecommendation(
        knee: lastGood,
        maxStable: lastGood,
        firstUnstable: firstBad,
        confidenceNotes: confidenceNotes
    )

    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printField("Knee (last good)", recommendation.knee.map(String.init) ?? "n/a")
    BenchmarkRuntimeRenderer.printField("Max stable", recommendation.maxStable.map(String.init) ?? "n/a")
    BenchmarkRuntimeRenderer.printField("First unstable", recommendation.firstUnstable.map(String.init) ?? "n/a")
    if !confidenceNotes.isEmpty {
        for note in confidenceNotes {
            BenchmarkRuntimeRenderer.printField("Note", note)
        }
    }

    let summary = LimitRunSummary(
        metadata: makeLimitMetadata(corpusPath: corpusPath),
        config: LimitRunConfigJSON(
            workload: config.workload.rawValue,
            startLoad: config.startLoad,
            stepLoad: config.stepLoad,
            maxLoad: config.maxLoad,
            imageJobs: imageJobs,
            videoJobs: videoJobs,
            errorRateThreshold: config.errorRateThreshold,
            timeoutThresholdSeconds: config.timeoutThresholdSeconds,
            memoryCapMB: config.memoryCapMB,
            thermalThreshold: config.thermalThreshold,
            refinementSteps: config.refinementSteps,
            soakSeconds: config.soakSeconds,
            videoPreset: preset,
            videoTimeout: timeout
        ),
        steps: steps,
        recommendation: recommendation
    )

    if let outputPath = config.jsonOutputPath, !outputPath.isEmpty {
        do {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            let jsonData = try encoder.encode(summary)
            try jsonData.write(to: URL(fileURLWithPath: outputPath), options: .atomic)
            BenchmarkRuntimeRenderer.printField("JSON output", outputPath, semantic: .success)
        } catch {
            BenchmarkRuntimeRenderer.printField("JSON output", "write failed: \(error)", semantic: .error)
        }
    } else {
        BenchmarkRuntimeRenderer.printField("JSON output", "not written (use --limit-json-out <path>)")
    }
}

private func runLimitStep(
    phase: String,
    workload: LimitWorkload,
    load: Int,
    hardware: HardwareProfile,
    images: [MediaFile],
    videos: [MediaFile],
    imageJobs: Int,
    videoJobs: Int,
    preset: String,
    timeout: Int,
    config: LimitFinderConfig
) async throws -> LimitStepExecution {
    let run = try await runLimitWorkload(
        workload: workload,
        load: load,
        hardware: hardware,
        images: images,
        videos: videos,
        imageJobs: imageJobs,
        videoJobs: videoJobs,
        preset: preset,
        timeout: timeout
    )

    let stats = Stats(run.pipeline.latencies)
    let totalJobs = max(1, run.pipeline.completed + run.pipeline.failed)
    let failureRate = Double(run.pipeline.failed) / Double(totalJobs)
    let throughputPerMinute = run.pipeline.totalSeconds > 0
        ? Double(run.pipeline.completed) / run.pipeline.totalSeconds * 60
        : 0
    let stopReason = limitStopReason(pipeline: run.pipeline, stats: stats, config: config)
    let summary = LimitStepSummary(
        phase: phase,
        workload: workload.rawValue,
        load: load,
        imageConcurrency: run.imageConcurrency,
        videoConcurrency: run.videoConcurrency,
        completed: run.pipeline.completed,
        failed: run.pipeline.failed,
        failureRate: failureRate,
        throughputPerMinute: throughputPerMinute,
        latency: LimitLatencySummary(
            p50Seconds: stats.p50,
            p95Seconds: stats.p95,
            minSeconds: stats.min,
            maxSeconds: stats.max,
            count: stats.count
        ),
        totalSeconds: run.pipeline.totalSeconds,
        peakMemoryMB: run.pipeline.peakMemoryMB,
        thermalState: run.pipeline.thermalState,
        stopReason: stopReason
    )

    return LimitStepExecution(summary: summary, isBad: stopReason != nil)
}

private func runLimitSoakStep(
    phase: String,
    workload: LimitWorkload,
    load: Int,
    durationSeconds: Int,
    hardware: HardwareProfile,
    images: [MediaFile],
    videos: [MediaFile],
    imageJobs: Int,
    videoJobs: Int,
    preset: String,
    timeout: Int,
    config: LimitFinderConfig
) async throws -> LimitStepExecution {
    let clock = ContinuousClock()
    let soakStart = clock.now
    var completed = 0
    var failed = 0
    var totalSeconds = 0.0
    var latencies: [Double] = []
    var peakMemoryMB = getMemoryMB()
    var imageConcurrency = 0
    var videoConcurrency = 0
    var thermalState = getThermalState()

    repeat {
        let run = try await runLimitWorkload(
            workload: workload,
            load: load,
            hardware: hardware,
            images: images,
            videos: videos,
            imageJobs: imageJobs,
            videoJobs: videoJobs,
            preset: preset,
            timeout: timeout
        )
        completed += run.pipeline.completed
        failed += run.pipeline.failed
        totalSeconds += run.pipeline.totalSeconds
        latencies.append(contentsOf: run.pipeline.latencies)
        peakMemoryMB = max(peakMemoryMB, run.pipeline.peakMemoryMB)
        imageConcurrency = run.imageConcurrency
        videoConcurrency = run.videoConcurrency
        thermalState = hottestThermalState(thermalState, run.pipeline.thermalState)
    } while (clock.now - soakStart).seconds < Double(durationSeconds)

    let aggregate = PipelineResult(
        completed: completed,
        failed: failed,
        totalSeconds: totalSeconds,
        latencies: latencies,
        peakMemoryMB: peakMemoryMB,
        thermalState: thermalState
    )
    let stats = Stats(aggregate.latencies)
    let totalJobs = max(1, aggregate.completed + aggregate.failed)
    let failureRate = Double(aggregate.failed) / Double(totalJobs)
    let throughputPerMinute = aggregate.totalSeconds > 0
        ? Double(aggregate.completed) / aggregate.totalSeconds * 60
        : 0
    let stopReason = limitStopReason(pipeline: aggregate, stats: stats, config: config)

    let summary = LimitStepSummary(
        phase: phase,
        workload: workload.rawValue,
        load: load,
        imageConcurrency: imageConcurrency,
        videoConcurrency: videoConcurrency,
        completed: aggregate.completed,
        failed: aggregate.failed,
        failureRate: failureRate,
        throughputPerMinute: throughputPerMinute,
        latency: LimitLatencySummary(
            p50Seconds: stats.p50,
            p95Seconds: stats.p95,
            minSeconds: stats.min,
            maxSeconds: stats.max,
            count: stats.count
        ),
        totalSeconds: aggregate.totalSeconds,
        peakMemoryMB: aggregate.peakMemoryMB,
        thermalState: aggregate.thermalState,
        stopReason: stopReason
    )

    return LimitStepExecution(summary: summary, isBad: stopReason != nil)
}

private func runLimitWorkload(
    workload: LimitWorkload,
    load: Int,
    hardware: HardwareProfile,
    images: [MediaFile],
    videos: [MediaFile],
    imageJobs: Int,
    videoJobs: Int,
    preset: String,
    timeout: Int
) async throws -> LimitWorkloadRunResult {
    switch workload {
    case .image:
        let imageConcurrency = load
        let result = try await runImagePipeline(images: images, jobCount: imageJobs, maxConcurrent: imageConcurrency)
        return LimitWorkloadRunResult(pipeline: result, imageConcurrency: imageConcurrency, videoConcurrency: 0)

    case .video:
        let videoConcurrency = load
        let result = try await runVideoPipeline(
            videos: videos,
            jobCount: videoJobs,
            maxConcurrent: videoConcurrency,
            preset: preset,
            timeout: timeout
        )
        return LimitWorkloadRunResult(pipeline: result, imageConcurrency: 0, videoConcurrency: videoConcurrency)

    case .mixed:
        let (imageConcurrency, videoConcurrency) = mixedConcurrency(forLoad: load, hardware: hardware)
        let clock = ContinuousClock()
        let start = clock.now

        async let imageResult = runImagePipeline(
            images: images,
            jobCount: imageJobs,
            maxConcurrent: imageConcurrency
        )
        async let videoResult = runVideoPipeline(
            videos: videos,
            jobCount: videoJobs,
            maxConcurrent: videoConcurrency,
            preset: preset,
            timeout: timeout
        )

        let (ir, vr) = try await (imageResult, videoResult)
        let wallSeconds = (clock.now - start).seconds
        let combined = PipelineResult(
            completed: ir.completed + vr.completed,
            failed: ir.failed + vr.failed,
            totalSeconds: wallSeconds,
            latencies: ir.latencies + vr.latencies,
            peakMemoryMB: max(ir.peakMemoryMB, vr.peakMemoryMB),
            thermalState: getThermalState()
        )
        return LimitWorkloadRunResult(
            pipeline: combined,
            imageConcurrency: imageConcurrency,
            videoConcurrency: videoConcurrency
        )
    }
}

private func mixedConcurrency(forLoad load: Int, hardware: HardwareProfile) -> (Int, Int) {
    let imageConcurrency = max(1, load)
    // Mixed runs are dominated by image work; cap video concurrency to a hardware-aware ceiling.
    let maxVideoConcurrency = max(1, hardware.videoEncodeEngines + 1)
    let videoConcurrency = min(imageConcurrency, maxVideoConcurrency)
    return (imageConcurrency, videoConcurrency)
}

private func limitStopReason(pipeline: PipelineResult, stats: Stats, config: LimitFinderConfig) -> String? {
    let totalJobs = max(1, pipeline.completed + pipeline.failed)
    let failureRate = Double(pipeline.failed) / Double(totalJobs)

    if failureRate > config.errorRateThreshold {
        return "failure_rate_threshold_exceeded"
    }
    if stats.p95 > config.timeoutThresholdSeconds {
        return "timeout_threshold_exceeded"
    }
    if pipeline.peakMemoryMB >= config.memoryCapMB {
        return "memory_cap_exceeded"
    }
    if thermalRank(pipeline.thermalState) >= thermalRank(config.thermalThreshold) {
        return "thermal_threshold_exceeded"
    }
    return nil
}

private func thermalRank(_ state: String) -> Int {
    switch state.lowercased() {
    case "nominal": return 0
    case "fair": return 1
    case "serious": return 2
    case "critical": return 3
    default: return 4
    }
}

private func hottestThermalState(_ lhs: String, _ rhs: String) -> String {
    thermalRank(lhs) >= thermalRank(rhs) ? lhs : rhs
}

private func normalizedLimitConfig(_ raw: LimitFinderConfig, hardware: HardwareProfile) -> LimitFinderConfig {
    var config = raw
    config.startLoad = max(1, raw.startLoad)
    config.stepLoad = max(1, raw.stepLoad)

    if raw.maxLoad > 0 {
        config.maxLoad = max(config.startLoad, raw.maxLoad)
    } else {
        // Default: allow slight oversubscription.
        config.maxLoad = max(config.startLoad, max(1, hardware.totalCores) + 2)
    }
    config.errorRateThreshold = max(0, raw.errorRateThreshold)
    config.timeoutThresholdSeconds = max(0.001, raw.timeoutThresholdSeconds)
    config.memoryCapMB = max(1, raw.memoryCapMB)
    config.refinementSteps = max(0, raw.refinementSteps)
    config.soakSeconds = max(0, raw.soakSeconds)

    switch raw.thermalThreshold.lowercased() {
    case "nominal", "fair", "serious", "critical":
        config.thermalThreshold = raw.thermalThreshold.lowercased()
    default:
        config.thermalThreshold = "serious"
    }

    return config
}

private func makeLimitMetadata(corpusPath: String) -> LimitRunMetadata {
    let process = ProcessInfo.processInfo
    return LimitRunMetadata(
        timestamp: ISO8601DateFormatter().string(from: Date()),
        host: process.environment["HOSTNAME"] ?? "unknown",
        osVersion: process.operatingSystemVersionString,
        cpuCores: process.processorCount,
        physicalMemoryMB: Int(process.physicalMemory / (1024 * 1024)),
        corpusPath: corpusPath,
        gitCommit: currentGitCommit()
    )
}

private func currentGitCommit() -> String? {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = ["git", "rev-parse", "--short", "HEAD"]

    let stdout = Pipe()
    process.standardOutput = stdout
    process.standardError = Pipe()

    do {
        try process.run()
    } catch {
        return nil
    }
    process.waitUntilExit()
    guard process.terminationStatus == 0 else { return nil }

    let data = stdout.fileHandleForReading.readDataToEndOfFile()
    guard var text = String(data: data, encoding: .utf8) else { return nil }
    text = text.trimmingCharacters(in: .whitespacesAndNewlines)
    return text.isEmpty ? nil : text
}

private func makeConfidenceNotes(
    workload: LimitWorkload,
    maxLoad: Int,
    lastGood: Int?,
    firstBad: Int?,
    refinementSteps: Int,
    soakSeconds: Int
) -> [String] {
    var notes: [String] = []

    if let good = lastGood, let bad = firstBad {
        if bad - good <= 1 {
            notes.append("Stable/unstable boundary isolated to adjacent loads.")
        } else if refinementSteps == 0 {
            notes.append("Refinement disabled; boundary remains coarse.")
        } else {
            notes.append("Boundary remains wider than one load step after refinement.")
        }
    } else if lastGood == nil {
        notes.append("No stable step observed; start load may already be unstable.")
    } else if firstBad == nil {
        notes.append("No unstable step observed up to max load \(maxLoad).")
    }

    if soakSeconds > 0 {
        notes.append("Soak executed for \(soakSeconds)s at knee and knee+1 where available.")
    }

    notes.append("Workload tested: \(workload.rawValue).")
    return notes
}

private let limitStepColumns: [BenchmarkRuntimeTableColumn] = [
    BenchmarkRuntimeTableColumn(header: "Phase", width: 10),
    BenchmarkRuntimeTableColumn(header: "Load", width: 6),
    BenchmarkRuntimeTableColumn(header: "Assets/min", width: 10, alignment: .right),
    BenchmarkRuntimeTableColumn(header: "Fail", width: 6, alignment: .right),
    BenchmarkRuntimeTableColumn(header: "P95", width: 8, alignment: .right),
    BenchmarkRuntimeTableColumn(header: "Peak mem", width: 8, alignment: .right),
    BenchmarkRuntimeTableColumn(header: "Thermal", width: 8),
    BenchmarkRuntimeTableColumn(header: "Stop", width: 7),
]

private func printLimitStep(_ step: LimitStepSummary, columns: [BenchmarkRuntimeTableColumn]) {
    let loadText: String
    if step.workload == "mixed" {
        loadText = "i\(step.imageConcurrency)/v\(step.videoConcurrency)"
    } else if step.workload == "image" {
        loadText = "i\(step.imageConcurrency)"
    } else {
        loadText = "v\(step.videoConcurrency)"
    }

    let stopText = step.stopReason ?? "none"
    BenchmarkRuntimeRenderer.printTableRow(
        [
            step.phase,
            loadText,
            String(format: "%.1f", step.throughputPerMinute),
            String(format: "%.1f%%", step.failureRate * 100),
            fmt(step.latency.p95Seconds),
            "\(step.peakMemoryMB)MB",
            step.thermalState,
            stopText,
        ],
        columns: columns,
        semantics: [nil, nil, nil, nil, nil, nil, nil, step.stopReason == nil ? nil : .error]
    )
}
