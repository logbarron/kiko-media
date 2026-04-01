import Foundation
import AVFoundation
import KikoMediaCore

private func progressBar(label: String? = nil, done: Int, total: Int, width: Int = 20) -> String {
    let filled = total > 0 ? done * width / total : 0
    let bar = String(repeating: "\u{25B8}", count: filled) + String(repeating: "\u{00B7}", count: width - filled)
    let prefix = if let label, !label.isEmpty {
        "\(label) "
    } else {
        ""
    }
    return "\r  \(prefix)\(done)/\(total) \(bar)"
}

private func clearProgressBar() {
    BenchOutput.write("\r\u{1B}[2K")
}

private func uniqueMediaFilesByPath(_ files: [MediaFile]) -> [MediaFile] {
    var uniqueFiles: [MediaFile] = []
    uniqueFiles.reserveCapacity(files.count)

    var seenPaths = Set<String>()
    for file in files where seenPaths.insert(file.path).inserted {
        uniqueFiles.append(file)
    }

    return uniqueFiles
}

struct ConcurrencySweepPoint: Sendable {
    let concurrency: Int
    let throughputPerMinute: Double
    let p50Seconds: Double
    let p95Seconds: Double
    let peakMemoryMB: Int
}

struct MixedConcurrencySweepPoint: Sendable {
    let imageConcurrency: Int
    let videoConcurrency: Int
    let throughputPerMinute: Double
    let peakMemoryMB: Int
}

struct LocalVideoAffineSample: Sendable, Equatable {
    let sourcePath: String
    let frameCount: Double
    let processMS: Double
}

struct VideoConcurrencySweepResult: Sendable {
    let points: [ConcurrencySweepPoint]
    let corpusFrameCounts: [Double]
    let localAffineSamples: [LocalVideoAffineSample]
}

func benchmarkImageConcurrency(corpus: [MediaFile], hardware: HardwareProfile) async throws -> [ConcurrencySweepPoint] {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Image Concurrency Sweep")

    let images = corpus.filter { $0.type == .image }
    guard !images.isEmpty else {
        BenchOutput.line("  No image files in media folder, skipping")
        return []
    }

    let jobsPerLevel = max(images.count * 3, 20)
    let levels = imageSweepLevels(totalCores: hardware.totalCores)
    var points: [ConcurrencySweepPoint] = []

    BenchmarkRuntimeRenderer.printField(
        "Files",
        "\(images.count) images, reused to \(jobsPerLevel) jobs per level"
    )
    BenchmarkRuntimeRenderer.printField(
        "Sweep levels",
        "\(levels.map(String.init).joined(separator: ", ")) (cores: \(hardware.totalCores))"
    )
    BenchmarkRuntimeRenderer.printDetail("Sub-tasks run in parallel per job (matching production)")
    BenchOutput.line("")
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Concurrency", width: 11, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Images/min", width: 10, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "P50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "P95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Peak mem", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Thermal", width: 8),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for (levelIndex, level) in levels.enumerated() {
        let progressLabel = "Level \(levelIndex + 1)/\(levels.count) · concurrency \(level)"
        BenchOutput.write(progressBar(label: progressLabel, done: 0, total: jobsPerLevel))
        let result = try await runImagePipeline(
            images: images,
            jobCount: jobsPerLevel,
            maxConcurrent: level
        ) { done, total in
            BenchOutput.write(progressBar(label: progressLabel, done: done, total: total))
        }
        clearProgressBar()
        let perMin = Double(result.completed) / result.totalSeconds * 60
        let stats = Stats(result.latencies)
        BenchmarkRuntimeRenderer.printTableRow(
            [
                String(level),
                String(format: "%.1f", perMin),
                fmt(stats.p50),
                fmt(stats.p95),
                "\(result.peakMemoryMB)MB",
                result.thermalState,
            ],
            columns: columns
        )
        points.append(
            ConcurrencySweepPoint(
                concurrency: level,
                throughputPerMinute: perMin,
                p50Seconds: stats.p50,
                p95Seconds: stats.p95,
                peakMemoryMB: result.peakMemoryMB
            )
        )
    }

    return points
}

func benchmarkVideoConcurrency(
    corpus: [MediaFile],
    hardware: HardwareProfile,
    preset: String = AVAssetExportPreset1920x1080,
    timeout: Int = 300
) async throws -> VideoConcurrencySweepResult {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Video Concurrency Sweep")

    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else {
        BenchOutput.line("  No video files in media folder, skipping")
        return VideoConcurrencySweepResult(points: [], corpusFrameCounts: [], localAffineSamples: [])
    }
    let uniqueVideos = uniqueMediaFilesByPath(videos)
    let frameAnalysisLabel = "Frame analysis"
    BenchOutput.write(progressBar(label: frameAnalysisLabel, done: 0, total: uniqueVideos.count))
    let precomputedFrameCounts = await extractVideoFrameCounts(videos: videos) { done, total in
        BenchOutput.write(progressBar(label: frameAnalysisLabel, done: done, total: total))
    }
    clearProgressBar()
    BenchOutput.line("  \(frameAnalysisLabel) complete: \(precomputedFrameCounts.count)/\(uniqueVideos.count) estimates")

    let calibrationLabel = "Local calibration"
    BenchOutput.write(progressBar(label: calibrationLabel, done: 0, total: uniqueVideos.count))
    let localAffineSamples = try await collectLocalVideoAffineSamples(
        videos: videos,
        preset: preset,
        timeout: timeout,
        frameCountByPath: precomputedFrameCounts
    ) { done, total in
        BenchOutput.write(progressBar(label: calibrationLabel, done: done, total: total))
    }
    clearProgressBar()
    BenchOutput.line("  \(calibrationLabel) complete: \(localAffineSamples.count)/\(uniqueVideos.count) samples")

    let jobsPerLevel = videos.count
    let levels = videoSweepLevels(videoEncodeEngines: hardware.videoEncodeEngines, totalCores: hardware.totalCores)
    var points: [ConcurrencySweepPoint] = []
    var aggregatedFrameCounts = precomputedFrameCounts

    BenchmarkRuntimeRenderer.printField(
        "Files",
        "\(videos.count) videos, \(jobsPerLevel) jobs per level"
    )
    BenchmarkRuntimeRenderer.printField(
        "Sweep levels",
        "\(levels.map(String.init).joined(separator: ", ")) (encode engines: \(hardware.videoEncodeEngines))"
    )
    BenchmarkRuntimeRenderer.printDetail("Sub-tasks run in parallel per job (matching production)")
    BenchOutput.line("")
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Concurrency", width: 11, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Videos/min", width: 10, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "P50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "P95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Peak mem", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Thermal", width: 8),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for (levelIndex, level) in levels.enumerated() {
        let progressLabel = "Level \(levelIndex + 1)/\(levels.count) · concurrency \(level)"
        BenchOutput.write(progressBar(label: progressLabel, done: 0, total: jobsPerLevel))
        let result = try await runVideoPipeline(
            videos: videos,
            jobCount: jobsPerLevel,
            maxConcurrent: level,
            preset: preset,
            timeout: timeout,
            frameCountByPath: precomputedFrameCounts
        ) { done, total in
            BenchOutput.write(progressBar(label: progressLabel, done: done, total: total))
        }
        aggregatedFrameCounts.merge(result.videoFrameCountsBySourcePath) { _, rhs in rhs }
        clearProgressBar()
        let perMin = Double(result.completed) / result.totalSeconds * 60
        let stats = Stats(result.latencies)
        BenchmarkRuntimeRenderer.printTableRow(
            [
                String(level),
                String(format: "%.1f", perMin),
                fmt(stats.p50),
                fmt(stats.p95),
                "\(result.peakMemoryMB)MB",
                result.thermalState,
            ],
            columns: columns
        )
        points.append(
            ConcurrencySweepPoint(
                concurrency: level,
                throughputPerMinute: perMin,
                p50Seconds: stats.p50,
                p95Seconds: stats.p95,
                peakMemoryMB: result.peakMemoryMB
            )
        )
    }

    let corpusFrameCounts = videos.compactMap { aggregatedFrameCounts[$0.path] }
    return VideoConcurrencySweepResult(
        points: points,
        corpusFrameCounts: corpusFrameCounts,
        localAffineSamples: localAffineSamples
    )
}

func benchmarkMixedRatioSweep(
    corpus: [MediaFile],
    hardware: HardwareProfile,
    preset: String = AVAssetExportPreset1920x1080,
    timeout: Int = 300
) async throws -> [MixedConcurrencySweepPoint] {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Mixed Workload Ratio Sweep")

    let images = corpus.filter { $0.type == .image }
    let videos = corpus.filter { $0.type == .video }
    guard !images.isEmpty, !videos.isEmpty else {
        BenchOutput.line("  Need both images and videos in media folder, skipping")
        return []
    }

    let imageJobs = max(images.count * 3, 24)
    let videoJobs = videos.count
    let imageLevels = mixedImageSweepLevels(totalCores: hardware.totalCores)
    let videoLevels = mixedVideoSweepLevels(videoEncodeEngines: hardware.videoEncodeEngines)
    var points: [MixedConcurrencySweepPoint] = []

    BenchmarkRuntimeRenderer.printField(
        "Jobs per combination",
        "\(imageJobs) image jobs + \(videoJobs) video jobs"
    )
    BenchmarkRuntimeRenderer.printField(
        "Sweep levels",
        "images [\(imageLevels.map(String.init).joined(separator: ", "))], videos [\(videoLevels.map(String.init).joined(separator: ", "))]"
    )
    BenchOutput.line("")
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Img", width: 3, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Vid", width: 3, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Assets/min", width: 10, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Img/min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Vid/min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Peak mem", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Thermal", width: 8),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for imgLevel in imageLevels {
        for vidLevel in videoLevels {
            let clock = ContinuousClock()
            let startTime = clock.now

            async let imgResult = runImagePipeline(images: images, jobCount: imageJobs, maxConcurrent: imgLevel)
            async let vidResult = runVideoPipeline(videos: videos, jobCount: videoJobs, maxConcurrent: vidLevel, preset: preset, timeout: timeout)

            let (ir, vr) = try await (imgResult, vidResult)

            let wallSeconds = max((clock.now - startTime).seconds, 0.001)
            let totalCompleted = ir.completed + vr.completed
            let assetsPerMin = Double(totalCompleted) / wallSeconds * 60
            let imgPerMin = Double(ir.completed) / wallSeconds * 60
            let vidPerMin = Double(vr.completed) / wallSeconds * 60
            let peakMem = max(ir.peakMemoryMB, vr.peakMemoryMB)

            BenchmarkRuntimeRenderer.printTableRow(
                [
                    String(imgLevel),
                    String(vidLevel),
                    String(format: "%.1f", assetsPerMin),
                    String(format: "%.1f", imgPerMin),
                    String(format: "%.1f", vidPerMin),
                    "\(peakMem)MB",
                    getThermalState(),
                ],
                columns: columns
            )
            points.append(
                MixedConcurrencySweepPoint(
                    imageConcurrency: imgLevel,
                    videoConcurrency: vidLevel,
                    throughputPerMinute: assetsPerMin,
                    peakMemoryMB: peakMem
                )
            )
        }
    }

    return points
}

@discardableResult
func printConcurrencyRecommendationCard(
    hardware: HardwareProfile,
    imageSweep: [ConcurrencySweepPoint],
    videoSweep: [ConcurrencySweepPoint],
    mixedSweep: [MixedConcurrencySweepPoint]
) -> (imageConcurrency: Int, videoConcurrency: Int)? {
    guard !imageSweep.isEmpty || !videoSweep.isEmpty || !mixedSweep.isEmpty else { return nil }

    let imagePick = pickKneePoint(imageSweep)
    let videoPick = pickKneePoint(videoSweep)
    let mixedPick = pickKneePoint(mixedSweep)

    let intFormatter = NumberFormatter()
    intFormatter.locale = Locale(identifier: "en_US_POSIX")
    intFormatter.numberStyle = .decimal

    func fmtInt(_ n: Int) -> String {
        intFormatter.string(from: NSNumber(value: n)) ?? "\(n)"
    }

    func fmtPerMin(_ v: Double) -> String {
        if v < 10 { return String(format: "%.1f", v) }
        if v < 100 { return String(format: "%.0f", v) }
        return fmtInt(Int(v.rounded()))
    }

    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printSubsectionTitle("Concurrency Recommendations")
    BenchmarkRuntimeRenderer.printField("System", hardware.summary)
    BenchOutput.line("")
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Workload", width: 16),
        BenchmarkRuntimeTableColumn(header: "Recommended", width: 16),
        BenchmarkRuntimeTableColumn(header: "Throughput", width: 12, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Peak Memory", width: 10, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    if let pick = imagePick {
        BenchmarkRuntimeRenderer.printTableRow(
            [
                "Images only",
                "\(pick.concurrency) concurrent",
                "\(fmtPerMin(pick.throughputPerMinute))/min",
                "~\(fmtInt(pick.peakMemoryMB))MB",
            ],
            columns: columns
        )
    }
    if let pick = videoPick {
        BenchmarkRuntimeRenderer.printTableRow(
            [
                "Videos only",
                "\(pick.concurrency) concurrent",
                "\(fmtPerMin(pick.throughputPerMinute))/min",
                "~\(fmtInt(pick.peakMemoryMB))MB",
            ],
            columns: columns
        )
    }
    if let pick = mixedPick {
        BenchmarkRuntimeRenderer.printTableRow(
            [
                "Mixed (default)",
                "\(pick.imageConcurrency) img + \(pick.videoConcurrency) vid",
                "\(fmtPerMin(pick.throughputPerMinute))/min",
                "~\(fmtInt(pick.peakMemoryMB))MB",
            ],
            columns: columns
        )
    }

    if let i = imagePick, let v = videoPick {
        BenchOutput.line("")
        BenchmarkRuntimeRenderer.printMetricItem("Suggested .env", "MAX_CONCURRENT_IMAGES=\(i.concurrency)")
        BenchOutput.line("    MAX_CONCURRENT_VIDEOS=\(v.concurrency)")
    }

    if let pick = mixedPick {
        return (pick.imageConcurrency, pick.videoConcurrency)
    }
    return nil
}

private func imageSweepLevels(totalCores: Int) -> [Int] {
    let cores = max(1, totalCores)
    let maxLevel = cores <= 4 ? cores : (cores + 2) // allow slight oversubscription on larger machines
    if maxLevel <= 4 { return Array(1...maxLevel) }

    let step = max(1, maxLevel / 4)
    var levels: [Int] = []
    var seen = Set<Int>()

    func add(_ v: Int) {
        guard v > 0, seen.insert(v).inserted else { return }
        levels.append(v)
    }

    add(1)
    for v in stride(from: step, through: maxLevel, by: step) {
        add(v)
    }
    add(maxLevel)

    return levels
}

private func videoSweepLevels(videoEncodeEngines: Int, totalCores: Int) -> [Int] {
    let engines = max(1, videoEncodeEngines)
    let cores = max(1, totalCores)
    let maxLevel = min(cores, engines * 2 + 1)
    return Array(1...maxLevel)
}

private func mixedImageSweepLevels(totalCores: Int) -> [Int] {
    let maxLevel = imageSweepLevels(totalCores: totalCores).last ?? 1
    let base = max(1, (maxLevel + 3) / 4) // ~25% of max, rounded up

    var levels: [Int] = []
    var seen = Set<Int>()
    func add(_ v: Int) {
        guard v > 0, seen.insert(v).inserted else { return }
        levels.append(v)
    }

    add(min(maxLevel, base))
    add(min(maxLevel, base + 1))
    add(min(maxLevel, base * 2))

    return levels.sorted()
}

private func mixedVideoSweepLevels(videoEncodeEngines: Int) -> [Int] {
    let engines = max(1, videoEncodeEngines)
    let second = max(2, engines)

    var levels: [Int] = []
    var seen = Set<Int>()
    func add(_ v: Int) {
        guard v > 0, seen.insert(v).inserted else { return }
        levels.append(v)
    }

    add(1)
    add(second)
    return levels.sorted()
}

private func extractVideoFrameCounts(
    videos: [MediaFile],
    onProgress: (@Sendable (Int, Int) -> Void)? = nil
) async -> [String: Double] {
    var frameCountsByPath: [String: Double] = [:]
    let uniqueVideos = uniqueMediaFilesByPath(videos)
    frameCountsByPath.reserveCapacity(uniqueVideos.count)
    onProgress?(0, uniqueVideos.count)

    for (index, video) in uniqueVideos.enumerated() {
        guard let probe = await VideoProcessor.probeRuntimeEstimate(sourcePath: video.path),
              probe.frameCount.isFinite,
              probe.frameCount > 0
        else {
            onProgress?(index + 1, uniqueVideos.count)
            continue
        }
        frameCountsByPath[video.path] = probe.frameCount
        onProgress?(index + 1, uniqueVideos.count)
    }
    return frameCountsByPath
}

func collectLocalVideoAffineSamples(
    videos: [MediaFile],
    preset: String = AVAssetExportPreset1920x1080,
    timeout: Int = 300,
    frameCountByPath: [String: Double] = [:],
    onProgress: (@Sendable (Int, Int) -> Void)? = nil
) async throws -> [LocalVideoAffineSample] {
    let uniqueVideos = uniqueMediaFilesByPath(videos)

    var resolvedFrameCounts = frameCountByPath
    var samples: [LocalVideoAffineSample] = []
    samples.reserveCapacity(uniqueVideos.count)
    onProgress?(0, uniqueVideos.count)

    for (index, video) in uniqueVideos.enumerated() {
        let result = try await runVideoPipeline(
            videos: [video],
            jobCount: 1,
            maxConcurrent: 1,
            preset: preset,
            timeout: timeout,
            frameCountByPath: resolvedFrameCounts
        )
        guard result.completed > 0 else {
            onProgress?(index + 1, uniqueVideos.count)
            continue
        }

        resolvedFrameCounts.merge(result.videoFrameCountsBySourcePath) { _, rhs in rhs }
        guard let frameCount = resolvedFrameCounts[video.path],
              frameCount.isFinite,
              frameCount > 0 else {
            onProgress?(index + 1, uniqueVideos.count)
            continue
        }

        let processMS = max(1, (result.latencies.first ?? result.totalSeconds) * 1_000.0)
        samples.append(
            LocalVideoAffineSample(
                sourcePath: video.path,
                frameCount: frameCount,
                processMS: processMS
            )
        )
        onProgress?(index + 1, uniqueVideos.count)
    }

    return samples
}

func pickKneePoint(_ points: [ConcurrencySweepPoint]) -> ConcurrencySweepPoint? {
    guard !points.isEmpty else { return nil }
    let peak = points.map(\.throughputPerMinute).max() ?? 0
    let threshold = peak * 0.98
    if let pick = points.filter({ $0.throughputPerMinute >= threshold }).min(by: { $0.concurrency < $1.concurrency }) {
        return pick
    }
    return points.max(by: { $0.throughputPerMinute < $1.throughputPerMinute })
}

private func pickKneePoint(_ points: [MixedConcurrencySweepPoint]) -> MixedConcurrencySweepPoint? {
    guard !points.isEmpty else { return nil }
    let peak = points.map(\.throughputPerMinute).max() ?? 0
    let threshold = peak * 0.98
    let candidates = points.filter { $0.throughputPerMinute >= threshold }
    guard !candidates.isEmpty else {
        return points.max(by: { $0.throughputPerMinute < $1.throughputPerMinute })
    }
    return candidates.min {
        let lhsLoad = $0.imageConcurrency + $0.videoConcurrency
        let rhsLoad = $1.imageConcurrency + $1.videoConcurrency
        if lhsLoad != rhsLoad { return lhsLoad < rhsLoad }
        if $0.imageConcurrency != $1.imageConcurrency { return $0.imageConcurrency < $1.imageConcurrency }
        if $0.videoConcurrency != $1.videoConcurrency { return $0.videoConcurrency < $1.videoConcurrency }
        return $0.throughputPerMinute > $1.throughputPerMinute
    }
}

// MARK: - Pipeline JSON Mode

private struct PipelineBenchmarkJSONPayload: Encodable {
    let imageConcurrency: Int
    let videoConcurrency: Int

    enum CodingKeys: String, CodingKey {
        case imageConcurrency = "image_concurrency"
        case videoConcurrency = "video_concurrency"
    }
}

private enum PipelineBenchmarkJSONError: Error, CustomStringConvertible {
    case noImages
    case noVideos
    case noImageKnee
    case noVideoKnee
    case encodingFailed

    var description: String {
        switch self {
        case .noImages: return "no image files in media folder"
        case .noVideos: return "no video files in media folder"
        case .noImageKnee: return "image sweep produced no recommendation"
        case .noVideoKnee: return "video sweep produced no recommendation"
        case .encodingFailed: return "could not encode JSON payload"
        }
    }
}

func benchmarkPipelineJSON(
    corpus: [MediaFile],
    hardware: HardwareProfile,
    preset: String = AVAssetExportPreset1920x1080,
    timeout: Int = 300
) async throws -> String {
    let images = corpus.filter { $0.type == .image }
    guard !images.isEmpty else { throw PipelineBenchmarkJSONError.noImages }

    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else { throw PipelineBenchmarkJSONError.noVideos }

    let imageSweep = try await benchmarkImageConcurrency(corpus: corpus, hardware: hardware)
    guard let imagePick = pickKneePoint(imageSweep) else {
        throw PipelineBenchmarkJSONError.noImageKnee
    }

    let videoSweepResult = try await benchmarkVideoConcurrency(
        corpus: corpus,
        hardware: hardware,
        preset: preset,
        timeout: timeout
    )
    guard let videoPick = pickKneePoint(videoSweepResult.points) else {
        throw PipelineBenchmarkJSONError.noVideoKnee
    }

    let payload = PipelineBenchmarkJSONPayload(
        imageConcurrency: imagePick.concurrency,
        videoConcurrency: videoPick.concurrency
    )

    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let data = try encoder.encode(payload)
    guard let json = String(data: data, encoding: .utf8) else {
        throw PipelineBenchmarkJSONError.encodingFailed
    }
    return json
}
