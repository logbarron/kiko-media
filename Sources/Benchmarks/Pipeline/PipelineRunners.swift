import Foundation
import KikoMediaCore

struct PipelineResult: Sendable {
    let completed: Int
    let failed: Int
    let totalSeconds: Double
    let latencies: [Double]
    let peakMemoryMB: Int
    let thermalState: String
    let videoFrameCountsBySourcePath: [String: Double]

    init(
        completed: Int,
        failed: Int,
        totalSeconds: Double,
        latencies: [Double],
        peakMemoryMB: Int,
        thermalState: String,
        videoFrameCountsBySourcePath: [String: Double] = [:]
    ) {
        self.completed = completed
        self.failed = failed
        self.totalSeconds = totalSeconds
        self.latencies = latencies
        self.peakMemoryMB = peakMemoryMB
        self.thermalState = thermalState
        self.videoFrameCountsBySourcePath = videoFrameCountsBySourcePath
    }
}

private struct PipelineJobOutcome: Sendable {
    let elapsedSeconds: Double
    let success: Bool
    let sourcePath: String?
    let frameCount: Double?
}

func runImagePipeline(images: [MediaFile], jobCount: Int, maxConcurrent: Int, onProgress: ((Int, Int) -> Void)? = nil) async throws -> PipelineResult {
    let tmpDir = makeTempDir("pipeline-img")
    let thumbsDir = "\(tmpDir)/thumbs"
    let previewsDir = "\(tmpDir)/previews"
    try FileManager.default.createDirectory(atPath: thumbsDir, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(atPath: previewsDir, withIntermediateDirectories: true)
    defer { cleanup(tmpDir) }

    let processJob: @Sendable (MediaFile, Int, String, String) async -> PipelineJobOutcome = { file, idx, tDir, pDir in
        let jc = ContinuousClock()
        let js = jc.now
        let id = String(format: "%06d", idx)

        // Match production: structured concurrency with per-subtask error isolation.
        async let thumbSuccess: Bool = {
            do {
                try ImageProcessor.generateThumbnail(
                    sourcePath: file.path,
                    outputPath: "\(tDir)/\(id).jpg",
                    size: 512,
                    quality: 0.85,
                    maxPixels: BenchDefaults.maxImagePixels,
                    maxDimension: BenchDefaults.maxImageDimension,
                    maxCompressionRatio: BenchDefaults.maxCompressionRatio
                )
                return true
            } catch {
                return false
            }
        }()

        async let previewSuccess: Bool = {
            do {
                try ImageProcessor.generatePreview(
                    sourcePath: file.path,
                    outputPath: "\(pDir)/\(id).jpg",
                    size: 1440,
                    quality: 0.90,
                    maxPixels: BenchDefaults.maxImagePixels,
                    maxDimension: BenchDefaults.maxImageDimension,
                    maxCompressionRatio: BenchDefaults.maxCompressionRatio
                )
                return true
            } catch {
                return false
            }
        }()

        async let ts: String? = {
            ImageProcessor.extractTimestamp(sourcePath: file.path)
        }()

        let (thumbOK, previewOK, _) = await (thumbSuccess, previewSuccess, ts)
        return PipelineJobOutcome(
            elapsedSeconds: (jc.now - js).seconds,
            success: thumbOK && previewOK,
            sourcePath: nil,
            frameCount: nil
        )
    }

    return try await runPipelineLoopWithMetadata(jobCount: jobCount, maxConcurrent: maxConcurrent, onProgress: onProgress) { idx in
        let file = images[idx % images.count]
        return await processJob(file, idx, thumbsDir, previewsDir)
    }
}

func runVideoPipeline(
    videos: [MediaFile],
    jobCount: Int,
    maxConcurrent: Int,
    preset: String,
    timeout: Int,
    frameCountByPath: [String: Double] = [:],
    onProgress: ((Int, Int) -> Void)? = nil
) async throws -> PipelineResult {
    let tmpDir = makeTempDir("pipeline-vid")
    let thumbsDir = "\(tmpDir)/thumbs"
    let previewsDir = "\(tmpDir)/previews"
    try FileManager.default.createDirectory(atPath: thumbsDir, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(atPath: previewsDir, withIntermediateDirectories: true)
    defer { cleanup(tmpDir) }

    let processJob: @Sendable (MediaFile, Int, String, String) async -> PipelineJobOutcome = { file, idx, tDir, pDir in
        let jc = ContinuousClock()
        let js = jc.now
        let id = String(format: "%06d", idx)

        // Match production: structured concurrency with per-subtask error isolation.
        async let thumbSuccess: Bool = {
            do {
                try await VideoProcessor.generateThumbnail(
                    sourcePath: file.path,
                    outputPath: "\(tDir)/\(id).jpg",
                    size: 512,
                    time: 1.0,
                    quality: 0.85
                )
                return true
            } catch {
                return false
            }
        }()

        async let previewSuccess: Bool = {
            do {
                try await VideoProcessor.transcode(
                    sourcePath: file.path,
                    outputPath: "\(pDir)/\(id).mp4",
                    timeoutSeconds: timeout,
                    preset: preset
                )
                return true
            } catch {
                return false
            }
        }()

        let (thumbOK, previewOK) = await (thumbSuccess, previewSuccess)
        return PipelineJobOutcome(
            elapsedSeconds: (jc.now - js).seconds,
            success: thumbOK && previewOK,
            sourcePath: file.path,
            frameCount: frameCountByPath[file.path]
        )
    }

    return try await runPipelineLoopWithMetadata(jobCount: jobCount, maxConcurrent: maxConcurrent, onProgress: onProgress) { idx in
        let file = videos[idx % videos.count]
        return await processJob(file, idx, thumbsDir, previewsDir)
    }
}

func runPipelineLoop(
    jobCount: Int,
    maxConcurrent: Int,
    onProgress: ((Int, Int) -> Void)? = nil,
    processJob: @escaping @Sendable (Int) async -> (Double, Bool)
) async throws -> PipelineResult {
    try await runPipelineLoopWithMetadata(
        jobCount: jobCount,
        maxConcurrent: maxConcurrent,
        onProgress: onProgress
    ) { idx in
        let result = await processJob(idx)
        return PipelineJobOutcome(
            elapsedSeconds: result.0,
            success: result.1,
            sourcePath: nil,
            frameCount: nil
        )
    }
}

private func runPipelineLoopWithMetadata(
    jobCount: Int,
    maxConcurrent: Int,
    onProgress: ((Int, Int) -> Void)? = nil,
    processJob: @escaping @Sendable (Int) async -> PipelineJobOutcome
) async throws -> PipelineResult {
    let clock = ContinuousClock()
    let startTime = clock.now
    var peakMemory = getMemoryMB()
    var latencies: [Double] = []
    var videoFrameCountsBySourcePath: [String: Double] = [:]
    var completed = 0
    var failed = 0

    try await withThrowingTaskGroup(of: PipelineJobOutcome.self) { group in
        var nextJob = 0

        while nextJob < min(maxConcurrent, jobCount) {
            let idx = nextJob
            nextJob += 1
            group.addTask { await processJob(idx) }
        }

        for try await result in group {
            if let sourcePath = result.sourcePath,
               let frameCount = result.frameCount,
               frameCount.isFinite,
               frameCount > 0 {
                videoFrameCountsBySourcePath[sourcePath] = frameCount
            }
            let success = result.success
            if success { completed += 1 } else { failed += 1 }
            latencies.append(result.elapsedSeconds)
            try BenchmarkMemoryGuard.checkpoint(
                stage: "pipeline",
                detail: "job \(completed + failed)/\(jobCount)"
            )
            let mem = getMemoryMB()
            if mem > peakMemory { peakMemory = mem }
            onProgress?(completed + failed, jobCount)

            if nextJob < jobCount {
                let idx = nextJob
                nextJob += 1
                group.addTask { await processJob(idx) }
            }
        }
    }

    let totalTime = (clock.now - startTime).seconds
    return PipelineResult(
        completed: completed,
        failed: failed,
        totalSeconds: totalTime,
        latencies: latencies,
        peakMemoryMB: peakMemory,
        thermalState: getThermalState(),
        videoFrameCountsBySourcePath: videoFrameCountsBySourcePath
    )
}
