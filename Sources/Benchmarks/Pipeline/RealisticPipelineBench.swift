import Foundation
import AVFoundation
import KikoMediaCore

private actor RealisticPipelineProgressReporter {
    enum Workload {
        case image
        case video
    }

    private let imageTotal: Int
    private let videoTotal: Int
    private let transientEnabled: Bool
    private let transientWriter: @Sendable (String) -> Void
    private let clock = ContinuousClock()

    private var imageCompleted = 0
    private var videoCompleted = 0
    private var failed = 0
    private var startedAt: ContinuousClock.Instant?
    private var refreshTask: Task<Void, Never>?
    private var finished = false

    init(
        imageTotal: Int,
        videoTotal: Int,
        transientEnabled: Bool = BenchOutput.supportsTerminalOnlyWrites(),
        transientWriter: @escaping @Sendable (String) -> Void = BenchOutput.writeTerminalOnly
    ) {
        self.imageTotal = imageTotal
        self.videoTotal = videoTotal
        self.transientEnabled = transientEnabled
        self.transientWriter = transientWriter
    }

    func start() {
        guard transientEnabled, !finished else { return }
        startedAt = clock.now
        render(now: clock.now)
        refreshTask = Task { [weak self] in
            while let self {
                do {
                    try await Task.sleep(for: .seconds(1))
                } catch {
                    break
                }
                if Task.isCancelled {
                    break
                }
                await self.refresh()
            }
        }
    }

    func record(_ workload: Workload, succeeded: Bool) {
        guard transientEnabled, !finished else { return }
        switch workload {
        case .image:
            imageCompleted += 1
        case .video:
            videoCompleted += 1
        }
        if !succeeded {
            failed += 1
        }
        render(now: clock.now)
    }

    func finish() {
        refreshTask?.cancel()
        refreshTask = nil
        guard transientEnabled, !finished else {
            finished = true
            return
        }
        finished = true
        clear()
    }

    private func refresh() {
        guard transientEnabled, !finished else { return }
        render(now: clock.now)
    }

    private func render(now: ContinuousClock.Instant) {
        transientWriter("\r\u{1B}[2K")
        transientWriter(statusLine(now: now))
    }

    private func clear() {
        transientWriter("\r\u{1B}[2K")
    }

    private func statusLine(now: ContinuousClock.Instant) -> String {
        let totalCompleted = imageCompleted + videoCompleted
        let totalJobs = imageTotal + videoTotal
        let elapsed = if let startedAt {
            formatElapsed(max(0, (now - startedAt).seconds))
        } else {
            "00:00"
        }
        return "  Running: images \(imageCompleted)/\(imageTotal) · videos \(videoCompleted)/\(videoTotal) · total \(totalCompleted)/\(totalJobs) · \(failed) failed · \(elapsed)"
    }

    private func formatElapsed(_ seconds: Double) -> String {
        let totalSeconds = max(0, Int(seconds.rounded(.down)))
        let hours = totalSeconds / 3_600
        let minutes = (totalSeconds % 3_600) / 60
        let remainingSeconds = totalSeconds % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, remainingSeconds)
        }
        return String(format: "%02d:%02d", minutes, remainingSeconds)
    }
}

func benchmarkRealisticPipeline(
    corpus: [MediaFile],
    imageConcurrency: Int,
    videoConcurrency: Int,
    ssdPath: String? = nil,
    keepArtifacts: Bool = false,
    preset: String = AVAssetExportPreset1920x1080,
    timeout: Int = 300
) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Realistic Pipeline (DB + Processing + SHA256 + Archive)")

    let images = corpus.filter { $0.type == .image }
    let videos = corpus.filter { $0.type == .video }
    guard !images.isEmpty else {
        BenchOutput.line("  Need image files in media folder, skipping")
        return
    }

    let benchmarkPaths: SSDBenchmarkArtifactsPath? = {
        guard let ssdPath, !ssdPath.isEmpty else { return nil }
        return makeSSDBenchmarkArtifactsPath(ssdBase: ssdPath, leaf: "realistic-pipeline")
    }()

    let storage: StorageManager? = benchmarkPaths.map {
        StorageManager(externalSSDPath: $0.benchDir, sha256BufferSize: BenchDefaults.sha256BufferSize)
    }

    defer {
        if let benchmarkPaths, !keepArtifacts {
            cleanupSSDBenchmarkArtifacts(benchmarkPaths)
        }
    }

    let tmpDir = makeTempDir("realistic")
    let thumbsDir = "\(tmpDir)/thumbs"
    let previewsDir = "\(tmpDir)/previews"
    try FileManager.default.createDirectory(atPath: thumbsDir, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(atPath: previewsDir, withIntermediateDirectories: true)
    let db = try makeBenchmarkDatabase(path: "\(tmpDir)/bench.db")
    defer { cleanup(tmpDir) }

    let imageJobs = max(images.count * 3, 24)
    let videoJobs = videos.count
    let imgConcurrency = imageConcurrency
    let vidConcurrency = videoConcurrency

    BenchOutput.line("  \(imageJobs) image jobs (max \(imgConcurrency)) + \(videoJobs) video jobs (max \(vidConcurrency))")
    if storage != nil {
        BenchOutput.line("  Each job: DB write → parallel processing → SHA256 → SSD archive → DB write")
    } else {
        BenchOutput.line("  Each job: DB write → parallel processing → SHA256 → DB write (archive skipped: no --ssd-path)")
    }
    BenchOutput.line("")
    let progressReporter = RealisticPipelineProgressReporter(
        imageTotal: imageJobs,
        videoTotal: videoJobs
    )
    await progressReporter.start()

    let clock = ContinuousClock()
    let startTime = clock.now

    let ir: PipelineResult
    let vr: PipelineResult

    do {
        if !videos.isEmpty {
            async let imgR = runRealisticImagePipeline(
                images: images, jobCount: imageJobs, maxConcurrent: imgConcurrency,
                db: db, thumbsDir: thumbsDir, previewsDir: previewsDir,
                storage: storage,
                progressReporter: progressReporter
            )
            async let vidR = runRealisticVideoPipeline(
                videos: videos, jobCount: videoJobs, maxConcurrent: vidConcurrency,
                db: db, thumbsDir: thumbsDir, previewsDir: previewsDir,
                storage: storage,
                preset: preset, timeout: timeout,
                progressReporter: progressReporter
            )
            (ir, vr) = try await (imgR, vidR)
        } else {
            ir = try await runRealisticImagePipeline(
                images: images, jobCount: imageJobs, maxConcurrent: imgConcurrency,
                db: db, thumbsDir: thumbsDir, previewsDir: previewsDir,
                storage: storage,
                progressReporter: progressReporter
            )
            vr = PipelineResult(completed: 0, failed: 0, totalSeconds: 0, latencies: [], peakMemoryMB: 0, thermalState: "nominal")
        }
    } catch {
        await progressReporter.finish()
        throw error
    }
    await progressReporter.finish()

    let elapsed = (clock.now - startTime).seconds
    let totalCompleted = ir.completed + vr.completed
    let totalJobs = imageJobs + videoJobs
    let peakMem = max(ir.peakMemoryMB, vr.peakMemoryMB)

    printRow("Total time", fmt(elapsed))
    printRow("Completed", "\(totalCompleted) / \(totalJobs)")
    printRow("Failed", "\(ir.failed + vr.failed)")
    printRow("Throughput", "\(String(format: "%.1f", Double(totalCompleted) / elapsed * 60)) assets/min")
    printRow("Peak memory", "\(peakMem)MB")
    printRow("Thermal state", getThermalState())
}

private func runRealisticImagePipeline(
    images: [MediaFile], jobCount: Int, maxConcurrent: Int,
    db: Database, thumbsDir: String, previewsDir: String,
    storage: StorageManager?,
    progressReporter: RealisticPipelineProgressReporter?
) async throws -> PipelineResult {
    let processJob: @Sendable (MediaFile, Int, String, String, Database, StorageManager?) async -> (Double, Bool) = { file, idx, tDir, pDir, db, storage in
        let jc = ContinuousClock()
        let js = jc.now
        func finish(_ succeeded: Bool) async -> (Double, Bool) {
            await progressReporter?.record(.image, succeeded: succeeded)
            return ((jc.now - js).seconds, succeeded)
        }
        do {
            let id = "img-\(String(format: "%06d", idx))"

            _ = try await db.insertQueued(id: id, type: .image, originalName: file.name)
            try await db.updateStatus(id: id, status: .processing)

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

            async let timestampResult: String? = {
                ImageProcessor.extractTimestamp(sourcePath: file.path)
            }()

            let (thumbOK, previewOK, timestamp) = await (thumbSuccess, previewSuccess, timestampResult)
            guard thumbOK, previewOK else {
                return await finish(false)
            }

            _ = try SHA256Utility.calculateSHA256(
                path: file.path,
                bufferSize: BenchDefaults.sha256BufferSize
            )

            if let storage {
                let result = await storage.archiveOriginal(sourcePath: file.path, assetId: id, originalName: file.name)
                guard result.isSafelyStored else {
                    try await db.updateStatus(id: id, status: .failed)
                    return await finish(false)
                }
            }

            try await db.markComplete(id: id, timestamp: timestamp ?? "")

            return await finish(true)
        } catch {
            return await finish(false)
        }
    }

    return try await runPipelineLoop(jobCount: jobCount, maxConcurrent: maxConcurrent) { idx in
        let file = images[idx % images.count]
        return await processJob(file, idx, thumbsDir, previewsDir, db, storage)
    }
}

private func runRealisticVideoPipeline(
    videos: [MediaFile], jobCount: Int, maxConcurrent: Int,
    db: Database, thumbsDir: String, previewsDir: String,
    storage: StorageManager?,
    preset: String, timeout: Int,
    progressReporter: RealisticPipelineProgressReporter?
) async throws -> PipelineResult {
    let processJob: @Sendable (MediaFile, Int, String, String, Database, StorageManager?) async -> (Double, Bool) = { file, idx, tDir, pDir, db, storage in
        let jc = ContinuousClock()
        let js = jc.now
        func finish(_ succeeded: Bool) async -> (Double, Bool) {
            await progressReporter?.record(.video, succeeded: succeeded)
            return ((jc.now - js).seconds, succeeded)
        }
        do {
            let id = "vid-\(String(format: "%06d", idx))"

            _ = try await db.insertQueued(id: id, type: .video, originalName: file.name)
            try await db.updateStatus(id: id, status: .processing)

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
            guard thumbOK, previewOK else {
                return await finish(false)
            }

            _ = try SHA256Utility.calculateSHA256(
                path: file.path,
                bufferSize: BenchDefaults.sha256BufferSize
            )

            if let storage {
                let result = await storage.archiveOriginal(sourcePath: file.path, assetId: id, originalName: file.name)
                guard result.isSafelyStored else {
                    try await db.updateStatus(id: id, status: .failed)
                    return await finish(false)
                }
            }

            try await db.markComplete(id: id, timestamp: "")

            return await finish(true)
        } catch {
            return await finish(false)
        }
    }

    return try await runPipelineLoop(jobCount: jobCount, maxConcurrent: maxConcurrent) { idx in
        let file = videos[idx % videos.count]
        return await processJob(file, idx, thumbsDir, previewsDir, db, storage)
    }
}
