import Foundation
import AVFoundation
import KikoMediaCore

// Pipeline walkthrough is a narrative, single-asset breakdown.
// It must preserve production semantics: parallel work is parallel, and totals are wall time.
enum PipelineWalkthrough {
    static func printIfEnabled(plan: BenchmarkPlan, media: [MediaFile]) async {
        guard shouldPrint(plan: plan), !media.isEmpty else { return }

        BenchOutput.line("## Upload Pipeline: Per-Step Breakdown")
        BenchOutput.line("")
        BenchOutput.line("Single-asset timing breakdown. Parallel steps show wall time plus sub-steps.")

        let sampleImage = media.first(where: { $0.type == .image })
        let sampleVideo = media.first(where: { $0.type == .video })

        if let sampleImage {
            BenchOutput.line("")
            await printImageWalkthrough(plan: plan, file: sampleImage)
        } else {
            BenchOutput.line("")
            BenchOutput.line("No image files in media folder, skipping image walkthrough.")
        }

        if let sampleVideo {
            BenchOutput.line("")
            await printVideoWalkthrough(plan: plan, file: sampleVideo)
        } else {
            BenchOutput.line("")
            BenchOutput.line("No video files in media folder, skipping video walkthrough.")
        }

        BenchOutput.line("")
        BenchOutput.line("Note: This breakdown is for one asset. Throughput and contention are measured in the pipeline stages.")
    }

    private static func shouldPrint(plan: BenchmarkPlan) -> Bool {
        if plan.runLimitFinder { return false }
        // Default/full runs, plus the pipeline stage, benefit from the narrative output.
        if plan.components.count > 1 { return true }
        return plan.components.first == .pipeline
    }

    // MARK: - Image

    private static func printImageWalkthrough(plan: BenchmarkPlan, file: MediaFile) async {
        let ext = fileExtensionUppercased(path: file.path)
        let size = formatByteSize(file.sizeBytes)
        let dims = (try? ImageProcessor.validateDimensions(
            sourcePath: file.path,
            maxPixels: BenchDefaults.maxImagePixels,
            maxDimension: BenchDefaults.maxImageDimension,
            maxCompressionRatio: BenchDefaults.maxCompressionRatio
        )).map { "\($0.width)x\($0.height)" }
        let dimPart = dims.map { ", \($0) \(ext)" } ?? ""
        BenchOutput.line("Sample image: \(file.name) (\(size)\(dimPart))")
        BenchOutput.line("")

        printWalkthroughHeader()

        let detectStats = measureSync(iterations: 50) {
            _ = ImageProcessor.isImage(path: file.path)
        }
        printWalkthroughRow(
            step: "Media type detection",
            p50: detectStats.p50,
            p95: detectStats.p95,
            notes: "CGImageSource probe"
        )

        let processing = await measureParallelImageProcessing(file: file, iterations: 5)
        printWalkthroughRow(
            step: "Parallel processing (wall)",
            p50: processing.wall.p50,
            p95: processing.wall.p95,
            notes: "wall ~= max(thumb, preview, timestamp)"
        )
        printWalkthroughRow(
            step: "  - Thumbnail (512px JPEG)",
            p50: processing.thumb.p50,
            p95: processing.thumb.p95,
            notes: "subsample x\(processing.thumbSubsample)"
        )
        printWalkthroughRow(
            step: "  - Preview (1440px JPEG)",
            p50: processing.preview.p50,
            p95: processing.preview.p95,
            notes: "subsample x\(processing.previewSubsample)"
        )
        printWalkthroughRow(
            step: "  - Timestamp (EXIF/TIFF)",
            p50: processing.timestamp.p50,
            p95: processing.timestamp.p95,
            notes: processing.timestampFound ? "found" : "missing"
        )

        let archiveStats = await maybeMeasureArchive(plan: plan, file: file)
        if let archiveStats {
            printWalkthroughRow(
                step: "Archive to SSD",
                p50: archiveStats.p50,
                p95: archiveStats.p95,
                notes: "copy + SHA256 verify (hashing included)"
            )
        } else {
            printWalkthroughRow(
                step: "Archive to SSD",
                p50: nil,
                p95: nil,
                notes: "skipped (no external SSD folder)"
            )
        }

        let dbStats = await measureDBInsertUpdate(iterations: 50, file: file, kind: "image")
        printWalkthroughRow(
            step: "DB insert + update",
            p50: dbStats.p50,
            p95: dbStats.p95,
            notes: "queued + complete"
        )

        printWalkthroughFooter()

        let bottleneck = maxStepName(
            [("thumbnail", processing.thumb.p50), ("preview", processing.preview.p50), ("timestamp", processing.timestamp.p50)]
        )
        let approx = detectStats.p50 + processing.wall.p50 + (archiveStats?.p50 ?? 0) + dbStats.p50
        let suffix = archiveStats == nil ? " (archive skipped)" : ""
        BenchOutput.line("  Approx image wall time\(suffix)".padding(toWidth: 28) + " " + fmt(approx).padding(toWidth: 11) + " " + "".padding(toWidth: 11) + " bottleneck: \(bottleneck)")
    }

    private struct ParallelImageProcessingStats: Sendable {
        let wall: Stats
        let thumb: Stats
        let preview: Stats
        let timestamp: Stats
        let thumbSubsample: Int
        let previewSubsample: Int
        let timestampFound: Bool
    }

    private static func measureParallelImageProcessing(file: MediaFile, iterations: Int) async -> ParallelImageProcessingStats {
        let dims = (try? ImageProcessor.validateDimensions(
            sourcePath: file.path,
            maxPixels: BenchDefaults.maxImagePixels,
            maxDimension: BenchDefaults.maxImageDimension,
            maxCompressionRatio: BenchDefaults.maxCompressionRatio
        )) ?? (width: 0, height: 0)
        let sourceMax = max(dims.width, dims.height)
        let thumbSub: Int = {
            let ratio = sourceMax / 512
            if ratio >= 8 { return 8 }
            if ratio >= 4 { return 4 }
            if ratio >= 2 { return 2 }
            return 1
        }()
        let prevSub: Int = {
            let ratio = sourceMax / 1440
            if ratio >= 8 { return 8 }
            if ratio >= 4 { return 4 }
            if ratio >= 2 { return 2 }
            return 1
        }()

        let tmpDir = makeTempDir("walkthrough-img")
        defer { cleanup(tmpDir) }

        var wall: [Double] = []
        var thumb: [Double] = []
        var preview: [Double] = []
        var ts: [Double] = []
        wall.reserveCapacity(iterations)
        thumb.reserveCapacity(iterations)
        preview.reserveCapacity(iterations)
        ts.reserveCapacity(iterations)

        var foundAnyTimestamp = false

        for i in 0..<iterations {
            let clock = ContinuousClock()
            let startWall = clock.now

            async let thumbSeconds: Double = {
                let out = "\(tmpDir)/thumb-\(i).jpg"
                let d = try measure {
                    try ImageProcessor.generateThumbnail(
                        sourcePath: file.path,
                        outputPath: out,
                        size: 512,
                        quality: 0.85,
                        maxPixels: BenchDefaults.maxImagePixels,
                        maxDimension: BenchDefaults.maxImageDimension,
                        maxCompressionRatio: BenchDefaults.maxCompressionRatio
                    )
                }
                return d.seconds
            }()

            async let previewSeconds: Double = {
                let out = "\(tmpDir)/preview-\(i).jpg"
                let d = try measure {
                    try ImageProcessor.generatePreview(
                        sourcePath: file.path,
                        outputPath: out,
                        size: 1440,
                        quality: 0.90,
                        maxPixels: BenchDefaults.maxImagePixels,
                        maxDimension: BenchDefaults.maxImageDimension,
                        maxCompressionRatio: BenchDefaults.maxCompressionRatio
                    )
                }
                return d.seconds
            }()

            async let timestampResult: (Double, Bool) = {
                var found = false
                let d = measure {
                    found = ImageProcessor.extractTimestamp(sourcePath: file.path) != nil
                }
                return (d.seconds, found)
            }()

            do {
                let (t, p, tr) = try await (thumbSeconds, previewSeconds, timestampResult)
                let (s, found) = tr
                if found { foundAnyTimestamp = true }
                let w = (clock.now - startWall).seconds
                wall.append(w)
                thumb.append(t)
                preview.append(p)
                ts.append(s)
            } catch {
                // Best-effort narrative: stop collecting and return what we have.
                break
            }
        }

        return ParallelImageProcessingStats(
            wall: Stats(wall),
            thumb: Stats(thumb),
            preview: Stats(preview),
            timestamp: Stats(ts),
            thumbSubsample: thumbSub,
            previewSubsample: prevSub,
            timestampFound: foundAnyTimestamp
        )
    }

    // MARK: - Video

    private static func printVideoWalkthrough(plan: BenchmarkPlan, file: MediaFile) async {
        let size = formatByteSize(file.sizeBytes)
        let info = try? await getVideoInfo(path: file.path)
        let infoPart: String = {
            guard let info else { return "" }
            return ", \(info.codec) \(info.width)x\(info.height) \(String(format: "%.0f", info.fps))fps \(String(format: "%.0f", info.duration))s"
        }()
        BenchOutput.line("Sample video: \(file.name) (\(size)\(infoPart))")
        BenchOutput.line("")

        printWalkthroughHeader()

        let detectStats = await measureAsyncStats(iterations: 10) {
            _ = await VideoProcessor.isVideo(sourcePath: file.path)
        }
        printWalkthroughRow(
            step: "Media type detection",
            p50: detectStats.p50,
            p95: detectStats.p95,
            notes: "AVURLAsset probe"
        )

        let processing = await measureParallelVideoProcessing(file: file, iterations: 1, preset: plan.videoPreset, timeout: plan.videoTimeoutSeconds)
        printWalkthroughRow(
            step: "Parallel processing (wall)",
            p50: processing.wall.p50,
            p95: processing.wall.p95,
            notes: "wall ~= max(thumb, transcode, timestamp)"
        )
        printWalkthroughRow(
            step: "  - Video thumbnail (512px)",
            p50: processing.thumb.p50,
            p95: processing.thumb.p95,
            notes: "frame at t=1.0s"
        )
        let transcodeNote: String = {
            if let avgOutMB = processing.avgTranscodeOutMB, let ratio = processing.avgTranscodeOutPercent {
                return "\(avgOutMB)MB (\(ratio)% of input)"
            }
            return "output size unavailable"
        }()
        printWalkthroughRow(
            step: "  - Transcode (selected preset)",
            p50: processing.transcode.p50,
            p95: processing.transcode.p95,
            notes: transcodeNote
        )
        printWalkthroughRow(
            step: "  - Timestamp (metadata)",
            p50: processing.timestamp.p50,
            p95: processing.timestamp.p95,
            notes: processing.timestampFound ? "found" : "missing"
        )

        let archiveStats = await maybeMeasureArchive(plan: plan, file: file)
        if let archiveStats {
            printWalkthroughRow(
                step: "Archive to SSD",
                p50: archiveStats.p50,
                p95: archiveStats.p95,
                notes: "copy + SHA256 verify (hashing included)"
            )
        } else {
            printWalkthroughRow(
                step: "Archive to SSD",
                p50: nil,
                p95: nil,
                notes: "skipped (no external SSD folder)"
            )
        }

        let dbStats = await measureDBInsertUpdate(iterations: 50, file: file, kind: "video")
        printWalkthroughRow(
            step: "DB insert + update",
            p50: dbStats.p50,
            p95: dbStats.p95,
            notes: "queued + complete"
        )

        printWalkthroughFooter()

        let bottleneck = maxStepName(
            [("thumb", processing.thumb.p50), ("transcode", processing.transcode.p50), ("timestamp", processing.timestamp.p50)]
        )
        let approx = detectStats.p50 + processing.wall.p50 + (archiveStats?.p50 ?? 0) + dbStats.p50
        let suffix = archiveStats == nil ? " (archive skipped)" : ""
        BenchOutput.line("  Approx video wall time\(suffix)".padding(toWidth: 28) + " " + fmt(approx).padding(toWidth: 11) + " " + "".padding(toWidth: 11) + " bottleneck: \(bottleneck)")
    }

    private struct ParallelVideoProcessingStats: Sendable {
        let wall: Stats
        let thumb: Stats
        let transcode: Stats
        let timestamp: Stats
        let timestampFound: Bool
        let avgTranscodeOutMB: Int?
        let avgTranscodeOutPercent: Int?
    }

    private static func measureParallelVideoProcessing(
        file: MediaFile,
        iterations: Int,
        preset: String,
        timeout: Int
    ) async -> ParallelVideoProcessingStats {
        let tmpDir = makeTempDir("walkthrough-vid")
        defer { cleanup(tmpDir) }

        var wall: [Double] = []
        var thumb: [Double] = []
        var transcode: [Double] = []
        var ts: [Double] = []
        wall.reserveCapacity(iterations)
        thumb.reserveCapacity(iterations)
        transcode.reserveCapacity(iterations)
        ts.reserveCapacity(iterations)

        var outSizes: [Int] = []
        outSizes.reserveCapacity(iterations)

        var foundAnyTimestamp = false

        for i in 0..<iterations {
            let clock = ContinuousClock()
            let startWall = clock.now

            async let thumbSeconds: Double = {
                let out = "\(tmpDir)/thumb-\(i).jpg"
                let d = try await measureAsync {
                    try await VideoProcessor.generateThumbnail(
                        sourcePath: file.path,
                        outputPath: out,
                        size: 512,
                        time: 1.0,
                        quality: 0.85
                    )
                }
                return d.seconds
            }()

            async let transcodeResult: (Double, Int?) = {
                let out = "\(tmpDir)/transcode-\(i).mp4"
                let d = try await measureAsync {
                    try await VideoProcessor.transcode(
                        sourcePath: file.path,
                        outputPath: out,
                        timeoutSeconds: timeout,
                        preset: preset
                    )
                }
                let outSize: Int? = (try? FileManager.default.attributesOfItem(atPath: out))?[.size] as? Int
                return (d.seconds, outSize)
            }()

            async let timestampResult: (Double, Bool) = {
                var found = false
                let d = await measureAsync {
                    found = (await VideoProcessor.extractTimestamp(sourcePath: file.path) != nil)
                }
                return (d.seconds, found)
            }()

            do {
                let (t, xr, tr) = try await (thumbSeconds, transcodeResult, timestampResult)
                let (x, outSize) = xr
                let (s, found) = tr
                if let outSize { outSizes.append(outSize) }
                if found { foundAnyTimestamp = true }
                let w = (clock.now - startWall).seconds
                wall.append(w)
                thumb.append(t)
                transcode.append(x)
                ts.append(s)
            } catch {
                break
            }
        }

        let avgOutMB: Int? = {
            guard !outSizes.isEmpty else { return nil }
            return outSizes.reduce(0, +) / outSizes.count / (1024 * 1024)
        }()

        let avgOutPercent: Int? = {
            guard !outSizes.isEmpty, file.sizeBytes > 0 else { return nil }
            let avgOut = outSizes.reduce(0, +) / outSizes.count
            return Int((Double(avgOut) / Double(file.sizeBytes)) * 100.0)
        }()

        return ParallelVideoProcessingStats(
            wall: Stats(wall),
            thumb: Stats(thumb),
            transcode: Stats(transcode),
            timestamp: Stats(ts),
            timestampFound: foundAnyTimestamp,
            avgTranscodeOutMB: avgOutMB,
            avgTranscodeOutPercent: avgOutPercent
        )
    }

    // MARK: - Archive (single file)

    private static func maybeMeasureArchive(plan: BenchmarkPlan, file: MediaFile) async -> Stats? {
        guard let ssdBase = plan.ssdPath, !ssdBase.isEmpty else {
            return nil
        }

        let seconds = await measureArchiveOnce(file: file, ssdPath: ssdBase)
        guard let seconds else { return nil }
        return Stats([seconds])
    }

    private static func measureArchiveOnce(file: MediaFile, ssdPath: String) async -> Double? {
        let fm = FileManager.default

        let benchmarkPaths = makeSSDBenchmarkArtifactsPath(ssdBase: ssdPath, leaf: "walkthrough-archive")
        let benchDir = benchmarkPaths.benchDir

        do {
            guard VolumeUtils.isMounted(volumeContainingPath: benchDir) else { return nil }
            try fm.createDirectory(atPath: benchDir, withIntermediateDirectories: true)
            defer { cleanupSSDBenchmarkArtifacts(benchmarkPaths) }

            let storage = StorageManager(
                externalSSDPath: benchDir,
                sha256BufferSize: BenchDefaults.sha256BufferSize
            )

            let clock = ContinuousClock()
            let start = clock.now

            let assetId = UUID().uuidString
            let result = await storage.archiveOriginal(sourcePath: file.path, assetId: assetId, originalName: file.name)
            guard result.isSafelyStored else { return nil }
            return (clock.now - start).seconds
        } catch {
            return nil
        }
    }

    // MARK: - DB

    private static func measureDBInsertUpdate(iterations: Int, file: MediaFile, kind: String) async -> Stats {
        let tmpDir = makeTempDir("walkthrough-db")
        defer { cleanup(tmpDir) }

        let db: Database
        do {
            db = try makeBenchmarkDatabase(path: "\(tmpDir)/walkthrough.db")
        } catch {
            return Stats([])
        }

        let type: Asset.AssetType = (kind == "image") ? .image : .video

        var seconds: [Double] = []
        seconds.reserveCapacity(iterations)

        for i in 0..<iterations {
            let id = "walk-\(kind)-\(String(format: "%06d", i + 1))"
            do {
                let d = try await measureAsync {
                    _ = try await db.insertQueued(id: id, type: type, originalName: file.name)
                    try await db.markComplete(id: id, timestamp: "")
                }
                seconds.append(d.seconds)
            } catch {
                break
            }
        }

        return Stats(seconds)
    }

    // MARK: - Formatting

    private static func printWalkthroughHeader() {
        BenchOutput.line("  Step".padding(toWidth: 28) + " " + "p50".padding(toWidth: 11) + " " + "p95".padding(toWidth: 11) + " Notes")
        BenchOutput.line("  " + String(repeating: "-", count: 28 + 1 + 11 + 1 + 11 + 1 + 10))
    }

    private static func printWalkthroughFooter() {
        BenchOutput.line("  " + String(repeating: "-", count: 28 + 1 + 11 + 1 + 11 + 1 + 10))
    }

    private static func printWalkthroughRow(step: String, p50: Double?, p95: Double?, notes: String) {
        let p50s = p50.map(fmt) ?? ""
        let p95s = p95.map(fmt) ?? ""
        let line = "  \(step.padding(toWidth: 28)) \(p50s.padding(toWidth: 11)) \(p95s.padding(toWidth: 11)) \(notes)"
        BenchOutput.line(line)
    }

    private static func fileExtensionUppercased(path: String) -> String {
        let ext = URL(fileURLWithPath: path).pathExtension
        return ext.isEmpty ? "?" : ext.uppercased()
    }

    static func formatByteSize(_ bytes: Int) -> String {
        BenchmarkByteFormatter.format(bytes)
    }

    private static func maxStepName(_ pairs: [(String, Double)]) -> String {
        pairs.max(by: { $0.1 < $1.1 })?.0 ?? "?"
    }

    private static func measureSync(iterations: Int, _ block: () throws -> Void) rethrows -> Stats {
        var seconds: [Double] = []
        seconds.reserveCapacity(iterations)
        for _ in 0..<iterations {
            let d = try measure(block)
            seconds.append(d.seconds)
        }
        return Stats(seconds)
    }

    private static func measureAsyncStats(iterations: Int, _ block: () async throws -> Void) async -> Stats {
        var seconds: [Double] = []
        seconds.reserveCapacity(iterations)
        for _ in 0..<iterations {
            do {
                let d = try await measureAsync(block)
                seconds.append(d.seconds)
            } catch {
                break
            }
        }
        return Stats(seconds)
    }
}

private extension String {
    func padding(toWidth width: Int) -> String {
        if count >= width { return self }
        return self + String(repeating: " ", count: width - count)
    }
}
