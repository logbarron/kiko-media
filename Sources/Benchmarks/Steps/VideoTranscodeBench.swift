import Foundation
import AVFoundation
import KikoMediaCore

func benchmarkVideoTranscode(
    corpus: [MediaFile],
    preset: String = AVAssetExportPreset1920x1080,
    timeout: Int = 300
) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Video Transcode Detailed (AVAssetExportSession preset → .mp4)")
    BenchmarkRuntimeRenderer.printField("Preset", preset)
    BenchmarkRuntimeRenderer.printField("Timeout", "\(timeout)s")
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else { BenchOutput.line("  No video files in media folder, skipping"); return }
    let iterations = 1
    BenchmarkRuntimeRenderer.printField("Iterations", "\(iterations) per video")
    BenchOutput.line("")

    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "File", width: 14),
        BenchmarkRuntimeTableColumn(header: "Source", width: 17),
        BenchmarkRuntimeTableColumn(header: "p50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "CPU mean", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "CPU p95", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Pipe FPS", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Out MB", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Size %", width: 6, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Out video", width: 16),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    let tmpDir = makeTempDir("vid-transcode")
    defer { cleanup(tmpDir) }
    for file in videos {
        let info = try await getVideoInfo(path: file.path)
        var durations: [Double] = []
        var cpuSamples: [Double] = []
        var cpuAllSamples: [Double] = []
        var outputSizes: [Int] = []
        var lastOutputPath: String? = nil
        for i in 0..<iterations {
            try BenchmarkMemoryGuard.checkpoint(
                stage: "video-transcode",
                detail: "\(file.name) iteration \(i + 1)"
            )
            let out = "\(tmpDir)/transcode-\(file.name)-\(i).mp4"

            let collector = CPUCollector()
            let sampler = Task.detached {
                while !Task.isCancelled {
                    await collector.add(getCPUPercent())
                    try? await Task.sleep(for: .milliseconds(200))
                }
            }

            let d = try await measureAsync {
                try await VideoProcessor.transcode(
                    sourcePath: file.path,
                    outputPath: out,
                    timeoutSeconds: timeout,
                    preset: preset
                )
            }
            sampler.cancel()
            let samples = await collector.snapshot()
            if !samples.isEmpty {
                let stats = Stats(samples)
                cpuSamples.append(stats.mean)
                cpuAllSamples.append(contentsOf: samples)
            }
            durations.append(d.seconds)
            if let attrs = try? FileManager.default.attributesOfItem(atPath: out),
               let size = attrs[.size] as? Int {
            outputSizes.append(size)
            }
            lastOutputPath = out
        }
        let stats = Stats(durations)
        let avgCPU = cpuSamples.isEmpty ? 0 : cpuSamples.reduce(0, +) / Double(cpuSamples.count)
        let cpuStats = cpuAllSamples.isEmpty ? nil : Stats(cpuAllSamples)
        let pipeFPS = (stats.p50 > 0 && info.estimatedFrames > 0)
            ? Int(Double(info.estimatedFrames) / stats.p50) : 0
        let avgOutMB = outputSizes.isEmpty ? 0 : outputSizes.reduce(0, +) / outputSizes.count / (1024 * 1024)
        let sizeRatio = (!outputSizes.isEmpty && file.sizeBytes > 0)
            ? String(format: "%.0f", Double(outputSizes.reduce(0, +) / outputSizes.count) / Double(file.sizeBytes) * 100)
            : "?"
        let sizePercent = sizeRatio == "?" ? "?" : "\(sizeRatio)%"

        let outputVideoProfile: String
        if let lastOutputPath, let outInfo = try? await getVideoInfo(path: lastOutputPath) {
            outputVideoProfile = "\(outInfo.codec) \(outInfo.width)x\(outInfo.height)"
        } else {
            outputVideoProfile = "n/a"
        }

        BenchmarkRuntimeRenderer.printTableRow(
            [
                file.description,
                "\(info.codec) \(info.width)x\(info.height)",
                fmt(stats.p50),
                fmt(stats.p95),
                cpuSamples.isEmpty ? "n/a" : String(format: "%.0f%%", avgCPU),
                cpuStats.map { String(format: "%.0f%%", $0.p95) } ?? "n/a",
                "\(pipeFPS)",
                "\(avgOutMB)",
                sizePercent,
                outputVideoProfile,
            ],
            columns: columns
        )
    }
}
