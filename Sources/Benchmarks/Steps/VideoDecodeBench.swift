import Foundation
import AVFoundation
import CoreVideo
import KikoMediaCore

func decodeAllFrames(path: String) async throws -> (frames: Int, seconds: Double) {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    let tracks = try await asset.loadTracks(withMediaType: .video)
    guard let track = tracks.first else { throw VideoProcessorError.noVideoTrack }
    let reader = try AVAssetReader(asset: asset)
    let output = AVAssetReaderTrackOutput(track: track, outputSettings: [
        kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange
    ])
    output.alwaysCopiesSampleData = false
    reader.add(output)
    let clock = ContinuousClock()
    let start = clock.now
    guard reader.startReading() else {
        throw reader.error ?? VideoProcessorError.noVideoTrack
    }
    var frames = 0
    while true {
        let decoded = autoreleasepool { () -> Bool in
            guard let sampleBuffer = output.copyNextSampleBuffer() else {
                return false
            }
            frames += 1
            CMSampleBufferInvalidate(sampleBuffer)
            return true
        }
        if !decoded {
            break
        }
    }
    switch reader.status {
    case .completed:
        break
    case .failed, .cancelled:
        throw reader.error ?? VideoProcessorError.noVideoTrack
    case .reading, .unknown:
        break
    @unknown default:
        break
    }
    return (frames, (clock.now - start).seconds)
}

func benchmarkDecodeOnly(corpus: [MediaFile]) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Decode-Only (AVAssetReader → NV12, no encode)")
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else { BenchOutput.line("  No video files in media folder, skipping"); return }
    let iterations = 1
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "File", width: 14),
        BenchmarkRuntimeTableColumn(header: "Codec", width: 6),
        BenchmarkRuntimeTableColumn(header: "Resolution", width: 11),
        BenchmarkRuntimeTableColumn(header: "Frames", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "max", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "n", width: 3, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Decode FPS", width: 10, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Realtime", width: 8, alignment: .right),
    ]

    BenchmarkRuntimeRenderer.printField("Iterations", "\(iterations) per video")
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for file in videos {
        let info = try await getVideoInfo(path: file.path)
        var durations: [Double] = []
        var frameCounts: [Int] = []
        for iteration in 0..<iterations {
            try BenchmarkMemoryGuard.checkpoint(
                stage: "video-decode",
                detail: "\(file.name) iteration \(iteration + 1)"
            )
            let (frames, elapsed) = try await decodeAllFrames(path: file.path)
            durations.append(elapsed)
            frameCounts.append(frames)
        }
        let avgFrames = frameCounts.reduce(0, +) / max(1, frameCounts.count)
        let stats = Stats(durations)
        let decodeFPS = stats.p50 > 0 ? Int(Double(avgFrames) / stats.p50) : 0
        let realtime = info.fps > 0 ? String(format: "%.0f", Double(decodeFPS) / Double(info.fps)) : "?"
        BenchmarkRuntimeRenderer.printTableRow(
            [
                file.description,
                info.codec,
                "\(info.width)x\(info.height)",
                "\(avgFrames)",
                fmt(stats.p50),
                fmt(stats.p95),
                fmt(stats.min),
                fmt(stats.max),
                "\(stats.count)",
                "\(decodeFPS)",
                "\(realtime)x",
            ],
            columns: columns
        )
    }
}
