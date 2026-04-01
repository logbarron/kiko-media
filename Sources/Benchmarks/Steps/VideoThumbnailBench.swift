import Foundation
import KikoMediaCore

func benchmarkVideoThumbnails(corpus: [MediaFile]) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Video Thumbnail Generation (512px)")
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else { BenchOutput.line("  No video files in media folder, skipping"); return }
    let iterations = 1
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "File", width: 14),
        BenchmarkRuntimeTableColumn(header: "p50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "max", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "n", width: 3, alignment: .right),
    ]

    BenchmarkRuntimeRenderer.printField("Iterations", "\(iterations) per video")
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    let tmpDir = makeTempDir("vid-thumb")
    defer { cleanup(tmpDir) }
    for file in videos {
        var durations: [Double] = []
        for i in 0..<iterations {
            let out = "\(tmpDir)/vthumb-\(file.name)-\(i).jpg"
            let d = try await measureAsync {
                try await VideoProcessor.generateThumbnail(
                    sourcePath: file.path,
                    outputPath: out,
                    size: 512,
                    time: 1.0,
                    quality: 0.85
                )
            }
            durations.append(d.seconds)
        }
        let stats = Stats(durations)
        BenchmarkRuntimeRenderer.printTableRow(
            [
                file.description,
                fmt(stats.p50),
                fmt(stats.p95),
                fmt(stats.min),
                fmt(stats.max),
                "\(stats.count)",
            ],
            columns: columns
        )
    }
}
