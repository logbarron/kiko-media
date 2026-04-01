import Foundation
import KikoMediaCore

func benchmarkImageTimestamp(corpus: [MediaFile]) {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Image Timestamp Extraction")
    let images = corpus.filter { $0.type == .image }
    guard !images.isEmpty else { BenchOutput.line("  No image files in media folder, skipping"); return }
    let iterations = 50
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "File", width: 14),
        BenchmarkRuntimeTableColumn(header: "p50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "max", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "n", width: 3, alignment: .right),
    ]

    BenchmarkRuntimeRenderer.printField("Iterations", "\(iterations) per image")
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for file in images {
        var durations: [Double] = []
        for _ in 0..<iterations {
            let d = measure {
                _ = ImageProcessor.extractTimestamp(sourcePath: file.path)
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
