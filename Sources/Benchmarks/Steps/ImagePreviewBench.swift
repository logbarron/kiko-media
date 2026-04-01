import Foundation
import KikoMediaCore

func benchmarkImagePreviews(corpus: [MediaFile]) throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Image Preview Generation (1440px)")
    let images = corpus.filter { $0.type == .image }
    guard !images.isEmpty else { BenchOutput.line("  No image files in media folder, skipping"); return }
    let iterations = 10
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

    let tmpDir = makeTempDir("img-preview")
    defer { cleanup(tmpDir) }
    for file in images {
        var durations: [Double] = []
        for i in 0..<iterations {
            let out = "\(tmpDir)/preview-\(file.name)-\(i).jpg"
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
