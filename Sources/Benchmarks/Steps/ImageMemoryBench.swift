import Foundation
import KikoMediaCore

func benchmarkImageMemory(corpus: [MediaFile]) throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Peak Memory During Image Processing")
    let images = corpus.filter { $0.type == .image }
    guard !images.isEmpty else { BenchOutput.line("  No image files in media folder, skipping"); return }
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "File", width: 14),
        BenchmarkRuntimeTableColumn(header: "Delta", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Before", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "After", width: 8, alignment: .right),
    ]

    BenchmarkRuntimeRenderer.printTableHeader(columns)

    let tmpDir = makeTempDir("mem-bench")
    defer { cleanup(tmpDir) }
    for file in images {
        let memBefore = getMemoryMB()
        let out = "\(tmpDir)/mem-\(file.name).jpg"
        try ImageProcessor.generateThumbnail(
            sourcePath: file.path,
            outputPath: out,
            size: 512,
            quality: 0.85,
            maxPixels: BenchDefaults.maxImagePixels,
            maxDimension: BenchDefaults.maxImageDimension,
            maxCompressionRatio: BenchDefaults.maxCompressionRatio
        )
        let memAfter = getMemoryMB()
        let delta = memAfter - memBefore
        let deltaText: String
        if delta > 0 {
            deltaText = "+\(delta)MB"
        } else {
            deltaText = "\(delta)MB"
        }
        BenchmarkRuntimeRenderer.printTableRow(
            [
                file.description,
                deltaText,
                "\(memBefore)MB",
                "\(memAfter)MB",
            ],
            columns: columns
        )
    }
}
