import Foundation
import KikoMediaCore

func benchmarkThumbnailSizeCurve(corpus: [MediaFile]) throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Thumbnail Size Curve (JPEG thumbnails)")

    let images = corpus.filter { $0.type == .image }
    guard !images.isEmpty else {
        BenchOutput.line("  No image files in media folder, skipping")
        return
    }

    let sampleCount = min(5, images.count)
    let sample = Array(images.prefix(sampleCount))
    BenchmarkRuntimeRenderer.printField("Files", "\(sample.count) images (sampled)")
    BenchOutput.line("")

    let sizes: [Int] = [256, 384, 512, 640, 768, 1024]
    let tmpDir = makeTempDir("thumb-size-curve")
    defer { cleanup(tmpDir) }

    struct Row {
        let size: Int
        let avgTimeSeconds: Double
        let avgSizeBytes: Double
    }

    let fm = FileManager.default
    var rows: [Row] = []
    rows.reserveCapacity(sizes.count)

    for size in sizes {
        var totalSeconds = 0.0
        var totalBytes = 0.0

        for (i, file) in sample.enumerated() {
            let out = "\(tmpDir)/thumb-s\(size)-\(i).jpg"
            let d = try measure {
                try ImageProcessor.generateThumbnail(
                    sourcePath: file.path,
                    outputPath: out,
                    size: size,
                    quality: 0.85,
                    maxPixels: BenchDefaults.maxImagePixels,
                    maxDimension: BenchDefaults.maxImageDimension,
                    maxCompressionRatio: BenchDefaults.maxCompressionRatio
                )
            }
            totalSeconds += d.seconds

            if let attrs = try? fm.attributesOfItem(atPath: out),
               let fileSize = attrs[.size] as? NSNumber {
                totalBytes += fileSize.doubleValue
            }
        }

        let denom = Double(sample.count)
        rows.append(Row(size: size, avgTimeSeconds: totalSeconds / denom, avgSizeBytes: totalBytes / denom))
    }

    guard let baseline = rows.first(where: { $0.size == 512 }) else {
        return
    }

    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Size", width: 6, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Avg time", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Avg size", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "vs 512", width: 8, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for row in rows {
        let avgMs = row.avgTimeSeconds * 1000.0
        let avgKB = row.avgSizeBytes / 1024.0

        let vs: String
        if row.size == baseline.size {
            vs = "baseline"
        } else if baseline.avgSizeBytes > 0 {
            let delta = (row.avgSizeBytes - baseline.avgSizeBytes) / baseline.avgSizeBytes * 100.0
            vs = String(format: "%+.0f%%", delta)
        } else {
            vs = "n/a"
        }

        BenchmarkRuntimeRenderer.printTableRow(
            [
                "\(row.size)",
                String(format: "%.1fms", avgMs),
                String(format: "%.0fKB", avgKB),
                vs,
            ],
            columns: columns
        )
    }
}
