import Foundation
import KikoMediaCore

func benchmarkJPEGQualityCurve(corpus: [MediaFile]) throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("JPEG Quality Curve (512px thumbnails)")

    let images = corpus.filter { $0.type == .image }
    guard !images.isEmpty else {
        BenchOutput.line("  No image files in media folder, skipping")
        return
    }

    let sampleCount = min(5, images.count)
    let sample = Array(images.prefix(sampleCount))
    BenchmarkRuntimeRenderer.printField("Files", "\(sample.count) images (sampled)")
    BenchOutput.line("")

    let qualities: [Double] = [0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
    let tmpDir = makeTempDir("jpeg-quality")
    defer { cleanup(tmpDir) }

    struct Row {
        let quality: Double
        let avgTimeSeconds: Double
        let avgSizeBytes: Double
    }

    let fm = FileManager.default
    var rows: [Row] = []
    rows.reserveCapacity(qualities.count)

    for quality in qualities {
        var totalSeconds = 0.0
        var totalBytes = 0.0

        for (i, file) in sample.enumerated() {
            let qLabel = String(format: "%.2f", quality).replacingOccurrences(of: ".", with: "_")
            let out = "\(tmpDir)/thumb-q\(qLabel)-\(i).jpg"

            let d = try measure {
                try ImageProcessor.generateThumbnail(
                    sourcePath: file.path,
                    outputPath: out,
                    size: 512,
                    quality: quality,
                    maxPixels: BenchDefaults.maxImagePixels,
                    maxDimension: BenchDefaults.maxImageDimension,
                    maxCompressionRatio: BenchDefaults.maxCompressionRatio
                )
            }
            totalSeconds += d.seconds

            if let attrs = try? fm.attributesOfItem(atPath: out),
               let size = attrs[.size] as? NSNumber {
                totalBytes += size.doubleValue
            }
        }

        let denom = Double(sample.count)
        rows.append(Row(
            quality: quality,
            avgTimeSeconds: totalSeconds / denom,
            avgSizeBytes: totalBytes / denom
        ))
    }

    guard let baseline = rows.first(where: { abs($0.quality - 0.85) < 0.0001 }) else {
        return
    }

    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Quality", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Avg time", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Avg size", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "vs 0.85", width: 8, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for row in rows {
        let avgMs = row.avgTimeSeconds * 1000.0
        let avgKB = row.avgSizeBytes / 1024.0

        let vs: String
        if abs(row.quality - baseline.quality) < 0.0001 {
            vs = "baseline"
        } else if baseline.avgSizeBytes > 0 {
            let delta = (row.avgSizeBytes - baseline.avgSizeBytes) / baseline.avgSizeBytes * 100.0
            vs = String(format: "%+.0f%%", delta)
        } else {
            vs = "n/a"
        }

        BenchmarkRuntimeRenderer.printTableRow(
            [
                String(format: "%.2f", row.quality),
                String(format: "%.1fms", avgMs),
                String(format: "%.0fKB", avgKB),
                vs,
            ],
            columns: columns
        )
    }
}
