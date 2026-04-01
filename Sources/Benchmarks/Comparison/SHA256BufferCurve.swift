import Foundation
import KikoMediaCore

func benchmarkSHA256BufferCurve(corpus: [MediaFile]) throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("SHA256 Buffer Curve (streaming hash throughput)")

    guard !corpus.isEmpty else {
        BenchOutput.line("  No media files in media folder, skipping")
        return
    }

    let bufferSizes: [Int] = [
        64 * 1024,
        256 * 1024,
        512 * 1024,
        BenchDefaults.sha256BufferSize,
        2 * 1024 * 1024,
        4 * 1024 * 1024,
    ]

    let totalBytes = corpus.reduce(into: Int64(0)) { $0 += Int64($1.sizeBytes) }
    let totalMB = Double(totalBytes) / (1024.0 * 1024.0)
    BenchmarkRuntimeRenderer.printField("Files", "\(corpus.count) (\(String(format: "%.0f", totalMB))MB total)")
    BenchOutput.line("")

    struct Row {
        let bufferSize: Int
        let mbPerSec: Double
    }

    var rows: [Row] = []
    rows.reserveCapacity(bufferSizes.count)

    for bufferSize in bufferSizes {
        let d = try measure {
            for file in corpus {
                try BenchmarkMemoryGuard.checkpoint(
                    stage: "comparison-sha256-buffer",
                    detail: "\(formatBufferSize(bufferSize)) · \(file.name)"
                )
                _ = try SHA256Utility.calculateSHA256(path: file.path, bufferSize: bufferSize)
            }
        }

        // Duration.seconds can be extremely small in synthetic cases; avoid /0.
        let seconds = max(0.000_001, d.seconds)
        rows.append(Row(bufferSize: bufferSize, mbPerSec: totalMB / seconds))
    }

    guard let baseline = rows.first(where: { $0.bufferSize == BenchDefaults.sha256BufferSize }) else {
        return
    }

    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Buffer", width: 6, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "MB/s", width: 6, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "vs 1MB", width: 8, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for row in rows {
        let label = formatBufferSize(row.bufferSize)
        let throughput = String(format: "%.0f", row.mbPerSec)

        let vs: String
        if row.bufferSize == baseline.bufferSize {
            vs = "baseline"
        } else if baseline.mbPerSec > 0 {
            let delta = (row.mbPerSec - baseline.mbPerSec) / baseline.mbPerSec * 100.0
            vs = String(format: "%+.0f%%", delta)
        } else {
            vs = "n/a"
        }

        BenchmarkRuntimeRenderer.printTableRow(
            [label, throughput, vs],
            columns: columns
        )
    }
}

private func formatBufferSize(_ bytes: Int) -> String {
    if bytes % (1024 * 1024) == 0 {
        return "\(bytes / (1024 * 1024))MB"
    }
    return "\(bytes / 1024)KB"
}
