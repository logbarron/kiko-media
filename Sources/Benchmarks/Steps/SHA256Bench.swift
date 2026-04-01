import Foundation
import KikoMediaCore

func benchmarkSHA256(corpus: [MediaFile]) throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("SHA256 Hashing")
    guard !corpus.isEmpty else {
        BenchOutput.line("  No media files in media folder, skipping")
        return
    }
    let iterations = 10

    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "File", width: 14),
        BenchmarkRuntimeTableColumn(header: "p50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "max", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "n", width: 3, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "MB/s", width: 6, alignment: .right),
    ]

    BenchmarkRuntimeRenderer.printField("Iterations", "\(iterations) per file")
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for file in corpus {
        var durations: [Double] = []
        for iteration in 0..<iterations {
            try BenchmarkMemoryGuard.checkpoint(
                stage: "sha256",
                detail: "\(file.name) iteration \(iteration + 1)"
            )
            let d = try measure {
                _ = try SHA256Utility.calculateSHA256(
                    path: file.path,
                    bufferSize: BenchDefaults.sha256BufferSize
                )
            }
            durations.append(d.seconds)
        }
        let stats = Stats(durations)
        let mbPerSec = Double(file.sizeBytes) / (1024 * 1024) / stats.p50
        BenchmarkRuntimeRenderer.printTableRow(
            [
                file.description,
                fmt(stats.p50),
                fmt(stats.p95),
                fmt(stats.min),
                fmt(stats.max),
                "\(stats.count)",
                String(format: "%.0f", mbPerSec),
            ],
            columns: columns
        )
    }
}
