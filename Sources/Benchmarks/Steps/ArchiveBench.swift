import Foundation
import KikoMediaCore

func benchmarkArchiveToSSD(
    corpus: [MediaFile],
    ssdPath: String,
    sha256BufferSize: Int = 1_048_576,
    keepArtifacts: Bool
) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Archive Originals to External SSD (copy + SHA256 verify)")
    guard !corpus.isEmpty else {
        BenchOutput.line("  No media files in media folder, skipping")
        return
    }

    let fm = FileManager.default

    let benchmarkPaths = makeSSDBenchmarkArtifactsPath(ssdBase: ssdPath, leaf: "archive")
    let benchDir = benchmarkPaths.benchDir

    BenchmarkRuntimeRenderer.printField("SSD base", ssdPath)
    BenchmarkRuntimeRenderer.printField("Bench dir", benchDir)
    BenchmarkRuntimeRenderer.printField("SHA256 buffer", "\(sha256BufferSize) bytes")
    BenchOutput.line("")

    guard VolumeUtils.isMounted(volumeContainingPath: benchDir) else {
        BenchmarkRuntimeRenderer.printField(
            "Status",
            "SSD not mounted for path: \(benchDir)",
            semantic: .error
        )
        return
    }

    defer {
        BenchOutput.line("")
        if keepArtifacts {
            BenchmarkRuntimeRenderer.printField("Cleanup", "Keeping artifacts at \(benchDir)")
        } else {
            BenchmarkRuntimeRenderer.printField("Cleanup", "Removing \(benchDir)")
            cleanupSSDBenchmarkArtifacts(benchmarkPaths)
        }
    }

    try fm.createDirectory(atPath: benchDir, withIntermediateDirectories: true)

    let storage = StorageManager(externalSSDPath: benchDir, sha256BufferSize: sha256BufferSize)

    let clock = ContinuousClock()
    let startWall = clock.now

    var fileSeconds: [Double] = []
    fileSeconds.reserveCapacity(corpus.count)

    var totalBytes: Int64 = 0
    var okCount = 0
    var failCount = 0

    for (idx, file) in corpus.enumerated() {
        let assetId = String(format: "bench-%06d", idx + 1)
        totalBytes += Int64(file.sizeBytes)

        let start = clock.now
        let result = await storage.archiveOriginal(sourcePath: file.path, assetId: assetId, originalName: file.name)
        let elapsed = clock.now - start
        fileSeconds.append(elapsed.seconds)

        if result.isSafelyStored {
            okCount += 1
        } else {
            failCount += 1
        }
    }

    let wallSeconds = (clock.now - startWall).seconds
    let mb = Double(totalBytes) / (1024.0 * 1024.0)
    let mbps = wallSeconds > 0 ? (mb / wallSeconds) : 0

    let perFileStats = Stats(fileSeconds)
    let summaryColumns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "OK", width: 4, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Failed", width: 6, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Data MB", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "MB/s", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p50", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "max", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "n", width: 3, alignment: .right),
    ]

    BenchmarkRuntimeRenderer.printSubsectionTitle("Archive Summary")
    BenchmarkRuntimeRenderer.printTableHeader(summaryColumns)
    BenchmarkRuntimeRenderer.printTableRow(
        [
            "\(okCount)",
            "\(failCount)",
            String(format: "%.1f", mb),
            String(format: "%.1f", mbps),
            fmt(perFileStats.p50),
            fmt(perFileStats.p95),
            fmt(perFileStats.min),
            fmt(perFileStats.max),
            "\(perFileStats.count)",
        ],
        columns: summaryColumns,
        semantics: [
            nil,
            failCount > 0 ? .error : nil,
            nil,
            nil,
            nil,
            nil,
            nil,
            nil,
            nil,
        ]
    )

}
