import Foundation
import KikoMediaCore

func benchmarkDatabase() async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Database Write Throughput")
    let tmpDir = makeTempDir("db-bench")
    defer { cleanup(tmpDir) }
    let count = 1000

    do {
        let db = try makeBenchmarkDatabase(path: "\(tmpDir)/seq.db")
        let d = try await measureAsync {
            for i in 0..<count {
                _ = try await db.insertQueued(id: "asset-\(i)", type: .image, originalName: "test.jpg")
            }
        }
        let rate = Double(count) / d.seconds
        BenchmarkRuntimeRenderer.printMetricItem("Sequential inserts", "\(count) rows in \(fmtDuration(d))  (\(String(format: "%.0f", rate)) rows/s)")
    }

    do {
        let db = try makeBenchmarkDatabase(path: "\(tmpDir)/conc.db")
        let workers = 8
        let perWorker = count / workers
        let d = try await measureAsync {
            try await withThrowingTaskGroup(of: Void.self) { group in
                for w in 0..<workers {
                    group.addTask {
                        for i in 0..<perWorker {
                            _ = try await db.insertQueued(id: "w\(w)-\(i)", type: .image, originalName: "test.jpg")
                        }
                    }
                }
                try await group.waitForAll()
            }
        }
        let rate = Double(count) / d.seconds
        BenchmarkRuntimeRenderer.printMetricItem("Concurrent inserts (\(workers) tasks)", "\(count) rows in \(fmtDuration(d))  (\(String(format: "%.0f", rate)) rows/s)")
    }

    do {
        let db = try makeBenchmarkDatabase(path: "\(tmpDir)/rw.db")
        for i in 0..<500 {
            _ = try await db.insertComplete(
                id: "seed-\(i)",
                type: .image,
                timestamp: "2025:01:01 12:00:00",
                originalName: "test.jpg",
                status: .complete
            )
        }
        let d = try await measureAsync {
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    for i in 0..<200 {
                        _ = try await db.insertQueued(id: "new-\(i)", type: .image, originalName: "test.jpg")
                    }
                }
                for _ in 0..<4 {
                    group.addTask {
                        for _ in 0..<125 {
                            _ = try await db.getAllAssets(limit: 100, offset: 0)
                        }
                    }
                }
                try await group.waitForAll()
            }
        }
        BenchmarkRuntimeRenderer.printMetricItem("Reads under write load", "500 reads + 200 writes in \(fmtDuration(d))")
    }
}
