import Foundation
import Testing
@testable import benchmarks

@Suite("Benchmark leaderboard polish", Testing.ParallelizationTrait.serialized)
struct BenchmarkLeaderboardPolishTests {
    @Test("leaderboard ranks successful configs ahead of failed configs")
    func leaderboardRanksSuccessfulConfigsAheadOfFailedConfigs() async throws {
        let workers = [
            ThunderboltBoundWorkerSpec(
                host: "10.0.0.2",
                connectHost: "10.0.0.2",
                slots: 2,
                sourceIP: "10.0.0.1",
                bridgeName: "bridge0"
            ),
        ]
        let baseline = ThunderboltBurstConfig(localSlots: 1, remoteSlots: [0])
        let failedConfig = ThunderboltBurstConfig(localSlots: 2, remoteSlots: [0])
        let runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)] = [
            (
                baseline,
                ThunderboltBurstResult(
                    wallSeconds: 20.0,
                    completed: 12,
                    failed: 0,
                    completionSeconds: [1, 2, 3, 4]
                )
            ),
            (
                failedConfig,
                ThunderboltBurstResult(
                    wallSeconds: 5.0,
                    completed: 3,
                    failed: 2,
                    completionSeconds: [1, 2]
                )
            ),
        ]

        let workerLabels = ["10.0.0.2": "W1"]
        let report = try await captureReport {
            printThunderboltLeaderboard(workers: workers, workerLabels: workerLabels, runs: runs, baseline: baseline)
        }

        let successIndex = report.range(of: "L=1 W1=0")?.lowerBound
        let failedIndex = report.range(of: "L=2 W1=0")?.lowerBound
        #expect(successIndex != nil)
        #expect(failedIndex != nil)
        if let successIndex, let failedIndex {
            #expect(successIndex < failedIndex)
        }
    }

    private func captureReport(_ writeBody: @Sendable () throws -> Void) async throws -> String {
        try await BenchOutputCaptureGate.shared.withExclusive {
            let reportDirectory = ".build/leaderboard-polish-tests-\(UUID().uuidString)"
            let reportURL = try BenchOutput.startReport(reportDirectory: reportDirectory)
            let reportDirectoryURL = reportURL.deletingLastPathComponent()

            defer {
                BenchOutput.finishReport()
                try? FileManager.default.removeItem(at: reportDirectoryURL)
            }

            try writeBody()
            BenchOutput.finishReport()
            return try String(contentsOf: reportURL, encoding: .utf8)
        }
    }
}
