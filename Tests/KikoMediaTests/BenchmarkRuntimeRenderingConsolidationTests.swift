import Foundation
import Testing
@testable import benchmarks

@Suite("Benchmark runtime rendering consolidation", Testing.ParallelizationTrait.serialized)
struct BenchmarkRuntimeRenderingConsolidationTests {
    @Test("runtime renderer uses the shared field and menu layout in reports")
    func runtimeRendererUsesSharedFieldAndMenuLayout() async throws {
        let report = try await captureReport {
            BenchmarkRuntimeRenderer.printFieldSection(
                "Run Metadata",
                fields: [
                    BenchmarkRuntimeField(label: "Mode", value: "comparison"),
                    BenchmarkRuntimeField(label: "Report", value: "/tmp/bench-results/report.md"),
                ]
            )
            BenchmarkRuntimeRenderer.printMenuSection(
                "Benchmark Manifest",
                rows: [
                    BenchmarkRuntimeMenuRow(
                        title: "comparison · Comparison benches",
                        details: [
                            "Curves for tradeoffs.",
                            "Expected runtime: varies",
                        ]
                    ),
                    BenchmarkRuntimeMenuRow(
                        title: "pipeline · Pipeline benches",
                        details: ["Concurrency sweeps + realistic pipeline."]
                    ),
                ],
                startAt: 3
            )
        }

        let expected = [
            "",
            "  Run Metadata",
            "",
            "  Mode: comparison",
            "  Report: /tmp/bench-results/report.md",
            "",
            "  Benchmark Manifest",
            "",
            "    3. comparison · Comparison benches",
            "       Curves for tradeoffs.",
            "       Expected runtime: varies",
            "    4. pipeline · Pipeline benches",
            "       Concurrency sweeps + realistic pipeline.",
        ].joined(separator: "\n") + "\n"

        #expect(report == expected)
    }

    private func captureReport(_ writeBody: @Sendable () throws -> Void) async throws -> String {
        try await BenchOutputCaptureGate.shared.withExclusive {
            let reportDirectory = ".build/runtime-render-tests-\(UUID().uuidString)"
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
