import Foundation
import Testing
@testable import benchmarks

@Suite("BenchOutput report sanitization", Testing.ParallelizationTrait.serialized)
struct BenchOutputReportSanitizationTests {
    @Test("report excludes carriage-return progress artifacts")
    func reportExcludesCarriageReturnProgressArtifacts() async throws {
        let report = try await captureReport {
            BenchOutput.line("## Header")
            BenchOutput.write("\r  1/3 [>>>>......]")
            BenchOutput.write("\r  2/3 [>>>>>>....]")
            BenchOutput.write("\r\u{1B}[2K")
            BenchOutput.line("| Result | 42 |")
            BenchOutput.line("Footer")
        }

        #expect(!report.contains("\r"))
        #expect(!report.contains("1/3 [>>>>......]"))
        #expect(!report.contains("2/3 [>>>>>>....]"))
        #expect(report.contains("## Header\n"))
        #expect(report.contains("| Result | 42 |\n"))
        #expect(report.contains("Footer\n"))
    }

    @Test("overwritten transient writes do not garble stable output")
    func overwrittenTransientWritesDoNotGarbleStableOutput() async throws {
        let report = try await captureReport {
            BenchOutput.line("Start")
            BenchOutput.write("\r\u{1B}[2K")
            BenchOutput.write("  3/10 transient")
            BenchOutput.write("\r\u{1B}[2K")
            BenchOutput.write("  7/10 transient")
            BenchOutput.write("\r\u{1B}[2K")
            BenchOutput.line("Final stable row")
        }

        #expect(!report.contains("transient"))
        #expect(report.contains("Start\n"))
        #expect(report.contains("Final stable row\n"))
    }

    @Test("stable partial writes remain intact when completed with newline")
    func stablePartialWritesRemainIntact() async throws {
        let report = try await captureReport {
            BenchOutput.write("  Probing worker capabilities ... ")
            BenchOutput.line("2/2 detected")
        }

        #expect(report == "  Probing worker capabilities ... 2/2 detected\n")
    }

    @Test("finishReport is idempotent and preserves completed report output")
    func finishReportIsIdempotent() async throws {
        try await BenchOutputCaptureGate.shared.withExclusive {
            let reportDirectory = ".build/bench-output-tests-\(UUID().uuidString)"
            let reportURL = try BenchOutput.startReport(reportDirectory: reportDirectory)
            let reportDirectoryURL = reportURL.deletingLastPathComponent()

            defer {
                BenchOutput.finishReport()
                try? FileManager.default.removeItem(at: reportDirectoryURL)
            }

            BenchOutput.line("Run interrupted")
            BenchOutput.finishReport()
            BenchOutput.finishReport()

            let report = try String(contentsOf: reportURL, encoding: .utf8)
            #expect(report.contains("Run interrupted\n"))
        }
    }

    private func captureReport(_ writeBody: @Sendable () throws -> Void) async throws -> String {
        try await BenchOutputCaptureGate.shared.withExclusive {
            let reportDirectory = ".build/bench-output-tests-\(UUID().uuidString)"
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
