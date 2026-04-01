import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Benchmark runtime utilities")
struct BenchmarkRuntimeUtilityTests {
    @Test("empty-corpus benchmark entry points print explicit skip reasons")
    func benchmarkEntryPointsPrintExplicitEmptyCorpusSkipReasons() async throws {
        let emptyCorpus: [MediaFile] = []

        let imageConfig: LimitFinderConfig = {
            var config = LimitFinderConfig()
            config.workload = .image
            return config
        }()

        let videoConfig: LimitFinderConfig = {
            var config = LimitFinderConfig()
            config.workload = .video
            return config
        }()

        let mixedConfig: LimitFinderConfig = {
            var config = LimitFinderConfig()
            config.workload = .mixed
            return config
        }()

        let report = try await captureReport {
            try benchmarkJPEGQualityCurve(corpus: emptyCorpus)
            try benchmarkThumbnailSizeCurve(corpus: emptyCorpus)
            try benchmarkSHA256BufferCurve(corpus: emptyCorpus)
            await benchmarkTranscodePresetComparison(corpus: emptyCorpus, timeoutSeconds: 1)

            try await benchmarkArchiveToSSD(
                corpus: emptyCorpus,
                ssdPath: NSTemporaryDirectory(),
                keepArtifacts: false
            )
            try await benchmarkRealisticPipeline(
                corpus: emptyCorpus,
                imageConcurrency: 1,
                videoConcurrency: 1
            )

            try await benchmarkLimitFinder(
                corpus: emptyCorpus,
                corpusPath: NSTemporaryDirectory(),
                preset: "test-preset",
                timeout: 1,
                hardware: fixtureHardwareProfile(),
                config: imageConfig
            )
            try await benchmarkLimitFinder(
                corpus: emptyCorpus,
                corpusPath: NSTemporaryDirectory(),
                preset: "test-preset",
                timeout: 1,
                hardware: fixtureHardwareProfile(),
                config: videoConfig
            )
            try await benchmarkLimitFinder(
                corpus: emptyCorpus,
                corpusPath: NSTemporaryDirectory(),
                preset: "test-preset",
                timeout: 1,
                hardware: fixtureHardwareProfile(),
                config: mixedConfig
            )
        }

        #expect(lineCount(in: report, containing: "JPEG Quality Curve (512px thumbnails)") == 1)
        #expect(lineCount(in: report, containing: "Thumbnail Size Curve (JPEG thumbnails)") == 1)
        #expect(lineCount(in: report, containing: "SHA256 Buffer Curve (streaming hash throughput)") == 1)
        #expect(lineCount(in: report, containing: "Transcode Preset Comparison (<= 1080p, .mp4)") == 1)
        #expect(lineCount(in: report, containing: "Archive Originals to External SSD (copy + SHA256 verify)") == 1)
        #expect(lineCount(in: report, containing: "Realistic Pipeline (DB + Processing + SHA256 + Archive)") == 1)
        #expect(lineCount(in: report, containing: "Limit Finder") == 3)
        #expect(report.contains("  Timeout: 1s\n"))

        #expect(lineCount(in: report, containing: "No image files in media folder, skipping") == 3)
        #expect(lineCount(in: report, containing: "No media files in media folder, skipping") == 2)
        #expect(lineCount(in: report, containing: "No video files in media folder, skipping") == 2)
        #expect(lineCount(in: report, containing: "Need image files in media folder, skipping") == 1)
        #expect(lineCount(in: report, containing: "Mixed mode needs both images and videos in media folder, skipping") == 1)
    }

    @Test("benchmark memory guard bootstrap stays one-shot and status mirrors the bootstrapped config")
    func benchmarkMemoryGuardBootstrapMirrorsConfiguration() {
        let first = BenchmarkMemoryGuard.bootstrap()
        let second = BenchmarkMemoryGuard.bootstrap()

        switch (first, second) {
        case let (.some(first), .some(second)):
            #expect(first.limitMB == second.limitMB)
            #expect(first.warningMB == second.warningMB)
            #expect(first.limitMB > 0)
            #expect(first.warningMB <= first.limitMB)
            #expect(first.warningMB == max(1, Int(Double(first.limitMB) * 0.85)))
            #expect(first.physicalMemoryMB > 0)
            #expect(!first.source.isEmpty)
            #expect(BenchmarkMemoryGuard.statusSummary() == "enabled (\(first.limitMB)MB hard cap, \(first.warningMB)MB warning)")
        case (nil, nil):
            let status = BenchmarkMemoryGuard.statusSummary()
            #expect(status.hasPrefix("disabled ("))
            #expect(status.hasSuffix(")"))
        default:
            Issue.record("Bootstrap should not change enablement between calls")
        }

        #expect(BenchmarkMemoryGuard.peakMB() > 0)
    }

    private func fixtureHardwareProfile() -> HardwareProfile {
        HardwareProfile(
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            totalCores: 10,
            memoryGB: 24,
            videoEncodeEngines: 1,
            hwEncoderNames: []
        )
    }

    private func captureReport(_ writeBody: @Sendable () async throws -> Void) async throws -> String {
        try await BenchOutputCaptureGate.shared.withExclusive {
            let reportDirectory = ".build/runtime-utility-tests-\(UUID().uuidString)"
            let reportURL = try BenchOutput.startReport(reportDirectory: reportDirectory)
            let reportDirectoryURL = reportURL.deletingLastPathComponent()

            defer {
                BenchOutput.finishReport()
                try? FileManager.default.removeItem(at: reportDirectoryURL)
            }

            try await writeBody()
            BenchOutput.finishReport()
            return try String(contentsOf: reportURL, encoding: .utf8)
        }
    }

    private func lineCount(in text: String, containing needle: String) -> Int {
        text.split(whereSeparator: \.isNewline).reduce(0) { partial, line in
            partial + (line.contains(needle) ? 1 : 0)
        }
    }
}
