import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt showdown metadata output", Testing.ParallelizationTrait.serialized)
struct ThunderboltShowdownMetadataOutputTests {
    @Test("standalone showdown emits canonical metadata exactly once")
    func standaloneShowdownEmitsCanonicalMetadataOnce() async throws {
        let report = try await captureReport {
            var threwNoVideos = false
            do {
                try await benchmarkThunderboltShowdown(
                    corpus: [],
                    preset: defaultVideoPreset,
                    timeout: defaultVideoTimeoutSeconds,
                    hardware: testHardwareProfile(),
                    profiles: [.allAtOnce],
                    modelMode: .auto,
                    priorUpdatePolicy: .promoteForce
                )
            } catch let error as ThunderboltBenchmarkJSONError {
                if case .noVideos = error {
                    threwNoVideos = true
                } else {
                    throw error
                }
            }

            #expect(threwNoVideos)
        }

        #expect(metadataLabelCount(in: report, label: "Arrival profiles") == 1)
        #expect(metadataLabelCount(in: report, label: "Policy order") == 1)
        #expect(metadataLabelCount(in: report, label: "CA model mode") == 1)
        #expect(metadataLabelCount(in: report, label: "Model update") == 1)
    }

    @Test("JSON payload preserves configured worker order when some workers are excluded")
    func jsonPayloadPreservesConfiguredWorkerOrderWhenSomeWorkersAreExcluded() throws {
        let configuredWorkers = parseThunderboltWorkers("worker-a:2,worker-b:3,worker-c:4")

        let payloadWorkers = try buildThunderboltBenchmarkJSONWorkerPayload(
            configuredWorkers: configuredWorkers,
            reachableConfiguredIndices: [0, 2],
            bestConfig: ThunderboltBurstConfig(localSlots: 2, remoteSlots: [1, 4])
        )

        #expect(payloadWorkers.workers.map { $0.host } == ["worker-a", "worker-b", "worker-c"])
        #expect(payloadWorkers.workers.map { $0.configuredSlots } == [2, 3, 4])
        #expect(payloadWorkers.remoteWorkers.map { $0.slots } == [1, 0, 4])

        try validateThunderboltBenchmarkJSONPayload(
            ThunderboltBenchmarkJSONPayload(
                schemaVersion: thunderboltDelegatedBenchmarkSchemaVersion,
                workers: payloadWorkers.workers,
                bestConfig: ThunderboltBenchmarkJSONBestConfig(
                    localSlots: 2,
                    remoteWorkers: payloadWorkers.remoteWorkers,
                    wallSeconds: 12,
                    completedVideos: 3,
                    failedVideos: 0,
                    videosPerMin: 15
                )
            ),
            configuredWorkers: configuredWorkers
        )
    }

    @Test("JSON thunderbolt entry throws when all bridge-bound workers are unreachable")
    func jsonThunderboltEntryThrowsWhenAllBridgeBoundWorkersAreUnreachable() async throws {
        do {
            try await TestEnvironment.withEnvironment([
                "TB_WORKERS": "worker-a:2",
                "TB_PORT": "7300",
            ]) {
                _ = try await benchmarkThunderboltJSON(
                    corpus: sampleVideoCorpus(),
                    hardware: testHardwareProfile(),
                    resolveBoundWorkers: { workers in
                        (
                            [
                                ThunderboltBoundWorkerSpec(
                                    host: workers[0].host,
                                    connectHost: "10.0.0.2",
                                    slots: workers[0].slots,
                                    sourceIP: "10.0.0.1",
                                    bridgeName: "bridge0"
                                )
                            ],
                            []
                        )
                    },
                    benchmarkConnectivity: { workers, _, _ in
                        workers.map { worker in
                            ThunderboltConnectivityResult(
                                worker: worker,
                                reachable: false,
                                connectMillis: 7
                            )
                        }
                    }
                )
            }
            Issue.record("Expected worker unreachable error")
        } catch let error as ThunderboltBenchmarkJSONError {
            guard case .workerUnreachable(let host, let port) = error else {
                Issue.record("Unexpected error: \(error)")
                return
            }
            #expect(host == "worker-a")
            #expect(port == 7300)
        }
    }

    private func captureReport(
        _ writeBody: @Sendable () async throws -> Void
    ) async throws -> String {
        try await BenchOutputCaptureGate.shared.withExclusive {
            let reportDirectory = ".build/thunderbolt-showdown-output-\(UUID().uuidString)"
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

    private func metadataLabelCount(in report: String, label: String) -> Int {
        let needle = "\(label):"
        return report.split(whereSeparator: \.isNewline).reduce(0) { partial, line in
            partial + (line.contains(needle) ? 1 : 0)
        }
    }

    private func testHardwareProfile() -> HardwareProfile {
        HardwareProfile(
            chipName: "Apple M4",
            performanceCores: 4,
            efficiencyCores: 6,
            totalCores: 10,
            memoryGB: 16,
            videoEncodeEngines: 1,
            hwEncoderNames: []
        )
    }

    private func sampleVideoCorpus() -> [MediaFile] {
        [
            MediaFile(
                path: "/tmp/sample.mov",
                name: "sample.mov",
                type: .video,
                sizeBytes: 1_024
            )
        ]
    }
}
