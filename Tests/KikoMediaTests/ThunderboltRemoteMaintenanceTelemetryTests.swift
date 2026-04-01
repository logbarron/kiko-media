import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt remote maintenance telemetry")
struct ThunderboltRemoteMaintenanceTelemetryTests {
    @Test("valid isolated remote sample is accepted")
    func validIsolatedRemoteSampleAccepted() {
        let sample = makeThunderboltRemoteMaintenanceTelemetrySample(
            host: "worker-a",
            workerSignature: "sig-remote-a",
            concurrency: 1,
            isolated: true,
            success: true,
            actualExecutor: "worker-a",
            processNanos: 9_000_000,
            txInMS: 4.5,
            txOutMS: 2.0,
            publishOverheadMS: 1.0
        )

        #expect(sample.invalidationReason == nil)
        #expect(sample.validForPriorGeneration)
    }

    @Test("fallback or local contamination invalidates sample")
    func localFallbackContaminationInvalidatesSample() {
        let sample = makeThunderboltRemoteMaintenanceTelemetrySample(
            host: "worker-a",
            workerSignature: "sig-remote-a",
            concurrency: 1,
            isolated: true,
            success: true,
            actualExecutor: "local-fallback",
            processNanos: 9_000_000,
            txInMS: 4.5,
            txOutMS: 2.0,
            publishOverheadMS: 1.0
        )

        #expect(sample.invalidationReason == .localFallback)
        #expect(!sample.validForPriorGeneration)
    }

    @Test("remote process time stays separate from transfer and publish estimates")
    func remoteProcessTimeSeparateFromTransferTelemetry() throws {
        let lowOverheadResult = buildThunderboltRemoteMaintenanceMachine(
            worker: makeTelemetryWorker(),
            caps: makeTelemetryCaps(),
            preset: defaultVideoPreset,
            telemetry: makeRepresentativeTelemetrySet(txScale: 1)
        )
        let highOverheadResult = buildThunderboltRemoteMaintenanceMachine(
            worker: makeTelemetryWorker(),
            caps: makeTelemetryCaps(),
            preset: defaultVideoPreset,
            telemetry: makeRepresentativeTelemetrySet(txScale: 100)
        )
        let lowOverheadMachine = try #require(lowOverheadResult.machine)
        let highOverheadMachine = try #require(highOverheadResult.machine)

        #expect(abs(lowOverheadMachine.fixedOverheadMS - highOverheadMachine.fixedOverheadMS) < 0.001)
        #expect(abs(lowOverheadMachine.msPerFrameC1 - highOverheadMachine.msPerFrameC1) < 0.001)
        #expect(
            lowOverheadMachine.cells.map { $0.degradationRatio }
                == highOverheadMachine.cells.map { $0.degradationRatio }
        )
    }

    @Test("insufficient isolated remote data is rejected for prior generation")
    func insufficientIsolatedRemoteDataRejected() {
        let c2Only = makeThunderboltRemoteMaintenanceTelemetrySample(
            host: "worker-a",
            workerSignature: "sig-remote-a",
            concurrency: 2,
            isolated: true,
            success: true,
            actualExecutor: "worker-a",
            processNanos: 8_000_000
        )
        let contaminatedC1 = makeThunderboltRemoteMaintenanceTelemetrySample(
            host: "worker-a",
            workerSignature: "sig-remote-a",
            concurrency: 1,
            isolated: true,
            success: true,
            actualExecutor: "local-fallback",
            processNanos: 7_000_000
        )

        let eligibility = evaluateThunderboltRemoteMaintenancePriorEligibility(
            samples: [c2Only, contaminatedC1]
        )

        #expect(!eligibility.workerEligible)
        #expect(eligibility.eligibleConcurrencies.isEmpty)
    }
}

private func makeRepresentativeTelemetrySet(txScale: Double) -> [ThunderboltRemoteMaintenanceTelemetrySample] {
    [
        makeRepresentativeTelemetrySample(frameCount: 100, concurrency: 1, processMS: 300, txScale: txScale),
        makeRepresentativeTelemetrySample(frameCount: 200, concurrency: 1, processMS: 500, txScale: txScale),
        makeRepresentativeTelemetrySample(frameCount: 400, concurrency: 1, processMS: 900, txScale: txScale),
        makeRepresentativeTelemetrySample(frameCount: 100, concurrency: 2, processMS: 450, txScale: txScale),
        makeRepresentativeTelemetrySample(frameCount: 200, concurrency: 2, processMS: 750, txScale: txScale),
        makeRepresentativeTelemetrySample(frameCount: 400, concurrency: 2, processMS: 1_350, txScale: txScale),
    ]
}

private func makeRepresentativeTelemetrySample(
    frameCount: Double,
    concurrency: Int,
    processMS: Double,
    txScale: Double
) -> ThunderboltRemoteMaintenanceTelemetrySample {
    makeThunderboltRemoteMaintenanceTelemetrySample(
        host: "worker-a",
        workerSignature: "sig-remote-a",
        concurrency: concurrency,
        isolated: true,
        success: true,
        actualExecutor: "worker-a",
        processNanos: UInt64(processMS * 1_000_000.0),
        txInMS: 7.25 * txScale,
        txOutMS: 3.5 * txScale,
        publishOverheadMS: 1.75 * txScale,
        videoPath: "/tmp/video-\(Int(frameCount)).mov",
        frameCount: frameCount
    )
}

private func makeTelemetryWorker() -> ThunderboltBoundWorkerSpec {
    ThunderboltBoundWorkerSpec(
        host: "worker-a",
        connectHost: "worker-a",
        slots: 2,
        sourceIP: "10.0.0.10",
        bridgeName: "bridge0"
    )
}

private func makeTelemetryCaps() -> WorkerCaps {
    let payload: [String: Any] = [
        "worker_signature": "sig-remote-a",
        "ms_per_frame_c1": 0.9,
        "degradation_curve": [
            "1": 1.0,
            "2": 1.2,
        ],
    ]
    let data = try! JSONSerialization.data(withJSONObject: payload, options: [])
    return try! JSONDecoder().decode(WorkerCaps.self, from: data)
}
