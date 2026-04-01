import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Drift baseline behavior")
struct DriftBaselineTests {
    @Test("baseline snapshot seeds drift baseline from running ETA")
    func baselineSnapshotSeedsBaseline() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let dispatcher = makeTestDispatcher(env: env)
        await dispatcher.testHookSeedRunningEstimate(workerIndex: 0, slotIndex: 0, estRemainingMS: 1_200)

        let before = await dispatcher.testHookDriftState(workerIndex: 0, slotIndex: 0)
        #expect(before.baselineRemainingMS == nil)
        #expect(before.baselineAtNanos == nil)

        await dispatcher.noteBaselineSnapshot()

        let after = await dispatcher.testHookDriftState(workerIndex: 0, slotIndex: 0)
        #expect(after.baselineRemainingMS == 1_200)
        #expect(after.baselineAtNanos != nil)
        #expect(after.armed)
    }

    @Test("eta drift trigger requires a baseline snapshot")
    func etaDriftRequiresBaselineSnapshot() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let dispatcher = makeTestDispatcher(env: env)
        let recorder = DriftTriggerRecorder()
        await dispatcher.setRecomputeSignal { trigger in
            Task { await recorder.record(trigger) }
        }

        await dispatcher.testHookSeedRunningEstimate(workerIndex: 0, slotIndex: 0, estRemainingMS: 1_000)
        await dispatcher.testHookInjectRunningTick(workerIndex: 0, slotIndex: 0, estRemainingMS: 100)
        await dispatcher.testHookInjectRunningTick(workerIndex: 0, slotIndex: 0, estRemainingMS: 100)
        try? await Task.sleep(for: .milliseconds(75))
        #expect(await recorder.count(.etaDrift) == 0)

        await dispatcher.testHookSeedRunningEstimate(workerIndex: 0, slotIndex: 0, estRemainingMS: 1_000)
        await dispatcher.noteBaselineSnapshot()
        await dispatcher.testHookInjectRunningTick(workerIndex: 0, slotIndex: 0, estRemainingMS: 100)
        await dispatcher.testHookInjectRunningTick(workerIndex: 0, slotIndex: 0, estRemainingMS: 100)

        let triggered = try await waitUntil(timeoutSeconds: 1.5) {
            await recorder.count(.etaDrift) == 1
        }
        #expect(triggered)
    }
}

private func makeTestDispatcher(env: TestEnv) -> ThunderboltDispatcher {
    let bridge = ThunderboltDispatcher.BridgeSource(
        name: "bridge-test",
        ip: "127.0.0.1",
        network: 0x7F00_0000,
        mask: 0xFF00_0000
    )
    return ThunderboltDispatcher(
        workers: [.init(host: "127.0.0.1", slots: 1)],
        port: 6553,
        connectTimeout: 500,
        thumbsDir: env.thumbsDir,
        previewsDir: env.previewsDir,
        sha256BufferSize: env.config.sha256BufferSize,
        complexityAwareSchedulingEnabled: true,
        bridgeSourcesOverride: [bridge]
    )
}

private actor DriftTriggerRecorder {
    private var triggers: [ThunderboltDispatcher.RecomputeTrigger] = []

    func record(_ trigger: ThunderboltDispatcher.RecomputeTrigger) {
        triggers.append(trigger)
    }

    func count(_ trigger: ThunderboltDispatcher.RecomputeTrigger) -> Int {
        triggers.filter { $0 == trigger }.count
    }
}

private func waitUntil(
    timeoutSeconds: TimeInterval,
    pollEveryMillis: UInt64 = 25,
    condition: @escaping @Sendable () async throws -> Bool
) async throws -> Bool {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        if try await condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(Int(pollEveryMillis)))
    }
    return try await condition()
}
