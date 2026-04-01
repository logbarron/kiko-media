import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Recompute trigger coalescing")
struct RecomputeCoalescingTests {
    @Test("slot-down burst emits one SLOT_DOWN_BATCH recompute signal")
    func slotDownBurstCoalescesToSingleSignal() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeComplexityAwareConfig(env: env)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-recompute-coalesce"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true
        )

        let recorder = RecomputeTriggerRecorder()
        await processor.setRecomputeSignal { trigger in
            Task { await recorder.record(trigger) }
        }

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    await processor.requestRecomputeFromDispatcher(trigger: .slotDownBatch)
                }
            }
            await group.waitForAll()
        }

        let gotOne = try await waitUntil(timeoutSeconds: 5.0) {
            await recorder.count(.slotDownBatch) == 1
        }
        #expect(gotOne)
        #expect(await recorder.count(.slotDownBatch) == 1)
    }

    @Test("non-slot-down triggers are not coalesced by slot-down batch gate")
    func nonSlotDownTriggersRemainDirect() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeComplexityAwareConfig(env: env)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-recompute-direct"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true
        )

        let recorder = RecomputeTriggerRecorder()
        await processor.setRecomputeSignal { trigger in
            Task { await recorder.record(trigger) }
        }

        await processor.requestRecomputeFromDispatcher(trigger: .fail)
        await processor.requestRecomputeFromDispatcher(trigger: .fail)
        await processor.requestRecomputeFromDispatcher(trigger: .fail)

        let gotThree = try await waitUntil(timeoutSeconds: 1.5) {
            await recorder.count(.fail) == 3
        }
        #expect(gotThree)
        #expect(await recorder.count(.fail) == 3)
    }

    @Test("completion refill pass is prioritized ahead of generic deferred recomputes")
    func completionRefillPassIsPrioritizedAheadOfGenericDeferredRecomputes() {
        var coordinator = CARecomputeCoordinator()

        let beganInitialRun = coordinator.beginRecomputeRun(
            requestedPassKind: .genericRecompute,
            reconsiderHeldJobs: false
        )
        let initialPass = coordinator.beginRecomputePass()
        #expect(beganInitialRun)
        #expect(initialPass.kind == .genericRecompute)
        #expect(!initialPass.reconsiderHeldJobs)

        let queuedGenericPass = coordinator.beginRecomputeRun(
            requestedPassKind: .genericRecompute,
            reconsiderHeldJobs: false
        )
        let queuedCompletionPass = coordinator.beginRecomputeRun(
            requestedPassKind: .completionRefill,
            reconsiderHeldJobs: true
        )
        #expect(!queuedGenericPass)
        #expect(!queuedCompletionPass)
        #expect(coordinator.requiresAnotherRecomputePass)

        let prioritizedPass = coordinator.beginRecomputePass()
        #expect(prioritizedPass.kind == .completionRefill)
        #expect(prioritizedPass.reconsiderHeldJobs)
        #expect(coordinator.requiresAnotherRecomputePass)

        let remainingPass = coordinator.beginRecomputePass()
        #expect(remainingPass.kind == .genericRecompute)
        #expect(!remainingPass.reconsiderHeldJobs)
        #expect(!coordinator.requiresAnotherRecomputePass)

        coordinator.finishRecomputeRun()
    }
}

private actor RecomputeTriggerRecorder {
    private var values: [ThunderboltDispatcher.RecomputeTrigger] = []

    func record(_ trigger: ThunderboltDispatcher.RecomputeTrigger) {
        values.append(trigger)
    }

    func count(_ trigger: ThunderboltDispatcher.RecomputeTrigger) -> Int {
        values.filter { $0 == trigger }.count
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

private func makeComplexityAwareConfig(env: TestEnv) -> Config {
    Config(
        publicPort: env.config.publicPort,
        internalPort: env.config.internalPort,
        bindAddress: env.config.bindAddress,
        uploadDir: env.config.uploadDir,
        thumbsDir: env.config.thumbsDir,
        previewsDir: env.config.previewsDir,
        logsDir: env.config.logsDir,
        moderatedDir: env.config.moderatedDir,
        externalSSDPath: env.config.externalSSDPath,
        databasePath: env.config.databasePath,
        turnstileSecret: env.config.turnstileSecret,
        sessionHmacSecret: env.config.sessionHmacSecret,
        videoTranscodePreset: env.config.videoTranscodePreset,
        tbWorkers: "127.0.0.1:1"
    )
}
