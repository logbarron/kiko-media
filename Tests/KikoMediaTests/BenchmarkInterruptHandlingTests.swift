import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Benchmark interrupt handling", Testing.ParallelizationTrait.serialized)
struct BenchmarkInterruptHandlingTests {
    @Test("first interrupt is recorded, repeats are idempotent")
    func interruptRequestIsIdempotent() {
        let state = BenchmarkInterruptState()
        #expect(state.isInterrupted == false)

        let first = state.requestInterrupt()
        let second = state.requestInterrupt()

        #expect(first == true)
        #expect(second == false)
        #expect(state.isInterrupted == true)
    }

    @Test("throwIfInterrupted throws once interrupt has been requested")
    func throwIfInterruptedAfterRequest() {
        let state = BenchmarkInterruptState()
        _ = state.requestInterrupt()

        #expect(throws: BenchmarkInterruptError.self) {
            try state.throwIfInterrupted()
        }
    }

    @Test("interrupt classifier maps cancellation and explicit interrupt state")
    func interruptClassifierCoversCancellationAndState() {
        let state = BenchmarkInterruptState()
        let baseline = BenchmarkError.emptyMediaFolder("/tmp/media")

        #expect(!isBenchmarkInterrupted(baseline, interruptState: state))
        #expect(isBenchmarkInterrupted(CancellationError(), interruptState: state))
        #expect(isBenchmarkInterrupted(BenchmarkInterruptError.interrupted, interruptState: nil))

        _ = state.requestInterrupt()
        #expect(isBenchmarkInterrupted(baseline, interruptState: state))
    }

    @Test("runWithSIGINTHandling first interrupt cancels and repeat trigger is idempotent")
    func runWithSIGINTHandlingCancelAndIdempotency() async throws {
        let trigger = InterruptTrigger()
        let cancelCount = LockedIntCounter()
        let messageCount = LockedIntCounter()

        let hooks = BenchmarkSIGINTHandlingHooks(
            installInterruptHandler: { onInterrupt in
                trigger.install(onInterrupt)
                return {}
            },
            emitInterruptMessage: { _ in
                messageCount.increment()
            },
            runBenchmarksOperation: { _, _ in
                try await withTaskCancellationHandler {
                    while true {
                        try await Task.sleep(for: .seconds(1))
                    }
                } onCancel: {
                    cancelCount.increment()
                }
            },
            classifyInterruptedError: isBenchmarkInterrupted
        )

        let runTask = Task {
            try await runWithSIGINTHandling(plan: BenchmarkPlan(), hooks: hooks)
        }

        let installed = await waitForCondition(timeoutSeconds: 1.0) {
            trigger.hasHandler()
        }
        if !installed {
            runTask.cancel()
            Issue.record("Expected interrupt handler to be installed")
            return
        }

        trigger.fire()
        trigger.fire()

        do {
            try await runTask.value
            Issue.record("Expected interruption to map to BenchmarkInterruptError")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(messageCount.value() == 1)
        #expect(cancelCount.value() == 1)
    }

    @Test("interrupted classification maps to exit code 130")
    func interruptedClassificationMapsToExit130() async throws {
        let hooks = BenchmarkSIGINTHandlingHooks(
            installInterruptHandler: { _ in { } },
            emitInterruptMessage: { _ in },
            runBenchmarksOperation: { _, _ in
                throw CancellationError()
            },
            classifyInterruptedError: isBenchmarkInterrupted
        )

        do {
            try await runWithSIGINTHandling(plan: BenchmarkPlan(), hooks: hooks)
            Issue.record("Expected cancellation to map to BenchmarkInterruptError")
        } catch let error as BenchmarkInterruptError {
            #expect(benchmarkExitCode(for: error) == 130)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("burst sweep loop throws when cancellation is checked inside the dispatch loop")
    func burstSweepLoopThrowsWhenCancellationCheckedInsideDispatchLoop() async {
        let loopChecks = LockedIntCounter()

        do {
            _ = try await runThunderboltBurstConfig(
                config: ThunderboltBurstConfig(localSlots: 1, remoteSlots: []),
                workers: [],
                videos: [makeInterruptTestVideo(named: "burst.mov")],
                port: 7_000,
                connectTimeout: 100,
                preset: defaultVideoPreset,
                timeout: 1,
                loopCancellationCheck: { point in
                    guard point == .burstDispatchLoop else { return }
                    if loopChecks.incrementAndGet() == 2 {
                        throw BenchmarkInterruptError.interrupted
                    }
                }
            )
            Issue.record("Expected burst dispatch loop to throw interruption")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(loopChecks.value() >= 2)
    }

    @Test("prior maintenance local sweep throws when cancellation is checked inside the sweep loop")
    func priorMaintenanceLocalSweepThrowsWhenCancellationCheckedInsideSweepLoop() async {
        let loopChecks = LockedIntCounter()
        let setup = makeInterruptTestShowdownSetup(videos: [makeInterruptTestVideo(named: "local-sweep.mov")])

        do {
            _ = try await buildThunderboltShowdownPriorCandidateArtifact(
                corpus: setup.videos,
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: makeInterruptTestHardware(),
                setup: setup,
                currentCanonicalArtifact: nil,
                preflight: .localPriorGap,
                burstConfigRunner: { _, _, _, _, _, _, _ in
                    Issue.record("Local burst runner should not execute after sweep cancellation")
                    return ThunderboltBurstResult(
                        wallSeconds: 1,
                        completed: 1,
                        failed: 0,
                        completionSeconds: [1]
                    )
                },
                frameCountEstimator: { _ in
                    Issue.record("Frame counting should not run after sweep cancellation")
                    return []
                },
                localAffineSampleCollector: { _, _, _, _ in
                    Issue.record("Affine sample collection should not run after sweep cancellation")
                    return []
                },
                loopCancellationCheck: { point in
                    guard point == .priorLocalSweep else { return }
                    if loopChecks.incrementAndGet() == 1 {
                        throw BenchmarkInterruptError.interrupted
                    }
                }
            )
            Issue.record("Expected prior local sweep to throw interruption")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(loopChecks.value() == 1)
    }

    @Test("remote telemetry loop throws when cancellation is checked inside telemetry collection")
    func remoteTelemetryLoopThrowsWhenCancellationCheckedInsideTelemetryCollection() async {
        let loopChecks = LockedIntCounter()
        let roundTripCalls = LockedIntCounter()
        let worker = ThunderboltBoundWorkerSpec(
            host: "worker-a",
            connectHost: "worker-a",
            slots: 1,
            sourceIP: "10.0.0.10",
            bridgeName: "bridge0"
        )
        let sample = ThunderboltRemoteMaintenancePreparedSample(
            video: makeInterruptTestVideo(named: "remote-telemetry.mov"),
            frameCount: 120,
            sha256: "sha256:test"
        )

        do {
            _ = try await collectThunderboltRemoteMaintenanceTelemetryInterruptibly(
                worker: worker,
                workerSignature: "sig-remote-a",
                samples: [sample],
                port: 7_000,
                connectTimeout: 100,
                roundTripRunner: { _, _, _, _, _, _ in
                    roundTripCalls.increment()
                    return ThunderboltRoundTripResult(
                        success: true,
                        sendSeconds: 0.01,
                        processNanos: 1,
                        receiveSeconds: 0.01,
                        totalSeconds: 0.02
                    )
                },
                loopCancellationCheck: { point in
                    guard point == .priorRemoteTelemetry else { return }
                    if loopChecks.incrementAndGet() == 2 {
                        throw BenchmarkInterruptError.interrupted
                    }
                }
            )
            Issue.record("Expected remote telemetry loop to throw interruption")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(loopChecks.value() >= 2)
        #expect(roundTripCalls.value() == 0)
    }

    @Test("showdown rethrows interrupted prior maintenance instead of continuing in auto mode")
    func showdownRethrowsInterruptedPriorMaintenanceInAutoMode() async {
        let maintenanceCalls = LockedIntCounter()
        let setup = makeInterruptTestShowdownSetup(videos: [makeInterruptTestVideo(named: "showdown.mov")])

        do {
            try await benchmarkThunderboltMeasuredShowdown(
                corpus: setup.videos,
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: makeInterruptTestHardware(),
                profiles: [],
                modelMode: .auto,
                priorUpdatePolicy: .candidateOnly,
                preparedSetup: setup,
                priorMaintenanceRunner: { _, _, _, _, _, _, _, _, _ in
                    maintenanceCalls.increment()
                    throw BenchmarkInterruptError.interrupted
                }
            )
            Issue.record("Expected prior maintenance interruption to abort showdown")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(maintenanceCalls.value() == 1)
    }

    @Test("non-JSON post-sweep prior emission rethrows interruption instead of reporting ordinary failure")
    func nonJSONPostSweepPriorEmissionRethrowsInterruption() async {
        let corpus = [makeInterruptTestVideo(named: "post-sweep-nonjson.mov")]
        var outputLines: [String] = []

        do {
            _ = try await emitThunderboltPriorAfterBurstSweepIfNeeded(
                corpus: corpus,
                videos: corpus,
                runs: [],
                workers: [],
                port: 7_000,
                connectTimeout: 100,
                hardware: makeInterruptTestHardware(),
                preset: defaultVideoPreset,
                timeout: 120,
                emitPrior: { _, _, _, _, _, _, _, _, _, _, _ in
                    throw BenchmarkInterruptError.interrupted
                },
                outputLine: { outputLines.append($0) }
            )
            Issue.record("Expected post-sweep prior emission interruption to abort the run")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(outputLines.isEmpty)
    }

    @Test("JSON post-sweep prior emission rethrows interruption instead of reporting ordinary failure")
    func jsonPostSweepPriorEmissionRethrowsInterruption() async {
        let corpus = [makeInterruptTestVideo(named: "post-sweep-json.mov")]
        var outputLines: [String] = []

        do {
            _ = try await emitThunderboltJSONPriorAfterBurstSweepIfNeeded(
                corpus: corpus,
                videos: corpus,
                runs: [],
                workers: [],
                port: 7_000,
                connectTimeout: 100,
                hardware: makeInterruptTestHardware(),
                preset: defaultVideoPreset,
                timeout: 120,
                emitPrior: { _, _, _, _, _, _, _, _, _, _, _ in
                    throw BenchmarkInterruptError.interrupted
                },
                outputLine: { outputLines.append($0) }
            )
            Issue.record("Expected JSON post-sweep prior emission interruption to abort the run")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(outputLines.isEmpty)
    }

    @Test("optimized burst sweep rethrows interruption instead of converting it into penalty rows")
    func optimizedBurstSweepRethrowsInterruption() async {
        let evalCalls = LockedIntCounter()

        do {
            _ = try await optimizeBurstConcurrency(
                ceilings: [1, 1, 1, 1, 1],
                evaluate: { _ in
                    if evalCalls.incrementAndGet() == 2 {
                        throw BenchmarkInterruptError.interrupted
                    }
                    return 1.0
                },
                numVideos: 6,
                topK: 0
            )
            Issue.record("Expected optimized burst sweep interruption to abort the search")
        } catch let error as BenchmarkInterruptError {
            switch error {
            case .interrupted:
                break
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(evalCalls.value() == 2)
    }

    @Test("optimized burst sweep preserves penalty rows for ordinary evaluation failures")
    func optimizedBurstSweepPreservesPenaltyRowsForOrdinaryFailures() async throws {
        let result = try await optimizeBurstConcurrency(
            ceilings: [1, 1, 1, 1, 1],
            evaluate: { config in
                if config == [0, 1, 0, 0, 0] {
                    throw InterruptTestFailure.ordinaryEvaluationFailure
                }
                return 1.0 + Double(config.reduce(0, +))
            },
            numVideos: 6,
            topK: 0
        )

        #expect(result.history.contains { $0.elapsed == 1_000_000.0 })
    }

    @Test("direct thunderbolt CA run aborts after cancellation instead of waiting for later arrivals")
    func directThunderboltCARunAbortsAfterCancellation() async {
        let runnerCalls = LockedIntCounter()
        let setup = makeInterruptTestShowdownSetup(
            videos: [
                makeInterruptTestVideo(named: "ca-direct-1.mov"),
                makeInterruptTestVideo(named: "ca-direct-2.mov"),
            ]
        )

        let runTask = Task {
            try await runThunderboltCA(
                corpus: setup.videos,
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: makeInterruptTestHardware(),
                policy: .fifo,
                profile: .trickle,
                preparedSetup: setup,
                localVideoRunner: { _, _, _, _, _, _ in
                    runnerCalls.increment()
                    return true
                }
            )
        }

        let firstJobRan = await waitForCondition(timeoutSeconds: 5.0) {
            runnerCalls.value() == 1
        }
        if !firstJobRan {
            runTask.cancel()
            Issue.record("Expected first direct CA job to run before cancellation")
            return
        }

        runTask.cancel()

        do {
            _ = try await runTask.value
            Issue.record("Expected direct CA run cancellation to abort the run")
        } catch {
            #expect(isBenchmarkInterrupted(error, interruptState: nil))
        }

        #expect(runnerCalls.value() == 1)
    }

    @Test("direct thunderbolt CA run aborts if cancellation lands after dispatch handoff and before the next job starts")
    func directThunderboltCARunAbortsAtDispatchHandoff() async {
        let runnerCalls = LockedIntCounter()
        let secondDispatchReached = LockedIntCounter()
        let handoffGate = InterruptAsyncGate()
        let setup = makeInterruptTestShowdownSetup(
            videos: [
                makeInterruptTestVideo(named: "ca-handoff-1.mov"),
                makeInterruptTestVideo(named: "ca-handoff-2.mov"),
            ]
        )

        let runTask = Task {
            try await runThunderboltCA(
                corpus: setup.videos,
                preset: defaultVideoPreset,
                timeout: 120,
                hardware: makeInterruptTestHardware(),
                policy: .fifo,
                profile: .allAtOnce,
                preparedSetup: setup,
                localVideoRunner: { _, _, _, _, _, _ in
                    runnerCalls.increment()
                    return true
                },
                dispatchHandoffHook: { _, videoIndex in
                    guard videoIndex == 1 else { return }
                    secondDispatchReached.increment()
                    await handoffGate.wait()
                }
            )
        }

        let reachedSecondDispatch = await waitForCondition(timeoutSeconds: 5.0) {
            secondDispatchReached.value() == 1
        }
        if !reachedSecondDispatch {
            runTask.cancel()
            await handoffGate.open()
            Issue.record("Expected direct CA run to reach the second dispatch handoff")
            return
        }

        runTask.cancel()
        await handoffGate.open()

        do {
            _ = try await runTask.value
            Issue.record("Expected direct CA handoff cancellation to abort the run")
        } catch {
            #expect(isBenchmarkInterrupted(error, interruptState: nil))
        }

        #expect(runnerCalls.value() == 1)
    }
}

private final class InterruptTrigger: @unchecked Sendable {
    private let lock = NSLock()
    private var onInterrupt: (() -> Void)?

    func install(_ onInterrupt: @escaping () -> Void) {
        lock.lock()
        self.onInterrupt = onInterrupt
        lock.unlock()
    }

    func hasHandler() -> Bool {
        lock.lock()
        let value = onInterrupt != nil
        lock.unlock()
        return value
    }

    func fire() {
        lock.lock()
        let callback = onInterrupt
        lock.unlock()
        callback?()
    }
}

private final class LockedIntCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    func increment() {
        lock.lock()
        count += 1
        lock.unlock()
    }

    func incrementAndGet() -> Int {
        lock.lock()
        count += 1
        let value = count
        lock.unlock()
        return value
    }

    func value() -> Int {
        lock.lock()
        let value = count
        lock.unlock()
        return value
    }
}

private actor InterruptAsyncGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { continuation in
            if isOpen {
                continuation.resume()
                return
            }
            waiters.append(continuation)
        }
    }

    func open() {
        guard !isOpen else { return }
        isOpen = true
        let currentWaiters = waiters
        waiters.removeAll(keepingCapacity: false)
        for waiter in currentWaiters {
            waiter.resume()
        }
    }
}

private func waitForCondition(
    timeoutSeconds: TimeInterval,
    pollEveryMillis: UInt64 = 10,
    condition: @escaping @Sendable () -> Bool
) async -> Bool {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        if condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(Int(pollEveryMillis)))
    }
    return condition()
}

private func makeInterruptTestVideo(named name: String) -> MediaFile {
    MediaFile(
        path: "/tmp/\(name)",
        name: name,
        type: .video,
        sizeBytes: 1_024
    )
}

private func makeInterruptTestHardware() -> HardwareProfile {
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

private func makeInterruptTestShowdownSetup(
    videos: [MediaFile],
    reachableWorkers: [ThunderboltBoundWorkerSpec] = []
) -> ThunderboltCARunSetup {
    ThunderboltCARunSetup(
        port: 7_000,
        connectTimeout: 100,
        videos: videos,
        videoCosts: makeDefaultResolvedVideoCosts(count: videos.count),
        priorTable: BenchmarkPriorTable(),
        localSignature: "sig-local",
        localSlotCount: 1,
        localMSPerFrameC1: 1.0,
        sourceHashes: [:],
        slots: [.local(index: 1)],
        machineProfiles: [
            ThunderboltCAMachineProfile(
                id: "local",
                msPerFrameC1: 1.0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0
            ),
        ],
        slotBindings: [
            ThunderboltCASlotBinding(machineIndex: 0, slotID: "local#s1"),
        ],
        machineIndexByHost: [:],
        modelInputs: [
            ThunderboltCAModelInputRow(
                machineID: "local",
                slotCount: 1,
                msPerFrameC1: 1.0,
                msSource: "test",
                curveSource: "test",
                txInMS: 0
            ),
        ],
        diagnostics: ThunderboltCAModelDiagnostics(
            mode: .auto,
            coverageRows: [],
            strictExclusions: [],
            reachableWorkerCount: reachableWorkers.count,
            reachableSlotCount: reachableWorkers.reduce(0) { $0 + $1.slots },
            modeledWorkerCount: 0,
            modeledSlotCount: 0,
            fallbackActive: false,
            localPriorGap: true,
            remotePriorGap: !reachableWorkers.isEmpty
        ),
        reachableWorkers: reachableWorkers,
        workerCapsByHost: [:]
    )
}

private enum InterruptTestFailure: Error {
    case ordinaryEvaluationFailure
}
