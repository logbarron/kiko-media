import Foundation
import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt CA completion accounting")
struct ThunderboltCACompletionAccountingTests {
    @Test("prepared setup model inputs preserve tail telemetry values through run observability")
    func preparedSetupModelInputs_preserveTailTelemetryValuesThroughRunObservability() async throws {
        let fixture = try CARunFixture()
        defer { fixture.cleanup() }

        let setup = makePreparedSetup(
            video: fixture.video,
            mode: .remoteDispatch,
            includeSourceHash: true
        )
        let observed = try await runThunderboltCA(
            corpus: [fixture.video],
            preset: "test-preset",
            timeout: 1,
            hardware: accountingHardwareProfile(),
            policy: .fifo,
            profile: .allAtOnce,
            preparedSetup: setup,
            localVideoRunner: { _, _, _, _, _, _ in
                Issue.record("Local runner should not execute when prepared remote dispatch succeeds")
                return false
            },
            roundTripRunner: { _, _, _, _, _, _ in
                ThunderboltRoundTripResult(
                    success: true,
                    sendSeconds: 0.001,
                    processNanos: 8_000_000,
                    receiveSeconds: 0.001,
                    totalSeconds: 0.002,
                    txOutMS: 7,
                    publishOverheadMS: 11
                )
            }
        )

        let remoteInput = observed.observability.modelInputs.first(where: { $0.machineID == accountingRemoteMachineID })
        let localInput = observed.observability.modelInputs.first(where: { $0.machineID == accountingLocalMachineID })

        #expect(remoteInput?.txInMS == 5)
        #expect(remoteInput?.txOutMS == 7)
        #expect(remoteInput?.publishOverheadMS == 11)
        #expect(localInput?.txInMS == 0)
        #expect(localInput?.txOutMS == 0)
        #expect(localInput?.publishOverheadMS == 0)
    }

    @Test("remote success contributes adaptation to remote machine")
    func remoteSuccess_contributesRemoteAdaptation() async throws {
        let fixture = try CARunFixture()
        defer { fixture.cleanup() }

        let setup = makePreparedSetup(
            video: fixture.video,
            mode: .remoteDispatch,
            includeSourceHash: true
        )

        let observed = try await runThunderboltCA(
            corpus: [fixture.video],
            preset: "test-preset",
            timeout: 1,
            hardware: accountingHardwareProfile(),
            policy: .fifo,
            profile: .trickle,
            preparedSetup: setup,
            localVideoRunner: { _, _, _, _, _, _ in
                Issue.record("Local runner should not execute when remote dispatch succeeds")
                return false
            },
            roundTripRunner: { _, _, _, _, _, _ in
                ThunderboltRoundTripResult(
                    success: true,
                    sendSeconds: 0.001,
                    processNanos: 8_000_000,
                    receiveSeconds: 0.001,
                    totalSeconds: 0.002
                )
            }
        )

        #expect(observed.result.totalJobs == 1)
        #expect(observed.result.successfulJobs == 1)
        #expect(observed.result.failedCount == 0)
        #expect(observed.result.jobs.first?.actualExecutor == accountingRemoteWorker.host)
        #expect(
            adaptationCompletions(
                rows: observed.observability.adaptation,
                machineID: accountingRemoteMachineID
            ) == 1
        )
        #expect(
            adaptationCompletions(
                rows: observed.observability.adaptation,
                machineID: accountingLocalMachineID
            ) == 0
        )
    }

    @Test("remote failure fallback completes with the shared local adaptation path")
    func remoteFailureFallback_completesWithTheSharedLocalAdaptationPath() async throws {
        let fixture = try CARunFixture()
        defer { fixture.cleanup() }

        let setup = makePreparedSetup(
            video: fixture.video,
            mode: .remoteDispatch,
            includeSourceHash: true
        )

        let observed = try await runThunderboltCA(
            corpus: [fixture.video],
            preset: "test-preset",
            timeout: 1,
            hardware: accountingHardwareProfile(),
            policy: .fifo,
            profile: .allAtOnce,
            preparedSetup: setup,
            localVideoRunner: { _, _, _, _, _, _ in
                try? await Task.sleep(nanoseconds: 1_000_000)
                return true
            },
            roundTripRunner: { _, _, _, _, _, _ in
                ThunderboltRoundTripResult(
                    success: false,
                    sendSeconds: 0.001,
                    processNanos: 2_000_000,
                    receiveSeconds: 0.001,
                    totalSeconds: 0.002,
                    firstRunningLatencySecondsEstimate: 0.001,
                    slotHealthDownOnFailure: true
                )
            }
        )

        #expect(observed.result.totalJobs == 1)
        #expect(observed.result.successfulJobs == 1)
        #expect(observed.result.failedCount == 0)
        #expect(observed.result.jobs.first?.actualExecutor == "local-fallback")
        #expect(observed.observability.predictions.first?.executorMismatch == true)
        #expect(
            adaptationCompletions(
                rows: observed.observability.adaptation,
                machineID: accountingLocalMachineID
            ) == 1
        )
        #expect(
            adaptationCompletions(
                rows: observed.observability.adaptation,
                machineID: accountingRemoteMachineID
            ) == 0
        )
        let localAdaptation = try #require(
            observed.observability.adaptation.first { $0.machineID == accountingLocalMachineID }
        )
        #expect(localAdaptation.finalMSPerFrameC1 < localAdaptation.initialMSPerFrameC1)
    }

    @Test("local native completion still contributes adaptation")
    func localNativeCompletion_stillContributesAdaptation() async throws {
        let fixture = try CARunFixture()
        defer { fixture.cleanup() }

        let setup = makePreparedSetup(
            video: fixture.video,
            mode: .localOnly,
            includeSourceHash: false
        )

        let observed = try await runThunderboltCA(
            corpus: [fixture.video],
            preset: "test-preset",
            timeout: 1,
            hardware: accountingHardwareProfile(),
            policy: .fifo,
            profile: .allAtOnce,
            preparedSetup: setup,
            localVideoRunner: { _, _, _, _, _, _ in
                try? await Task.sleep(nanoseconds: 1_000_000)
                return true
            },
            roundTripRunner: { _, _, _, _, _, _ in
                Issue.record("Remote runner should not execute for local-only setup")
                return ThunderboltRoundTripResult(
                    success: false,
                    sendSeconds: 0,
                    processNanos: 0,
                    receiveSeconds: 0,
                    totalSeconds: 0
                )
            }
        )

        #expect(observed.result.totalJobs == 1)
        #expect(observed.result.successfulJobs == 1)
        #expect(observed.result.failedCount == 0)
        #expect(observed.result.jobs.first?.actualExecutor == "local")
        #expect(
            adaptationCompletions(
                rows: observed.observability.adaptation,
                machineID: accountingLocalMachineID
            ) == 1
        )
    }

    @Test("held observability keeps original decision timing through relaunch")
    func heldObservability_keepsOriginalDecisionTimingThroughRelaunch() async throws {
        let fixture = try CARunFixture()
        defer { fixture.cleanup() }

        let first = try fixture.makeVideo(named: "held-first.mov", fillByte: 0x41, sizeBytes: 25_000_000)
        let second = try fixture.makeVideo(named: "held-local.mov", fillByte: 0x42, sizeBytes: 5_000_000)
        let held = try fixture.makeVideo(named: "held-second.mov", fillByte: 0x43, sizeBytes: 1_000_000)
        let observed = try await runThunderboltCA(
            corpus: [first, second, held],
            preset: "test-preset",
            timeout: 1,
            hardware: accountingHardwareProfile(),
            policy: .complexityAware,
            profile: .allAtOnce,
            preparedSetup: makeHeldObservabilitySetup(videos: [first, second, held]),
            localVideoRunner: { _, _, _, _, _, _ in
                try? await Task.sleep(for: .milliseconds(400))
                return true
            },
            roundTripRunner: { _, video, _, _, _, _ in
                if video.name == first.name {
                    Thread.sleep(forTimeInterval: 1.2)
                    return ThunderboltRoundTripResult(
                        success: true,
                        sendSeconds: 0.001,
                        processNanos: 1_200_000_000,
                        receiveSeconds: 0.001,
                        totalSeconds: 1.202
                    )
                }
                Thread.sleep(forTimeInterval: 0.02)
                return ThunderboltRoundTripResult(
                    success: true,
                    sendSeconds: 0.001,
                    processNanos: 20_000_000,
                    receiveSeconds: 0.001,
                    totalSeconds: 0.022
                )
            }
        )

        let jobsByVideoName = Dictionary(
            uniqueKeysWithValues: observed.result.jobs.map { ($0.videoName, $0) }
        )
        let predictionsByVideoName = Dictionary(
            uniqueKeysWithValues: zip(observed.result.jobs, observed.observability.predictions).map { pair in
                (pair.0.videoName, pair.1)
            }
        )
        let firstJob = try #require(jobsByVideoName[first.name])
        let heldJob = try #require(jobsByVideoName[held.name])
        let heldPrediction = try #require(predictionsByVideoName[held.name])
        let firstCompletedAt = try #require(firstJob.completedAtSeconds)
        let heldPredictedSlotReadyMS = try #require(heldPrediction.predictedSlotReadyMS)
        let heldActualStartMS = try #require(heldPrediction.actualStartMS)
        let heldActualDoneMS = try #require(heldPrediction.actualDoneMS)

        #expect(!heldJob.actualExecutor.isEmpty)
        #expect(heldPrediction.waited == true)
        #expect(heldPrediction.decisionAtSeconds >= heldJob.arrivalAtSeconds)
        #expect(heldPrediction.decisionAtSeconds < firstCompletedAt)
        #expect(heldPredictedSlotReadyMS > 0)
        #expect(heldActualDoneMS > heldActualStartMS)
    }

    @Test("benchmark runtime waits for arrivals before resuming coordinator batch scheduling")
    func benchmarkRuntimeWaitsForArrivalsBeforeResumingCoordinatorBatchScheduling() async throws {
        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .fifo,
            videoCosts: makeResolvedVideoCosts(frameCounts: [100]),
            machineProfiles: [
                ThunderboltCAMachineProfile(
                    id: "m0",
                    msPerFrameC1: 1.0,
                    degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                    txInMS: 0
                ),
            ],
            slotBindings: [
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "m0-s0"),
            ]
        )
        let resumed = QueueWaitProbe()
        let waitTask = Task {
            await runtimeEngine.waitForWork()
            await resumed.markReady()
        }
        defer { waitTask.cancel() }

        try? await Task.sleep(for: .milliseconds(50))
        #expect(await resumed.isReady() == false)

        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        let didResume = try await waitUntil(timeoutSeconds: 1) {
            await resumed.isReady()
        }
        #expect(didResume)

        let batch = await runtimeEngine.scheduleBatch(freeSlotOrdinals: [0], nowSeconds: 0)
        #expect(batch.count == 1)
        #expect(batch.first?.slotOrdinal == 0)
        #expect(batch.first?.item.index == 0)
    }

    @Test("coordinator batch fills all free slots and can include a future reservation")
    func coordinatorBatch_fillsAllFreeSlotsAndCanIncludeFutureReservation() async {
        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .complexityAware,
            videoCosts: makeResolvedVideoCosts(frameCounts: [100, 50, 80]),
            machineProfiles: [
                ThunderboltCAMachineProfile(
                    id: "m0",
                    msPerFrameC1: 1.0,
                    degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                    txInMS: 0
                ),
                ThunderboltCAMachineProfile(
                    id: "m1",
                    msPerFrameC1: 1.0,
                    degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                    txInMS: 0
                ),
            ],
            slotBindings: [
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "m0-s0"),
                ThunderboltCASlotBinding(machineIndex: 1, slotID: "m1-s0"),
            ]
        )

        for i in 0..<3 {
            await runtimeEngine.enqueue(index: i, arrivalAtSeconds: 0)
        }
        await runtimeEngine.finishArrivals()

        let batch = await runtimeEngine.scheduleBatch(freeSlotOrdinals: [0, 1], nowSeconds: 0)
        let readyNow = batch.filter { ($0.item.predictedSlotReadyMS ?? 0) <= 0 }
        let futureReady = batch.filter { ($0.item.predictedSlotReadyMS ?? 0) > 0 }

        #expect(readyNow.count == 2)
        #expect(Set(readyNow.map(\.item.index)).count == 2)
        #expect(futureReady.count == 1)
        #expect(Set(batch.map(\.item.index)) == [0, 1, 2])
    }

    @Test("fifo batch distributes work to free slots")
    func fifoBatch_distributesWorkToFreeSlots() async {
        let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
            policy: .fifo,
            videoCosts: makeResolvedVideoCosts(frameCounts: [100, 50]),
            machineProfiles: [
                ThunderboltCAMachineProfile(
                    id: "m0",
                    msPerFrameC1: 1.0,
                    degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)],
                    txInMS: 0
                ),
            ],
            slotBindings: [
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "s0"),
                ThunderboltCASlotBinding(machineIndex: 0, slotID: "s1"),
            ]
        )

        await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
        await runtimeEngine.enqueue(index: 1, arrivalAtSeconds: 0)
        await runtimeEngine.finishArrivals()

        let batch = await runtimeEngine.scheduleBatch(freeSlotOrdinals: [0, 1], nowSeconds: 0)
        #expect(batch.count == 2)
        #expect(batch[0].slotOrdinal == 0)
        #expect(batch[0].item.index == 0)
        #expect(batch[1].slotOrdinal == 1)
        #expect(batch[1].item.index == 1)
    }

    @Test("benchmark completion does not requeue stable held dispatches")
    func benchmarkCompletion_doesNotRequeueStableHeldDispatches() async {
        let runtimeEngine = makeWS3HeldRuntimeEngine()

        let initialHold = await makeWS3FutureHold(runtimeEngine: runtimeEngine)
        #expect(initialHold.slotOrdinal == 1)
        #expect(await runtimeEngine.pendingIndicesSnapshot().isEmpty)

        #expect(await runtimeEngine.markCompletedSlotsReady([0], nowSeconds: 0.1) == false)
        #expect(await runtimeEngine.releaseReadyHeldDispatches(nowSeconds: 0.1) == false)
        #expect(await runtimeEngine.pendingIndicesSnapshot().isEmpty)
        #expect(await runtimeEngine.heldDispatchSnapshot().count == 1)
    }

    @Test("benchmark hold invalidation uses targetReadyAtDriftThresholdMS")
    func benchmarkHoldInvalidation_usesTargetReadyAtDriftThresholdMS() async {
        let runtimeEngine = makeWS3HeldRuntimeEngine()
        let initialHold = await makeWS3FutureHold(runtimeEngine: runtimeEngine)
        let driftSeconds = CAHoldInvalidation.targetReadyAtDriftThresholdMS / 1_000.0

        await runtimeEngine.markSlotReady(
            slotOrdinal: initialHold.slotOrdinal,
            nowSeconds: initialHold.targetReadyAtSeconds + driftSeconds - 0.001
        )
        #expect(await runtimeEngine.markCompletedSlotsReady([0], nowSeconds: 0.1) == false)
        #expect(await runtimeEngine.pendingIndicesSnapshot().isEmpty)
        #expect(await runtimeEngine.heldDispatchSnapshot().count == 1)

        await runtimeEngine.markSlotReady(
            slotOrdinal: initialHold.slotOrdinal,
            nowSeconds: initialHold.targetReadyAtSeconds + driftSeconds + 0.001
        )
        #expect(await runtimeEngine.markCompletedSlotsReady([0], nowSeconds: 0.2))
        #expect(await runtimeEngine.pendingIndicesSnapshot() == [1])
        #expect(await runtimeEngine.heldDispatchSnapshot().isEmpty)
    }

    @Test("benchmark hold wake releases once without stale loop")
    func benchmarkHoldWake_releasesOnceWithoutStaleLoop() async {
        let runtimeEngine = makeWS3HeldRuntimeEngine()
        let initialHold = await makeWS3FutureHold(runtimeEngine: runtimeEngine)

        #expect(
            await runtimeEngine.releaseReadyHeldDispatches(
                nowSeconds: initialHold.wakeAtSeconds + 0.01
            )
        )
        #expect(await runtimeEngine.heldDispatchSnapshot().isEmpty)
        #expect(await runtimeEngine.pendingIndicesSnapshot() == [1])
        await runtimeEngine.markSlotReady(
            slotOrdinal: initialHold.slotOrdinal,
            nowSeconds: initialHold.wakeAtSeconds + 0.01
        )

        let batch = await runtimeEngine.scheduleRuntimeBatch(
            freeSlotOrdinals: [0, 1],
            nowSeconds: initialHold.wakeAtSeconds + 0.01,
            totalJobCount: 1
        )
        #expect(batch.dispatches.count == 1)
        #expect(batch.dispatches.first?.slotOrdinal == 1)
        #expect(batch.dispatches.first?.item.index == 1)
        #expect(await runtimeEngine.releaseReadyHeldDispatches(nowSeconds: initialHold.wakeAtSeconds + 0.02) == false)
        #expect(await runtimeEngine.heldDispatchSnapshot().isEmpty)
    }
}

private enum CAAccountingSetupMode {
    case localOnly
    case remoteDispatch
}

private struct CARunFixture {
    let tempDirectory: URL
    let video: MediaFile

    init() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("tb-ca-accounting-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let videoURL = root.appendingPathComponent("sample.mov")
        let bytes = Data(repeating: 0x61, count: 4_096)
        try bytes.write(to: videoURL, options: .atomic)

        self.tempDirectory = root
        self.video = MediaFile(
            path: videoURL.path,
            name: videoURL.lastPathComponent,
            type: .video,
            sizeBytes: bytes.count
        )
    }

    func makeVideo(named name: String, fillByte: UInt8, sizeBytes: Int) throws -> MediaFile {
        let videoURL = tempDirectory.appendingPathComponent(name)
        try Data(repeating: fillByte, count: 4_096).write(to: videoURL, options: .atomic)
        return MediaFile(
            path: videoURL.path,
            name: videoURL.lastPathComponent,
            type: .video,
            sizeBytes: sizeBytes
        )
    }

    func cleanup() {
        try? FileManager.default.removeItem(at: tempDirectory)
    }
}

private let accountingCurve = [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)]
private let accountingLocalMachineID = "local"
private let accountingRemoteMachineID = "worker-a:7000"
private let accountingRemoteWorker = ThunderboltBoundWorkerSpec(
    host: "worker-a",
    connectHost: "127.0.0.2",
    slots: 1,
    sourceIP: "10.0.0.10",
    bridgeName: "bridge0"
)

private func makePreparedSetup(
    video: MediaFile,
    mode: CAAccountingSetupMode,
    includeSourceHash: Bool
) -> ThunderboltCARunSetup {
    let localMachine = ThunderboltCAMachineProfile(
        id: accountingLocalMachineID,
        msPerFrameC1: 1.0,
        degradationCurve: accountingCurve,
        txInMS: 0
    )

    let slots: [ThunderboltCASlot]
    let machineProfiles: [ThunderboltCAMachineProfile]
    let slotBindings: [ThunderboltCASlotBinding]
    let machineIndexByHost: [String: Int]
    let reachableWorkers: [ThunderboltBoundWorkerSpec]
    let diagnostics: ThunderboltCAModelDiagnostics

    switch mode {
    case .localOnly:
        slots = [.local(index: 1)]
        machineProfiles = [localMachine]
        slotBindings = [ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-1")]
        machineIndexByHost = [:]
        reachableWorkers = []
        diagnostics = ThunderboltCAModelDiagnostics(
            mode: .strict,
            coverageRows: [],
            strictExclusions: [],
            reachableWorkerCount: 0,
            reachableSlotCount: 0,
            modeledWorkerCount: 0,
            modeledSlotCount: 0,
            fallbackActive: false,
            localPriorGap: false,
            remotePriorGap: false
        )
    case .remoteDispatch:
        let remoteMachine = ThunderboltCAMachineProfile(
            id: accountingRemoteMachineID,
            msPerFrameC1: 0.8,
            degradationCurve: accountingCurve,
            txInMS: 5,
            txOutMS: 7,
            publishOverheadMS: 11
        )
        slots = [.remote(worker: accountingRemoteWorker, index: 1)]
        machineProfiles = [localMachine, remoteMachine]
        slotBindings = [ThunderboltCASlotBinding(machineIndex: 1, slotID: "remote-1")]
        machineIndexByHost = [accountingRemoteWorker.host: 1]
        reachableWorkers = [accountingRemoteWorker]
        diagnostics = ThunderboltCAModelDiagnostics(
            mode: .strict,
            coverageRows: [],
            strictExclusions: [],
            reachableWorkerCount: 1,
            reachableSlotCount: 1,
            modeledWorkerCount: 1,
            modeledSlotCount: 1,
            fallbackActive: false,
            localPriorGap: false,
            remotePriorGap: false
        )
    }

    let modelInputs = machineProfiles.enumerated().map { index, profile in
        let slotCount = slotBindings.reduce(into: 0) { partial, binding in
            if binding.machineIndex == index {
                partial += 1
            }
        }
        return ThunderboltCAModelInputRow(
            machineID: profile.id,
            slotCount: slotCount,
            msPerFrameC1: profile.msPerFrameC1,
            msSource: "test",
            curveSource: "test",
            txInMS: profile.txInMS,
            txOutMS: profile.txOutMS,
            publishOverheadMS: profile.publishOverheadMS
        )
    }

    return ThunderboltCARunSetup(
        port: 7_000,
        connectTimeout: 100,
        videos: [video],
        videoCosts: [makeDefaultResolvedVideoCost()],
        priorTable: BenchmarkPriorTable(),
        localSignature: "local-test-signature",
        localSlotCount: 1,
        localMSPerFrameC1: 1.0,
        sourceHashes: includeSourceHash ? [video.path: "sha-test"] : [:],
        slots: slots,
        machineProfiles: machineProfiles,
        slotBindings: slotBindings,
        machineIndexByHost: machineIndexByHost,
        modelInputs: modelInputs,
        diagnostics: diagnostics,
        reachableWorkers: reachableWorkers,
        workerCapsByHost: [:]
    )
}

private func makeWS3HeldRuntimeEngine() -> CABenchmarkRuntimeEngine {
    makeThunderboltCABenchmarkRuntimeEngine(
        policy: .complexityAware,
        videoCosts: makeResolvedVideoCosts(frameCounts: [100, 100]),
        machineProfiles: [
            ThunderboltCAMachineProfile(
                id: "local",
                msPerFrameC1: 50.0,
                degradationCurve: accountingCurve,
                txInMS: 0
            ),
            ThunderboltCAMachineProfile(
                id: "remote",
                msPerFrameC1: 1.0,
                degradationCurve: accountingCurve,
                txInMS: 0
            ),
        ],
        slotBindings: [
            ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-s1"),
            ThunderboltCASlotBinding(machineIndex: 1, slotID: "remote-s1"),
        ]
    )
}

private func makeWS3FutureHold(
    runtimeEngine: CABenchmarkRuntimeEngine
) async -> CABenchmarkRuntimeEngine.HeldDispatchSnapshot {
    await runtimeEngine.enqueue(index: 0, arrivalAtSeconds: 0)
    await runtimeEngine.enqueue(index: 1, arrivalAtSeconds: 0)
    await runtimeEngine.finishArrivals()
    await runtimeEngine.markSlotReady(slotOrdinal: 1, nowSeconds: 3.0)

    let batch = await runtimeEngine.scheduleRuntimeBatch(
        freeSlotOrdinals: [0],
        nowSeconds: 0,
        totalJobCount: 2
    )
    #expect(batch.dispatches.count == 1)
    #expect(batch.dispatches.first?.slotOrdinal == 0)
    #expect(batch.dispatches.first?.item.index == 0)
    #expect(batch.madeProgress)
    guard let held = await runtimeEngine.heldDispatchSnapshot().first else {
        fatalError("Expected benchmark runtime to create a held dispatch")
    }
    #expect(held.index == 1)
    #expect(held.slotOrdinal == 1)
    return held
}

private func makeHeldObservabilitySetup(videos: [MediaFile]) -> ThunderboltCARunSetup {
    let localMachine = ThunderboltCAMachineProfile(
        id: accountingLocalMachineID,
        msPerFrameC1: 50.0,
        degradationCurve: accountingCurve,
        txInMS: 0
    )
    let remoteMachine = ThunderboltCAMachineProfile(
        id: accountingRemoteMachineID,
        msPerFrameC1: 1.0,
        degradationCurve: accountingCurve,
        txInMS: 0
    )
    let modelInputs = [
        ThunderboltCAModelInputRow(
            machineID: localMachine.id,
            slotCount: 1,
            msPerFrameC1: localMachine.msPerFrameC1,
            msSource: "test(local)",
            curveSource: "test(local)",
            txInMS: 0
        ),
        ThunderboltCAModelInputRow(
            machineID: remoteMachine.id,
            slotCount: 1,
            msPerFrameC1: remoteMachine.msPerFrameC1,
            msSource: "test(remote)",
            curveSource: "test(remote)",
            txInMS: 0
        ),
    ]

    return ThunderboltCARunSetup(
        port: 7000,
        connectTimeout: 100,
        videos: videos,
        videoCosts: makeDefaultResolvedVideoCosts(count: videos.count),
        priorTable: BenchmarkPriorTable(),
        localSignature: "sig-local-held-accounting",
        localSlotCount: 1,
        localMSPerFrameC1: localMachine.msPerFrameC1,
        localFixedOverheadMS: 0,
        sourceHashes: Dictionary(uniqueKeysWithValues: videos.map { ($0.path, "sha-\($0.name)") }),
        slots: [
            .local(index: 0),
            .remote(worker: accountingRemoteWorker, index: 0),
        ],
        machineProfiles: [localMachine, remoteMachine],
        slotBindings: [
            ThunderboltCASlotBinding(machineIndex: 0, slotID: "local-slot"),
            ThunderboltCASlotBinding(machineIndex: 1, slotID: "remote-slot"),
        ],
        machineIndexByHost: [accountingRemoteWorker.host: 1],
        modelInputs: modelInputs,
        diagnostics: ThunderboltCAModelDiagnostics(
            mode: .strict,
            coverageRows: [],
            strictExclusions: [],
            reachableWorkerCount: 1,
            reachableSlotCount: 1,
            modeledWorkerCount: 1,
            modeledSlotCount: 1,
            fallbackActive: false,
            localPriorGap: false,
            remotePriorGap: false
        ),
        reachableWorkers: [accountingRemoteWorker],
        workerCapsByHost: [:]
    )
}

private func adaptationCompletions(rows: [ThunderboltCAAdaptationRow], machineID: String) -> Int {
    rows.first(where: { $0.machineID == machineID })?.completions ?? 0
}

private actor QueueWaitProbe {
    private var ready = false

    func markReady() {
        ready = true
    }

    func isReady() -> Bool {
        ready
    }
}

private func waitUntil(
    timeoutSeconds: TimeInterval,
    pollEveryMillis: UInt64 = 10,
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

private func accountingHardwareProfile() -> HardwareProfile {
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
