import Foundation
import KikoMediaCore

typealias ThunderboltCALocalVideoRunner = @Sendable (
    _ video: MediaFile,
    _ uploadId: String,
    _ thumbsDir: String,
    _ previewsDir: String,
    _ preset: String,
    _ timeout: Int
) async -> Bool

typealias ThunderboltCARoundTripRunner = @Sendable (
    _ worker: ThunderboltBoundWorkerSpec,
    _ video: MediaFile,
    _ sourceSHA256: String,
    _ port: Int,
    _ connectTimeout: Int,
    _ tempDir: String
) -> ThunderboltRoundTripResult

typealias ThunderboltCADispatchHandoffHook = @Sendable (
    _ slotOrdinal: Int,
    _ videoIndex: Int
) async -> Void

private func runThunderboltBlockingRoundTrip(
    roundTripRunner: @escaping ThunderboltCARoundTripRunner,
    worker: ThunderboltBoundWorkerSpec,
    video: MediaFile,
    sourceSHA: String,
    port: Int,
    connectTimeout: Int,
    tempDir: String
) async -> ThunderboltRoundTripResult {
    await Task.detached(priority: nil) {
        roundTripRunner(
            worker,
            video,
            sourceSHA,
            port,
            connectTimeout,
            tempDir
        )
    }.value
}

private actor ThunderboltCASlotCompletionQueue {
    private var completedSlots: [Int] = []

    func push(_ slotOrdinal: Int) {
        completedSlots.append(slotOrdinal)
    }

    func drain() -> [Int] {
        let drained = completedSlots
        completedSlots.removeAll(keepingCapacity: true)
        return drained
    }
}

private actor ThunderboltCAStore {
    private var jobs: [ThunderboltCAJobRecord] = []
    private var predictions: [ThunderboltCAPredictionSample] = []
    private var failed = 0

    func append(
        _ job: ThunderboltCAJobRecord,
        prediction: ThunderboltCAPredictionSample? = nil
    ) -> (completed: Int, failed: Int) {
        jobs.append(job)
        if let prediction {
            predictions.append(prediction)
        }
        if !job.success {
            failed += 1
        }
        return (jobs.count, failed)
    }

    func snapshot() -> [ThunderboltCAJobRecord] {
        jobs
    }

    func predictionSnapshot() -> [ThunderboltCAPredictionSample] {
        predictions
    }
}

// Narrow benchmark-owned seam so retained P4 suites can build the shared runtime
// directly from benchmark DTOs without going through the legacy adapter layer.
func makeThunderboltCABenchmarkRuntimeEngine(
    policy: CASchedulerPolicy,
    videoCosts: [CAResolvedVideoCost],
    machineProfiles: [ThunderboltCAMachineProfile],
    slotBindings: [ThunderboltCASlotBinding]
) -> CABenchmarkRuntimeEngine {
    CABenchmarkRuntimeEngine(
        policy: policy.sharedBenchmarkRuntimePolicy,
        videoCosts: videoCosts,
        machineProfiles: machineProfiles.map(\.sharedBenchmarkRuntimeMachineProfile),
        slotBindings: slotBindings.map(\.sharedBenchmarkRuntimeSlotBinding)
    )
}

func runThunderboltCA(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    policy: CASchedulerPolicy,
    profile: CAArrivalProfile,
    modelMode: ThunderboltCAModelMode = .strict,
    slotOverrides: ThunderboltCASlotOverrides? = nil,
    preparedSetup: ThunderboltCARunSetup? = nil,
    localVideoRunner: @escaping ThunderboltCALocalVideoRunner = runThunderboltLocalVideoJob,
    roundTripRunner: @escaping ThunderboltCARoundTripRunner = runThunderboltRoundTrip,
    dispatchHandoffHook: ThunderboltCADispatchHandoffHook? = nil,
    progress: (@Sendable (_ completed: Int, _ total: Int, _ failed: Int, _ elapsedSeconds: Double) async -> Void)? = nil
) async throws -> ThunderboltCAObservedRun {
    let setup = if let preparedSetup {
        preparedSetup
    } else {
        try await prepareThunderboltCARunSetup(
            corpus: corpus,
            preset: preset,
            hardware: hardware,
            slotOverrides: slotOverrides,
            mode: modelMode
        )
    }
    let port = setup.port
    let connectTimeout = setup.connectTimeout
    let videos = setup.videos
    let videoCosts = setup.videoCosts
    let sourceHashes = setup.sourceHashes
    let slots = setup.slots
    let machineProfiles = setup.machineProfiles
    let slotBindings = setup.slotBindings
    let machineIndexByHost = setup.machineIndexByHost
    let modelInputs = setup.modelInputs

    let arrivalOffsets = caArrivalOffsets(profile: profile, count: videos.count)
    let runtimeEngine = makeThunderboltCABenchmarkRuntimeEngine(
        policy: policy,
        videoCosts: videoCosts,
        machineProfiles: machineProfiles,
        slotBindings: slotBindings
    )
    let store = ThunderboltCAStore()

    let tempDir = makeTempDir("tb-ca")
    let thumbsDir = "\(tempDir)/thumbs"
    let previewsDir = "\(tempDir)/previews"
    try FileManager.default.createDirectory(atPath: thumbsDir, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(atPath: previewsDir, withIntermediateDirectories: true)
    defer { cleanup(tempDir) }

    let clock = ContinuousClock()
    let started = clock.now
    let machineIndexByHostSnapshot = machineIndexByHost
    let machineProfilesSnapshot = machineProfiles

    let slotChannelPairs: [(stream: AsyncStream<ThunderboltCADispatchItem>, continuation: AsyncStream<ThunderboltCADispatchItem>.Continuation)] = slots.indices.map { _ in
        AsyncStream<ThunderboltCADispatchItem>.makeStream()
    }
    let slotStreams = slotChannelPairs.map(\.stream)
    let slotContinuations = slotChannelPairs.map(\.continuation)

    let completionQueue = ThunderboltCASlotCompletionQueue()

    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask(priority: .high) {
            do {
                var previousOffset = 0.0
                for index in videos.indices {
                    try Task.checkCancellation()
                    let offset = arrivalOffsets[index]
                    let waitSeconds = offset - previousOffset
                    if waitSeconds > 0 {
                        let waitNanos = UInt64(waitSeconds * 1_000_000_000)
                        try await Task.sleep(nanoseconds: waitNanos)
                        try Task.checkCancellation()
                    }
                    try Task.checkCancellation()
                    await runtimeEngine.enqueue(index: index, arrivalAtSeconds: offset)
                    if index < videos.index(before: videos.endIndex),
                       arrivalOffsets[videos.index(after: index)] == offset {
                        await Task.yield()
                    }
                    previousOffset = offset
                }
                await runtimeEngine.finishArrivals()
            } catch {
                await runtimeEngine.finishArrivals()
                throw error
            }
        }

        for (slotOrdinal, slot) in slots.enumerated() {
            let channel = slotStreams[slotOrdinal]
            group.addTask {
                for await item in channel {
                    try Task.checkCancellation()
                    if let dispatchHandoffHook {
                        await dispatchHandoffHook(slotOrdinal, item.index)
                    }
                    let index = item.index
                    let video = videos[index]
                    let uploadId = String(format: "tb-ca-%03d-%06d", slotOrdinal + 1, index)
                    let frameCount = if videoCosts.indices.contains(index) {
                        max(1, videoCosts[index].frameCount)
                    } else {
                        CAProfileAndFallbackMath.fallbackFrameCount(durationSeconds: nil, frameCount: nil)
                    }
                    let dispatchStartedAt = (clock.now - started).seconds
                    let decisionAtSeconds = item.decisionAtSeconds ?? dispatchStartedAt
                    try Task.checkCancellation()

                    let success: Bool
                    let processNanos: UInt64
                    let actualExecutor: String
                    let actualStartSeconds: Double
                    let completionMachineIndex: Int
                    let completionConcurrency: Int?
                    let countsTowardAdaptation: Bool
                    let transferOverheadSampleMS: Double?
                    let transferOverheadMachineIndex: Int?
                    let slotHealthDown: Bool?
                    switch slot {
                    case .local:
                        let localStarted = clock.now
                        actualStartSeconds = (localStarted - started).seconds
                        success = await localVideoRunner(
                            video,
                            uploadId,
                            thumbsDir,
                            previewsDir,
                            preset,
                            timeout
                        )
                        processNanos = UInt64(max(0, (clock.now - localStarted).seconds * 1_000_000_000))
                        actualExecutor = "local"
                        completionMachineIndex = 0
                        completionConcurrency = item.dispatchConcurrency
                        countsTowardAdaptation = true
                        transferOverheadSampleMS = nil
                        transferOverheadMachineIndex = nil
                        slotHealthDown = nil
                    case .remote(let worker, _):
                        let remoteMachineIndex = machineIndexByHostSnapshot[worker.host]
                        if let sourceSHA = sourceHashes[video.path] {
                            let remoteResult = await runThunderboltBlockingRoundTrip(
                                roundTripRunner: roundTripRunner,
                                worker: worker,
                                video: video,
                                sourceSHA: sourceSHA,
                                port: port,
                                connectTimeout: connectTimeout,
                                tempDir: tempDir
                            )
                            if remoteResult.success {
                                success = true
                                processNanos = remoteResult.processNanos
                                actualExecutor = worker.host
                                actualStartSeconds = dispatchStartedAt + max(0, remoteResult.firstRunningLatencySecondsEstimate ?? 0)
                                completionMachineIndex = remoteMachineIndex ?? 0
                                completionConcurrency = item.dispatchConcurrency
                                countsTowardAdaptation = true
                                transferOverheadSampleMS = remoteResult.firstRunningLatencySecondsEstimate.map { max(0, $0 * 1_000.0) }
                                transferOverheadMachineIndex = remoteMachineIndex
                                slotHealthDown = false
                            } else {
                                let localStarted = clock.now
                                actualStartSeconds = (localStarted - started).seconds
                                success = await localVideoRunner(
                                    video,
                                    uploadId,
                                    thumbsDir,
                                    previewsDir,
                                    preset,
                                    timeout
                                )
                                processNanos = UInt64(max(0, (clock.now - localStarted).seconds * 1_000_000_000))
                                actualExecutor = "local-fallback"
                                completionMachineIndex = 0
                                completionConcurrency = nil
                                countsTowardAdaptation = true
                                transferOverheadSampleMS = remoteResult.firstRunningLatencySecondsEstimate.map { max(0, $0 * 1_000.0) }
                                transferOverheadMachineIndex = remoteMachineIndex
                                slotHealthDown = remoteResult.slotHealthDownOnFailure ?? true
                            }
                        } else {
                            let localStarted = clock.now
                            actualStartSeconds = (localStarted - started).seconds
                            success = await localVideoRunner(
                                video,
                                uploadId,
                                thumbsDir,
                                previewsDir,
                                preset,
                                timeout
                            )
                            processNanos = UInt64(max(0, (clock.now - localStarted).seconds * 1_000_000_000))
                            actualExecutor = "local-fallback"
                            completionMachineIndex = 0
                            completionConcurrency = nil
                            countsTowardAdaptation = true
                            transferOverheadSampleMS = nil
                            transferOverheadMachineIndex = nil
                            slotHealthDown = nil
                        }
                    }

                    if let slotHealthDown {
                        await runtimeEngine.recordSlotHealth(slotOrdinal: slotOrdinal, isDown: slotHealthDown)
                    }
                    if let transferOverheadMachineIndex {
                        await runtimeEngine.recordTransferOverhead(
                            machineIndex: transferOverheadMachineIndex,
                            sampleMS: transferOverheadSampleMS
                        )
                    }

                    if success, countsTowardAdaptation {
                        await runtimeEngine.recordCompletion(
                            machineIndex: completionMachineIndex,
                            frameCount: frameCount,
                            processNanos: processNanos,
                            concurrencyHint: completionConcurrency
                        )
                    }

                    let completedAt = (clock.now - started).seconds
                    let actualStartMS = max(0, (actualStartSeconds - decisionAtSeconds) * 1_000.0)
                    let actualDoneMS = success ? max(0, (completedAt - decisionAtSeconds) * 1_000.0) : nil
                    let predictedMachineID: String = {
                        guard machineProfilesSnapshot.indices.contains(item.dispatchMachineIndex) else {
                            return "unknown"
                        }
                        return machineProfilesSnapshot[item.dispatchMachineIndex].id
                    }()
                    let prediction = ThunderboltCAPredictionSample(
                        machineID: predictedMachineID,
                        decisionAtSeconds: decisionAtSeconds,
                        predictedSlotReadyMS: item.predictedSlotReadyMS,
                        predictedStartMS: item.predictedStartMS,
                        predictedDoneMS: item.predictedDoneMS,
                        actualStartMS: actualStartMS,
                        actualDoneMS: actualDoneMS,
                        waited: item.waited,
                        success: success,
                        executorMismatch: completionMachineIndex != item.dispatchMachineIndex
                    )
                    let snapshot = await store.append(
                        ThunderboltCAJobRecord(
                            jobId: uploadId,
                            videoName: video.name,
                            slotLabel: slot.label,
                            actualExecutor: actualExecutor,
                            processNanos: processNanos,
                            frameCount: frameCount,
                            arrivalAtSeconds: item.arrivalAtSeconds,
                            completedAtSeconds: success ? completedAt : nil,
                            success: success
                        ),
                        prediction: prediction
                    )
                    if let progress {
                        await progress(snapshot.completed, videos.count, snapshot.failed, completedAt)
                    }
                    await completionQueue.push(slotOrdinal)
                    try Task.checkCancellation()
                }
            }
        }

        group.addTask(priority: .high) {
            var freeSlots = Set(slots.indices)

            while true {
                try Task.checkCancellation()

                let completedSlots = await completionQueue.drain()
                if !completedSlots.isEmpty {
                    freeSlots.formUnion(completedSlots)
                    let nowSeconds = (clock.now - started).seconds
                    _ = await runtimeEngine.markCompletedSlotsReady(
                        completedSlots,
                        nowSeconds: nowSeconds
                    )
                }

                let nowSeconds = (clock.now - started).seconds
                _ = await runtimeEngine.releaseReadyHeldDispatches(nowSeconds: nowSeconds)

                let hasPending = await runtimeEngine.hasPending
                let hasHeldDispatches = await runtimeEngine.hasHeldDispatches
                let isComplete = await runtimeEngine.isComplete

                if isComplete, !hasHeldDispatches, freeSlots.count == slots.count {
                    break
                }

                if !freeSlots.isEmpty, hasPending {
                    let batch = await runtimeEngine.scheduleRuntimeBatch(
                        freeSlotOrdinals: freeSlots,
                        nowSeconds: nowSeconds,
                        totalJobCount: videos.count
                    )
                    for dispatch in batch.dispatches {
                        let slotOrdinal = dispatch.slotOrdinal
                        guard freeSlots.contains(slotOrdinal) else { continue }
                        freeSlots.remove(slotOrdinal)
                        slotContinuations[slotOrdinal].yield(
                            ThunderboltCADispatchItem(sharedItem: dispatch.item)
                        )
                    }
                    if batch.madeProgress {
                        continue
                    }
                }

                if isComplete, freeSlots.count < slots.count {
                    do {
                        try await Task.sleep(nanoseconds: 5_000_000)
                    } catch {
                        if Task.isCancelled { break }
                    }
                    continue
                }

                if !hasPending, hasHeldDispatches {
                    let nextWakeSeconds = (await runtimeEngine.nextHeldWakeSeconds) ?? nowSeconds
                    let sleepSeconds = min(0.005, max(0.001, nextWakeSeconds - nowSeconds))
                    do {
                        try await Task.sleep(nanoseconds: UInt64(sleepSeconds * 1_000_000_000))
                    } catch {
                        if Task.isCancelled { break }
                    }
                    continue
                }

                if !hasPending, !isComplete {
                    await runtimeEngine.waitForWork()
                    if Task.isCancelled { break }
                    continue
                }

                if freeSlots.isEmpty {
                    do {
                        try await Task.sleep(nanoseconds: 5_000_000)
                    } catch {
                        if Task.isCancelled { break }
                    }
                    continue
                }

                do {
                    try await Task.sleep(nanoseconds: 5_000_000)
                } catch {
                    if Task.isCancelled { break }
                }
            }
            for continuation in slotContinuations {
                continuation.finish()
            }
        }

        try await group.waitForAll()
    }

    let jobs = await store.snapshot()
    let predictions = await store.predictionSnapshot()
    let adaptationRows = await runtimeEngine.adaptationRows().map(ThunderboltCAAdaptationRow.init(sharedRow:))
    let solverTelemetryRaw = await runtimeEngine.solverTelemetrySnapshot()
    let successfulJobs = jobs.compactMap { job -> SchedulingSuccessfulJob? in
        guard job.success, let completedAt = job.completedAtSeconds else { return nil }
        return SchedulingSuccessfulJob(
            arriveAtSeconds: job.arrivalAtSeconds,
            liveAtSeconds: completedAt
        )
    }
    let failedCount = jobs.count - successfulJobs.count
    let metrics = SchedulingMetricMath.compute(
        successfulJobs: successfulJobs,
        failedCount: failedCount
    )

    let solverTelemetryRows = solverTelemetryRaw.map { t in
        ThunderboltCASolverTelemetryRow(
            nodesVisited: t.nodesVisited,
            prunedByPickCount: t.prunedByPickCount,
            prunedByMakespan: t.prunedByMakespan,
            prunedByCompletionSum: t.prunedByCompletionSum,
            incumbentUpdates: t.incumbentUpdates,
            maxDepth: t.maxDepth,
            solverWallMS: t.solverWallMS
        )
    }
    let observability = ThunderboltCAObservability(
        policy: policy,
        modelInputs: modelInputs,
        adaptation: adaptationRows,
        predictions: predictions,
        solverTelemetry: solverTelemetryRows
    )
    let result = ThunderboltCARunResult(
        schedulerPolicy: policy.rawValue,
        arrivalProfile: profile.rawValue,
        totalJobs: jobs.count,
        successfulJobs: successfulJobs.count,
        failedCount: failedCount,
        metrics: ThunderboltCAMetricsJSON(
            sumWSeconds: metrics.sumWSeconds,
            p95Seconds: metrics.p95Seconds,
            makespanSeconds: metrics.makespanSeconds,
            failedCount: metrics.failedCount
        ),
        jobs: jobs,
        observability: observability
    )
    return ThunderboltCAObservedRun(
        result: result,
        observability: observability
    )
}

func prepareThunderboltCARunSetup(
    corpus: [MediaFile],
    preset: String,
    hardware: HardwareProfile,
    slotOverrides: ThunderboltCASlotOverrides?,
    mode: ThunderboltCAModelMode,
    priorTableOverride: BenchmarkPriorTable? = nil,
    settingsOverride: ThunderboltSettingsResolution? = nil,
    reachableWorkersOverride: [ThunderboltBoundWorkerSpec]? = nil,
    workerCapsOverride: [WorkerCaps?]? = nil,
    remoteTxInEstimateMSByHost: [String: Double] = [:],
    remoteTxOutEstimateMSByHost: [String: Double] = [:],
    remotePublishOverheadEstimateMSByHost: [String: Double] = [:]
) async throws -> ThunderboltCARunSetup {
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else {
        throw ThunderboltBenchmarkJSONError.noVideos
    }

    let settings = settingsOverride ?? resolveThunderboltBenchmarkSettings()
    let connectivity: [ThunderboltConnectivityResult]
    let reachableWorkers: [ThunderboltBoundWorkerSpec]
    if let reachableWorkersOverride {
        reachableWorkers = reachableWorkersOverride
        connectivity = reachableWorkersOverride.map { worker in
            ThunderboltConnectivityResult(worker: worker, reachable: true, connectMillis: 0)
        }
    } else {
        let configuredWorkers: [ThunderboltBoundWorkerSpec]
        if let workersRaw = settings.workersRaw, !workersRaw.isEmpty {
            let workers = parseThunderboltWorkers(workersRaw)
            configuredWorkers = resolveBridgeBoundWorkers(workers: workers).0
        } else {
            configuredWorkers = []
        }

        let probeTargets = configuredWorkers.map {
            ThunderboltWorkerProbeTarget(
                host: $0.connectHost,
                port: settings.port,
                sourceIP: $0.sourceIP
            )
        }
        let reachability = ThunderboltWorkerProbe.measureReachability(
            endpoints: probeTargets,
            timeoutMS: settings.connectTimeout
        )
        connectivity = zip(configuredWorkers, reachability).map { worker, result in
            ThunderboltConnectivityResult(
                worker: worker,
                reachable: result.reachable,
                connectMillis: result.connectMillis
            )
        }
        reachableWorkers = connectivity.filter(\.reachable).map { result -> ThunderboltBoundWorkerSpec in
            if let overrideSlots = slotOverrides?.remoteSlotsByHost[result.worker.host] {
                return ThunderboltBoundWorkerSpec(
                    host: result.worker.host,
                    connectHost: result.worker.connectHost,
                    slots: max(0, overrideSlots),
                    sourceIP: result.worker.sourceIP,
                    bridgeName: result.worker.bridgeName
                )
            }
            return result.worker
        }
    }
    let fallbackRemoteTxInEstimateMSByHost = connectivity.reduce(into: [String: Double]()) { partial, result in
        guard let estimateMS = sanitizedPositiveMS(result.connectMillis) else { return }
        partial[result.worker.host] = estimateMS
    }
    let providedRemoteTxInEstimateMSByHost = remoteTxInEstimateMSByHost.reduce(into: [String: Double]()) { partial, entry in
        guard let estimateMS = sanitizedPositiveMS(entry.value) else { return }
        partial[entry.key] = estimateMS
    }
    let providedRemoteTxOutEstimateMSByHost = remoteTxOutEstimateMSByHost.reduce(into: [String: Double]()) { partial, entry in
        guard let estimateMS = sanitizedPositiveMS(entry.value) else { return }
        partial[entry.key] = estimateMS
    }
    let providedRemotePublishOverheadEstimateMSByHost = remotePublishOverheadEstimateMSByHost.reduce(
        into: [String: Double]()
    ) { partial, entry in
        guard let estimateMS = sanitizedPositiveMS(entry.value) else { return }
        partial[entry.key] = estimateMS
    }
    let resolvedRemoteTxInEstimateMSByHost = fallbackRemoteTxInEstimateMSByHost
        .merging(providedRemoteTxInEstimateMSByHost) { _, provided in provided }
    let measuredTailTelemetry = measureThunderboltCATailTelemetryEstimates(
        workers: reachableWorkers,
        videos: videos,
        port: settings.port,
        connectTimeout: settings.connectTimeout,
        providedTxOutEstimateMSByHost: providedRemoteTxOutEstimateMSByHost,
        providedPublishOverheadEstimateMSByHost: providedRemotePublishOverheadEstimateMSByHost
    )
    let resolvedRemoteTxOutEstimateMSByHost = measuredTailTelemetry.txOutMSByHost
        .merging(providedRemoteTxOutEstimateMSByHost) { _, provided in provided }
    let resolvedRemotePublishOverheadEstimateMSByHost = measuredTailTelemetry.publishOverheadMSByHost
        .merging(providedRemotePublishOverheadEstimateMSByHost) { _, provided in provided }

    let localSlotDefault = resolveLocalCASlotsDefault()
    let localSlotCount = max(1, slotOverrides?.localSlots ?? localSlotDefault)
    let slots = caSlots(localSlots: localSlotCount, reachableWorkers: reachableWorkers)

    var priorTable = priorTableOverride ?? loadCAPriorTable()
    let localSignature = WorkerSignatureBuilder.make(
        chipName: hardware.chipName,
        performanceCores: hardware.performanceCores,
        efficiencyCores: hardware.efficiencyCores,
        videoEncodeEngines: hardware.videoEncodeEngines,
        preset: preset,
        osVersion: ProcessInfo.processInfo.operatingSystemVersion
    )

    let workerCaps = workerCapsOverride ?? ThunderboltWorkerProbe.queryCapabilities(
        endpoints: reachableWorkers.map {
            ThunderboltWorkerProbeTarget(
                host: $0.connectHost,
                port: settings.port,
                sourceIP: $0.sourceIP
            )
        },
        timeoutMS: settings.connectTimeout
    )
    var workerCapsByHost: [String: WorkerCaps] = [:]
    workerCapsByHost.reserveCapacity(reachableWorkers.count)
    for (worker, caps) in zip(reachableWorkers, workerCaps) {
        if let caps {
            workerCapsByHost[worker.host] = caps
        }
    }
    for caps in workerCaps.compactMap({ $0 }) {
        guard let signature = caps.workerSignature,
              let osVersion = caps.osVersion,
              let priorCells = caps.priorCells,
              !priorCells.isEmpty
        else {
            continue
        }
        let existingMachine = priorTable.machines.first(where: { $0.signature == signature })
        let resolvedMSPerFrameC1 = if let existingMachine,
            existingMachine.msPerFrameC1.isFinite,
            existingMachine.msPerFrameC1 > 0 {
            existingMachine.msPerFrameC1
        } else if let capsMSPerFrameC1 = caps.msPerFrameC1,
            capsMSPerFrameC1.isFinite,
            capsMSPerFrameC1 > 0 {
            capsMSPerFrameC1
        } else {
            0.0
        }
        let resolvedFixedOverheadMS = if let existingMachine,
            existingMachine.fixedOverheadMS.isFinite,
            existingMachine.fixedOverheadMS > 0 {
            existingMachine.fixedOverheadMS
        } else {
            0.0
        }
        let resolvedAvgCorpusFrameCount = if let existingMachine,
            existingMachine.avgCorpusFrameCount.isFinite,
            existingMachine.avgCorpusFrameCount > 0 {
            existingMachine.avgCorpusFrameCount
        } else {
            0.0
        }
            priorTable.merge(
                remoteMachine: BenchmarkPriorMachine(
                    signature: signature,
                    chipName: existingMachine?.chipName ?? caps.chipName ?? "unknown",
                    performanceCores: existingMachine?.performanceCores ?? caps.performanceCores ?? 0,
                efficiencyCores: existingMachine?.efficiencyCores ?? caps.efficiencyCores ?? 0,
                videoEncodeEngines: existingMachine?.videoEncodeEngines ?? caps.videoEncodeEngines ?? 0,
                osVersion: existingMachine?.osVersion ?? osVersion,
                    transcodePreset: existingMachine?.transcodePreset ?? preset,
                    msPerFrameC1: resolvedMSPerFrameC1,
                    fixedOverheadMS: resolvedFixedOverheadMS,
                    avgCorpusFrameCount: resolvedAvgCorpusFrameCount,
                    affineModelSource: existingMachine?.affineModelSource ?? .legacyHeuristic,
                    cells: priorCells
                )
            )
        }

    let localPrior = priorTable.machines.first(where: { $0.signature == localSignature })
    let localProfile = CAProfileAndFallbackMath.localPriorProfileShaping(from: localPrior)
    let localMSPerFrameC1 = localProfile.msPerFrameC1
    let localMSSource = localProfile.usedFallbackMSPerFrameC1 ? "fallback(default-c1)" : "prior(local)"
    let localFixedOverheadMS = localProfile.fixedOverheadMS
    let localCurve = localProfile.degradationCurve
    let localCurveSource = localProfile.usedFallbackDegradationCurve ? "fallback(flat-c1)" : "prior(local)"

    let modelBuild = buildThunderboltCAModelProfiles(
        mode: mode,
        port: settings.port,
        slots: slots,
        reachableWorkers: reachableWorkers,
        workerCaps: workerCaps,
        priorTable: priorTable,
        remoteTxInEstimateMSByHost: resolvedRemoteTxInEstimateMSByHost,
        remoteTxOutEstimateMSByHost: resolvedRemoteTxOutEstimateMSByHost,
        remotePublishOverheadEstimateMSByHost: resolvedRemotePublishOverheadEstimateMSByHost,
        localMSPerFrameC1: localMSPerFrameC1,
        localFixedOverheadMS: localFixedOverheadMS,
        localMSSource: localMSSource,
        localCurve: localCurve,
        localCurveSource: localCurveSource
    )

    let videoCosts = await caResolvedVideoCosts(
        videos: videos,
        localMSPerFrameC1: localMSPerFrameC1,
        localFixedOverheadMS: localFixedOverheadMS
    )

    if mode == .strict, !modelBuild.diagnostics.strictExclusions.isEmpty {
        throw ThunderboltBenchmarkJSONError.caStrictRemoteExclusion(modelBuild.diagnostics.strictExclusions)
    }

    let sourceHashes: [String: String]
    if modelBuild.diagnostics.modeledSlotCount > 0 {
        sourceHashes = try precomputeSourceHashes(videos)
    } else {
        sourceHashes = [:]
    }

    return ThunderboltCARunSetup(
        port: settings.port,
        connectTimeout: settings.connectTimeout,
        videos: videos,
        videoCosts: videoCosts,
        priorTable: priorTable,
        localSignature: localSignature,
        localSlotCount: localSlotCount,
        localMSPerFrameC1: localMSPerFrameC1,
        localFixedOverheadMS: localFixedOverheadMS,
        sourceHashes: sourceHashes,
        slots: slots,
        machineProfiles: modelBuild.machineProfiles,
        slotBindings: modelBuild.slotBindings,
        machineIndexByHost: modelBuild.machineIndexByHost,
        modelInputs: modelBuild.modelInputs,
        diagnostics: modelBuild.diagnostics,
        reachableWorkers: reachableWorkers,
        workerCapsByHost: workerCapsByHost
    )
}

func measureThunderboltCATailTelemetryEstimates(
    workers: [ThunderboltBoundWorkerSpec],
    videos: [MediaFile],
    port: Int,
    connectTimeout: Int,
    providedTxOutEstimateMSByHost: [String: Double] = [:],
    providedPublishOverheadEstimateMSByHost: [String: Double] = [:],
    roundTripRunner: @escaping ThunderboltCARoundTripRunner = runThunderboltRoundTrip
) -> ThunderboltCATailTelemetryEstimates {
    let workersByHost = workers.reduce(into: [String: ThunderboltBoundWorkerSpec]()) { partial, worker in
        partial[worker.host] = worker
    }
    let videosByPath = videos.reduce(into: [String: MediaFile]()) { partial, video in
        partial[video.path] = video
    }
    let sharedEstimates = ThunderboltTailTelemetrySeedMeasurement.measure(
        endpoints: workers.map {
            $0.sharedTailTelemetrySeedProbeEndpoint(
                port: port,
                connectTimeout: connectTimeout
            )
        },
        sampleCandidates: videos.map(\.sharedTailTelemetrySeedSampleCandidate),
        providedTxOutEstimateMSByID: providedTxOutEstimateMSByHost,
        providedPublishOverheadEstimateMSByID: providedPublishOverheadEstimateMSByHost,
        sha256BufferSize: BenchDefaults.sha256BufferSize,
        roundTripRunner: { endpoint, sample, sourceSHA256, tempDir in
            guard let worker = workersByHost[endpoint.id],
                  let video = videosByPath[sample.path] else {
                return ThunderboltRawRoundTripResult(
                    success: false,
                    sendSeconds: 0,
                    processNanos: 0,
                    receiveSeconds: 0,
                    totalSeconds: 0
                )
            }
            return roundTripRunner(
                worker,
                video,
                sourceSHA256,
                endpoint.port,
                endpoint.connectTimeoutMS,
                tempDir
            ).sharedRawRoundTripResult
        }
    )
    return ThunderboltCATailTelemetryEstimates(sharedEstimates: sharedEstimates)
}

private func sanitizedPositiveMS(_ value: Double?) -> Double? {
    guard let value,
          value.isFinite,
          value > 0 else {
        return nil
    }
    return max(0, value)
}
