import Foundation
import Darwin
import OSLog

package actor ThunderboltDispatcher {
    private struct SlotState: Sendable {
        let id: String
        var fd: Int32?
        var isBusy: Bool
        var isDown: Bool
        var dispatchedFrameCount: Double?
        var dispatchedConcurrency: Int?
        var currentDispatchJobHandle: UInt32?
        var downReason: String?
        var lastEstRemainingMS: UInt32?
        var dispatchStartedAtNanos: UInt64?
        var driftBaselineRemainingMS: Double?
        var driftBaselineAtNanos: UInt64?
        var driftHighCount: Int
        var driftLowCount: Int
        var driftArmed: Bool
        var lastDriftTriggerNanos: UInt64?

        mutating func resetDriftTracking() {
            driftBaselineRemainingMS = nil
            driftBaselineAtNanos = nil
            driftHighCount = 0
            driftLowCount = 0
            driftArmed = true
            lastDriftTriggerNanos = nil
        }

        mutating func clearDispatchMetadata() {
            dispatchedFrameCount = nil
            dispatchedConcurrency = nil
            currentDispatchJobHandle = nil
            dispatchStartedAtNanos = nil
        }
    }

    private struct WorkerConnection: Sendable {
        let host: String
        let port: Int
        let maxSlots: Int
        var slots: [SlotState]
        var observedProcessNanosEMA: Double?
        var liveMSPerFrameC1EMA: Double?
        var liveMSPerFrameC1ErrorEMA: Double?
        var liveMSPerFrameC1AbsErrorEMA: Double?
        var workerSignature: String?
        var latestCaps: WorkerCaps?
        var priorP50MSByConcurrency: [Int: Int]
        var transferStartBaselineMS: Double?
        var transferOverheadEstimateMS: Double?
        var txOutEstimateMS: Double?
        var publishOverheadEstimateMS: Double?

        var activeSlots: Int {
            slots.reduce(into: 0) { count, slot in
                if slot.isBusy {
                    count += 1
                }
            }
        }
    }

    package struct BridgeSource: Sendable {
        package let name: String
        package let ip: String
        package let network: UInt32
        package let mask: UInt32
    }

    package enum RecomputeTrigger: String, Sendable {
        case arrive = "ARRIVE"
        case finish = "FINISH"
        case fail = "FAIL"
        case slotDownBatch = "SLOT_DOWN_BATCH"
        case slotUp = "SLOT_UP"
        case etaDrift = "ETA_DRIFT"
    }

    package enum DispatchResult: Sendable, Equatable {
        case success
        case fallbackLocal
        case transientRetry(slotHealthDown: Bool)
        case permanentFailure
    }

    private enum DispatchOutcome: Sendable {
        case success(
            processNanos: UInt64,
            firstRunningLatencyMS: UInt32?,
            lastEstRemainingMS: UInt32?,
            txOutMS: Double?,
            publishOverheadMS: Double?
        )
        case failed(
            reason: String,
            errorClass: ProgressTickV2.ErrorClass?,
            firstRunningLatencyMS: UInt32?,
            slotHealthDown: Bool
        )
    }

    private var workers: [WorkerConnection]
    private let connectTimeoutMS: Int
    private let thumbsDir: String
    private let previewsDir: String
    private let sha256BufferSize: Int
    private let monotonicNowNanos: @Sendable () -> UInt64
    private let bridgeSources: [BridgeSource]
    private let bridgesAvailable: Bool
    private let complexityAwareSchedulingEnabled: Bool
    private let sessionID: UInt32
    private let videoTranscodePreset: String
    private var resolvedWorkerAddressByHost: [String: UInt32]
    private var unresolvedWorkerHosts: [String: UInt64]
    private let connectTransport: @Sendable (_ host: String, _ port: Int, _ timeoutMS: Int, _ sourceIP: String?) -> Int32?
    private let queryCapabilitiesTransport: @Sendable (_ host: String, _ port: Int, _ timeoutMS: Int, _ sourceIP: String?) -> WorkerCaps?
    private let onRetryIncrement: (@Sendable (String) async -> Int?)?
    private let onRetrySeed: (@Sendable (String) async -> Int)?
    private var benchmarkPriorTable: BenchmarkPriorTable
    private var priorMergedWorkers: Set<String> = []
    private var priorProbeTasksByWorkerHost: [String: Task<Void, Never>] = [:]
    private var downSlotProbeTasks: [String: Task<Void, Never>] = [:]
    private var graceWaitersBySlot: [String: [UUID: CheckedContinuation<Bool, Never>]] = [:]
    private var nextJobHandle: UInt32 = 1
    private var jobHandleExhausted = false
    private var isShuttingDown = false
    private var recomputeSignal: CARecomputeCoordinator.Signal?
    private var lastDriftRecomputeSignalNanos: UInt64?
    private var preflightUnavailableDispatchCount = 0

    package init(
        workers: [Config.ThunderboltWorker],
        port: Int,
        connectTimeout: Int,
        thumbsDir: String,
        previewsDir: String,
        sha256BufferSize: Int,
        complexityAwareSchedulingEnabled: Bool = false,
        benchmarkPriorTable: BenchmarkPriorTable = BenchmarkPriorTable(),
        videoTranscodePreset: String = "",
        monotonicNowNanosOverride: (@Sendable () -> UInt64)? = nil,
        bridgeSourcesOverride: [BridgeSource]? = nil,
        connectOverride: (@Sendable (_ host: String, _ port: Int, _ timeoutMS: Int, _ sourceIP: String?) -> Int32?)? = nil,
        queryCapabilitiesOverride: (@Sendable (_ host: String, _ port: Int, _ timeoutMS: Int, _ sourceIP: String?) -> WorkerCaps?)? = nil,
        onRetryIncrement: (@Sendable (String) async -> Int?)? = nil,
        onRetrySeed: (@Sendable (String) async -> Int)? = nil
    ) {
        self.workers = workers.compactMap { worker in
            guard worker.slots > 0 else { return nil }
            let slotStates = (0 ..< worker.slots).map { offset in
                SlotState(
                    id: "\(worker.host)#s\(offset + 1)",
                    fd: nil,
                    isBusy: false,
                    isDown: false,
                    dispatchedFrameCount: nil,
                    dispatchedConcurrency: nil,
                    currentDispatchJobHandle: nil,
                    downReason: nil,
                    lastEstRemainingMS: nil,
                    dispatchStartedAtNanos: nil,
                    driftBaselineRemainingMS: nil,
                    driftBaselineAtNanos: nil,
                    driftHighCount: 0,
                    driftLowCount: 0,
                    driftArmed: true,
                    lastDriftTriggerNanos: nil
                )
            }
            return WorkerConnection(
                host: worker.host,
                port: port,
                maxSlots: worker.slots,
                slots: slotStates,
                observedProcessNanosEMA: nil,
                liveMSPerFrameC1EMA: nil,
                liveMSPerFrameC1ErrorEMA: nil,
                liveMSPerFrameC1AbsErrorEMA: nil,
                workerSignature: nil,
                latestCaps: nil,
                priorP50MSByConcurrency: [:],
                transferStartBaselineMS: nil,
                transferOverheadEstimateMS: nil,
                txOutEstimateMS: nil,
                publishOverheadEstimateMS: nil
            )
        }
        self.connectTimeoutMS = max(1, connectTimeout)
        self.thumbsDir = thumbsDir
        self.previewsDir = previewsDir
        self.sha256BufferSize = sha256BufferSize
        self.monotonicNowNanos = monotonicNowNanosOverride ?? { DispatchTime.now().uptimeNanoseconds }
        self.bridgeSources = bridgeSourcesOverride ?? Self.discoverBridgeSources()
        self.bridgesAvailable = !self.bridgeSources.isEmpty
        self.complexityAwareSchedulingEnabled = complexityAwareSchedulingEnabled
        self.sessionID = UInt32.random(in: UInt32.min ... UInt32.max)
        self.benchmarkPriorTable = benchmarkPriorTable
        self.videoTranscodePreset = videoTranscodePreset
        var resolvedAddresses: [String: UInt32] = [:]
        var unresolvedHosts: [String: UInt64] = [:]
        let initialUnresolvedRetryNanos = DispatchTime.now().uptimeNanoseconds &+ Self.unresolvedHostRetryBackoffNanos
        for worker in self.workers {
            guard resolvedAddresses[worker.host] == nil,
                  unresolvedHosts[worker.host] == nil else {
                continue
            }
            if let address = Self.resolveWorkerIPv4(worker.host) {
                resolvedAddresses[worker.host] = address
            } else {
                unresolvedHosts[worker.host] = initialUnresolvedRetryNanos
            }
        }
        self.resolvedWorkerAddressByHost = resolvedAddresses
        self.unresolvedWorkerHosts = unresolvedHosts
        self.connectTransport = connectOverride ?? { host, port, timeoutMS, sourceIP in
            ThunderboltTransport.connect(host: host, port: port, timeoutMS: timeoutMS, sourceIP: sourceIP)
        }
        self.queryCapabilitiesTransport = queryCapabilitiesOverride ?? { host, port, timeoutMS, sourceIP in
            ThunderboltTransport.queryCapabilities(host: host, port: port, timeoutMS: timeoutMS, sourceIP: sourceIP)
        }
        self.onRetryIncrement = onRetryIncrement
        self.onRetrySeed = onRetrySeed
    }

    package func dispatch(
        uploadId: String,
        filePath: String,
        originalName: String,
        mimeType: String?,
        targetWorkerIndex: Int,
        targetSlotIndex: Int,
        frameCount: Double = 0,
        successfulExecutionSampleModel: CASuccessfulExecutionSampleModel? = nil
    ) async -> DispatchResult {
        guard !isShuttingDown else { return .fallbackLocal }
        guard bridgesAvailable else { return .fallbackLocal }
        guard workers.indices.contains(targetWorkerIndex),
              workers[targetWorkerIndex].slots.indices.contains(targetSlotIndex) else {
            Logger.kiko.error(
                "Invalid dispatch target indices worker=\(targetWorkerIndex, privacy: .public) slot=\(targetSlotIndex, privacy: .public) for \(uploadId, privacy: .public)"
            )
            signalRecompute(.fail)
            return .permanentFailure
        }

        let safeName = URL(fileURLWithPath: originalName).lastPathComponent
        let thumbsDir = self.thumbsDir
        let previewsDir = self.previewsDir
        let sha256BufferSize = self.sha256BufferSize
        let complexityAwareSchedulingEnabled = self.complexityAwareSchedulingEnabled
        let sessionID = self.sessionID
        let monotonicNowNanos = self.monotonicNowNanos

        warmupPrior()
        let workerIndex = targetWorkerIndex
        let slotIndex = targetSlotIndex
        let worker = workers[workerIndex]
        let slotID = worker.slots[slotIndex].id

        if workers[workerIndex].slots[slotIndex].isBusy || workers[workerIndex].slots[slotIndex].isDown {
            preflightUnavailableDispatchCount += 1
            let slotIsDown = workers[workerIndex].slots[slotIndex].isDown
            let reason = slotIsDown ? "target slot down" : "target slot busy"
            Logger.kiko.warning(
                "Target slot \(slotID, privacy: .public) unavailable for \(uploadId, privacy: .public): \(reason, privacy: .public)"
            )
            return await handleTransientFailure(
                uploadId: uploadId,
                slotID: slotID,
                reason: reason,
                workerIndex: workerIndex,
                slotIndex: slotIndex,
                attemptGraceRecovery: slotIsDown,
                slotHealthDownForRetry: slotIsDown
            )
        }

        guard let sourceIP = sourceIP(for: worker.host) else {
            Logger.kiko.warning(
                "Worker \(worker.host, privacy: .public) has no bridge source route, falling back to local"
            )
            return .fallbackLocal
        }
        mergeRemotePriorIfNeeded(workerIndex: workerIndex, sourceIP: sourceIP)

        workers[workerIndex].slots[slotIndex].isBusy = true
        workers[workerIndex].slots[slotIndex].lastEstRemainingMS = nil
        workers[workerIndex].slots[slotIndex].dispatchStartedAtNanos = monotonicNowNanos()
        workers[workerIndex].slots[slotIndex].dispatchedFrameCount = frameCount.isFinite && frameCount > 0 ? frameCount : nil
        workers[workerIndex].slots[slotIndex].dispatchedConcurrency = max(1, workers[workerIndex].activeSlots)
        if workers[workerIndex].slots[slotIndex].fd == nil {
            guard let fd = connectTransport(
                worker.host,
                worker.port,
                connectTimeoutMS,
                sourceIP
            ) else {
                workers[workerIndex].slots[slotIndex].isBusy = false
                workers[workerIndex].slots[slotIndex].clearDispatchMetadata()
                markSlotDown(workerIndex: workerIndex, slotIndex: slotIndex, reason: "connect failed")
                Logger.kiko.warning(
                    "Worker \(worker.host, privacy: .public) unreachable on slot \(slotID, privacy: .public), retrying via scheduler"
                )
                return await handleTransientFailure(
                    uploadId: uploadId,
                    slotID: slotID,
                    reason: "connect failed",
                    workerIndex: workerIndex,
                    slotIndex: slotIndex,
                    attemptGraceRecovery: true,
                    slotHealthDownForRetry: true
                )
            }
            workers[workerIndex].slots[slotIndex].fd = fd
            markSlotUp(workerIndex: workerIndex, slotIndex: slotIndex, includeFreshStream: true)
        }

        guard let slotFD = workers[workerIndex].slots[slotIndex].fd else {
            workers[workerIndex].slots[slotIndex].isBusy = false
            workers[workerIndex].slots[slotIndex].clearDispatchMetadata()
            markSlotDown(workerIndex: workerIndex, slotIndex: slotIndex, reason: "slot fd unavailable")
            Logger.kiko.warning(
                "Slot \(slotID, privacy: .public) lost connection before dispatch, retrying via scheduler"
            )
            return await handleTransientFailure(
                uploadId: uploadId,
                slotID: slotID,
                reason: "slot fd unavailable",
                workerIndex: workerIndex,
                slotIndex: slotIndex,
                attemptGraceRecovery: true,
                slotHealthDownForRetry: true
            )
        }

        guard let jobHandle = allocateJobHandle() else {
            workers[workerIndex].slots[slotIndex].isBusy = false
            workers[workerIndex].slots[slotIndex].clearDispatchMetadata()
            Logger.kiko.error(
                "Job handle space exhausted for session \(String(sessionID), privacy: .public); marking \(uploadId, privacy: .public) permanent failure"
            )
            signalRecompute(.fail)
            return .permanentFailure
        }
        let mime = Self.makeWorkerMime(
            baseMime: mimeType ?? "",
            complexityAwareSchedulingEnabled: complexityAwareSchedulingEnabled,
            jobHandle: jobHandle,
            sessionID: sessionID
        )
        workers[workerIndex].slots[slotIndex].currentDispatchJobHandle = jobHandle

        Logger.kiko.info(
            "Dispatching \(uploadId, privacy: .public) to worker \(worker.host, privacy: .public) slot \(slotID, privacy: .public)"
        )

        let runningTickSignal: @Sendable (UInt32) -> Void = { [self] estRemainingMS in
            Task {
                await self.handleRunningTickFromStream(
                    workerIndex: workerIndex,
                    slotIndex: slotIndex,
                    estRemainingMS: estRemainingMS,
                    expectedJobHandle: jobHandle
                )
            }
        }
        let outcome = await Task.detached(priority: .userInitiated) {
            Self.dispatchToWorker(
                uploadId: uploadId,
                filePath: filePath,
                originalName: safeName,
                mimeType: mime,
                fd: slotFD,
                thumbsDir: thumbsDir,
                previewsDir: previewsDir,
                sha256BufferSize: sha256BufferSize,
                complexityAwareSchedulingEnabled: complexityAwareSchedulingEnabled,
                jobHandle: jobHandle,
                sessionID: sessionID,
                onRunningTick: runningTickSignal,
                monotonicNowNanos: monotonicNowNanos
            )
        }.value

        let dispatchedFrameCount = workers[workerIndex].slots[slotIndex].dispatchedFrameCount
        let dispatchedConcurrency = workers[workerIndex].slots[slotIndex].dispatchedConcurrency
        workers[workerIndex].slots[slotIndex].isBusy = false
        workers[workerIndex].slots[slotIndex].clearDispatchMetadata()

        switch outcome {
        case .success(
            let processNanos,
            let firstRunningLatencyMS,
            let lastEstRemainingMS,
            let txOutMS,
            let publishOverheadMS
        ):
            markSlotUp(workerIndex: workerIndex, slotIndex: slotIndex)
            updateTransferLatencyModel(workerIndex: workerIndex, slotIndex: slotIndex, sampleMS: firstRunningLatencyMS)
            updateTailTelemetryModel(
                workerIndex: workerIndex,
                txOutSampleMS: txOutMS,
                publishOverheadSampleMS: publishOverheadMS
            )
            if let lastEstRemainingMS {
                workers[workerIndex].slots[slotIndex].lastEstRemainingMS = lastEstRemainingMS
            }
            if let ema = workers[workerIndex].observedProcessNanosEMA {
                workers[workerIndex].observedProcessNanosEMA = ema * 0.8 + Double(processNanos) * 0.2
            } else {
                workers[workerIndex].observedProcessNanosEMA = Double(processNanos)
            }
            if let frameCount = dispatchedFrameCount, frameCount > 0 {
                let concurrency = max(1, dispatchedConcurrency ?? 1)
                let sampleModel = successfulExecutionSampleModel
                    ?? resolvedSuccessfulExecutionSampleModel(workerIndex: workerIndex)
                recordSuccessfulExecutionSample(
                    workerIndex: workerIndex,
                    processNanos: processNanos,
                    frameCount: frameCount,
                    concurrency: concurrency,
                    sampleModel: sampleModel
                )
            }
            let seconds = Double(processNanos) / 1_000_000_000
            Logger.kiko.info(
                "Worker \(worker.host, privacy: .public) completed \(uploadId, privacy: .public) in \(seconds, privacy: .public)s"
            )
            return .success
        case .failed(let reason, let errorClass, let firstRunningLatencyMS, let slotHealthDown):
            updateTransferLatencyModel(workerIndex: workerIndex, slotIndex: slotIndex, sampleMS: firstRunningLatencyMS)
            if slotHealthDown {
                markSlotDown(workerIndex: workerIndex, slotIndex: slotIndex, reason: reason)
            }
            if let fd = workers[workerIndex].slots[slotIndex].fd {
                ThunderboltTransport.closeConnection(fd: fd)
                workers[workerIndex].slots[slotIndex].fd = nil
            }

            if errorClass == .permanent {
                signalRecompute(.fail)
                Logger.kiko.warning(
                    "Remote dispatch marked permanent for \(uploadId, privacy: .public) after slot \(slotID, privacy: .public): \(reason, privacy: .public)"
                )
                return .permanentFailure
            }

            return await handleTransientFailure(
                uploadId: uploadId,
                slotID: slotID,
                reason: reason,
                workerIndex: workerIndex,
                slotIndex: slotIndex,
                attemptGraceRecovery: slotHealthDown,
                slotHealthDownForRetry: slotHealthDown
            )
        }
    }

    private func handleTransientFailure(
        uploadId: String,
        slotID: String,
        reason: String,
        workerIndex: Int,
        slotIndex: Int,
        attemptGraceRecovery: Bool,
        slotHealthDownForRetry: Bool
    ) async -> DispatchResult {
        if attemptGraceRecovery {
            startDownSlotProbe(workerIndex: workerIndex, slotIndex: slotIndex)
            if await waitForGraceRecovery(
                workerIndex: workerIndex,
                slotIndex: slotIndex,
                slotID: slotID
            ) {
                Logger.kiko.info(
                    "Recovered slot \(slotID, privacy: .public) within grace for \(uploadId, privacy: .public); re-queueing without retry increment"
                )
                signalRecompute(.fail)
                return .transientRetry(slotHealthDown: false)
            }
        }

        let persistedRetryCount: Int
        if let onRetryIncrement {
            guard let durableRetryCount = await onRetryIncrement(uploadId) else {
                signalRecompute(.fail)
                Logger.kiko.error(
                    "Retry persistence failed for \(uploadId, privacy: .public) after transient remote failure on \(slotID, privacy: .public); escalating to permanent failure"
                )
                return .permanentFailure
            }
            persistedRetryCount = max(0, durableRetryCount)
        } else {
            let seedRetryCount = max(0, await onRetrySeed?(uploadId) ?? 0)
            persistedRetryCount = seedRetryCount + 1
        }

        if persistedRetryCount > 2 {
            signalRecompute(.fail)
            Logger.kiko.warning(
                "Remote dispatch escalated to permanent for \(uploadId, privacy: .public) after retry \(persistedRetryCount, privacy: .public): \(reason, privacy: .public)"
            )
            return .permanentFailure
        }

        signalRecompute(.fail)
        Logger.kiko.warning(
            "Transient remote dispatch failure for \(uploadId, privacy: .public) on \(slotID, privacy: .public); re-queue retry \(persistedRetryCount, privacy: .public)/2"
        )
        return .transientRetry(slotHealthDown: slotHealthDownForRetry)
    }

    private func startDownSlotProbe(workerIndex: Int, slotIndex: Int) {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return
        }
        let slotID = workers[workerIndex].slots[slotIndex].id
        guard downSlotProbeTasks[slotID] == nil else { return }

        downSlotProbeTasks[slotID] = Task.detached(priority: .utility) { [self] in
            var backoffNanos = Self.downSlotProbeInitialNanos
            var isFirstProbe = true

            while !Task.isCancelled {
                let jitterNanos: UInt64
                if isFirstProbe {
                    jitterNanos = 0
                    isFirstProbe = false
                } else {
                    jitterNanos = UInt64.random(in: 0 ... Self.downSlotProbeJitterNanos)
                }
                try? await Task.sleep(nanoseconds: backoffNanos + jitterNanos)
                if Task.isCancelled { return }

                let recovered = await self.probeDownSlotOnce(
                    workerIndex: workerIndex,
                    slotIndex: slotIndex,
                    slotID: slotID
                )
                if recovered {
                    await self.removeDownSlotProbeTask(slotID: slotID)
                    return
                }

                backoffNanos = min(backoffNanos << 1, Self.downSlotProbeMaxNanos)
            }
        }
    }

    private func stopDownSlotProbe(slotID: String) {
        guard let task = downSlotProbeTasks.removeValue(forKey: slotID) else { return }
        task.cancel()
    }

    private func removeDownSlotProbeTask(slotID: String) {
        downSlotProbeTasks.removeValue(forKey: slotID)
    }

    private func probeDownSlotOnce(
        workerIndex: Int,
        slotIndex: Int,
        slotID: String
    ) async -> Bool {
        guard !isShuttingDown else { return true }
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return true
        }
        guard workers[workerIndex].slots[slotIndex].isDown else {
            return true
        }
        guard workers[workerIndex].slots[slotIndex].fd == nil else {
            markSlotUp(workerIndex: workerIndex, slotIndex: slotIndex, includeFreshStream: true)
            return true
        }
        guard let sourceIP = sourceIP(for: workers[workerIndex].host) else {
            return false
        }

        let host = workers[workerIndex].host
        let port = workers[workerIndex].port
        let timeoutMS = min(max(connectTimeoutMS, 1), Self.downSlotProbeConnectTimeoutMS)
        let connectTransport = self.connectTransport
        let recoveredFD = await Task(priority: .utility) {
            connectTransport(
                host,
                port,
                timeoutMS,
                sourceIP
            )
        }.value

        guard let recoveredFD else { return false }
        if isShuttingDown || Task.isCancelled {
            ThunderboltTransport.closeConnection(fd: recoveredFD)
            return true
        }
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            ThunderboltTransport.closeConnection(fd: recoveredFD)
            return true
        }
        guard workers[workerIndex].slots[slotIndex].isDown else {
            ThunderboltTransport.closeConnection(fd: recoveredFD)
            return true
        }

        workers[workerIndex].slots[slotIndex].fd = recoveredFD
        workers[workerIndex].slots[slotIndex].isBusy = false
        workers[workerIndex].slots[slotIndex].clearDispatchMetadata()
        markSlotUp(workerIndex: workerIndex, slotIndex: slotIndex, includeFreshStream: true)
        Logger.kiko.info("Recovered down slot \(slotID, privacy: .public) via long-down probe")
        return true
    }

    package func shutdown() {
        isShuttingDown = true
        for task in priorProbeTasksByWorkerHost.values {
            task.cancel()
        }
        priorProbeTasksByWorkerHost.removeAll(keepingCapacity: false)
        for task in downSlotProbeTasks.values {
            task.cancel()
        }
        downSlotProbeTasks.removeAll(keepingCapacity: false)
        for waiters in graceWaitersBySlot.values {
            for continuation in waiters.values {
                continuation.resume(returning: false)
            }
        }
        graceWaitersBySlot.removeAll(keepingCapacity: false)
        for workerIndex in workers.indices {
            for slotIndex in workers[workerIndex].slots.indices {
                if let fd = workers[workerIndex].slots[slotIndex].fd {
                    ThunderboltTransport.closeConnection(fd: fd)
                    workers[workerIndex].slots[slotIndex].fd = nil
                }
            }
        }
    }

    package func hasBridges() -> Bool {
        bridgesAvailable
    }

    package func benchmarkPriorSnapshot() -> BenchmarkPriorTable {
        benchmarkPriorTable
    }

    package struct CASlotSnapshot: Sendable {
        package let slotIndex: Int
        package let id: String
        package let isBusy: Bool
        package let isDown: Bool
        package let estimatedRemainingMS: Double?
    }

    package struct CAWorkerSnapshot: Sendable {
        package let workerIndex: Int
        package let host: String
        package let port: Int
        package let workerSignature: String?
        package let caps: WorkerCaps?
        package let liveMSPerFrameC1: Double?
        package let transferOverheadEstimateMS: Double?
        package let txOutEstimateMS: Double?
        package let publishOverheadEstimateMS: Double?
        package let slots: [CASlotSnapshot]
    }

    package func complexityAwareSnapshot() -> [CAWorkerSnapshot] {
        let nowNanos = monotonicNowNanos()
        return workers.enumerated().map { workerIndex, worker in
            let slots = worker.slots.enumerated().map { slotIndex, slot in
                CASlotSnapshot(
                    slotIndex: slotIndex,
                    id: slot.id,
                    isBusy: slot.isBusy,
                    isDown: slot.isDown,
                    estimatedRemainingMS: slot.isBusy
                        ? Self.estimatedRemainingMS(worker: worker, slot: slot, nowNanos: nowNanos)
                        : nil
                )
            }
            return CAWorkerSnapshot(
                workerIndex: workerIndex,
                host: worker.host,
                port: worker.port,
                workerSignature: worker.workerSignature,
                caps: worker.latestCaps,
                liveMSPerFrameC1: worker.liveMSPerFrameC1EMA,
                transferOverheadEstimateMS: worker.transferOverheadEstimateMS,
                txOutEstimateMS: worker.txOutEstimateMS,
                publishOverheadEstimateMS: worker.publishOverheadEstimateMS,
                slots: slots
            )
        }
    }

    package func dispatchCapacity() -> Int {
        workers.reduce(into: 0) { total, worker in
            total += worker.slots.reduce(into: 0) { upCount, slot in
                if !slot.isDown {
                    upCount += 1
                }
            }
        }
    }

    package func setRecomputeSignal(_ signal: @escaping CARecomputeCoordinator.Signal) {
        recomputeSignal = signal
    }

    package func noteBaselineSnapshot() {
        let nowNanos = DispatchTime.now().uptimeNanoseconds
        for workerIndex in workers.indices {
            for slotIndex in workers[workerIndex].slots.indices where workers[workerIndex].slots[slotIndex].isBusy {
                if let remainingMS = workers[workerIndex].slots[slotIndex].lastEstRemainingMS {
                    workers[workerIndex].slots[slotIndex].driftBaselineRemainingMS = Double(remainingMS)
                    workers[workerIndex].slots[slotIndex].driftBaselineAtNanos = nowNanos
                    workers[workerIndex].slots[slotIndex].driftHighCount = 0
                    workers[workerIndex].slots[slotIndex].driftLowCount = 0
                    workers[workerIndex].slots[slotIndex].driftArmed = true
                } else {
                    workers[workerIndex].slots[slotIndex].driftBaselineRemainingMS = nil
                    workers[workerIndex].slots[slotIndex].driftBaselineAtNanos = nil
                    workers[workerIndex].slots[slotIndex].driftHighCount = 0
                    workers[workerIndex].slots[slotIndex].driftLowCount = 0
                }
            }
        }
    }

    package func testHookSeedRunningEstimate(workerIndex: Int, slotIndex: Int, estRemainingMS: UInt32) {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return
        }
        workers[workerIndex].slots[slotIndex].isBusy = true
        workers[workerIndex].slots[slotIndex].lastEstRemainingMS = estRemainingMS
        workers[workerIndex].slots[slotIndex].dispatchStartedAtNanos = monotonicNowNanos()
    }

    package func testHookInjectRunningTick(workerIndex: Int, slotIndex: Int, estRemainingMS: UInt32) {
        handleRunningTickFromStream(
            workerIndex: workerIndex,
            slotIndex: slotIndex,
            estRemainingMS: estRemainingMS
        )
    }

    package func testHookSetNextJobHandle(_ handle: UInt32) {
        nextJobHandle = handle
        jobHandleExhausted = false
    }

    package func testHookAllocateJobHandle() -> UInt32? {
        allocateJobHandle()
    }

    package func testHookPreflightUnavailableDispatchCount() -> Int {
        preflightUnavailableDispatchCount
    }

    package func testHookDriftState(workerIndex: Int, slotIndex: Int) -> (
        baselineRemainingMS: Double?,
        baselineAtNanos: UInt64?,
        highCount: Int,
        lowCount: Int,
        armed: Bool
    ) {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return (nil, nil, 0, 0, true)
        }
        let slot = workers[workerIndex].slots[slotIndex]
        return (
            slot.driftBaselineRemainingMS,
            slot.driftBaselineAtNanos,
            slot.driftHighCount,
            slot.driftLowCount,
            slot.driftArmed
        )
    }

    private func allocateJobHandle() -> UInt32? {
        guard !jobHandleExhausted else { return nil }
        let handle = nextJobHandle
        if handle == UInt32.max {
            jobHandleExhausted = true
        } else {
            nextJobHandle = handle + 1
        }
        return handle
    }

    private static func estimatedRemainingMS(
        worker: WorkerConnection,
        slot: SlotState,
        nowNanos: UInt64,
        concurrencyOverride: Int? = nil
    ) -> Double? {
        if let liveRemainingMS = slot.lastEstRemainingMS {
            return Double(liveRemainingMS)
        }

        let concurrency = max(1, concurrencyOverride ?? worker.activeSlots)
        let priorMS = worker.priorP50MSByConcurrency[concurrency].map(Double.init)
        let observedMS = worker.observedProcessNanosEMA.map { $0 / 1_000_000.0 }
        guard let totalRuntimeMS = observedMS ?? priorMS else { return nil }
        guard let dispatchStartedAtNanos = slot.dispatchStartedAtNanos else {
            return max(0, totalRuntimeMS)
        }
        let elapsedMS = nowNanos >= dispatchStartedAtNanos
            ? Double(nowNanos - dispatchStartedAtNanos) / 1_000_000.0
            : 0
        return max(0, totalRuntimeMS - elapsedMS)
    }

    private func updateTailTelemetryModel(
        workerIndex: Int,
        txOutSampleMS: Double?,
        publishOverheadSampleMS: Double?
    ) {
        guard workers.indices.contains(workerIndex) else { return }

        let update = ThunderboltAdaptiveTelemetryReducer.nextTailUpdate(
            previousTxOutEstimateMS: workers[workerIndex].txOutEstimateMS,
            previousPublishOverheadEstimateMS: workers[workerIndex].publishOverheadEstimateMS,
            txOutSampleMS: txOutSampleMS,
            publishOverheadSampleMS: publishOverheadSampleMS
        )
        workers[workerIndex].txOutEstimateMS = update.txOutEstimateMS
        workers[workerIndex].publishOverheadEstimateMS = update.publishOverheadEstimateMS
    }

    private func updateTransferLatencyModel(workerIndex: Int, slotIndex: Int, sampleMS: UInt32?) {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return
        }
        guard let update = TransferOverheadEstimator.reduce(
            previousBaseline: workers[workerIndex].transferStartBaselineMS,
            previousEstimate: workers[workerIndex].transferOverheadEstimateMS,
            sampleMS: sampleMS
        ) else {
            return
        }
        workers[workerIndex].transferStartBaselineMS = update.baseline
        workers[workerIndex].transferOverheadEstimateMS = update.estimate
    }

    private func markSlotDown(workerIndex: Int, slotIndex: Int, reason: String) {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return
        }
        let slot = workers[workerIndex].slots[slotIndex]
        let becameDown = !slot.isDown
        workers[workerIndex].slots[slotIndex].isDown = true
        workers[workerIndex].slots[slotIndex].downReason = reason
        workers[workerIndex].slots[slotIndex].lastEstRemainingMS = nil
        workers[workerIndex].slots[slotIndex].dispatchStartedAtNanos = nil
        workers[workerIndex].slots[slotIndex].resetDriftTracking()
        Logger.kiko.warning(
            "SLOT_DOWN \(slot.id, privacy: .public) (\(reason, privacy: .public))"
        )
        if becameDown {
            signalRecompute(.slotDownBatch)
        }
    }

    private func markSlotUp(workerIndex: Int, slotIndex: Int, includeFreshStream: Bool = false) {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return
        }
        let slot = workers[workerIndex].slots[slotIndex]
        if slot.isDown || includeFreshStream {
            Logger.kiko.info("SLOT_UP \(slot.id, privacy: .public)")
            signalRecompute(.slotUp)
        }
        stopDownSlotProbe(slotID: slot.id)
        workers[workerIndex].slots[slotIndex].isDown = false
        workers[workerIndex].slots[slotIndex].downReason = nil
        workers[workerIndex].slots[slotIndex].resetDriftTracking()
        resumeGraceWaiters(slotID: slot.id, recovered: true)
    }

    private func handleRunningTickFromStream(
        workerIndex: Int,
        slotIndex: Int,
        estRemainingMS: UInt32,
        expectedJobHandle: UInt32? = nil
    ) {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return
        }
        var slot = workers[workerIndex].slots[slotIndex]
        if let expectedJobHandle,
           slot.currentDispatchJobHandle != expectedJobHandle {
            return
        }
        slot.lastEstRemainingMS = estRemainingMS

        let nowNanos = DispatchTime.now().uptimeNanoseconds
        let liveEtaMS = Double(estRemainingMS)
        var shouldTriggerDrift = false
        if let etaAtLastSolve = slot.driftBaselineRemainingMS,
           let lastSolveNanos = slot.driftBaselineAtNanos {
            let elapsedSinceLastSolveMS = nowNanos >= lastSolveNanos
                ? Double(nowNanos - lastSolveNanos) / 1_000_000.0
                : 0
            let expected = max(0, etaAtLastSolve - elapsedSinceLastSolveMS)
            let drift = abs(liveEtaMS - expected)
            let threshold = max(
                Self.driftThresholdFloorMS,
                min(Self.driftThresholdCeilingMS, 0.20 * etaAtLastSolve)
            )

            if drift >= threshold {
                slot.driftHighCount += 1
                slot.driftLowCount = 0
                if slot.driftArmed && slot.driftHighCount >= Self.driftConsecutiveRequired {
                    let lastTrigger = slot.lastDriftTriggerNanos ?? 0
                    let canTrigger = slot.lastDriftTriggerNanos == nil
                        || nowNanos - lastTrigger >= Self.driftRecomputeCoalesceNanos
                    if canTrigger {
                        slot.lastDriftTriggerNanos = nowNanos
                        slot.driftArmed = false
                        shouldTriggerDrift = true
                    }
                    slot.driftHighCount = 0
                }
            } else if drift <= threshold * 0.5 {
                slot.driftLowCount += 1
                slot.driftHighCount = 0
                if slot.driftLowCount >= Self.driftConsecutiveRequired {
                    slot.driftArmed = true
                    slot.driftLowCount = 0
                }
            } else {
                slot.driftHighCount = 0
                slot.driftLowCount = 0
            }
        }

        workers[workerIndex].slots[slotIndex] = slot
        if shouldTriggerDrift {
            signalRecompute(.etaDrift)
        }
    }

    private func waitForGraceRecovery(
        workerIndex: Int,
        slotIndex: Int,
        slotID: String
    ) async -> Bool {
        guard workers.indices.contains(workerIndex),
              workers[workerIndex].slots.indices.contains(slotIndex) else {
            return false
        }
        guard workers[workerIndex].slots[slotIndex].isDown else {
            return true
        }

        let waiterID = UUID()
        return await withCheckedContinuation { continuation in
            graceWaitersBySlot[slotID, default: [:]][waiterID] = continuation
            Task { [self] in
                try? await Task.sleep(nanoseconds: Self.slotRecoveryGraceNanos)
                self.resolveGraceWaiter(
                    slotID: slotID,
                    waiterID: waiterID,
                    recovered: false
                )
            }
        }
    }

    private func resolveGraceWaiter(slotID: String, waiterID: UUID, recovered: Bool) {
        guard var waiters = graceWaitersBySlot[slotID],
              let continuation = waiters.removeValue(forKey: waiterID) else {
            return
        }
        if waiters.isEmpty {
            graceWaitersBySlot.removeValue(forKey: slotID)
        } else {
            graceWaitersBySlot[slotID] = waiters
        }
        continuation.resume(returning: recovered)
    }

    private func resumeGraceWaiters(slotID: String, recovered: Bool) {
        guard let waiters = graceWaitersBySlot.removeValue(forKey: slotID) else { return }
        for continuation in waiters.values {
            continuation.resume(returning: recovered)
        }
    }

    private func signalRecompute(_ trigger: RecomputeTrigger) {
        guard complexityAwareSchedulingEnabled else { return }
        guard let recomputeSignal else { return }

        switch trigger {
        case .arrive:
            recomputeSignal(.arrive)
        case .finish:
            recomputeSignal(.finish)
        case .slotDownBatch:
            recomputeSignal(.slotDownBatch)
        case .etaDrift:
            let now = DispatchTime.now().uptimeNanoseconds
            if let last = lastDriftRecomputeSignalNanos,
               now - last < Self.driftRecomputeCoalesceNanos {
                return
            }
            lastDriftRecomputeSignalNanos = now
            recomputeSignal(.etaDrift)
        case .fail:
            recomputeSignal(.fail)
        case .slotUp:
            recomputeSignal(.slotUp)
        }
    }

    private func mergeRemotePriorIfNeeded(workerIndex: Int, sourceIP: String?) {
        guard workers.indices.contains(workerIndex) else { return }
        let worker = workers[workerIndex]
        guard !priorMergedWorkers.contains(worker.host) else { return }
        guard priorProbeTasksByWorkerHost[worker.host] == nil else { return }
        guard let sourceIP else { return }

        let host = worker.host
        let port = worker.port
        let timeoutMS = connectTimeoutMS
        let queryCapabilitiesTransport = self.queryCapabilitiesTransport
        priorProbeTasksByWorkerHost[host] = Task.detached(priority: .utility) { [self] in
            let caps = queryCapabilitiesTransport(host, port, timeoutMS, sourceIP)
            await self.finishRemotePriorMergeProbe(workerHost: host, caps: caps)
        }
    }

    private func finishRemotePriorMergeProbe(workerHost: String, caps: WorkerCaps?) {
        priorProbeTasksByWorkerHost.removeValue(forKey: workerHost)
        guard !isShuttingDown else { return }
        guard !priorMergedWorkers.contains(workerHost) else { return }
        guard let caps else { return }
        let signature = caps.workerSignature

        var effectivePriorCells: [BenchmarkPriorCell] = []
        if let signature,
           let osVersion = caps.osVersion,
           let priorCells = caps.priorCells,
           !priorCells.isEmpty {
            let existingMachine = benchmarkPriorTable.machines.first { $0.signature == signature }
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
            benchmarkPriorTable.merge(
                remoteMachine: BenchmarkPriorMachine(
                    signature: signature,
                    chipName: existingMachine?.chipName ?? caps.chipName ?? "unknown",
                    performanceCores: existingMachine?.performanceCores ?? caps.performanceCores ?? 0,
                    efficiencyCores: existingMachine?.efficiencyCores ?? caps.efficiencyCores ?? 0,
                    videoEncodeEngines: existingMachine?.videoEncodeEngines ?? caps.videoEncodeEngines ?? 0,
                    osVersion: existingMachine?.osVersion ?? osVersion,
                    transcodePreset: existingMachine?.transcodePreset ?? videoTranscodePreset,
                    msPerFrameC1: resolvedMSPerFrameC1,
                    fixedOverheadMS: resolvedFixedOverheadMS,
                    avgCorpusFrameCount: resolvedAvgCorpusFrameCount,
                    affineModelSource: existingMachine?.affineModelSource ?? .legacyHeuristic,
                    cells: priorCells
                )
            )
            effectivePriorCells = priorCells
            Logger.kiko.info(
                "Merged remote benchmark prior for worker \(workerHost, privacy: .public): \(priorCells.count) cell(s)"
            )
        } else {
            if let signature {
                effectivePriorCells = Self.priorCells(forSignature: signature, table: benchmarkPriorTable)
            }
            if !effectivePriorCells.isEmpty {
                Logger.kiko.info(
                    "Applied persisted benchmark prior for worker \(workerHost, privacy: .public): \(effectivePriorCells.count) cell(s)"
                )
            }
        }

        let priorByConcurrency = Dictionary(
            effectivePriorCells.map { ($0.concurrency, $0.msPerVideoP50) },
            uniquingKeysWith: { _, newer in newer }
        )
        if effectivePriorCells.isEmpty {
            Logger.kiko.warning(
                "Merged worker \(workerHost, privacy: .public) has no effective benchmark prior cells"
            )
        }
        for workerIndex in workers.indices where workers[workerIndex].host == workerHost {
            workers[workerIndex].workerSignature = signature
            workers[workerIndex].latestCaps = caps
            workers[workerIndex].priorP50MSByConcurrency = priorByConcurrency
        }
        priorMergedWorkers.insert(workerHost)
    }

    package static func priorCells(forSignature signature: String, table: BenchmarkPriorTable) -> [BenchmarkPriorCell] {
        table.machines.first(where: { $0.signature == signature })?.cells ?? []
    }

    func recordSuccessfulExecutionSampleForTesting(
        workerIndex: Int,
        processNanos: UInt64,
        frameCount: Double,
        concurrency: Int,
        successfulExecutionSampleModel: CASuccessfulExecutionSampleModel
    ) {
        recordSuccessfulExecutionSample(
            workerIndex: workerIndex,
            processNanos: processNanos,
            frameCount: frameCount,
            concurrency: concurrency,
            sampleModel: successfulExecutionSampleModel
        )
    }

    func recordTransferOverheadSampleForTesting(
        workerIndex: Int,
        slotIndex: Int = 0,
        sampleMS: UInt32?
    ) {
        updateTransferLatencyModel(
            workerIndex: workerIndex,
            slotIndex: slotIndex,
            sampleMS: sampleMS
        )
    }

    private func recordSuccessfulExecutionSample(
        workerIndex: Int,
        processNanos: UInt64,
        frameCount: Double,
        concurrency: Int,
        sampleModel: CASuccessfulExecutionSampleModel?
    ) {
        guard workers.indices.contains(workerIndex),
              frameCount.isFinite,
              frameCount > 0 else {
            return
        }
        guard let sampleModel,
              let actualMSPerFrame = ThunderboltAdaptiveTelemetryReducer.normalizedMSPerFrameC1(
                    processNanos: processNanos,
                    frameCount: frameCount,
                    model: sampleModel,
                    concurrency: concurrency
              ),
              let update = LiveAdaptiveMSPerFrameC1Estimator.next(
                    previousEstimate: workers[workerIndex].liveMSPerFrameC1EMA,
                    previousSmoothedError: workers[workerIndex].liveMSPerFrameC1ErrorEMA,
                    previousSmoothedAbsoluteError: workers[workerIndex].liveMSPerFrameC1AbsErrorEMA,
                    initialEstimate: sampleModel.msPerFrameC1,
                    observed: actualMSPerFrame
              ) else {
            return
        }
        workers[workerIndex].liveMSPerFrameC1EMA = update.estimate
        workers[workerIndex].liveMSPerFrameC1ErrorEMA = update.smoothedError
        workers[workerIndex].liveMSPerFrameC1AbsErrorEMA = update.smoothedAbsoluteError
    }

    private func resolvedSuccessfulExecutionSampleModel(
        workerIndex: Int
    ) -> CASuccessfulExecutionSampleModel? {
        guard workers.indices.contains(workerIndex) else { return nil }
        let worker = workers[workerIndex]

        if let signature = worker.workerSignature {
            let priorMachine = benchmarkPriorTable.exactMachine(signature: signature)
                ?? benchmarkPriorTable.hardwareCompatibleMachine(signature: signature)
            if let priorMachine,
               let affineModel = CAProfileAndFallbackMath.resolvedRemoteAffineModel(from: priorMachine) {
                return CASuccessfulExecutionSampleModel(
                    msPerFrameC1: affineModel.msPerFrameC1,
                    fixedOverheadMS: affineModel.fixedOverheadMS,
                    degradationCurve: CAProfileAndFallbackMath.degradationCurve(from: priorMachine)
                )
            }
        }

        let capsCurve = CAProfileAndFallbackMath.degradationCurve(from: worker.latestCaps?.priorCells)
        if !capsCurve.isEmpty,
           let capsMSPerFrameC1 = CAProfileAndFallbackMath.validMSPerFrameC1(
                worker.liveMSPerFrameC1EMA ?? worker.latestCaps?.msPerFrameC1
           ) {
            return CASuccessfulExecutionSampleModel(
                msPerFrameC1: capsMSPerFrameC1,
                fixedOverheadMS: 0,
                degradationCurve: capsCurve
            )
        }

        let rawCapsCurve = CAProfileAndFallbackMath.degradationCurve(from: worker.latestCaps?.degradationCurve)
        if !rawCapsCurve.isEmpty,
           let capsMSPerFrameC1 = CAProfileAndFallbackMath.validMSPerFrameC1(
                worker.liveMSPerFrameC1EMA ?? worker.latestCaps?.msPerFrameC1
           ) {
            return CASuccessfulExecutionSampleModel(
                msPerFrameC1: capsMSPerFrameC1,
                fixedOverheadMS: 0,
                degradationCurve: rawCapsCurve
            )
        }

        if let liveMSPerFrameC1 = CAProfileAndFallbackMath.validMSPerFrameC1(worker.liveMSPerFrameC1EMA) {
            return CASuccessfulExecutionSampleModel(
                msPerFrameC1: liveMSPerFrameC1,
                fixedOverheadMS: 0,
                degradationCurve: [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)]
            )
        }
        return nil
    }

    package func warmupPrior() {
        for workerIndex in workers.indices {
            let sourceIP = sourceIP(for: workers[workerIndex].host)
            mergeRemotePriorIfNeeded(workerIndex: workerIndex, sourceIP: sourceIP)
        }
    }

    private func sourceIP(for host: String) -> String? {
        let workerAddress: UInt32
        if let cached = resolvedWorkerAddressByHost[host] {
            workerAddress = cached
        } else {
            let nowNanos = DispatchTime.now().uptimeNanoseconds
            if let retryAfterNanos = unresolvedWorkerHosts[host], nowNanos < retryAfterNanos {
                return nil
            }
            guard let resolved = Self.resolveWorkerIPv4(host) else {
                unresolvedWorkerHosts[host] = nowNanos &+ Self.unresolvedHostRetryBackoffNanos
                return nil
            }
            unresolvedWorkerHosts.removeValue(forKey: host)
            resolvedWorkerAddressByHost[host] = resolved
            workerAddress = resolved
        }

        return Self.sourceIPForWorkerAddress(workerAddress, bridgeSources: bridgeSources)
    }

    private static func dispatchToWorker(
        uploadId: String,
        filePath: String,
        originalName: String,
        mimeType: String,
        fd: Int32,
        thumbsDir: String,
        previewsDir: String,
        sha256BufferSize: Int,
        complexityAwareSchedulingEnabled: Bool,
        jobHandle: UInt32,
        sessionID: UInt32,
        onRunningTick: (@Sendable (UInt32) -> Void)?,
        monotonicNowNanos: @Sendable () -> UInt64
    ) -> DispatchOutcome {
        let previewPath = "\(previewsDir)/\(uploadId).mp4"
        let thumbPath = "\(thumbsDir)/\(uploadId).jpg"

        let fail: (String, ProgressTickV2.ErrorClass?, UInt32?, Bool) -> DispatchOutcome = { reason, errorClass, firstRunningLatencyMS, slotHealthDown in
            cleanupPaths(previewPath: previewPath, thumbPath: thumbPath)
            return .failed(
                reason: reason,
                errorClass: errorClass,
                firstRunningLatencyMS: firstRunningLatencyMS,
                slotHealthDown: slotHealthDown
            )
        }

        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: filePath)
            guard let sizeValue = attributes[.size] as? NSNumber else {
                return fail("could not read source file size", nil, nil, true)
            }
            let fileSize = sizeValue.intValue
            guard fileSize >= 0 else {
                return fail("invalid source file size", nil, nil, true)
            }
            let sourceSHA256 = try SHA256Utility.calculateSHA256(path: filePath, bufferSize: sha256BufferSize)

            let dispatchSentAtNanos = monotonicNowNanos()
            guard ThunderboltTransport.sendHeader(
                fd: fd,
                fileSize: fileSize,
                sha256Hex: sourceSHA256,
                name: originalName,
                mime: mimeType
            ) else {
                return fail("send header failed", nil, nil, true)
            }

            guard ThunderboltTransport.sendFileData(fd: fd, filePath: filePath, fileSize: fileSize) else {
                return fail("send file failed", nil, nil, true)
            }

            var firstRunningLatencyMS: UInt32?
            var lastEstRemainingMS: UInt32?
            if complexityAwareSchedulingEnabled {
                switch readTickV2Stream(
                    fd: fd,
                    expectedJobHandle: jobHandle,
                    expectedSessionID: sessionID,
                    dispatchSentAtNanos: dispatchSentAtNanos,
                    onRunningTick: onRunningTick,
                    monotonicNowNanos: monotonicNowNanos
                ) {
                case .complete(let latencyMS, let estRemainingMS):
                    firstRunningLatencyMS = latencyMS
                    lastEstRemainingMS = estRemainingMS
                case .failed(let reason, let errorClass, let latencyMS, let slotHealthDown):
                    return fail(reason, errorClass, latencyMS, slotHealthDown)
                }
            }
            let tickCompletionBoundaryNanos = monotonicNowNanos()

            let response: (
                status: UInt8,
                processNanos: UInt64,
                prevSize: Int,
                prevSHA256: String,
                thumbSize: Int,
                thumbSHA256: String
            )?
            if complexityAwareSchedulingEnabled {
                ThunderboltTransport.setReadTimeout(fd: fd, timeoutMS: postTickReadTimeoutMS)
                response = readResponseHeaderAfterTickStream(
                    fd: fd,
                    expectedJobHandle: jobHandle,
                    expectedSessionID: sessionID
                )
            } else {
                response = ThunderboltTransport.readResponseHeader(fd: fd)
            }

            guard let response else {
                return fail("read response header failed", nil, firstRunningLatencyMS, true)
            }

            guard response.status == 0x01 else {
                return fail("worker returned status \(response.status)", nil, firstRunningLatencyMS, true)
            }

            guard ThunderboltTransport.readToFile(fd: fd, count: response.prevSize, path: previewPath) else {
                return fail("read preview payload failed", nil, firstRunningLatencyMS, true)
            }

            guard ThunderboltTransport.readToFile(fd: fd, count: response.thumbSize, path: thumbPath) else {
                return fail("read thumbnail payload failed", nil, firstRunningLatencyMS, true)
            }
            let payloadReadCompletedAtNanos = monotonicNowNanos()

            let previewSHA256 = try SHA256Utility.calculateSHA256(path: previewPath, bufferSize: sha256BufferSize)
            guard previewSHA256 == response.prevSHA256.lowercased() else {
                return fail("preview SHA mismatch", nil, firstRunningLatencyMS, true)
            }

            let thumbSHA256 = try SHA256Utility.calculateSHA256(path: thumbPath, bufferSize: sha256BufferSize)
            guard thumbSHA256 == response.thumbSHA256.lowercased() else {
                return fail("thumbnail SHA mismatch", nil, firstRunningLatencyMS, true)
            }
            let finalizationCompletedAtNanos = monotonicNowNanos()

            return .success(
                processNanos: response.processNanos,
                firstRunningLatencyMS: firstRunningLatencyMS,
                lastEstRemainingMS: lastEstRemainingMS,
                txOutMS: elapsedMilliseconds(
                    startNanos: tickCompletionBoundaryNanos,
                    endNanos: payloadReadCompletedAtNanos
                ),
                publishOverheadMS: elapsedMilliseconds(
                    startNanos: payloadReadCompletedAtNanos,
                    endNanos: finalizationCompletedAtNanos
                )
            )
        } catch {
            return fail("error: \(error.localizedDescription)", nil, nil, true)
        }
    }

    private static func cleanupPaths(previewPath: String, thumbPath: String) {
        try? FileManager.default.removeItem(atPath: previewPath)
        try? FileManager.default.removeItem(atPath: thumbPath)
    }

    private static func elapsedMilliseconds(startNanos: UInt64, endNanos: UInt64) -> Double {
        guard endNanos >= startNanos else { return 0 }
        return Double(endNanos - startNanos) / 1_000_000.0
    }

    private enum TickStreamOutcome {
        case complete(
            firstRunningLatencyMS: UInt32?,
            lastEstRemainingMS: UInt32?
        )
        case failed(
            reason: String,
            errorClass: ProgressTickV2.ErrorClass?,
            firstRunningLatencyMS: UInt32?,
            slotHealthDown: Bool
        )
    }

    private static func readTickV2Stream(
        fd: Int32,
        expectedJobHandle: UInt32,
        expectedSessionID: UInt32,
        dispatchSentAtNanos: UInt64,
        onRunningTick: (@Sendable (UInt32) -> Void)?,
        monotonicNowNanos: @Sendable () -> UInt64
    ) -> TickStreamOutcome {
        var previousProgress: Float?
        var consecutiveInvalid = 0
        var firstRunningLatencyMS: UInt32?
        var lastEstRemainingMS: UInt32?

        while true {
            guard let frame = ThunderboltTransport.readTickFrameV2(
                fd: fd,
                timeoutMS: slotTickStalenessTimeoutMS
            ) else {
                return .failed(
                    reason: "tick v2 stream stale/closed (>1s without tick)",
                    errorClass: .transient,
                    firstRunningLatencyMS: firstRunningLatencyMS,
                    slotHealthDown: true
                )
            }

            guard let tick = try? ProgressTickV2.decode(frame) else {
                consecutiveInvalid += 1
                if consecutiveInvalid >= 4 {
                    return .failed(
                        reason: "tick v2 decode failed 4 consecutive times",
                        errorClass: .transient,
                        firstRunningLatencyMS: firstRunningLatencyMS,
                        slotHealthDown: true
                    )
                }
                continue
            }

            let outcome = ProgressTickV2Validator.validate(
                tick: tick,
                previousProgress: previousProgress,
                knownJobHandle: expectedJobHandle,
                expectedSessionID: expectedSessionID
            )
            switch outcome {
            case .valid:
                consecutiveInvalid = 0
                previousProgress = tick.progress

                switch tick.status {
                case .running:
                    if firstRunningLatencyMS == nil {
                        let nowNanos = monotonicNowNanos()
                        let elapsedFromTickNanos = UInt64(tick.elapsedMS) * 1_000_000
                        let runningStartedAtNanos = nowNanos >= elapsedFromTickNanos
                            ? nowNanos - elapsedFromTickNanos
                            : 0
                        let delta = runningStartedAtNanos >= dispatchSentAtNanos
                            ? (runningStartedAtNanos - dispatchSentAtNanos) / 1_000_000
                            : 0
                        firstRunningLatencyMS = UInt32(min(delta, UInt64(UInt32.max)))
                    }

                    lastEstRemainingMS = tick.estRemainingMS
                    onRunningTick?(tick.estRemainingMS)
                    continue
                case .complete:
                    return .complete(
                        firstRunningLatencyMS: firstRunningLatencyMS,
                        lastEstRemainingMS: lastEstRemainingMS
                    )
                case .failed:
                    return .failed(
                        reason: "worker reported failed tick v2 with error class \(tick.errorClass.rawValue)",
                        errorClass: tick.errorClass,
                        firstRunningLatencyMS: firstRunningLatencyMS,
                        slotHealthDown: false
                    )
                }
            case .invalid(let issue):
                consecutiveInvalid += 1
                if consecutiveInvalid >= 4 {
                    return .failed(
                        reason: "tick v2 invalid 4 consecutive times (\(issue))",
                        errorClass: .transient,
                        firstRunningLatencyMS: firstRunningLatencyMS,
                        slotHealthDown: true
                    )
                }
            }
        }
    }

    private static func readResponseHeaderAfterTickStream(
        fd: Int32,
        expectedJobHandle: UInt32,
        expectedSessionID: UInt32
    ) -> (
        status: UInt8,
        processNanos: UInt64,
        prevSize: Int,
        prevSHA256: String,
        thumbSize: Int,
        thumbSHA256: String
    )? {
        let tickFrameBytes = ProgressTickV2.encodedByteCount
        let responseHeaderBytes = 145

        var trailingRunningTickCount = 0
        while trailingRunningTickCount < maxTrailingRunningTicksBeforeHeader {
            guard let prefix = ThunderboltTransport.readBytes(fd: fd, count: tickFrameBytes) else {
                return nil
            }

            let isTrailingRunningTick: Bool
            if let tick = try? ProgressTickV2.decode(prefix),
               tick.status == .running,
               case .valid = ProgressTickV2Validator.validate(
                   tick: tick,
                   previousProgress: nil,
                   knownJobHandle: expectedJobHandle,
                   expectedSessionID: expectedSessionID
               ) {
                isTrailingRunningTick = true
            } else {
                isTrailingRunningTick = false
            }

            if isTrailingRunningTick {
                trailingRunningTickCount += 1
                continue
            }

            guard let suffix = ThunderboltTransport.readBytes(
                fd: fd,
                count: responseHeaderBytes - tickFrameBytes
            ) else {
                return nil
            }
            var header = Data(capacity: responseHeaderBytes)
            header.append(prefix)
            header.append(suffix)
            return ThunderboltTransport.parseResponseHeader(header)
        }

        return ThunderboltTransport.readResponseHeader(fd: fd)
    }

    private static let workerMimeTickTag = "#kiko-v2:"
    package static let slotTickStalenessTimeoutMS: Int32 = 1_001
    private static let postTickReadTimeoutMS: Int32 = 5_000
    private static let maxTrailingRunningTicksBeforeHeader = 8
    private static let driftThresholdFloorMS: Double = 250
    private static let driftThresholdCeilingMS: Double = 2_000
    private static let driftConsecutiveRequired = 2
    private static let driftRecomputeCoalesceNanos: UInt64 = 200_000_000
    private static let slotRecoveryGraceNanos: UInt64 = 1_500_000_000
    private static let downSlotProbeInitialNanos: UInt64 = 250_000_000
    private static let downSlotProbeMaxNanos: UInt64 = 4_000_000_000
    private static let downSlotProbeJitterNanos: UInt64 = 100_000_000
    private static let downSlotProbeConnectTimeoutMS: Int = 500
    private static let unresolvedHostRetryBackoffNanos: UInt64 = 30_000_000_000

    package static func isTickStreamStale(elapsedMS: UInt64) -> Bool {
        elapsedMS > 1_000
    }

    private static func makeWorkerMime(
        baseMime: String,
        complexityAwareSchedulingEnabled: Bool,
        jobHandle: UInt32,
        sessionID: UInt32
    ) -> String {
        guard complexityAwareSchedulingEnabled else { return baseMime }
        let base = baseMime.replacingOccurrences(of: workerMimeTickTag, with: "")
        return "\(base)\(workerMimeTickTag)h=\(jobHandle),s=\(sessionID)"
    }

    package static func discoverBridgeSources() -> [BridgeSource] {
        var list: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&list) == 0, let first = list else { return [] }
        defer { freeifaddrs(first) }

        var results: [BridgeSource] = []
        var cursor: UnsafeMutablePointer<ifaddrs>? = first
        while let current = cursor {
            defer { cursor = current.pointee.ifa_next }

            let interfaceName = String(cString: current.pointee.ifa_name)
            guard interfaceName.hasPrefix("bridge"),
                  let address = current.pointee.ifa_addr,
                  let netmask = current.pointee.ifa_netmask,
                  address.pointee.sa_family == UInt8(AF_INET),
                  netmask.pointee.sa_family == UInt8(AF_INET)
            else {
                continue
            }

            let addressValue = address.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee }
            let netmaskValue = netmask.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee }
            guard let sourceIP = ipv4String(addressValue.sin_addr) else { continue }

            let ip = UInt32(bigEndian: addressValue.sin_addr.s_addr)
            let mask = UInt32(bigEndian: netmaskValue.sin_addr.s_addr)
            let network = ip & mask
            results.append(BridgeSource(name: interfaceName, ip: sourceIP, network: network, mask: mask))
        }

        return results
    }

    package static func sourceIPForWorkerHost(_ host: String, bridgeSources: [BridgeSource]) -> String? {
        guard let workerAddress = resolveWorkerIPv4(host) else { return nil }
        return sourceIPForWorkerAddress(workerAddress, bridgeSources: bridgeSources)
    }

    private static func parseIPv4(_ host: String) -> UInt32? {
        var address = in_addr()
        let rc = host.withCString { cString in
            inet_pton(AF_INET, cString, &address)
        }
        guard rc == 1 else { return nil }
        return UInt32(bigEndian: address.s_addr)
    }

    private static func resolveWorkerIPv4(_ host: String) -> UInt32? {
        if let literalAddress = parseIPv4(host) {
            return literalAddress
        }

        var hints = addrinfo(
            ai_flags: AI_ADDRCONFIG,
            ai_family: AF_INET,
            ai_socktype: SOCK_STREAM,
            ai_protocol: IPPROTO_TCP,
            ai_addrlen: 0,
            ai_canonname: nil,
            ai_addr: nil,
            ai_next: nil
        )
        var result: UnsafeMutablePointer<addrinfo>?
        let resolveRC = getaddrinfo(host, nil, &hints, &result)
        guard resolveRC == 0, let start = result else { return nil }
        defer { freeaddrinfo(start) }

        var current: UnsafeMutablePointer<addrinfo>? = start
        while let ai = current {
            defer { current = ai.pointee.ai_next }
            guard ai.pointee.ai_family == AF_INET,
                  let socketAddress = ai.pointee.ai_addr else {
                continue
            }
            let addressValue = socketAddress.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee }
            return UInt32(bigEndian: addressValue.sin_addr.s_addr)
        }
        return nil
    }

    private static func sourceIPForWorkerAddress(_ workerAddress: UInt32, bridgeSources: [BridgeSource]) -> String? {
        if let source = bridgeSources.first(where: { (workerAddress & $0.mask) == $0.network }) {
            return source.ip
        }
        return nil
    }

    package static func ipv4String(_ address: in_addr) -> String? {
        var mutableAddress = address
        var buffer = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
        let cString = inet_ntop(AF_INET, &mutableAddress, &buffer, socklen_t(INET_ADDRSTRLEN))
        guard cString != nil else { return nil }
        let bytes = buffer.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }
        return String(decoding: bytes, as: UTF8.self)
    }
}
