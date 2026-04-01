import Foundation
import OSLog
import UniformTypeIdentifiers

package enum SchedulingPolicy: Sendable {
    case fifo
    case complexityAware
}

package enum EstimateConfidence: Sendable, Equatable {
    case high
    case low
}

package actor MediaProcessor {
    package struct VideoHoldMetadata: Sendable, Equatable {
        package let wakeAt: Date
        package let targetSlotID: String
    }

    package enum VideoJobState: Sendable, Equatable {
        case queued
        case held(VideoHoldMetadata)
        case active
    }

    package struct ProcessingLaunchEvent: Sendable, Equatable {
        package enum Site: Sendable, Equatable {
            case imageThumbnail
            case imagePreview
            case imageTimestamp
            case videoThumbnail
            case videoPreview
            case videoTimestamp
            case archiveDetached
        }

        package let site: Site
        package let inheritsProcessingContext: Bool
    }

    @TaskLocal
    static var processingLaunchObserver: (@Sendable (ProcessingLaunchEvent) -> Void)?

    @TaskLocal
    static var processingTopologyContext: UUID?

    @TaskLocal
    static var videoDispatchObserver: (@Sendable (_ uploadId: String, _ frameCount: Double, _ videoCost: CAResolvedVideoCost) -> Void)?

    @TaskLocal
    static var metadataProbeTimeoutOverrideNanos: UInt64?

    package struct RebuildProbeConcurrencyEvent: Sendable, Equatable {
        package enum Phase: Sendable, Equatable {
            case scheduled
            case completed
        }

        package let phase: Phase
        package let inFlight: Int
        package let cap: Int
    }

    @TaskLocal
    static var rebuildProbeConcurrencyObserver: (@Sendable (RebuildProbeConcurrencyEvent) -> Void)?

    @TaskLocal
    static var rebuildMountCheckOverride: (@Sendable (String) -> Bool)?

    package typealias RemoteVideoDispatchOverride =
        @Sendable (_ uploadId: String, _ filePath: String, _ originalName: String, _ mimeType: String?) async
            -> (thumb: Bool, preview: Bool)?
    package typealias LocalVideoProcessingOverride =
        @Sendable (_ uploadId: String, _ filePath: String, _ mimeType: String?) async
            -> (thumb: Bool, preview: Bool, timestamp: String)
    package typealias StatusUpdateOverride =
        @Sendable (_ id: String, _ status: Asset.AssetStatus) async throws -> Void
    package typealias MarkCompleteOverride =
        @Sendable (_ id: String, _ timestamp: String) async throws -> Void

    package static func withProcessingLaunchObserver<T>(
        _ observer: @escaping @Sendable (ProcessingLaunchEvent) -> Void,
        operation: () throws -> T
    ) rethrows -> T {
        try $processingLaunchObserver.withValue(observer) {
            try operation()
        }
    }

    package static func withProcessingLaunchObserver<T>(
        _ observer: @escaping @Sendable (ProcessingLaunchEvent) -> Void,
        operation: () async throws -> T
    ) async rethrows -> T {
        try await $processingLaunchObserver.withValue(observer) {
            try await operation()
        }
    }

    package static func withRebuildProbeConcurrencyObserver<T>(
        _ observer: @escaping @Sendable (RebuildProbeConcurrencyEvent) -> Void,
        operation: () throws -> T
    ) rethrows -> T {
        try $rebuildProbeConcurrencyObserver.withValue(observer) {
            try operation()
        }
    }

    package static func withRebuildProbeConcurrencyObserver<T>(
        _ observer: @escaping @Sendable (RebuildProbeConcurrencyEvent) -> Void,
        operation: () async throws -> T
    ) async rethrows -> T {
        try await $rebuildProbeConcurrencyObserver.withValue(observer) {
            try await operation()
        }
    }

    package static func withVideoDispatchObserver<T>(
        _ observer: @escaping @Sendable (_ uploadId: String, _ frameCount: Double, _ videoCost: CAResolvedVideoCost) -> Void,
        operation: () throws -> T
    ) rethrows -> T {
        try $videoDispatchObserver.withValue(observer) {
            try operation()
        }
    }

    package static func withVideoDispatchObserver<T>(
        _ observer: @escaping @Sendable (_ uploadId: String, _ frameCount: Double, _ videoCost: CAResolvedVideoCost) -> Void,
        operation: () async throws -> T
    ) async rethrows -> T {
        try await $videoDispatchObserver.withValue(observer) {
            try await operation()
        }
    }

    package static func withRebuildMountCheckOverride<T>(
        _ override: @escaping @Sendable (String) -> Bool,
        operation: () throws -> T
    ) rethrows -> T {
        try $rebuildMountCheckOverride.withValue(override) {
            try operation()
        }
    }

    package static func withRebuildMountCheckOverride<T>(
        _ override: @escaping @Sendable (String) -> Bool,
        operation: () async throws -> T
    ) async rethrows -> T {
        try await $rebuildMountCheckOverride.withValue(override) {
            try await operation()
        }
    }

    static func emitProcessingLaunchEvent(
        _ site: ProcessingLaunchEvent.Site,
        observer: (@Sendable (ProcessingLaunchEvent) -> Void)? = nil
    ) {
        let event = ProcessingLaunchEvent(
            site: site,
            inheritsProcessingContext: processingTopologyContext != nil
        )
        if let observer {
            observer(event)
        } else {
            processingLaunchObserver?(event)
        }
    }

    static func emitRebuildProbeConcurrencyEvent(
        phase: RebuildProbeConcurrencyEvent.Phase,
        inFlight: Int,
        cap: Int,
        observer: (@Sendable (RebuildProbeConcurrencyEvent) -> Void)? = nil
    ) {
        let event = RebuildProbeConcurrencyEvent(
            phase: phase,
            inFlight: inFlight,
            cap: cap
        )
        if let observer {
            observer(event)
        } else {
            rebuildProbeConcurrencyObserver?(event)
        }
    }

    let config: Config
    let database: Database
    let archiveOriginal: @Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult
    let moderationMarkers: ModerationMarkers
    let thunderboltDispatcher: ThunderboltDispatcher?
    let videoSelectionPolicy: SchedulingPolicy
    private let localComplexityAwareProfile: ComplexityAwareMachineProfile?
    let remoteVideoDispatchOverride: RemoteVideoDispatchOverride?
    let localVideoProcessingOverride: LocalVideoProcessingOverride?
    let statusUpdateOverride: StatusUpdateOverride?
    let markCompleteOverride: MarkCompleteOverride?
    private let configuredVideoSlots: Int

    private typealias ComplexityAwareMachineProfile = CAValidatedPriorProfile

    package enum VideoRoutingDirective: Sendable {
        case local(localSlotIndex: Int)
        case remote(workerIndex: Int, slotIndex: Int, slotID: String)
    }

    private var imageQueue: [ProcessingJob] = []
    private var imageQueueHead = 0
    private var pendingHeldVideoState = CAPendingHeldVideoState()
    private var heldVideoWakeTask: Task<Void, Never>?

    private var activeJobs: [String: Asset.AssetType] = [:]
    private var activeImageCount = 0
    private var activeTasks: [String: Task<Void, Never>] = [:]
    private var dispatchState = CADispatchState()
    private var localLiveMSPerFrameC1EMA: Double?
    private var localLiveMSPerFrameC1ErrorEMA: Double?
    private var localLiveMSPerFrameC1AbsErrorEMA: Double?
    private var isShuttingDown = false
    private var recomputeCoordinator = CARecomputeCoordinator()
    private var recomputeSignal: CARecomputeCoordinator.Signal?
    private var schedulingSuccessfulJobs: [SchedulingSuccessfulJob] = []
    private var schedulingSuccessfulIDs: Set<String> = []
    private var schedulingFailedIDs: Set<String> = []
    private var schedulingVideoIDs: Set<String> = []
    private static let metadataProbeTimeoutNanos: UInt64 = 25_000_000

    package init(
        config: Config,
        database: Database,
        moderationMarkers: ModerationMarkers,
        archiveOriginal: (@Sendable (_ sourcePath: String, _ assetId: String, _ originalName: String) async -> ArchiveResult)? = nil,
        thunderboltDispatcher: ThunderboltDispatcher? = nil,
        benchmarkPriorTable: BenchmarkPriorTable? = nil,
        priorArtifactState: CAActivationGate.PriorArtifactState? = nil,
        complexityAwareSchedulingEnabled: Bool? = nil,
        remoteVideoDispatchOverride: RemoteVideoDispatchOverride? = nil,
        localVideoProcessingOverride: LocalVideoProcessingOverride? = nil,
        statusUpdateOverride: StatusUpdateOverride? = nil,
        markCompleteOverride: MarkCompleteOverride? = nil
    ) {
        self.config = config
        self.database = database
        self.moderationMarkers = moderationMarkers
        let effectiveThunderboltDispatcher = config.videoSchedulerPolicy == .none ? nil : thunderboltDispatcher
        self.thunderboltDispatcher = effectiveThunderboltDispatcher
        self.localComplexityAwareProfile = CAActivationGate.resolveLocalPriorProfile(
            priorTable: benchmarkPriorTable,
            videoTranscodePreset: config.videoTranscodePreset
        )
        let effectiveComplexityAwareSchedulingEnabled: Bool
        switch config.videoSchedulerPolicy {
        case .auto:
            let effectivePriorArtifactState = priorArtifactState ?? (benchmarkPriorTable == nil ? .missing : .loaded)
            let activationDecision = CAActivationGate.evaluate(
                workersPresent: !config.thunderboltWorkers.isEmpty,
                priorArtifactState: effectivePriorArtifactState,
                localPriorProfile: self.localComplexityAwareProfile,
                strictTickV2Accepted: complexityAwareSchedulingEnabled
            )
            effectiveComplexityAwareSchedulingEnabled = activationDecision.isEnabled
        case .fifo, .none:
            effectiveComplexityAwareSchedulingEnabled = false
        }
        self.videoSelectionPolicy = effectiveComplexityAwareSchedulingEnabled ? .complexityAware : .fifo
        self.remoteVideoDispatchOverride = remoteVideoDispatchOverride
        self.localVideoProcessingOverride = localVideoProcessingOverride
        self.statusUpdateOverride = statusUpdateOverride
        self.markCompleteOverride = markCompleteOverride
        let remoteVideoSlots = effectiveThunderboltDispatcher == nil
            ? 0
            : config.thunderboltWorkers.reduce(into: 0) { total, worker in
                total += worker.slots
            }
        self.configuredVideoSlots = config.maxConcurrentVideos + remoteVideoSlots

        if let archiveOriginal {
            self.archiveOriginal = archiveOriginal
        } else {
            let storage = StorageManager(externalSSDPath: config.externalSSDPath, sha256BufferSize: config.sha256BufferSize)
            self.archiveOriginal = { sourcePath, assetId, originalName in
                await storage.archiveOriginal(sourcePath: sourcePath, assetId: assetId, originalName: originalName)
            }
        }

        if effectiveComplexityAwareSchedulingEnabled, let thunderboltDispatcher = effectiveThunderboltDispatcher {
            Task.detached(priority: .utility) {
                await thunderboltDispatcher.warmupPrior()
            }
        }
    }

    package func enqueueRecoveryJob(
        uploadId: String,
        originalName: String,
        filePath: String,
        assetType: Asset.AssetType,
        arrivalAtSeconds: Double = Date().timeIntervalSince1970,
        isRepair: Bool = false,
        restoreStatus: Asset.AssetStatus? = nil
    ) async {
        enqueueJob(
            await makeQueuedJob(
                uploadId: uploadId,
                originalName: originalName,
                filePath: filePath,
                assetType: assetType,
                arrivalAtSeconds: arrivalAtSeconds,
                isRepair: isRepair,
                restoreStatus: restoreStatus
            )
        )
    }

    package func restartQueuesAfterRecovery() async {
        await processQueues()
    }

    package func canAcceptWebhookBackpressure() -> Bool {
        guard !isShuttingDown else { return false }
        return pendingQueueCount() < config.maxPendingWebhookJobs
    }

    package func enqueueWebhookAsset(
        uploadId: String,
        originalName: String,
        filePath: String,
        assetType: Asset.AssetType
    ) async -> Bool {
        await enqueue(
            uploadId: uploadId,
            originalName: originalName,
            filePath: filePath,
            assetType: assetType
        )
    }

    package func enqueue(uploadId: String, originalName: String, filePath: String, assetType: Asset.AssetType) async -> Bool {
        guard canAcceptWebhookBackpressure() else {
            if isShuttingDown {
                Logger.kiko.warning("Skipping enqueue for \(uploadId): media processor is shutting down")
            } else {
                Logger.kiko.warning(
                    "Rejecting enqueue for \(uploadId): pending queue depth reached limit (\(self.config.maxPendingWebhookJobs))"
                )
            }
            return false
        }

        enqueueJob(
            await makeQueuedJob(
                uploadId: uploadId,
                originalName: originalName,
                filePath: filePath,
                assetType: assetType
            )
        )

        signalRecompute(.arrive)
        await processQueues(reconsiderHeldJobs: true)
        return true
    }

    package func shutdown() async {
        guard !isShuttingDown else { return }
        isShuttingDown = true
        recomputeCoordinator.cancelPendingSlotDownBatchFlush()
        applyHeldVideoWakePlan(pendingHeldVideoState.clearAll())

        imageQueue.removeAll(keepingCapacity: false)
        dispatchState.clearShutdownPendingState()
        imageQueueHead = 0

        let runningTasks = Array(activeTasks.values)
        guard !runningTasks.isEmpty else {
            logSchedulingMetrics(reason: "shutdown")
            Logger.kiko.info("Media processor shutdown complete: no active jobs")
            return
        }

        Logger.kiko.info("Media processor shutdown: cancelling \(runningTasks.count) active jobs")
        for task in runningTasks {
            task.cancel()
        }
        for task in runningTasks {
            await task.value
        }
        logSchedulingMetrics(reason: "shutdown")
        Logger.kiko.info("Media processor shutdown complete")
    }

    func enqueueJob(_ job: ProcessingJob) {
        switch job.assetType {
        case .image:
            imageQueue.append(job)
        case .video:
            pendingHeldVideoState.enqueue(job)
            schedulingVideoIDs.insert(job.uploadId)
        }
    }

    func deferTransientRequeue(_ job: ProcessingJob) {
        dispatchState.deferTransientRequeue(job)
    }

    private func pendingQueueCount() -> Int {
        let imagePending = imageQueue.count - imageQueueHead
        return imagePending + pendingHeldVideoState.pendingCount
    }

    package func setVideoHold(uploadId: String, hold: VideoHoldMetadata?) async -> Bool {
        guard !isShuttingDown else { return false }
        guard activeJobs[uploadId] != .video else { return false }

        if let hold {
            if hold.wakeAt <= Date() {
                if pendingHeldVideoState.heldMetadata(uploadId: uploadId) != nil {
                    return await releaseVideoHold(uploadId: uploadId)
                }
                guard pendingHeldVideoState.containsQueued(uploadId: uploadId) else {
                    return false
                }
                await processQueues()
                return true
            }

            if let wakePlan = pendingHeldVideoState.updateHeld(
                uploadId: uploadId,
                hold: hold,
                allowScheduling: !isShuttingDown
            ) {
                applyHeldVideoWakePlan(wakePlan)
                return true
            }

            guard let wakePlan = pendingHeldVideoState.moveQueuedToHeld(
                uploadId: uploadId,
                hold: hold,
                allowScheduling: !isShuttingDown
            ) else {
                return false
            }
            applyHeldVideoWakePlan(wakePlan)
            return true
        }

        return await releaseVideoHold(uploadId: uploadId)
    }

    package func videoJobState(uploadId: String) -> VideoJobState? {
        if activeJobs[uploadId] == .video {
            return .active
        }
        if let hold = pendingHeldVideoState.heldMetadata(uploadId: uploadId) {
            return .held(hold)
        }
        if pendingHeldVideoState.containsQueued(uploadId: uploadId) {
            return .queued
        }
        return nil
    }

    func processQueues(reconsiderHeldJobs: Bool = false) async {
        await processQueues(
            reconsiderHeldJobs: reconsiderHeldJobs,
            requestedPassKind: .genericRecompute
        )
    }

    private func processCompletionRefillQueues(reconsiderHeldJobs: Bool = false) async {
        await processQueues(
            reconsiderHeldJobs: reconsiderHeldJobs,
            requestedPassKind: .completionRefill
        )
    }

    private func processQueues(
        reconsiderHeldJobs: Bool,
        requestedPassKind: CARecomputeCoordinator.PassKind
    ) async {
        guard !isShuttingDown else { return }
        if reconsiderHeldJobs {
            _ = await reconsiderHeldVideoJobsForRecompute()
        } else {
            _ = releaseReadyHeldVideoJobs()
        }
        if !recomputeCoordinator.beginRecomputeRun(
            requestedPassKind: requestedPassKind,
            reconsiderHeldJobs: reconsiderHeldJobs
        ) {
            return
        }
        defer { recomputeCoordinator.finishRecomputeRun() }

        repeat {
            let pass = recomputeCoordinator.beginRecomputePass()
            await Task.yield()
            var attemptedIdleCapacityHeldReconsideration = false

            while imageQueueHead < imageQueue.count && activeImageCount < config.maxConcurrentImages {
                let job = imageQueue[imageQueueHead]
                imageQueueHead += 1

                guard activeJobs[job.uploadId] == nil else { continue }

                activeJobs[job.uploadId] = job.assetType
                activeImageCount += 1

                let task = Task {
                    await self.process(job: job)
                    await self.jobCompleted(uploadId: job.uploadId)
                }
                activeTasks[job.uploadId] = task
            }

            while true {
                let effectiveTotalVideoSlots = await currentVideoSlotCapacity()
                guard dispatchState.activeVideoCount < effectiveTotalVideoSlots else { break }

                let availableNowSlots = effectiveTotalVideoSlots - dispatchState.activeVideoCount

                let batch = await dequeueNextVideoJobs(
                    maxCount: availableNowSlots
                )
                if batch.jobs.isEmpty {
                    if batch.madeHoldDecisions {
                        continue
                    }
                    if pass.reconsiderHeldJobs,
                       !attemptedIdleCapacityHeldReconsideration,
                       await reconsiderHeldVideoJobsForIdleCapacity() {
                        attemptedIdleCapacityHeldReconsideration = true
                        continue
                    }
                    break
                }

                var launchedAny = false
                for selected in batch.jobs {
                    let job = selected.job
                    guard activeJobs[job.uploadId] == nil else { continue }

                    activeJobs[job.uploadId] = job.assetType
                    dispatchState.registerActiveVideo(job: job, routing: selected.routing)

                    let task = Task {
                        await self.process(job: job)
                        await self.jobCompleted(uploadId: job.uploadId)
                    }
                    activeTasks[job.uploadId] = task
                    launchedAny = true
                }

                if !launchedAny {
                    continue
                }
            }

            compactQueue(&imageQueue, head: &imageQueueHead)
            pendingHeldVideoState.compactQueuedIfNeeded(threshold: config.queueCompactionThreshold)

            if videoSelectionPolicy == .complexityAware, let thunderboltDispatcher {
                await thunderboltDispatcher.noteBaselineSnapshot()
            }
        } while recomputeCoordinator.requiresAnotherRecomputePass
    }

    private struct VideoDequeueBatch {
        let jobs: [VideoDequeueSelection]
        let madeHoldDecisions: Bool
    }

    private struct VideoDequeueSelection {
        let job: ProcessingJob
        let routing: VideoRoutingDirective
    }

    private func dequeueNextVideoJobs(
        maxCount: Int
    ) async -> VideoDequeueBatch {
        guard maxCount > 0 else {
            return VideoDequeueBatch(jobs: [], madeHoldDecisions: false)
        }

        switch videoSelectionPolicy {
        case .fifo:
            let availableRoutes = await fifoRoutingDirectives(limit: maxCount)
            guard !availableRoutes.isEmpty else {
                return VideoDequeueBatch(jobs: [], madeHoldDecisions: false)
            }

            var selected: [VideoDequeueSelection] = []
            selected.reserveCapacity(min(maxCount, availableRoutes.count))
            while selected.count < availableRoutes.count,
                  let job = pendingHeldVideoState.takeNextQueued() {
                selected.append(
                    VideoDequeueSelection(
                        job: job,
                        routing: availableRoutes[selected.count]
                    )
                )
            }
            return VideoDequeueBatch(jobs: selected, madeHoldDecisions: false)

        case .complexityAware:
            let pendingStart = pendingHeldVideoState.queuedStartIndex
            let pendingEnd = pendingHeldVideoState.queuedEndIndex
            guard pendingStart < pendingEnd else {
                return VideoDequeueBatch(jobs: [], madeHoldDecisions: false)
            }
            let nowMS = Double(Date().timeIntervalSince1970 * 1_000)
            guard let projectedSlotAssembly = await complexityAwareProjectedSlotSelection(nowMS: nowMS),
                  !projectedSlotAssembly.machineContexts.isEmpty else {
                return VideoDequeueBatch(jobs: [], madeHoldDecisions: false)
            }

            struct PendingEntry {
                let absoluteIndex: Int
                let job: ProcessingJob
                let caJob: CAJob
            }

            var pending: [PendingEntry] = []
            pending.reserveCapacity(pendingEnd - pendingStart)
            for absoluteIndex in pendingStart..<pendingEnd {
                guard let job = pendingHeldVideoState.queuedJob(atAbsoluteIndex: absoluteIndex) else {
                    continue
                }
                pending.append(
                    PendingEntry(
                        absoluteIndex: absoluteIndex,
                        job: job,
                        caJob: Self.makeComplexityAwareJob(job, enqueueOrder: absoluteIndex)
                    )
                )
            }
            guard !pending.isEmpty else {
                return VideoDequeueBatch(jobs: [], madeHoldDecisions: false)
            }

            var pendingByAbsoluteIndex: [Int: PendingEntry] = [:]
            pendingByAbsoluteIndex.reserveCapacity(pending.count)
            var projectedPendingJobs: [CAProjectedSlotSelection.PendingJob<Int>] = []
            projectedPendingJobs.reserveCapacity(pending.count)
            for pendingEntry in pending {
                pendingByAbsoluteIndex[pendingEntry.absoluteIndex] = pendingEntry
                projectedPendingJobs.append(
                    CAProjectedSlotSelection.PendingJob(
                        token: pendingEntry.absoluteIndex,
                        job: pendingEntry.caJob,
                        excludedRemoteSlot: dispatchState.excludedRemoteSlot(uploadId: pendingEntry.job.uploadId)
                    )
                )
            }

            let pickPlan = projectedSlotAssembly.plan(
                pendingJobs: projectedPendingJobs,
                nowMS: nowMS,
                maxCount: maxCount
            )
            guard pickPlan.hasSelections else {
                return VideoDequeueBatch(jobs: [], madeHoldDecisions: false)
            }

            for token in pickPlan.clearedExcludedTokens {
                guard let pendingEntry = pendingByAbsoluteIndex[token] else { continue }
                dispatchState.clearTransientRemoteExclusion(uploadId: pendingEntry.job.uploadId)
            }

            var selected: [VideoDequeueSelection] = []
            var held: [CAPendingHeldVideoState.HeldJob] = []
            var selectedAbsoluteIndices: [Int] = []
            selected.reserveCapacity(pickPlan.dispatches.count)
            held.reserveCapacity(pickPlan.holds.count)
            selectedAbsoluteIndices.reserveCapacity(pickPlan.consumedTokens.count)

            for token in pickPlan.consumedTokens {
                guard let pendingEntry = pendingByAbsoluteIndex[token] else { continue }
                selectedAbsoluteIndices.append(pendingEntry.absoluteIndex)
            }

            for dispatch in pickPlan.dispatches {
                guard let pendingEntry = pendingByAbsoluteIndex[dispatch.token] else { continue }
                selected.append(
                    VideoDequeueSelection(
                        job: pendingEntry.job,
                        routing: routingDirective(for: dispatch)
                    )
                )
            }

            for holdDecision in pickPlan.holds {
                guard let pendingEntry = pendingByAbsoluteIndex[holdDecision.token] else { continue }
                let wakeAt = Date(timeIntervalSince1970: holdDecision.wakeAtMS / 1_000)
                held.append(
                    CAPendingHeldVideoState.HeldJob(
                        job: pendingEntry.job,
                        hold: VideoHoldMetadata(
                            wakeAt: wakeAt,
                            targetSlotID: holdDecision.targetSlotID
                        )
                    )
                )
            }

            guard !selected.isEmpty || !held.isEmpty else {
                return VideoDequeueBatch(jobs: [], madeHoldDecisions: false)
            }

            pendingHeldVideoState.removeQueued(atAbsoluteIndicesDescending: selectedAbsoluteIndices.sorted(by: >))

            if let wakePlan = pendingHeldVideoState.storeHeldJobs(held, allowScheduling: !isShuttingDown) {
                applyHeldVideoWakePlan(wakePlan)
            }

            return VideoDequeueBatch(
                jobs: selected,
                madeHoldDecisions: !held.isEmpty
            )
        }
    }

    private enum ProbeOutcome {
        case probe(VideoRuntimeProbeResult?)
        case timeout
    }

    package func makeQueuedJob(
        uploadId: String,
        originalName: String,
        filePath: String,
        assetType: Asset.AssetType,
        arrivalAtSeconds: Double = Date().timeIntervalSince1970,
        isRepair: Bool = false,
        restoreStatus: Asset.AssetStatus? = nil
    ) async -> ProcessingJob {
        let resolvedVideoCost: CAResolvedVideoCost?
        if assetType == .video {
            let mimeType = Self.enqueueMIMEType(filePath: filePath, originalName: originalName)
            resolvedVideoCost = await Self.resolveVideoCostForQueue(
                filePath: filePath,
                mimeType: mimeType,
                localMSPerFrameC1: localComplexityAwareProfile?.msPerFrameC1,
                localFixedOverheadMS: localComplexityAwareProfile?.fixedOverheadMS ?? 0
            )
        } else {
            resolvedVideoCost = nil
        }

        return ProcessingJob(
            uploadId: uploadId,
            originalName: originalName,
            filePath: filePath,
            assetType: assetType,
            arrivalAtSeconds: arrivalAtSeconds,
            resolvedVideoCost: resolvedVideoCost,
            isRepair: isRepair,
            restoreStatus: restoreStatus
        )
    }

    package static func resolveVideoCostForQueue(
        filePath: String,
        mimeType: String?,
        localMSPerFrameC1: Double?,
        localFixedOverheadMS: Double = 0
    ) async -> CAResolvedVideoCost {
        let probeTimeoutNanos = metadataProbeTimeoutOverrideNanos ?? Self.metadataProbeTimeoutNanos
        let outcome = await withTaskGroup(of: ProbeOutcome.self, returning: ProbeOutcome.self) { group in
            group.addTask {
                .probe(await VideoProcessor.probeRuntimeEstimate(sourcePath: filePath, mimeType: mimeType))
            }
            group.addTask {
                try? await Task.sleep(nanoseconds: probeTimeoutNanos)
                return .timeout
            }
            let first = await group.next() ?? .timeout
            group.cancelAll()
            return first
        }

        switch outcome {
        case .probe(let probe):
            let resolvedRuntimeSeconds: Double?
            let confidence: EstimateConfidence
            if let probe,
               probe.runtimeEstimateSeconds.isFinite,
               probe.runtimeEstimateSeconds > 0 {
                resolvedRuntimeSeconds = probe.runtimeEstimateSeconds
                confidence = .high
            } else {
                resolvedRuntimeSeconds = nil
                confidence = .low
            }
            return CAProfileAndFallbackMath.resolveVideoCost(
                frameCount: probe?.frameCount,
                durationSeconds: probe?.durationSeconds,
                runtimeSeconds: resolvedRuntimeSeconds,
                confidence: confidence,
                runtimeSourceWhenPresent: .probeEstimate,
                localMSPerFrameC1: localMSPerFrameC1,
                localFixedOverheadMS: localFixedOverheadMS
            )
        case .timeout:
            return CAProfileAndFallbackMath.resolveVideoCost(
                frameCount: nil,
                durationSeconds: nil,
                runtimeSeconds: nil,
                confidence: .low,
                runtimeSourceWhenPresent: .estimatedProcessingRuntime,
                localMSPerFrameC1: localMSPerFrameC1,
                localFixedOverheadMS: localFixedOverheadMS
            )
        }
    }

    package static func enqueueMIMEType(filePath: String, originalName: String) -> String? {
        if URL(fileURLWithPath: filePath).pathExtension.isEmpty {
            let ext = (originalName as NSString).pathExtension.lowercased()
            return ext.isEmpty ? nil : UTType(filenameExtension: ext)?.preferredMIMEType
        }
        return nil
    }

    private func runtimeEstimateForActiveVideoJob(_ job: ProcessingJob) -> Double {
        guard let videoCost = job.resolvedVideoCost else {
            return 0.001
        }

        if videoCost.derivation.frameCountSource != .defaultFallback,
           let runtimeSeconds = CAProfileAndFallbackMath.runtimeSeconds(
                frameCount: videoCost.frameCount,
                localMSPerFrameC1: localLiveMSPerFrameC1EMA ?? localComplexityAwareProfile?.msPerFrameC1,
                localFixedOverheadMS: localComplexityAwareProfile?.fixedOverheadMS ?? 0,
                degradationFactor: localProjectedDegradationFactor()
           ) {
            return runtimeSeconds
        }

        if videoCost.runtimeSeconds.isFinite {
            return max(0.001, videoCost.runtimeSeconds)
        }
        return 0.001
    }

    private func localProjectedDegradationFactor() -> Double {
        guard let localProfile = localComplexityAwareProfile else {
            return 1.0
        }
        return CAProfileAndFallbackMath.resolvedDegradation(
            from: localProfile.degradationCurve,
            concurrency: dispatchState.currentLocalActiveVideoCount()
        ).factor
    }

    func markLocalVideoRuntimeStart(uploadId: String) {
        dispatchState.markLocalVideoRuntimeStart(uploadId: uploadId)
    }

    package static func makeComplexityAwareJob(_ job: ProcessingJob, enqueueOrder: Int) -> CAJob {
        guard let resolvedFrameCount = job.resolvedVideoCost?.frameCount,
              resolvedFrameCount.isFinite,
              resolvedFrameCount > 0 else {
            preconditionFailure("Missing resolved video cost for complexity-aware job \(job.uploadId)")
        }
        let rawArrivalMS = job.arrivalAtSeconds * 1_000
        let arrivalMS = (rawArrivalMS.isFinite && rawArrivalMS >= 0) ? rawArrivalMS : 0
        return CAJob(
            id: job.uploadId,
            arrivalAtMS: arrivalMS,
            enqueueOrder: enqueueOrder,
            frameCount: resolvedFrameCount
        )
    }

    package static func dispatchFrameCount(for job: ProcessingJob) -> Double? {
        job.resolvedVideoCost?.frameCount
    }

    package func processingJobSnapshot(uploadId: String) -> ProcessingJob? {
        if let job = pendingHeldVideoState.job(uploadId: uploadId) {
            return job
        }
        return dispatchState.activeJob(uploadId: uploadId)
    }

    package func localLiveMSPerFrameC1Estimate() -> Double? {
        let estimate = localLiveMSPerFrameC1EMA ?? localComplexityAwareProfile?.msPerFrameC1
        guard let estimate, estimate.isFinite, estimate > 0 else { return nil }
        return estimate
    }

    func updateLocalLiveMSPerFrame(processNanos: UInt64, frameCount: Double) {
        guard frameCount.isFinite, frameCount > 0 else { return }
        let localConcurrency = max(1, dispatchState.currentLocalActiveVideoCount())
        let sampleModel = CASuccessfulExecutionSampleModel(
            msPerFrameC1: localComplexityAwareProfile?.msPerFrameC1 ?? 1.0,
            fixedOverheadMS: localComplexityAwareProfile?.fixedOverheadMS ?? 0,
            degradationCurve: localComplexityAwareProfile?.degradationCurve
                ?? [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)]
        )
        guard let actualMSPerFrame = ThunderboltAdaptiveTelemetryReducer.normalizedMSPerFrameC1(
            processNanos: processNanos,
            frameCount: frameCount,
            model: sampleModel,
            concurrency: localConcurrency
        ) else {
            return
        }

        guard let update = LiveAdaptiveMSPerFrameC1Estimator.next(
            previousEstimate: localLiveMSPerFrameC1EMA,
            previousSmoothedError: localLiveMSPerFrameC1ErrorEMA,
            previousSmoothedAbsoluteError: localLiveMSPerFrameC1AbsErrorEMA,
            initialEstimate: sampleModel.msPerFrameC1,
            observed: actualMSPerFrame
        ) else {
            return
        }
        localLiveMSPerFrameC1EMA = update.estimate
        localLiveMSPerFrameC1ErrorEMA = update.smoothedError
        localLiveMSPerFrameC1AbsErrorEMA = update.smoothedAbsoluteError
    }

    private func localRemainingRuntimeSnapshotMS(nowNanos: UInt64) -> [Double] {
        dispatchState.localRemainingRuntimeSnapshotMS(nowNanos: nowNanos) { [self] job in
            runtimeEstimateForActiveVideoJob(job)
        }
    }

    private func fifoRoutingDirectives(limit: Int) async -> [VideoRoutingDirective] {
        guard limit > 0 else { return [] }
        var directives: [VideoRoutingDirective] = []
        directives.reserveCapacity(limit)

        if let thunderboltDispatcher {
            let remoteSnapshot = await thunderboltDispatcher.complexityAwareSnapshot()
            for worker in remoteSnapshot {
                for slot in worker.slots where !slot.isBusy && !slot.isDown {
                    directives.append(
                        .remote(
                            workerIndex: worker.workerIndex,
                            slotIndex: slot.slotIndex,
                            slotID: slot.id
                        )
                    )
                    if directives.count == limit {
                        return directives
                    }
                }
            }
        } else if remoteVideoDispatchOverride != nil {
            for (workerIndex, worker) in config.thunderboltWorkers.enumerated() {
                for slotIndex in 0..<worker.slots {
                    directives.append(
                        .remote(
                            workerIndex: workerIndex,
                            slotIndex: slotIndex,
                            slotID: "\(worker.host)#s\(slotIndex + 1)"
                        )
                    )
                    if directives.count == limit {
                        return directives
                    }
                }
            }
        }

        let localAvailable = max(0, config.maxConcurrentVideos - dispatchState.currentLocalActiveVideoCount())
        for localSlotIndex in 0..<localAvailable {
            directives.append(.local(localSlotIndex: localSlotIndex))
            if directives.count == limit {
                break
            }
        }
        return directives
    }

    private func complexityAwareProjectedSlotSelection(nowMS: Double) async -> CAProjectedSlotSelection.Assembly? {
        guard let localProfile = localComplexityAwareProfile else {
            return nil
        }

        let nowNanos = DispatchTime.now().uptimeNanoseconds
        let localRemainingMS = localRemainingRuntimeSnapshotMS(nowNanos: nowNanos)
        let sharedBuild = await productionTopologyModelBuild(localProfile: localProfile)
        return CAProjectedSlotSelection.assemble(
            nowMS: nowMS,
            localRemainingMS: localRemainingMS,
            topology: sharedBuild.buildResult,
            remoteWorkers: sharedBuild.remoteSnapshot
        )
    }

    private func currentVideoSlotCapacity() async -> Int {
        guard videoSelectionPolicy == .complexityAware else {
            return configuredVideoSlots
        }
        guard localComplexityAwareProfile != nil else {
            return config.maxConcurrentVideos
        }
        guard let localProfile = localComplexityAwareProfile else {
            return config.maxConcurrentVideos
        }
        let sharedBuild = await productionTopologyModelBuild(localProfile: localProfile)
        return sharedBuild.buildResult.diagnostics.totalExecutableSlotCount
    }

    package static func buildProductionTopologyModel(
        localSlotCount: Int,
        localProfile: CAValidatedPriorProfile,
        localLiveMSPerFrameC1: Double? = nil,
        priorTable: BenchmarkPriorTable,
        remoteSnapshot: [ThunderboltDispatcher.CAWorkerSnapshot]
    ) -> CATopologyModelBuildResult {
        CATopologyModelBuilder.build(
            mode: .auto,
            localSlotCount: localSlotCount,
            localProfile: CATopologyModelLocalProfile(
                machineID: "local",
                msPerFrameC1: localLiveMSPerFrameC1 ?? localProfile.msPerFrameC1,
                fixedOverheadMS: localProfile.fixedOverheadMS,
                degradationCurve: localProfile.degradationCurve,
                msSource: "prior(local)",
                curveSource: "prior(local)"
            ),
            priorTable: priorTable,
            remoteWorkers: remoteSnapshot.map(\.sharedTopologyModelRemoteWorkerInput)
        )
    }

    private func productionTopologyModelBuild(
        localProfile: CAValidatedPriorProfile
    ) async -> (buildResult: CATopologyModelBuildResult, remoteSnapshot: [ThunderboltDispatcher.CAWorkerSnapshot]) {
        guard let thunderboltDispatcher else {
            return (
                Self.buildProductionTopologyModel(
                    localSlotCount: config.maxConcurrentVideos,
                    localProfile: localProfile,
                    localLiveMSPerFrameC1: localLiveMSPerFrameC1EMA,
                    priorTable: BenchmarkPriorTable(),
                    remoteSnapshot: []
                ),
                []
            )
        }

        let priorTable = await thunderboltDispatcher.benchmarkPriorSnapshot()
        let remoteSnapshot = await thunderboltDispatcher.complexityAwareSnapshot()
        let buildResult = Self.buildProductionTopologyModel(
            localSlotCount: config.maxConcurrentVideos,
            localProfile: localProfile,
            localLiveMSPerFrameC1: localLiveMSPerFrameC1EMA,
            priorTable: priorTable,
            remoteSnapshot: remoteSnapshot
        )
        return (buildResult, remoteSnapshot)
    }

    package func remoteExecutionSampleModelForDispatch(
        workerIndex: Int
    ) async -> CASuccessfulExecutionSampleModel? {
        guard let localProfile = localComplexityAwareProfile else {
            return nil
        }
        let sharedBuild = await productionTopologyModelBuild(localProfile: localProfile)
        guard sharedBuild.remoteSnapshot.indices.contains(workerIndex) else {
            return nil
        }
        let workerHost = sharedBuild.remoteSnapshot[workerIndex].host
        guard let machineIndex = sharedBuild.buildResult.machineIndexByHost[workerHost],
              sharedBuild.buildResult.machineProfiles.indices.contains(machineIndex) else {
            return nil
        }
        let machineProfile = sharedBuild.buildResult.machineProfiles[machineIndex]
        return CASuccessfulExecutionSampleModel(
            msPerFrameC1: machineProfile.msPerFrameC1,
            fixedOverheadMS: machineProfile.fixedOverheadMS,
            degradationCurve: machineProfile.degradationCurve
        )
    }

    func selectedVideoRoutingDirective(for uploadId: String) -> VideoRoutingDirective {
        dispatchState.consumeRoutingDirective(for: uploadId)
    }

    func recordTransientRemoteExclusion(uploadId: String, workerIndex: Int, slotIndex: Int) {
        dispatchState.recordTransientRemoteExclusion(
            uploadId: uploadId,
            workerIndex: workerIndex,
            slotIndex: slotIndex
        )
    }

    func clearTransientRemoteExclusion(uploadId: String) {
        dispatchState.clearTransientRemoteExclusion(uploadId: uploadId)
    }

    func beginRemoteVideoDispatch(uploadId: String) {
        dispatchState.beginRemoteVideoDispatch(uploadId: uploadId)
    }

    func endRemoteVideoDispatch(uploadId: String) {
        dispatchState.endRemoteVideoDispatch(uploadId: uploadId)
    }

    func applyRemoteDispatchOutcome(
        uploadId: String,
        routingDirective: VideoRoutingDirective,
        outcome: CADispatchState.RemoteDispatchOutcome
    ) {
        dispatchState.applyRemoteDispatchOutcome(
            uploadId: uploadId,
            routingDirective: routingDirective,
            outcome: outcome,
            allowTransientRemoteExclusion: videoSelectionPolicy == .complexityAware
        )
    }

    private func routingDirective(
        for dispatch: CAProjectedSlotSelection.DispatchPick<Int>
    ) -> VideoRoutingDirective {
        switch dispatch.route {
        case .local:
            return .local(localSlotIndex: dispatch.slotIndex)
        case .remote(let workerIndex):
            return .remote(
                workerIndex: workerIndex,
                slotIndex: dispatch.slotIndex,
                slotID: dispatch.slotID
            )
        }
    }

    private struct HoldTargetState {
        let readyAtMS: Double
        let isDown: Bool
        let remoteSlotKey: CARemoteSlotKey?
    }

    private func compactQueue(_ queue: inout [ProcessingJob], head: inout Int) {
        if head > config.queueCompactionThreshold && head > queue.count / 2 {
            queue.removeFirst(head)
            head = 0
        }
    }

    private func releaseVideoHold(uploadId: String) async -> Bool {
        guard let wakePlan = pendingHeldVideoState.releaseHeld(
            uploadId: uploadId,
            allowScheduling: !isShuttingDown
        ) else {
            return false
        }
        applyHeldVideoWakePlan(wakePlan)
        await processQueues()
        return true
    }

    @discardableResult
    private func releaseReadyHeldVideoJobs(now: Date = Date()) -> Bool {
        let result = pendingHeldVideoState.releaseReadyHeldJobs(
            now: now,
            allowScheduling: !isShuttingDown
        )
        if let wakePlan = result.wakePlan {
            applyHeldVideoWakePlan(wakePlan)
        }
        return result.releasedAny
    }

    @discardableResult
    private func reconsiderHeldVideoJobsForRecompute(now: Date = Date()) async -> Bool {
        guard pendingHeldVideoState.hasHeldJobs else {
            return false
        }

        guard videoSelectionPolicy == .complexityAware else {
            return releaseHeldVideoJobsForRecompute()
        }

        let nowMS = Double(now.timeIntervalSince1970 * 1_000)
        guard let assembly = await complexityAwareProjectedSlotSelection(nowMS: nowMS),
              !assembly.machineContexts.isEmpty else {
            return releaseHeldVideoJobsForRecompute()
        }

        let heldEntries = pendingHeldVideoState.heldEntries()
        guard !heldEntries.isEmpty else {
            return false
        }

        var invalidatedUploadIDs: [String] = []
        invalidatedUploadIDs.reserveCapacity(heldEntries.count)

        for heldEntry in heldEntries {
            if heldEntry.hold.wakeAt <= now {
                invalidatedUploadIDs.append(heldEntry.uploadId)
                continue
            }

            let targetState = holdTargetState(
                forTargetSlotID: heldEntry.hold.targetSlotID,
                in: assembly
            )
            let targetStillPossible = holdTargetStillPossible(
                uploadId: heldEntry.uploadId,
                targetState: targetState
            )
            let baselineReadyAtMS = heldEntry.hold.wakeAt.timeIntervalSince1970 * 1_000

            if CAHoldInvalidation.invalidationReason(
                baselineReadyAtMS: baselineReadyAtMS,
                currentReadyAtMS: targetState?.readyAtMS,
                slotIsDown: targetState?.isDown ?? false,
                targetStillPossible: targetStillPossible
            ) != nil {
                invalidatedUploadIDs.append(heldEntry.uploadId)
            }
        }

        guard !invalidatedUploadIDs.isEmpty else {
            return false
        }

        let result = pendingHeldVideoState.releaseHeldJobs(
            uploadIDs: invalidatedUploadIDs,
            allowScheduling: !isShuttingDown
        )
        if let wakePlan = result.wakePlan {
            applyHeldVideoWakePlan(wakePlan)
        }
        return result.releasedAny
    }

    @discardableResult
    private func releaseHeldVideoJobsForRecompute() -> Bool {
        let result = pendingHeldVideoState.releaseHeldJobsForRecompute(
            allowScheduling: !isShuttingDown
        )
        if let wakePlan = result.wakePlan {
            applyHeldVideoWakePlan(wakePlan)
        }
        return result.releasedAny
    }

    @discardableResult
    private func reconsiderHeldVideoJobsForIdleCapacity(now: Date = Date()) async -> Bool {
        guard videoSelectionPolicy == .complexityAware,
              pendingHeldVideoState.hasHeldJobs,
              !pendingHeldVideoState.hasQueuedJobs else {
            return false
        }

        let nowMS = Double(now.timeIntervalSince1970 * 1_000)
        guard let assembly = await complexityAwareProjectedSlotSelection(nowMS: nowMS),
              assembly.machineContexts.contains(where: { machineContext in
                  machineContext.slots.contains { slot in
                      !slot.isDown && slot.readyAtMS <= nowMS
                  }
              }) else {
            return false
        }

        return releaseHeldVideoJobsForRecompute()
    }

    private func holdTargetState(
        forTargetSlotID targetSlotID: String,
        in assembly: CAProjectedSlotSelection.Assembly
    ) -> HoldTargetState? {
        for machineContext in assembly.machineContexts {
            for slotIndex in machineContext.slots.indices {
                let slot = machineContext.slots[slotIndex]
                guard slot.id == targetSlotID else { continue }
                return HoldTargetState(
                    readyAtMS: slot.readyAtMS,
                    isDown: slot.isDown,
                    remoteSlotKey: machineContext.remoteSlotKey(slotIndex: slotIndex)
                )
            }
        }
        return nil
    }

    private func holdTargetStillPossible(
        uploadId: String,
        targetState: HoldTargetState?
    ) -> Bool {
        guard let targetState else {
            return false
        }
        guard let remoteSlotKey = targetState.remoteSlotKey else {
            return true
        }
        return dispatchState.excludedRemoteSlot(uploadId: uploadId) != remoteSlotKey
    }

    private func applyHeldVideoWakePlan(_ wakePlan: CAPendingHeldVideoState.WakePlan) {
        heldVideoWakeTask?.cancel()
        heldVideoWakeTask = nil

        guard !isShuttingDown,
              let nextWakeAt = wakePlan.wakeAt else {
            return
        }

        let sleepSeconds = max(0, nextWakeAt.timeIntervalSinceNow)
        let sleepNanos = UInt64(sleepSeconds * 1_000_000_000)
        heldVideoWakeTask = Task { [self] in
            if sleepNanos > 0 {
                try? await Task.sleep(nanoseconds: sleepNanos)
            }
            guard !Task.isCancelled else { return }
            await self.handleHeldVideoWake(token: wakePlan.token)
        }
    }

    private func handleHeldVideoWake(token: UInt64) async {
        heldVideoWakeTask = nil
        guard !isShuttingDown else { return }
        let result = pendingHeldVideoState.handleWake(
            token: token,
            now: Date(),
            allowScheduling: true
        )
        guard result.isCurrentToken else { return }
        if let wakePlan = result.wakePlan {
            applyHeldVideoWakePlan(wakePlan)
        }
        guard result.releasedAny else { return }
        await processQueues()
    }

    private func jobCompleted(uploadId: String) async {
        activeTasks.removeValue(forKey: uploadId)
        var freedVideoCapacity = false

        if let type = activeJobs.removeValue(forKey: uploadId) {
            switch type {
            case .image:
                activeImageCount = max(0, activeImageCount - 1)
            case .video:
                dispatchState.completeActiveVideo(uploadId: uploadId)
                freedVideoCapacity = true
            }
        }

        if let deferredJob = dispatchState.takeDeferredTransientRequeue(uploadId: uploadId) {
            if isShuttingDown {
                Logger.kiko.info(
                    "Skipping deferred transient requeue for \(uploadId, privacy: .public): media processor is shutting down"
                )
            } else {
                enqueueJob(deferredJob)
                Logger.kiko.warning(
                    "Re-queued \(uploadId, privacy: .public) after transient remote dispatch failure"
                )
            }
        }

        guard freedVideoCapacity else {
            await processQueues()
            return
        }

        signalRecompute(.finish)
        await processCompletionRefillQueues(reconsiderHeldJobs: true)
    }

    package func requestRecomputeFromDispatcher(trigger: ThunderboltDispatcher.RecomputeTrigger) async {
        let directTrigger = recomputeCoordinator.intakeDispatcherTrigger(
            trigger,
            allowSlotDownBatchCoalescing: videoSelectionPolicy == .complexityAware || pendingHeldVideoState.hasHeldJobs,
            allowScheduling: !isShuttingDown,
            flushSlotDownBatch: { [self] in
                await self.flushSlotDownBatchRecompute()
            }
        )
        guard let directTrigger else {
            return
        }
        signalRecompute(directTrigger)
        let requestedPassKind: CARecomputeCoordinator.PassKind =
            directTrigger == .finish ? .completionRefill : .genericRecompute
        await processQueues(
            reconsiderHeldJobs: true,
            requestedPassKind: requestedPassKind
        )
    }

    private func flushSlotDownBatchRecompute() async {
        guard recomputeCoordinator.beginSlotDownBatchFlush(allowScheduling: !isShuttingDown) else {
            return
        }
        defer { recomputeCoordinator.finishSlotDownBatchFlush() }
        signalRecompute(.slotDownBatch)
        await processQueues(
            reconsiderHeldJobs: true,
            requestedPassKind: .genericRecompute
        )
    }

    package func setRecomputeSignal(_ signal: @escaping CARecomputeCoordinator.Signal) {
        recomputeSignal = signal
    }

    package func schedulingMetricsSnapshot() -> SchedulingMetricsSummary? {
        guard videoSelectionPolicy == .complexityAware else { return nil }
        return SchedulingMetricMath.compute(successfulJobs: schedulingSuccessfulJobs, failedCount: schedulingFailedIDs.count)
    }

    private func signalRecompute(_ trigger: ThunderboltDispatcher.RecomputeTrigger) {
        guard videoSelectionPolicy == .complexityAware else { return }
        recomputeSignal?(trigger)
    }

    func recordSchedulingSuccessIfNeeded(uploadId: String, asset: Asset?) {
        guard videoSelectionPolicy == .complexityAware else { return }
        guard !schedulingSuccessfulIDs.contains(uploadId) else { return }
        guard let asset,
              asset.type == .video,
              let completedAt = asset.completedAt else {
            return
        }

        schedulingVideoIDs.insert(uploadId)
        schedulingSuccessfulIDs.insert(uploadId)
        schedulingFailedIDs.remove(uploadId)
        schedulingSuccessfulJobs.append(
            SchedulingSuccessfulJob(
                arriveAtSeconds: asset.createdAt.timeIntervalSince1970,
                liveAtSeconds: completedAt.timeIntervalSince1970
            )
        )
    }

    func recordStatusTransition(id: String, status: Asset.AssetStatus) {
        guard videoSelectionPolicy == .complexityAware else { return }
        guard schedulingVideoIDs.contains(id) else { return }
        if status == .failed {
            schedulingFailedIDs.insert(id)
            return
        }
        schedulingFailedIDs.remove(id)
    }

    private func logSchedulingMetrics(reason: String) {
        guard let metrics = schedulingMetricsSnapshot() else { return }
        Logger.kiko.info(
            """
            Scheduling metrics (\(reason, privacy: .public)): \
            sumW=\(metrics.sumWSeconds, privacy: .public)s \
            p95=\(metrics.p95Seconds, privacy: .public)s \
            makespan=\(metrics.makespanSeconds, privacy: .public)s \
            failed_count=\(metrics.failedCount, privacy: .public)
            """
        )
    }
}

package struct ProcessingJob: Sendable {
    package let uploadId: String
    package let originalName: String
    package let filePath: String
    package let assetType: Asset.AssetType
    package let arrivalAtSeconds: Double
    package let resolvedVideoCost: CAResolvedVideoCost?
    package let isRepair: Bool
    package let restoreStatus: Asset.AssetStatus?

    package init(
        uploadId: String,
        originalName: String,
        filePath: String,
        assetType: Asset.AssetType,
        arrivalAtSeconds: Double = Date().timeIntervalSince1970,
        resolvedVideoCost: CAResolvedVideoCost? = nil,
        estimatedVideoRuntimeSeconds: Double? = nil,
        frameCount: Double? = nil,
        probedDurationSeconds: Double? = nil,
        videoEstimateConfidence: EstimateConfidence = .low,
        isRepair: Bool = false,
        restoreStatus: Asset.AssetStatus? = nil
    ) {
        self.uploadId = uploadId
        self.originalName = originalName
        self.filePath = filePath
        self.assetType = assetType
        self.arrivalAtSeconds = arrivalAtSeconds
        self.resolvedVideoCost = if assetType == .video {
            resolvedVideoCost ?? CAProfileAndFallbackMath.resolveVideoCost(
                frameCount: frameCount,
                durationSeconds: probedDurationSeconds,
                runtimeSeconds: estimatedVideoRuntimeSeconds,
                confidence: videoEstimateConfidence,
                runtimeSourceWhenPresent: .estimatedProcessingRuntime,
                localMSPerFrameC1: nil
            )
        } else {
            nil
        }
        self.isRepair = isRepair
        self.restoreStatus = restoreStatus
    }
}
