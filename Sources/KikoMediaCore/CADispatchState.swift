import Foundation

package struct CADispatchState {
    package struct ActiveVideoRuntimeState: Sendable {
        package var startedAtNanos: UInt64?
        package let job: ProcessingJob

        package init(startedAtNanos: UInt64?, job: ProcessingJob) {
            self.startedAtNanos = startedAtNanos
            self.job = job
        }
    }

    package enum RemoteDispatchOutcome: Sendable {
        case success
        case transientRetry(slotHealthDown: Bool)
        case permanentFailure
        case fallbackLocal
        case unavailable
    }

    private var activeVideoRuntimeByUploadID: [String: ActiveVideoRuntimeState] = [:]
    private var remoteVideoInFlightIDs: Set<String> = []
    private var deferredTransientRequeuesByUploadID: [String: ProcessingJob] = [:]
    private var selectedVideoRoutingByUploadID: [String: MediaProcessor.VideoRoutingDirective] = [:]
    private var excludedRemoteSlotByUploadID: [String: CARemoteSlotKey] = [:]

    package init() {}

    package var activeVideoCount: Int {
        activeVideoRuntimeByUploadID.count
    }

    package mutating func registerActiveVideo(
        job: ProcessingJob,
        routing: MediaProcessor.VideoRoutingDirective
    ) {
        selectedVideoRoutingByUploadID[job.uploadId] = routing
        activeVideoRuntimeByUploadID[job.uploadId] = ActiveVideoRuntimeState(
            startedAtNanos: nil,
            job: job
        )
    }

    package mutating func consumeRoutingDirective(
        for uploadId: String
    ) -> MediaProcessor.VideoRoutingDirective {
        if let directive = selectedVideoRoutingByUploadID.removeValue(forKey: uploadId) {
            return directive
        }
        return .local(localSlotIndex: 0)
    }

    package mutating func markLocalVideoRuntimeStart(uploadId: String) {
        guard var state = activeVideoRuntimeByUploadID[uploadId],
              state.startedAtNanos == nil else {
            return
        }
        state.startedAtNanos = DispatchTime.now().uptimeNanoseconds
        activeVideoRuntimeByUploadID[uploadId] = state
    }

    package mutating func beginRemoteVideoDispatch(uploadId: String) {
        remoteVideoInFlightIDs.insert(uploadId)
    }

    package mutating func endRemoteVideoDispatch(uploadId: String) {
        remoteVideoInFlightIDs.remove(uploadId)
    }

    package mutating func applyRemoteDispatchOutcome(
        uploadId: String,
        routingDirective: MediaProcessor.VideoRoutingDirective,
        outcome: RemoteDispatchOutcome,
        allowTransientRemoteExclusion: Bool
    ) {
        switch outcome {
        case .success, .permanentFailure, .fallbackLocal, .unavailable:
            clearTransientRemoteExclusion(uploadId: uploadId)
        case .transientRetry(let slotHealthDown):
            guard allowTransientRemoteExclusion,
                  !slotHealthDown,
                  case .remote(let workerIndex, let slotIndex, _) = routingDirective else {
                clearTransientRemoteExclusion(uploadId: uploadId)
                return
            }
            recordTransientRemoteExclusion(
                uploadId: uploadId,
                workerIndex: workerIndex,
                slotIndex: slotIndex
            )
        }
    }

    package mutating func deferTransientRequeue(_ job: ProcessingJob) {
        deferredTransientRequeuesByUploadID[job.uploadId] = job
    }

    package mutating func takeDeferredTransientRequeue(uploadId: String) -> ProcessingJob? {
        deferredTransientRequeuesByUploadID.removeValue(forKey: uploadId)
    }

    package mutating func clearShutdownPendingState() {
        deferredTransientRequeuesByUploadID.removeAll(keepingCapacity: false)
        selectedVideoRoutingByUploadID.removeAll(keepingCapacity: false)
        excludedRemoteSlotByUploadID.removeAll(keepingCapacity: false)
    }

    package func excludedRemoteSlot(uploadId: String) -> CARemoteSlotKey? {
        excludedRemoteSlotByUploadID[uploadId]
    }

    package mutating func recordTransientRemoteExclusion(
        uploadId: String,
        workerIndex: Int,
        slotIndex: Int
    ) {
        excludedRemoteSlotByUploadID[uploadId] = CARemoteSlotKey(
            workerIndex: workerIndex,
            slotIndex: slotIndex
        )
    }

    package mutating func clearTransientRemoteExclusion(uploadId: String) {
        excludedRemoteSlotByUploadID.removeValue(forKey: uploadId)
    }

    package func currentLocalActiveVideoCount() -> Int {
        activeVideoRuntimeByUploadID.keys.reduce(into: 0) { count, uploadId in
            if isActiveLocalVideo(uploadId: uploadId) {
                count += 1
            }
        }
    }

    package func activeJob(uploadId: String) -> ProcessingJob? {
        activeVideoRuntimeByUploadID[uploadId]?.job
    }

    package func localRemainingRuntimeSnapshotMS(
        nowNanos: UInt64,
        runtimeEstimate: (ProcessingJob) -> Double
    ) -> [Double] {
        activeVideoRuntimeByUploadID.compactMap { uploadId, state -> Double? in
            guard isActiveLocalVideo(uploadId: uploadId) else { return nil }
            let estimatedRuntimeSeconds = runtimeEstimate(state.job)
            guard estimatedRuntimeSeconds.isFinite else { return nil }
            let elapsedSeconds: Double
            if let startedAtNanos = state.startedAtNanos,
               nowNanos >= startedAtNanos {
                elapsedSeconds = Double(nowNanos - startedAtNanos) / 1_000_000_000
            } else {
                elapsedSeconds = 0
            }
            return max(0, estimatedRuntimeSeconds - elapsedSeconds) * 1_000
        }
        .sorted()
    }

    package mutating func completeActiveVideo(uploadId: String) {
        selectedVideoRoutingByUploadID.removeValue(forKey: uploadId)
        activeVideoRuntimeByUploadID.removeValue(forKey: uploadId)
    }

    private func isActiveLocalVideo(uploadId: String) -> Bool {
        if case .remote = selectedVideoRoutingByUploadID[uploadId] {
            return false
        }
        return !remoteVideoInFlightIDs.contains(uploadId)
    }
}
