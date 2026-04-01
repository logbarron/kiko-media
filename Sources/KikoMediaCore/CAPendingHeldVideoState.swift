import Foundation

package struct CAPendingHeldVideoState {
    package struct HeldJob: Sendable {
        package let job: ProcessingJob
        package var hold: MediaProcessor.VideoHoldMetadata

        package init(job: ProcessingJob, hold: MediaProcessor.VideoHoldMetadata) {
            self.job = job
            self.hold = hold
        }
    }

    package struct HeldEntry: Sendable {
        package let uploadId: String
        package let job: ProcessingJob
        package let hold: MediaProcessor.VideoHoldMetadata

        package init(
            uploadId: String,
            job: ProcessingJob,
            hold: MediaProcessor.VideoHoldMetadata
        ) {
            self.uploadId = uploadId
            self.job = job
            self.hold = hold
        }
    }

    package struct WakePlan: Sendable {
        package let token: UInt64
        package let wakeAt: Date?

        package init(token: UInt64, wakeAt: Date?) {
            self.token = token
            self.wakeAt = wakeAt
        }
    }

    package struct ReleaseResult: Sendable {
        package let releasedAny: Bool
        package let wakePlan: WakePlan?

        package init(releasedAny: Bool, wakePlan: WakePlan?) {
            self.releasedAny = releasedAny
            self.wakePlan = wakePlan
        }
    }

    package struct WakeHandlingResult: Sendable {
        package let isCurrentToken: Bool
        package let releasedAny: Bool
        package let wakePlan: WakePlan?

        package init(isCurrentToken: Bool, releasedAny: Bool, wakePlan: WakePlan?) {
            self.isCurrentToken = isCurrentToken
            self.releasedAny = releasedAny
            self.wakePlan = wakePlan
        }
    }

    private var queuedJobs: [ProcessingJob] = []
    private var queueHead = 0
    private var heldJobsByUploadID: [String: HeldJob] = [:]
    private var heldWakeToken: UInt64 = 0

    package init() {}

    package var pendingCount: Int {
        max(0, queuedJobs.count - queueHead) + heldJobsByUploadID.count
    }

    package var hasHeldJobs: Bool {
        !heldJobsByUploadID.isEmpty
    }

    package var hasQueuedJobs: Bool {
        queueHead < queuedJobs.count
    }

    package var queuedStartIndex: Int {
        queueHead
    }

    package var queuedEndIndex: Int {
        queuedJobs.endIndex
    }

    package mutating func enqueue(_ job: ProcessingJob) {
        queuedJobs.append(job)
    }

    package mutating func clearAll() -> WakePlan {
        queuedJobs.removeAll(keepingCapacity: false)
        queueHead = 0
        heldJobsByUploadID.removeAll(keepingCapacity: false)
        return nextWakePlan(allowScheduling: false)
    }

    package func containsQueued(uploadId: String) -> Bool {
        guard queueHead < queuedJobs.count else { return false }
        return queuedJobs[queueHead...].contains(where: { $0.uploadId == uploadId })
    }

    package func heldMetadata(uploadId: String) -> MediaProcessor.VideoHoldMetadata? {
        heldJobsByUploadID[uploadId]?.hold
    }

    package func heldEntries() -> [HeldEntry] {
        heldJobsByUploadID.map { uploadId, heldJob in
            HeldEntry(
                uploadId: uploadId,
                job: heldJob.job,
                hold: heldJob.hold
            )
        }
    }

    package func queuedJob(atAbsoluteIndex absoluteIndex: Int) -> ProcessingJob? {
        guard absoluteIndex >= queueHead, queuedJobs.indices.contains(absoluteIndex) else {
            return nil
        }
        return queuedJobs[absoluteIndex]
    }

    package func job(uploadId: String) -> ProcessingJob? {
        if let heldJob = heldJobsByUploadID[uploadId] {
            return heldJob.job
        }
        guard queueHead < queuedJobs.count else { return nil }
        return queuedJobs[queueHead...].first { $0.uploadId == uploadId }
    }

    package mutating func takeNextQueued() -> ProcessingJob? {
        guard queueHead < queuedJobs.count else { return nil }
        let job = queuedJobs[queueHead]
        queueHead += 1
        return job
    }

    package mutating func removeQueued(atAbsoluteIndicesDescending absoluteIndices: [Int]) {
        for absoluteIndex in absoluteIndices where absoluteIndex >= queueHead && queuedJobs.indices.contains(absoluteIndex) {
            queuedJobs.remove(at: absoluteIndex)
        }
    }

    package mutating func compactQueuedIfNeeded(threshold: Int) {
        if queueHead > threshold && queueHead > queuedJobs.count / 2 {
            queuedJobs.removeFirst(queueHead)
            queueHead = 0
        }
    }

    package mutating func updateHeld(
        uploadId: String,
        hold: MediaProcessor.VideoHoldMetadata,
        allowScheduling: Bool
    ) -> WakePlan? {
        guard var heldJob = heldJobsByUploadID[uploadId] else { return nil }
        heldJob.hold = hold
        heldJobsByUploadID[uploadId] = heldJob
        return nextWakePlan(allowScheduling: allowScheduling)
    }

    package mutating func moveQueuedToHeld(
        uploadId: String,
        hold: MediaProcessor.VideoHoldMetadata,
        allowScheduling: Bool
    ) -> WakePlan? {
        guard let queuedJob = removeQueued(uploadId: uploadId) else { return nil }
        heldJobsByUploadID[uploadId] = HeldJob(job: queuedJob, hold: hold)
        return nextWakePlan(allowScheduling: allowScheduling)
    }

    package mutating func storeHeldJobs(
        _ heldJobs: [HeldJob],
        allowScheduling: Bool
    ) -> WakePlan? {
        guard !heldJobs.isEmpty else { return nil }
        for heldJob in heldJobs {
            heldJobsByUploadID[heldJob.job.uploadId] = heldJob
        }
        return nextWakePlan(allowScheduling: allowScheduling)
    }

    package mutating func releaseHeld(
        uploadId: String,
        allowScheduling: Bool
    ) -> WakePlan? {
        guard let heldJob = heldJobsByUploadID.removeValue(forKey: uploadId) else {
            return nil
        }
        insertQueuedPreservingArrival(heldJob.job)
        return nextWakePlan(allowScheduling: allowScheduling)
    }

    package mutating func releaseReadyHeldJobs(
        now: Date,
        allowScheduling: Bool
    ) -> ReleaseResult {
        guard !heldJobsByUploadID.isEmpty else {
            return ReleaseResult(releasedAny: false, wakePlan: nil)
        }

        let readyUploadIDs = sortedHeldUploadIDs(
            heldJobsByUploadID.compactMap { uploadId, heldJob in
                heldJob.hold.wakeAt <= now ? uploadId : nil
            }
        )
        guard !readyUploadIDs.isEmpty else {
            return ReleaseResult(
                releasedAny: false,
                wakePlan: nextWakePlan(allowScheduling: allowScheduling)
            )
        }

        return releaseHeldJobs(readyUploadIDs, allowScheduling: allowScheduling)
    }

    package mutating func releaseHeldJobsForRecompute(
        allowScheduling: Bool
    ) -> ReleaseResult {
        guard !heldJobsByUploadID.isEmpty else {
            return ReleaseResult(releasedAny: false, wakePlan: nil)
        }
        return releaseHeldJobs(
            sortedHeldUploadIDs(Array(heldJobsByUploadID.keys)),
            allowScheduling: allowScheduling
        )
    }

    package mutating func releaseHeldJobs(
        uploadIDs: [String],
        allowScheduling: Bool
    ) -> ReleaseResult {
        releaseHeldJobs(
            sortedHeldUploadIDs(uploadIDs),
            allowScheduling: allowScheduling
        )
    }

    package mutating func handleWake(
        token: UInt64,
        now: Date,
        allowScheduling: Bool
    ) -> WakeHandlingResult {
        guard token == heldWakeToken else {
            return WakeHandlingResult(isCurrentToken: false, releasedAny: false, wakePlan: nil)
        }

        let releaseResult = releaseReadyHeldJobs(now: now, allowScheduling: allowScheduling)
        return WakeHandlingResult(
            isCurrentToken: true,
            releasedAny: releaseResult.releasedAny,
            wakePlan: releaseResult.wakePlan
        )
    }

    private mutating func removeQueued(uploadId: String) -> ProcessingJob? {
        guard queueHead < queuedJobs.count,
              let absoluteIndex = queuedJobs[queueHead...].firstIndex(where: { $0.uploadId == uploadId }) else {
            return nil
        }
        return queuedJobs.remove(at: absoluteIndex)
    }

    private mutating func insertQueuedPreservingArrival(_ job: ProcessingJob) {
        let insertionIndex = queuedJobs[queueHead...].firstIndex { queuedJob in
            queuedJob.arrivalAtSeconds > job.arrivalAtSeconds
        } ?? queuedJobs.endIndex
        queuedJobs.insert(job, at: insertionIndex)
    }

    private func sortedHeldUploadIDs(_ uploadIDs: [String]) -> [String] {
        uploadIDs.sorted { lhs, rhs in
            guard let lhsJob = heldJobsByUploadID[lhs]?.job,
                  let rhsJob = heldJobsByUploadID[rhs]?.job else {
                return lhs < rhs
            }
            if lhsJob.arrivalAtSeconds == rhsJob.arrivalAtSeconds {
                return lhs < rhs
            }
            return lhsJob.arrivalAtSeconds < rhsJob.arrivalAtSeconds
        }
    }

    private mutating func releaseHeldJobs(
        _ uploadIDs: [String],
        allowScheduling: Bool
    ) -> ReleaseResult {
        guard !uploadIDs.isEmpty else {
            return ReleaseResult(
                releasedAny: false,
                wakePlan: nextWakePlan(allowScheduling: allowScheduling)
            )
        }

        for uploadId in uploadIDs {
            guard let heldJob = heldJobsByUploadID.removeValue(forKey: uploadId) else { continue }
            insertQueuedPreservingArrival(heldJob.job)
        }

        return ReleaseResult(
            releasedAny: true,
            wakePlan: nextWakePlan(allowScheduling: allowScheduling)
        )
    }

    private mutating func nextWakePlan(allowScheduling: Bool) -> WakePlan {
        heldWakeToken &+= 1
        let wakeAt = allowScheduling ? heldJobsByUploadID.values.map(\.hold.wakeAt).min() : nil
        return WakePlan(token: heldWakeToken, wakeAt: wakeAt)
    }
}
