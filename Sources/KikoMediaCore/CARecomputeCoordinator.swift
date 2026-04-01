import Foundation

package struct CARecomputeCoordinator {
    package typealias Trigger = ThunderboltDispatcher.RecomputeTrigger
    package typealias Signal = @Sendable (Trigger) -> Void

    package enum PassKind: Sendable, Equatable {
        case genericRecompute
        case completionRefill
    }

    package struct Pass: Sendable, Equatable {
        package let kind: PassKind
        package let reconsiderHeldJobs: Bool

        package init(kind: PassKind, reconsiderHeldJobs: Bool) {
            self.kind = kind
            self.reconsiderHeldJobs = reconsiderHeldJobs
        }
    }

    private var recomputeInFlight = false
    private var genericRecomputePending = false
    private var genericRecomputePendingReconsiderHeldJobs = false
    private var completionRefillPending = false
    private var completionRefillPendingReconsiderHeldJobs = false
    private var slotDownBatchFlushScheduled = false
    private var slotDownBatchFlushTask: Task<Void, Never>?

    package init() {}

    package mutating func beginRecomputeRun(
        requestedPassKind: PassKind,
        reconsiderHeldJobs: Bool
    ) -> Bool {
        markPassPending(
            requestedPassKind,
            reconsiderHeldJobs: reconsiderHeldJobs
        )
        guard !recomputeInFlight else {
            return false
        }
        recomputeInFlight = true
        return true
    }

    package mutating func finishRecomputeRun() {
        recomputeInFlight = false
    }

    package mutating func beginRecomputePass() -> Pass {
        if completionRefillPending {
            completionRefillPending = false
            defer { completionRefillPendingReconsiderHeldJobs = false }
            return Pass(
                kind: .completionRefill,
                reconsiderHeldJobs: completionRefillPendingReconsiderHeldJobs
            )
        }

        genericRecomputePending = false
        defer { genericRecomputePendingReconsiderHeldJobs = false }
        return Pass(
            kind: .genericRecompute,
            reconsiderHeldJobs: genericRecomputePendingReconsiderHeldJobs
        )
    }

    package var requiresAnotherRecomputePass: Bool {
        completionRefillPending || genericRecomputePending
    }

    package mutating func intakeDispatcherTrigger(
        _ trigger: Trigger,
        allowSlotDownBatchCoalescing: Bool,
        allowScheduling: Bool,
        flushSlotDownBatch: @escaping @Sendable () async -> Void
    ) -> Trigger? {
        guard trigger == .slotDownBatch else { return trigger }
        scheduleSlotDownBatchFlushIfNeeded(
            allowSlotDownBatchCoalescing: allowSlotDownBatchCoalescing,
            allowScheduling: allowScheduling,
            flushSlotDownBatch: flushSlotDownBatch
        )
        return nil
    }

    package mutating func beginSlotDownBatchFlush(allowScheduling: Bool) -> Bool {
        slotDownBatchFlushTask = nil
        guard slotDownBatchFlushScheduled else { return false }
        guard allowScheduling else {
            slotDownBatchFlushScheduled = false
            return false
        }
        return true
    }

    package mutating func finishSlotDownBatchFlush() {
        slotDownBatchFlushScheduled = false
    }

    package mutating func cancelPendingSlotDownBatchFlush() {
        slotDownBatchFlushTask?.cancel()
        slotDownBatchFlushTask = nil
        slotDownBatchFlushScheduled = false
    }

    private mutating func scheduleSlotDownBatchFlushIfNeeded(
        allowSlotDownBatchCoalescing: Bool,
        allowScheduling: Bool,
        flushSlotDownBatch: @escaping @Sendable () async -> Void
    ) {
        guard allowSlotDownBatchCoalescing else { return }
        guard allowScheduling else { return }
        guard !slotDownBatchFlushScheduled else { return }

        slotDownBatchFlushScheduled = true
        slotDownBatchFlushTask = Task {
            await Task.yield()
            await flushSlotDownBatch()
        }
    }

    private mutating func markPassPending(
        _ passKind: PassKind,
        reconsiderHeldJobs: Bool
    ) {
        switch passKind {
        case .genericRecompute:
            genericRecomputePending = true
            genericRecomputePendingReconsiderHeldJobs =
                genericRecomputePendingReconsiderHeldJobs || reconsiderHeldJobs
        case .completionRefill:
            completionRefillPending = true
            completionRefillPendingReconsiderHeldJobs =
                completionRefillPendingReconsiderHeldJobs || reconsiderHeldJobs
        }
    }
}
