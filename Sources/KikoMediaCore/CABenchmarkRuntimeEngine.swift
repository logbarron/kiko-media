import Foundation

package enum CABenchmarkRuntimePolicy: Sendable {
    case fifo
    case complexityAware
}

package struct CABenchmarkRuntimeMachineProfile: Sendable {
    package let id: String
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let degradationCurve: [CADegradationPoint]
    package let txInMS: Double
    package let txOutMS: Double
    package let publishOverheadMS: Double
    package let modeledConcurrencyCap: Int?

    package init(
        id: String,
        msPerFrameC1: Double,
        fixedOverheadMS: Double = 0,
        degradationCurve: [CADegradationPoint],
        txInMS: Double,
        txOutMS: Double = 0,
        publishOverheadMS: Double = 0,
        modeledConcurrencyCap: Int? = nil
    ) {
        self.id = id
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.degradationCurve = degradationCurve
        self.txInMS = txInMS
        self.txOutMS = txOutMS
        self.publishOverheadMS = publishOverheadMS
        self.modeledConcurrencyCap = modeledConcurrencyCap
    }
}

package struct CABenchmarkRuntimeSlotBinding: Sendable {
    package let machineIndex: Int
    package let slotID: String

    package init(machineIndex: Int, slotID: String) {
        self.machineIndex = machineIndex
        self.slotID = slotID
    }
}

package struct CABenchmarkRuntimeDispatchItem: Sendable {
    package let index: Int
    package let arrivalAtSeconds: Double
    package let dispatchConcurrency: Int?
    package let dispatchMachineIndex: Int
    package let decisionAtSeconds: Double?
    package let predictedSlotReadyMS: Double?
    package let predictedStartMS: Double?
    package let predictedDoneMS: Double?
    package let waited: Bool

    package init(
        index: Int,
        arrivalAtSeconds: Double,
        dispatchConcurrency: Int?,
        dispatchMachineIndex: Int,
        decisionAtSeconds: Double?,
        predictedSlotReadyMS: Double?,
        predictedStartMS: Double?,
        predictedDoneMS: Double?,
        waited: Bool
    ) {
        self.index = index
        self.arrivalAtSeconds = arrivalAtSeconds
        self.dispatchConcurrency = dispatchConcurrency
        self.dispatchMachineIndex = dispatchMachineIndex
        self.decisionAtSeconds = decisionAtSeconds
        self.predictedSlotReadyMS = predictedSlotReadyMS
        self.predictedStartMS = predictedStartMS
        self.predictedDoneMS = predictedDoneMS
        self.waited = waited
    }
}

private extension CABenchmarkRuntimeDispatchItem {
    static let logicalDecisionLagThresholdSeconds = 0.1

    func rebasedDecisionReference(to decisionAtSeconds: Double) -> Self {
        let currentDecisionAtSeconds = self.decisionAtSeconds ?? decisionAtSeconds
        let decisionShiftMS = max(0, (currentDecisionAtSeconds - decisionAtSeconds) * 1_000.0)
        return CABenchmarkRuntimeDispatchItem(
            index: index,
            arrivalAtSeconds: arrivalAtSeconds,
            dispatchConcurrency: dispatchConcurrency,
            dispatchMachineIndex: dispatchMachineIndex,
            decisionAtSeconds: decisionAtSeconds,
            predictedSlotReadyMS: predictedSlotReadyMS.map { decisionShiftMS + $0 },
            predictedStartMS: predictedStartMS.map { decisionShiftMS + $0 },
            predictedDoneMS: predictedDoneMS.map { decisionShiftMS + $0 },
            waited: waited || decisionShiftMS > 0
        )
    }
}

package struct CABenchmarkRuntimeScheduledDispatch: Sendable {
    package let slotOrdinal: Int
    package let item: CABenchmarkRuntimeDispatchItem

    package init(slotOrdinal: Int, item: CABenchmarkRuntimeDispatchItem) {
        self.slotOrdinal = slotOrdinal
        self.item = item
    }
}

package struct CABenchmarkRuntimeAdaptationRow: Sendable {
    package let machineID: String
    package let completions: Int
    package let initialMSPerFrameC1: Double
    package let finalMSPerFrameC1: Double

    package init(
        machineID: String,
        completions: Int,
        initialMSPerFrameC1: Double,
        finalMSPerFrameC1: Double
    ) {
        self.machineID = machineID
        self.completions = completions
        self.initialMSPerFrameC1 = initialMSPerFrameC1
        self.finalMSPerFrameC1 = finalMSPerFrameC1
    }
}

package struct CABenchmarkRuntimeScheduleResult: Sendable {
    package let dispatches: [CABenchmarkRuntimeScheduledDispatch]
    package let madeProgress: Bool

    package init(
        dispatches: [CABenchmarkRuntimeScheduledDispatch],
        madeProgress: Bool
    ) {
        self.dispatches = dispatches
        self.madeProgress = madeProgress
    }
}

package actor CABenchmarkRuntimeEngine {
    private struct HeldDispatch: Sendable {
        let slotOrdinal: Int
        let item: CABenchmarkRuntimeDispatchItem
        let targetReadyAtSeconds: Double
        let wakeAtSeconds: Double
    }

    package struct HeldDispatchSnapshot: Sendable, Equatable {
        package let index: Int
        package let slotOrdinal: Int
        package let targetReadyAtSeconds: Double
        package let wakeAtSeconds: Double
    }

    private let policy: CABenchmarkRuntimePolicy
    private let videoCosts: [CAResolvedVideoCost]
    private let machineProfiles: [CABenchmarkRuntimeMachineProfile]
    private let slotBindings: [CABenchmarkRuntimeSlotBinding]
    private let machineSlotCounts: [Int]
    private var slotReadyAtMS: [Double]
    private let initialMachineMSPerFrameC1: [Double]
    private var machineMSPerFrameC1: [Double]
    private var machineMSPerFrameErrorEMA: [Double?]
    private var machineMSPerFrameAbsErrorEMA: [Double?]
    private let initialMachineTxInMS: [Double]
    private var machineTxInMS: [Double]
    private let machineTxOutMS: [Double]
    private let machinePublishOverheadMS: [Double]
    private var transferStartBaselineMSByMachine: [Double?]
    private var transferOverheadEstimateMSByMachine: [Double?]
    private var completionCountsByMachine: [Int]
    private var pending: [Int] = []
    private var arrivals: [Int: Double] = [:]
    private var arrivalsComplete = false
    private let slotOrdinalByRef: [CASlotRef: Int]
    private var slotIsDown: [Bool]
    private var arrivalWaiters: [CheckedContinuation<Void, Never>] = []
    private var collectedSolverTelemetry: [CASolverTelemetry] = []
    private var heldDispatches: [HeldDispatch] = []
    private var preservedDecisionAtSecondsByIndex: [Int: Double] = [:]

    package init(
        policy: CABenchmarkRuntimePolicy,
        videoCosts: [CAResolvedVideoCost],
        machineProfiles: [CABenchmarkRuntimeMachineProfile],
        slotBindings: [CABenchmarkRuntimeSlotBinding]
    ) {
        self.policy = policy
        self.videoCosts = videoCosts
        self.machineProfiles = machineProfiles
        self.slotBindings = slotBindings
        self.slotReadyAtMS = Array(repeating: 0, count: slotBindings.count)
        let initialMSPerFrame = machineProfiles.map(\.msPerFrameC1)
        self.initialMachineMSPerFrameC1 = initialMSPerFrame
        self.machineMSPerFrameC1 = initialMSPerFrame
        self.machineMSPerFrameErrorEMA = Array(repeating: nil, count: machineProfiles.count)
        self.machineMSPerFrameAbsErrorEMA = Array(repeating: nil, count: machineProfiles.count)
        let initialTxIn = machineProfiles.map { max(0, $0.txInMS) }
        self.initialMachineTxInMS = initialTxIn
        self.machineTxInMS = initialTxIn
        self.machineTxOutMS = machineProfiles.map { max(0, $0.txOutMS) }
        self.machinePublishOverheadMS = machineProfiles.map { max(0, $0.publishOverheadMS) }
        self.transferStartBaselineMSByMachine = initialTxIn.map { $0 > 0 ? $0 : nil }
        self.transferOverheadEstimateMSByMachine = initialTxIn.map { $0 > 0 ? $0 : nil }
        self.completionCountsByMachine = Array(repeating: 0, count: machineProfiles.count)
        self.slotIsDown = Array(repeating: false, count: slotBindings.count)

        var slotCounts = Array(repeating: 0, count: machineProfiles.count)
        for binding in slotBindings where slotCounts.indices.contains(binding.machineIndex) {
            slotCounts[binding.machineIndex] += 1
        }
        self.machineSlotCounts = slotCounts.map { max(1, $0) }

        var nextSlotIndexByMachine = Array(repeating: 0, count: machineProfiles.count)
        var refToOrdinal: [CASlotRef: Int] = [:]
        refToOrdinal.reserveCapacity(slotBindings.count)
        for (slotOrdinal, binding) in slotBindings.enumerated() {
            guard nextSlotIndexByMachine.indices.contains(binding.machineIndex) else { continue }
            let slotRef = CASlotRef(
                machineIndex: binding.machineIndex,
                slotIndex: nextSlotIndexByMachine[binding.machineIndex]
            )
            nextSlotIndexByMachine[binding.machineIndex] += 1
            refToOrdinal[slotRef] = slotOrdinal
        }
        self.slotOrdinalByRef = refToOrdinal
    }

    package func enqueue(index: Int, arrivalAtSeconds: Double) {
        insertPending(index: index, arrivalAtSeconds: arrivalAtSeconds)
        resumeArrivalWaiters()
    }

    package func finishArrivals() {
        arrivalsComplete = true
        resumeArrivalWaiters()
    }

    package var hasPending: Bool { !pending.isEmpty }
    package var hasHeldDispatches: Bool { !heldDispatches.isEmpty }
    package var isComplete: Bool { arrivalsComplete && pending.isEmpty }
    package var nextHeldWakeSeconds: Double? { heldDispatches.map(\.wakeAtSeconds).min() }

    package func pendingIndicesSnapshot() -> [Int] {
        pending
    }

    package func heldDispatchSnapshot() -> [HeldDispatchSnapshot] {
        heldDispatches.map { held in
            HeldDispatchSnapshot(
                index: held.item.index,
                slotOrdinal: held.slotOrdinal,
                targetReadyAtSeconds: held.targetReadyAtSeconds,
                wakeAtSeconds: held.wakeAtSeconds
            )
        }
    }

    package func waitForWork() async {
        if !pending.isEmpty || arrivalsComplete { return }
        await waitForArrivalSignal()
    }

    package func scheduleBatch(
        freeSlotOrdinals: Set<Int>,
        nowSeconds: Double
    ) -> [CABenchmarkRuntimeScheduledDispatch] {
        guard !pending.isEmpty, !freeSlotOrdinals.isEmpty else { return [] }
        let nowMS = max(0, nowSeconds * 1_000)

        if policy == .fifo {
            return scheduleFIFOBatch(freeSlotOrdinals: freeSlotOrdinals)
        } else {
            return scheduleCABatch(freeSlotOrdinals: freeSlotOrdinals, nowMS: nowMS)
        }
    }

    package func scheduleRuntimeBatch(
        freeSlotOrdinals: Set<Int>,
        nowSeconds: Double,
        totalJobCount: Int
    ) -> CABenchmarkRuntimeScheduleResult {
        let batch = scheduleBatch(
            freeSlotOrdinals: freeSlotOrdinals,
            nowSeconds: nowSeconds
        )
        guard !batch.isEmpty else {
            return CABenchmarkRuntimeScheduleResult(dispatches: [], madeProgress: false)
        }

        var dispatches: [CABenchmarkRuntimeScheduledDispatch] = []
        dispatches.reserveCapacity(batch.count)
        var madeProgress = false

        for scheduledDispatch in batch {
            let item = scheduledDispatch.item
            let decisionReferenceAtSeconds = preservedDecisionAtSecondsByIndex[item.index]
                ?? {
                    let scheduledDecisionAt = item.arrivalAtSeconds
                    let actualDecisionAt = item.decisionAtSeconds ?? scheduledDecisionAt
                    if totalJobCount == 1 {
                        return actualDecisionAt
                    }
                    let schedulerLagSeconds = max(0, actualDecisionAt - scheduledDecisionAt)
                    if item.waited || schedulerLagSeconds > CABenchmarkRuntimeDispatchItem.logicalDecisionLagThresholdSeconds {
                        return scheduledDecisionAt
                    }
                    return actualDecisionAt
                }()
            let preparedItem = item.rebasedDecisionReference(to: decisionReferenceAtSeconds)

            if policy == .complexityAware,
               let predictedSlotReadyMS = item.predictedSlotReadyMS,
               predictedSlotReadyMS > 0 {
                let targetReadyAtSeconds = (preparedItem.decisionAtSeconds ?? nowSeconds)
                    + ((preparedItem.predictedSlotReadyMS ?? predictedSlotReadyMS) / 1_000.0)
                preservedDecisionAtSecondsByIndex[preparedItem.index] = decisionReferenceAtSeconds
                heldDispatches.append(
                    HeldDispatch(
                        slotOrdinal: scheduledDispatch.slotOrdinal,
                        item: preparedItem,
                        targetReadyAtSeconds: targetReadyAtSeconds,
                        wakeAtSeconds: targetReadyAtSeconds
                    )
                )
                madeProgress = true
                continue
            }

            guard freeSlotOrdinals.contains(scheduledDispatch.slotOrdinal) else {
                if let decisionAtSeconds = preparedItem.decisionAtSeconds {
                    preservedDecisionAtSecondsByIndex[preparedItem.index] = decisionAtSeconds
                }
                let targetReadyAtSeconds = (preparedItem.decisionAtSeconds ?? nowSeconds)
                    + max(0, (preparedItem.predictedSlotReadyMS ?? 0) / 1_000.0)
                heldDispatches.append(
                    HeldDispatch(
                        slotOrdinal: scheduledDispatch.slotOrdinal,
                        item: preparedItem,
                        targetReadyAtSeconds: targetReadyAtSeconds,
                        wakeAtSeconds: targetReadyAtSeconds
                    )
                )
                madeProgress = true
                continue
            }

            preservedDecisionAtSecondsByIndex.removeValue(forKey: preparedItem.index)
            dispatches.append(
                CABenchmarkRuntimeScheduledDispatch(
                    slotOrdinal: scheduledDispatch.slotOrdinal,
                    item: preparedItem
                )
            )
            madeProgress = true
        }

        return CABenchmarkRuntimeScheduleResult(
            dispatches: dispatches,
            madeProgress: madeProgress
        )
    }

    package func markCompletedSlotsReady(
        _ completedSlotOrdinals: [Int],
        nowSeconds: Double
    ) -> Bool {
        guard !completedSlotOrdinals.isEmpty else { return false }
        for slotOrdinal in completedSlotOrdinals {
            markSlotReady(slotOrdinal: slotOrdinal, nowSeconds: nowSeconds)
        }
        return invalidateHeldDispatchesForTargetStateChanges()
    }

    package func releaseReadyHeldDispatches(nowSeconds: Double) -> Bool {
        let readyIndices = heldDispatches.indices.filter { heldDispatches[$0].wakeAtSeconds <= nowSeconds }
        return requeueHeldDispatches(at: readyIndices)
    }

    package func markSlotReady(slotOrdinal: Int, nowSeconds: Double) {
        guard slotReadyAtMS.indices.contains(slotOrdinal) else { return }
        slotReadyAtMS[slotOrdinal] = max(0, nowSeconds * 1_000)
    }

    package func recordCompletion(
        machineIndex: Int,
        frameCount: Double,
        processNanos: UInt64,
        concurrencyHint: Int?
    ) {
        guard machineProfiles.indices.contains(machineIndex),
              machineMSPerFrameC1.indices.contains(machineIndex),
              frameCount.isFinite,
              frameCount > 0,
              processNanos > 0 else {
            return
        }

        let observedConcurrency: Int = {
            if let concurrencyHint, concurrencyHint > 0 {
                return concurrencyHint
            }
            if machineSlotCounts.indices.contains(machineIndex) {
                return machineSlotCounts[machineIndex]
            }
            return 1
        }()

        let sampleModel = CASuccessfulExecutionSampleModel(
            msPerFrameC1: initialMachineMSPerFrameC1.indices.contains(machineIndex)
                ? initialMachineMSPerFrameC1[machineIndex]
                : machineProfiles[machineIndex].msPerFrameC1,
            fixedOverheadMS: machineProfiles[machineIndex].fixedOverheadMS,
            degradationCurve: machineProfiles[machineIndex].degradationCurve
        )
        guard let observedMSPerFrame = ThunderboltAdaptiveTelemetryReducer.normalizedMSPerFrameC1(
            processNanos: processNanos,
            frameCount: frameCount,
            model: sampleModel,
            concurrency: observedConcurrency
        ) else {
            return
        }

        let previous = machineMSPerFrameC1[machineIndex]
        if previous.isFinite,
           previous > 0,
           let update = adaptedMSPerFrameC1Update(
                machineIndex: machineIndex,
                previousEstimate: previous,
                observedEstimate: observedMSPerFrame
           ) {
            machineMSPerFrameC1[machineIndex] = update.estimate
            machineMSPerFrameErrorEMA[machineIndex] = update.smoothedError
            machineMSPerFrameAbsErrorEMA[machineIndex] = update.smoothedAbsoluteError
        } else {
            machineMSPerFrameC1[machineIndex] = observedMSPerFrame
        }
        if completionCountsByMachine.indices.contains(machineIndex) {
            completionCountsByMachine[machineIndex] += 1
        }
    }

    package func recordTransferOverhead(machineIndex: Int, sampleMS: Double?) {
        guard machineTxInMS.indices.contains(machineIndex),
              transferStartBaselineMSByMachine.indices.contains(machineIndex),
              transferOverheadEstimateMSByMachine.indices.contains(machineIndex) else {
            return
        }

        guard let update = TransferOverheadEstimator.reduce(
            previousBaseline: transferStartBaselineMSByMachine[machineIndex],
            previousEstimate: transferOverheadEstimateMSByMachine[machineIndex],
            sampleMS: sampleMS
        ) else {
            return
        }
        transferStartBaselineMSByMachine[machineIndex] = update.baseline
        transferOverheadEstimateMSByMachine[machineIndex] = update.estimate

        let estimated = transferOverheadEstimateMSByMachine[machineIndex] ?? initialMachineTxInMS[machineIndex]
        machineTxInMS[machineIndex] = max(0, estimated)
    }

    package func recordSlotHealth(slotOrdinal: Int, isDown: Bool) {
        guard slotIsDown.indices.contains(slotOrdinal) else { return }
        slotIsDown[slotOrdinal] = isDown
    }

    package func solverTelemetrySnapshot() -> [CASolverTelemetry] {
        collectedSolverTelemetry
    }

    func machineSnapshotForTesting() -> [CAMachine] {
        machinesForDispatch()
    }

    package func adaptationRows() -> [CABenchmarkRuntimeAdaptationRow] {
        var rows: [CABenchmarkRuntimeAdaptationRow] = []
        rows.reserveCapacity(machineProfiles.count)
        for index in machineProfiles.indices {
            rows.append(
                CABenchmarkRuntimeAdaptationRow(
                    machineID: machineProfiles[index].id,
                    completions: completionCountsByMachine.indices.contains(index) ? completionCountsByMachine[index] : 0,
                    initialMSPerFrameC1: initialMachineMSPerFrameC1.indices.contains(index)
                        ? initialMachineMSPerFrameC1[index]
                        : machineProfiles[index].msPerFrameC1,
                    finalMSPerFrameC1: machineMSPerFrameC1.indices.contains(index)
                        ? machineMSPerFrameC1[index]
                        : machineProfiles[index].msPerFrameC1
                )
            )
        }
        return rows
    }

    private func scheduleFIFOBatch(
        freeSlotOrdinals: Set<Int>
    ) -> [CABenchmarkRuntimeScheduledDispatch] {
        let sortedSlots = freeSlotOrdinals.sorted()
        var results: [CABenchmarkRuntimeScheduledDispatch] = []
        results.reserveCapacity(min(sortedSlots.count, pending.count))

        for slotOrdinal in sortedSlots {
            guard !pending.isEmpty else { break }
            let index = pending.remove(at: 0)
            let arrivalAtSeconds = arrivals.removeValue(forKey: index) ?? 0
            let dispatchMachineIndex: Int = {
                guard slotBindings.indices.contains(slotOrdinal),
                      machineProfiles.indices.contains(slotBindings[slotOrdinal].machineIndex) else {
                    return 0
                }
                return slotBindings[slotOrdinal].machineIndex
            }()
            results.append(
                CABenchmarkRuntimeScheduledDispatch(
                    slotOrdinal: slotOrdinal,
                    item: CABenchmarkRuntimeDispatchItem(
                        index: index,
                        arrivalAtSeconds: arrivalAtSeconds,
                        dispatchConcurrency: nil,
                        dispatchMachineIndex: dispatchMachineIndex,
                        decisionAtSeconds: nil,
                        predictedSlotReadyMS: nil,
                        predictedStartMS: nil,
                        predictedDoneMS: nil,
                        waited: false
                    )
                )
            )
        }
        return results
    }

    private func scheduleCABatch(
        freeSlotOrdinals: Set<Int>,
        nowMS: Double
    ) -> [CABenchmarkRuntimeScheduledDispatch] {
        let dispatchMachines = machinesForDispatch()
        let pendingJobs = pending.enumerated().map { offset, videoIndex in
            makePendingPickJob(videoIndex: videoIndex, enqueueOrder: offset)
        }
        let maxReadyNowCount = min(freeSlotOrdinals.count, pendingJobs.count)
        let pickInvocation = ComplexityAwareScheduler.pickTwoStageBatchWithTelemetry(
            pendingJobs: pendingJobs,
            machines: dispatchMachines,
            nowMS: nowMS,
            maxReadyNowCount: maxReadyNowCount
        )
        let pickResult = pickInvocation.result
        collectedSolverTelemetry.append(pickInvocation.telemetry)
        let scheduledPicks = pickResult.readyNowPicks + pickResult.reservationPicks
        guard !scheduledPicks.isEmpty else { return [] }

        var results: [CABenchmarkRuntimeScheduledDispatch] = []
        results.reserveCapacity(scheduledPicks.count)

        for pick in scheduledPicks {
            guard let slotOrdinal = slotOrdinalByRef[pick.slot],
                  let pendingOffset = pending.firstIndex(of: pick.token) else {
                continue
            }
            let predictedSlotReadyMS = max(0, pick.score.tReadySlotMS - nowMS)
            let predictedStartMS = max(0, pick.score.tStartMS - nowMS)
            let predictedDoneMS = max(0, pick.score.tDoneMS - nowMS)
            let index = pending.remove(at: pendingOffset)
            let arrivalAtSeconds = arrivals.removeValue(forKey: index) ?? 0
            results.append(
                CABenchmarkRuntimeScheduledDispatch(
                    slotOrdinal: slotOrdinal,
                    item: CABenchmarkRuntimeDispatchItem(
                        index: index,
                        arrivalAtSeconds: arrivalAtSeconds,
                        dispatchConcurrency: pick.score.clampedConcurrency,
                        dispatchMachineIndex: pick.slot.machineIndex,
                        decisionAtSeconds: nowMS / 1_000.0,
                        predictedSlotReadyMS: predictedSlotReadyMS,
                        predictedStartMS: predictedStartMS,
                        predictedDoneMS: predictedDoneMS,
                        waited: predictedSlotReadyMS > 0
                    )
                )
            )

            if slotReadyAtMS.indices.contains(slotOrdinal),
                 pickResult.projectedMachines.indices.contains(pick.slot.machineIndex),
                 pickResult.projectedMachines[pick.slot.machineIndex].slots.indices.contains(pick.slot.slotIndex) {
                slotReadyAtMS[slotOrdinal] = pickResult
                    .projectedMachines[pick.slot.machineIndex]
                    .slots[pick.slot.slotIndex]
                    .readyAtMS
            }
        }
        return results
    }

    private func resumeArrivalWaiters() {
        guard !arrivalWaiters.isEmpty else { return }
        let waiters = arrivalWaiters
        arrivalWaiters.removeAll(keepingCapacity: true)
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func waitForArrivalSignal() async {
        await withCheckedContinuation { continuation in
            if !pending.isEmpty || arrivalsComplete {
                continuation.resume()
                return
            }
            arrivalWaiters.append(continuation)
        }
    }

    package func makePendingPickJob(
        videoIndex: Int,
        enqueueOrder: Int
    ) -> CAPendingPickJob<Int> {
        guard videoCosts.indices.contains(videoIndex) else {
            preconditionFailure("Missing carried video cost for benchmark runtime job index \(videoIndex)")
        }
        let arrivalAtMS = max(0, (arrivals[videoIndex] ?? 0) * 1_000)
        let frameCount = max(1, videoCosts[videoIndex].frameCount)
        return CAPendingPickJob(
            token: videoIndex,
            job: CAJob(
                id: String(videoIndex),
                arrivalAtMS: arrivalAtMS,
                enqueueOrder: enqueueOrder,
                frameCount: frameCount
            )
        )
    }

    private func insertPending(index: Int, arrivalAtSeconds: Double) {
        let insertionIndex = pending.firstIndex { pendingIndex in
            (arrivals[pendingIndex] ?? 0) > arrivalAtSeconds
        } ?? pending.endIndex
        pending.insert(index, at: insertionIndex)
        arrivals[index] = arrivalAtSeconds
    }

    private func requeueHeldDispatches(at indices: [Int]) -> Bool {
        guard !indices.isEmpty else { return false }

        var released: [HeldDispatch] = []
        released.reserveCapacity(indices.count)
        for index in indices.sorted(by: >) {
            released.append(heldDispatches.remove(at: index))
        }
        released.sort { lhs, rhs in
            if lhs.item.arrivalAtSeconds == rhs.item.arrivalAtSeconds {
                return lhs.item.index < rhs.item.index
            }
            return lhs.item.arrivalAtSeconds < rhs.item.arrivalAtSeconds
        }
        for held in released {
            if let decisionAtSeconds = held.item.decisionAtSeconds {
                preservedDecisionAtSecondsByIndex[held.item.index] = decisionAtSeconds
            }
            insertPending(index: held.item.index, arrivalAtSeconds: held.item.arrivalAtSeconds)
        }
        resumeArrivalWaiters()
        return true
    }

    private func invalidateHeldDispatchesForTargetStateChanges() -> Bool {
        let indices = heldDispatches.indices.filter { index in
            let held = heldDispatches[index]
            guard slotReadyAtMS.indices.contains(held.slotOrdinal),
                  slotIsDown.indices.contains(held.slotOrdinal) else {
                return true
            }
            return CAHoldInvalidation.invalidationReason(
                baselineReadyAtMS: held.targetReadyAtSeconds * 1_000.0,
                currentReadyAtMS: slotReadyAtMS[held.slotOrdinal],
                slotIsDown: slotIsDown[held.slotOrdinal],
                targetStillPossible: true
            ) != nil
        }
        return requeueHeldDispatches(at: indices)
    }

    private func adaptedMSPerFrameC1Update(
        machineIndex: Int,
        previousEstimate: Double,
        observedEstimate: Double
    ) -> LiveAdaptiveMSPerFrameC1Estimator.Update? {
        guard machineMSPerFrameErrorEMA.indices.contains(machineIndex),
              machineMSPerFrameAbsErrorEMA.indices.contains(machineIndex) else {
            return nil
        }

        let isFirstObservation = completionCountsByMachine.indices.contains(machineIndex)
            ? completionCountsByMachine[machineIndex] == 0
            : true
        return LiveAdaptiveMSPerFrameC1Estimator.next(
            previousEstimate: previousEstimate,
            previousSmoothedError: isFirstObservation ? nil : machineMSPerFrameErrorEMA[machineIndex],
            previousSmoothedAbsoluteError: isFirstObservation
                ? nil
                : machineMSPerFrameAbsErrorEMA[machineIndex],
            initialEstimate: initialMachineMSPerFrameC1.indices.contains(machineIndex)
                ? initialMachineMSPerFrameC1[machineIndex]
                : nil,
            observed: observedEstimate
        )
    }

    private func machinesForDispatch() -> [CAMachine] {
        var slotsByMachine = Array(repeating: [CASlot](), count: machineProfiles.count)
        for machineIndex in machineProfiles.indices {
            if machineSlotCounts.indices.contains(machineIndex) {
                slotsByMachine[machineIndex].reserveCapacity(machineSlotCounts[machineIndex])
            }
        }

        for (slotOrdinal, binding) in slotBindings.enumerated() {
            guard slotsByMachine.indices.contains(binding.machineIndex) else { continue }
            let readyAtMS = slotReadyAtMS.indices.contains(slotOrdinal) ? slotReadyAtMS[slotOrdinal] : 0
            slotsByMachine[binding.machineIndex].append(
                CASlot(
                    id: binding.slotID,
                    readyAtMS: readyAtMS,
                    isDown: slotIsDown.indices.contains(slotOrdinal) ? slotIsDown[slotOrdinal] : false
                )
            )
        }

        var machines: [CAMachine] = []
        machines.reserveCapacity(machineProfiles.count)
        for machineIndex in machineProfiles.indices {
            let profile = machineProfiles[machineIndex]
            machines.append(
                CAMachine(
                    id: profile.id,
                    slots: slotsByMachine[machineIndex],
                    msPerFrameC1: machineMSPerFrameC1.indices.contains(machineIndex)
                        ? machineMSPerFrameC1[machineIndex]
                        : profile.msPerFrameC1,
                    fixedOverheadMS: profile.fixedOverheadMS,
                    degradationCurve: profile.degradationCurve,
                    txInMS: machineTxInMS.indices.contains(machineIndex)
                        ? machineTxInMS[machineIndex]
                        : profile.txInMS,
                    txOutMS: machineTxOutMS.indices.contains(machineIndex)
                        ? machineTxOutMS[machineIndex]
                        : profile.txOutMS,
                    publishOverheadMS: machinePublishOverheadMS.indices.contains(machineIndex)
                        ? machinePublishOverheadMS[machineIndex]
                        : profile.publishOverheadMS,
                    modeledConcurrencyCap: profile.modeledConcurrencyCap
                )
            )
        }
        return machines
    }
}
