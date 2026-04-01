import Foundation

package typealias CAMilliseconds = Double

package struct CAJob: Sendable, Equatable {
    package let id: String
    package let arrivalAtMS: CAMilliseconds
    package let enqueueOrder: Int
    package let frameCount: Double

    package init(id: String, arrivalAtMS: CAMilliseconds, enqueueOrder: Int, frameCount: Double) {
        self.id = id
        self.arrivalAtMS = arrivalAtMS
        self.enqueueOrder = enqueueOrder
        self.frameCount = frameCount
    }
}

package struct CASlot: Sendable, Equatable {
    package let id: String
    package let readyAtMS: CAMilliseconds
    package let isDown: Bool

    package init(id: String, readyAtMS: CAMilliseconds, isDown: Bool = false) {
        self.id = id
        self.readyAtMS = readyAtMS
        self.isDown = isDown
    }
}

package struct CADegradationPoint: Sendable, Equatable {
    package let concurrency: Int
    package let ratioToC1: Double

    package init(concurrency: Int, ratioToC1: Double) {
        self.concurrency = concurrency
        self.ratioToC1 = ratioToC1
    }
}

package struct CAMachine: Sendable, Equatable {
    package let id: String
    package let slots: [CASlot]
    package let msPerFrameC1: Double
    package let fixedOverheadMS: CAMilliseconds
    package let degradationCurve: [CADegradationPoint]
    package let txInMS: CAMilliseconds
    package let txOutMS: CAMilliseconds
    package let publishOverheadMS: CAMilliseconds
    package let modeledConcurrencyCap: Int?

    package init(
        id: String,
        slots: [CASlot],
        msPerFrameC1: Double,
        fixedOverheadMS: CAMilliseconds = 0,
        degradationCurve: [CADegradationPoint],
        txInMS: CAMilliseconds,
        txOutMS: CAMilliseconds = 0,
        publishOverheadMS: CAMilliseconds = 0,
        modeledConcurrencyCap: Int? = nil
    ) {
        self.id = id
        self.slots = slots
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.degradationCurve = CAProfileAndFallbackMath.repairedDenseDegradationCurve(from: degradationCurve)
        self.txInMS = txInMS
        self.txOutMS = txOutMS
        self.publishOverheadMS = publishOverheadMS
        self.modeledConcurrencyCap = modeledConcurrencyCap.map { max(1, $0) }
    }
}

package struct CACandidate: Sendable, Equatable {
    package let job: CAJob
    package let machine: CAMachine
    package let slot: CASlot

    package init(job: CAJob, machine: CAMachine, slot: CASlot) {
        self.job = job
        self.machine = machine
        self.slot = slot
    }
}

package struct CAScore: Sendable, Equatable {
    package let tReadySlotMS: CAMilliseconds
    package let tReadyInputMS: CAMilliseconds
    package let tStartMS: CAMilliseconds
    package let activeSlotsAtStart: Int
    package let clampedConcurrency: Int
    package let degradationFactor: Double
    package let runMS: CAMilliseconds
    package let tDoneMS: CAMilliseconds

    package init(
        tReadySlotMS: CAMilliseconds,
        tReadyInputMS: CAMilliseconds,
        tStartMS: CAMilliseconds,
        activeSlotsAtStart: Int,
        clampedConcurrency: Int,
        degradationFactor: Double,
        runMS: CAMilliseconds,
        tDoneMS: CAMilliseconds
    ) {
        self.tReadySlotMS = tReadySlotMS
        self.tReadyInputMS = tReadyInputMS
        self.tStartMS = tStartMS
        self.activeSlotsAtStart = activeSlotsAtStart
        self.clampedConcurrency = clampedConcurrency
        self.degradationFactor = degradationFactor
        self.runMS = runMS
        self.tDoneMS = tDoneMS
    }
}

package struct CAScoredCandidate: Sendable, Equatable {
    package let candidate: CACandidate
    package let score: CAScore

    package init(candidate: CACandidate, score: CAScore) {
        self.candidate = candidate
        self.score = score
    }
}

package struct CASlotRef: Sendable, Equatable, Hashable {
    package let machineIndex: Int
    package let slotIndex: Int

    package init(machineIndex: Int, slotIndex: Int) {
        self.machineIndex = machineIndex
        self.slotIndex = slotIndex
    }
}

package struct CAPendingPickJob<Token: Hashable & Sendable>: Sendable {
    package let token: Token
    package let job: CAJob
    package let excludedSlot: CASlotRef?

    package init(token: Token, job: CAJob, excludedSlot: CASlotRef? = nil) {
        self.token = token
        self.job = job
        self.excludedSlot = excludedSlot
    }
}

extension CAPendingPickJob: Equatable where Token: Equatable {}

package enum CAReadyPolicy: Sendable {
    case readyNowOnly
    case includeFutureReady
}

package struct CAPick<Token: Hashable & Sendable>: Sendable {
    package let token: Token
    package let slot: CASlotRef
    package let score: CAScore
    package let excludedSlotWasCleared: Bool

    package init(
        token: Token,
        slot: CASlotRef,
        score: CAScore,
        excludedSlotWasCleared: Bool
    ) {
        self.token = token
        self.slot = slot
        self.score = score
        self.excludedSlotWasCleared = excludedSlotWasCleared
    }
}

extension CAPick: Equatable where Token: Equatable {}

package struct CAPickResult<Token: Hashable & Sendable>: Sendable {
    package let picks: [CAPick<Token>]
    package let projectedMachines: [CAMachine]
    package let clearedExcludedTokens: [Token]

    package init(
        picks: [CAPick<Token>],
        projectedMachines: [CAMachine],
        clearedExcludedTokens: [Token]
    ) {
        self.picks = picks
        self.projectedMachines = projectedMachines
        self.clearedExcludedTokens = clearedExcludedTokens
    }
}

extension CAPickResult: Equatable where Token: Equatable {}

package struct CATwoStagePickResult<Token: Hashable & Sendable>: Sendable {
    package let readyNowPicks: [CAPick<Token>]
    package let reservationPicks: [CAPick<Token>]
    package let projectedMachines: [CAMachine]
    package let clearedExcludedTokens: [Token]

    package init(
        readyNowPicks: [CAPick<Token>],
        reservationPicks: [CAPick<Token>],
        projectedMachines: [CAMachine],
        clearedExcludedTokens: [Token]
    ) {
        self.readyNowPicks = readyNowPicks
        self.reservationPicks = reservationPicks
        self.projectedMachines = projectedMachines
        self.clearedExcludedTokens = clearedExcludedTokens
    }
}

extension CATwoStagePickResult: Equatable where Token: Equatable {}

package struct CADecisionOption: Sendable, Equatable {
    package let slot: CASlotRef
    package let score: CAScore

    package init(slot: CASlotRef, score: CAScore) {
        self.slot = slot
        self.score = score
    }
}

package enum CADecisionAction: Sendable, Equatable {
    case dispatchNow(slot: CASlotRef)
    case holdUntil(timeMS: CAMilliseconds, slot: CASlotRef)
}

package struct CAFutureAwareDecision: Sendable, Equatable {
    package let action: CADecisionAction
    package let score: CAScore
    package let bestReadyNowAlternative: CADecisionOption?

    package init(
        action: CADecisionAction,
        score: CAScore,
        bestReadyNowAlternative: CADecisionOption?
    ) {
        self.action = action
        self.score = score
        self.bestReadyNowAlternative = bestReadyNowAlternative
    }

    package var chosenSlot: CASlotRef {
        switch action {
        case .dispatchNow(let slot):
            slot
        case .holdUntil(_, let slot):
            slot
        }
    }

    package var predictedSlotReadyMS: CAMilliseconds {
        score.tReadySlotMS
    }

    package var predictedStartMS: CAMilliseconds {
        score.tStartMS
    }

    package var predictedDoneMS: CAMilliseconds {
        score.tDoneMS
    }
}

package struct CASolverTelemetry: Sendable, Equatable {
    package var nodesVisited: Int
    package var prunedByPickCount: Int
    package var prunedByMakespan: Int
    package var prunedByCompletionSum: Int
    package var incumbentUpdates: Int
    package var maxDepth: Int
    package var solverWallMS: Double

    package init(
        nodesVisited: Int = 0,
        prunedByPickCount: Int = 0,
        prunedByMakespan: Int = 0,
        prunedByCompletionSum: Int = 0,
        incumbentUpdates: Int = 0,
        maxDepth: Int = 0,
        solverWallMS: Double = 0
    ) {
        self.nodesVisited = nodesVisited
        self.prunedByPickCount = prunedByPickCount
        self.prunedByMakespan = prunedByMakespan
        self.prunedByCompletionSum = prunedByCompletionSum
        self.incumbentUpdates = incumbentUpdates
        self.maxDepth = maxDepth
        self.solverWallMS = solverWallMS
    }
}

package enum ComplexityAwareScheduler {
    package static let reservationMinimumBenefitMS: CAMilliseconds = 1

    private final class SolverTelemetryStore: @unchecked Sendable {
        private let lock = NSLock()
        private var value = CASolverTelemetry()

        func load() -> CASolverTelemetry {
            lock.lock()
            defer { lock.unlock() }
            return value
        }

        func store(_ telemetry: CASolverTelemetry) {
            lock.lock()
            value = telemetry
            lock.unlock()
        }
    }

    private static let solverTelemetryStore = SolverTelemetryStore()

    private struct BatchChoice<Token: Hashable & Sendable> {
        let pendingIndex: Int
        let slot: CASlotRef
        let token: Token
        let scored: CAScoredCandidate
        let excludedSlotWasCleared: Bool
    }

    private struct BatchPlan<Token: Hashable & Sendable> {
        let picks: [BatchChoice<Token>]
        let projectedMachines: [CAMachine]
    }

    private struct ScoredSlotCandidate {
        let slot: CASlotRef
        let scored: CAScoredCandidate
    }

    package static func frameCount(durationSeconds: Double, nominalFrameRate: Double?) -> Double {
        let frameRate: Double
        if let nominalFrameRate, nominalFrameRate > 0 {
            frameRate = nominalFrameRate
        } else {
            frameRate = 24
        }
        return max(durationSeconds, 0) * frameRate
    }

    package static func activeSlots(on machine: CAMachine, at startMS: CAMilliseconds) -> Int {
        var count = 0
        for slot in machine.slots {
            if !slot.isDown && slot.readyAtMS > startMS {
                count += 1
            }
        }
        return count
    }

    package static func degradationFactor(for machine: CAMachine, concurrency: Int) -> Double {
        resolvedDegradation(for: machine, concurrency: concurrency).factor
    }

    package static func score(candidate: CACandidate, nowMS: CAMilliseconds) -> CAScore? {
        guard candidate.slot.isDown == false else {
            return nil
        }

        let tReadySlotMS = capacityConstrainedSlotReadyMS(
            on: candidate.machine,
            slotReadyAtMS: candidate.slot.readyAtMS
        )
        let tReadyInputMS = nowMS + candidate.machine.txInMS
        let tStartMS = max(tReadySlotMS, tReadyInputMS)

        let activeSlotsAtStart = activeSlots(on: candidate.machine, at: tStartMS)
        let requestedConcurrency = activeSlotsAtStart + 1
        let degradation = resolvedDegradation(for: candidate.machine, concurrency: requestedConcurrency)

        let variableRunMS = candidate.job.frameCount * candidate.machine.msPerFrameC1 * degradation.factor
        let concurrencyPenaltyMS = activeSlotsAtStart > 0 ? Double(activeSlotsAtStart * activeSlotsAtStart) * variableRunMS * 0.10 : 0
        let runMS = candidate.machine.fixedOverheadMS + variableRunMS + concurrencyPenaltyMS
        let tDoneMS = tStartMS + runMS + candidate.machine.txOutMS + candidate.machine.publishOverheadMS

        return CAScore(
            tReadySlotMS: tReadySlotMS,
            tReadyInputMS: tReadyInputMS,
            tStartMS: tStartMS,
            activeSlotsAtStart: activeSlotsAtStart,
            clampedConcurrency: degradation.clampedConcurrency,
            degradationFactor: degradation.factor,
            runMS: runMS,
            tDoneMS: tDoneMS
        )
    }

    package static func tieBreakLessThan(_ lhs: CAScoredCandidate, _ rhs: CAScoredCandidate) -> Bool {
        if lhs.score.tDoneMS != rhs.score.tDoneMS {
            return lhs.score.tDoneMS < rhs.score.tDoneMS
        }
        if lhs.score.runMS != rhs.score.runMS {
            return lhs.score.runMS < rhs.score.runMS
        }
        if lhs.candidate.job.arrivalAtMS != rhs.candidate.job.arrivalAtMS {
            return lhs.candidate.job.arrivalAtMS < rhs.candidate.job.arrivalAtMS
        }
        if lhs.candidate.job.enqueueOrder != rhs.candidate.job.enqueueOrder {
            return lhs.candidate.job.enqueueOrder < rhs.candidate.job.enqueueOrder
        }
        if lhs.candidate.machine.id != rhs.candidate.machine.id {
            return lhs.candidate.machine.id < rhs.candidate.machine.id
        }
        if lhs.candidate.slot.id != rhs.candidate.slot.id {
            return lhs.candidate.slot.id < rhs.candidate.slot.id
        }
        if lhs.candidate.job.id != rhs.candidate.job.id {
            return lhs.candidate.job.id < rhs.candidate.job.id
        }
        return false
    }

    package static func batchObjectiveLessThan(
        _ lhs: [CAScoredCandidate],
        _ rhs: [CAScoredCandidate],
        baselineTailMS: CAMilliseconds = 0
    ) -> Bool {
        let lhsEffectiveMakespan = effectiveMakespan(for: lhs, baselineTailMS: baselineTailMS)
        let rhsEffectiveMakespan = effectiveMakespan(for: rhs, baselineTailMS: baselineTailMS)
        if lhsEffectiveMakespan != rhsEffectiveMakespan {
            return lhsEffectiveMakespan < rhsEffectiveMakespan
        }

        if lhsEffectiveMakespan == baselineTailMS,
           rhsEffectiveMakespan == baselineTailMS,
           baselineTailMS > 0 {
            let lhsFrameSum = scheduledFrameSum(for: lhs)
            let rhsFrameSum = scheduledFrameSum(for: rhs)
            if lhsFrameSum != rhsFrameSum {
                return lhsFrameSum > rhsFrameSum
            }
        }

        let lhsCompletionSum = projectedCompletionSum(for: lhs)
        let rhsCompletionSum = projectedCompletionSum(for: rhs)
        if lhsCompletionSum != rhsCompletionSum {
            return lhsCompletionSum < rhsCompletionSum
        }

        for index in 0..<min(lhs.count, rhs.count) {
            if tieBreakLessThan(lhs[index], rhs[index]) {
                return true
            }
            if tieBreakLessThan(rhs[index], lhs[index]) {
                return false
            }
        }

        return false
    }

    package static func selectBestCandidate(_ candidates: [CACandidate], nowMS: CAMilliseconds) -> CAScoredCandidate? {
        var best: CAScoredCandidate?

        for candidate in candidates {
            guard let score = score(candidate: candidate, nowMS: nowMS) else {
                continue
            }
            let scored = CAScoredCandidate(candidate: candidate, score: score)
            if let currentBest = best, tieBreakLessThan(scored, currentBest) {
                best = scored
            } else if best == nil {
                best = scored
            }
        }

        return best
    }

    package static func futureAwareDecision(
        for job: CAJob,
        machines: [CAMachine],
        nowMS: CAMilliseconds
    ) -> CAFutureAwareDecision? {
        guard let bestFutureReady = selectBestCandidate(
            job: job,
            machines: machines,
            nowMS: nowMS,
            readyPolicy: .includeFutureReady
        ) else {
            return nil
        }

        let bestReadyNow = selectBestCandidate(
            job: job,
            machines: machines,
            nowMS: nowMS,
            readyPolicy: .readyNowOnly
        )

        let chosen: ScoredSlotCandidate
        if let bestReadyNow,
           bestFutureReady.scored.score.tReadySlotMS > nowMS,
           bestFutureReady.scored.score.tDoneMS < bestReadyNow.scored.score.tDoneMS {
            chosen = bestFutureReady
        } else if let bestReadyNow {
            chosen = bestReadyNow
        } else {
            chosen = bestFutureReady
        }

        let action: CADecisionAction
        if chosen.scored.score.tReadySlotMS <= nowMS {
            action = .dispatchNow(slot: chosen.slot)
        } else {
            action = .holdUntil(timeMS: chosen.scored.score.tReadySlotMS, slot: chosen.slot)
        }

        return CAFutureAwareDecision(
            action: action,
            score: chosen.scored.score,
            bestReadyNowAlternative: bestReadyNow.map {
                CADecisionOption(slot: $0.slot, score: $0.scored.score)
            }
        )
    }

    package static func pickBatch<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy
    ) -> CAPickResult<Token> {
        pickBatchWithTelemetry(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxCount,
            readyPolicy: readyPolicy
        ).result
    }

    package static func pickTwoStageBatch<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxReadyNowCount: Int
    ) -> CATwoStagePickResult<Token> {
        pickTwoStageBatchWithTelemetry(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxReadyNowCount: maxReadyNowCount
        ).result
    }

    package static var lastSolverTelemetry: CASolverTelemetry {
        solverTelemetryStore.load()
    }

    package static func pickBatchWithTelemetry<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy
    ) -> (result: CAPickResult<Token>, telemetry: CASolverTelemetry) {
        guard maxCount > 0, !pendingJobs.isEmpty, !machines.isEmpty else {
            let telemetry = CASolverTelemetry()
            storeSolverTelemetry(telemetry)
            return (
                CAPickResult(
                    picks: [],
                    projectedMachines: machines,
                    clearedExcludedTokens: []
                ),
                telemetry
            )
        }

        let (bestPlan, telemetry) = resolveBestBatchPlanWithTelemetry(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxCount,
            readyPolicy: readyPolicy
        )
        let result = buildPickResult(from: bestPlan)
        storeSolverTelemetry(telemetry)

        return (result, telemetry)
    }

    package static func pickTwoStageBatchWithTelemetry<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxReadyNowCount: Int
    ) -> (result: CATwoStagePickResult<Token>, telemetry: CASolverTelemetry) {
        guard maxReadyNowCount > 0, !pendingJobs.isEmpty, !machines.isEmpty else {
            let telemetry = CASolverTelemetry()
            storeSolverTelemetry(telemetry)
            return (
                CATwoStagePickResult(
                    readyNowPicks: [],
                    reservationPicks: [],
                    projectedMachines: machines,
                    clearedExcludedTokens: []
                ),
                telemetry
            )
        }

        let baselineReadyNowInvocation = pickBatchWithTelemetry(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxReadyNowCount,
            readyPolicy: .readyNowOnly
        )
        var combined = baselineReadyNowInvocation.telemetry

        let baselineCandidate = buildTwoStageCandidateWithTelemetry(
            readyNowResult: baselineReadyNowInvocation.result,
            pendingJobs: pendingJobs,
            nowMS: nowMS,
            maxReadyNowCount: maxReadyNowCount
        )
        combined = combinedTelemetry(combined, baselineCandidate.telemetry)
        storeSolverTelemetry(combined)
        return (baselineCandidate.result, combined)
    }

    package static func pickBatchOracle<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy
    ) -> CAPickResult<Token> {
        guard maxCount > 0, !pendingJobs.isEmpty, !machines.isEmpty else {
            storeSolverTelemetry(CASolverTelemetry())
            return CAPickResult(
                picks: [],
                projectedMachines: machines,
                clearedExcludedTokens: []
            )
        }

        let (bestPlan, telemetry) = resolveBestBatchPlanOracleWithTelemetry(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxCount,
            readyPolicy: readyPolicy
        )
        storeSolverTelemetry(telemetry)

        return buildPickResult(from: bestPlan)
    }

    private static func buildPickResult<Token: Hashable & Sendable>(
        from plan: BatchPlan<Token>
    ) -> CAPickResult<Token> {
        var picks: [CAPick<Token>] = []
        picks.reserveCapacity(plan.picks.count)
        var clearedExcludedTokens: [Token] = []
        clearedExcludedTokens.reserveCapacity(plan.picks.count)
        for pick in plan.picks {
            picks.append(
                CAPick(
                    token: pick.token,
                    slot: pick.slot,
                    score: pick.scored.score,
                    excludedSlotWasCleared: pick.excludedSlotWasCleared
                )
            )
            if pick.excludedSlotWasCleared {
                clearedExcludedTokens.append(pick.token)
            }
        }

        return CAPickResult(
            picks: picks,
            projectedMachines: plan.projectedMachines,
            clearedExcludedTokens: clearedExcludedTokens
        )
    }

    private static func buildTwoStageCandidateWithTelemetry<Token: Hashable & Sendable>(
        readyNowResult: CAPickResult<Token>,
        pendingJobs: [CAPendingPickJob<Token>],
        nowMS: CAMilliseconds,
        maxReadyNowCount: Int
    ) -> (result: CATwoStagePickResult<Token>, telemetry: CASolverTelemetry) {
        let readyNowTokens = Set(readyNowResult.picks.map(\.token))
        let remainingPendingJobs = pendingJobs.filter { pending in
            !readyNowTokens.contains(pending.token)
        }
        let reservationCapacity = min(remainingPendingJobs.count, maxReadyNowCount)

        guard reservationCapacity > 0, !remainingPendingJobs.isEmpty else {
            return (
                CATwoStagePickResult(
                    readyNowPicks: readyNowResult.picks,
                    reservationPicks: [],
                    projectedMachines: readyNowResult.projectedMachines,
                    clearedExcludedTokens: readyNowResult.clearedExcludedTokens
                ),
                CASolverTelemetry()
            )
        }

        let remainingPendingJobsByToken = Dictionary(
            uniqueKeysWithValues: remainingPendingJobs.map { ($0.token, $0) }
        )

        // Two-pass stage 2: priority pass reserves the longest jobs for fast
        // future slots (reducing the makespan tail), fill pass adds normal SPT
        // picks to maintain throughput for short jobs.
        let priorityCount = min(3, reservationCapacity)

        if remainingPendingJobs.count > reservationCapacity {
            let priorityJobs = Array(
                remainingPendingJobs
                    .sorted { $0.job.frameCount > $1.job.frameCount }
                    .prefix(priorityCount)
            )

            let priorityInvocation = pickBatchWithTelemetry(
                pendingJobs: priorityJobs,
                machines: readyNowResult.projectedMachines,
                nowMS: nowMS,
                maxCount: priorityCount,
                readyPolicy: .includeFutureReady
            )
            var combined = priorityInvocation.telemetry

            var allReservationPicks: [CAPick<Token>] = []
            var projectedMachines = readyNowResult.projectedMachines
            var clearedExcludedTokens = readyNowResult.clearedExcludedTokens
            var priorityPickedTokens = Set<Token>()

            for pick in priorityInvocation.result.picks {
                guard pick.score.tReadySlotMS > nowMS else { continue }
                if let pending = remainingPendingJobsByToken[pick.token],
                   let readyNowDoneMS = selectBestCandidate(
                        job: pending.job,
                        machines: projectedMachines,
                        nowMS: nowMS,
                        readyPolicy: .readyNowOnly
                   )?.scored.score.tDoneMS,
                   readyNowDoneMS - pick.score.tDoneMS < reservationMinimumBenefitMS {
                    continue
                }
                allReservationPicks.append(pick)
                priorityPickedTokens.insert(pick.token)
                reserveProjectedSlot(
                    machines: &projectedMachines,
                    slot: pick.slot,
                    doneAtMS: pick.score.tDoneMS
                )
                if pick.excludedSlotWasCleared {
                    appendUniqueToken(pick.token, to: &clearedExcludedTokens)
                }
            }

            let fillCapacity = reservationCapacity - allReservationPicks.count
            if fillCapacity > 0 {
                let fillJobs = remainingPendingJobs.filter {
                    !priorityPickedTokens.contains($0.token)
                }
                let fillInvocation = pickBatchWithTelemetry(
                    pendingJobs: fillJobs,
                    machines: projectedMachines,
                    nowMS: nowMS,
                    maxCount: fillCapacity,
                    readyPolicy: .includeFutureReady
                )
                combined = combinedTelemetry(combined, fillInvocation.telemetry)

                for pick in fillInvocation.result.picks {
                    guard pick.score.tReadySlotMS > nowMS else { continue }
                    if let pending = remainingPendingJobsByToken[pick.token],
                       let readyNowDoneMS = selectBestCandidate(
                            job: pending.job,
                            machines: projectedMachines,
                            nowMS: nowMS,
                            readyPolicy: .readyNowOnly
                       )?.scored.score.tDoneMS,
                       readyNowDoneMS - pick.score.tDoneMS < reservationMinimumBenefitMS {
                        continue
                    }
                    allReservationPicks.append(pick)
                    reserveProjectedSlot(
                        machines: &projectedMachines,
                        slot: pick.slot,
                        doneAtMS: pick.score.tDoneMS
                    )
                    if pick.excludedSlotWasCleared {
                        appendUniqueToken(pick.token, to: &clearedExcludedTokens)
                    }
                }
            }

            return (
                CATwoStagePickResult(
                    readyNowPicks: readyNowResult.picks,
                    reservationPicks: allReservationPicks,
                    projectedMachines: projectedMachines,
                    clearedExcludedTokens: clearedExcludedTokens
                ),
                combined
            )
        }

        let reservationInvocation = pickBatchWithTelemetry(
            pendingJobs: remainingPendingJobs,
            machines: readyNowResult.projectedMachines,
            nowMS: nowMS,
            maxCount: reservationCapacity,
            readyPolicy: .includeFutureReady
        )

        let result = acceptedTwoStageCandidate(
            readyNowResult: readyNowResult,
            reservationResult: reservationInvocation.result,
            remainingPendingJobsByToken: remainingPendingJobsByToken,
            nowMS: nowMS
        )
        return (
            result,
            reservationInvocation.telemetry
        )
    }

    private static func acceptedTwoStageCandidate<Token: Hashable & Sendable>(
        readyNowResult: CAPickResult<Token>,
        reservationResult: CAPickResult<Token>,
        remainingPendingJobsByToken: [Token: CAPendingPickJob<Token>],
        nowMS: CAMilliseconds
    ) -> CATwoStagePickResult<Token> {
        var acceptedReservationPicks: [CAPick<Token>] = []
        acceptedReservationPicks.reserveCapacity(reservationResult.picks.count)
        var projectedMachines = readyNowResult.projectedMachines
        var clearedExcludedTokens = readyNowResult.clearedExcludedTokens

        for pick in reservationResult.picks {
            guard pick.score.tReadySlotMS > nowMS else {
                continue
            }
            if let pending = remainingPendingJobsByToken[pick.token],
               let readyNowDoneMS = selectBestCandidate(
                    job: pending.job,
                    machines: projectedMachines,
                    nowMS: nowMS,
                    readyPolicy: .readyNowOnly
               )?.scored.score.tDoneMS,
               readyNowDoneMS - pick.score.tDoneMS < reservationMinimumBenefitMS {
                continue
            }

            acceptedReservationPicks.append(pick)
            reserveProjectedSlot(
                machines: &projectedMachines,
                slot: pick.slot,
                doneAtMS: pick.score.tDoneMS
            )
            if pick.excludedSlotWasCleared {
                appendUniqueToken(pick.token, to: &clearedExcludedTokens)
            }
        }

        return CATwoStagePickResult(
            readyNowPicks: readyNowResult.picks,
            reservationPicks: acceptedReservationPicks,
            projectedMachines: projectedMachines,
            clearedExcludedTokens: clearedExcludedTokens
        )
    }

    private static func storeSolverTelemetry(_ telemetry: CASolverTelemetry) {
        solverTelemetryStore.store(telemetry)
    }

    private static func resolveBestBatchPlanWithTelemetry<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy
    ) -> (plan: BatchPlan<Token>, telemetry: CASolverTelemetry) {
        var telemetry = CASolverTelemetry()
        let startTime = ContinuousClock.now
        let baselineTailMS = committedTailMS(for: machines)

        let plan = resolveBestBatchPlanBnB(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxCount,
            readyPolicy: readyPolicy,
            baselineTailMS: baselineTailMS,
            telemetry: &telemetry
        )

        let elapsed = ContinuousClock.now - startTime
        telemetry.solverWallMS = Double(elapsed.components.seconds) * 1000.0
            + Double(elapsed.components.attoseconds) / 1_000_000_000_000_000.0

        return (plan, telemetry)
    }

    private static func resolveBestBatchPlanBnB<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy,
        baselineTailMS: CAMilliseconds,
        telemetry: inout CASolverTelemetry
    ) -> BatchPlan<Token> {
        guard maxCount > 0, !pendingJobs.isEmpty else {
            return BatchPlan(picks: [], projectedMachines: machines)
        }

        let choices = enumerateBatchChoices(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            readyPolicy: readyPolicy
        )
        guard !choices.isEmpty else {
            return BatchPlan(picks: [], projectedMachines: machines)
        }

        var incumbent = greedySeedPlan(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxCount,
            readyPolicy: readyPolicy
        )
        telemetry.incumbentUpdates += 1

        bnbSearch(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxCount,
            readyPolicy: readyPolicy,
            baselineTailMS: baselineTailMS,
            picksAccum: [],
            depth: 0,
            currentMakespan: 0,
            currentCompletionSum: 0,
            currentFrameSum: 0,
            incumbent: &incumbent,
            telemetry: &telemetry
        )

        return incumbent
    }

    private static func greedySeedPlan<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy
    ) -> BatchPlan<Token> {
        var remaining = pendingJobs
        var currentMachines = machines
        var picks: [BatchChoice<Token>] = []
        picks.reserveCapacity(maxCount)

        for _ in 0..<maxCount {
            guard !remaining.isEmpty else { break }
            let choices = enumerateBatchChoices(
                pendingJobs: remaining,
                machines: currentMachines,
                nowMS: nowMS,
                readyPolicy: readyPolicy
            )
            guard !choices.isEmpty else { break }

            var best = choices[0]
            for i in 1..<choices.count {
                if batchChoiceLessThan(choices[i], best) {
                    best = choices[i]
                }
            }

            picks.append(best)
            reserveProjectedSlot(
                machines: &currentMachines,
                slot: best.slot,
                doneAtMS: best.scored.score.tDoneMS
            )

            let originalPendingIndex = best.pendingIndex
            remaining.remove(at: originalPendingIndex)
        }

        return BatchPlan(picks: picks, projectedMachines: currentMachines)
    }

    private struct SlotSymmetryKey: Hashable {
        let pendingIndex: Int
        let machineIndex: Int
        let readyAtMS: Double
        let isDown: Bool
        let excludedSlotWasCleared: Bool
        let excludedByIndices: [Int]
    }

    private static func deduplicateSymmetricSlots<Token: Hashable & Sendable>(
        _ choices: [BatchChoice<Token>],
        pendingJobs: [CAPendingPickJob<Token>]
    ) -> [BatchChoice<Token>] {
        var bestByKey: [SlotSymmetryKey: BatchChoice<Token>] = [:]
        bestByKey.reserveCapacity(choices.count)

        for choice in choices {
            var excludedBy: [Int] = []
            for (idx, pending) in pendingJobs.enumerated() {
                if idx == choice.pendingIndex { continue }
                if let excluded = pending.excludedSlot,
                   excluded.machineIndex == choice.slot.machineIndex,
                   excluded.slotIndex == choice.slot.slotIndex {
                    excludedBy.append(idx)
                }
            }
            let key = SlotSymmetryKey(
                pendingIndex: choice.pendingIndex,
                machineIndex: choice.slot.machineIndex,
                readyAtMS: choice.scored.candidate.slot.readyAtMS,
                isDown: choice.scored.candidate.slot.isDown,
                excludedSlotWasCleared: choice.excludedSlotWasCleared,
                excludedByIndices: excludedBy
            )
            if let existing = bestByKey[key] {
                if batchChoiceLessThan(choice, existing) {
                    bestByKey[key] = choice
                }
            } else {
                bestByKey[key] = choice
            }
        }

        return Array(bestByKey.values)
    }

    private static func bnbSearch<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy,
        baselineTailMS: CAMilliseconds,
        picksAccum: [BatchChoice<Token>],
        depth: Int,
        currentMakespan: CAMilliseconds,
        currentCompletionSum: CAMilliseconds,
        currentFrameSum: Double,
        incumbent: inout BatchPlan<Token>,
        telemetry: inout CASolverTelemetry
    ) {
        telemetry.nodesVisited += 1
        if depth > telemetry.maxDepth {
            telemetry.maxDepth = depth
        }

        guard maxCount > 0, !pendingJobs.isEmpty else {
            let leaf = BatchPlan(picks: picksAccum, projectedMachines: machines)
            if batchPlanLessThan(leaf, incumbent, baselineTailMS: baselineTailMS) {
                incumbent = leaf
                telemetry.incumbentUpdates += 1
            }
            return
        }

        let choices = enumerateBatchChoices(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            readyPolicy: readyPolicy
        )

        if choices.isEmpty {
            let leaf = BatchPlan(picks: picksAccum, projectedMachines: machines)
            if batchPlanLessThan(leaf, incumbent, baselineTailMS: baselineTailMS) {
                incumbent = leaf
                telemetry.incumbentUpdates += 1
            }
            return
        }

        let viableJobCount = Set(choices.map(\.pendingIndex)).count
        let maxPossiblePicks = picksAccum.count + min(maxCount, viableJobCount)
        if maxPossiblePicks < incumbent.picks.count {
            telemetry.prunedByPickCount += 1
            return
        }

        let incumbentScored = incumbent.picks.map(\.scored)
        let incumbentMakespan = effectiveMakespan(for: incumbentScored, baselineTailMS: baselineTailMS)
        let incumbentCompletionSum = projectedCompletionSum(for: incumbentScored)
        let incumbentFrameSum = scheduledFrameSum(for: incumbentScored)

        let remainingCapacity = min(maxCount, viableJobCount)

        var optimisticByJob: [Int: CAMilliseconds] = [:]
        optimisticByJob.reserveCapacity(viableJobCount)
        for choice in choices {
            let tDone = choice.scored.score.tDoneMS
            if let existing = optimisticByJob[choice.pendingIndex] {
                if tDone < existing {
                    optimisticByJob[choice.pendingIndex] = tDone
                }
            } else {
                optimisticByJob[choice.pendingIndex] = tDone
            }
        }

        var sortedOptimistic = Array(optimisticByJob.values)
        sortedOptimistic.sort()

        var optimisticFrameSumByJob: [Int: Double] = [:]
        optimisticFrameSumByJob.reserveCapacity(viableJobCount)
        for choice in choices {
            let frameCount = choice.scored.candidate.job.frameCount
            if let existing = optimisticFrameSumByJob[choice.pendingIndex] {
                if frameCount > existing {
                    optimisticFrameSumByJob[choice.pendingIndex] = frameCount
                }
            } else {
                optimisticFrameSumByJob[choice.pendingIndex] = frameCount
            }
        }

        var sortedOptimisticFrames = Array(optimisticFrameSumByJob.values)
        sortedOptimisticFrames.sort(by: >)

        if maxPossiblePicks == incumbent.picks.count {
            let rthOptimistic = sortedOptimistic[min(remainingCapacity - 1, sortedOptimistic.count - 1)]
            let lowerBoundMakespan = max(baselineTailMS, currentMakespan, rthOptimistic)

            if lowerBoundMakespan > incumbentMakespan {
                telemetry.prunedByMakespan += 1
                return
            }

            if lowerBoundMakespan == incumbentMakespan {
                if lowerBoundMakespan == baselineTailMS && baselineTailMS > 0 {
                    var upperBoundFrameSum = currentFrameSum
                    for i in 0..<min(remainingCapacity, sortedOptimisticFrames.count) {
                        upperBoundFrameSum += sortedOptimisticFrames[i]
                    }
                    if upperBoundFrameSum < incumbentFrameSum {
                        telemetry.prunedByCompletionSum += 1
                        return
                    }
                    if upperBoundFrameSum == incumbentFrameSum {
                        var lowerBoundCompletionSum = currentCompletionSum
                        for i in 0..<min(remainingCapacity, sortedOptimistic.count) {
                            lowerBoundCompletionSum += sortedOptimistic[i]
                        }
                        if lowerBoundCompletionSum > incumbentCompletionSum {
                            telemetry.prunedByCompletionSum += 1
                            return
                        }
                    }
                } else {
                    var lowerBoundCompletionSum = currentCompletionSum
                    for i in 0..<min(remainingCapacity, sortedOptimistic.count) {
                        lowerBoundCompletionSum += sortedOptimistic[i]
                    }
                    if lowerBoundCompletionSum > incumbentCompletionSum {
                        telemetry.prunedByCompletionSum += 1
                        return
                    }
                }
            }
        }

        let deduped = deduplicateSymmetricSlots(choices, pendingJobs: pendingJobs)

        var sorted = deduped
        sorted.sort { batchChoiceLessThan($0, $1) }

        for choice in sorted {
            var projectedMachines = machines
            reserveProjectedSlot(
                machines: &projectedMachines,
                slot: choice.slot,
                doneAtMS: choice.scored.score.tDoneMS
            )

            var remaining = pendingJobs
            remaining.remove(at: choice.pendingIndex)

            var nextPicks = picksAccum
            nextPicks.append(choice)

            let nextMakespan = max(currentMakespan, choice.scored.score.tDoneMS)
            let nextCompletionSum = currentCompletionSum + choice.scored.score.tDoneMS
            let nextFrameSum = currentFrameSum + choice.scored.candidate.job.frameCount

            bnbSearch(
                pendingJobs: remaining,
                machines: projectedMachines,
                nowMS: nowMS,
                maxCount: maxCount - 1,
                readyPolicy: readyPolicy,
                baselineTailMS: baselineTailMS,
                picksAccum: nextPicks,
                depth: depth + 1,
                currentMakespan: nextMakespan,
                currentCompletionSum: nextCompletionSum,
                currentFrameSum: nextFrameSum,
                incumbent: &incumbent,
                telemetry: &telemetry
            )
        }
    }

    private static func resolveBestBatchPlanOracleWithTelemetry<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy
    ) -> (plan: BatchPlan<Token>, telemetry: CASolverTelemetry) {
        var telemetry = CASolverTelemetry()
        let startTime = ContinuousClock.now
        let baselineTailMS = committedTailMS(for: machines)

        let plan = resolveBestBatchPlanOracle(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: maxCount,
            readyPolicy: readyPolicy,
            baselineTailMS: baselineTailMS,
            depth: 0,
            telemetry: &telemetry
        )

        let elapsed = ContinuousClock.now - startTime
        telemetry.solverWallMS = Double(elapsed.components.seconds) * 1000.0
            + Double(elapsed.components.attoseconds) / 1_000_000_000_000_000.0
        return (plan, telemetry)
    }

    private static func resolveBestBatchPlanOracle<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        maxCount: Int,
        readyPolicy: CAReadyPolicy,
        baselineTailMS: CAMilliseconds,
        depth: Int,
        telemetry: inout CASolverTelemetry
    ) -> BatchPlan<Token> {
        telemetry.nodesVisited += 1
        if depth > telemetry.maxDepth {
            telemetry.maxDepth = depth
        }

        guard maxCount > 0, !pendingJobs.isEmpty else {
            return BatchPlan(picks: [], projectedMachines: machines)
        }

        let choices = enumerateBatchChoices(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            readyPolicy: readyPolicy
        )
        guard !choices.isEmpty else {
            return BatchPlan(picks: [], projectedMachines: machines)
        }

        var bestPlan: BatchPlan<Token>?
        for choice in choices {
            var projectedMachines = machines
            reserveProjectedSlot(
                machines: &projectedMachines,
                slot: choice.slot,
                doneAtMS: choice.scored.score.tDoneMS
            )

            var remaining = pendingJobs
            remaining.remove(at: choice.pendingIndex)

            let suffix = resolveBestBatchPlanOracle(
                pendingJobs: remaining,
                machines: projectedMachines,
                nowMS: nowMS,
                maxCount: maxCount - 1,
                readyPolicy: readyPolicy,
                baselineTailMS: baselineTailMS,
                depth: depth + 1,
                telemetry: &telemetry
            )

            var picks = [choice]
            picks.reserveCapacity(1 + suffix.picks.count)
            picks.append(contentsOf: suffix.picks)
            let plan = BatchPlan(picks: picks, projectedMachines: suffix.projectedMachines)

            if let currentBest = bestPlan {
                if batchPlanLessThan(plan, currentBest, baselineTailMS: baselineTailMS) {
                    bestPlan = plan
                    telemetry.incumbentUpdates += 1
                }
            } else {
                bestPlan = plan
                telemetry.incumbentUpdates += 1
            }
        }

        return bestPlan ?? BatchPlan(picks: [], projectedMachines: machines)
    }

    private static func enumerateBatchChoices<Token: Hashable & Sendable>(
        pendingJobs: [CAPendingPickJob<Token>],
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        readyPolicy: CAReadyPolicy
    ) -> [BatchChoice<Token>] {
        var choices: [BatchChoice<Token>] = []
        for pendingIndex in pendingJobs.indices {
            let pending = pendingJobs[pendingIndex]
            let preferredChoices = enumerateBatchChoices(
                pendingIndex: pendingIndex,
                pending: pending,
                machines: machines,
                nowMS: nowMS,
                readyPolicy: readyPolicy,
                excludedSlot: pending.excludedSlot,
                excludedSlotWasCleared: false
            )
            if !preferredChoices.isEmpty {
                choices.append(contentsOf: preferredChoices)
                continue
            }
            if pending.excludedSlot != nil {
                choices.append(
                    contentsOf: enumerateBatchChoices(
                        pendingIndex: pendingIndex,
                        pending: pending,
                        machines: machines,
                        nowMS: nowMS,
                        readyPolicy: readyPolicy,
                        excludedSlot: nil,
                        excludedSlotWasCleared: true
                    )
                )
            }
        }
        return choices
    }

    private static func enumerateBatchChoices<Token: Hashable & Sendable>(
        pendingIndex: Int,
        pending: CAPendingPickJob<Token>,
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        readyPolicy: CAReadyPolicy,
        excludedSlot: CASlotRef?,
        excludedSlotWasCleared: Bool
    ) -> [BatchChoice<Token>] {
        var choices: [BatchChoice<Token>] = []
        for machineIndex in machines.indices {
            let machine = machines[machineIndex]
            for slotIndex in machine.slots.indices {
                let slot = machine.slots[slotIndex]
                guard slot.isDown == false else { continue }
                guard isSlotReady(slot, nowMS: nowMS, policy: readyPolicy) else { continue }
                if let excludedSlot,
                   excludedSlot.machineIndex == machineIndex,
                   excludedSlot.slotIndex == slotIndex {
                    continue
                }

                let candidate = CACandidate(
                    job: pending.job,
                    machine: machine,
                    slot: slot
                )
                guard let score = score(candidate: candidate, nowMS: nowMS) else {
                    continue
                }
                if readyPolicy == .readyNowOnly,
                   score.tReadySlotMS > nowMS {
                    continue
                }

                let scored = CAScoredCandidate(candidate: candidate, score: score)
                choices.append(
                    BatchChoice(
                        pendingIndex: pendingIndex,
                        slot: CASlotRef(machineIndex: machineIndex, slotIndex: slotIndex),
                        token: pending.token,
                        scored: scored,
                        excludedSlotWasCleared: excludedSlotWasCleared
                    )
                )
            }
        }

        return choices
    }

    private static func isSlotReady(_ slot: CASlot, nowMS: CAMilliseconds, policy: CAReadyPolicy) -> Bool {
        switch policy {
        case .readyNowOnly:
            return slot.readyAtMS <= nowMS
        case .includeFutureReady:
            return true
        }
    }

    private static func selectBestCandidate(
        job: CAJob,
        machines: [CAMachine],
        nowMS: CAMilliseconds,
        readyPolicy: CAReadyPolicy
    ) -> ScoredSlotCandidate? {
        var best: ScoredSlotCandidate?

        for machineIndex in machines.indices {
            let machine = machines[machineIndex]
            for slotIndex in machine.slots.indices {
                let slot = machine.slots[slotIndex]
                guard slot.isDown == false else { continue }
                guard isSlotReady(slot, nowMS: nowMS, policy: readyPolicy) else { continue }

                let candidate = CACandidate(job: job, machine: machine, slot: slot)
                guard let score = score(candidate: candidate, nowMS: nowMS) else {
                    continue
                }
                if readyPolicy == .readyNowOnly,
                   score.tReadySlotMS > nowMS {
                    continue
                }

                let scored = CAScoredCandidate(candidate: candidate, score: score)
                let resolved = ScoredSlotCandidate(
                    slot: CASlotRef(machineIndex: machineIndex, slotIndex: slotIndex),
                    scored: scored
                )
                if let currentBest = best {
                    if tieBreakLessThan(scored, currentBest.scored) {
                        best = resolved
                    }
                } else {
                    best = resolved
                }
            }
        }

        return best
    }

    private static func reserveProjectedSlot(
        machines: inout [CAMachine],
        slot: CASlotRef,
        doneAtMS: CAMilliseconds
    ) {
        guard machines.indices.contains(slot.machineIndex),
              machines[slot.machineIndex].slots.indices.contains(slot.slotIndex) else {
            return
        }
        let machine = machines[slot.machineIndex]
        let currentSlot = machine.slots[slot.slotIndex]
        let nextReadyAtMS = max(currentSlot.readyAtMS, doneAtMS)
        var updatedSlots = machine.slots
        updatedSlots[slot.slotIndex] = CASlot(
            id: currentSlot.id,
            readyAtMS: nextReadyAtMS,
            isDown: currentSlot.isDown
        )
        machines[slot.machineIndex] = CAMachine(
            id: machine.id,
            slots: updatedSlots,
            msPerFrameC1: machine.msPerFrameC1,
            fixedOverheadMS: machine.fixedOverheadMS,
            degradationCurve: machine.degradationCurve,
            txInMS: machine.txInMS,
            txOutMS: machine.txOutMS,
            publishOverheadMS: machine.publishOverheadMS,
            modeledConcurrencyCap: machine.modeledConcurrencyCap
        )
    }

    private static func batchPlanLessThan<Token: Hashable & Sendable>(
        _ lhs: BatchPlan<Token>,
        _ rhs: BatchPlan<Token>,
        baselineTailMS: CAMilliseconds = 0
    ) -> Bool {
        if lhs.picks.count != rhs.picks.count {
            return lhs.picks.count > rhs.picks.count
        }

        let lhsScored = lhs.picks.map(\.scored)
        let rhsScored = rhs.picks.map(\.scored)
        if batchObjectiveLessThan(lhsScored, rhsScored, baselineTailMS: baselineTailMS) {
            return true
        }
        if batchObjectiveLessThan(rhsScored, lhsScored, baselineTailMS: baselineTailMS) {
            return false
        }

        for index in lhs.picks.indices {
            let left = lhs.picks[index]
            let right = rhs.picks[index]
            if batchChoiceLessThan(left, right) {
                return true
            }
            if batchChoiceLessThan(right, left) {
                return false
            }
        }

        return false
    }

    private static func batchChoiceLessThan<Token: Hashable & Sendable>(
        _ lhs: BatchChoice<Token>,
        _ rhs: BatchChoice<Token>
    ) -> Bool {
        if tieBreakLessThan(lhs.scored, rhs.scored) {
            return true
        }
        if tieBreakLessThan(rhs.scored, lhs.scored) {
            return false
        }
        if lhs.excludedSlotWasCleared != rhs.excludedSlotWasCleared {
            return lhs.excludedSlotWasCleared == false
        }
        if lhs.slot.machineIndex != rhs.slot.machineIndex {
            return lhs.slot.machineIndex < rhs.slot.machineIndex
        }
        if lhs.slot.slotIndex != rhs.slot.slotIndex {
            return lhs.slot.slotIndex < rhs.slot.slotIndex
        }
        return false
    }

    private static func projectedMakespan(for picks: [CAScoredCandidate]) -> CAMilliseconds {
        var makespan = 0.0
        for pick in picks where pick.score.tDoneMS > makespan {
            makespan = pick.score.tDoneMS
        }
        return makespan
    }

    private static func effectiveMakespan(
        for picks: [CAScoredCandidate],
        baselineTailMS: CAMilliseconds
    ) -> CAMilliseconds {
        max(baselineTailMS, projectedMakespan(for: picks))
    }

    private static func projectedCompletionSum(for picks: [CAScoredCandidate]) -> CAMilliseconds {
        var total = 0.0
        for pick in picks {
            total += pick.score.tDoneMS
        }
        return total
    }

    private static func scheduledFrameSum(for picks: [CAScoredCandidate]) -> Double {
        var total = 0.0
        for pick in picks {
            total += pick.candidate.job.frameCount
        }
        return total
    }

    private static func combinedTelemetry(
        _ lhs: CASolverTelemetry,
        _ rhs: CASolverTelemetry
    ) -> CASolverTelemetry {
        CASolverTelemetry(
            nodesVisited: lhs.nodesVisited + rhs.nodesVisited,
            prunedByPickCount: lhs.prunedByPickCount + rhs.prunedByPickCount,
            prunedByMakespan: lhs.prunedByMakespan + rhs.prunedByMakespan,
            prunedByCompletionSum: lhs.prunedByCompletionSum + rhs.prunedByCompletionSum,
            incumbentUpdates: lhs.incumbentUpdates + rhs.incumbentUpdates,
            maxDepth: max(lhs.maxDepth, rhs.maxDepth),
            solverWallMS: lhs.solverWallMS + rhs.solverWallMS
        )
    }

    private static func appendUniqueToken<Token: Equatable>(
        _ token: Token,
        to tokens: inout [Token]
    ) {
        guard !tokens.contains(token) else { return }
        tokens.append(token)
    }

    private static func resolvedDegradation(for machine: CAMachine, concurrency: Int) -> (clampedConcurrency: Int, factor: Double) {
        CAProfileAndFallbackMath.resolvedDegradation(
            from: machine.degradationCurve,
            concurrency: concurrency
        )
    }

    private static func committedTailMS(for machines: [CAMachine]) -> CAMilliseconds {
        var tailMS = 0.0
        for machine in machines {
            for slot in machine.slots where !slot.isDown && slot.readyAtMS > tailMS {
                tailMS = slot.readyAtMS
            }
        }
        return tailMS
    }

    private static func capacityConstrainedSlotReadyMS(
        on machine: CAMachine,
        slotReadyAtMS: CAMilliseconds
    ) -> CAMilliseconds {
        guard let concurrencyCap = machine.modeledConcurrencyCap else {
            return slotReadyAtMS
        }

        var constrainedReadyAtMS = slotReadyAtMS
        while activeSlots(on: machine, at: constrainedReadyAtMS) >= concurrencyCap {
            guard let nextReadyAtMS = nextReadyAtMS(on: machine, after: constrainedReadyAtMS),
                  nextReadyAtMS > constrainedReadyAtMS else {
                break
            }
            constrainedReadyAtMS = nextReadyAtMS
        }
        return constrainedReadyAtMS
    }

    private static func nextReadyAtMS(
        on machine: CAMachine,
        after timeMS: CAMilliseconds
    ) -> CAMilliseconds? {
        var nextReadyAtMS: CAMilliseconds?
        for slot in machine.slots where !slot.isDown && slot.readyAtMS > timeMS {
            if let currentBest = nextReadyAtMS {
                if slot.readyAtMS < currentBest {
                    nextReadyAtMS = slot.readyAtMS
                }
            } else {
                nextReadyAtMS = slot.readyAtMS
            }
        }
        return nextReadyAtMS
    }
}
