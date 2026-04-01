import Foundation
import Testing
@testable import KikoMediaCore

@Suite("CA solver coverage")
struct CASolverCoverageTests {
    @Test("fairness-corrected batch plan assigns big and small jobs to different machines")
    func fairnessCorrectedBatchPlan_assignsBigAndSmallJobsToDifferentMachines() {
        let machines = [
            makeMachine(id: "fast", slotReadyAtMS: [0], msPerFrameC1: 1),
            makeMachine(id: "slow", slotReadyAtMS: [0], msPerFrameC1: 20),
        ]
        let firstBatch = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "small", job: .init(id: "small", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 1)),
                .init(token: "big", job: .init(id: "big", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100)),
            ],
            machines: machines,
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .includeFutureReady
        )

        #expect(firstBatch.picks.count == 2)

        let picksByToken = Dictionary(uniqueKeysWithValues: firstBatch.picks.map { ($0.token, $0.slot) })
        #expect(picksByToken["big"] == CASlotRef(machineIndex: 0, slotIndex: 0))
        #expect(picksByToken["small"] == CASlotRef(machineIndex: 1, slotIndex: 0))

        let secondBatch = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "follow-up", job: .init(id: "follow-up", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 50)),
            ],
            machines: firstBatch.projectedMachines,
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )

        #expect(secondBatch.picks.count == 1)
        #expect(secondBatch.picks[0].token == "follow-up")
        #expect(secondBatch.picks[0].score.tReadySlotMS > 0)
    }

    @Test("big jobs pick the faster machine when a fast slot is ready")
    func bigJobs_pickFasterMachine() {
        let machines = [
            makeMachine(id: "slow", slotReadyAtMS: [0], msPerFrameC1: 3),
            makeMachine(id: "fast", slotReadyAtMS: [0], msPerFrameC1: 1),
        ]
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                CAPendingPickJob(
                    token: "big",
                    job: CAJob(id: "big", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 300)
                ),
            ],
            machines: machines,
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )

        #expect(result.picks.count == 1)
        #expect(result.picks[0].token == "big")
        #expect(result.picks[0].slot == CASlotRef(machineIndex: 1, slotIndex: 0))
    }

    @Test("future-aware helper distinguishes the ready-now winner from the future-best winner")
    func futureAwareHelper_distinguishesReadyNowWinnerFromFutureBestWinner() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let machines = [
            makeMachine(id: "slow", slotReadyAtMS: [0], msPerFrameC1: 2.5),
            makeMachine(id: "fast", slotReadyAtMS: [10], msPerFrameC1: 1),
        ]

        let readyNow = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                CAPendingPickJob(token: "job", job: job),
            ],
            machines: machines,
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        let futureAware = ComplexityAwareScheduler.futureAwareDecision(
            for: job,
            machines: machines,
            nowMS: 0
        )

        #expect(readyNow.picks.count == 1)
        #expect(readyNow.picks[0].slot == CASlotRef(machineIndex: 0, slotIndex: 0))
        #expect(futureAware?.chosenSlot == CASlotRef(machineIndex: 1, slotIndex: 0))
        #expect(futureAware?.bestReadyNowAlternative?.slot == readyNow.picks[0].slot)

        switch futureAware?.action {
        case .some(.holdUntil(let timeMS, let slot)):
            #expect(timeMS == 10)
            #expect(slot == CASlotRef(machineIndex: 1, slotIndex: 0))
        default:
            Issue.record("expected future-aware helper to hold for the faster future slot")
        }
    }

    @Test("two-stage batch dispatches all ready-now jobs before considering reservations")
    func twoStageBatch_dispatchesAllReadyNowJobsBeforeConsideringReservations() {
        let machines = [
            makeMachine(id: "slow-a", slotReadyAtMS: [0], msPerFrameC1: 3),
            makeMachine(id: "slow-b", slotReadyAtMS: [0], msPerFrameC1: 4),
            makeMachine(id: "future-fast", slotReadyAtMS: [10], msPerFrameC1: 1),
        ]

        let result = ComplexityAwareScheduler.pickTwoStageBatch(
            pendingJobs: [
                .init(token: "job-0", job: .init(id: "job-0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)),
                .init(token: "job-1", job: .init(id: "job-1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 10)),
            ],
            machines: machines,
            nowMS: 0,
            maxReadyNowCount: 2
        )

        #expect(result.readyNowPicks.count == 2)
        #expect(result.reservationPicks.isEmpty)
        #expect(result.readyNowPicks.allSatisfy { $0.score.tReadySlotMS <= 0 })
    }

    @Test("two-stage batch creates reservations only after the ready-now budget is filled")
    func twoStageBatch_createsReservationsOnlyAfterReadyNowBudgetIsFilled() {
        let machines = [
            makeMachine(id: "slow-a", slotReadyAtMS: [0], msPerFrameC1: 5),
            makeMachine(id: "slow-b", slotReadyAtMS: [0], msPerFrameC1: 6),
            makeMachine(id: "future-fast", slotReadyAtMS: [15], msPerFrameC1: 1),
        ]

        let result = ComplexityAwareScheduler.pickTwoStageBatch(
            pendingJobs: [
                .init(token: "job-0", job: .init(id: "job-0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)),
                .init(token: "job-1", job: .init(id: "job-1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 10)),
                .init(token: "job-2", job: .init(id: "job-2", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 10)),
            ],
            machines: machines,
            nowMS: 0,
            maxReadyNowCount: 2
        )

        #expect(result.readyNowPicks.count == 2)
        #expect(result.reservationPicks.count == 1)
        #expect(result.readyNowPicks.allSatisfy { $0.score.tReadySlotMS <= 0 })
        #expect(result.reservationPicks.allSatisfy { $0.score.tReadySlotMS > 0 })

        let consumedTokens = Set(result.readyNowPicks.map(\.token) + result.reservationPicks.map(\.token))
        #expect(consumedTokens == ["job-0", "job-1", "job-2"])
    }

    @Test("excluded slot retries without exclusion when it is the only candidate")
    func excludedSlot_retriesWithoutExclusionWhenNeeded() {
        let excludedSlot = CASlotRef(machineIndex: 0, slotIndex: 0)
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                CAPendingPickJob(
                    token: "retry",
                    job: CAJob(id: "retry", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100),
                    excludedSlot: excludedSlot
                ),
            ],
            machines: [
                makeMachine(id: "remote", slotReadyAtMS: [0], msPerFrameC1: 1),
            ],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )

        #expect(result.picks.count == 1)
        #expect(result.picks[0].token == "retry")
        #expect(result.picks[0].slot == excludedSlot)
        #expect(result.picks[0].excludedSlotWasCleared)
        #expect(result.clearedExcludedTokens == ["retry"])
    }

    @Test("non-winning exclusion retry does not clear exclusion token")
    func nonWinningExclusionRetry_doesNotClearToken() {
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(
                    token: "slow-retry",
                    job: .init(id: "slow-retry", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 500),
                    excludedSlot: CASlotRef(machineIndex: 0, slotIndex: 0)
                ),
                .init(
                    token: "fast",
                    job: .init(id: "fast", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 10)
                ),
            ],
            machines: [
                makeMachine(id: "remote", slotReadyAtMS: [0], msPerFrameC1: 1),
            ],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )

        #expect(result.picks.count == 1)
        #expect(result.picks[0].token == "fast")
        #expect(result.clearedExcludedTokens.isEmpty)
    }

    @Test("consumer-style exclusion map clears retry token only after it wins")
    func consumerStyleExclusionMap_clearsRetryTokenOnlyAfterWinner() {
        let machines = [
            makeMachine(id: "remote", slotReadyAtMS: [0], msPerFrameC1: 1),
        ]
        let excludedSlot = CASlotRef(machineIndex: 0, slotIndex: 0)
        let retryJob = CAJob(id: "slow-retry", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 500)
        let fastJob = CAJob(id: "fast", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 10)

        var excludedByToken: [String: CASlotRef] = [
            "slow-retry": excludedSlot,
        ]

        let firstCycle = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "slow-retry", job: retryJob, excludedSlot: excludedByToken["slow-retry"]),
                .init(token: "fast", job: fastJob, excludedSlot: excludedByToken["fast"]),
            ],
            machines: machines,
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )
        applyClearedExcludedTokens(firstCycle.clearedExcludedTokens, to: &excludedByToken)

        #expect(firstCycle.picks.count == 1)
        #expect(firstCycle.picks[0].token == "fast")
        #expect(excludedByToken["slow-retry"] == excludedSlot)

        let secondCycle = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "slow-retry", job: retryJob, excludedSlot: excludedByToken["slow-retry"]),
            ],
            machines: machines,
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )
        applyClearedExcludedTokens(secondCycle.clearedExcludedTokens, to: &excludedByToken)

        #expect(secondCycle.picks.count == 1)
        #expect(secondCycle.picks[0].token == "slow-retry")
        #expect(secondCycle.clearedExcludedTokens == ["slow-retry"])
        #expect(excludedByToken["slow-retry"] == nil)
    }

    @Test("projected slot readyAt matches tDone when txIn dominates start")
    func projectedReadyAt_matchesTDone_whenTxInDominatesStart() {
        let machine = makeMachine(
            id: "machine",
            slotReadyAtMS: [0],
            msPerFrameC1: 1,
            txInMS: 25,
            txOutMS: 7,
            publishOverheadMS: 3
        )
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                CAPendingPickJob(
                    token: "job",
                    job: CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
                ),
            ],
            machines: [machine],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )

        #expect(result.picks.count == 1)
        #expect(result.picks[0].score.tStartMS == result.picks[0].score.tReadyInputMS)
        #expect(result.picks[0].score.tStartMS > result.picks[0].score.tReadySlotMS)
        #expect(result.picks[0].score.tDoneMS > result.picks[0].score.tStartMS)
        #expect(result.projectedMachines[0].slots[0].readyAtMS == result.picks[0].score.tDoneMS)
    }

    @Test("projected slot readyAt matches tDone when slot readiness dominates start")
    func projectedReadyAt_matchesTDone_whenSlotReadinessDominatesStart() {
        let machine = makeMachine(
            id: "machine",
            slotReadyAtMS: [80],
            msPerFrameC1: 1,
            txInMS: 20,
            txOutMS: 7,
            publishOverheadMS: 3
        )
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                CAPendingPickJob(
                    token: "job",
                    job: CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
                ),
            ],
            machines: [machine],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )

        #expect(result.picks.count == 1)
        #expect(result.picks[0].score.tReadySlotMS > result.picks[0].score.tReadyInputMS)
        #expect(result.picks[0].score.tStartMS == result.picks[0].score.tReadySlotMS)
        #expect(result.picks[0].score.tDoneMS > result.picks[0].score.tStartMS)
        #expect(result.projectedMachines[0].slots[0].readyAtMS == result.picks[0].score.tDoneMS)
    }

    @Test("projected readyAt influences active slot occupancy in subsequent picks")
    func projectedReadyAt_influencesActiveSlotOccupancy() {
        let machine = CAMachine(
            id: "machine",
            slots: [
                CASlot(id: "slot-0", readyAtMS: 0),
                CASlot(id: "slot-1", readyAtMS: 0),
            ],
            msPerFrameC1: 1,
            degradationCurve: [
                .init(concurrency: 1, ratioToC1: 1),
                .init(concurrency: 2, ratioToC1: 2),
            ],
            txInMS: 120,
            txOutMS: 50
        )
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "j1", job: .init(id: "j1", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
                .init(token: "j2", job: .init(id: "j2", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100)),
            ],
            machines: [machine],
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .includeFutureReady
        )

        #expect(result.picks.count == 2)
        #expect(result.picks[0].slot == CASlotRef(machineIndex: 0, slotIndex: 0))
        #expect(result.picks[1].slot == CASlotRef(machineIndex: 0, slotIndex: 1))
        #expect(result.picks[0].score.tDoneMS > result.picks[1].score.tStartMS)
        #expect(result.picks[1].score.tStartMS == result.picks[1].score.tReadyInputMS)
        #expect(result.picks[1].score.activeSlotsAtStart == 1)
        #expect(result.picks[1].score.clampedConcurrency == 2)
    }

    @Test("tie-break remains deterministic across repeated batch picks")
    func tieBreak_remainsDeterministicAcrossBatchRuns() {
        let machines = [
            makeMachine(id: "a-machine", slotReadyAtMS: [0], msPerFrameC1: 1),
            makeMachine(id: "b-machine", slotReadyAtMS: [0], msPerFrameC1: 1),
        ]
        let pending: [CAPendingPickJob<String>] = [
            .init(token: "j1", job: .init(id: "j1", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            .init(token: "j2", job: .init(id: "j2", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100)),
        ]

        let baseline = ComplexityAwareScheduler.pickBatch(
            pendingJobs: pending,
            machines: machines,
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .readyNowOnly
        )
        #expect(baseline.picks.map(\.token) == ["j1", "j2"])
        #expect(baseline.picks.map(\.slot.machineIndex) == [0, 1])

        for _ in 0..<100 {
            let next = ComplexityAwareScheduler.pickBatch(
                pendingJobs: pending,
                machines: machines,
                nowMS: 0,
                maxCount: 2,
                readyPolicy: .readyNowOnly
            )
            #expect(next == baseline)
        }
    }
}

private func applyClearedExcludedTokens(
    _ clearedTokens: [String],
    to excludedByToken: inout [String: CASlotRef]
) {
    for token in clearedTokens {
        excludedByToken.removeValue(forKey: token)
    }
}

private func makeMachine(
    id: String,
    slotReadyAtMS: [Double],
    msPerFrameC1: Double,
    txInMS: Double = 0,
    txOutMS: Double = 0,
    publishOverheadMS: Double = 0,
    fixedOverheadMS: Double = 0
) -> CAMachine {
    CAMachine(
        id: id,
        slots: slotReadyAtMS.enumerated().map { index, readyAtMS in
            CASlot(id: "\(id)-slot-\(index)", readyAtMS: readyAtMS)
        },
        msPerFrameC1: msPerFrameC1,
        fixedOverheadMS: fixedOverheadMS,
        degradationCurve: [
            CADegradationPoint(concurrency: 1, ratioToC1: 1.0),
            CADegradationPoint(concurrency: 2, ratioToC1: 1.2),
        ],
        txInMS: txInMS,
        txOutMS: txOutMS,
        publishOverheadMS: publishOverheadMS
    )
}
