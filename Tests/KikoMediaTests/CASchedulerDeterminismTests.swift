import Testing
@testable import KikoMediaCore

@Suite("CA scheduler deterministic invariants")
struct CASchedulerDeterminismTests {
    @Test("singleLocalSlot_singleJob_trivialCase")
    func singleLocalSlot_singleJob_trivialCase() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let slot = CASlot(id: "slot", readyAtMS: 5)
        let machine = CAMachine(
            id: "local",
            slots: [slot],
            msPerFrameC1: 2,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let candidate = CACandidate(job: job, machine: machine, slot: slot)
        let score = try! #require(ComplexityAwareScheduler.score(candidate: candidate, nowMS: 0))

        #expect(score.tReadySlotMS == 5)
        #expect(score.tReadyInputMS == 0)
        #expect(score.tStartMS == max(score.tReadySlotMS, score.tReadyInputMS))
        #expect(score.runMS > 0)
        #expect(score.tDoneMS == score.tStartMS + score.runMS)
    }

    @Test("runMS includes fixed overhead plus variable work")
    func runMS_includesFixedOverheadPlusVariableWork() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let slot = CASlot(id: "slot", readyAtMS: 5)
        let baseMachine = CAMachine(
            id: "local",
            slots: [slot],
            msPerFrameC1: 2,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )
        let machineWithOverhead = CAMachine(
            id: "local",
            slots: [slot],
            msPerFrameC1: 2,
            fixedOverheadMS: 7,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let baseScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: baseMachine, slot: slot),
            nowMS: 0
        ))
        let overheadScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: machineWithOverhead, slot: slot),
            nowMS: 0
        ))

        #expect(overheadScore.tStartMS == baseScore.tStartMS)
        #expect(overheadScore.runMS == baseScore.runMS + 7)
        #expect(overheadScore.tDoneMS == baseScore.tDoneMS + 7)
    }

    @Test("tDone_includesTxOutAndPublishTail")
    func tDone_includesTxOutAndPublishTail() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let slot = CASlot(id: "slot", readyAtMS: 5)
        let baseMachine = CAMachine(
            id: "machine",
            slots: [slot],
            msPerFrameC1: 2,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )
        let tailedMachine = CAMachine(
            id: "machine",
            slots: [slot],
            msPerFrameC1: 2,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0,
            txOutMS: 7,
            publishOverheadMS: 3
        )

        let baseScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: baseMachine, slot: slot),
            nowMS: 0
        ))
        let tailedScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: tailedMachine, slot: slot),
            nowMS: 0
        ))

        #expect(tailedScore.tStartMS == baseScore.tStartMS)
        #expect(tailedScore.runMS == baseScore.runMS)
        #expect(tailedScore.tDoneMS == baseScore.tDoneMS + 10)
    }

    @Test("multipleMachines_minimumTDoneWins")
    func multipleMachines_minimumTDoneWins() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let slotA = CASlot(id: "slot", readyAtMS: 0)
        let machineA = CAMachine(
            id: "slow",
            slots: [slotA],
            msPerFrameC1: 2,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let slotB = CASlot(id: "slot", readyAtMS: 0)
        let machineB = CAMachine(
            id: "fast",
            slots: [slotB],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let slowScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: machineA, slot: slotA),
            nowMS: 0
        ))
        let fastScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: machineB, slot: slotB),
            nowMS: 0
        ))

        let selected = ComplexityAwareScheduler.selectBestCandidate(
            [
                CACandidate(job: job, machine: machineA, slot: slotA),
                CACandidate(job: job, machine: machineB, slot: slotB),
            ],
            nowMS: 0
        )

        #expect(selected?.candidate.machine.id == "fast")
        #expect(fastScore.tDoneMS < slowScore.tDoneMS)
        #expect(selected?.score == fastScore)
    }

    @Test("future-aware decision holds for a faster future slot when it wins end-to-end")
    func futureAwareDecision_holdsForFasterFutureSlot_whenItWinsEndToEnd() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let slow = CAMachine(
            id: "slow",
            slots: [.init(id: "slow-slot", readyAtMS: 0)],
            msPerFrameC1: 2.5,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )
        let fast = CAMachine(
            id: "fast",
            slots: [.init(id: "fast-slot", readyAtMS: 10)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let readyNowScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: slow, slot: slow.slots[0]),
            nowMS: 0
        ))
        let futureScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: fast, slot: fast.slots[0]),
            nowMS: 0
        ))

        let decision = ComplexityAwareScheduler.futureAwareDecision(
            for: job,
            machines: [slow, fast],
            nowMS: 0
        )

        switch decision?.action {
        case .some(.holdUntil(let timeMS, let slot)):
            #expect(timeMS == 10)
            #expect(slot == CASlotRef(machineIndex: 1, slotIndex: 0))
        default:
            Issue.record("expected hold decision for the future-ready fast slot")
        }

        #expect(decision?.chosenSlot == CASlotRef(machineIndex: 1, slotIndex: 0))
        #expect(decision?.predictedSlotReadyMS == futureScore.tReadySlotMS)
        #expect(decision?.predictedStartMS == futureScore.tStartMS)
        #expect(decision?.predictedDoneMS == futureScore.tDoneMS)
        #expect(decision?.bestReadyNowAlternative?.slot == CASlotRef(machineIndex: 0, slotIndex: 0))
        #expect(decision?.bestReadyNowAlternative?.score == readyNowScore)
        #expect(decision.map { $0.predictedDoneMS < readyNowScore.tDoneMS } == true)
    }

    @Test("future-aware decision dispatches now when the ready slot beats the future slot")
    func futureAwareDecision_dispatchesNow_whenReadySlotWinsEndToEnd() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let slow = CAMachine(
            id: "slow",
            slots: [.init(id: "slow-slot", readyAtMS: 0)],
            msPerFrameC1: 2.5,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )
        let fast = CAMachine(
            id: "fast",
            slots: [.init(id: "fast-slot", readyAtMS: 30)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let readyNowScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: slow, slot: slow.slots[0]),
            nowMS: 0
        ))
        let futureScore = try! #require(ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: fast, slot: fast.slots[0]),
            nowMS: 0
        ))

        let decision = ComplexityAwareScheduler.futureAwareDecision(
            for: job,
            machines: [slow, fast],
            nowMS: 0
        )

        switch decision?.action {
        case .some(.dispatchNow(let slot)):
            #expect(slot == CASlotRef(machineIndex: 0, slotIndex: 0))
        default:
            Issue.record("expected dispatch-now decision for the ready slow slot")
        }

        #expect(decision?.chosenSlot == CASlotRef(machineIndex: 0, slotIndex: 0))
        #expect(decision?.predictedSlotReadyMS == readyNowScore.tReadySlotMS)
        #expect(decision?.predictedStartMS == readyNowScore.tStartMS)
        #expect(decision?.predictedDoneMS == readyNowScore.tDoneMS)
        #expect(decision?.bestReadyNowAlternative?.slot == CASlotRef(machineIndex: 0, slotIndex: 0))
        #expect(decision?.bestReadyNowAlternative?.score == readyNowScore)
        #expect(readyNowScore.tDoneMS < futureScore.tDoneMS)
    }

    @Test("batch objective prefers lower makespan over greedy fast-slot overload")
    func batchObjective_prefersLowerMakespanOverGreedyFastSlotOverload() {
        let fastNow = makeDeterministicMachine(id: "fast", slotReadyAtMS: [0], msPerFrameC1: 1)
        let fastAfterSmall = makeDeterministicMachine(id: "fast", slotReadyAtMS: [1], msPerFrameC1: 1)
        let slowNow = makeDeterministicMachine(id: "slow", slotReadyAtMS: [0], msPerFrameC1: 20)

        let greedyOverloadPlan = [
            makeManualScoredCandidate(
                jobID: "small",
                machineID: fastNow.id,
                slotID: fastNow.slots[0].id,
                runMS: 1,
                tDoneMS: 2
            ),
            makeManualScoredCandidate(
                jobID: "big",
                enqueueOrder: 1,
                machineID: fastAfterSmall.id,
                slotID: fastAfterSmall.slots[0].id,
                runMS: 100,
                tDoneMS: 101
            ),
        ]
        let fairnessCorrectedPlan = [
            makeManualScoredCandidate(
                jobID: "big",
                enqueueOrder: 1,
                machineID: fastNow.id,
                slotID: fastNow.slots[0].id,
                runMS: 100,
                tDoneMS: 100
            ),
            makeManualScoredCandidate(
                jobID: "small",
                machineID: slowNow.id,
                slotID: slowNow.slots[0].id,
                runMS: 20,
                tDoneMS: 20
            ),
        ]

        #expect(projectedMakespanMS(fairnessCorrectedPlan) < projectedMakespanMS(greedyOverloadPlan))
        #expect(projectedCompletionSumMS(fairnessCorrectedPlan) > projectedCompletionSumMS(greedyOverloadPlan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(fairnessCorrectedPlan, greedyOverloadPlan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(greedyOverloadPlan, fairnessCorrectedPlan) == false)
    }

    @Test("pickBatch avoids fast-slot overload when lower makespan wins")
    func pickBatch_avoidsFastSlotOverload_whenLowerMakespanWins() {
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "small", job: .init(id: "small", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 1)),
                .init(token: "big", job: .init(id: "big", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100)),
            ],
            machines: [
                makeDeterministicMachine(id: "fast", slotReadyAtMS: [0], msPerFrameC1: 1),
                makeDeterministicMachine(id: "slow", slotReadyAtMS: [0], msPerFrameC1: 20),
            ],
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .readyNowOnly
        )

        #expect(result.picks.count == 2)

        let picksByToken = Dictionary(uniqueKeysWithValues: result.picks.map { ($0.token, $0.slot) })
        #expect(picksByToken["big"] == CASlotRef(machineIndex: 0, slotIndex: 0))
        #expect(picksByToken["small"] == CASlotRef(machineIndex: 1, slotIndex: 0))
    }

    @Test("batch objective uses lower completion sum when projected makespan ties")
    func batchObjective_usesLowerCompletionSumWhenProjectedMakespanTies() {
        let fastNow = makeDeterministicMachine(id: "fast", slotReadyAtMS: [0], msPerFrameC1: 10)
        let fasterSlow = makeDeterministicMachine(id: "slow-a", slotReadyAtMS: [0], msPerFrameC1: 20)
        let slowerSlow = makeDeterministicMachine(id: "slow-b", slotReadyAtMS: [0], msPerFrameC1: 60)

        let lowerSumPlan = [
            makeManualScoredCandidate(
                jobID: "big",
                machineID: fastNow.id,
                slotID: fastNow.slots[0].id,
                runMS: 100,
                tDoneMS: 100
            ),
            makeManualScoredCandidate(
                jobID: "small",
                enqueueOrder: 1,
                machineID: fasterSlow.id,
                slotID: fasterSlow.slots[0].id,
                runMS: 20,
                tDoneMS: 20
            ),
        ]
        let higherSumPlan = [
            makeManualScoredCandidate(
                jobID: "big",
                machineID: fastNow.id,
                slotID: fastNow.slots[0].id,
                runMS: 100,
                tDoneMS: 100
            ),
            makeManualScoredCandidate(
                jobID: "small",
                enqueueOrder: 1,
                machineID: slowerSlow.id,
                slotID: slowerSlow.slots[0].id,
                runMS: 60,
                tDoneMS: 60
            ),
        ]

        #expect(projectedMakespanMS(lowerSumPlan) == projectedMakespanMS(higherSumPlan))
        #expect(projectedCompletionSumMS(lowerSumPlan) < projectedCompletionSumMS(higherSumPlan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(lowerSumPlan, higherSumPlan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(higherSumPlan, lowerSumPlan) == false)
    }

    @Test("batch objective uses higher scheduled frame sum when baseline tail dominates makespan")
    func batchObjective_usesHigherScheduledFrameSumWhenBaselineTailDominatesMakespan() {
        let moreFramesButHigherRawMakespan = [
            makeManualScoredCandidate(
                jobID: "big",
                machineID: "machine-a",
                slotID: "slot-a",
                runMS: 150,
                tDoneMS: 150,
                frameCount: 150
            ),
            makeManualScoredCandidate(
                jobID: "mid",
                enqueueOrder: 1,
                machineID: "machine-b",
                slotID: "slot-b",
                runMS: 40,
                tDoneMS: 40,
                frameCount: 40
            ),
        ]
        let fewerFramesButLowerRawMakespan = [
            makeManualScoredCandidate(
                jobID: "medium-a",
                machineID: "machine-a",
                slotID: "slot-a",
                runMS: 120,
                tDoneMS: 120,
                frameCount: 120
            ),
            makeManualScoredCandidate(
                jobID: "medium-b",
                enqueueOrder: 1,
                machineID: "machine-b",
                slotID: "slot-b",
                runMS: 60,
                tDoneMS: 60,
                frameCount: 60
            ),
        ]

        #expect(projectedMakespanMS(moreFramesButHigherRawMakespan) > projectedMakespanMS(fewerFramesButLowerRawMakespan))
        #expect(projectedFrameSum(moreFramesButHigherRawMakespan) > projectedFrameSum(fewerFramesButLowerRawMakespan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(fewerFramesButLowerRawMakespan, moreFramesButHigherRawMakespan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(moreFramesButHigherRawMakespan, fewerFramesButLowerRawMakespan) == false)
        #expect(
            ComplexityAwareScheduler.batchObjectiveLessThan(
                moreFramesButHigherRawMakespan,
                fewerFramesButLowerRawMakespan,
                baselineTailMS: 200
            )
        )
        #expect(
            ComplexityAwareScheduler.batchObjectiveLessThan(
                fewerFramesButLowerRawMakespan,
                moreFramesButHigherRawMakespan,
                baselineTailMS: 200
            ) == false
        )
    }

    @Test("pickBatch prefers more scheduled work when existing tail dominates new batch makespan")
    func pickBatch_prefersMoreScheduledWorkWhenExistingTailDominatesNewBatchMakespan() {
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "long", job: .init(id: "long", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 150)),
                .init(token: "medium-a", job: .init(id: "medium-a", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 120)),
                .init(token: "medium-b", job: .init(id: "medium-b", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 80)),
                .init(token: "tiny", job: .init(id: "tiny", arrivalAtMS: 0, enqueueOrder: 3, frameCount: 1)),
            ],
            machines: [
                makeDeterministicMachine(id: "free-a", slotReadyAtMS: [0], msPerFrameC1: 1),
                makeDeterministicMachine(id: "free-b", slotReadyAtMS: [0], msPerFrameC1: 1),
                makeDeterministicMachine(id: "tail", slotReadyAtMS: [200], msPerFrameC1: 1),
            ],
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .readyNowOnly
        )

        #expect(result.picks.count == 2)
        #expect(Set(result.picks.map(\.token)) == ["long", "medium-a"])
    }

    @Test("batch objective falls back to existing deterministic tie-break when objective terms tie")
    func batchObjective_fallsBackToExistingDeterministicTieBreak_whenObjectiveTermsTie() {
        let machineA = makeDeterministicMachine(id: "a-machine", slotReadyAtMS: [0], msPerFrameC1: 1)
        let machineB = makeDeterministicMachine(id: "b-machine", slotReadyAtMS: [0], msPerFrameC1: 1)

        let leftPlan = [
            makeManualScoredCandidate(
                jobID: "first",
                machineID: machineA.id,
                slotID: machineA.slots[0].id,
                runMS: 10,
                tDoneMS: 10
            ),
            makeManualScoredCandidate(
                jobID: "second",
                enqueueOrder: 1,
                machineID: machineB.id,
                slotID: machineB.slots[0].id,
                runMS: 10,
                tDoneMS: 10
            ),
        ]
        let rightPlan = [
            makeManualScoredCandidate(
                jobID: "first",
                machineID: machineB.id,
                slotID: machineB.slots[0].id,
                runMS: 10,
                tDoneMS: 10
            ),
            makeManualScoredCandidate(
                jobID: "second",
                enqueueOrder: 1,
                machineID: machineA.id,
                slotID: machineA.slots[0].id,
                runMS: 10,
                tDoneMS: 10
            ),
        ]

        #expect(projectedMakespanMS(leftPlan) == projectedMakespanMS(rightPlan))
        #expect(projectedCompletionSumMS(leftPlan) == projectedCompletionSumMS(rightPlan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(leftPlan, rightPlan))
        #expect(ComplexityAwareScheduler.batchObjectiveLessThan(rightPlan, leftPlan) == false)
    }

    @Test("pickBatch uses existing deterministic tie-break when objective terms tie")
    func pickBatch_usesExistingDeterministicTieBreak_whenObjectiveTermsTie() {
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [
                .init(token: "first", job: .init(id: "first", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)),
                .init(token: "second", job: .init(id: "second", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 10)),
            ],
            machines: [
                makeDeterministicMachine(id: "b-machine", slotReadyAtMS: [0], msPerFrameC1: 1),
                makeDeterministicMachine(id: "a-machine", slotReadyAtMS: [0], msPerFrameC1: 1),
            ],
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .readyNowOnly
        )

        #expect(result.picks.count == 2)

        let picksByToken = Dictionary(uniqueKeysWithValues: result.picks.map { ($0.token, $0.slot) })
        #expect(picksByToken["first"] == CASlotRef(machineIndex: 1, slotIndex: 0))
        #expect(picksByToken["second"] == CASlotRef(machineIndex: 0, slotIndex: 0))
    }

    @Test("tieBreak_lowerRunWins_whenTDoneEqual")
    func tieBreak_lowerRunWins_whenTDoneEqual() {
        let higherRun = makeManualScoredCandidate(
            jobID: "job",
            machineID: "a",
            slotID: "slot-a",
            runMS: 30,
            tDoneMS: 100
        )
        let lowerRun = makeManualScoredCandidate(
            jobID: "job",
            machineID: "b",
            slotID: "slot-b",
            runMS: 20,
            tDoneMS: 100
        )

        #expect(ComplexityAwareScheduler.tieBreakLessThan(lowerRun, higherRun))
        #expect(ComplexityAwareScheduler.tieBreakLessThan(higherRun, lowerRun) == false)
    }

    @Test("tieBreak_earlierArrivalWins_afterRunTie")
    func tieBreak_earlierArrivalWins_afterRunTie() {
        let early = makeManualScoredCandidate(
            jobID: "early",
            arrivalAtMS: 1,
            machineID: "machine",
            slotID: "slot",
            runMS: 20,
            tDoneMS: 100
        )
        let late = makeManualScoredCandidate(
            jobID: "late",
            arrivalAtMS: 2,
            machineID: "machine",
            slotID: "slot",
            runMS: 20,
            tDoneMS: 100
        )

        #expect(ComplexityAwareScheduler.tieBreakLessThan(early, late))
        #expect(ComplexityAwareScheduler.tieBreakLessThan(late, early) == false)
    }

    @Test("tieBreak_lowerEnqueueOrderWins_afterArrivalTie")
    func tieBreak_lowerEnqueueOrderWins_afterArrivalTie() {
        let low = makeManualScoredCandidate(
            jobID: "low",
            arrivalAtMS: 1,
            enqueueOrder: 0,
            machineID: "machine",
            slotID: "slot",
            runMS: 20,
            tDoneMS: 100
        )
        let high = makeManualScoredCandidate(
            jobID: "high",
            arrivalAtMS: 1,
            enqueueOrder: 1,
            machineID: "machine",
            slotID: "slot",
            runMS: 20,
            tDoneMS: 100
        )

        #expect(ComplexityAwareScheduler.tieBreakLessThan(low, high))
        #expect(ComplexityAwareScheduler.tieBreakLessThan(high, low) == false)
    }

    @Test("tieBreak_machineSlotJobLexicalLevels")
    func tieBreak_machineSlotJobLexicalLevels() {
        let score = CAScore(
            tReadySlotMS: 0,
            tReadyInputMS: 0,
            tStartMS: 0,
            activeSlotsAtStart: 0,
            clampedConcurrency: 1,
            degradationFactor: 1,
            runMS: 10,
            tDoneMS: 10
        )

        let machineA = CAScoredCandidate(
            candidate: CACandidate(
                job: CAJob(id: "job", arrivalAtMS: 1, enqueueOrder: 1, frameCount: 10),
                machine: CAMachine(
                    id: "a-machine",
                    slots: [.init(id: "slot", readyAtMS: 0)],
                    msPerFrameC1: 1,
                    degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
                    txInMS: 0
                ),
                slot: .init(id: "slot", readyAtMS: 0)
            ),
            score: score
        )

        let machineB = CAScoredCandidate(
            candidate: CACandidate(
                job: CAJob(id: "job", arrivalAtMS: 1, enqueueOrder: 1, frameCount: 10),
                machine: CAMachine(
                    id: "b-machine",
                    slots: [.init(id: "slot", readyAtMS: 0)],
                    msPerFrameC1: 1,
                    degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
                    txInMS: 0
                ),
                slot: .init(id: "slot", readyAtMS: 0)
            ),
            score: score
        )

        #expect(ComplexityAwareScheduler.tieBreakLessThan(machineA, machineB))

        let slotA = CAScoredCandidate(
            candidate: CACandidate(
                job: CAJob(id: "job", arrivalAtMS: 1, enqueueOrder: 1, frameCount: 10),
                machine: CAMachine(
                    id: "machine",
                    slots: [.init(id: "a-slot", readyAtMS: 0)],
                    msPerFrameC1: 1,
                    degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
                    txInMS: 0
                ),
                slot: .init(id: "a-slot", readyAtMS: 0)
            ),
            score: score
        )

        let slotB = CAScoredCandidate(
            candidate: CACandidate(
                job: CAJob(id: "job", arrivalAtMS: 1, enqueueOrder: 1, frameCount: 10),
                machine: CAMachine(
                    id: "machine",
                    slots: [.init(id: "b-slot", readyAtMS: 0)],
                    msPerFrameC1: 1,
                    degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
                    txInMS: 0
                ),
                slot: .init(id: "b-slot", readyAtMS: 0)
            ),
            score: score
        )

        #expect(ComplexityAwareScheduler.tieBreakLessThan(slotA, slotB))

        let jobA = CAScoredCandidate(
            candidate: CACandidate(
                job: CAJob(id: "a-job", arrivalAtMS: 1, enqueueOrder: 1, frameCount: 10),
                machine: CAMachine(
                    id: "machine",
                    slots: [.init(id: "slot", readyAtMS: 0)],
                    msPerFrameC1: 1,
                    degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
                    txInMS: 0
                ),
                slot: .init(id: "slot", readyAtMS: 0)
            ),
            score: score
        )

        let jobB = CAScoredCandidate(
            candidate: CACandidate(
                job: CAJob(id: "b-job", arrivalAtMS: 1, enqueueOrder: 1, frameCount: 10),
                machine: CAMachine(
                    id: "machine",
                    slots: [.init(id: "slot", readyAtMS: 0)],
                    msPerFrameC1: 1,
                    degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
                    txInMS: 0
                ),
                slot: .init(id: "slot", readyAtMS: 0)
            ),
            score: score
        )

        #expect(ComplexityAwareScheduler.tieBreakLessThan(jobA, jobB))
    }

    @Test("downSlotExcluded_fromCandidateSet")
    func downSlotExcluded_fromCandidateSet() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)

        let downSlot = CASlot(id: "down", readyAtMS: 0, isDown: true)
        let downMachine = CAMachine(
            id: "down-machine",
            slots: [downSlot],
            msPerFrameC1: 0.1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let upSlot = CASlot(id: "up", readyAtMS: 0)
        let upMachine = CAMachine(
            id: "up-machine",
            slots: [upSlot],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let downCandidate = CACandidate(job: job, machine: downMachine, slot: downSlot)
        let upCandidate = CACandidate(job: job, machine: upMachine, slot: upSlot)

        #expect(ComplexityAwareScheduler.score(candidate: downCandidate, nowMS: 0) == nil)

        let selected = ComplexityAwareScheduler.selectBestCandidate([downCandidate, upCandidate], nowMS: 0)
        #expect(selected?.candidate.machine.id == "up-machine")
    }

    @Test("clampAtMaxProfiledConc_andRepairNonMonotonicCurveConservatively")
    func clampAtMaxProfiledConc_andRepairNonMonotonicCurveConservatively() {
        let candidateSlot = CASlot(id: "candidate", readyAtMS: 0)
        let busySlots: [CASlot] = [
            .init(id: "busy-1", readyAtMS: 100),
            .init(id: "busy-2", readyAtMS: 100),
            .init(id: "busy-3", readyAtMS: 100),
            .init(id: "busy-4", readyAtMS: 100),
            .init(id: "busy-5", readyAtMS: 100),
        ]

        let machine = CAMachine(
            id: "machine",
            slots: [candidateSlot] + busySlots,
            msPerFrameC1: 1,
            degradationCurve: [
                .init(concurrency: 1, ratioToC1: 1.0),
                .init(concurrency: 2, ratioToC1: 1.4),
                .init(concurrency: 3, ratioToC1: 1.1),
            ],
            txInMS: 0
        )

        let factor = ComplexityAwareScheduler.degradationFactor(for: machine, concurrency: 99)
        #expect(abs(factor - 1.4) < 0.000_001)

        let score = ComplexityAwareScheduler.score(
            candidate: CACandidate(
                job: .init(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10),
                machine: machine,
                slot: candidateSlot
            ),
            nowMS: 0
        )

        #expect(score?.activeSlotsAtStart == 5)
        #expect(score?.clampedConcurrency == 3)
        #expect(score.map { abs($0.degradationFactor - 1.4) < 0.000_001 } == true)
    }

    @Test("missing exact degradation point no longer falls back to one")
    func missingExactDegradationPoint_noLongerFallsBackToOne() {
        let candidateSlot = CASlot(id: "candidate", readyAtMS: 0)
        let busySlot = CASlot(id: "busy-1", readyAtMS: 100)
        let machine = CAMachine(
            id: "machine",
            slots: [candidateSlot, busySlot],
            msPerFrameC1: 1,
            degradationCurve: [
                .init(concurrency: 1, ratioToC1: 1.0),
                .init(concurrency: 3, ratioToC1: 1.6),
            ],
            txInMS: 0
        )

        let factor = ComplexityAwareScheduler.degradationFactor(for: machine, concurrency: 2)
        #expect(abs(factor - 1.3) < 0.000_001)

        let score = ComplexityAwareScheduler.score(
            candidate: CACandidate(
                job: .init(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10),
                machine: machine,
                slot: candidateSlot
            ),
            nowMS: 0
        )

        #expect(score?.clampedConcurrency == 2)
        #expect(score.map { abs($0.degradationFactor - 1.3) < 0.000_001 } == true)
    }

    @Test("txInLocalZero_vsRemoteNonZero")
    func txInLocalZero_vsRemoteNonZero() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)
        let localSlot = CASlot(id: "slot", readyAtMS: 0)
        let local = CAMachine(
            id: "local",
            slots: [localSlot],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let remoteSlot = CASlot(id: "slot", readyAtMS: 0)
        let remote = CAMachine(
            id: "remote",
            slots: [remoteSlot],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 50
        )

        let localScore = ComplexityAwareScheduler.score(
            candidate: .init(job: job, machine: local, slot: localSlot),
            nowMS: 0
        )
        let remoteScore = ComplexityAwareScheduler.score(
            candidate: .init(job: job, machine: remote, slot: remoteSlot),
            nowMS: 0
        )

        #expect(localScore?.tStartMS == 0)
        #expect(remoteScore?.tStartMS == 50)

        let selected = ComplexityAwareScheduler.selectBestCandidate(
            [
                .init(job: job, machine: remote, slot: remoteSlot),
                .init(job: job, machine: local, slot: localSlot),
            ],
            nowMS: 0
        )

        #expect(selected?.candidate.machine.id == "local")
    }

    @Test("allSlotsBusy_readyAtOrderingMatters")
    func allSlotsBusy_readyAtOrderingMatters() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 5)

        let earlierSlot = CASlot(id: "slot", readyAtMS: 30)
        let earlierMachine = CAMachine(
            id: "earlier",
            slots: [earlierSlot, .init(id: "other", readyAtMS: 200)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let laterSlot = CASlot(id: "slot", readyAtMS: 60)
        let laterMachine = CAMachine(
            id: "later",
            slots: [laterSlot, .init(id: "other", readyAtMS: 200)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let selected = ComplexityAwareScheduler.selectBestCandidate(
            [
                .init(job: job, machine: laterMachine, slot: laterSlot),
                .init(job: job, machine: earlierMachine, slot: earlierSlot),
            ],
            nowMS: 0
        )

        #expect(selected?.candidate.machine.id == "earlier")
        #expect(selected?.score.tStartMS == 30)
    }

    @Test("determinism_sameInputSameOutput_over100Runs")
    func determinism_sameInputSameOutput_over100Runs() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 10)

        let machineA = CAMachine(
            id: "a-machine",
            slots: [.init(id: "slot", readyAtMS: 10)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )
        let machineB = CAMachine(
            id: "b-machine",
            slots: [.init(id: "slot", readyAtMS: 10)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )
        let machineC = CAMachine(
            id: "c-machine",
            slots: [.init(id: "slot", readyAtMS: 20)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )

        let candidates: [CACandidate] = [
            .init(job: job, machine: machineB, slot: machineB.slots[0]),
            .init(job: job, machine: machineC, slot: machineC.slots[0]),
            .init(job: job, machine: machineA, slot: machineA.slots[0]),
        ]

        let baseline = ComplexityAwareScheduler.selectBestCandidate(candidates, nowMS: 0)
        #expect(baseline?.candidate.machine.id == "a-machine")

        for _ in 0..<100 {
            let next = ComplexityAwareScheduler.selectBestCandidate(candidates, nowMS: 0)
            #expect(next == baseline)
        }
    }
}

private func makeDeterministicMachine(
    id: String,
    slotReadyAtMS: [Double],
    msPerFrameC1: Double
) -> CAMachine {
    CAMachine(
        id: id,
        slots: slotReadyAtMS.enumerated().map { index, readyAtMS in
            CASlot(id: "\(id)-slot-\(index)", readyAtMS: readyAtMS)
        },
        msPerFrameC1: msPerFrameC1,
        degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
        txInMS: 0
    )
}

private func makeManualScoredCandidate(
    jobID: String,
    arrivalAtMS: Double = 0,
    enqueueOrder: Int = 0,
    machineID: String,
    slotID: String,
    runMS: Double,
    tDoneMS: Double,
    tReadySlotMS: Double = 0,
    tReadyInputMS: Double = 0,
    tStartMS: Double? = nil,
    frameCount: Double = 1
) -> CAScoredCandidate {
    let slot = CASlot(id: slotID, readyAtMS: tReadySlotMS)
    let machine = CAMachine(
        id: machineID,
        slots: [slot],
        msPerFrameC1: 1,
        degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
        txInMS: 0
    )
    let actualStartMS = tStartMS ?? max(tReadySlotMS, tReadyInputMS)
    return CAScoredCandidate(
        candidate: CACandidate(
            job: CAJob(id: jobID, arrivalAtMS: arrivalAtMS, enqueueOrder: enqueueOrder, frameCount: frameCount),
            machine: machine,
            slot: slot
        ),
        score: CAScore(
            tReadySlotMS: tReadySlotMS,
            tReadyInputMS: tReadyInputMS,
            tStartMS: actualStartMS,
            activeSlotsAtStart: 0,
            clampedConcurrency: 1,
            degradationFactor: 1,
            runMS: runMS,
            tDoneMS: tDoneMS
        )
    )
}

private func projectedMakespanMS(_ plan: [CAScoredCandidate]) -> Double {
    plan.reduce(0) { max($0, $1.score.tDoneMS) }
}

private func projectedCompletionSumMS(_ plan: [CAScoredCandidate]) -> Double {
    plan.reduce(0) { $0 + $1.score.tDoneMS }
}

private func projectedFrameSum(_ plan: [CAScoredCandidate]) -> Double {
    plan.reduce(0) { $0 + $1.candidate.job.frameCount }
}
