import Testing
@testable import KikoMediaCore

@Suite("CA remote dispatch selection")
struct CARemoteDispatchSelectionTests {
    @Test("busy slot loses to idle slot on same machine")
    func busySlot_losesToIdleSlot_onSameMachine() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)
        let idleSlot = CASlot(id: "idle", readyAtMS: 0)
        let busySlot = CASlot(id: "busy", readyAtMS: 500)
        let machine = CAMachine(
            id: "machine",
            slots: [idleSlot, busySlot],
            msPerFrameC1: 1,
            degradationCurve: [
                .init(concurrency: 1, ratioToC1: 1),
                .init(concurrency: 2, ratioToC1: 1.5),
            ],
            txInMS: 0
        )

        let selected = ComplexityAwareScheduler.selectBestCandidate(
            [
                CACandidate(job: job, machine: machine, slot: busySlot),
                CACandidate(job: job, machine: machine, slot: idleSlot),
            ],
            nowMS: 0
        )
        let idleScore = ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: machine, slot: idleSlot),
            nowMS: 0
        )
        let busyScore = ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: machine, slot: busySlot),
            nowMS: 0
        )

        #expect(selected?.candidate.slot.id == "idle")
        if let idleScore, let busyScore {
            #expect(idleScore.tDoneMS < busyScore.tDoneMS)
        }
        #expect(selected?.score == idleScore)
    }

    @Test("degradation factor affects machine selection at higher concurrency")
    func degradationFactor_affectsMachineSelection_atHigherConcurrency() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)

        let degradedCandidate = CASlot(id: "candidate", readyAtMS: 0)
        let degradedMachine = CAMachine(
            id: "degraded",
            slots: [
                degradedCandidate,
                CASlot(id: "busy-1", readyAtMS: 400),
                CASlot(id: "busy-2", readyAtMS: 400),
            ],
            msPerFrameC1: 1,
            degradationCurve: [
                .init(concurrency: 1, ratioToC1: 1.0),
                .init(concurrency: 2, ratioToC1: 2.0),
                .init(concurrency: 3, ratioToC1: 4.0),
            ],
            txInMS: 0
        )

        let stableSlot = CASlot(id: "slot", readyAtMS: 0)
        let stableMachine = CAMachine(
            id: "stable",
            slots: [stableSlot],
            msPerFrameC1: 2,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
            txInMS: 0
        )

        let degradedScore = ComplexityAwareScheduler.score(
            candidate: CACandidate(job: job, machine: degradedMachine, slot: degradedCandidate),
            nowMS: 0
        )
        #expect(degradedScore?.clampedConcurrency == 3)
        #expect(degradedScore.map { abs($0.degradationFactor - 4.0) < 0.000_001 } == true)

        let selected = ComplexityAwareScheduler.selectBestCandidate(
            [
                CACandidate(job: job, machine: degradedMachine, slot: degradedCandidate),
                CACandidate(job: job, machine: stableMachine, slot: stableSlot),
            ],
            nowMS: 0
        )

        #expect(selected?.candidate.machine.id == "stable")
    }

    @Test("frame count proportionally affects run time in scoring")
    func frameCount_proportionallyAffectsRunTime_inScoring() {
        let machine = CAMachine(
            id: "machine",
            slots: [CASlot(id: "slot", readyAtMS: 0)],
            msPerFrameC1: 2,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 0
        )
        let slot = machine.slots[0]
        let shortJob = CAJob(id: "short", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 50)
        let longJob = CAJob(id: "long", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 150)

        let shortScore = ComplexityAwareScheduler.score(
            candidate: CACandidate(job: shortJob, machine: machine, slot: slot),
            nowMS: 0
        )
        let longScore = ComplexityAwareScheduler.score(
            candidate: CACandidate(job: longJob, machine: machine, slot: slot),
            nowMS: 0
        )

        #expect(shortScore != nil)
        #expect(longScore != nil)
        if let shortScore, let longScore {
            #expect(abs(longScore.runMS - shortScore.runMS * 3) < 0.000_001)
        }

        let selected = ComplexityAwareScheduler.selectBestCandidate(
            [
                CACandidate(job: longJob, machine: machine, slot: slot),
                CACandidate(job: shortJob, machine: machine, slot: slot),
            ],
            nowMS: 0
        )
        #expect(selected?.candidate.job.id == "short")
    }

    @Test("tie break ignores txOut versus publish split when totals are equal")
    func tieBreak_ignoresTailSplit_whenTotalsAreEqual() {
        let job = CAJob(id: "job", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)

        let machineA = CAMachine(
            id: "a-machine",
            slots: [CASlot(id: "slot", readyAtMS: 0)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 20,
            txOutMS: 30,
            publishOverheadMS: 10
        )
        let machineB = CAMachine(
            id: "b-machine",
            slots: [CASlot(id: "slot", readyAtMS: 0)],
            msPerFrameC1: 1,
            degradationCurve: [.init(concurrency: 1, ratioToC1: 1)],
            txInMS: 20,
            txOutMS: 5,
            publishOverheadMS: 35
        )

        let candidates: [CACandidate] = [
            CACandidate(job: job, machine: machineB, slot: machineB.slots[0]),
            CACandidate(job: job, machine: machineA, slot: machineA.slots[0]),
        ]

        let baseline = ComplexityAwareScheduler.selectBestCandidate(candidates, nowMS: 0)
        let machineAScore = ComplexityAwareScheduler.score(candidate: candidates[1], nowMS: 0)
        let machineBScore = ComplexityAwareScheduler.score(candidate: candidates[0], nowMS: 0)

        #expect(machineAScore.map(\.tDoneMS) == machineBScore.map(\.tDoneMS))
        #expect(baseline?.candidate.machine.id == "a-machine")

        for _ in 0..<100 {
            let next = ComplexityAwareScheduler.selectBestCandidate(candidates, nowMS: 0)
            #expect(next == baseline)
        }
    }
}
