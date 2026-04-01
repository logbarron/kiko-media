import Testing
@testable import KikoMediaCore

@Suite("CA projected-state reservations")
struct CAVisibleQueueOracleTests {
    @Test("two-stage accepts a reservation when only the original root state had a ready-now alternative")
    func twoStage_acceptsReservationAgainstProjectedState() {
        let nowMS = 0.0
        let machines = [
            makeVisibleQueueMachine(id: "lane-a", slotReadyAtMS: [0], msPerFrameC1: 1),
            makeVisibleQueueMachine(id: "lane-b", slotReadyAtMS: [0], msPerFrameC1: 1),
        ]
        let pendingJobs: [CAPendingPickJob<String>] = [
            .init(token: "long", job: .init(id: "long", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 9)),
            .init(token: "medium", job: .init(id: "medium", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 8)),
            .init(token: "short", job: .init(id: "short", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 1)),
        ]

        let stageOne = ComplexityAwareScheduler.pickBatch(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: 2,
            readyPolicy: .readyNowOnly
        )
        let remainingJobs = pendingJobs.filter { pending in
            !Set(stageOne.picks.map(\.token)).contains(pending.token)
        }

        let rawReservation = ComplexityAwareScheduler.pickBatch(
            pendingJobs: remainingJobs,
            machines: stageOne.projectedMachines,
            nowMS: nowMS,
            maxCount: 1,
            readyPolicy: .includeFutureReady
        )
        let originalReadyNow = ComplexityAwareScheduler.pickBatch(
            pendingJobs: remainingJobs,
            machines: machines,
            nowMS: nowMS,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        let twoStage = ComplexityAwareScheduler.pickTwoStageBatch(
            pendingJobs: pendingJobs,
            machines: machines,
            nowMS: nowMS,
            maxReadyNowCount: 2
        )

        #expect(Set(stageOne.picks.map(\.token)) == ["medium", "short"])
        #expect(rawReservation.picks.count == 1)
        #expect(rawReservation.picks[0].token == "long")
        #expect(rawReservation.picks[0].score.tReadySlotMS == 1)
        #expect(rawReservation.picks[0].score.tDoneMS == 10)
        #expect(originalReadyNow.picks.count == 1)
        #expect(originalReadyNow.picks[0].token == "long")
        #expect(originalReadyNow.picks[0].score.tDoneMS == 9)
        #expect(twoStage.reservationPicks.count == 1)
        #expect(twoStage.reservationPicks[0].token == "long")
        #expect(twoStage.reservationPicks[0].score.tDoneMS == 10)
    }
}

private func makeVisibleQueueMachine(
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
        degradationCurve: [
            .init(concurrency: 1, ratioToC1: 1.0),
        ],
        txInMS: 0
    )
}
