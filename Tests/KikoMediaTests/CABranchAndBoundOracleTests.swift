import Testing
@testable import KikoMediaCore

@Suite("CA branch-and-bound oracle differential tests")
struct CABranchAndBoundOracleTests {

    // MARK: - Trivial cases

    @Test("empty inputs produce identical empty results")
    func emptyInputs() {
        let result = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [CAPendingPickJob<String>](),
            machines: [],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        let oracle = ComplexityAwareScheduler.pickBatchOracle(
            pendingJobs: [CAPendingPickJob<String>](),
            machines: [],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        assertOracleMatchesLive(live: result, oracle: oracle)
    }

    @Test("single job single slot produces identical result")
    func singleJobSingleSlot() {
        let jobs = [CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100))]
        let machines = [makeMachine(id: "m0", slotCount: 1, msPerFrame: 2.0)]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .includeFutureReady)
    }

    // MARK: - Multi-job multi-machine

    @Test("two jobs two machines readyNowOnly")
    func twoJobsTwoMachines_readyNowOnly() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 50)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 1, msPerFrame: 1.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 3.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
    }

    @Test("two jobs two machines includeFutureReady")
    func twoJobsTwoMachines_includeFutureReady() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 50)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 1, msPerFrame: 1.0, slotReadyAtMS: [50]),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 3.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .includeFutureReady)
    }

    @Test("three jobs three machines maxCount 2")
    func threeJobsThreeMachines_maxCount2() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 200)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100)),
            CAPendingPickJob(token: "c", job: CAJob(id: "j2", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 50)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 2, msPerFrame: 1.0),
            makeMachine(id: "medium", slotCount: 1, msPerFrame: 2.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 4.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
    }

    @Test("oracle matches live when baseline tail dominates current batch makespan")
    func baselineTailDominatesCurrentBatchMakespan() {
        let jobs = [
            CAPendingPickJob(token: "long", job: CAJob(id: "long", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 150)),
            CAPendingPickJob(token: "medium-a", job: CAJob(id: "medium-a", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 120)),
            CAPendingPickJob(token: "medium-b", job: CAJob(id: "medium-b", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 80)),
            CAPendingPickJob(token: "tiny", job: CAJob(id: "tiny", arrivalAtMS: 0, enqueueOrder: 3, frameCount: 1)),
        ]
        let machines = [
            makeMachine(id: "free-a", slotCount: 1, msPerFrame: 1.0),
            makeMachine(id: "free-b", slotCount: 1, msPerFrame: 1.0),
            makeMachine(id: "tail", slotCount: 1, msPerFrame: 1.0, slotReadyAtMS: [200]),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
    }

    @Test("three jobs three machines maxCount 3")
    func threeJobsThreeMachines_maxCount3() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 200)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100)),
            CAPendingPickJob(token: "c", job: CAJob(id: "j2", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 50)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 2, msPerFrame: 1.0),
            makeMachine(id: "medium", slotCount: 1, msPerFrame: 2.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 4.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .includeFutureReady)
    }

    // MARK: - Slot reuse (sequential queuing)

    @Test("slot reuse: two jobs one slot maxCount 2")
    func slotReuse_twoJobsOneSlot() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 50)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 30)),
        ]
        let machines = [makeMachine(id: "m0", slotCount: 1, msPerFrame: 2.0)]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .includeFutureReady)
    }

    // MARK: - Degradation curves

    @Test("multi-slot machine with degradation curve")
    func degradationCurve() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 80)),
        ]
        let machines = [
            CAMachine(
                id: "fast",
                slots: [
                    CASlot(id: "s0", readyAtMS: 0),
                    CASlot(id: "s1", readyAtMS: 0),
                    CASlot(id: "s2", readyAtMS: 0),
                ],
                msPerFrameC1: 3.157,
                degradationCurve: [
                    .init(concurrency: 1, ratioToC1: 1.0),
                    .init(concurrency: 2, ratioToC1: 1.514),
                    .init(concurrency: 3, ratioToC1: 2.237),
                ],
                txInMS: 10
            )
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .includeFutureReady)
    }

    // MARK: - Down slots

    @Test("down slot excluded from picks")
    func downSlot() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
        ]
        let machines = [
            CAMachine(
                id: "m0",
                slots: [
                    CASlot(id: "s0", readyAtMS: 0, isDown: true),
                    CASlot(id: "s1", readyAtMS: 0),
                ],
                msPerFrameC1: 2.0,
                degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0
            ),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .readyNowOnly)
    }

    // MARK: - Excluded slots and exclusion clearing

    @Test("excluded slot skipped when alternatives exist")
    func excludedSlot_skipped() {
        let jobs = [
            CAPendingPickJob(
                token: "a",
                job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100),
                excludedSlot: CASlotRef(machineIndex: 0, slotIndex: 0)
            ),
        ]
        let machines = [
            makeMachine(id: "m0", slotCount: 2, msPerFrame: 2.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .readyNowOnly)
    }

    @Test("excluded slot cleared when no alternatives exist")
    func excludedSlot_cleared() {
        let jobs = [
            CAPendingPickJob(
                token: "a",
                job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100),
                excludedSlot: CASlotRef(machineIndex: 0, slotIndex: 0)
            ),
        ]
        let machines = [
            makeMachine(id: "m0", slotCount: 1, msPerFrame: 2.0),
        ]
        let live = callPickBatch(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .readyNowOnly)
        let oracle = callPickBatchOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .readyNowOnly)
        assertOracleMatchesLive(live: live, oracle: oracle)
        #expect(live.picks.first?.excludedSlotWasCleared == true)
        #expect(live.clearedExcludedTokens == ["a"])
    }

    @Test("mixed exclusion: one job excluded, one not, multi-pick")
    func mixedExclusion_multiPick() {
        let jobs = [
            CAPendingPickJob(
                token: "excluded",
                job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100),
                excludedSlot: CASlotRef(machineIndex: 0, slotIndex: 0)
            ),
            CAPendingPickJob(
                token: "normal",
                job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 50)
            ),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 2, msPerFrame: 1.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 3.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .includeFutureReady)
    }

    @Test("identical ready slots remain exact when an exclusion narrows one job")
    func identicalReadySlots_withExclusion_remainExact() {
        let jobs = [
            CAPendingPickJob(
                token: "t0",
                job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 20)
            ),
            CAPendingPickJob(
                token: "t1",
                job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100),
                excludedSlot: CASlotRef(machineIndex: 0, slotIndex: 0)
            ),
            CAPendingPickJob(
                token: "t2",
                job: CAJob(id: "j2", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 10)
            ),
        ]
        let machines = [
            CAMachine(
                id: "m0",
                slots: [
                    CASlot(id: "m0-s0", readyAtMS: 100),
                    CASlot(id: "m0-s1", readyAtMS: 100),
                ],
                msPerFrameC1: 2.0,
                degradationCurve: [
                    .init(concurrency: 1, ratioToC1: 1.0),
                    .init(concurrency: 2, ratioToC1: 4.0),
                    .init(concurrency: 3, ratioToC1: 3.0),
                ],
                txInMS: 5
            ),
        ]

        assertLiveMatchesOracle(
            jobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 3,
            readyPolicy: .includeFutureReady
        )
    }

    @Test("dedup keeps comparator-minimal representative within a symmetry class")
    func dedup_keepsComparatorMinimalRepresentative() {
        let jobs = [
            CAPendingPickJob(
                token: "a",
                job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100),
                excludedSlot: CASlotRef(machineIndex: 0, slotIndex: 0)
            ),
        ]
        let machines = [
            CAMachine(
                id: "m0",
                slots: (1...10).map { slotNumber in
                    CASlot(id: "m0#s\(slotNumber)", readyAtMS: 0)
                },
                msPerFrameC1: 2.0,
                degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0
            ),
        ]

        let live = callPickBatch(
            jobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        let oracle = callPickBatchOracle(
            jobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )

        assertOracleMatchesLive(live: live, oracle: oracle)
        #expect(live.picks.first?.slot == CASlotRef(machineIndex: 0, slotIndex: 9))
    }

    // MARK: - Mixed readyAtMS

    @Test("mixed readyAtMS across machines")
    func mixedReadyAtMS() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 80)),
            CAPendingPickJob(token: "c", job: CAJob(id: "j2", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 60)),
        ]
        let machines = [
            makeMachine(id: "m0", slotCount: 1, msPerFrame: 1.0, slotReadyAtMS: [50]),
            makeMachine(id: "m1", slotCount: 2, msPerFrame: 2.0, slotReadyAtMS: [0, 30]),
            makeMachine(id: "m2", slotCount: 1, msPerFrame: 3.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .includeFutureReady)
    }

    // MARK: - txIn / txOut / publishOverhead / fixedOverhead

    @Test("overhead fields preserved through oracle")
    func overheadFields() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 50)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 30)),
        ]
        let machines = [
            CAMachine(
                id: "local",
                slots: [CASlot(id: "s0", readyAtMS: 0)],
                msPerFrameC1: 2.0,
                fixedOverheadMS: 10,
                degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 0,
                txOutMS: 5,
                publishOverheadMS: 3
            ),
            CAMachine(
                id: "remote",
                slots: [CASlot(id: "s0", readyAtMS: 0)],
                msPerFrameC1: 1.5,
                fixedOverheadMS: 5,
                degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 20,
                txOutMS: 15,
                publishOverheadMS: 8
            ),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
    }

    // MARK: - Production-like topology

    @Test("three-machine heterogeneous topology")
    func realHardwareTopology() {
        let jobs = (0..<5).map { i in
            CAPendingPickJob(
                token: "v\(i)",
                job: CAJob(id: "video-\(i)", arrivalAtMS: 0, enqueueOrder: i, frameCount: Double(200 + i * 100))
            )
        }
        let machines = [
            CAMachine(
                id: "coordinator",
                slots: [CASlot(id: "c-s0", readyAtMS: 0), CASlot(id: "c-s1", readyAtMS: 0)],
                msPerFrameC1: 4.479,
                degradationCurve: [
                    .init(concurrency: 1, ratioToC1: 1.0),
                    .init(concurrency: 2, ratioToC1: 1.943),
                ],
                txInMS: 0
            ),
            CAMachine(
                id: "w1",
                slots: [CASlot(id: "w1-s0", readyAtMS: 0), CASlot(id: "w1-s1", readyAtMS: 0), CASlot(id: "w1-s2", readyAtMS: 0)],
                msPerFrameC1: 3.157,
                degradationCurve: [
                    .init(concurrency: 1, ratioToC1: 1.0),
                    .init(concurrency: 2, ratioToC1: 1.514),
                    .init(concurrency: 3, ratioToC1: 2.237),
                ],
                txInMS: 5
            ),
            CAMachine(
                id: "w2",
                slots: [CASlot(id: "w2-s0", readyAtMS: 0), CASlot(id: "w2-s1", readyAtMS: 0)],
                msPerFrameC1: 9.135,
                degradationCurve: [
                    .init(concurrency: 1, ratioToC1: 1.0),
                    .init(concurrency: 2, ratioToC1: 1.791),
                ],
                txInMS: 5
            ),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 5, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 5, readyPolicy: .includeFutureReady)
    }

    // MARK: - Arrival time and enqueue order variation

    @Test("varied arrival times and enqueue orders")
    func variedArrivalAndEnqueue() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 10, enqueueOrder: 2, frameCount: 80)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 5, enqueueOrder: 0, frameCount: 120)),
            CAPendingPickJob(token: "c", job: CAJob(id: "j2", arrivalAtMS: 10, enqueueOrder: 1, frameCount: 60)),
        ]
        let machines = [
            makeMachine(id: "m0", slotCount: 2, msPerFrame: 2.0),
            makeMachine(id: "m1", slotCount: 1, msPerFrame: 3.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .readyNowOnly)
    }

    // MARK: - maxCount variations

    @Test("maxCount 0 returns empty")
    func maxCountZero() {
        let jobs = [CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100))]
        let machines = [makeMachine(id: "m0", slotCount: 1, msPerFrame: 2.0)]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 0, readyPolicy: .readyNowOnly)
    }

    @Test("maxCount exceeds job count")
    func maxCountExceedsJobCount() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 50)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 2, msPerFrame: 1.0),
            makeMachine(id: "slow", slotCount: 2, msPerFrame: 3.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 10, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 10, readyPolicy: .includeFutureReady)
    }

    // MARK: - readyNowOnly with all slots busy

    @Test("readyNowOnly with all slots in the future returns empty")
    func allSlotsFuture_readyNowOnly() {
        let jobs = [CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100))]
        let machines = [makeMachine(id: "m0", slotCount: 1, msPerFrame: 2.0, slotReadyAtMS: [500])]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .readyNowOnly)
    }

    @Test("includeFutureReady with all slots in the future still picks")
    func allSlotsFuture_includeFutureReady() {
        let jobs = [CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100))]
        let machines = [makeMachine(id: "m0", slotCount: 1, msPerFrame: 2.0, slotReadyAtMS: [500])]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 1, readyPolicy: .includeFutureReady)
    }

    // MARK: - Tie-break determinism under identical machines

    @Test("identical machines produce deterministic assignment via machine.id tie-break")
    func identicalMachines_deterministic() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 100)),
        ]
        let machines = [
            makeMachine(id: "b-machine", slotCount: 1, msPerFrame: 2.0),
            makeMachine(id: "a-machine", slotCount: 1, msPerFrame: 2.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
    }

    // MARK: - Greedy suboptimality example from research doc

    @Test("oracle matches live on the greedy suboptimality example")
    func greedySuboptimalityExample() {
        let jobs = [
            CAPendingPickJob(token: "big", job: CAJob(id: "big", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 1000)),
            CAPendingPickJob(token: "medium", job: CAJob(id: "medium", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 500)),
            CAPendingPickJob(token: "small", job: CAJob(id: "small", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 100)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 1, msPerFrame: 1.0),
            makeMachine(id: "medium", slotCount: 1, msPerFrame: 2.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 4.0),
        ]
        let live = callPickBatch(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .readyNowOnly)
        let oracle = callPickBatchOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .readyNowOnly)
        assertOracleMatchesLive(live: live, oracle: oracle)
        #expect(live.picks.count == 3)
    }

    // MARK: - Live solver telemetry sanity

    @Test("live B&B solver telemetry proves real solver work on multi-pick")
    func liveSolverTelemetry_provesRealSolverWork() {
        let jobs = [
            CAPendingPickJob(token: "big", job: CAJob(id: "big", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 1000)),
            CAPendingPickJob(token: "medium", job: CAJob(id: "medium", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 500)),
            CAPendingPickJob(token: "small", job: CAJob(id: "small", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 100)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 1, msPerFrame: 1.0),
            makeMachine(id: "medium", slotCount: 1, msPerFrame: 2.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 4.0),
        ]

        _ = ComplexityAwareScheduler.pickBatch(
            pendingJobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 3,
            readyPolicy: .readyNowOnly
        )
        let telemetry = ComplexityAwareScheduler.lastSolverTelemetry

        #expect(telemetry.nodesVisited > 1)
        #expect(telemetry.incumbentUpdates >= 1)
        #expect(telemetry.maxDepth >= 2)
        #expect(telemetry.solverWallMS >= 0)
        #expect(
            telemetry.prunedByPickCount > 0
            || telemetry.prunedByMakespan > 0
            || telemetry.prunedByCompletionSum > 0,
            "at least one prune counter must be greater than zero"
        )
    }

    // MARK: - Telemetry

    @Test("oracle populates telemetry with non-trivial values on multi-pick")
    func oracleTelemetry_multiPick() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 80)),
            CAPendingPickJob(token: "c", job: CAJob(id: "j2", arrivalAtMS: 0, enqueueOrder: 2, frameCount: 60)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 2, msPerFrame: 1.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 3.0),
        ]

        _ = ComplexityAwareScheduler.pickBatchOracle(
            pendingJobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 3,
            readyPolicy: .readyNowOnly
        )
        let telemetry = ComplexityAwareScheduler.lastSolverTelemetry

        #expect(telemetry.nodesVisited > 1)
        #expect(telemetry.incumbentUpdates >= 1)
        #expect(telemetry.maxDepth >= 2)
        #expect(telemetry.solverWallMS >= 0)
    }

    @Test("oracle telemetry resets between calls")
    func oracleTelemetry_resetsBetweenCalls() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 80)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 2, msPerFrame: 1.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 3.0),
        ]
        _ = ComplexityAwareScheduler.pickBatchOracle(
            pendingJobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .readyNowOnly
        )
        let first = ComplexityAwareScheduler.lastSolverTelemetry

        _ = ComplexityAwareScheduler.pickBatchOracle(
            pendingJobs: [CAPendingPickJob<String>](),
            machines: [],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        let second = ComplexityAwareScheduler.lastSolverTelemetry

        #expect(first.nodesVisited > 0)
        #expect(second.nodesVisited == 0)
        #expect(second.incumbentUpdates == 0)
        #expect(second.maxDepth == 0)
    }

    @Test("live telemetry resets on empty fast path")
    func liveTelemetry_resetsBetweenCalls() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 80)),
        ]
        let machines = [
            makeMachine(id: "fast", slotCount: 2, msPerFrame: 1.0),
            makeMachine(id: "slow", slotCount: 1, msPerFrame: 3.0),
        ]

        _ = ComplexityAwareScheduler.pickBatch(
            pendingJobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 2,
            readyPolicy: .readyNowOnly
        )
        let first = ComplexityAwareScheduler.lastSolverTelemetry

        _ = ComplexityAwareScheduler.pickBatch(
            pendingJobs: [CAPendingPickJob<String>](),
            machines: [],
            nowMS: 0,
            maxCount: 1,
            readyPolicy: .readyNowOnly
        )
        let second = ComplexityAwareScheduler.lastSolverTelemetry

        #expect(first.nodesVisited > 0)
        #expect(second.nodesVisited == 0)
        #expect(second.incumbentUpdates == 0)
        #expect(second.maxDepth == 0)
    }

    // MARK: - Determinism

    @Test("oracle produces identical results over 50 runs")
    func oracleDeterminism() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 5, enqueueOrder: 1, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 80)),
            CAPendingPickJob(token: "c", job: CAJob(id: "j2", arrivalAtMS: 5, enqueueOrder: 2, frameCount: 60)),
        ]
        let machines = [
            makeMachine(id: "m0", slotCount: 2, msPerFrame: 1.5, slotReadyAtMS: [0, 20]),
            makeMachine(id: "m1", slotCount: 1, msPerFrame: 3.0),
        ]

        let baseline = ComplexityAwareScheduler.pickBatchOracle(
            pendingJobs: jobs,
            machines: machines,
            nowMS: 0,
            maxCount: 3,
            readyPolicy: .includeFutureReady
        )

        for _ in 0..<50 {
            let run = ComplexityAwareScheduler.pickBatchOracle(
                pendingJobs: jobs,
                machines: machines,
                nowMS: 0,
                maxCount: 3,
                readyPolicy: .includeFutureReady
            )
            #expect(run == baseline)
        }
    }

    // MARK: - Larger bounded cases (N <= 8, S <= 5, K <= 5)

    @Test("N=5, S=4, K=4 with varied topology")
    func largerBounded_5_4_4() {
        let jobs = (0..<5).map { i in
            CAPendingPickJob(
                token: "t\(i)",
                job: CAJob(id: "j\(i)", arrivalAtMS: Double(i * 3), enqueueOrder: i, frameCount: Double(50 + i * 40))
            )
        }
        let machines = [
            makeMachine(id: "m0", slotCount: 2, msPerFrame: 1.0, slotReadyAtMS: [0, 15]),
            makeMachine(id: "m1", slotCount: 2, msPerFrame: 2.5, slotReadyAtMS: [0, 0]),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 4, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 4, readyPolicy: .includeFutureReady)
    }

    @Test("N=6, S=5, K=5 with asymmetric machines")
    func largerBounded_6_5_5() {
        let jobs = (0..<6).map { i in
            CAPendingPickJob(
                token: "t\(i)",
                job: CAJob(id: "j\(i)", arrivalAtMS: 0, enqueueOrder: i, frameCount: Double(30 + i * 50))
            )
        }
        let machines = [
            CAMachine(
                id: "fast",
                slots: [CASlot(id: "s0", readyAtMS: 0), CASlot(id: "s1", readyAtMS: 0)],
                msPerFrameC1: 1.0,
                degradationCurve: [
                    .init(concurrency: 1, ratioToC1: 1.0),
                    .init(concurrency: 2, ratioToC1: 1.5),
                ],
                txInMS: 0
            ),
            CAMachine(
                id: "medium",
                slots: [CASlot(id: "s0", readyAtMS: 0), CASlot(id: "s1", readyAtMS: 0)],
                msPerFrameC1: 2.5,
                degradationCurve: [
                    .init(concurrency: 1, ratioToC1: 1.0),
                    .init(concurrency: 2, ratioToC1: 1.8),
                ],
                txInMS: 10
            ),
            CAMachine(
                id: "slow",
                slots: [CASlot(id: "s0", readyAtMS: 0)],
                msPerFrameC1: 5.0,
                degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 20
            ),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 5, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 5, readyPolicy: .includeFutureReady)
    }

    @Test("N=8, S=3, K=3 bounded case")
    func largerBounded_8_3_3() {
        let jobs = (0..<8).map { i in
            CAPendingPickJob(
                token: "t\(i)",
                job: CAJob(id: "j\(i)", arrivalAtMS: Double(i % 3) * 5, enqueueOrder: i, frameCount: Double(20 + i * 30))
            )
        }
        let machines = [
            makeMachine(id: "m0", slotCount: 1, msPerFrame: 1.5),
            makeMachine(id: "m1", slotCount: 1, msPerFrame: 3.0),
            makeMachine(id: "m2", slotCount: 1, msPerFrame: 5.0),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .readyNowOnly)
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 3, readyPolicy: .includeFutureReady)
    }

    // MARK: - projectedMachines deep equality

    @Test("projectedMachines match exactly after multi-pick")
    func projectedMachinesDeepEquality() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 0, enqueueOrder: 0, frameCount: 100)),
            CAPendingPickJob(token: "b", job: CAJob(id: "j1", arrivalAtMS: 0, enqueueOrder: 1, frameCount: 80)),
        ]
        let machines = [
            makeMachine(id: "m0", slotCount: 2, msPerFrame: 2.0),
            makeMachine(id: "m1", slotCount: 1, msPerFrame: 3.0),
        ]
        let live = callPickBatch(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
        let oracle = callPickBatchOracle(jobs: jobs, machines: machines, nowMS: 0, maxCount: 2, readyPolicy: .readyNowOnly)
        #expect(live.projectedMachines == oracle.projectedMachines)
        for (lm, om) in zip(live.projectedMachines, oracle.projectedMachines) {
            #expect(lm.id == om.id)
            #expect(lm.slots.count == om.slots.count)
            for (ls, os) in zip(lm.slots, om.slots) {
                #expect(ls.id == os.id)
                #expect(ls.readyAtMS == os.readyAtMS)
                #expect(ls.isDown == os.isDown)
            }
            #expect(lm.msPerFrameC1 == om.msPerFrameC1)
            #expect(lm.fixedOverheadMS == om.fixedOverheadMS)
            #expect(lm.txInMS == om.txInMS)
            #expect(lm.txOutMS == om.txOutMS)
            #expect(lm.publishOverheadMS == om.publishOverheadMS)
        }
    }

    // MARK: - nowMS variations

    @Test("non-zero nowMS affects tReadyInputMS correctly")
    func nonZeroNowMS() {
        let jobs = [
            CAPendingPickJob(token: "a", job: CAJob(id: "j0", arrivalAtMS: 50, enqueueOrder: 0, frameCount: 100)),
        ]
        let machines = [
            CAMachine(
                id: "remote",
                slots: [CASlot(id: "s0", readyAtMS: 0)],
                msPerFrameC1: 2.0,
                degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
                txInMS: 30
            ),
        ]
        assertLiveMatchesOracle(jobs: jobs, machines: machines, nowMS: 100, maxCount: 1, readyPolicy: .readyNowOnly)
    }
}

// MARK: - Helpers

private func makeMachine(
    id: String,
    slotCount: Int,
    msPerFrame: Double,
    slotReadyAtMS: [Double]? = nil
) -> CAMachine {
    let slots = (0..<slotCount).map { i in
        CASlot(id: "\(id)-s\(i)", readyAtMS: slotReadyAtMS.map { i < $0.count ? $0[i] : 0 } ?? 0)
    }
    return CAMachine(
        id: id,
        slots: slots,
        msPerFrameC1: msPerFrame,
        degradationCurve: [.init(concurrency: 1, ratioToC1: 1.0)],
        txInMS: 0
    )
}

private func callPickBatch(
    jobs: [CAPendingPickJob<String>],
    machines: [CAMachine],
    nowMS: Double,
    maxCount: Int,
    readyPolicy: CAReadyPolicy
) -> CAPickResult<String> {
    ComplexityAwareScheduler.pickBatch(
        pendingJobs: jobs,
        machines: machines,
        nowMS: nowMS,
        maxCount: maxCount,
        readyPolicy: readyPolicy
    )
}

private func callPickBatchOracle(
    jobs: [CAPendingPickJob<String>],
    machines: [CAMachine],
    nowMS: Double,
    maxCount: Int,
    readyPolicy: CAReadyPolicy
) -> CAPickResult<String> {
    ComplexityAwareScheduler.pickBatchOracle(
        pendingJobs: jobs,
        machines: machines,
        nowMS: nowMS,
        maxCount: maxCount,
        readyPolicy: readyPolicy
    )
}

private func assertLiveMatchesOracle(
    jobs: [CAPendingPickJob<String>],
    machines: [CAMachine],
    nowMS: Double,
    maxCount: Int,
    readyPolicy: CAReadyPolicy,
    sourceLocation: SourceLocation = #_sourceLocation
) {
    let live = callPickBatch(jobs: jobs, machines: machines, nowMS: nowMS, maxCount: maxCount, readyPolicy: readyPolicy)
    let oracle = callPickBatchOracle(jobs: jobs, machines: machines, nowMS: nowMS, maxCount: maxCount, readyPolicy: readyPolicy)
    assertOracleMatchesLive(live: live, oracle: oracle, sourceLocation: sourceLocation)
}

private func assertOracleMatchesLive(
    live: CAPickResult<String>,
    oracle: CAPickResult<String>,
    sourceLocation: SourceLocation = #_sourceLocation
) {
    #expect(live.picks.count == oracle.picks.count, "pick count mismatch", sourceLocation: sourceLocation)
    for (i, (lp, op)) in zip(live.picks, oracle.picks).enumerated() {
        #expect(lp.token == op.token, "token mismatch at pick \(i)", sourceLocation: sourceLocation)
        #expect(lp.slot == op.slot, "slot mismatch at pick \(i)", sourceLocation: sourceLocation)
        #expect(lp.score == op.score, "score mismatch at pick \(i)", sourceLocation: sourceLocation)
        #expect(lp.excludedSlotWasCleared == op.excludedSlotWasCleared, "excludedSlotWasCleared mismatch at pick \(i)", sourceLocation: sourceLocation)
    }
    #expect(live.clearedExcludedTokens == oracle.clearedExcludedTokens, "clearedExcludedTokens mismatch", sourceLocation: sourceLocation)
    #expect(live.projectedMachines == oracle.projectedMachines, "projectedMachines mismatch", sourceLocation: sourceLocation)
}
