import Testing
@testable import benchmarks
@testable import KikoMediaCore

@Suite("Thunderbolt showdown guidance")
struct ThunderboltShowdownGuidanceTests {
    @Test("guidance recommends CA when CA wins most scorecards")
    func guidanceRecommendsCA() {
        let lines = showdownGuidanceLines(
            sumWScore: ShowdownScore(fifo: 0, ca: 3, ties: 0),
            p95Score: ShowdownScore(fifo: 1, ca: 2, ties: 0),
            wallScore: ShowdownScore(fifo: 0, ca: 3, ties: 0),
            profileWins: ShowdownScore(fifo: 1, ca: 2, ties: 0),
            totalFailedAcrossRuns: 0,
            preflight: .healthy
        )

        #expect(lines.first?.contains("Enable CA in production") == true)
    }

    @Test("shared policy does not enable CA when p95 blocks the aggregate contract")
    func sharedPolicyRequiresP95NonRegressionBeforeEnablingCA() {
        let verdict = BenchmarkShowdownPolicyKernel.verdict(
            aggregateDecision: BenchmarkShowdownComparatorDecision(
                pass: false,
                failedCountNonRegression: true,
                makespanNonRegression: true,
                sumWImproved: true,
                p95NonRegression: false
            ),
            sumWScore: BenchmarkShowdownScore(fifo: 0, ca: 3, ties: 0),
            p95Score: BenchmarkShowdownScore(fifo: 3, ca: 0, ties: 0),
            wallScore: BenchmarkShowdownScore(fifo: 0, ca: 3, ties: 0),
            profileWins: BenchmarkShowdownScore(fifo: 0, ca: 3, ties: 0),
            totalFailedAcrossRuns: 0,
            preflight: .healthy
        )

        #expect(!verdict.comparatorPass)
        #expect(verdict.guidance == .inconclusive(requiresPriorRefresh: false))
    }

    @Test("guidance recommends FIFO plus prior refresh when FIFO wins with prior gaps")
    func guidanceRecommendsFIFOWithPriorGap() {
        let lines = showdownGuidanceLines(
            sumWScore: ShowdownScore(fifo: 3, ca: 0, ties: 0),
            p95Score: ShowdownScore(fifo: 2, ca: 1, ties: 0),
            wallScore: ShowdownScore(fifo: 3, ca: 0, ties: 0),
            profileWins: ShowdownScore(fifo: 2, ca: 1, ties: 0),
            totalFailedAcrossRuns: 0,
            preflight: .localAndRemotePriorGap
        )

        #expect(lines.first?.contains("Keep FIFO in production") == true)
        #expect(lines.last?.contains("--refresh-prior-before-showdown --promote-prior") == true)
    }

    @Test("guidance prioritizes reliability when failures occurred")
    func guidancePrioritizesReliability() {
        let lines = showdownGuidanceLines(
            sumWScore: ShowdownScore(),
            p95Score: ShowdownScore(),
            wallScore: ShowdownScore(),
            profileWins: ShowdownScore(),
            totalFailedAcrossRuns: 2,
            preflight: .healthy
        )

        #expect(lines.first?.contains("Stabilize reliability") == true)
    }

    @Test("shared policy keeps FIFO when CA only wins non-wall scorecards")
    func sharedPolicyKeepsFIFOOnWallRegression() {
        let winner = BenchmarkShowdownPolicyKernel.winner(
            fifoMetrics: BenchmarkShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 12.0,
                p95Seconds: 10.0,
                makespanSeconds: 20.0
            ),
            caMetrics: BenchmarkShowdownComparatorMetrics(
                failedCount: 0,
                sumWSeconds: 10.0,
                p95Seconds: 9.0,
                makespanSeconds: 25.0
            )
        )
        let verdict = BenchmarkShowdownPolicyKernel.verdict(
            sumWScore: BenchmarkShowdownScore(fifo: 0, ca: 1, ties: 0),
            p95Score: BenchmarkShowdownScore(fifo: 0, ca: 1, ties: 0),
            wallScore: BenchmarkShowdownScore(fifo: 1, ca: 0, ties: 0),
            profileWins: BenchmarkShowdownScore(fifo: 0, ca: 1, ties: 0),
            totalFailedAcrossRuns: 0,
            preflight: .healthy
        )

        #expect(winner == .fifo)
        #expect(verdict.guidance == .keepFIFO(requiresPriorRefresh: false))
    }
}
