package enum BenchmarkShowdownSelection: Sendable, Equatable {
    case fifo
    case complexityAware
}

package struct BenchmarkShowdownComparatorMetrics: Sendable, Equatable {
    package let failedCount: Double
    package let sumWSeconds: Double
    package let p95Seconds: Double
    package let makespanSeconds: Double

    package init(
        failedCount: Double,
        sumWSeconds: Double,
        p95Seconds: Double,
        makespanSeconds: Double
    ) {
        self.failedCount = failedCount
        self.sumWSeconds = sumWSeconds
        self.p95Seconds = p95Seconds
        self.makespanSeconds = makespanSeconds
    }
}

package struct BenchmarkShowdownScore: Sendable, Equatable {
    package var fifo: Int
    package var ca: Int
    package var ties: Int

    package init(fifo: Int = 0, ca: Int = 0, ties: Int = 0) {
        self.fifo = fifo
        self.ca = ca
        self.ties = ties
    }
}

package struct BenchmarkShowdownComparatorDecision: Sendable, Equatable {
    package let pass: Bool
    package let failedCountNonRegression: Bool
    package let makespanNonRegression: Bool
    package let sumWImproved: Bool
    package let p95NonRegression: Bool

    package init(
        pass: Bool,
        failedCountNonRegression: Bool,
        makespanNonRegression: Bool,
        sumWImproved: Bool,
        p95NonRegression: Bool
    ) {
        self.pass = pass
        self.failedCountNonRegression = failedCountNonRegression
        self.makespanNonRegression = makespanNonRegression
        self.sumWImproved = sumWImproved
        self.p95NonRegression = p95NonRegression
    }
}

package enum BenchmarkShowdownGuidanceDecision: Sendable, Equatable {
    case stabilizeReliability
    case keepFIFO(requiresPriorRefresh: Bool)
    case enableCA
    case inconclusive(requiresPriorRefresh: Bool)
}

package struct BenchmarkShowdownVerdict: Sendable, Equatable {
    package let guidance: BenchmarkShowdownGuidanceDecision
    package let comparatorPass: Bool

    package init(
        guidance: BenchmarkShowdownGuidanceDecision,
        comparatorPass: Bool
    ) {
        self.guidance = guidance
        self.comparatorPass = comparatorPass
    }
}

package enum BenchmarkShowdownPolicyKernel {
    package static func comparatorDecision(
        fifoMetrics: BenchmarkShowdownComparatorMetrics,
        caMetrics: BenchmarkShowdownComparatorMetrics
    ) -> BenchmarkShowdownComparatorDecision {
        let failedCountNonRegression = lessThanOrNearlyEqual(
            caMetrics.failedCount,
            fifoMetrics.failedCount
        )
        let makespanNonRegression = lessThanOrNearlyEqual(
            caMetrics.makespanSeconds,
            fifoMetrics.makespanSeconds
        )
        let sumWImproved = strictlyLessThan(
            caMetrics.sumWSeconds,
            fifoMetrics.sumWSeconds
        )
        let p95NonRegression = lessThanOrNearlyEqual(
            caMetrics.p95Seconds,
            fifoMetrics.p95Seconds
        )

        return BenchmarkShowdownComparatorDecision(
            pass: failedCountNonRegression && makespanNonRegression && sumWImproved && p95NonRegression,
            failedCountNonRegression: failedCountNonRegression,
            makespanNonRegression: makespanNonRegression,
            sumWImproved: sumWImproved,
            p95NonRegression: p95NonRegression
        )
    }

    package static func winner(
        fifoMetrics: BenchmarkShowdownComparatorMetrics,
        caMetrics: BenchmarkShowdownComparatorMetrics
    ) -> BenchmarkShowdownSelection {
        if !nearlyEqual(fifoMetrics.failedCount, caMetrics.failedCount) {
            return fifoMetrics.failedCount < caMetrics.failedCount ? .fifo : .complexityAware
        }
        if !nearlyEqual(fifoMetrics.makespanSeconds, caMetrics.makespanSeconds) {
            return fifoMetrics.makespanSeconds < caMetrics.makespanSeconds ? .fifo : .complexityAware
        }
        if !nearlyEqual(fifoMetrics.sumWSeconds, caMetrics.sumWSeconds) {
            return fifoMetrics.sumWSeconds < caMetrics.sumWSeconds ? .fifo : .complexityAware
        }
        if !nearlyEqual(fifoMetrics.p95Seconds, caMetrics.p95Seconds) {
            return fifoMetrics.p95Seconds < caMetrics.p95Seconds ? .fifo : .complexityAware
        }
        return .fifo
    }

    package static func verdict(
        aggregateDecision: BenchmarkShowdownComparatorDecision,
        sumWScore: BenchmarkShowdownScore,
        p95Score: BenchmarkShowdownScore,
        wallScore: BenchmarkShowdownScore,
        profileWins: BenchmarkShowdownScore,
        totalFailedAcrossRuns: Int,
        preflight: BenchmarkPriorPreflightClassification
    ) -> BenchmarkShowdownVerdict {
        let comparatorPass = totalFailedAcrossRuns == 0
            && aggregateDecision.pass
            && profileWins.ca > profileWins.fifo

        if totalFailedAcrossRuns > 0 {
            return BenchmarkShowdownVerdict(
                guidance: .stabilizeReliability,
                comparatorPass: false
            )
        }

        let requiresPriorRefresh = preflight != .healthy
        if comparatorPass {
            return BenchmarkShowdownVerdict(
                guidance: .enableCA,
                comparatorPass: true
            )
        }

        if wallScore.fifo > wallScore.ca {
            return BenchmarkShowdownVerdict(
                guidance: .keepFIFO(requiresPriorRefresh: requiresPriorRefresh),
                comparatorPass: comparatorPass
            )
        }

        let scorecards = [sumWScore, p95Score, wallScore, profileWins]
        let fifoWins = scorecards.reduce(0) { $0 + ($1.fifo > $1.ca ? 1 : 0) }

        if fifoWins >= 3 {
            return BenchmarkShowdownVerdict(
                guidance: .keepFIFO(requiresPriorRefresh: requiresPriorRefresh),
                comparatorPass: comparatorPass
            )
        }

        return BenchmarkShowdownVerdict(
            guidance: .inconclusive(requiresPriorRefresh: requiresPriorRefresh),
            comparatorPass: comparatorPass
        )
    }

    package static func verdict(
        sumWScore: BenchmarkShowdownScore,
        p95Score: BenchmarkShowdownScore,
        wallScore: BenchmarkShowdownScore,
        profileWins: BenchmarkShowdownScore,
        totalFailedAcrossRuns: Int,
        preflight: BenchmarkPriorPreflightClassification
    ) -> BenchmarkShowdownVerdict {
        verdict(
            aggregateDecision: BenchmarkShowdownComparatorDecision(
                pass: totalFailedAcrossRuns == 0
                    && wallScore.ca >= wallScore.fifo
                    && sumWScore.ca > sumWScore.fifo
                    && p95Score.ca >= p95Score.fifo,
                failedCountNonRegression: totalFailedAcrossRuns == 0,
                makespanNonRegression: wallScore.ca >= wallScore.fifo,
                sumWImproved: sumWScore.ca > sumWScore.fifo,
                p95NonRegression: p95Score.ca >= p95Score.fifo
            ),
            sumWScore: sumWScore,
            p95Score: p95Score,
            wallScore: wallScore,
            profileWins: profileWins,
            totalFailedAcrossRuns: totalFailedAcrossRuns,
            preflight: preflight
        )
    }

    private static func nearlyEqual(_ lhs: Double, _ rhs: Double) -> Bool {
        abs(lhs - rhs) < 0.000_001
    }

    private static func lessThanOrNearlyEqual(_ lhs: Double, _ rhs: Double) -> Bool {
        lhs < rhs || nearlyEqual(lhs, rhs)
    }

    private static func strictlyLessThan(_ lhs: Double, _ rhs: Double) -> Bool {
        lhs < rhs && !nearlyEqual(lhs, rhs)
    }
}
