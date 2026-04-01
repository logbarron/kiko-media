import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Scheduling metric math")
struct SchedulingMetricsTests {
    @Test("computes sumW, p95, makespan, and failed_count")
    func computesAllMetrics() {
        let successfulJobs: [SchedulingSuccessfulJob] = [
            .init(arriveAtSeconds: 0, liveAtSeconds: 10),   // W=10
            .init(arriveAtSeconds: 5, liveAtSeconds: 13),   // W=8
            .init(arriveAtSeconds: 6, liveAtSeconds: 20),   // W=14
        ]

        let metrics = SchedulingMetricMath.compute(
            successfulJobs: successfulJobs,
            failedCount: 2
        )

        #expect(metrics.sumWSeconds == 32)
        #expect(abs(metrics.p95Seconds - 13.6) < 0.000_001)
        #expect(metrics.makespanSeconds == 20)
        #expect(metrics.failedCount == 2)
    }

    @Test("returns zero scheduling metrics when there are no successful jobs")
    func zeroMetricsForNoSuccesses() {
        let metrics = SchedulingMetricMath.compute(
            successfulJobs: [],
            failedCount: 4
        )

        #expect(metrics.sumWSeconds == 0)
        #expect(metrics.p95Seconds == 0)
        #expect(metrics.makespanSeconds == 0)
        #expect(metrics.failedCount == 4)
    }
}
