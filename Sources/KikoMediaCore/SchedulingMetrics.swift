import Foundation

package struct SchedulingSuccessfulJob: Sendable, Equatable {
    package let arriveAtSeconds: Double
    package let liveAtSeconds: Double

    package init(arriveAtSeconds: Double, liveAtSeconds: Double) {
        self.arriveAtSeconds = arriveAtSeconds
        self.liveAtSeconds = liveAtSeconds
    }

    package var wallSeconds: Double {
        max(0, liveAtSeconds - arriveAtSeconds)
    }
}

package struct SchedulingMetricsSummary: Sendable, Equatable {
    package let sumWSeconds: Double
    package let p95Seconds: Double
    package let makespanSeconds: Double
    package let failedCount: Int
}

package enum SchedulingMetricMath {
    package static func compute(
        successfulJobs: [SchedulingSuccessfulJob],
        failedCount: Int
    ) -> SchedulingMetricsSummary {
        let walls = successfulJobs.map(\.wallSeconds).sorted()
        let sumWSeconds = walls.reduce(0, +)
        let p95Seconds = percentile95(sorted: walls)
        let makespanSeconds: Double = {
            guard !successfulJobs.isEmpty else { return 0 }
            let minArrive = successfulJobs.map(\.arriveAtSeconds).min() ?? 0
            let maxLive = successfulJobs.map(\.liveAtSeconds).max() ?? 0
            return max(0, maxLive - minArrive)
        }()

        return SchedulingMetricsSummary(
            sumWSeconds: sumWSeconds,
            p95Seconds: p95Seconds,
            makespanSeconds: makespanSeconds,
            failedCount: max(0, failedCount)
        )
    }

    private static func percentile95(sorted: [Double]) -> Double {
        guard !sorted.isEmpty else { return 0 }
        let index = 0.95 * Double(sorted.count - 1)
        let lower = Int(index)
        let upper = min(lower + 1, sorted.count - 1)
        let fraction = index - Double(lower)
        return sorted[lower] + fraction * (sorted[upper] - sorted[lower])
    }
}
