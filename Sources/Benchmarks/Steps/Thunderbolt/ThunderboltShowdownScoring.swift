import Foundation
import KikoMediaCore

typealias ShowdownScore = BenchmarkShowdownScore
typealias ThunderboltShowdownComparatorDecision = BenchmarkShowdownComparatorDecision
typealias ThunderboltShowdownComparatorMetrics = BenchmarkShowdownComparatorMetrics
typealias ThunderboltShowdownVerdict = BenchmarkShowdownVerdict

struct ThunderboltShowdownProfileResult {
    let profile: CAArrivalProfile
    let fifo: ThunderboltShowdownPolicyAggregate
    let ca: ThunderboltShowdownPolicyAggregate
    let sequences: [ThunderboltShowdownSequenceResult]

    var winner: CASchedulerPolicy {
        showdownWinnerPolicy(fifo: fifo, ca: ca)
    }
}

struct ThunderboltShowdownSequenceResult {
    let order: String
    let first: ThunderboltCAObservedRun
    let second: ThunderboltCAObservedRun

    var fifoRun: ThunderboltCARunResult {
        if first.result.schedulerPolicy == CASchedulerPolicy.fifo.rawValue {
            return first.result
        }
        return second.result
    }

    var caRun: ThunderboltCARunResult {
        if first.result.schedulerPolicy == CASchedulerPolicy.complexityAware.rawValue {
            return first.result
        }
        return second.result
    }

    var fifoObserved: ThunderboltCAObservedRun {
        if first.result.schedulerPolicy == CASchedulerPolicy.fifo.rawValue {
            return first
        }
        return second
    }

    var caObserved: ThunderboltCAObservedRun {
        if first.result.schedulerPolicy == CASchedulerPolicy.complexityAware.rawValue {
            return first
        }
        return second
    }
}

struct ThunderboltShowdownPolicyAggregate {
    let runs: [ThunderboltCAObservedRun]

    var avgSumWSeconds: Double {
        average { $0.result.metrics.sumWSeconds }
    }

    var avgP95Seconds: Double {
        average { $0.result.metrics.p95Seconds }
    }

    var avgMakespanSeconds: Double {
        average { $0.result.metrics.makespanSeconds }
    }

    var avgFailedCount: Double {
        average { Double($0.result.failedCount) }
    }

    var totalJobsAcrossRuns: Int {
        runs.reduce(0) { $0 + $1.result.totalJobs }
    }

    var successfulJobsAcrossRuns: Int {
        runs.reduce(0) { $0 + $1.result.successfulJobs }
    }

    private func average(_ value: (ThunderboltCAObservedRun) -> Double) -> Double {
        guard !runs.isEmpty else { return 0 }
        let sum = runs.reduce(0) { $0 + value($1) }
        return sum / Double(runs.count)
    }
}

func showdownComparatorMetrics(
    _ aggregate: ThunderboltShowdownPolicyAggregate
) -> ThunderboltShowdownComparatorMetrics {
    ThunderboltShowdownComparatorMetrics(
        failedCount: aggregate.avgFailedCount,
        sumWSeconds: aggregate.avgSumWSeconds,
        p95Seconds: aggregate.avgP95Seconds,
        makespanSeconds: aggregate.avgMakespanSeconds
    )
}

actor ThunderboltShowdownProgressReporter {
    private let totalRuns: Int
    private let milestones = [25, 50, 75]
    private var nextMilestoneIndexByRun: [Int: Int] = [:]
    private var lastFailedByRun: [Int: Int] = [:]

    init(totalRuns: Int) {
        self.totalRuns = totalRuns
    }

    func beginRun(
        run: Int,
        profile: CAArrivalProfile,
        policy: CASchedulerPolicy,
        sequenceLabel: String,
        totalJobs: Int
    ) {
        nextMilestoneIndexByRun[run] = 0
        lastFailedByRun[run] = 0
        writeTransient(
            statusLine(
                run: run,
                profile: profile,
                policy: policy,
                sequenceLabel: sequenceLabel,
                completed: 0,
                total: max(totalJobs, 0),
                failed: 0,
                elapsedSeconds: nil
            )
        )
    }

    func updateRun(
        run: Int,
        profile: CAArrivalProfile,
        policy: CASchedulerPolicy,
        sequenceLabel: String,
        completed: Int,
        total: Int,
        failed: Int,
        elapsedSeconds: Double
    ) {
        guard total > 0 else { return }
        guard completed < total else { return }

        let previousFailed = lastFailedByRun[run] ?? 0
        let failedChanged = failed != previousFailed
        var milestoneTriggered = false
        if var nextIndex = nextMilestoneIndexByRun[run], nextIndex < milestones.count {
            let percent = Int((Double(completed) / Double(total)) * 100)
            if percent >= milestones[nextIndex] {
                milestoneTriggered = true
                while nextIndex < milestones.count, percent >= milestones[nextIndex] {
                    nextIndex += 1
                }
                nextMilestoneIndexByRun[run] = nextIndex
            }
        }

        let shouldPrint = milestoneTriggered || failedChanged
        guard shouldPrint else { return }
        lastFailedByRun[run] = failed
        writeTransient(
            statusLine(
                run: run,
                profile: profile,
                policy: policy,
                sequenceLabel: sequenceLabel,
                completed: completed,
                total: total,
                failed: failed,
                elapsedSeconds: elapsedSeconds
            )
        )
    }

    func finishRun(
        run: Int,
        profile: CAArrivalProfile,
        policy: CASchedulerPolicy,
        sequenceLabel: String,
        result: ThunderboltCARunResult
    ) {
        clearTransient()
        BenchOutput.line(
            statusLine(
                run: run,
                profile: profile,
                policy: policy,
                sequenceLabel: sequenceLabel,
                completed: result.successfulJobs,
                total: result.totalJobs,
                failed: result.failedCount,
                elapsedSeconds: result.metrics.makespanSeconds
            )
        )
        nextMilestoneIndexByRun.removeValue(forKey: run)
        lastFailedByRun.removeValue(forKey: run)
    }

    private func statusLine(
        run: Int,
        profile: CAArrivalProfile,
        policy: CASchedulerPolicy,
        sequenceLabel: String,
        completed: Int,
        total: Int,
        failed: Int,
        elapsedSeconds: Double?
    ) -> String {
        let label = "[\(run)/\(totalRuns)] \(profile.rawValue) · \(sequenceLabel) · \(showdownPolicyLabel(policy))"
        let elapsedSuffix = elapsedSeconds.map { String(format: " · %.1fs elapsed", $0) } ?? ""
        let percent = total > 0 ? Int((Double(completed) / Double(total)) * 100.0) : 0
        return "  \(label)   \(completed)/\(total) · \(percent)% · \(failed) failed\(elapsedSuffix)"
    }

    private func writeTransient(_ text: String) {
        BenchOutput.write("\r\u{1B}[2K")
        BenchOutput.write(text)
    }

    private func clearTransient() {
        BenchOutput.write("\r\u{1B}[2K")
    }
}

func showdownPolicyLabel(_ policy: CASchedulerPolicy) -> String {
    switch policy {
    case .fifo:
        return "FIFO"
    case .complexityAware:
        return "CA"
    }
}

func showdownPair(_ fifoValue: Double, _ caValue: Double) -> String {
    String(format: "%.3f / %.3f", fifoValue, caValue)
}

func showdownDelta(_ value: Double) -> String {
    String(format: "%+.3fs", value)
}

func showdownWinnerPolicy(
    fifo: ThunderboltShowdownPolicyAggregate,
    ca: ThunderboltShowdownPolicyAggregate
) -> CASchedulerPolicy {
    showdownWinnerPolicy(
        fifoMetrics: ThunderboltShowdownComparatorMetrics(
            failedCount: fifo.avgFailedCount,
            sumWSeconds: fifo.avgSumWSeconds,
            p95Seconds: fifo.avgP95Seconds,
            makespanSeconds: fifo.avgMakespanSeconds
        ),
        caMetrics: ThunderboltShowdownComparatorMetrics(
            failedCount: ca.avgFailedCount,
            sumWSeconds: ca.avgSumWSeconds,
            p95Seconds: ca.avgP95Seconds,
            makespanSeconds: ca.avgMakespanSeconds
        )
    )
}

func showdownWinnerPolicy(
    fifoMetrics: ThunderboltShowdownComparatorMetrics,
    caMetrics: ThunderboltShowdownComparatorMetrics
) -> CASchedulerPolicy {
    BenchmarkShowdownPolicyKernel.winner(
        fifoMetrics: fifoMetrics,
        caMetrics: caMetrics
    ).benchmarkSchedulerPolicy
}

func showdownMetricScore(
    _ rows: [ThunderboltShowdownProfileResult],
    values: (ThunderboltShowdownProfileResult) -> (Double, Double)
) -> ShowdownScore {
    var score = ShowdownScore()
    for row in rows {
        let metric = values(row)
        if showdownNearlyEqual(metric.0, metric.1) {
            score.ties += 1
        } else if metric.0 < metric.1 {
            score.fifo += 1
        } else {
            score.ca += 1
        }
    }
    return score
}

func showdownProfileWinnerScore(_ rows: [ThunderboltShowdownProfileResult]) -> ShowdownScore {
    var score = ShowdownScore()
    for row in rows {
        switch row.winner {
        case .fifo:
            score.fifo += 1
        case .complexityAware:
            score.ca += 1
        }
    }
    return score
}

func showdownWinnerLabel(_ score: ShowdownScore) -> String {
    if score.fifo > score.ca {
        return "FIFO (\(score.fifo)/\(score.fifo + score.ca + score.ties) profiles)"
    }
    if score.ca > score.fifo {
        return "CA (\(score.ca)/\(score.fifo + score.ca + score.ties) profiles)"
    }
    return "tie (\(score.fifo)-\(score.ca), \(score.ties) tied)"
}

func showdownGuidanceLines(
    sumWScore: ShowdownScore,
    p95Score: ShowdownScore,
    wallScore: ShowdownScore,
    profileWins: ShowdownScore,
    totalFailedAcrossRuns: Int,
    preflight: ThunderboltShowdownPreflightClassification
) -> [String] {
    showdownGuidanceLines(
        verdict: showdownVerdict(
            sumWScore: sumWScore,
            p95Score: p95Score,
            wallScore: wallScore,
            profileWins: profileWins,
            totalFailedAcrossRuns: totalFailedAcrossRuns,
            preflight: preflight
        )
    )
}

func showdownVerdict(
    aggregateDecision: ThunderboltShowdownComparatorDecision,
    sumWScore: ShowdownScore,
    p95Score: ShowdownScore,
    wallScore: ShowdownScore,
    profileWins: ShowdownScore,
    totalFailedAcrossRuns: Int,
    preflight: ThunderboltShowdownPreflightClassification
) -> ThunderboltShowdownVerdict {
    BenchmarkShowdownPolicyKernel.verdict(
        aggregateDecision: aggregateDecision,
        sumWScore: sumWScore,
        p95Score: p95Score,
        wallScore: wallScore,
        profileWins: profileWins,
        totalFailedAcrossRuns: totalFailedAcrossRuns,
        preflight: preflight.sharedBenchmarkPreflightClassification
    )
}

func showdownVerdict(
    sumWScore: ShowdownScore,
    p95Score: ShowdownScore,
    wallScore: ShowdownScore,
    profileWins: ShowdownScore,
    totalFailedAcrossRuns: Int,
    preflight: ThunderboltShowdownPreflightClassification
) -> ThunderboltShowdownVerdict {
    BenchmarkShowdownPolicyKernel.verdict(
        sumWScore: sumWScore,
        p95Score: p95Score,
        wallScore: wallScore,
        profileWins: profileWins,
        totalFailedAcrossRuns: totalFailedAcrossRuns,
        preflight: preflight.sharedBenchmarkPreflightClassification
    )
}

func showdownGuidanceLines(verdict: ThunderboltShowdownVerdict) -> [String] {
    switch verdict.guidance {
    case .stabilizeReliability:
        return [
            "Stabilize reliability before switching policies; investigate failed jobs first.",
            "Rerun showdown after reliability is back to 100%.",
        ]

    case .keepFIFO(let requiresPriorRefresh):
        if requiresPriorRefresh {
            return [
                "Keep FIFO in production for now (set VIDEO_SCHEDULER_POLICY=fifo).",
                "Close prior coverage gaps and rerun with --refresh-prior-before-showdown --promote-prior.",
            ]
        }
        return [
            "Keep FIFO in production for now (set VIDEO_SCHEDULER_POLICY=fifo).",
            "If you want CA, rerun showdown with a larger representative corpus.",
        ]

    case .enableCA:
        return [
            "Enable CA in production (set VIDEO_SCHEDULER_POLICY=auto; keep FIFO fallback available).",
            "Refresh benchmark-prior after hardware, OS, or preset changes.",
        ]

    case .inconclusive(let requiresPriorRefresh):
        if requiresPriorRefresh {
            return [
                "Treat this as inconclusive; keep the current policy.",
                "Close prior coverage gaps, then rerun showdown.",
            ]
        }
        return [
            "Treat this as inconclusive; keep the current policy.",
            "Rerun showdown with more videos/profiles before changing scheduler policy.",
        ]
    }
}

func showdownNearlyEqual(_ lhs: Double, _ rhs: Double) -> Bool {
    abs(lhs - rhs) < 0.000_001
}

private extension BenchmarkShowdownSelection {
    var benchmarkSchedulerPolicy: CASchedulerPolicy {
        switch self {
        case .fifo:
            return .fifo
        case .complexityAware:
            return .complexityAware
        }
    }
}

private extension ThunderboltShowdownPreflightClassification {
    var sharedBenchmarkPreflightClassification: BenchmarkPriorPreflightClassification {
        switch self {
        case .healthy:
            return .healthy
        case .localPriorGap:
            return .localPriorGap
        case .remotePriorGap:
            return .remotePriorGap
        case .localAndRemotePriorGap:
            return .localAndRemotePriorGap
        }
    }
}
