import Foundation
import Synchronization
import KikoMediaCore

func burstConfigLabel(_ config: ThunderboltBurstConfig, workers: [ThunderboltBoundWorkerSpec], workerLabels: [String: String]) -> String {
    var parts: [String] = ["L=\(config.localSlots)"]
    for (worker, slots) in zip(workers, config.remoteSlots) {
        let label = workerLabels[worker.host] ?? worker.host
        parts.append("\(label)=\(slots)")
    }
    return parts.joined(separator: " ")
}

func burstConfigFromOptimizerConfig(
    _ config: [Int],
    workerCount: Int
) -> ThunderboltBurstConfig? {
    guard config.count == workerCount + 1 else { return nil }
    guard config.allSatisfy({ $0 >= 0 }) else { return nil }
    return ThunderboltBurstConfig(
        localSlots: config[0],
        remoteSlots: Array(config.dropFirst())
    )
}

func burstSweepColumns(
    workerCount: Int,
    includePrediction: Bool = false
) -> [BenchmarkRuntimeTableColumn] {
    var cols: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Run", width: 7),
        BenchmarkRuntimeTableColumn(header: "Local", width: 5, alignment: .right),
    ]
    for i in 1...workerCount {
        cols.append(BenchmarkRuntimeTableColumn(header: "W\(i)", width: 4, alignment: .right))
    }
    if includePrediction {
        cols.append(BenchmarkRuntimeTableColumn(header: "Prediction", width: 10, alignment: .right))
    }
    cols.append(contentsOf: [
        BenchmarkRuntimeTableColumn(header: "Video/m", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Wall time", width: 9, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Fail", width: 4, alignment: .right),
    ])
    return cols
}

func burstSweepPhaseTag(_ phase: String) -> String {
    switch phase {
    case "profile":
        return "P1"
    case "model":
        return "P2"
    case "refine":
        return "P3"
    case "brute":
        return "BF"
    default:
        return phase
    }
}

func burstSweepPredictionCell(_ predicted: Double?) -> String {
    guard let predicted, predicted.isFinite else { return "" }
    return String(format: "%.1fs", predicted)
}

func burstSweepOptimizerRow(
    phase: String,
    config: ThunderboltBurstConfig,
    predicted: Double?,
    result: ThunderboltBurstResult?,
    elapsed: Double,
    fallbackFailedCount: Int
) -> [String] {
    let measuredWall = result?.wallSeconds ?? elapsed
    let completed = result?.completed ?? 0
    let failed = result?.failed ?? fallbackFailedCount
    let vpm = measuredWall > 0 ? (Double(completed) / measuredWall * 60) : 0
    let wallCell = String(format: "%.1fs", measuredWall)

    var cells: [String] = [
        burstSweepPhaseTag(phase),
        String(config.localSlots),
    ]
    for slots in config.remoteSlots {
        cells.append(String(slots))
    }
    cells.append(burstSweepPredictionCell(predicted))
    cells.append(contentsOf: [
        String(format: "%.1f", vpm),
        wallCell,
        String(failed),
    ])
    return cells
}

func burstSweepRow(
    run: Int,
    total: Int,
    config: ThunderboltBurstConfig,
    result: ThunderboltBurstResult
) -> [String] {
    let vpm = result.wallSeconds > 0 ? (Double(result.completed) / result.wallSeconds * 60) : 0
    var cells: [String] = [
        "\(run)/\(total)",
        String(config.localSlots),
    ]
    for slots in config.remoteSlots {
        cells.append(String(slots))
    }
    cells.append(contentsOf: [
        String(format: "%.1f", vpm),
        String(format: "%.1fs", result.wallSeconds),
        String(result.failed),
    ])
    return cells
}

func burstSweepProgressBar(done: Int, total: Int, width: Int = 20) -> String {
    let filled = total > 0 ? done * width / total : 0
    let bar = String(repeating: "\u{25B8}", count: filled) + String(repeating: "\u{00B7}", count: width - filled)
    return "\r  \(bar)"
}

func printThunderboltLeaderboard(
    workers: [ThunderboltBoundWorkerSpec],
    workerLabels: [String: String],
    runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)],
    baseline: ThunderboltBurstConfig
) {
    guard !runs.isEmpty else {
        BenchOutput.line("  No runs to rank.")
        return
    }

    let baselineWall = runs.first(where: { $0.0 == baseline })?.1.wallSeconds ?? 0
    let ranked = runs.sorted { lhs, rhs in
        let lhsSucceeded = lhs.1.failed == 0
        let rhsSucceeded = rhs.1.failed == 0
        if lhsSucceeded != rhsSucceeded {
            return lhsSucceeded && !rhsSucceeded
        }
        if lhs.1.wallSeconds != rhs.1.wallSeconds {
            return lhs.1.wallSeconds < rhs.1.wallSeconds
        }
        if lhs.0.localSlots != rhs.0.localSlots {
            return lhs.0.localSlots < rhs.0.localSlots
        }
        return lhs.0.remoteSlots.lexicographicallyPrecedes(rhs.0.remoteSlots)
    }

    let top3 = Array(ranked.prefix(3))

    BenchOutput.line("  Leaderboard (top 3)")

    let columns = [
        BenchmarkRuntimeTableColumn(header: "Config", width: 16),
        BenchmarkRuntimeTableColumn(header: "Wall", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "V/min", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95 done", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Speedup", width: 7, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for (config, result) in top3 {
        let vpm = result.wallSeconds > 0 ? (Double(result.completed) / result.wallSeconds * 60) : 0
        let p95Done = Stats(result.completionSeconds).p95
        let speedup = baselineWall > 0 ? baselineWall / result.wallSeconds : 0
        BenchmarkRuntimeRenderer.printTableRow(
            [
                burstConfigLabel(config, workers: workers, workerLabels: workerLabels),
                String(format: "%.1fs", result.wallSeconds),
                String(format: "%.1f", vpm),
                String(format: "%.1fs", p95Done),
                String(format: "%.2fx", speedup),
            ],
            columns: columns
        )
    }
    BenchOutput.line("")
}

func mbPerSecond(bytes: Int, seconds: Double) -> Double {
    guard bytes > 0, seconds > 0 else { return 0 }
    return Double(bytes) / 1_000_000 / seconds
}

func formatThunderboltCorpusSize(_ bytes: Int) -> String {
    BenchmarkByteFormatter.format(bytes)
}
