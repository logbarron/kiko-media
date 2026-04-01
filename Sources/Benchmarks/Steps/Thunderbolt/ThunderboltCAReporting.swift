import Foundation
import KikoMediaCore

struct ThunderboltCAAdaptationAggregateRow {
    let machineID: String
    let avgCompletions: Double
    let avgInitialMSPerFrameC1: Double
    let avgFinalMSPerFrameC1: Double
}

struct ThunderboltCAPredictionBucketRow {
    let label: String
    let count: Int
    let meanRatio: Double
    let meanAbsErrorPercent: Double
}

func resolveWorkerLabel(_ id: String, workerLabels: [String: String]) -> String {
    if let label = workerLabels[id] { return label }
    if let colon = id.lastIndex(of: ":") {
        let host = String(id[id.startIndex..<colon])
        if let label = workerLabels[host] { return label }
    }
    return id
}

func printThunderboltCAModelEligibilitySummary(
    diagnostics: ThunderboltCAModelDiagnostics,
    modelInputs: [ThunderboltCAModelInputRow],
    workerLabels: [String: String] = [:]
) {
    BenchmarkRuntimeRenderer.printField("Model mode", diagnostics.mode.rawValue)
    BenchmarkRuntimeRenderer.printField(
        "Remote coverage",
        "\(diagnostics.modeledWorkerCount)/\(diagnostics.reachableWorkerCount) workers, " +
        "\(diagnostics.modeledSlotCount)/\(diagnostics.reachableSlotCount) slots"
    )
    BenchmarkRuntimeRenderer.printField(
        "Executable capacity",
        "\(diagnostics.totalExecutableSlotCount) total " +
            "(\(diagnostics.localExecutableSlotCount) local + \(diagnostics.remoteExecutableSlotCount) remote)"
    )
    BenchmarkRuntimeRenderer.printField(
        "Confidence tiers",
        "exact=\(diagnostics.exactPriorSlotCount), " +
            "compatible=\(diagnostics.hardwareCompatiblePriorSlotCount), " +
            "caps=\(diagnostics.capabilityBackedSlotCount), " +
            "local=\(diagnostics.localFallbackSlotCount)"
    )
    if diagnostics.fallbackActive {
        BenchOutput.line("  Warning: fallback model active (non-prior sources used for one or more remotes).")
    }

    if diagnostics.coverageRows.isEmpty {
        BenchOutput.line("  No reachable remote workers detected for CA modeling.")
    } else {
        let coverageColumns = [
            BenchmarkRuntimeTableColumn(header: "remote", width: 22),
            BenchmarkRuntimeTableColumn(header: "reachable", width: 9, alignment: .right),
            BenchmarkRuntimeTableColumn(header: "exec", width: 5, alignment: .right),
            BenchmarkRuntimeTableColumn(header: "modeled", width: 7, alignment: .right),
            BenchmarkRuntimeTableColumn(header: "tier", width: 21),
            BenchmarkRuntimeTableColumn(header: "mult", width: 4, alignment: .right),
            BenchmarkRuntimeTableColumn(header: "cap", width: 3, alignment: .right),
            BenchmarkRuntimeTableColumn(header: "ms source", width: 18),
            BenchmarkRuntimeTableColumn(header: "curve source", width: 15),
            BenchmarkRuntimeTableColumn(header: "status", width: 34),
        ]
        BenchmarkRuntimeRenderer.printTableHeader(coverageColumns)
        for row in diagnostics.coverageRows {
            BenchmarkRuntimeRenderer.printTableRow(
                [
                    resolveWorkerLabel(row.host, workerLabels: workerLabels),
                    "\(row.reachableSlots)",
                    "\(row.executableSlots)",
                    "\(row.modeledSlots)",
                    row.confidenceTier ?? "-",
                    row.modeledSlots > 0 ? String(format: "%.2f", row.confidenceMultiplier) : "-",
                    row.concurrencyCap.map(String.init) ?? "-",
                    row.msSource,
                    row.curveSource,
                    row.note,
                ],
                columns: coverageColumns
            )
        }
    }

    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printBody("Machine model sources")
    printThunderboltCAModelInputTable(modelInputs, workerLabels: workerLabels)
}

func printThunderboltCAModelInputTable(_ modelInputs: [ThunderboltCAModelInputRow], workerLabels: [String: String] = [:]) {
    let modelColumns = [
        BenchmarkRuntimeTableColumn(header: "machine", width: 18),
        BenchmarkRuntimeTableColumn(header: "slots", width: 5, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "ms/f@c1", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "tier", width: 21),
        BenchmarkRuntimeTableColumn(header: "mult", width: 4, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "cap", width: 3, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "ms source", width: 18),
        BenchmarkRuntimeTableColumn(header: "curve source", width: 21),
        BenchmarkRuntimeTableColumn(header: "tx_in", width: 5, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "tx_out", width: 6, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "publish", width: 7, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(modelColumns)
    for row in modelInputs {
        BenchmarkRuntimeRenderer.printTableRow(
            [
                resolveWorkerLabel(row.machineID, workerLabels: workerLabels),
                "\(row.slotCount)",
                String(format: "%.3f", row.msPerFrameC1),
                row.confidenceTier ?? "-",
                String(format: "%.2f", row.confidenceMultiplier),
                row.concurrencyCap.map(String.init) ?? "-",
                row.msSource,
                row.curveSource,
                String(format: "%.1f", row.txInMS),
                String(format: "%.1f", row.txOutMS),
                String(format: "%.1f", row.publishOverheadMS),
            ],
            columns: modelColumns
        )
    }
}

func printThunderboltCAObservability(
    title: String,
    modelInputs: [ThunderboltCAModelInputRow],
    adaptationRuns: [ThunderboltCAObservedRun],
    predictionRuns: [ThunderboltCAObservedRun],
    includePrediction: Bool,
    includeModelInputs: Bool = true,
    workerLabels: [String: String] = [:]
) {
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printBody(title)
    if includeModelInputs {
        printThunderboltCAModelInputTable(modelInputs, workerLabels: workerLabels)
    }

    let adaptationRows = aggregateCAAdaptationRows(adaptationRuns)
    BenchmarkRuntimeRenderer.printBody("Adaptation")
    let adaptationColumns = [
        BenchmarkRuntimeTableColumn(header: "machine", width: 18),
        BenchmarkRuntimeTableColumn(header: "jobs", width: 5, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "init ms/f", width: 9, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "final ms/f", width: 10, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "delta", width: 9, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "init_left", width: 9, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(adaptationColumns)
    for row in adaptationRows {
        let deltaRatio = row.avgInitialMSPerFrameC1 > 0
            ? ((row.avgFinalMSPerFrameC1 / row.avgInitialMSPerFrameC1) - 1.0) * 100.0
            : 0
        let initLeft = pow(0.8, row.avgCompletions)
        BenchmarkRuntimeRenderer.printTableRow(
            [
                resolveWorkerLabel(row.machineID, workerLabels: workerLabels),
                String(format: "%.1f", row.avgCompletions),
                String(format: "%.3f", row.avgInitialMSPerFrameC1),
                String(format: "%.3f", row.avgFinalMSPerFrameC1),
                String(format: "%+.1f%%", deltaRatio),
                String(format: "%.2f", initLeft),
            ],
            columns: adaptationColumns
        )
    }

    guard includePrediction else { return }
    let predictionSummary = aggregateCAPredictionBuckets(predictionRuns)
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printBody("Prediction accuracy (decision E2E)")
    let predictionColumns = [
        BenchmarkRuntimeTableColumn(header: "bucket", width: 12),
        BenchmarkRuntimeTableColumn(header: "jobs", width: 5, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "share", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "mean ratio", width: 10, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "mean abs err", width: 12, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(predictionColumns)
    for bucket in predictionSummary.rows {
        let share = predictionSummary.included > 0
            ? (Double(bucket.count) / Double(predictionSummary.included)) * 100.0
            : 0
        BenchmarkRuntimeRenderer.printTableRow(
            [
                bucket.label,
                "\(bucket.count)",
                String(format: "%.1f%%", share),
                String(format: "%.2fx", bucket.meanRatio),
                String(format: "%.1f%%", bucket.meanAbsErrorPercent),
            ],
            columns: predictionColumns
        )
    }
    BenchmarkRuntimeRenderer.printBody(
        "Coverage: \(predictionSummary.included)/\(predictionSummary.total) included " +
        "(\(predictionSummary.failed) failed, \(predictionSummary.mismatch) mismatch, \(predictionSummary.noModel) no-model)"
    )

    let solverAggregate = aggregateCASolverTelemetry(predictionRuns)
    if solverAggregate.invocations > 0 {
        BenchOutput.line("")
        BenchmarkRuntimeRenderer.printBody("Solver telemetry")
        BenchmarkRuntimeRenderer.printField("invocations", "\(solverAggregate.invocations)")
        BenchmarkRuntimeRenderer.printField("nodes visited (total)", "\(solverAggregate.totalNodesVisited)")
        BenchmarkRuntimeRenderer.printField("pruned by pick count", "\(solverAggregate.totalPrunedByPickCount)")
        BenchmarkRuntimeRenderer.printField("pruned by makespan", "\(solverAggregate.totalPrunedByMakespan)")
        BenchmarkRuntimeRenderer.printField("pruned by completion sum", "\(solverAggregate.totalPrunedByCompletionSum)")
        BenchmarkRuntimeRenderer.printField("incumbent updates", "\(solverAggregate.totalIncumbentUpdates)")
        BenchmarkRuntimeRenderer.printField("max depth", "\(solverAggregate.maxDepth)")
        BenchmarkRuntimeRenderer.printField("solver wall (total)", String(format: "%.3fms", solverAggregate.totalSolverWallMS))
    }
}

func aggregateCAAdaptationRows(
    _ runs: [ThunderboltCAObservedRun]
) -> [ThunderboltCAAdaptationAggregateRow] {
    var byMachine: [String: (sumCompletions: Double, sumInitial: Double, sumFinal: Double, count: Int)] = [:]

    for run in runs {
        for row in run.observability.adaptation {
            var aggregate = byMachine[row.machineID] ?? (0, 0, 0, 0)
            aggregate.sumCompletions += Double(row.completions)
            aggregate.sumInitial += row.initialMSPerFrameC1
            aggregate.sumFinal += row.finalMSPerFrameC1
            aggregate.count += 1
            byMachine[row.machineID] = aggregate
        }
    }

    return byMachine.keys.sorted().map { machineID in
        let aggregate = byMachine[machineID] ?? (0, 0, 0, 0)
        let divisor = max(1, aggregate.count)
        return ThunderboltCAAdaptationAggregateRow(
            machineID: machineID,
            avgCompletions: aggregate.sumCompletions / Double(divisor),
            avgInitialMSPerFrameC1: aggregate.sumInitial / Double(divisor),
            avgFinalMSPerFrameC1: aggregate.sumFinal / Double(divisor)
        )
    }
}

func predictionBucketLabel(_ ratio: Double) -> String {
    if ratio < 0.80 {
        return "<0.80x"
    }
    if ratio < 0.95 {
        return "0.80-0.95x"
    }
    if ratio <= 1.05 {
        return "0.95-1.05x"
    }
    if ratio <= 1.20 {
        return "1.05-1.20x"
    }
    return ">1.20x"
}

func predictionBucketOrder(_ label: String) -> Int {
    switch label {
    case "<0.80x":
        return 0
    case "0.80-0.95x":
        return 1
    case "0.95-1.05x":
        return 2
    case "1.05-1.20x":
        return 3
    case ">1.20x":
        return 4
    default:
        return 99
    }
}

func aggregateCAPredictionBuckets(
    _ runs: [ThunderboltCAObservedRun]
) -> (
    rows: [ThunderboltCAPredictionBucketRow],
    total: Int,
    included: Int,
    failed: Int,
    mismatch: Int,
    noModel: Int
) {
    let samples = runs.flatMap { $0.observability.predictions }
    var failed = 0
    var mismatch = 0
    var noModel = 0
    var buckets: [String: (count: Int, sumRatio: Double, sumAbsError: Double)] = [:]

    for sample in samples {
        guard sample.success else {
            failed += 1
            continue
        }
        guard !sample.executorMismatch else {
            mismatch += 1
            continue
        }
        guard let predicted = sample.predictedDoneMS,
              let actual = sample.actualDoneMS,
              predicted.isFinite, actual.isFinite,
              predicted > 0, actual > 0 else {
            noModel += 1
            continue
        }

        let ratio = actual / predicted
        let label = predictionBucketLabel(ratio)
        var bucket = buckets[label] ?? (0, 0, 0)
        bucket.count += 1
        bucket.sumRatio += ratio
        bucket.sumAbsError += abs(ratio - 1.0) * 100.0
        buckets[label] = bucket
    }

    let rows = buckets.map { label, bucket in
        ThunderboltCAPredictionBucketRow(
            label: label,
            count: bucket.count,
            meanRatio: bucket.sumRatio / Double(max(1, bucket.count)),
            meanAbsErrorPercent: bucket.sumAbsError / Double(max(1, bucket.count))
        )
    }.sorted { lhs, rhs in
        predictionBucketOrder(lhs.label) < predictionBucketOrder(rhs.label)
    }

    let included = rows.reduce(0) { $0 + $1.count }
    return (
        rows: rows,
        total: samples.count,
        included: included,
        failed: failed,
        mismatch: mismatch,
        noModel: noModel
    )
}

struct ThunderboltCASolverTelemetryAggregate {
    let invocations: Int
    let totalNodesVisited: Int
    let totalPrunedByPickCount: Int
    let totalPrunedByMakespan: Int
    let totalPrunedByCompletionSum: Int
    let totalIncumbentUpdates: Int
    let maxDepth: Int
    let totalSolverWallMS: Double
}

func aggregateCASolverTelemetry(
    _ runs: [ThunderboltCAObservedRun]
) -> ThunderboltCASolverTelemetryAggregate {
    let rows = runs.flatMap { $0.observability.solverTelemetry }
    var totalNodes = 0
    var totalPickCount = 0
    var totalMakespan = 0
    var totalCompletionSum = 0
    var totalIncumbent = 0
    var maxD = 0
    var totalWall = 0.0

    for row in rows {
        totalNodes += row.nodesVisited
        totalPickCount += row.prunedByPickCount
        totalMakespan += row.prunedByMakespan
        totalCompletionSum += row.prunedByCompletionSum
        totalIncumbent += row.incumbentUpdates
        if row.maxDepth > maxD {
            maxD = row.maxDepth
        }
        totalWall += row.solverWallMS
    }

    return ThunderboltCASolverTelemetryAggregate(
        invocations: rows.count,
        totalNodesVisited: totalNodes,
        totalPrunedByPickCount: totalPickCount,
        totalPrunedByMakespan: totalMakespan,
        totalPrunedByCompletionSum: totalCompletionSum,
        totalIncumbentUpdates: totalIncumbent,
        maxDepth: maxD,
        totalSolverWallMS: totalWall
    )
}

func printThunderboltCASummary(_ result: ThunderboltCARunResult) {
    BenchmarkRuntimeRenderer.printField("Policy", result.schedulerPolicy)
    BenchmarkRuntimeRenderer.printField("Profile", result.arrivalProfile)
    BenchmarkRuntimeRenderer.printField("Jobs", "\(result.totalJobs) total, \(result.successfulJobs) successful, \(result.failedCount) failed")
    BenchmarkRuntimeRenderer.printField("sumW", String(format: "%.3fs", result.metrics.sumWSeconds))
    BenchmarkRuntimeRenderer.printField("p95", String(format: "%.3fs", result.metrics.p95Seconds))
    BenchmarkRuntimeRenderer.printField("makespan", String(format: "%.3fs", result.metrics.makespanSeconds))
}

@discardableResult
func runAndReportThunderboltCA(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    policy: CASchedulerPolicy,
    profile: CAArrivalProfile,
    modelMode: ThunderboltCAModelMode
) async throws -> ThunderboltCARunResult {
    BenchmarkRuntimeRenderer.printField("CA model mode", modelMode.rawValue)
    let setup = try await prepareThunderboltCARunSetup(
        corpus: corpus,
        preset: preset,
        hardware: hardware,
        slotOverrides: nil,
        mode: modelMode
    )
    printThunderboltCAModelEligibilitySummary(
        diagnostics: setup.diagnostics,
        modelInputs: setup.modelInputs
    )
    BenchOutput.line("")

    let observed = try await runThunderboltCA(
        corpus: corpus,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        policy: policy,
        profile: profile,
        modelMode: modelMode,
        preparedSetup: setup
    )
    let result = observed.result
    printThunderboltCASummary(result)
    printThunderboltCAObservability(
        title: "CA Observability",
        modelInputs: observed.observability.modelInputs,
        adaptationRuns: [observed],
        predictionRuns: [observed],
        includePrediction: policy == .complexityAware
    )
    return result
}

func renderThunderboltCAJSON(_ result: ThunderboltCARunResult) throws -> String {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    let data = try encoder.encode(result)
    guard let json = String(data: data, encoding: .utf8) else {
        throw ThunderboltBenchmarkJSONError.encodingFailed
    }
    return json
}

func writeThunderboltCAJSON(_ result: ThunderboltCARunResult, toPath path: String) throws {
    let fileURL = URL(fileURLWithPath: path)
    try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
    let json = try renderThunderboltCAJSON(result)
    guard let data = json.data(using: .utf8) else {
        throw ThunderboltBenchmarkJSONError.encodingFailed
    }
    try data.write(to: fileURL, options: .atomic)
}

func writeThunderboltCASummary(_ result: ThunderboltCARunResult, toPath path: String) throws {
    let fileURL = URL(fileURLWithPath: path)
    try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
    let body = """
    # CA Run Summary

    - scheduler_policy: \(result.schedulerPolicy)
    - arrival_profile: \(result.arrivalProfile)
    - total_jobs: \(result.totalJobs)
    - successful_jobs: \(result.successfulJobs)
    - failed_count: \(result.failedCount)

    ## Metrics

    - sumW_seconds: \(String(format: "%.6f", result.metrics.sumWSeconds))
    - p95_seconds: \(String(format: "%.6f", result.metrics.p95Seconds))
    - makespan_seconds: \(String(format: "%.6f", result.metrics.makespanSeconds))
    """
    try body.write(to: fileURL, atomically: true, encoding: .utf8)
}

struct ThunderboltCAAcceptanceDecision {
    let pass: Bool
    let sumWImproved: Bool
    let failedCountNonRegression: Bool
    let p95Regressed: Bool
    let makespanRegressed: Bool
}

func evaluateThunderboltCAAcceptanceGate(
    fifoMetrics: ThunderboltShowdownComparatorMetrics,
    caMetrics: ThunderboltShowdownComparatorMetrics
) -> ThunderboltCAAcceptanceDecision {
    let sharedDecision = BenchmarkShowdownPolicyKernel.comparatorDecision(
        fifoMetrics: fifoMetrics,
        caMetrics: caMetrics
    )

    return ThunderboltCAAcceptanceDecision(
        pass: sharedDecision.pass,
        sumWImproved: sharedDecision.sumWImproved,
        failedCountNonRegression: sharedDecision.failedCountNonRegression,
        p95Regressed: !sharedDecision.p95NonRegression,
        makespanRegressed: !sharedDecision.makespanNonRegression
    )
}

func writeThunderboltCAAcceptanceReport(
    _ report: ThunderboltCAAcceptanceReport,
    toPath path: String
) throws {
    let fileURL = URL(fileURLWithPath: path)
    try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    try encoder.encode(report).write(to: fileURL, options: .atomic)
}

func runAndReportThunderboltCAAcceptance(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    outputPath: String?,
    modelMode: ThunderboltCAModelMode
) async throws {
    let videos = corpus.filter { $0.type == .video }
    let expectedVideoCount = videos.count
    let corpusSignature = caAcceptanceCorpusSignature(videos: videos)
    let profiles: [CAArrivalProfile] = [
        .allAtOnce,
        .burst_1_20_5_5_1,
        .trickle,
    ]
    let policySequences: [[CASchedulerPolicy]] = [
        [.fifo, .complexityAware],
        [.complexityAware, .fifo],
    ]

    BenchmarkRuntimeRenderer.printField("Policy order", "order-neutral (FIFO->CA and CA->FIFO)")
    BenchmarkRuntimeRenderer.printField("CA model mode", modelMode.rawValue)
    let setup = try await prepareThunderboltCARunSetup(
        corpus: corpus,
        preset: preset,
        hardware: hardware,
        slotOverrides: nil,
        mode: modelMode
    )
    printThunderboltCAModelEligibilitySummary(
        diagnostics: setup.diagnostics,
        modelInputs: setup.modelInputs
    )
    BenchOutput.line("")

    var profileResults: [ThunderboltCAAcceptanceProfileResult] = []
    var failedProfiles: [String] = []

    for profile in profiles {
        BenchmarkRuntimeRenderer.printSubsectionTitle("Acceptance profile: \(profile.rawValue)")
        var fifoRuns: [ThunderboltCAObservedRun] = []
        var caRuns: [ThunderboltCAObservedRun] = []
        var sequenceResults: [ThunderboltShowdownSequenceResult] = []

        for sequence in policySequences {
            var orderedRuns: [ThunderboltCAObservedRun] = []
            orderedRuns.reserveCapacity(sequence.count)
            for policy in sequence {
                let observed = try await runThunderboltCA(
                    corpus: corpus,
                    preset: preset,
                    timeout: timeout,
                    hardware: hardware,
                    policy: policy,
                    profile: profile,
                    modelMode: modelMode,
                    preparedSetup: setup
                )
                orderedRuns.append(observed)
                switch policy {
                case .fifo:
                    fifoRuns.append(observed)
                case .complexityAware:
                    caRuns.append(observed)
                }
            }
            if orderedRuns.count == 2 {
                sequenceResults.append(
                    ThunderboltShowdownSequenceResult(
                        order: sequence.map(showdownPolicyLabel).joined(separator: "->"),
                        first: orderedRuns[0],
                        second: orderedRuns[1]
                    )
                )
            }
        }

        let fifoJobCount = fifoRuns.first?.result.jobs.count ?? 0
        let caJobCount = caRuns.first?.result.jobs.count ?? 0
        guard fifoRuns.allSatisfy({ $0.result.jobs.count == expectedVideoCount }),
              caRuns.allSatisfy({ $0.result.jobs.count == expectedVideoCount }) else {
            throw ThunderboltCAAcceptanceError.inconsistentJobCount(
                profile: profile.rawValue,
                expected: expectedVideoCount,
                fifoActual: fifoJobCount,
                caActual: caJobCount
            )
        }

        let fifoAggregate = ThunderboltShowdownPolicyAggregate(runs: fifoRuns)
        let caAggregate = ThunderboltShowdownPolicyAggregate(runs: caRuns)
        let decision = evaluateThunderboltCAAcceptanceGate(
            fifoMetrics: showdownComparatorMetrics(fifoAggregate),
            caMetrics: showdownComparatorMetrics(caAggregate)
        )
        if !decision.pass {
            failedProfiles.append(profile.rawValue)
        }

        let orderColumns = [
            BenchmarkRuntimeTableColumn(header: "order", width: 14),
            BenchmarkRuntimeTableColumn(header: "sumW (FIFO/CA)", width: 19, alignment: .right),
            BenchmarkRuntimeTableColumn(header: "p95 (FIFO/CA)", width: 17, alignment: .right),
            BenchmarkRuntimeTableColumn(header: "wall (FIFO/CA)", width: 17, alignment: .right),
        ]
        BenchmarkRuntimeRenderer.printTableHeader(orderColumns)
        for sequence in sequenceResults {
            let fifoRun = sequence.fifoRun
            let caRun = sequence.caRun
            BenchmarkRuntimeRenderer.printTableRow(
                [
                    sequence.order,
                    showdownPair(fifoRun.metrics.sumWSeconds, caRun.metrics.sumWSeconds),
                    showdownPair(fifoRun.metrics.p95Seconds, caRun.metrics.p95Seconds),
                    showdownPair(fifoRun.metrics.makespanSeconds, caRun.metrics.makespanSeconds),
                ],
                columns: orderColumns
            )
        }
        BenchOutput.line("")

        BenchmarkRuntimeRenderer.printField("FIFO sumW (avg)", String(format: "%.3fs", fifoAggregate.avgSumWSeconds))
        BenchmarkRuntimeRenderer.printField("CA sumW (avg)", String(format: "%.3fs", caAggregate.avgSumWSeconds))
        BenchmarkRuntimeRenderer.printField("FIFO failed_count (avg)", String(format: "%.2f", fifoAggregate.avgFailedCount))
        BenchmarkRuntimeRenderer.printField("CA failed_count (avg)", String(format: "%.2f", caAggregate.avgFailedCount))
        BenchmarkRuntimeRenderer.printField("Acceptance", decision.pass ? "pass" : "fail")

        printThunderboltCAObservability(
            title: "CA Observability · \(profile.rawValue)",
            modelInputs: caRuns.first?.observability.modelInputs ?? fifoRuns.first?.observability.modelInputs ?? [],
            adaptationRuns: fifoRuns + caRuns,
            predictionRuns: caRuns,
            includePrediction: true
        )

        profileResults.append(
            ThunderboltCAAcceptanceProfileResult(
                profile: profile.rawValue,
                fifo: ThunderboltCAMetricsJSON(
                    sumWSeconds: fifoAggregate.avgSumWSeconds,
                    p95Seconds: fifoAggregate.avgP95Seconds,
                    makespanSeconds: fifoAggregate.avgMakespanSeconds,
                    failedCount: Int(fifoAggregate.avgFailedCount.rounded())
                ),
                ca: ThunderboltCAMetricsJSON(
                    sumWSeconds: caAggregate.avgSumWSeconds,
                    p95Seconds: caAggregate.avgP95Seconds,
                    makespanSeconds: caAggregate.avgMakespanSeconds,
                    failedCount: Int(caAggregate.avgFailedCount.rounded())
                ),
                pass: decision.pass,
                sumWImproved: decision.sumWImproved,
                failedCountNonRegression: decision.failedCountNonRegression,
                p95Regressed: decision.p95Regressed,
                makespanRegressed: decision.makespanRegressed
            )
        )
        BenchOutput.line("")
    }

    let report = ThunderboltCAAcceptanceReport(
        generatedAt: ISO8601DateFormatter().string(from: Date()),
        corpusVideoCount: expectedVideoCount,
        corpusSignature: corpusSignature,
        profiles: profileResults,
        allPass: failedProfiles.isEmpty
    )

    if let outputPath, !outputPath.isEmpty {
        try writeThunderboltCAAcceptanceReport(report, toPath: outputPath)
    }
    if !failedProfiles.isEmpty {
        throw ThunderboltCAAcceptanceError.failedProfiles(failedProfiles)
    }
}
