import Foundation
import KikoMediaCore

typealias ThunderboltShowdownPriorMaintenanceRunner = @Sendable (
    _ corpus: [MediaFile],
    _ preset: String,
    _ timeout: Int,
    _ hardware: HardwareProfile,
    _ setup: ThunderboltCARunSetup,
    _ slotOverrides: ThunderboltCASlotOverrides?,
    _ modelMode: ThunderboltCAModelMode,
    _ preflight: ThunderboltShowdownPreflightClassification,
    _ priorUpdatePolicy: ThunderboltPriorUpdatePolicy
) async throws -> ThunderboltShowdownPriorMaintenanceResult?

private struct ThunderboltShowdownSessionContext {
    let setup: ThunderboltCARunSetup
    let labels: [String: String]
    let preflight: ThunderboltShowdownPreflightClassification
    let priorMaintenanceResult: ThunderboltShowdownPriorMaintenanceResult?
}

private struct ThunderboltShowdownSummary {
    let aggregateDecision: ThunderboltShowdownComparatorDecision
    let sumWScore: ShowdownScore
    let p95Score: ShowdownScore
    let wallScore: ShowdownScore
    let profileWins: ShowdownScore
    let totalJobsAcrossRuns: Int
    let totalSucceededAcrossRuns: Int
    let totalFailedAcrossRuns: Int
    let verdict: ThunderboltShowdownVerdict
}

func benchmarkThunderboltMeasuredShowdown(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    profiles: [CAArrivalProfile] = CAArrivalProfile.allCases,
    slotOverrides: ThunderboltCASlotOverrides? = nil,
    remoteTxInEstimateMSByHost: [String: Double] = [:],
    modelMode: ThunderboltCAModelMode = .auto,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy = .off,
    priorTableOverlay: BenchmarkPriorTable? = nil,
    workerLabels: [String: String] = [:],
    skipPriorMaintenance: Bool = false,
    initialPriorMaintenanceResult: ThunderboltShowdownPriorMaintenanceResult? = nil,
    preparedSetup: ThunderboltCARunSetup? = nil,
    priorMaintenanceRunner: @escaping ThunderboltShowdownPriorMaintenanceRunner = {
        corpus,
        preset,
        timeout,
        hardware,
        setup,
        slotOverrides,
        modelMode,
        preflight,
        priorUpdatePolicy in
        try await runThunderboltShowdownPriorMaintenance(
            corpus: corpus,
            preset: preset,
            timeout: timeout,
            hardware: hardware,
            setup: setup,
            slotOverrides: slotOverrides,
            modelMode: modelMode,
            preflight: preflight,
            priorUpdatePolicy: priorUpdatePolicy
        )
    }
) async throws {
    let policySequences: [[CASchedulerPolicy]] = [
        [.fifo, .complexityAware],
        [.complexityAware, .fifo],
    ]
    let runsPerProfile = policySequences.reduce(0) { partial, sequence in
        partial + sequence.count
    }
    let totalRuns = profiles.count * runsPerProfile
    let totalVideoJobs = corpus.filter { $0.type == .video }.count
    let progress = ThunderboltShowdownProgressReporter(totalRuns: totalRuns)

    let session = try await prepareThunderboltShowdownSession(
        corpus: corpus,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        profiles: profiles,
        slotOverrides: slotOverrides,
        remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost,
        modelMode: modelMode,
        priorUpdatePolicy: priorUpdatePolicy,
        priorTableOverlay: priorTableOverlay,
        workerLabels: workerLabels,
        skipPriorMaintenance: skipPriorMaintenance,
        initialPriorMaintenanceResult: initialPriorMaintenanceResult,
        preparedSetup: preparedSetup,
        priorMaintenanceRunner: priorMaintenanceRunner
    )

    var runIndex = 0
    var profileResults: [ThunderboltShowdownProfileResult] = []
    profileResults.reserveCapacity(profiles.count)

    for profile in profiles {
        let profileExecution = try await runThunderboltShowdownProfile(
            corpus: corpus,
            preset: preset,
            timeout: timeout,
            hardware: hardware,
            profile: profile,
            modelMode: modelMode,
            slotOverrides: slotOverrides,
            setup: session.setup,
            workerLabels: session.labels,
            totalVideoJobs: totalVideoJobs,
            policySequences: policySequences,
            progress: progress,
            startingRunIndex: runIndex
        )
        runIndex = profileExecution.nextRunIndex
        profileResults.append(profileExecution.result)
    }

    BenchOutput.line("")
    BenchOutput.line("  \u{2500}\u{2500}\u{2500} Results \u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}")
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printBody("Order-neutral averages (lower is better)")
    let resultColumns = [
        BenchmarkRuntimeTableColumn(header: "profile", width: 18),
        BenchmarkRuntimeTableColumn(header: "winner", width: 6),
        BenchmarkRuntimeTableColumn(header: "sumW (FIFO/CA)", width: 19, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95 (FIFO/CA)", width: 17, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "wall (FIFO/CA)", width: 17, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(resultColumns)
    for result in profileResults {
        BenchmarkRuntimeRenderer.printTableRow(
            [
                result.profile.rawValue,
                showdownPolicyLabel(result.winner),
                showdownPair(result.fifo.avgSumWSeconds, result.ca.avgSumWSeconds),
                showdownPair(result.fifo.avgP95Seconds, result.ca.avgP95Seconds),
                showdownPair(result.fifo.avgMakespanSeconds, result.ca.avgMakespanSeconds),
            ],
            columns: resultColumns
        )
    }

    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printBody("Deltas (CA \u{2212} FIFO)")
    let deltaColumns = [
        BenchmarkRuntimeTableColumn(header: "profile", width: 18),
        BenchmarkRuntimeTableColumn(header: "sumW_delta", width: 12, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95_delta", width: 12, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "wall_delta", width: 12, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(deltaColumns)
    for result in profileResults {
        BenchmarkRuntimeRenderer.printTableRow(
            [
                result.profile.rawValue,
                showdownDelta(result.ca.avgSumWSeconds - result.fifo.avgSumWSeconds),
                showdownDelta(result.ca.avgP95Seconds - result.fifo.avgP95Seconds),
                showdownDelta(result.ca.avgMakespanSeconds - result.fifo.avgMakespanSeconds),
            ],
            columns: deltaColumns
        )
    }

    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printBody("Run-order detail")
    let orderColumns = [
        BenchmarkRuntimeTableColumn(header: "profile", width: 18),
        BenchmarkRuntimeTableColumn(header: "order", width: 14),
        BenchmarkRuntimeTableColumn(header: "sumW (FIFO/CA)", width: 19, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "p95 (FIFO/CA)", width: 17, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "wall (FIFO/CA)", width: 17, alignment: .right),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(orderColumns)
    for result in profileResults {
        for sequence in result.sequences {
            let fifoRun = sequence.fifoRun
            let caRun = sequence.caRun
            BenchmarkRuntimeRenderer.printTableRow(
                [
                    result.profile.rawValue,
                    sequence.order,
                    showdownPair(fifoRun.metrics.sumWSeconds, caRun.metrics.sumWSeconds),
                    showdownPair(fifoRun.metrics.p95Seconds, caRun.metrics.p95Seconds),
                    showdownPair(fifoRun.metrics.makespanSeconds, caRun.metrics.makespanSeconds),
                ],
                columns: orderColumns
            )
        }
    }

    let allAdaptationRuns = profileResults.flatMap { $0.fifo.runs + $0.ca.runs }
    let allCARuns = profileResults.flatMap { $0.ca.runs }
    let modelInputs = allCARuns.first?.observability.modelInputs
        ?? profileResults.first?.fifo.runs.first?.observability.modelInputs
        ?? []
    printThunderboltCAObservability(
        title: "Aggregate model quality",
        modelInputs: modelInputs,
        adaptationRuns: allAdaptationRuns,
        predictionRuns: allCARuns,
        includePrediction: true,
        includeModelInputs: false,
        workerLabels: session.labels
    )

    let summary = summarizeThunderboltShowdown(profileResults, preflight: session.preflight)

    BenchOutput.line("")
    BenchOutput.line("  \u{2500}\u{2500}\u{2500} Verdict \u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}")
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printField("sumW winner", showdownWinnerLabel(summary.sumWScore))
    BenchmarkRuntimeRenderer.printField("p95 winner", showdownWinnerLabel(summary.p95Score))
    BenchmarkRuntimeRenderer.printField("wall winner", showdownWinnerLabel(summary.wallScore))
    BenchmarkRuntimeRenderer.printField("profile winner", showdownWinnerLabel(summary.profileWins))
    if summary.totalFailedAcrossRuns == 0 {
        BenchmarkRuntimeRenderer.printField(
            "reliability",
            "\(summary.totalSucceededAcrossRuns)/\(summary.totalJobsAcrossRuns) jobs succeeded"
        )
    } else {
        BenchmarkRuntimeRenderer.printField(
            "reliability",
            "\(summary.totalSucceededAcrossRuns)/\(summary.totalJobsAcrossRuns) jobs succeeded " +
                "(\(summary.totalFailedAcrossRuns) failed)"
        )
    }
    let aggregateContractFailures = [
        summary.aggregateDecision.failedCountNonRegression ? nil : "failed",
        summary.aggregateDecision.makespanNonRegression ? nil : "makespan",
        summary.aggregateDecision.sumWImproved ? nil : "sumW",
        summary.aggregateDecision.p95NonRegression ? nil : "p95",
    ].compactMap(\.self)
    let aggregateContractStatus = if summary.aggregateDecision.pass {
        "pass"
    } else {
        "fail (\(aggregateContractFailures.joined(separator: ", ")))"
    }
    BenchmarkRuntimeRenderer.printField("aggregate contract", aggregateContractStatus)
    BenchmarkRuntimeRenderer.printField("phase health", "mem \(getMemoryMB())MB · thermal \(getThermalState())")
    for line in showdownGuidanceLines(verdict: summary.verdict) {
        BenchmarkRuntimeRenderer.printField("next step", line)
    }

    try finalizeThunderboltShowdownPriorPromotionIfNeeded(
        maintenanceResult: session.priorMaintenanceResult,
        showdownComparatorPass: summary.verdict.comparatorPass
    )
}

private func prepareThunderboltShowdownSession(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    profiles: [CAArrivalProfile],
    slotOverrides: ThunderboltCASlotOverrides?,
    remoteTxInEstimateMSByHost: [String: Double],
    modelMode: ThunderboltCAModelMode,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy,
    priorTableOverlay: BenchmarkPriorTable?,
    workerLabels: [String: String],
    skipPriorMaintenance: Bool,
    initialPriorMaintenanceResult: ThunderboltShowdownPriorMaintenanceResult?,
    preparedSetup: ThunderboltCARunSetup?,
    priorMaintenanceRunner: @escaping ThunderboltShowdownPriorMaintenanceRunner
) async throws -> ThunderboltShowdownSessionContext {
    BenchOutput.line("")
    BenchOutput.line("  \u{2500}\u{2500}\u{2500} Setup \u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}")
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printField("Arrival profiles", profiles.map(\.rawValue).joined(separator: ", "))
    BenchmarkRuntimeRenderer.printField("Policy order", "order-neutral (FIFO->CA and CA->FIFO)")
    BenchmarkRuntimeRenderer.printField("CA model mode", modelMode.rawValue)
    BenchmarkRuntimeRenderer.printField("Model update", thunderboltPriorUpdatePolicyLabel(priorUpdatePolicy))

    var setup = if let preparedSetup {
        preparedSetup
    } else {
        try await prepareThunderboltCARunSetup(
            corpus: corpus,
            preset: preset,
            hardware: hardware,
            slotOverrides: slotOverrides,
            mode: modelMode,
            priorTableOverride: priorTableOverlay,
            remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost
        )
    }
    let labels = workerLabels.isEmpty
        ? Dictionary(uniqueKeysWithValues: setup.reachableWorkers.enumerated().map { ($0.element.host, "W\($0.offset + 1)") })
        : workerLabels
    let preflight = classifyThunderboltShowdownPreflight(
        localPriorGap: setup.diagnostics.localPriorGap,
        remotePriorGap: setup.diagnostics.remotePriorGap
    )
    var priorMaintenanceResult = initialPriorMaintenanceResult

    BenchmarkRuntimeRenderer.printField("Preflight", preflight.rawValue)
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printBody("Model eligibility")
    printThunderboltCAModelEligibilitySummary(
        diagnostics: setup.diagnostics,
        modelInputs: setup.modelInputs,
        workerLabels: labels
    )
    if modelMode == .auto,
       preflight != .healthy {
        BenchOutput.line("  Auto mode: prior gap detected; showdown proceeds with fallback modeling where needed.")
    }

    if priorUpdatePolicy != .off && !skipPriorMaintenance {
        BenchOutput.line("")
        BenchmarkRuntimeRenderer.printBody("Showdown prior maintenance")
        do {
            if let maintenanceResult = try await priorMaintenanceRunner(
                corpus,
                preset,
                timeout,
                hardware,
                setup,
                slotOverrides,
                modelMode,
                preflight,
                priorUpdatePolicy
            ) {
                priorMaintenanceResult = maintenanceResult
                setup = maintenanceResult.setup
                BenchOutput.line("")
                BenchmarkRuntimeRenderer.printBody("Post-maintenance model eligibility")
                printThunderboltCAModelEligibilitySummary(
                    diagnostics: setup.diagnostics,
                    modelInputs: setup.modelInputs,
                    workerLabels: labels
                )
            }
        } catch {
            if isBenchmarkInterrupted(error, interruptState: nil) {
                throw error
            }
            if modelMode == .auto {
                BenchOutput.line("  Warning: showdown prior maintenance skipped (\(error))")
            } else {
                throw error
            }
        }
    }

    BenchOutput.line("")
    return ThunderboltShowdownSessionContext(
        setup: setup,
        labels: labels,
        preflight: preflight,
        priorMaintenanceResult: priorMaintenanceResult
    )
}

private func runThunderboltShowdownProfile(
    corpus: [MediaFile],
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    profile: CAArrivalProfile,
    modelMode: ThunderboltCAModelMode,
    slotOverrides: ThunderboltCASlotOverrides?,
    setup: ThunderboltCARunSetup,
    workerLabels: [String: String],
    totalVideoJobs: Int,
    policySequences: [[CASchedulerPolicy]],
    progress: ThunderboltShowdownProgressReporter,
    startingRunIndex: Int
) async throws -> (result: ThunderboltShowdownProfileResult, nextRunIndex: Int) {
    var runIndex = startingRunIndex
    var fifoRuns: [ThunderboltCAObservedRun] = []
    var caRuns: [ThunderboltCAObservedRun] = []
    var sequenceResults: [ThunderboltShowdownSequenceResult] = []
    sequenceResults.reserveCapacity(policySequences.count)

    for sequence in policySequences {
        let sequenceLabel = sequence.map(showdownPolicyLabel).joined(separator: "->")
        var orderedRuns: [ThunderboltCAObservedRun] = []
        orderedRuns.reserveCapacity(sequence.count)

        for policy in sequence {
            runIndex += 1
            let run = runIndex
            await progress.beginRun(
                run: run,
                profile: profile,
                policy: policy,
                sequenceLabel: sequenceLabel,
                totalJobs: totalVideoJobs
            )
            let observed = try await runThunderboltCA(
                corpus: corpus,
                preset: preset,
                timeout: timeout,
                hardware: hardware,
                policy: policy,
                profile: profile,
                modelMode: modelMode,
                slotOverrides: slotOverrides,
                preparedSetup: setup,
                progress: { completed, total, failed, elapsedSeconds in
                    await progress.updateRun(
                        run: run,
                        profile: profile,
                        policy: policy,
                        sequenceLabel: sequenceLabel,
                        completed: completed,
                        total: total,
                        failed: failed,
                        elapsedSeconds: elapsedSeconds
                    )
                }
            )
            await progress.finishRun(
                run: run,
                profile: profile,
                policy: policy,
                sequenceLabel: sequenceLabel,
                result: observed.result
            )
            switch policy {
            case .fifo:
                fifoRuns.append(observed)
            case .complexityAware:
                caRuns.append(observed)
            }
            orderedRuns.append(observed)
        }

        if orderedRuns.count == 2 {
            sequenceResults.append(
                ThunderboltShowdownSequenceResult(
                    order: sequenceLabel,
                    first: orderedRuns[0],
                    second: orderedRuns[1]
                )
            )
        }
    }

    guard fifoRuns.count == 2, caRuns.count == 2 else {
        throw ThunderboltBenchmarkJSONError.invariantViolation(
            "expected 2 FIFO and 2 CA runs, got fifo=\(fifoRuns.count) ca=\(caRuns.count)"
        )
    }

    let result = ThunderboltShowdownProfileResult(
        profile: profile,
        fifo: ThunderboltShowdownPolicyAggregate(runs: fifoRuns),
        ca: ThunderboltShowdownPolicyAggregate(runs: caRuns),
        sequences: sequenceResults
    )

    printThunderboltCAObservability(
        title: "CA Observability · \(profile.rawValue)",
        modelInputs: caRuns.first?.observability.modelInputs ?? fifoRuns.first?.observability.modelInputs ?? [],
        adaptationRuns: fifoRuns + caRuns,
        predictionRuns: caRuns,
        includePrediction: true,
        includeModelInputs: false,
        workerLabels: workerLabels
    )
    BenchOutput.line("")

    return (result, runIndex)
}

private func summarizeThunderboltShowdown(
    _ profileResults: [ThunderboltShowdownProfileResult],
    preflight: ThunderboltShowdownPreflightClassification
) -> ThunderboltShowdownSummary {
    let fifoAggregate = ThunderboltShowdownPolicyAggregate(
        runs: profileResults.flatMap(\.fifo.runs)
    )
    let caAggregate = ThunderboltShowdownPolicyAggregate(
        runs: profileResults.flatMap(\.ca.runs)
    )
    let aggregateDecision = BenchmarkShowdownPolicyKernel.comparatorDecision(
        fifoMetrics: showdownComparatorMetrics(fifoAggregate),
        caMetrics: showdownComparatorMetrics(caAggregate)
    )
    let sumWScore = showdownMetricScore(profileResults) { ($0.fifo.avgSumWSeconds, $0.ca.avgSumWSeconds) }
    let p95Score = showdownMetricScore(profileResults) { ($0.fifo.avgP95Seconds, $0.ca.avgP95Seconds) }
    let wallScore = showdownMetricScore(profileResults) { ($0.fifo.avgMakespanSeconds, $0.ca.avgMakespanSeconds) }
    let profileWins = showdownProfileWinnerScore(profileResults)
    let totalJobsAcrossRuns = profileResults.reduce(0) { partial, profile in
        partial + profile.fifo.totalJobsAcrossRuns + profile.ca.totalJobsAcrossRuns
    }
    let totalSucceededAcrossRuns = profileResults.reduce(0) { partial, profile in
        partial + profile.fifo.successfulJobsAcrossRuns + profile.ca.successfulJobsAcrossRuns
    }
    let totalFailedAcrossRuns = totalJobsAcrossRuns - totalSucceededAcrossRuns

    return ThunderboltShowdownSummary(
        aggregateDecision: aggregateDecision,
        sumWScore: sumWScore,
        p95Score: p95Score,
        wallScore: wallScore,
        profileWins: profileWins,
        totalJobsAcrossRuns: totalJobsAcrossRuns,
        totalSucceededAcrossRuns: totalSucceededAcrossRuns,
        totalFailedAcrossRuns: totalFailedAcrossRuns,
        verdict: showdownVerdict(
            aggregateDecision: aggregateDecision,
            sumWScore: sumWScore,
            p95Score: p95Score,
            wallScore: wallScore,
            profileWins: profileWins,
            totalFailedAcrossRuns: totalFailedAcrossRuns,
            preflight: preflight
        )
    )
}
