import Foundation
import KikoMediaCore

func benchmarkThunderbolt(
    corpus: [MediaFile],
    preset: String = defaultVideoPreset,
    timeout: Int = defaultVideoTimeoutSeconds,
    hardware: HardwareProfile,
    sweepMode: BurstSearchStrategy = .optimized(),
    includeShowdown: Bool = true,
    showdownProfiles: [CAArrivalProfile] = CAArrivalProfile.allCases,
    showdownModelMode: ThunderboltCAModelMode = .auto,
    showdownPriorUpdatePolicy: ThunderboltPriorUpdatePolicy = .candidateOnly,
    resolveBoundWorkers: ThunderboltResolveBridgeBoundWorkersFunction = resolveBridgeBoundWorkers,
    benchmarkConnectivity: ThunderboltConnectivityFunction = benchmarkThunderboltConnectivity
) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Thunderbolt Offload")

    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else {
        BenchOutput.line("  No video files in media folder, skipping")
        return
    }

    let settings = resolveThunderboltBenchmarkSettings()
    for warning in settings.warnings {
        BenchOutput.line("  \(warning)")
    }

    guard let workersRaw = settings.workersRaw, !workersRaw.isEmpty else {
        BenchOutput.line("  TB_WORKERS is not configured (environment or com.kiko.media.plist).")
        BenchOutput.line("  Use --tb-workers host:slots[,host:slots] or run setup --thunderbolt.")
        return
    }

    let workers = parseThunderboltWorkers(workersRaw)
    guard !workers.isEmpty else {
        BenchOutput.line("  TB_WORKERS has no valid worker entries: \(workersRaw)")
        BenchOutput.line("  Expected format: host:slots[,host:slots]")
        return
    }

    let (boundWorkers, bindingIssues) = resolveBoundWorkers(workers)
    let port = settings.port
    let connectTimeout = settings.connectTimeout

    let fullUsesDistinctPriorMaintenance = shouldRunThunderboltFullPriorMaintenance(
        includeShowdown: includeShowdown,
        priorUpdatePolicy: showdownPriorUpdatePolicy
    )
    let totalPhases = if includeShowdown {
        fullUsesDistinctPriorMaintenance ? 6 : 5
    } else {
        4
    }
    BenchmarkRuntimeRenderer.printSubsectionTitle("Phase 1/\(totalPhases): Corpus Summary")
    let workersLabel = workers.enumerated().map { index, worker in
        "W\(index + 1)=\(worker.host) (\(worker.slots) slots)"
    }.joined(separator: ", ")
    BenchmarkRuntimeRenderer.printField("Workers", workersLabel)
    BenchmarkRuntimeRenderer.printField("Source", settings.workersSource)
    BenchmarkRuntimeRenderer.printField("Port", "\(port) (\(settings.portSource))")
    BenchmarkRuntimeRenderer.printField("Connect timeout", "\(connectTimeout)ms (\(settings.connectTimeoutSource))")
    let totalVideoBytes = videos.reduce(0) { $0 + $1.sizeBytes }
    let sortedBySize = videos.sorted { $0.sizeBytes > $1.sizeBytes }
    let largest = formatThunderboltCorpusSize(sortedBySize.first?.sizeBytes ?? 0)
    let smallest = formatThunderboltCorpusSize(sortedBySize.last?.sizeBytes ?? 0)
    BenchmarkRuntimeRenderer.printField(
        "Video corpus",
        "\(videos.count) files, \(formatThunderboltCorpusSize(totalVideoBytes)) (\(smallest)–\(largest))"
    )
    BenchOutput.line("")

    BenchmarkRuntimeRenderer.printSubsectionTitle("Phase 2/\(totalPhases): Network Health")

    for issue in bindingIssues {
        BenchOutput.line("  \(issue.worker.host): source-bind unavailable (\(issue.reason))")
    }

    guard !boundWorkers.isEmpty else {
        BenchOutput.line("  No bridge-bound workers available.")
        throw thunderboltZeroReachableWorkerError(
            bindingIssues: bindingIssues,
            connectivity: [],
            port: port
        )
    }

    let connectivity = benchmarkConnectivity(boundWorkers, port, connectTimeout)
    let healthColumns = [
        BenchmarkRuntimeTableColumn(header: "Worker", width: 6),
        BenchmarkRuntimeTableColumn(header: "Bridge", width: 7),
        BenchmarkRuntimeTableColumn(header: "Source", width: 14),
        BenchmarkRuntimeTableColumn(header: "Slots", width: 5, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Latency", width: 7, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Status", width: 11),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(healthColumns)
    for (index, result) in connectivity.enumerated() {
        let worker = result.worker
        let status = result.reachable ? "reachable" : "unreachable"
        let semantic: BenchmarkRuntimeSemantic? = result.reachable ? nil : .error
        BenchmarkRuntimeRenderer.printTableRow(
            [
                "W\(index + 1)",
                worker.bridgeName,
                worker.sourceIP,
                "\(worker.slots)",
                String(format: "%.1fms", result.connectMillis),
                status,
            ],
            columns: healthColumns,
            semantics: [nil, nil, nil, nil, nil, semantic]
        )
    }

    let reachableWorkerSelection = try requireThunderboltReachableWorkerSelection(
        configuredWorkers: workers,
        boundWorkers: boundWorkers,
        bindingIssues: bindingIssues,
        connectivity: connectivity,
        port: port
    )
    let reachableWorkers = reachableWorkerSelection.reachableWorkers

    var workerLabels: [String: String] = [:]
    for (index, worker) in workers.enumerated() {
        workerLabels[worker.host] = "W\(index + 1)"
    }
    let workerCaps = probeWorkerCapabilities(
        workers: reachableWorkers,
        port: port,
        connectTimeout: connectTimeout
    )
    var workerCapsByHost: [String: WorkerCaps] = [:]
    workerCapsByHost.reserveCapacity(reachableWorkers.count)
    for (worker, caps) in zip(reachableWorkers, workerCaps) {
        if let caps {
            workerCapsByHost[worker.host] = caps
        }
    }

    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printSubsectionTitle("Phase 3/\(totalPhases): Profiling")
    BenchOutput.line("  Precomputing source SHA-256 for \(videos.count) videos...")
    let sourceHashes = try precomputeSourceHashes(videos)
    let remoteTxInEstimateMSByHost = await benchmarkThunderboltProfilingTables(
        videos: videos,
        workers: reachableWorkers,
        workerLabels: workerLabels,
        sourceHashes: sourceHashes,
        port: port,
        connectTimeout: connectTimeout,
        preset: preset,
        timeout: timeout
    )

    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printSubsectionTitle("Phase 4/\(totalPhases): Burst Sweep + Leaderboard")
    var priorTableOverlay: BenchmarkPriorTable? = nil
    var fullPriorMaintenanceResult: ThunderboltShowdownPriorMaintenanceResult?
    let burstSweepPriorUpdatePolicy = effectiveThunderboltBurstSweepPriorUpdatePolicy(
        includeShowdown: includeShowdown,
        showdownPriorUpdatePolicy: showdownPriorUpdatePolicy
    )
    if let runs = try await benchmarkThunderboltBurstSweep(
        videos: videos,
        workers: reachableWorkers,
        workerLabels: workerLabels,
        port: port,
        connectTimeout: connectTimeout,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        sweepMode: sweepMode
    ) {
        if !fullUsesDistinctPriorMaintenance {
            let candidateArtifact = try await emitThunderboltPriorAfterBurstSweepIfNeeded(
                corpus: corpus,
                videos: videos,
                runs: runs,
                workers: reachableWorkers,
                port: port,
                connectTimeout: connectTimeout,
                hardware: hardware,
                preset: preset,
                timeout: timeout,
                priorUpdatePolicy: burstSweepPriorUpdatePolicy,
                workerCapsByHost: workerCapsByHost
            )
            if let candidateArtifact {
                priorTableOverlay = BenchmarkPriorTable(artifact: candidateArtifact)
            }
        }
    }

    guard includeShowdown else { return }
    if fullUsesDistinctPriorMaintenance {
        BenchOutput.line("")
        BenchmarkRuntimeRenderer.printSubsectionTitle("Phase 5/\(totalPhases): Full Prior Maintenance")
        if let maintenanceResult = try await runThunderboltFullPriorMaintenance(
            corpus: corpus,
            preset: preset,
            timeout: timeout,
            hardware: hardware,
            modelMode: showdownModelMode,
            priorUpdatePolicy: showdownPriorUpdatePolicy,
            priorTableOverlay: priorTableOverlay,
            remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost
        ) {
            fullPriorMaintenanceResult = maintenanceResult
            priorTableOverlay = BenchmarkPriorTable(artifact: maintenanceResult.candidateArtifact)
        }
    }
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printSubsectionTitle("Phase \(totalPhases)/\(totalPhases): Measured FIFO vs CA Showdown")
    try await benchmarkThunderboltMeasuredShowdown(
        corpus: corpus,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        profiles: showdownProfiles,
        remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost,
        modelMode: showdownModelMode,
        priorUpdatePolicy: showdownPriorUpdatePolicy,
        priorTableOverlay: priorTableOverlay,
        workerLabels: workerLabels,
        skipPriorMaintenance: fullUsesDistinctPriorMaintenance,
        initialPriorMaintenanceResult: fullPriorMaintenanceResult
    )
}

func benchmarkThunderboltShowdown(
    corpus: [MediaFile],
    preset: String = defaultVideoPreset,
    timeout: Int = defaultVideoTimeoutSeconds,
    hardware: HardwareProfile,
    profiles: [CAArrivalProfile] = CAArrivalProfile.allCases,
    slotOverrides: ThunderboltCASlotOverrides? = nil,
    modelMode: ThunderboltCAModelMode = .auto,
    priorUpdatePolicy: ThunderboltPriorUpdatePolicy = .off
) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Thunderbolt FIFO vs CA Showdown")
    if let slotOverrides {
        BenchmarkRuntimeRenderer.printField(
            "Local slots",
            slotOverrides.localSlots.map { "\($0)" } ?? "default"
        )
        if !slotOverrides.remoteSlotsByHost.isEmpty {
            let remote = slotOverrides.remoteSlotsByHost
                .sorted { lhs, rhs in lhs.key < rhs.key }
                .map { "\($0.key)=\($0.value)" }
                .joined(separator: ", ")
            BenchmarkRuntimeRenderer.printField("Remote slots", remote)
        }
    }
    BenchOutput.line("")
    try await benchmarkThunderboltMeasuredShowdown(
        corpus: corpus,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        profiles: profiles,
        slotOverrides: slotOverrides,
        modelMode: modelMode,
        priorUpdatePolicy: priorUpdatePolicy
    )
}

func benchmarkThunderboltProfilingTables(
    videos: [MediaFile],
    workers: [ThunderboltBoundWorkerSpec],
    workerLabels: [String: String],
    sourceHashes: [String: String],
    port: Int,
    connectTimeout: Int,
    preset: String,
    timeout: Int
) async -> [String: Double] {
    guard let sample = videos.max(by: { $0.sizeBytes < $1.sizeBytes }) else {
        BenchOutput.line("  No video available, skipping")
        return [:]
    }
    BenchmarkRuntimeRenderer.printField("Sample", "\(sample.name) (\(sample.description))")

    let columns = [
        BenchmarkRuntimeTableColumn(header: "Target", width: 8),
        BenchmarkRuntimeTableColumn(header: "Send", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Proc", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Recv", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Total", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Status", width: 8),
    ]
    BenchOutput.line("")
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    let localTemp = makeTempDir("tb-local-profile")
    defer { cleanup(localTemp) }
    let local = await runThunderboltLocalRoundTrip(
        video: sample,
        uploadId: "tb-phase3-local",
        thumbsDir: "\(localTemp)/thumbs",
        previewsDir: "\(localTemp)/previews",
        preset: preset,
        timeout: timeout
    )
    BenchmarkRuntimeRenderer.printTableRow(
        [
            "local",
            "\u{2013}",
            "\u{2013}",
            "\u{2013}",
            local.success ? fmt(local.seconds) : "-",
            local.success ? "ok" : "failed",
        ],
        columns: columns
    )

    guard let sourceSHA = sourceHashes[sample.path] else {
        for worker in workers {
            let label = workerLabels[worker.host] ?? worker.host
            BenchmarkRuntimeRenderer.printTableRow(
                [label, "-", "-", "-", "-", "no-hash"],
                columns: columns
            )
        }
        return [:]
    }

    var remoteTxInEstimateMSByHost: [String: Double] = [:]
    remoteTxInEstimateMSByHost.reserveCapacity(workers.count)
    let remoteTemp = makeTempDir("tb-remote-profile")
    defer { cleanup(remoteTemp) }
    for worker in workers {
        let label = workerLabels[worker.host] ?? worker.host
        let remote = runThunderboltRoundTrip(
            worker: worker,
            video: sample,
            sourceSHA256: sourceSHA,
            port: port,
            connectTimeout: connectTimeout,
            tempDir: remoteTemp
        )
        BenchmarkRuntimeRenderer.printTableRow(
            [
                label,
                remote.success ? fmt(remote.sendSeconds) : "-",
                remote.success ? fmt(Double(remote.processNanos) / 1e9) : "-",
                remote.success ? fmt(remote.receiveSeconds) : "-",
                remote.success ? fmt(remote.totalSeconds) : "-",
                remote.success ? "ok" : "failed",
            ],
            columns: columns
        )
        if let estimateSeconds = remote.firstRunningLatencySecondsEstimate,
           estimateSeconds.isFinite,
           estimateSeconds > 0 {
            remoteTxInEstimateMSByHost[worker.host] = estimateSeconds * 1_000.0
        }
    }

    return remoteTxInEstimateMSByHost
}

func benchmarkThunderboltCA(
    corpus: [MediaFile],
    preset: String = defaultVideoPreset,
    timeout: Int = defaultVideoTimeoutSeconds,
    hardware: HardwareProfile,
    policy: CASchedulerPolicy,
    profile: CAArrivalProfile,
    rawOutputPath: String?,
    summaryOutputPath: String?,
    modelMode: ThunderboltCAModelMode = .strict
) async throws -> ThunderboltCARunResult {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Thunderbolt CA")
    let result = try await runAndReportThunderboltCA(
        corpus: corpus,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        policy: policy,
        profile: profile,
        modelMode: modelMode
    )
    if let rawOutputPath, !rawOutputPath.isEmpty {
        try writeThunderboltCAJSON(result, toPath: rawOutputPath)
    }
    if let summaryOutputPath, !summaryOutputPath.isEmpty {
        try writeThunderboltCASummary(result, toPath: summaryOutputPath)
    }
    return result
}

func benchmarkThunderboltCAAcceptance(
    corpus: [MediaFile],
    preset: String = defaultVideoPreset,
    timeout: Int = defaultVideoTimeoutSeconds,
    hardware: HardwareProfile,
    outputPath: String?,
    modelMode: ThunderboltCAModelMode = .strict
) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Thunderbolt CA Acceptance")
    try await runAndReportThunderboltCAAcceptance(
        corpus: corpus,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        outputPath: outputPath,
        modelMode: modelMode
    )
}

func benchmarkThunderboltCAJSON(
    corpus: [MediaFile],
    preset: String = defaultVideoPreset,
    timeout: Int = defaultVideoTimeoutSeconds,
    hardware: HardwareProfile,
    policy: CASchedulerPolicy,
    profile: CAArrivalProfile,
    modelMode: ThunderboltCAModelMode = .strict
) async throws -> String {
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else {
        throw ThunderboltBenchmarkJSONError.noVideos
    }

    let settings = resolveThunderboltBenchmarkSettings()
    guard let workersRaw = settings.workersRaw, !workersRaw.isEmpty else {
        throw ThunderboltBenchmarkJSONError.workersNotConfigured
    }
    let workers = parseThunderboltWorkers(workersRaw)
    guard !workers.isEmpty else {
        throw ThunderboltBenchmarkJSONError.invalidWorkers(workersRaw)
    }
    let (boundWorkers, _) = resolveBridgeBoundWorkers(workers: workers)
    guard !boundWorkers.isEmpty else {
        throw ThunderboltBenchmarkJSONError.workersNotConfigured
    }

    let sourceHashes = try precomputeSourceHashes(videos)
    var workerLabels: [String: String] = [:]
    for (index, worker) in workers.enumerated() {
        workerLabels[worker.host] = "W\(index + 1)"
    }

    let remoteTxInEstimateMSByHost = await benchmarkThunderboltProfilingTables(
        videos: videos,
        workers: boundWorkers,
        workerLabels: workerLabels,
        sourceHashes: sourceHashes,
        port: settings.port,
        connectTimeout: settings.connectTimeout,
        preset: preset,
        timeout: timeout
    )

    let setup = try await prepareThunderboltCARunSetup(
        corpus: corpus,
        preset: preset,
        hardware: hardware,
        slotOverrides: nil,
        mode: modelMode,
        reachableWorkersOverride: boundWorkers,
        remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost
    )

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
    return try renderThunderboltCAJSON(observed.result)
}
