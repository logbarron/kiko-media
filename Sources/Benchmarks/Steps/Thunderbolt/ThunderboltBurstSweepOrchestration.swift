import Foundation
import KikoMediaCore

struct ThunderboltReachableWorkerSelection: Sendable {
    let reachableWorkers: [ThunderboltBoundWorkerSpec]
    let reachableConfiguredIndices: [Int]
}

struct ThunderboltBenchmarkJSONWorkerPayload: Sendable {
    let workers: [ThunderboltBenchmarkJSONWorker]
    let remoteWorkers: [ThunderboltBenchmarkJSONRemoteWorker]
}

typealias ThunderboltResolveBridgeBoundWorkersFunction = (
    [ThunderboltWorkerSpec]
) -> ([ThunderboltBoundWorkerSpec], [ThunderboltWorkerBindingIssue])

typealias ThunderboltConnectivityFunction = (
    [ThunderboltBoundWorkerSpec],
    Int,
    Int
) -> [ThunderboltConnectivityResult]

private struct ThunderboltBenchmarkJSONSession {
    let configuredWorkers: [ThunderboltWorkerSpec]
    let selection: ThunderboltReachableWorkerSelection
    let port: Int
    let connectTimeout: Int
    let workerCapsByHost: [String: WorkerCaps]
}

func resolveThunderboltReachableWorkerSelection(
    configuredWorkers: [ThunderboltWorkerSpec],
    boundWorkers: [ThunderboltBoundWorkerSpec],
    bindingIssues: [ThunderboltWorkerBindingIssue],
    connectivity: [ThunderboltConnectivityResult]
) -> ThunderboltReachableWorkerSelection {
    var reachableWorkers: [ThunderboltBoundWorkerSpec] = []
    reachableWorkers.reserveCapacity(connectivity.count)
    var reachableConfiguredIndices: [Int] = []
    reachableConfiguredIndices.reserveCapacity(connectivity.count)

    var nextBoundIndex = 0
    var nextIssueIndex = 0
    for (configuredIndex, worker) in configuredWorkers.enumerated() {
        if nextIssueIndex < bindingIssues.count,
           bindingIssues[nextIssueIndex].worker == worker {
            nextIssueIndex += 1
            continue
        }
        guard boundWorkers.indices.contains(nextBoundIndex) else {
            continue
        }
        let boundIndex = nextBoundIndex
        let boundWorker = boundWorkers[boundIndex]
        nextBoundIndex += 1
        guard connectivity.indices.contains(boundIndex),
              connectivity[boundIndex].reachable else {
            continue
        }
        reachableWorkers.append(boundWorker)
        reachableConfiguredIndices.append(configuredIndex)
    }

    return ThunderboltReachableWorkerSelection(
        reachableWorkers: reachableWorkers,
        reachableConfiguredIndices: reachableConfiguredIndices
    )
}

func thunderboltZeroReachableWorkerError(
    bindingIssues: [ThunderboltWorkerBindingIssue],
    connectivity: [ThunderboltConnectivityResult],
    port: Int
) -> ThunderboltBenchmarkJSONError {
    if let unreachable = connectivity.first(where: { !$0.reachable }) {
        return .workerUnreachable(host: unreachable.worker.host, port: port)
    }
    if !bindingIssues.isEmpty {
        return .workerBindingIssues(bindingIssues)
    }
    return .noBridgeSources
}

func requireThunderboltReachableWorkerSelection(
    configuredWorkers: [ThunderboltWorkerSpec],
    boundWorkers: [ThunderboltBoundWorkerSpec],
    bindingIssues: [ThunderboltWorkerBindingIssue],
    connectivity: [ThunderboltConnectivityResult],
    port: Int
) throws -> ThunderboltReachableWorkerSelection {
    let selection = resolveThunderboltReachableWorkerSelection(
        configuredWorkers: configuredWorkers,
        boundWorkers: boundWorkers,
        bindingIssues: bindingIssues,
        connectivity: connectivity
    )
    guard !selection.reachableWorkers.isEmpty else {
        throw thunderboltZeroReachableWorkerError(
            bindingIssues: bindingIssues,
            connectivity: connectivity,
            port: port
        )
    }
    return selection
}

private func thunderboltWorkerCapsByHost(
    workers: [ThunderboltBoundWorkerSpec],
    port: Int,
    connectTimeout: Int
) -> [String: WorkerCaps] {
    let workerCaps = probeWorkerCapabilities(
        workers: workers,
        port: port,
        connectTimeout: connectTimeout
    )
    var workerCapsByHost: [String: WorkerCaps] = [:]
    workerCapsByHost.reserveCapacity(workers.count)
    for (worker, caps) in zip(workers, workerCaps) {
        if let caps {
            workerCapsByHost[worker.host] = caps
        }
    }
    return workerCapsByHost
}

private func prepareThunderboltBenchmarkJSONSession(
    resolveBoundWorkers: ThunderboltResolveBridgeBoundWorkersFunction,
    benchmarkConnectivity: ThunderboltConnectivityFunction
) throws -> ThunderboltBenchmarkJSONSession {
    let settings = resolveThunderboltBenchmarkSettings()
    guard let workersRaw = settings.workersRaw, !workersRaw.isEmpty else {
        throw ThunderboltBenchmarkJSONError.workersNotConfigured
    }

    let configuredWorkers = parseThunderboltWorkers(workersRaw)
    guard !configuredWorkers.isEmpty else {
        throw ThunderboltBenchmarkJSONError.invalidWorkers(workersRaw)
    }

    let (boundWorkers, bindingIssues) = resolveBoundWorkers(configuredWorkers)
    let port = settings.port
    let connectTimeout = settings.connectTimeout

    if boundWorkers.isEmpty {
        throw thunderboltZeroReachableWorkerError(
            bindingIssues: bindingIssues,
            connectivity: [],
            port: port
        )
    }

    let connectivity = benchmarkConnectivity(boundWorkers, port, connectTimeout)
    let selection = try requireThunderboltReachableWorkerSelection(
        configuredWorkers: configuredWorkers,
        boundWorkers: boundWorkers,
        bindingIssues: bindingIssues,
        connectivity: connectivity,
        port: port
    )

    return ThunderboltBenchmarkJSONSession(
        configuredWorkers: configuredWorkers,
        selection: selection,
        port: port,
        connectTimeout: connectTimeout,
        workerCapsByHost: thunderboltWorkerCapsByHost(
            workers: selection.reachableWorkers,
            port: port,
            connectTimeout: connectTimeout
        )
    )
}

func buildThunderboltBenchmarkJSONWorkerPayload(
    configuredWorkers: [ThunderboltWorkerSpec],
    reachableConfiguredIndices: [Int],
    bestConfig: ThunderboltBurstConfig
) throws -> ThunderboltBenchmarkJSONWorkerPayload {
    guard bestConfig.remoteSlots.count == reachableConfiguredIndices.count else {
        throw ThunderboltBenchmarkJSONError.invariantViolation(
            "best_config.remote_workers count does not match reachable workers count"
        )
    }

    var remoteSlotsByConfiguredIndex = Array(repeating: 0, count: configuredWorkers.count)
    for (reachableIndex, configuredIndex) in reachableConfiguredIndices.enumerated() {
        guard configuredWorkers.indices.contains(configuredIndex) else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "reachable worker index is out of range for configured workers"
            )
        }
        remoteSlotsByConfiguredIndex[configuredIndex] = bestConfig.remoteSlots[reachableIndex]
    }

    let workers = configuredWorkers.enumerated().map { index, worker in
        ThunderboltBenchmarkJSONWorker(
            index: index,
            host: worker.host,
            configuredSlots: worker.slots
        )
    }
    let remoteWorkers = configuredWorkers.enumerated().map { index, worker in
        ThunderboltBenchmarkJSONRemoteWorker(
            index: index,
            host: worker.host,
            slots: remoteSlotsByConfiguredIndex[index]
        )
    }

    return ThunderboltBenchmarkJSONWorkerPayload(
        workers: workers,
        remoteWorkers: remoteWorkers
    )
}

private func encodeThunderboltBenchmarkJSONPayload(
    _ payload: ThunderboltBenchmarkJSONPayload
) throws -> String {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let data = try encoder.encode(payload)
    guard let json = String(data: data, encoding: .utf8) else {
        throw ThunderboltBenchmarkJSONError.encodingFailed
    }
    return json
}

private func makeThunderboltBenchmarkJSONPayload(
    configuredWorkers: [ThunderboltWorkerSpec],
    reachableConfiguredIndices: [Int],
    bestRun: (ThunderboltBurstConfig, ThunderboltBurstResult)
) throws -> ThunderboltBenchmarkJSONPayload {
    let workerPayload = try buildThunderboltBenchmarkJSONWorkerPayload(
        configuredWorkers: configuredWorkers,
        reachableConfiguredIndices: reachableConfiguredIndices,
        bestConfig: bestRun.0
    )
    let videosPerMin = bestRun.1.wallSeconds > 0
        ? (Double(bestRun.1.completed) / bestRun.1.wallSeconds * 60.0)
        : 0
    let payload = ThunderboltBenchmarkJSONPayload(
        schemaVersion: thunderboltDelegatedBenchmarkSchemaVersion,
        workers: workerPayload.workers,
        bestConfig: ThunderboltBenchmarkJSONBestConfig(
            localSlots: bestRun.0.localSlots,
            remoteWorkers: workerPayload.remoteWorkers,
            wallSeconds: bestRun.1.wallSeconds,
            completedVideos: bestRun.1.completed,
            failedVideos: bestRun.1.failed,
            videosPerMin: videosPerMin
        )
    )

    try validateThunderboltBenchmarkJSONPayload(
        payload,
        configuredWorkers: configuredWorkers
    )
    return payload
}

func benchmarkThunderboltJSON(
    corpus: [MediaFile],
    preset: String = defaultVideoPreset,
    timeout: Int = defaultVideoTimeoutSeconds,
    hardware: HardwareProfile,
    sweepMode: BurstSearchStrategy = .optimized(),
    resolveBoundWorkers: ThunderboltResolveBridgeBoundWorkersFunction = resolveBridgeBoundWorkers,
    benchmarkConnectivity: ThunderboltConnectivityFunction = benchmarkThunderboltConnectivity
) async throws -> String {
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else {
        throw ThunderboltBenchmarkJSONError.noVideos
    }
    let session = try prepareThunderboltBenchmarkJSONSession(
        resolveBoundWorkers: resolveBoundWorkers,
        benchmarkConnectivity: benchmarkConnectivity
    )
    let reachableWorkers = session.selection.reachableWorkers

    let execution = try await executeThunderboltBurstSweep(
        videos: videos,
        workers: reachableWorkers,
        port: session.port,
        connectTimeout: session.connectTimeout,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        sweepMode: sweepMode,
        headerPrinter: { columns in
            BenchmarkRuntimeRenderer.printTableHeader(columns)
        },
        rowPrinter: { values, columns, semantics in
            BenchmarkRuntimeRenderer.printTableRow(values, columns: columns, semantics: semantics)
        }
    )
    guard let best = execution.bestRun else {
        throw ThunderboltBenchmarkJSONError.noBurstConfigs
    }

    let payload = try makeThunderboltBenchmarkJSONPayload(
        configuredWorkers: session.configuredWorkers,
        reachableConfiguredIndices: session.selection.reachableConfiguredIndices,
        bestRun: best
    )

    _ = try await emitThunderboltJSONPriorAfterBurstSweepIfNeeded(
        corpus: corpus,
        videos: videos,
        runs: execution.evaluatedRuns,
        workers: reachableWorkers,
        port: session.port,
        connectTimeout: session.connectTimeout,
        hardware: hardware,
        preset: preset,
        timeout: timeout,
        workerCapsByHost: session.workerCapsByHost
    )
    return try encodeThunderboltBenchmarkJSONPayload(payload)
}

func validateThunderboltBenchmarkJSONPayload(
    _ payload: ThunderboltBenchmarkJSONPayload,
    configuredWorkers: [ThunderboltWorkerSpec]
) throws {
    guard payload.schemaVersion == thunderboltDelegatedBenchmarkSchemaVersion else {
        throw ThunderboltBenchmarkJSONError.invariantViolation(
            "\"schema_version\" must equal \(thunderboltDelegatedBenchmarkSchemaVersion)"
        )
    }
    guard payload.workers.count == configuredWorkers.count else {
        throw ThunderboltBenchmarkJSONError.invariantViolation(
            "\"workers\" count must match configured TB_WORKERS entries"
        )
    }

    for (expectedIndex, configuredWorker) in configuredWorkers.enumerated() {
        let payloadWorker = payload.workers[expectedIndex]
        guard payloadWorker.index == expectedIndex else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"workers[\(expectedIndex)].index\" must equal \(expectedIndex)"
            )
        }
        guard payloadWorker.host == configuredWorker.host else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"workers[\(expectedIndex)].host\" must preserve TB_WORKERS order and host token"
            )
        }
        guard payloadWorker.configuredSlots == configuredWorker.slots else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"workers[\(expectedIndex)].configured_slots\" must preserve configured slots"
            )
        }
    }

    let remoteWorkers = payload.bestConfig.remoteWorkers
    guard remoteWorkers.count == payload.workers.count else {
        throw ThunderboltBenchmarkJSONError.invariantViolation(
            "\"best_config.remote_workers\" must include one entry per worker"
        )
    }
    guard payload.bestConfig.completedVideos > 0 else {
        throw ThunderboltBenchmarkJSONError.invariantViolation(
            "\"best_config.completed_videos\" must be greater than 0"
        )
    }
    guard payload.bestConfig.failedVideos == 0 else {
        throw ThunderboltBenchmarkJSONError.invariantViolation(
            "\"best_config.failed_videos\" must equal 0"
        )
    }

    var seenIndices = Set<Int>()
    for (expectedPosition, remoteWorker) in remoteWorkers.enumerated() {
        guard remoteWorker.index == expectedPosition else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"best_config.remote_workers\" must be ordered by worker index"
            )
        }
        guard payload.workers.indices.contains(remoteWorker.index) else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"best_config.remote_workers[\(expectedPosition)].index\" is out of range"
            )
        }
        guard seenIndices.insert(remoteWorker.index).inserted else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"best_config.remote_workers\" contains duplicate index \(remoteWorker.index)"
            )
        }

        let payloadWorker = payload.workers[remoteWorker.index]
        guard remoteWorker.host == payloadWorker.host else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"best_config.remote_workers[\(expectedPosition)].host\" must match workers[index].host"
            )
        }

        guard (0...16).contains(remoteWorker.slots) else {
            throw ThunderboltBenchmarkJSONError.invariantViolation(
                "\"best_config.remote_workers[\(expectedPosition)].slots\" must be in 0...16"
            )
        }
    }
}

func benchmarkThunderboltBurstSweep(
    videos: [MediaFile],
    workers: [ThunderboltBoundWorkerSpec],
    workerLabels: [String: String],
    port: Int,
    connectTimeout: Int,
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    sweepMode: BurstSearchStrategy = .optimized()
) async throws -> [(ThunderboltBurstConfig, ThunderboltBurstResult)]? {
    let caps = probeWorkerCapabilities(workers: workers, port: port, connectTimeout: connectTimeout)
    let labels = workers.enumerated().compactMap { index, _ in
        caps[index] != nil ? "W\(index + 1)" : nil
    }
    BenchOutput.line("  Workers: \(labels.joined(separator: ", "))")

    let baseline = ThunderboltBurstConfig(localSlots: 1, remoteSlots: Array(repeating: 0, count: workers.count))
    let execution = try await executeThunderboltBurstSweep(
        videos: videos,
        workers: workers,
        port: port,
        connectTimeout: connectTimeout,
        preset: preset,
        timeout: timeout,
        hardware: hardware,
        sweepMode: sweepMode,
        headerPrinter: { columns in
            BenchmarkRuntimeRenderer.printTableHeader(columns)
        },
        rowPrinter: { values, columns, semantics in
            BenchmarkRuntimeRenderer.printTableRow(values, columns: columns, semantics: semantics)
        }
    )

    BenchOutput.line("")
    printThunderboltLeaderboard(
        workers: workers,
        workerLabels: workerLabels,
        runs: execution.displayedRuns,
        baseline: baseline
    )

    return execution.displayedRuns
}
