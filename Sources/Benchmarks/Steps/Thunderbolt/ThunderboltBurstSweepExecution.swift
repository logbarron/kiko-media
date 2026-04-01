import Foundation
import Synchronization
import KikoMediaCore

enum ThunderboltLoopCancellationPoint: Sendable {
    case burstDispatchLoop
    case priorLocalSweep
    case priorFrameCounting
    case priorAffineSampleCollection
    case priorRemoteSamplePreparation
    case priorRemoteTelemetry
}

struct ThunderboltBurstSweepExecutionResult {
    let displayedRuns: [(ThunderboltBurstConfig, ThunderboltBurstResult)]
    let evaluatedRuns: [(ThunderboltBurstConfig, ThunderboltBurstResult)]
    let bestRun: (ThunderboltBurstConfig, ThunderboltBurstResult)?
}

typealias ThunderboltBurstSweepConfigExecutor = @Sendable (
    ThunderboltBurstConfig,
    [ThunderboltBoundWorkerSpec],
    [MediaFile],
    Int,
    Int,
    String,
    Int,
    (@Sendable (Int, Int) -> Void)?
) async throws -> ThunderboltBurstResult

typealias ThunderboltBurstSweepHeaderPrinter = ([BenchmarkRuntimeTableColumn]) -> Void

typealias ThunderboltBurstSweepRowPrinter = (
    [String],
    [BenchmarkRuntimeTableColumn],
    [BenchmarkRuntimeSemantic?]
) -> Void

private func defaultThunderboltBurstSweepConfigExecutor(
    config: ThunderboltBurstConfig,
    workers: [ThunderboltBoundWorkerSpec],
    videos: [MediaFile],
    port: Int,
    connectTimeout: Int,
    preset: String,
    timeout: Int,
    onProgress: (@Sendable (Int, Int) -> Void)?
) async throws -> ThunderboltBurstResult {
    try await runThunderboltBurstConfig(
        config: config,
        workers: workers,
        videos: videos,
        port: port,
        connectTimeout: connectTimeout,
        preset: preset,
        timeout: timeout,
        onProgress: onProgress
    )
}

private func clearThunderboltBurstSweepProgressLine() {
    BenchOutput.write("\r\u{1B}[2K")
}

private func selectThunderboltBestSuccessfulBurstRun(
    from runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)]
) -> (ThunderboltBurstConfig, ThunderboltBurstResult)? {
    runs
        .filter { $0.1.failed == 0 }
        .min(by: { lhs, rhs in
            if lhs.1.wallSeconds != rhs.1.wallSeconds {
                return lhs.1.wallSeconds < rhs.1.wallSeconds
            }
            return lhs.0.remoteSlots.lexicographicallyPrecedes(rhs.0.remoteSlots)
        })
}

func thunderboltBurstConfigCount(maxSlots: [Int], localSlotsRange: ClosedRange<Int>) -> Int {
    var count = localSlotsRange.count
    for maxSlot in maxSlots {
        let (product, overflow) = count.multipliedReportingOverflow(by: maxSlot + 1)
        precondition(!overflow, "validated burst config count must fit in Int")
        count = product
    }
    if localSlotsRange.contains(0) {
        count -= 1
    }
    return count
}

func withEachThunderboltBurstConfig(
    maxSlots: [Int],
    localSlotsRange: ClosedRange<Int>,
    _ visit: (ThunderboltBurstConfig) async throws -> Void
) async rethrows {
    let suffixCapacities = thunderboltBurstSuffixCapacities(maxSlots)
    let maxRemoteTotal = suffixCapacities[0]
    let maxTotal = localSlotsRange.upperBound + maxRemoteTotal
    guard maxTotal > 0 else { return }

    var remoteSlots = Array(repeating: 0, count: maxSlots.count)
    for totalSlots in 1...maxTotal {
        let localLower = max(localSlotsRange.lowerBound, max(0, totalSlots - maxRemoteTotal))
        let localUpper = min(localSlotsRange.upperBound, totalSlots)
        guard localLower <= localUpper else { continue }

        for localSlots in localLower...localUpper {
            try await withEachThunderboltRemoteSlotVector(
                maxSlots: maxSlots,
                suffixCapacities: suffixCapacities,
                targetSum: totalSlots - localSlots,
                index: 0,
                current: &remoteSlots
            ) { remoteSlots in
                try await visit(
                    ThunderboltBurstConfig(localSlots: localSlots, remoteSlots: remoteSlots)
                )
            }
        }
    }
}

func withEachThunderboltBurstConfigBaselineFirst(
    maxSlots: [Int],
    localSlotsRange: ClosedRange<Int>,
    _ visit: (Int, Int, ThunderboltBurstConfig) async throws -> Void
) async rethrows {
    let totalCount = thunderboltBurstConfigCount(
        maxSlots: maxSlots,
        localSlotsRange: localSlotsRange
    )
    guard totalCount > 0 else { return }

    let baseline = ThunderboltBurstConfig(
        localSlots: 1,
        remoteSlots: Array(repeating: 0, count: maxSlots.count)
    )
    let includesBaseline = localSlotsRange.contains(1)

    var run = 0
    if includesBaseline {
        run += 1
        try await visit(run, totalCount, baseline)
    }

    try await withEachThunderboltBurstConfig(
        maxSlots: maxSlots,
        localSlotsRange: localSlotsRange
    ) { config in
        guard !(includesBaseline && config == baseline) else { return }
        run += 1
        try await visit(run, totalCount, config)
    }
}

private func thunderboltBurstSuffixCapacities(_ maxSlots: [Int]) -> [Int] {
    var suffixCapacities = Array(repeating: 0, count: maxSlots.count + 1)
    for index in maxSlots.indices.reversed() {
        suffixCapacities[index] = suffixCapacities[index + 1] + maxSlots[index]
    }
    return suffixCapacities
}

private func withEachThunderboltRemoteSlotVector(
    maxSlots: [Int],
    suffixCapacities: [Int],
    targetSum: Int,
    index: Int,
    current: inout [Int],
    visit: ([Int]) async throws -> Void
) async rethrows {
    if index == maxSlots.count {
        guard targetSum == 0 else { return }
        try await visit(current)
        return
    }

    let remainingCapacity = suffixCapacities[index + 1]
    let lowerBound = max(0, targetSum - remainingCapacity)
    let upperBound = min(maxSlots[index], targetSum)
    guard lowerBound <= upperBound else { return }

    for value in lowerBound...upperBound {
        current[index] = value
        try await withEachThunderboltRemoteSlotVector(
            maxSlots: maxSlots,
            suffixCapacities: suffixCapacities,
            targetSum: targetSum - value,
            index: index + 1,
            current: &current,
            visit: visit
        )
    }
}

func burstSweepLocalSlotsMax(videoEncodeEngines: Int, totalCores: Int) -> Int {
    ThunderboltCapabilities.sweepCeiling(totalCores: totalCores, videoEncodeEngines: videoEncodeEngines)
}

func validateThunderboltBurstSearchSpace(
    maxLocal: Int,
    maxRemoteSlots: [Int],
    sweepMode: BurstSearchStrategy,
    limit: Int = 100_000
) throws {
    guard case .bruteForce = sweepMode else {
        return
    }

    var configCount = maxLocal + 1
    let allowedPreSubtractionCount = limit == Int.max ? limit : limit + 1
    for maxSlot in maxRemoteSlots {
        let (product, overflow) = configCount.multipliedReportingOverflow(by: maxSlot + 1)
        if overflow || product > allowedPreSubtractionCount {
            throw ThunderboltBenchmarkJSONError.noBurstConfigs
        }
        configCount = product
    }
    configCount -= 1
    guard configCount <= limit else {
        throw ThunderboltBenchmarkJSONError.noBurstConfigs
    }
}

func executeThunderboltBurstSweep(
    videos: [MediaFile],
    workers: [ThunderboltBoundWorkerSpec],
    port: Int,
    connectTimeout: Int,
    preset: String,
    timeout: Int,
    hardware: HardwareProfile,
    sweepMode: BurstSearchStrategy = .optimized(),
    configExecutor: ThunderboltBurstSweepConfigExecutor = {
        config,
        workers,
        videos,
        port,
        connectTimeout,
        preset,
        timeout,
        onProgress in
        try await defaultThunderboltBurstSweepConfigExecutor(
            config: config,
            workers: workers,
            videos: videos,
            port: port,
            connectTimeout: connectTimeout,
            preset: preset,
            timeout: timeout,
            onProgress: onProgress
        )
    },
    headerPrinter: ThunderboltBurstSweepHeaderPrinter? = nil,
    rowPrinter: ThunderboltBurstSweepRowPrinter? = nil
) async throws -> ThunderboltBurstSweepExecutionResult {
    let maxRemoteSlots = effectiveRemoteMaxSlots(workers: workers)
    let maxLocal = burstSweepLocalSlotsMax(
        videoEncodeEngines: hardware.videoEncodeEngines,
        totalCores: hardware.totalCores
    )
    try validateThunderboltBurstSearchSpace(
        maxLocal: maxLocal,
        maxRemoteSlots: maxRemoteSlots,
        sweepMode: sweepMode
    )

    switch sweepMode {
    case .bruteForce:
        let configCount = thunderboltBurstConfigCount(
            maxSlots: maxRemoteSlots,
            localSlotsRange: 0...maxLocal
        )
        guard configCount > 0 else {
            throw ThunderboltBenchmarkJSONError.noBurstConfigs
        }

        let columns = burstSweepColumns(workerCount: workers.count)
        headerPrinter?(columns)

        var runs: [(ThunderboltBurstConfig, ThunderboltBurstResult)] = []
        runs.reserveCapacity(configCount)

        try await withEachThunderboltBurstConfigBaselineFirst(
            maxSlots: maxRemoteSlots,
            localSlotsRange: 0...maxLocal
        ) { run, total, config in
            let result = try await configExecutor(
                config,
                workers,
                videos,
                port,
                connectTimeout,
                preset,
                timeout
            ) { done, total in
                BenchOutput.write(burstSweepProgressBar(done: done, total: total))
            }
            clearThunderboltBurstSweepProgressLine()
            runs.append((config, result))
            rowPrinter?(
                burstSweepRow(run: run, total: total, config: config, result: result),
                columns,
                []
            )
        }

        return ThunderboltBurstSweepExecutionResult(
            displayedRuns: runs,
            evaluatedRuns: runs,
            bestRun: selectThunderboltBestSuccessfulBurstRun(from: runs)
        )

    case .optimized(let topK):
        let columns = burstSweepColumns(workerCount: workers.count, includePrediction: true)
        headerPrinter?(columns)

        let fullResults = Mutex<[[Int]: ThunderboltBurstResult]>([:])
        let evaluate: @Sendable ([Int]) async throws -> Double = { config in
            guard let burstConfig = burstConfigFromOptimizerConfig(config, workerCount: workers.count) else {
                throw ThunderboltBenchmarkJSONError.noBurstConfigs
            }
            let result = try await configExecutor(
                burstConfig,
                workers,
                videos,
                port,
                connectTimeout,
                preset,
                timeout
            ) { done, total in
                BenchOutput.write(burstSweepProgressBar(done: done, total: total))
            }
            clearThunderboltBurstSweepProgressLine()
            fullResults.withLock { results in
                results[config] = result
            }
            if result.failed > 0 {
                return 1_000_000.0
            }
            return result.wallSeconds
        }

        let ceilings = [maxLocal] + maxRemoteSlots
        let searchResult = try await optimizeBurstConcurrency(
            ceilings: ceilings,
            evaluate: evaluate,
            numVideos: videos.count,
            topK: topK,
            onEval: { record in
                guard let burstConfig = burstConfigFromOptimizerConfig(record.config, workerCount: workers.count) else {
                    return
                }
                let evalResult = fullResults.withLock { $0[record.config] }
                rowPrinter?(
                    burstSweepOptimizerRow(
                        phase: record.phase,
                        config: burstConfig,
                        predicted: record.predicted,
                        result: evalResult,
                        elapsed: record.elapsed,
                        fallbackFailedCount: videos.count
                    ),
                    columns,
                    []
                )
            }
        )

        let snapshot = fullResults.withLock { $0 }
        let displayedRuns = searchResult.history.compactMap { record -> (ThunderboltBurstConfig, ThunderboltBurstResult)? in
            guard let config = burstConfigFromOptimizerConfig(record.config, workerCount: workers.count) else {
                return nil
            }
            if let result = snapshot[record.config] {
                return (config, result)
            }
            return (
                config,
                ThunderboltBurstResult(
                    wallSeconds: record.elapsed,
                    completed: 0,
                    failed: videos.count,
                    completionSeconds: []
                )
            )
        }
        let evaluatedRuns = snapshot.compactMap { optimizerConfig, result -> (ThunderboltBurstConfig, ThunderboltBurstResult)? in
            guard let config = burstConfigFromOptimizerConfig(
                optimizerConfig,
                workerCount: workers.count
            ) else {
                return nil
            }
            return (config, result)
        }
        let bestRun: (ThunderboltBurstConfig, ThunderboltBurstResult)? = {
            guard let bestConfig = burstConfigFromOptimizerConfig(
                searchResult.bestConfig,
                workerCount: workers.count
            ),
            let bestResult = snapshot[searchResult.bestConfig] else {
                return nil
            }
            return (bestConfig, bestResult)
        }()

        return ThunderboltBurstSweepExecutionResult(
            displayedRuns: displayedRuns,
            evaluatedRuns: evaluatedRuns,
            bestRun: bestRun
        )
    }
}

func runThunderboltBurstConfig(
    config: ThunderboltBurstConfig,
    workers: [ThunderboltBoundWorkerSpec],
    videos: [MediaFile],
    port: Int,
    connectTimeout: Int,
    preset: String,
    timeout: Int,
    onProgress: (@Sendable (Int, Int) -> Void)? = nil,
    loopCancellationCheck: @escaping @Sendable (ThunderboltLoopCancellationPoint) throws -> Void = { _ in
        try Task.checkCancellation()
    }
) async throws -> ThunderboltBurstResult {
    let tempDir = makeTempDir("tb-burst")
    let thumbsDir = "\(tempDir)/thumbs"
    let previewsDir = "\(tempDir)/previews"
    try FileManager.default.createDirectory(atPath: thumbsDir, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(atPath: previewsDir, withIntermediateDirectories: true)
    defer { cleanup(tempDir) }

    let activeWorkers = zip(workers, config.remoteSlots).compactMap { worker, slots -> (host: String, slots: Int)? in
        slots > 0 ? (worker.connectHost, slots) : nil
    }

    let dispatcherWorkers = parseThunderboltWorkers(
        activeWorkers.map { "\($0.host):\($0.slots)" }.joined(separator: ",")
    )

    let dispatcher: ThunderboltDispatcher?
    if dispatcherWorkers.isEmpty {
        dispatcher = nil
    } else {
        let candidate = ThunderboltDispatcher(
            workers: dispatcherWorkers,
            port: port,
            connectTimeout: connectTimeout,
            thumbsDir: thumbsDir,
            previewsDir: previewsDir,
            sha256BufferSize: BenchDefaults.sha256BufferSize
        )
        dispatcher = await candidate.hasBridges() ? candidate : nil
    }
    let dispatcherTargets: [(workerIndex: Int, slotIndex: Int)] = dispatcherWorkers.enumerated().flatMap { workerIndex, worker in
        (0..<worker.slots).map { slotIndex in
            (workerIndex: workerIndex, slotIndex: slotIndex)
        }
    }

    let laneTargets: [(workerIndex: Int, slotIndex: Int)?] = {
        var targets = Array(repeating: Optional<(workerIndex: Int, slotIndex: Int)>.none, count: max(0, config.localSlots))
        for target in dispatcherTargets {
            targets.append(target)
        }
        if targets.isEmpty {
            targets = [nil]
        }
        return targets
    }()
    let cursor = ThunderboltJobCursor(total: videos.count)
    let store = ThunderboltBurstStore()

    let clock = ContinuousClock()
    let started = clock.now

    do {
        try await withThrowingTaskGroup(of: Void.self) { group in
            for (lane, laneTarget) in laneTargets.enumerated() {
                group.addTask {
                    while true {
                        try loopCancellationCheck(.burstDispatchLoop)
                        guard let index = await cursor.next() else { return }
                        try loopCancellationCheck(.burstDispatchLoop)

                        let video = videos[index]
                        let uploadId = String(format: "tb-burst-%02d-%06d", lane + 1, index)

                        let runLocalJob: @Sendable () async -> Bool = {
                            await runThunderboltLocalVideoJob(
                                video: video,
                                uploadId: uploadId,
                                thumbsDir: thumbsDir,
                                previewsDir: previewsDir,
                                preset: preset,
                                timeout: timeout
                            )
                        }

                        let success: Bool
                        if let target = laneTarget, let dispatcher {
                            try loopCancellationCheck(.burstDispatchLoop)
                            let dispatchResult = await dispatcher.dispatch(
                                uploadId: uploadId,
                                filePath: video.path,
                                originalName: video.name,
                                mimeType: nil,
                                targetWorkerIndex: target.workerIndex,
                                targetSlotIndex: target.slotIndex
                            )
                            switch dispatchResult {
                            case .success:
                                success = true
                            case .fallbackLocal, .transientRetry, .permanentFailure:
                                success = await runLocalJob()
                            }
                        } else {
                            success = await runLocalJob()
                        }

                        let completedAt = (clock.now - started).seconds
                        let done = await store.append(
                            ThunderboltBurstJob(completedAt: completedAt, success: success)
                        )
                        try loopCancellationCheck(.burstDispatchLoop)
                        onProgress?(done, videos.count)
                    }
                }
            }
            try await group.waitForAll()
        }
    } catch {
        if let dispatcher {
            await dispatcher.shutdown()
        }
        throw error
    }

    if let dispatcher {
        await dispatcher.shutdown()
    }

    let jobs = await store.snapshot()
    let wall = (clock.now - started).seconds
    let completionTimes = jobs.filter(\.success).map(\.completedAt)

    return ThunderboltBurstResult(
        wallSeconds: wall,
        completed: completionTimes.count,
        failed: jobs.count - completionTimes.count,
        completionSeconds: completionTimes
    )
}

func runThunderboltLocalRoundTrip(
    video: MediaFile,
    uploadId: String,
    thumbsDir: String,
    previewsDir: String,
    preset: String,
    timeout: Int
) async -> (seconds: Double, success: Bool) {
    try? FileManager.default.createDirectory(atPath: thumbsDir, withIntermediateDirectories: true)
    try? FileManager.default.createDirectory(atPath: previewsDir, withIntermediateDirectories: true)

    let clock = ContinuousClock()
    let started = clock.now
    let success = await runThunderboltLocalVideoJob(
        video: video,
        uploadId: uploadId,
        thumbsDir: thumbsDir,
        previewsDir: previewsDir,
        preset: preset,
        timeout: timeout
    )
    return ((clock.now - started).seconds, success)
}

func runThunderboltLocalVideoJob(
    video: MediaFile,
    uploadId: String,
    thumbsDir: String,
    previewsDir: String,
    preset: String,
    timeout: Int
) async -> Bool {
    let thumbPath = "\(thumbsDir)/\(uploadId).jpg"
    let previewPath = "\(previewsDir)/\(uploadId).mp4"

    async let thumbSuccess: Bool = {
        do {
            try await VideoProcessor.generateThumbnail(
                sourcePath: video.path,
                outputPath: thumbPath,
                size: 512,
                time: 1.0,
                quality: 0.85
            )
            return true
        } catch {
            return false
        }
    }()

    async let previewSuccess: Bool = {
        do {
            try await VideoProcessor.transcode(
                sourcePath: video.path,
                outputPath: previewPath,
                timeoutSeconds: timeout,
                preset: preset
            )
            return true
        } catch {
            return false
        }
    }()

    let (thumbOK, previewOK) = await (thumbSuccess, previewSuccess)
    return thumbOK && previewOK
}

func runThunderboltRoundTrip(
    worker: ThunderboltBoundWorkerSpec,
    video: MediaFile,
    sourceSHA256: String,
    port: Int,
    connectTimeout: Int,
    tempDir: String
) -> ThunderboltRoundTripResult {
    let result = ThunderboltRawExecution.runRemoteRoundTrip(
        host: worker.connectHost,
        port: port,
        sourceIP: worker.sourceIP,
        connectTimeoutMS: connectTimeout,
        filePath: video.path,
        fileSize: video.sizeBytes,
        originalName: video.name,
        mimeType: "",
        sourceSHA256: sourceSHA256,
        tempDir: tempDir,
        sha256BufferSize: BenchDefaults.sha256BufferSize
    )
    return ThunderboltRoundTripResult(
        success: result.success,
        sendSeconds: result.sendSeconds,
        processNanos: result.processNanos,
        receiveSeconds: result.receiveSeconds,
        totalSeconds: result.totalSeconds,
        firstRunningLatencySecondsEstimate: result.firstRunningLatencySecondsEstimate,
        txOutMS: result.txOutMS,
        publishOverheadMS: result.publishOverheadMS,
        slotHealthDownOnFailure: result.slotHealthDownOnFailure
    )
}
