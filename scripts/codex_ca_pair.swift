#!/usr/bin/env swift
import Dispatch
import Foundation

let validProfiles = ["all-at-once", "burst-1-20-5-5-1", "trickle"]
let bothOrders = [["fifo", "ca"], ["ca", "fifo"]]

enum ScriptError: LocalizedError {
    case cannotLocateRepositoryRoot
    case missingMediaFolder
    case missingValue(String)
    case invalidValue(option: String, value: String, reason: String)
    case conflictingPassthroughFlag(String)
    case benchmarkFailed(policy: String, status: Int32)
    case missingArtifact(String)
    case invalidOrder(String)
    case matrixOnlyOption(String)
    case regressionDetected(String)

    var errorDescription: String? {
        switch self {
        case .cannotLocateRepositoryRoot:
            return "Could not locate repository root (expected Package.swift)."
        case .missingMediaFolder:
            return "Missing media folder. Pass --media-folder <path> or use a positional path."
        case .missingValue(let option):
            return "Missing value for \(option)."
        case let .invalidValue(option, value, reason):
            return "Invalid value for \(option): \(value) (\(reason))."
        case let .conflictingPassthroughFlag(flag):
            return "Passthrough benchmark args cannot include \(flag); the script manages that flag."
        case let .benchmarkFailed(policy, status):
            return "Benchmark run for \(policy.uppercased()) failed with exit status \(status)."
        case let .missingArtifact(path):
            return "Expected artifact was not created: \(path)"
        case let .invalidOrder(value):
            return "Invalid --order '\(value)'. Expected fifo,ca or ca,fifo."
        case .matrixOnlyOption(let option):
            return "\(option) is only valid with --matrix."
        case .regressionDetected(let detail):
            return "Regression detected: \(detail)"
        }
    }
}

struct ScriptConfig {
    var mediaFolder: String?
    var arrivalProfile = "all-at-once"
    var arrivalProfileWasExplicit = false
    var modelMode = "auto"
    var videoPreset: String?
    var videoTimeout: Int?
    var order: [String] = ["fifo", "ca"]
    var orderWasExplicit = false
    var releaseBuild = true
    var outputDirectory: String?
    var benchmarkPassthrough: [String] = []
    var matrixMode = false
    var profilesSelection: [String]?
    var ordersSelection: [[String]]?
    var summaryOutput: String?
    var keepRaw = false
    var failOnRegression = false
}

struct RunArtifact: Decodable {
    let schedulerPolicy: String
    let arrivalProfile: String
    let totalJobs: Int
    let successfulJobs: Int
    let failedCount: Int
    let metrics: RunMetrics
    let jobs: [RunJob]
    let observability: RunObservability?

    enum CodingKeys: String, CodingKey {
        case schedulerPolicy = "scheduler_policy"
        case arrivalProfile = "arrival_profile"
        case totalJobs = "total_jobs"
        case successfulJobs = "successful_jobs"
        case failedCount = "failed_count"
        case metrics
        case jobs
        case observability
    }
}

struct RunMetrics: Decodable {
    let sumWSeconds: Double
    let p95Seconds: Double
    let makespanSeconds: Double
    let failedCount: Int

    enum CodingKeys: String, CodingKey {
        case sumWSeconds = "sumW_seconds"
        case p95Seconds = "p95_seconds"
        case makespanSeconds = "makespan_seconds"
        case failedCount = "failed_count"
    }
}

struct RunJob: Decodable {
    let actualExecutor: String

    enum CodingKeys: String, CodingKey {
        case actualExecutor = "actualExecutor"
    }
}

struct RunObservability: Decodable {
    let adaptation: [RunAdaptation]
    let predictions: [RunPrediction]
}

struct RunAdaptation: Decodable {
    let machineID: String
    let completions: Int
    let initialMSPerFrameC1: Double
    let finalMSPerFrameC1: Double

    enum CodingKeys: String, CodingKey {
        case machineID = "machine_id"
        case completions
        case initialMSPerFrameC1 = "initial_ms_per_frame_c1"
        case finalMSPerFrameC1 = "final_ms_per_frame_c1"
    }
}

struct RunPrediction: Decodable {
    let waited: Bool
    let executorMismatch: Bool

    enum CodingKeys: String, CodingKey {
        case waited
        case executorMismatch = "executor_mismatch"
    }
}

struct RunSummary {
    let policy: String
    let artifact: RunArtifact
    let rawPath: String
    let summaryPath: String

    var executorCounts: [(executor: String, count: Int)] {
        let counts = artifact.jobs.reduce(into: [String: Int]()) { partial, job in
            partial[job.actualExecutor, default: 0] += 1
        }
        return counts.keys.sorted {
            let left = counts[$0] ?? 0
            let right = counts[$1] ?? 0
            if left != right { return left > right }
            return $0 < $1
        }.map { ($0, counts[$0] ?? 0) }
    }

    var waitedCount: Int {
        artifact.observability?.predictions.filter(\.waited).count ?? 0
    }

    var executorMismatchCount: Int {
        artifact.observability?.predictions.filter(\.executorMismatch).count ?? 0
    }

    var adaptationRows: [RunAdaptation] {
        (artifact.observability?.adaptation ?? []).sorted { $0.machineID < $1.machineID }
    }
}

struct MetricsSnapshot: Codable {
    let sumWSeconds: Double
    let p95Seconds: Double
    let makespanSeconds: Double
    let failedCount: Int
}

struct ExecutorCountSnapshot: Codable {
    let executor: String
    let count: Int
}

struct AdaptationSnapshot: Codable {
    let machineID: String
    let completions: Int
    let initialMSPerFrameC1: Double
    let finalMSPerFrameC1: Double
    let deltaPercent: Double
}

struct PolicySnapshot: Codable {
    let policy: String
    let totalJobs: Int
    let successfulJobs: Int
    let failedCount: Int
    let metrics: MetricsSnapshot
    let executorCounts: [ExecutorCountSnapshot]
    let waitedCount: Int
    let executorMismatchCount: Int
    let adaptation: [AdaptationSnapshot]
}

struct ComparisonSnapshot: Codable {
    let failedCountDelta: Int
    let sumWSecondsDelta: Double
    let p95SecondsDelta: Double
    let makespanSecondsDelta: Double
    let pass: Bool
}

struct PolicyArtifactsSnapshot: Codable {
    let rawPath: String
    let summaryPath: String
}

struct RetainedArtifactsSnapshot: Codable {
    let outputDirectory: String
    let benchmarkReportDirectory: String
    let comparePath: String
    let fifo: PolicyArtifactsSnapshot
    let ca: PolicyArtifactsSnapshot
}

struct PairRunRecord: Codable {
    let profile: String
    let modelMode: String
    let order: [String]
    let fifo: PolicySnapshot
    let ca: PolicySnapshot
    let comparison: ComparisonSnapshot
    let artifacts: RetainedArtifactsSnapshot?
}

struct RollupPolicySnapshot: Codable {
    let sampleCount: Int
    let averageFailedCount: Double
    let averageSumWSeconds: Double
    let averageP95Seconds: Double
    let averageMakespanSeconds: Double
    let averageWaitedCount: Double
    let averageExecutorMismatchCount: Double
}

struct RollupComparisonSnapshot: Codable {
    let failedCountDelta: Double
    let sumWSecondsDelta: Double
    let p95SecondsDelta: Double
    let makespanSecondsDelta: Double
    let pass: Bool
}

struct MatrixRollupRecord: Codable {
    let profile: String
    let modelMode: String
    let observedOrders: [[String]]
    let fifo: RollupPolicySnapshot
    let ca: RollupPolicySnapshot
    let comparison: RollupComparisonSnapshot
}

struct MatrixSummaryArtifact: Codable {
    let generatedAt: String
    let mediaFolder: String
    let build: String
    let outputDirectory: String
    let keepRaw: Bool
    let failOnRegression: Bool
    let profiles: [String]
    let modelModes: [String]
    let orders: [[String]]
    let runs: [PairRunRecord]
    let rollups: [MatrixRollupRecord]
    let overallPass: Bool
}

var cleanupPaths = Set<String>()
var cleanupSignalSources: [DispatchSourceSignal] = []

struct PairSpec: Hashable {
    let profile: String
    let modelMode: String
    let order: [String]
}

struct PairExecutionResult {
    let record: PairRunRecord
    let outputDirectory: String
}

func printUsage() {
    let usage = """
    Usage:
      swift scripts/codex_ca_pair.swift --media-folder <path> [options] [-- <extra benchmark args>]
      swift scripts/codex_ca_pair.swift <media-folder> [options] [-- <extra benchmark args>]

    Pair mode options:
      --profile <profile>       Arrival profile: all-at-once, burst-1-20-5-5-1, trickle
                                Default: all-at-once
      --model-mode <mode>       CA model mode: auto or strict
                                Default: auto
      --order <fifo,ca|ca,fifo> Run order for the pair
                                Default: fifo,ca

    Shared options:
      --video-preset <preset>   Forwarded to benchmark
      --video-timeout <secs>    Forwarded to benchmark
      --out-dir <path>          Artifact directory
      --debug                   Use debug build instead of release
      --release                 Use release build (default)
      --fail-on-regression      Exit non-zero if CA fails the comparison gate

    Matrix mode options:
      --matrix                  Run a proof matrix instead of one pair
      --profiles <value>        all, or a comma-separated subset of profiles
      --orders <value>          both, fifo,ca, or ca,fifo
      --model-mode <mode>       auto, strict, or both
      --summary-out <path>      Aggregate JSON summary path
      --keep-raw                Keep per-run raw artifacts and markdown summaries

    Notes:
      - This is a thin wrapper over the existing thunderbolt CA benchmark path.
      - Extra args after `--` are forwarded to all benchmark runs.
      - The script reserves these benchmark flags and will reject them in passthrough:
        --stage, --media-folder, --arrival-profile, --scheduler-policy,
        --ca-raw-out, --ca-summary-out, --ca-model-mode, --report-dir,
        --json, --wizard, --ca-acceptance, --ca-acceptance-out
    """
    print(usage)
}

func locateRepositoryRoot(startingAt url: URL) -> URL? {
    var current = url.standardizedFileURL
    let fm = FileManager.default
    while true {
        if fm.fileExists(atPath: current.appendingPathComponent("Package.swift").path) {
            return current
        }
        let parent = current.deletingLastPathComponent()
        if parent.path == current.path {
            return nil
        }
        current = parent
    }
}

func expandedPath(_ value: String) -> String {
    NSString(string: value).expandingTildeInPath
}

func resolvePath(_ value: String, relativeTo base: URL) -> String {
    let expanded = expandedPath(value)
    if expanded.hasPrefix("/") {
        return URL(fileURLWithPath: expanded).standardizedFileURL.path
    }
    return base.appendingPathComponent(expanded).standardizedFileURL.path
}

func appendTimestamp(_ timestamp: String, to path: String) -> String {
    let url = URL(fileURLWithPath: path)
    let directory = url.deletingLastPathComponent()
    let filename = url.lastPathComponent
    let ext = url.pathExtension
    let stem: String
    if ext.isEmpty {
        stem = filename
    } else {
        stem = String(filename.dropLast(ext.count + 1))
    }
    let stampedName = ext.isEmpty
        ? "\(stem)-\(timestamp)"
        : "\(stem)-\(timestamp).\(ext)"
    return directory.appendingPathComponent(stampedName).path
}

func parseOrder(_ value: String) throws -> [String] {
    let policies = value
        .split(separator: ",", omittingEmptySubsequences: true)
        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
    guard policies.count == 2, Set(policies) == Set(["fifo", "ca"]) else {
        throw ScriptError.invalidOrder(value)
    }
    return policies
}

func parseProfilesSelection(_ value: String) throws -> [String] {
    let lowered = value.lowercased()
    if lowered == "all" {
        return validProfiles
    }

    var result: [String] = []
    var seen = Set<String>()
    let parts = lowered.split(separator: ",", omittingEmptySubsequences: true).map {
        $0.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    guard !parts.isEmpty else {
        throw ScriptError.invalidValue(option: "--profiles", value: value, reason: "expected all or a comma-separated profile list")
    }

    for part in parts {
        guard validProfiles.contains(part) else {
            throw ScriptError.invalidValue(option: "--profiles", value: value, reason: "unknown profile '\(part)'")
        }
        if seen.insert(part).inserted {
            result.append(part)
        }
    }
    return result
}

func parseOrdersSelection(_ value: String) throws -> [[String]] {
    let lowered = value.lowercased()
    if lowered == "both" {
        return bothOrders
    }
    return [try parseOrder(lowered)]
}

func requireValue(_ args: [String], _ index: Int, _ option: String) throws -> String {
    let next = index + 1
    guard args.indices.contains(next) else {
        throw ScriptError.missingValue(option)
    }
    return args[next]
}

func parseArguments(_ args: [String]) throws -> ScriptConfig {
    var config = ScriptConfig()
    var index = 0

    while index < args.count {
        let arg = args[index]
        if arg == "--" {
            config.benchmarkPassthrough = Array(args[(index + 1)...])
            break
        }

        switch arg {
        case "--help", "-h":
            printUsage()
            exit(0)

        case "--matrix":
            config.matrixMode = true
            index += 1

        case "--media-folder":
            config.mediaFolder = try requireValue(args, index, arg)
            index += 2

        case "--profile":
            let value = try requireValue(args, index, arg).lowercased()
            guard validProfiles.contains(value) else {
                throw ScriptError.invalidValue(option: arg, value: value, reason: "expected one of: \(validProfiles.joined(separator: ", "))")
            }
            config.arrivalProfile = value
            config.arrivalProfileWasExplicit = true
            index += 2

        case "--profiles":
            config.profilesSelection = try parseProfilesSelection(try requireValue(args, index, arg))
            index += 2

        case "--model-mode":
            let value = try requireValue(args, index, arg).lowercased()
            let valid = ["auto", "strict", "both"]
            guard valid.contains(value) else {
                throw ScriptError.invalidValue(option: arg, value: value, reason: "expected auto, strict, or both")
            }
            config.modelMode = value
            index += 2

        case "--video-preset":
            config.videoPreset = try requireValue(args, index, arg)
            index += 2

        case "--video-timeout":
            let value = try requireValue(args, index, arg)
            guard let parsed = Int(value), parsed > 0 else {
                throw ScriptError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            config.videoTimeout = parsed
            index += 2

        case "--order":
            config.order = try parseOrder(try requireValue(args, index, arg))
            config.orderWasExplicit = true
            index += 2

        case "--orders":
            config.ordersSelection = try parseOrdersSelection(try requireValue(args, index, arg))
            index += 2

        case "--out-dir":
            config.outputDirectory = try requireValue(args, index, arg)
            index += 2

        case "--summary-out":
            config.summaryOutput = try requireValue(args, index, arg)
            index += 2

        case "--keep-raw":
            config.keepRaw = true
            index += 1

        case "--fail-on-regression":
            config.failOnRegression = true
            index += 1

        case "--debug":
            config.releaseBuild = false
            index += 1

        case "--release":
            config.releaseBuild = true
            index += 1

        default:
            if arg.hasPrefix("-") {
                throw ScriptError.invalidValue(option: "arguments", value: arg, reason: "unknown option")
            }
            if config.mediaFolder == nil {
                config.mediaFolder = arg
                index += 1
            } else {
                throw ScriptError.invalidValue(option: "arguments", value: arg, reason: "unexpected extra positional argument")
            }
        }
    }

    guard config.mediaFolder != nil else {
        throw ScriptError.missingMediaFolder
    }

    let reservedFlags: Set<String> = [
        "--stage",
        "--component",
        "--media-folder",
        "--arrival-profile",
        "--scheduler-policy",
        "--ca-raw-out",
        "--ca-summary-out",
        "--ca-model-mode",
        "--report-dir",
        "--json",
        "--wizard",
        "--ca-acceptance",
        "--ca-acceptance-out",
    ]
    for flag in config.benchmarkPassthrough where reservedFlags.contains(flag) {
        throw ScriptError.conflictingPassthroughFlag(flag)
    }

    if !config.matrixMode {
        if config.profilesSelection != nil { throw ScriptError.matrixOnlyOption("--profiles") }
        if config.ordersSelection != nil { throw ScriptError.matrixOnlyOption("--orders") }
        if config.summaryOutput != nil { throw ScriptError.matrixOnlyOption("--summary-out") }
        if config.keepRaw { throw ScriptError.matrixOnlyOption("--keep-raw") }
        if config.modelMode == "both" {
            throw ScriptError.invalidValue(option: "--model-mode", value: config.modelMode, reason: "both is only valid with --matrix")
        }
    }

    return config
}

@discardableResult
func runProcess(executable: String, arguments: [String], currentDirectory: URL) throws -> Int32 {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = arguments
    process.currentDirectoryURL = currentDirectory
    process.standardInput = FileHandle.standardInput
    process.standardOutput = FileHandle.standardOutput
    process.standardError = FileHandle.standardError
    try process.run()
    process.waitUntilExit()
    return process.terminationStatus
}

func decodeArtifact(at path: String) throws -> RunArtifact {
    guard FileManager.default.fileExists(atPath: path) else {
        throw ScriptError.missingArtifact(path)
    }
    let data = try Data(contentsOf: URL(fileURLWithPath: path))
    return try JSONDecoder().decode(RunArtifact.self, from: data)
}

func formatSeconds(_ value: Double) -> String {
    String(format: "%.3fs", value)
}

func formatSignedSeconds(_ value: Double) -> String {
    String(format: "%+.3fs", value)
}

func formatSignedScalar(_ value: Double) -> String {
    String(format: "%+.3f", value)
}

func renderExecutorCounts(_ counts: [(executor: String, count: Int)]) -> String {
    guard !counts.isEmpty else { return "(none)" }
    return counts.map { "\($0.executor)=\($0.count)" }.joined(separator: ", ")
}

func renderAdaptation(_ rows: [RunAdaptation]) -> String {
    guard !rows.isEmpty else { return "(none)" }
    return rows.map { row in
        let deltaPercent = row.initialMSPerFrameC1 > 0
            ? ((row.finalMSPerFrameC1 / row.initialMSPerFrameC1) - 1.0) * 100.0
            : 0
        return "\(row.machineID) \(String(format: "%.3f", row.initialMSPerFrameC1))->\(String(format: "%.3f", row.finalMSPerFrameC1)) (\(String(format: "%+.1f%%", deltaPercent)), jobs=\(row.completions))"
    }.joined(separator: " | ")
}

func metricsSnapshot(from artifact: RunArtifact) -> MetricsSnapshot {
    MetricsSnapshot(
        sumWSeconds: artifact.metrics.sumWSeconds,
        p95Seconds: artifact.metrics.p95Seconds,
        makespanSeconds: artifact.metrics.makespanSeconds,
        failedCount: artifact.metrics.failedCount
    )
}

func policySnapshot(from summary: RunSummary) -> PolicySnapshot {
    let adaptation = summary.adaptationRows.map { row in
        let deltaPercent = row.initialMSPerFrameC1 > 0
            ? ((row.finalMSPerFrameC1 / row.initialMSPerFrameC1) - 1.0) * 100.0
            : 0
        return AdaptationSnapshot(
            machineID: row.machineID,
            completions: row.completions,
            initialMSPerFrameC1: row.initialMSPerFrameC1,
            finalMSPerFrameC1: row.finalMSPerFrameC1,
            deltaPercent: deltaPercent
        )
    }

    return PolicySnapshot(
        policy: summary.policy,
        totalJobs: summary.artifact.totalJobs,
        successfulJobs: summary.artifact.successfulJobs,
        failedCount: summary.artifact.failedCount,
        metrics: metricsSnapshot(from: summary.artifact),
        executorCounts: summary.executorCounts.map { ExecutorCountSnapshot(executor: $0.executor, count: $0.count) },
        waitedCount: summary.waitedCount,
        executorMismatchCount: summary.executorMismatchCount,
        adaptation: adaptation
    )
}

func comparisonPass(fifo: PolicySnapshot, ca: PolicySnapshot) -> Bool {
    ca.failedCount <= fifo.failedCount &&
    ca.metrics.sumWSeconds < fifo.metrics.sumWSeconds &&
    ca.metrics.makespanSeconds < fifo.metrics.makespanSeconds &&
    ca.metrics.p95Seconds <= fifo.metrics.p95Seconds
}

func comparisonSnapshot(fifo: PolicySnapshot, ca: PolicySnapshot) -> ComparisonSnapshot {
    ComparisonSnapshot(
        failedCountDelta: ca.failedCount - fifo.failedCount,
        sumWSecondsDelta: ca.metrics.sumWSeconds - fifo.metrics.sumWSeconds,
        p95SecondsDelta: ca.metrics.p95Seconds - fifo.metrics.p95Seconds,
        makespanSecondsDelta: ca.metrics.makespanSeconds - fifo.metrics.makespanSeconds,
        pass: comparisonPass(fifo: fifo, ca: ca)
    )
}

func renderComparisonMarkdown(
    mediaFolder: String,
    profile: String,
    modelMode: String,
    releaseBuild: Bool,
    order: [String],
    outputDirectory: String,
    benchmarkReportDirectory: String,
    summaries: [String: RunSummary]
) -> String {
    let fifo = summaries["fifo"]!
    let ca = summaries["ca"]!
    let wallDelta = ca.artifact.metrics.makespanSeconds - fifo.artifact.metrics.makespanSeconds
    let sumWDelta = ca.artifact.metrics.sumWSeconds - fifo.artifact.metrics.sumWSeconds
    let p95Delta = ca.artifact.metrics.p95Seconds - fifo.artifact.metrics.p95Seconds
    let failedDelta = ca.artifact.failedCount - fifo.artifact.failedCount

    return """
    # Codex CA Pair

    - media_folder: \(mediaFolder)
    - arrival_profile: \(profile)
    - model_mode: \(modelMode)
    - benchmark_build: \(releaseBuild ? "release" : "debug")
    - run_order: \(order.joined(separator: " -> "))
    - artifact_directory: \(outputDirectory)
    - benchmark_report_directory: \(benchmarkReportDirectory)

    ## Metrics

    | metric | fifo | ca | delta (ca - fifo) |
    | --- | ---: | ---: | ---: |
    | makespan_seconds | \(String(format: "%.3f", fifo.artifact.metrics.makespanSeconds)) | \(String(format: "%.3f", ca.artifact.metrics.makespanSeconds)) | \(String(format: "%+.3f", wallDelta)) |
    | sumW_seconds | \(String(format: "%.3f", fifo.artifact.metrics.sumWSeconds)) | \(String(format: "%.3f", ca.artifact.metrics.sumWSeconds)) | \(String(format: "%+.3f", sumWDelta)) |
    | p95_seconds | \(String(format: "%.3f", fifo.artifact.metrics.p95Seconds)) | \(String(format: "%.3f", ca.artifact.metrics.p95Seconds)) | \(String(format: "%+.3f", p95Delta)) |
    | failed_count | \(fifo.artifact.failedCount) | \(ca.artifact.failedCount) | \(failedDelta >= 0 ? "+" : "")\(failedDelta) |

    ## FIFO

    - raw_json: \(fifo.rawPath)
    - summary_md: \(fifo.summaryPath)
    - executors: \(renderExecutorCounts(fifo.executorCounts))
    - waited_predictions: \(fifo.waitedCount)
    - executor_mismatches: \(fifo.executorMismatchCount)
    - adaptation: \(renderAdaptation(fifo.adaptationRows))

    ## CA

    - raw_json: \(ca.rawPath)
    - summary_md: \(ca.summaryPath)
    - executors: \(renderExecutorCounts(ca.executorCounts))
    - waited_predictions: \(ca.waitedCount)
    - executor_mismatches: \(ca.executorMismatchCount)
    - adaptation: \(renderAdaptation(ca.adaptationRows))
    """
}

func failureReasons(for comparison: RollupComparisonSnapshot) -> [String] {
    var reasons: [String] = []
    if comparison.failedCountDelta > 0 {
        reasons.append("failed_count +\(String(format: "%.3f", comparison.failedCountDelta))")
    }
    if comparison.sumWSecondsDelta >= 0 {
        reasons.append("sumW \(formatSignedScalar(comparison.sumWSecondsDelta))")
    }
    if comparison.makespanSecondsDelta >= 0 {
        reasons.append("makespan \(formatSignedScalar(comparison.makespanSecondsDelta))")
    }
    if comparison.p95SecondsDelta > 0 {
        reasons.append("p95 \(formatSignedScalar(comparison.p95SecondsDelta))")
    }
    return reasons
}

func renderMatrixSummaryMarkdown(_ summary: MatrixSummaryArtifact, jsonPath: String) -> String {
    let runLines = summary.runs
        .sorted {
            if $0.modelMode != $1.modelMode { return $0.modelMode < $1.modelMode }
            if $0.profile != $1.profile { return $0.profile < $1.profile }
            return orderLabel($0.order) < orderLabel($1.order)
        }
        .map { run in
            let order = run.order.joined(separator: " -> ")
            let reasons = run.comparison.pass
                ? ""
                : failureReasons(
                    for: RollupComparisonSnapshot(
                        failedCountDelta: Double(run.comparison.failedCountDelta),
                        sumWSecondsDelta: run.comparison.sumWSecondsDelta,
                        p95SecondsDelta: run.comparison.p95SecondsDelta,
                        makespanSecondsDelta: run.comparison.makespanSecondsDelta,
                        pass: run.comparison.pass
                    )
                ).joined(separator: ", ")
            return "| \(run.modelMode) | \(run.profile) | \(order) | \(String(format: "%.3f", run.fifo.metrics.makespanSeconds)) | \(String(format: "%.3f", run.ca.metrics.makespanSeconds)) | \(formatSignedScalar(run.comparison.makespanSecondsDelta)) | \(String(format: "%.3f", run.fifo.metrics.sumWSeconds)) | \(String(format: "%.3f", run.ca.metrics.sumWSeconds)) | \(formatSignedScalar(run.comparison.sumWSecondsDelta)) | \(String(format: "%.3f", run.fifo.metrics.p95Seconds)) | \(String(format: "%.3f", run.ca.metrics.p95Seconds)) | \(formatSignedScalar(run.comparison.p95SecondsDelta)) | \(run.fifo.failedCount) | \(run.ca.failedCount) | \(run.comparison.failedCountDelta >= 0 ? "+" : "")\(run.comparison.failedCountDelta) | \(run.comparison.pass ? "PASS" : "FAIL") | \(reasons) |"
        }.joined(separator: "\n")

    let rollupLines = summary.rollups.map { rollup in
        let reasonText = rollup.comparison.pass ? "" : failureReasons(for: rollup.comparison).joined(separator: ", ")
        return "| \(rollup.modelMode) | \(rollup.profile) | \(String(format: "%.3f", rollup.fifo.averageMakespanSeconds)) | \(String(format: "%.3f", rollup.ca.averageMakespanSeconds)) | \(formatSignedScalar(rollup.comparison.makespanSecondsDelta)) | \(String(format: "%.3f", rollup.fifo.averageSumWSeconds)) | \(String(format: "%.3f", rollup.ca.averageSumWSeconds)) | \(formatSignedScalar(rollup.comparison.sumWSecondsDelta)) | \(String(format: "%.3f", rollup.fifo.averageP95Seconds)) | \(String(format: "%.3f", rollup.ca.averageP95Seconds)) | \(formatSignedScalar(rollup.comparison.p95SecondsDelta)) | \(String(format: "%.3f", rollup.fifo.averageFailedCount)) | \(String(format: "%.3f", rollup.ca.averageFailedCount)) | \(formatSignedScalar(rollup.comparison.failedCountDelta)) | \(rollup.comparison.pass ? "PASS" : "FAIL") | \(reasonText) |"
    }.joined(separator: "\n")

    let failedRollups = summary.rollups.filter { !$0.comparison.pass }
    let failureSection: String
    if failedRollups.isEmpty {
        failureSection = "## Failures\n\nNone.\n"
    } else {
        let lines = failedRollups.map { rollup in
            "- `\(rollup.modelMode)` / `\(rollup.profile)`: \(failureReasons(for: rollup.comparison).joined(separator: ", "))"
        }.joined(separator: "\n")
        failureSection = "## Failures\n\n\(lines)\n"
    }

    return """
    # Codex CA Matrix Summary

    - generated_at: \(summary.generatedAt)
    - media_folder: \(summary.mediaFolder)
    - build: \(summary.build)
    - keep_raw: \(summary.keepRaw ? "true" : "false")
    - fail_on_regression: \(summary.failOnRegression ? "true" : "false")
    - overall_pass: \(summary.overallPass ? "true" : "false")
    - json_summary: \(jsonPath)

    ## Runs

    | model_mode | profile | order | fifo_makespan | ca_makespan | delta_makespan | fifo_sumW | ca_sumW | delta_sumW | fifo_p95 | ca_p95 | delta_p95 | fifo_failed | ca_failed | delta_failed | verdict | reasons |
    | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
    \(runLines)

    ## Rollups

    | model_mode | profile | fifo_makespan | ca_makespan | delta_makespan | fifo_sumW | ca_sumW | delta_sumW | fifo_p95 | ca_p95 | delta_p95 | fifo_failed | ca_failed | delta_failed | verdict | reasons |
    | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
    \(rollupLines)

    \(failureSection)
    """
}

func orderLabel(_ order: [String]) -> String {
    order.joined(separator: "-")
}

func printPairComparison(_ record: PairRunRecord) {
    print("Comparison [\(record.modelMode) | \(record.profile) | \(record.order.joined(separator: " -> "))]")
    print("  makespan: \(formatSeconds(record.fifo.metrics.makespanSeconds)) / \(formatSeconds(record.ca.metrics.makespanSeconds))  delta \(formatSignedSeconds(record.comparison.makespanSecondsDelta))")
    print("  sumW:     \(formatSeconds(record.fifo.metrics.sumWSeconds)) / \(formatSeconds(record.ca.metrics.sumWSeconds))  delta \(formatSignedSeconds(record.comparison.sumWSecondsDelta))")
    print("  p95:      \(formatSeconds(record.fifo.metrics.p95Seconds)) / \(formatSeconds(record.ca.metrics.p95Seconds))  delta \(formatSignedSeconds(record.comparison.p95SecondsDelta))")
    print("  failed:   \(record.fifo.failedCount) / \(record.ca.failedCount)  delta \(record.comparison.failedCountDelta >= 0 ? "+" : "")\(record.comparison.failedCountDelta)")
    print("  fifo waited: \(record.fifo.waitedCount)  mismatches: \(record.fifo.executorMismatchCount)")
    print("  ca waited:   \(record.ca.waitedCount)  mismatches: \(record.ca.executorMismatchCount)")
    print("  pass: \(record.comparison.pass ? "yes" : "no")")
}

func average(_ values: [Double]) -> Double {
    guard !values.isEmpty else { return 0 }
    return values.reduce(0, +) / Double(values.count)
}

func buildRollups(from runs: [PairRunRecord]) -> [MatrixRollupRecord] {
    let grouped = Dictionary(grouping: runs) { (run: PairRunRecord) in
        "\(run.modelMode)|\(run.profile)"
    }

    return grouped.keys.sorted().compactMap { key in
        guard let group = grouped[key], let first = group.first else { return nil }

        let fifo = RollupPolicySnapshot(
            sampleCount: group.count,
            averageFailedCount: average(group.map { Double($0.fifo.failedCount) }),
            averageSumWSeconds: average(group.map { $0.fifo.metrics.sumWSeconds }),
            averageP95Seconds: average(group.map { $0.fifo.metrics.p95Seconds }),
            averageMakespanSeconds: average(group.map { $0.fifo.metrics.makespanSeconds }),
            averageWaitedCount: average(group.map { Double($0.fifo.waitedCount) }),
            averageExecutorMismatchCount: average(group.map { Double($0.fifo.executorMismatchCount) })
        )
        let ca = RollupPolicySnapshot(
            sampleCount: group.count,
            averageFailedCount: average(group.map { Double($0.ca.failedCount) }),
            averageSumWSeconds: average(group.map { $0.ca.metrics.sumWSeconds }),
            averageP95Seconds: average(group.map { $0.ca.metrics.p95Seconds }),
            averageMakespanSeconds: average(group.map { $0.ca.metrics.makespanSeconds }),
            averageWaitedCount: average(group.map { Double($0.ca.waitedCount) }),
            averageExecutorMismatchCount: average(group.map { Double($0.ca.executorMismatchCount) })
        )
        let comparison = RollupComparisonSnapshot(
            failedCountDelta: ca.averageFailedCount - fifo.averageFailedCount,
            sumWSecondsDelta: ca.averageSumWSeconds - fifo.averageSumWSeconds,
            p95SecondsDelta: ca.averageP95Seconds - fifo.averageP95Seconds,
            makespanSecondsDelta: ca.averageMakespanSeconds - fifo.averageMakespanSeconds,
            pass: ca.averageFailedCount <= fifo.averageFailedCount &&
                ca.averageSumWSeconds < fifo.averageSumWSeconds &&
                ca.averageMakespanSeconds < fifo.averageMakespanSeconds &&
                ca.averageP95Seconds <= fifo.averageP95Seconds
        )
        let orders = group
            .map(\.order)
            .sorted { orderLabel($0) < orderLabel($1) }

        return MatrixRollupRecord(
            profile: first.profile,
            modelMode: first.modelMode,
            observedOrders: orders,
            fifo: fifo,
            ca: ca,
            comparison: comparison
        )
    }
}

func encodeJSON<T: Encodable>(_ value: T, to path: String) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    let data = try encoder.encode(value)
    try data.write(to: URL(fileURLWithPath: path), options: .atomic)
}

func registerCleanupPath(_ path: String) {
    cleanupPaths.insert(path)
}

func unregisterCleanupPath(_ path: String) {
    cleanupPaths.remove(path)
}

func performRegisteredCleanup() {
    let fm = FileManager.default
    for path in cleanupPaths.sorted(by: { $0.count > $1.count }) {
        try? fm.removeItem(atPath: path)
    }
}

func removeDirectoryIfEffectivelyEmpty(_ path: String) {
    let fm = FileManager.default
    guard let contents = try? fm.contentsOfDirectory(atPath: path) else { return }
    let meaningful = contents.filter { $0 != ".DS_Store" }
    if meaningful.isEmpty {
        try? fm.removeItem(atPath: path)
    }
}

func installCleanupSignalHandlers() {
    signal(SIGINT, SIG_IGN)
    signal(SIGTERM, SIG_IGN)

    let signals = [SIGINT, SIGTERM]
    cleanupSignalSources = signals.map { signalNumber in
        let source = DispatchSource.makeSignalSource(signal: signalNumber, queue: .global())
        source.setEventHandler {
            performRegisteredCleanup()
            let code: Int32 = signalNumber == SIGINT ? 130 : 143
            exit(code)
        }
        source.resume()
        return source
    }
}

func resolvedProfiles(for config: ScriptConfig) -> [String] {
    if !config.matrixMode {
        return [config.arrivalProfile]
    }
    if let selection = config.profilesSelection {
        return selection
    }
    if config.arrivalProfileWasExplicit {
        return [config.arrivalProfile]
    }
    return validProfiles
}

func resolvedOrders(for config: ScriptConfig) -> [[String]] {
    if !config.matrixMode {
        return [config.order]
    }
    if let selection = config.ordersSelection {
        return selection
    }
    if config.orderWasExplicit {
        return [config.order]
    }
    return bothOrders
}

func resolvedModelModes(for config: ScriptConfig) -> [String] {
    if !config.matrixMode {
        return [config.modelMode]
    }
    if config.modelMode == "both" {
        return ["auto", "strict"]
    }
    return [config.modelMode]
}

func runPair(
    spec: PairSpec,
    mediaFolder: String,
    outputDirectory: String,
    repoRoot: URL,
    releaseBuild: Bool,
    videoPreset: String?,
    videoTimeout: Int?,
    benchmarkPassthrough: [String]
) throws -> PairExecutionResult {
    let benchmarkReportDirectory = "\(outputDirectory)/reports"
    try FileManager.default.createDirectory(atPath: outputDirectory, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(atPath: benchmarkReportDirectory, withIntermediateDirectories: true)

    let benchmarkArgumentsPrefix: [String] = releaseBuild
        ? ["run", "-c", "release", "benchmark"]
        : ["run", "benchmark"]

    var commonBenchmarkArgs = [
        "--stage", "thunderbolt",
        "--media-folder", mediaFolder,
        "--arrival-profile", spec.profile,
        "--ca-model-mode", spec.modelMode,
        "--report-dir", benchmarkReportDirectory,
    ]
    if let videoPreset {
        commonBenchmarkArgs.append(contentsOf: ["--video-preset", videoPreset])
    }
    if let videoTimeout {
        commonBenchmarkArgs.append(contentsOf: ["--video-timeout", String(videoTimeout)])
    }
    commonBenchmarkArgs.append(contentsOf: benchmarkPassthrough)

    print("Running pair [\(spec.modelMode) | \(spec.profile) | \(spec.order.joined(separator: " -> "))]")

    var summariesByPolicy: [String: RunSummary] = [:]
    for policy in spec.order {
        let rawPath = "\(outputDirectory)/\(policy).raw.json"
        let summaryPath = "\(outputDirectory)/\(policy).summary.md"
        let benchmarkArgs = benchmarkArgumentsPrefix + commonBenchmarkArgs + [
            "--scheduler-policy", policy,
            "--ca-raw-out", rawPath,
            "--ca-summary-out", summaryPath,
        ]

        print("  \(policy.uppercased()): /usr/bin/swift \(benchmarkArgs.joined(separator: " "))")
        let status = try runProcess(
            executable: "/usr/bin/swift",
            arguments: benchmarkArgs,
            currentDirectory: repoRoot
        )
        guard status == 0 else {
            throw ScriptError.benchmarkFailed(policy: policy, status: status)
        }

        let artifact = try decodeArtifact(at: rawPath)
        summariesByPolicy[policy] = RunSummary(
            policy: policy,
            artifact: artifact,
            rawPath: rawPath,
            summaryPath: summaryPath
        )
    }

    guard let fifo = summariesByPolicy["fifo"], let ca = summariesByPolicy["ca"] else {
        throw ScriptError.invalidOrder(spec.order.joined(separator: ","))
    }

    let comparePath = "\(outputDirectory)/compare.md"
    let compareBody = renderComparisonMarkdown(
        mediaFolder: mediaFolder,
        profile: spec.profile,
        modelMode: spec.modelMode,
        releaseBuild: releaseBuild,
        order: spec.order,
        outputDirectory: outputDirectory,
        benchmarkReportDirectory: benchmarkReportDirectory,
        summaries: summariesByPolicy
    )
    try compareBody.write(toFile: comparePath, atomically: true, encoding: .utf8)

    let fifoSnapshot = policySnapshot(from: fifo)
    let caSnapshot = policySnapshot(from: ca)
    let record = PairRunRecord(
        profile: spec.profile,
        modelMode: spec.modelMode,
        order: spec.order,
        fifo: fifoSnapshot,
        ca: caSnapshot,
        comparison: comparisonSnapshot(fifo: fifoSnapshot, ca: caSnapshot),
        artifacts: RetainedArtifactsSnapshot(
            outputDirectory: outputDirectory,
            benchmarkReportDirectory: benchmarkReportDirectory,
            comparePath: comparePath,
            fifo: PolicyArtifactsSnapshot(rawPath: fifo.rawPath, summaryPath: fifo.summaryPath),
            ca: PolicyArtifactsSnapshot(rawPath: ca.rawPath, summaryPath: ca.summaryPath)
        )
    )

    return PairExecutionResult(record: record, outputDirectory: outputDirectory)
}

let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
let scriptURL = URL(fileURLWithPath: CommandLine.arguments[0], relativeTo: cwd).standardizedFileURL
installCleanupSignalHandlers()

do {
    guard let repoRoot = locateRepositoryRoot(startingAt: scriptURL.deletingLastPathComponent()) else {
        throw ScriptError.cannotLocateRepositoryRoot
    }

    let config = try parseArguments(Array(CommandLine.arguments.dropFirst()))
    let mediaFolder = resolvePath(config.mediaFolder ?? "", relativeTo: cwd)

    let timestampFormatter = DateFormatter()
    timestampFormatter.locale = Locale(identifier: "en_US_POSIX")
    timestampFormatter.dateFormat = "yyyy-MM-dd_HHmmss"
    let timestamp = timestampFormatter.string(from: Date())

    let defaultOutputName = config.matrixMode ? "bench-results/codex-ca-matrix-\(timestamp)" : "bench-results/codex-ca-pair-\(timestamp)"
    let outputDirectory = config.outputDirectory.map { resolvePath($0, relativeTo: cwd) }
        ?? repoRoot.appendingPathComponent(defaultOutputName).path
    try FileManager.default.createDirectory(atPath: outputDirectory, withIntermediateDirectories: true)

    if config.matrixMode {
        let profiles = resolvedProfiles(for: config)
        let modelModes = resolvedModelModes(for: config)
        let orders = resolvedOrders(for: config)
        let runsRoot = "\(outputDirectory)/\(config.keepRaw ? "runs" : ".matrix-runs")"
        try FileManager.default.createDirectory(atPath: runsRoot, withIntermediateDirectories: true)
        if !config.keepRaw {
            registerCleanupPath(runsRoot)
            registerCleanupPath(outputDirectory)
        }

        print("Codex CA matrix")
        print("  media: \(mediaFolder)")
        print("  profiles: \(profiles.joined(separator: ", "))")
        print("  model modes: \(modelModes.joined(separator: ", "))")
        print("  orders: \(orders.map { $0.joined(separator: " -> ") }.joined(separator: " | "))")
        print("  build: \(config.releaseBuild ? "release" : "debug")")
        print("  output: \(outputDirectory)")
        print("  keep raw: \(config.keepRaw ? "yes" : "no")")
        print("")

        var runRecords: [PairRunRecord] = []
        for modelMode in modelModes {
            for profile in profiles {
                for order in orders {
                    let leafOutput = "\(runsRoot)/\(modelMode)/\(profile)/\(orderLabel(order))"
                    let result = try runPair(
                        spec: PairSpec(profile: profile, modelMode: modelMode, order: order),
                        mediaFolder: mediaFolder,
                        outputDirectory: leafOutput,
                        repoRoot: repoRoot,
                        releaseBuild: config.releaseBuild,
                        videoPreset: config.videoPreset,
                        videoTimeout: config.videoTimeout,
                        benchmarkPassthrough: config.benchmarkPassthrough
                    )

                    let record: PairRunRecord
                    if config.keepRaw {
                        record = result.record
                    } else {
                        record = PairRunRecord(
                            profile: result.record.profile,
                            modelMode: result.record.modelMode,
                            order: result.record.order,
                            fifo: result.record.fifo,
                            ca: result.record.ca,
                            comparison: result.record.comparison,
                            artifacts: nil
                        )
                        try? FileManager.default.removeItem(atPath: result.outputDirectory)
                    }

                    runRecords.append(record)
                    printPairComparison(record)
                    print("")
                }
            }
        }

        if !config.keepRaw {
            try? FileManager.default.removeItem(atPath: runsRoot)
            unregisterCleanupPath(runsRoot)
        }

        let rollups = buildRollups(from: runRecords)
        let overallPass = !rollups.isEmpty && rollups.allSatisfy(\.comparison.pass)
        let generatedAt = ISO8601DateFormatter().string(from: Date())
        let summaryPath = config.summaryOutput.map { appendTimestamp(timestamp, to: resolvePath($0, relativeTo: cwd)) }
            ?? "\(outputDirectory)/matrix-summary.json"
        let summaryMarkdownPath: String = {
            if summaryPath.hasSuffix(".json") {
                return String(summaryPath.dropLast(5)) + "-summary.md"
            }
            return summaryPath + "-summary.md"
        }()

        let summary = MatrixSummaryArtifact(
            generatedAt: generatedAt,
            mediaFolder: mediaFolder,
            build: config.releaseBuild ? "release" : "debug",
            outputDirectory: outputDirectory,
            keepRaw: config.keepRaw,
            failOnRegression: config.failOnRegression,
            profiles: profiles,
            modelModes: modelModes,
            orders: orders,
            runs: runRecords,
            rollups: rollups,
            overallPass: overallPass
        )
        try encodeJSON(summary, to: summaryPath)
        let markdownBody = renderMatrixSummaryMarkdown(summary, jsonPath: summaryPath)
        try markdownBody.write(toFile: summaryMarkdownPath, atomically: true, encoding: .utf8)

        if !config.keepRaw {
            let summaryInsideOutput = summaryPath.hasPrefix(outputDirectory + "/") || summaryPath == outputDirectory
            let markdownInsideOutput = summaryMarkdownPath.hasPrefix(outputDirectory + "/") || summaryMarkdownPath == outputDirectory
            if !(summaryInsideOutput || markdownInsideOutput) {
                removeDirectoryIfEffectivelyEmpty(outputDirectory)
                if !FileManager.default.fileExists(atPath: outputDirectory) {
                    unregisterCleanupPath(outputDirectory)
                }
            }
        }

        print("Rollups")
        for rollup in rollups {
            print("  [\(rollup.modelMode) | \(rollup.profile)] makespan \(formatSignedScalar(rollup.comparison.makespanSecondsDelta))  sumW \(formatSignedScalar(rollup.comparison.sumWSecondsDelta))  p95 \(formatSignedScalar(rollup.comparison.p95SecondsDelta))  failed \(formatSignedScalar(rollup.comparison.failedCountDelta))  pass \(rollup.comparison.pass ? "yes" : "no")")
        }
        print("")
        print("Summary")
        print("  json: \(summaryPath)")
        print("  markdown: \(summaryMarkdownPath)")
        print("  overall pass: \(overallPass ? "yes" : "no")")

        if config.failOnRegression && !overallPass {
            throw ScriptError.regressionDetected("matrix summary did not pass the CA comparison gate")
        }
    } else {
        let spec = PairSpec(
            profile: config.arrivalProfile,
            modelMode: config.modelMode,
            order: config.order
        )
        let result = try runPair(
            spec: spec,
            mediaFolder: mediaFolder,
            outputDirectory: outputDirectory,
            repoRoot: repoRoot,
            releaseBuild: config.releaseBuild,
            videoPreset: config.videoPreset,
            videoTimeout: config.videoTimeout,
            benchmarkPassthrough: config.benchmarkPassthrough
        )

        let record = result.record
        print("")
        printPairComparison(record)
        print("")
        print("Artifacts")
        if let artifacts = record.artifacts {
            print("  compare: \(artifacts.comparePath)")
            print("  fifo raw: \(artifacts.fifo.rawPath)")
            print("  ca raw: \(artifacts.ca.rawPath)")
        }

        if config.failOnRegression && !record.comparison.pass {
            throw ScriptError.regressionDetected("pair run did not pass the CA comparison gate")
        }
    }
} catch {
    fputs("Error: \(error.localizedDescription)\n", stderr)
    exit(1)
}
