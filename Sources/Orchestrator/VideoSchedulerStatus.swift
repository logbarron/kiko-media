import Foundation
import KikoMediaCore

struct VideoSchedulerPolicyResolution: Sendable, Equatable {
    let policy: VideoSchedulerPolicy
    let source: String
}

enum OperatorVideoRuntime: String, Sendable, Equatable {
    case complexityAwareOffload = "ca · offload"
    case fifoOffload = "fifo · offload"
    case fifoLocalOnly = "fifo · local-only"
}

enum OperatorVideoRuntimeReason: String, Sendable, Equatable {
    case forcedByPolicy = "forced by policy"
    case noWorkers = "no workers"
    case noBridge = "no bridge"
    case missingPrior = "missing prior"
    case invalidPrior = "invalid prior"
    case invalidLocalPrior = "invalid local prior"
    case tickV2Required = "tick v2 required"
    case complexityAwareReady = "ca ready"
}

struct OperatorVideoRuntimeSummary: Sendable, Equatable {
    let policy: VideoSchedulerPolicy
    let runtime: OperatorVideoRuntime
    let reason: OperatorVideoRuntimeReason
}

func resolveVideoSchedulerPolicy(
    processEnv: [String: String],
    persistedEnv: [String: String]
) -> VideoSchedulerPolicyResolution {
    let defaultPolicy = VideoSchedulerPolicy(
        trimmedRawValue: Config.stringDefaults["VIDEO_SCHEDULER_POLICY"] ?? VideoSchedulerPolicy.auto.rawValue
    ) ?? .auto
    let defaultSource = "default \(defaultPolicy.rawValue)"

    var resolvedPolicy = defaultPolicy
    var resolvedSource = defaultSource

    if let raw = trimmedSetting("VIDEO_SCHEDULER_POLICY", from: processEnv) {
        if let policy = VideoSchedulerPolicy(trimmedRawValue: raw) {
            resolvedPolicy = policy
            resolvedSource = "environment"
        } else {
            printWarning(
                "Ignoring invalid VIDEO_SCHEDULER_POLICY from environment: \(raw). Using persisted or default value."
            )
        }
    }

    if resolvedSource == defaultSource,
       let raw = trimmedSetting("VIDEO_SCHEDULER_POLICY", from: persistedEnv) {
        if let policy = VideoSchedulerPolicy(trimmedRawValue: raw) {
            resolvedPolicy = policy
            resolvedSource = "com.kiko.media.plist"
        } else {
            printWarning(
                "Ignoring invalid VIDEO_SCHEDULER_POLICY from com.kiko.media.plist: \(raw). Using default value."
            )
        }
    }

    return VideoSchedulerPolicyResolution(policy: resolvedPolicy, source: resolvedSource)
}

func summarizeVideoSchedulerRuntime(
    policy: VideoSchedulerPolicy,
    workersPresent: Bool,
    bridgeAvailable: Bool,
    activationDecision: CAActivationGate.Decision? = nil
) -> OperatorVideoRuntimeSummary {
    switch policy {
    case .none:
        return OperatorVideoRuntimeSummary(
            policy: policy,
            runtime: .fifoLocalOnly,
            reason: .forcedByPolicy
        )
    case .fifo, .auto:
        break
    }

    guard workersPresent else {
        return OperatorVideoRuntimeSummary(
            policy: policy,
            runtime: .fifoLocalOnly,
            reason: .noWorkers
        )
    }

    guard bridgeAvailable else {
        return OperatorVideoRuntimeSummary(
            policy: policy,
            runtime: .fifoLocalOnly,
            reason: .noBridge
        )
    }

    if policy == .fifo {
        return OperatorVideoRuntimeSummary(
            policy: policy,
            runtime: .fifoOffload,
            reason: .forcedByPolicy
        )
    }

    switch activationDecision {
    case .enabled:
        return OperatorVideoRuntimeSummary(
            policy: policy,
            runtime: .complexityAwareOffload,
            reason: .complexityAwareReady
        )
    case .disabled(let failure):
        return OperatorVideoRuntimeSummary(
            policy: policy,
            runtime: .fifoOffload,
            reason: operatorRuntimeReason(for: failure)
        )
    case nil:
        return OperatorVideoRuntimeSummary(
            policy: policy,
            runtime: .fifoOffload,
            reason: .missingPrior
        )
    }
}

func resolveOperatorVideoRuntimeSummary(
    processEnv: [String: String],
    persistedEnv: [String: String],
    workers: [ThunderboltStatusWorker],
    port: Int,
    connectTimeoutMS: Int
) -> (policyResolution: VideoSchedulerPolicyResolution, summary: OperatorVideoRuntimeSummary) {
    let policyResolution = resolveVideoSchedulerPolicy(
        processEnv: processEnv,
        persistedEnv: persistedEnv
    )

    let bridgeSources = ThunderboltDispatcher.discoverBridgeSources()
    let workersPresent = !workers.isEmpty
    let bridgeAvailable = !bridgeSources.isEmpty

    if policyResolution.policy == .none || !workersPresent || !bridgeAvailable {
        return (
            policyResolution,
            summarizeVideoSchedulerRuntime(
                policy: policyResolution.policy,
                workersPresent: workersPresent,
                bridgeAvailable: bridgeAvailable
            )
        )
    }

    let baseDirectory = expandTildePath(
        resolveSetting(
            "BASE_DIRECTORY",
            processEnv: processEnv,
            persistedEnv: persistedEnv,
            fallback: "~/Documents/kiko-media"
        )
    )
    let transcodePreset = resolveSetting(
        "VIDEO_TRANSCODE_PRESET",
        processEnv: processEnv,
        persistedEnv: persistedEnv,
        fallback: Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? ""
    )
    let activationDecision = resolveAutoActivationDecision(
        baseDirectory: baseDirectory,
        videoTranscodePreset: transcodePreset,
        workers: workers,
        port: port,
        connectTimeoutMS: connectTimeoutMS,
        bridgeSources: bridgeSources
    )

    return (
        policyResolution,
        summarizeVideoSchedulerRuntime(
            policy: policyResolution.policy,
            workersPresent: workersPresent,
            bridgeAvailable: bridgeAvailable,
            activationDecision: activationDecision
        )
    )
}

private func resolveAutoActivationDecision(
    baseDirectory: String,
    videoTranscodePreset: String,
    workers: [ThunderboltStatusWorker],
    port: Int,
    connectTimeoutMS: Int,
    bridgeSources: [ThunderboltDispatcher.BridgeSource]
) -> CAActivationGate.Decision {
    let priorPath = Config.benchmarkPriorPath(baseDirectoryPath: baseDirectory)
    let priorLoadResult = BenchmarkPriorArtifact.loadResult(fromPath: priorPath)
    let priorState = operatorPriorArtifactState(from: priorLoadResult)
    let priorTable: BenchmarkPriorTable? = {
        if case .loaded(let artifact) = priorLoadResult {
            return BenchmarkPriorTable(artifact: artifact)
        }
        return nil
    }()
    let localPriorProfile = CAActivationGate.resolveLocalPriorProfile(
        priorTable: priorTable,
        videoTranscodePreset: videoTranscodePreset
    )

    var activationDecision = CAActivationGate.evaluate(
        workersPresent: true,
        priorArtifactState: priorState,
        localPriorProfile: localPriorProfile
    )

    guard activationDecision.isEnabled else { return activationDecision }

    let endpoints = workers.map { worker in
        ThunderboltWorkerProbeTarget(
            host: worker.host,
            port: port,
            sourceIP: ThunderboltDispatcher.sourceIPForWorkerHost(worker.host, bridgeSources: bridgeSources)
        )
    }
    let capabilities = ThunderboltWorkerProbe.queryCapabilities(
        endpoints: endpoints,
        timeoutMS: connectTimeoutMS
    )
    let missingTickV2 = zip(workers, capabilities).compactMap { worker, caps in
        guard let caps,
              TickProtocolGate.isAccepted(
                version: caps.tickVersion,
                complexityAwareSchedulingEnabled: true
              ) else {
            return worker.host
        }
        return nil
    }

    if !missingTickV2.isEmpty {
        activationDecision = .disabled(.strictTickV2Rejected)
    }

    return activationDecision
}

private func operatorPriorArtifactState(
    from loadResult: BenchmarkPriorArtifact.LoadResult
) -> CAActivationGate.PriorArtifactState {
    switch loadResult {
    case .missing:
        return .missing
    case .invalid, .unsupportedVersion:
        return .invalid
    case .loaded:
        return .loaded
    }
}

private func operatorRuntimeReason(
    for failure: CAActivationGate.Failure
) -> OperatorVideoRuntimeReason {
    switch failure {
    case .noWorkers:
        return .noWorkers
    case .missingPriorArtifact:
        return .missingPrior
    case .invalidPriorArtifact:
        return .invalidPrior
    case .invalidLocalPrior:
        return .invalidLocalPrior
    case .strictTickV2Rejected:
        return .tickV2Required
    }
}

private func resolveSetting(
    _ key: String,
    processEnv: [String: String],
    persistedEnv: [String: String],
    fallback: String
) -> String {
    if let value = trimmedSetting(key, from: processEnv) {
        return value
    }
    if let value = trimmedSetting(key, from: persistedEnv) {
        return value
    }
    return fallback
}

private func trimmedSetting(
    _ key: String,
    from environment: [String: String]
) -> String? {
    guard let raw = environment[key]?.trimmingCharacters(in: .whitespacesAndNewlines),
          !raw.isEmpty else {
        return nil
    }
    return raw
}
