import Foundation
import KikoMediaCore

typealias ThunderboltWorkerSpec = Config.ThunderboltWorker

struct ThunderboltBoundWorkerSpec: Hashable, Sendable {
    let host: String
    let connectHost: String
    let slots: Int
    let sourceIP: String
    let bridgeName: String
}

extension ThunderboltBoundWorkerSpec {
    var sharedTopologyModelWorker: CATopologyModelWorker {
        CATopologyModelWorker(host: host, slots: slots)
    }

    func sharedTailTelemetrySeedProbeEndpoint(
        port: Int,
        connectTimeout: Int
    ) -> ThunderboltTailTelemetrySeedProbeEndpoint {
        ThunderboltTailTelemetrySeedProbeEndpoint(
            id: host,
            host: connectHost,
            port: port,
            sourceIP: sourceIP,
            connectTimeoutMS: connectTimeout
        )
    }
}

struct ThunderboltWorkerBindingIssue: Sendable {
    let worker: ThunderboltWorkerSpec
    let reason: String
}

struct ThunderboltConnectivityResult: Sendable {
    let worker: ThunderboltBoundWorkerSpec
    let reachable: Bool
    let connectMillis: Double
}

struct ThunderboltSettingsResolution: Sendable {
    let workersRaw: String?
    let workersSource: String
    let port: Int
    let portSource: String
    let connectTimeout: Int
    let connectTimeoutSource: String
    let warnings: [String]
}

let benchmarkThunderboltDefaultPort = Config.intDefaults["TB_PORT"]!.fallback
let benchmarkThunderboltDefaultConnectTimeout = Config.intDefaults["TB_CONNECT_TIMEOUT"]!.fallback

struct ThunderboltRoundTripResult: Sendable {
    let success: Bool
    let sendSeconds: Double
    let processNanos: UInt64
    let receiveSeconds: Double
    let totalSeconds: Double
    let firstRunningLatencySecondsEstimate: Double?
    let txOutMS: Double?
    let publishOverheadMS: Double?
    let slotHealthDownOnFailure: Bool?

    init(
        success: Bool,
        sendSeconds: Double,
        processNanos: UInt64,
        receiveSeconds: Double,
        totalSeconds: Double,
        firstRunningLatencySecondsEstimate: Double? = nil,
        txOutMS: Double? = nil,
        publishOverheadMS: Double? = nil,
        slotHealthDownOnFailure: Bool? = nil
    ) {
        self.success = success
        self.sendSeconds = sendSeconds
        self.processNanos = processNanos
        self.receiveSeconds = receiveSeconds
        self.totalSeconds = totalSeconds
        self.firstRunningLatencySecondsEstimate = firstRunningLatencySecondsEstimate
        self.txOutMS = txOutMS
        self.publishOverheadMS = publishOverheadMS
        self.slotHealthDownOnFailure = slotHealthDownOnFailure
    }
}

extension ThunderboltRoundTripResult {
    var sharedRawRoundTripResult: ThunderboltRawRoundTripResult {
        ThunderboltRawRoundTripResult(
            success: success,
            sendSeconds: sendSeconds,
            processNanos: processNanos,
            receiveSeconds: receiveSeconds,
            totalSeconds: totalSeconds,
            firstRunningLatencySecondsEstimate: firstRunningLatencySecondsEstimate,
            txOutMS: txOutMS,
            publishOverheadMS: publishOverheadMS,
            slotHealthDownOnFailure: slotHealthDownOnFailure
        )
    }
}

struct ThunderboltCATailTelemetryEstimates: Sendable {
    let txOutMSByHost: [String: Double]
    let publishOverheadMSByHost: [String: Double]
}

extension CASchedulerPolicy {
    var sharedBenchmarkRuntimePolicy: CABenchmarkRuntimePolicy {
        switch self {
        case .fifo:
            .fifo
        case .complexityAware:
            .complexityAware
        }
    }
}

extension ThunderboltCATailTelemetryEstimates {
    init(sharedEstimates: ThunderboltTailTelemetrySeedEstimates) {
        self.init(
            txOutMSByHost: sharedEstimates.txOutMSByID,
            publishOverheadMSByHost: sharedEstimates.publishOverheadMSByID
        )
    }
}

extension MediaFile {
    var sharedTailTelemetrySeedSampleCandidate: ThunderboltTailTelemetrySeedSampleCandidate {
        ThunderboltTailTelemetrySeedSampleCandidate(
            path: path,
            fileSize: sizeBytes,
            originalName: name
        )
    }
}

struct ThunderboltBurstConfig: Hashable, Sendable {
    let localSlots: Int
    let remoteSlots: [Int]
}

struct ThunderboltBurstResult: Sendable {
    let wallSeconds: Double
    let completed: Int
    let failed: Int
    let completionSeconds: [Double]
}

struct ThunderboltBurstJob: Sendable {
    let completedAt: Double
    let success: Bool
}

struct ThunderboltCAJobRecord: Sendable, Encodable {
    let jobId: String
    let videoName: String
    let slotLabel: String
    let actualExecutor: String
    let processNanos: UInt64
    let frameCount: Double
    let arrivalAtSeconds: Double
    let completedAtSeconds: Double?
    let success: Bool
}

struct ThunderboltCAMetricsJSON: Sendable, Encodable {
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

struct ThunderboltCARunResult: Sendable, Encodable {
    let schedulerPolicy: String
    let arrivalProfile: String
    let totalJobs: Int
    let successfulJobs: Int
    let failedCount: Int
    let metrics: ThunderboltCAMetricsJSON
    let jobs: [ThunderboltCAJobRecord]
    let observability: ThunderboltCAObservability?

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

struct ThunderboltCAAcceptanceProfileResult: Sendable, Encodable {
    let profile: String
    let fifo: ThunderboltCAMetricsJSON
    let ca: ThunderboltCAMetricsJSON
    let pass: Bool
    let sumWImproved: Bool
    let failedCountNonRegression: Bool
    let p95Regressed: Bool
    let makespanRegressed: Bool
}

struct ThunderboltCAMachineProfile: Sendable {
    let id: String
    let msPerFrameC1: Double
    let fixedOverheadMS: Double
    let degradationCurve: [CADegradationPoint]
    let txInMS: Double
    let txOutMS: Double
    let publishOverheadMS: Double
    let modeledConcurrencyCap: Int?

    init(
        id: String,
        msPerFrameC1: Double,
        fixedOverheadMS: Double = 0,
        degradationCurve: [CADegradationPoint],
        txInMS: Double,
        txOutMS: Double = 0,
        publishOverheadMS: Double = 0,
        modeledConcurrencyCap: Int? = nil
    ) {
        self.id = id
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.degradationCurve = degradationCurve
        self.txInMS = txInMS
        self.txOutMS = txOutMS
        self.publishOverheadMS = publishOverheadMS
        self.modeledConcurrencyCap = modeledConcurrencyCap
    }
}

extension ThunderboltCAMachineProfile {
    var sharedBenchmarkRuntimeMachineProfile: CABenchmarkRuntimeMachineProfile {
        CABenchmarkRuntimeMachineProfile(
            id: id,
            msPerFrameC1: msPerFrameC1,
            fixedOverheadMS: fixedOverheadMS,
            degradationCurve: degradationCurve,
            txInMS: txInMS,
            txOutMS: txOutMS,
            publishOverheadMS: publishOverheadMS,
            modeledConcurrencyCap: modeledConcurrencyCap
        )
    }

    init(sharedProfile: CATopologyModelMachineProfile) {
        self.init(
            id: sharedProfile.id,
            msPerFrameC1: sharedProfile.msPerFrameC1,
            fixedOverheadMS: sharedProfile.fixedOverheadMS,
            degradationCurve: sharedProfile.degradationCurve,
            txInMS: sharedProfile.txInMS,
            txOutMS: sharedProfile.txOutMS,
            publishOverheadMS: sharedProfile.publishOverheadMS,
            modeledConcurrencyCap: sharedProfile.modeledConcurrencyCap
        )
    }
}

struct ThunderboltCASlotBinding: Sendable {
    let machineIndex: Int
    let slotID: String
}

extension ThunderboltCASlotBinding {
    var sharedBenchmarkRuntimeSlotBinding: CABenchmarkRuntimeSlotBinding {
        CABenchmarkRuntimeSlotBinding(
            machineIndex: machineIndex,
            slotID: slotID
        )
    }

    init(sharedBinding: CATopologyModelSlotBinding) {
        self.init(
            machineIndex: sharedBinding.machineIndex,
            slotID: sharedBinding.slotID
        )
    }
}

enum ThunderboltCASlot: Sendable {
    case local(index: Int)
    case remote(worker: ThunderboltBoundWorkerSpec, index: Int)

    var label: String {
        switch self {
        case .local(let index):
            "local-\(index)"
        case .remote(let worker, let index):
            "\(worker.host)-\(index)"
        }
    }
}

struct ThunderboltCAAcceptanceReport: Sendable, Encodable {
    let generatedAt: String
    let corpusVideoCount: Int
    let corpusSignature: String
    let profiles: [ThunderboltCAAcceptanceProfileResult]
    let allPass: Bool

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case corpusVideoCount = "corpus_video_count"
        case corpusSignature = "corpus_signature"
        case profiles
        case allPass = "all_pass"
    }
}

struct ThunderboltCADispatchItem: Sendable {
    let index: Int
    let arrivalAtSeconds: Double
    let dispatchConcurrency: Int?
    let dispatchMachineIndex: Int
    let decisionAtSeconds: Double?
    let predictedSlotReadyMS: Double?
    let predictedStartMS: Double?
    let predictedDoneMS: Double?
    let waited: Bool
}

extension ThunderboltCADispatchItem {
    init(sharedItem: CABenchmarkRuntimeDispatchItem) {
        self.init(
            index: sharedItem.index,
            arrivalAtSeconds: sharedItem.arrivalAtSeconds,
            dispatchConcurrency: sharedItem.dispatchConcurrency,
            dispatchMachineIndex: sharedItem.dispatchMachineIndex,
            decisionAtSeconds: sharedItem.decisionAtSeconds,
            predictedSlotReadyMS: sharedItem.predictedSlotReadyMS,
            predictedStartMS: sharedItem.predictedStartMS,
            predictedDoneMS: sharedItem.predictedDoneMS,
            waited: sharedItem.waited
        )
    }
}

struct ThunderboltCAModelInputRow: Sendable, Encodable {
    let machineID: String
    let slotCount: Int
    let msPerFrameC1: Double
    let fixedOverheadMS: Double
    let msSource: String
    let curveSource: String
    let txInMS: Double
    let txOutMS: Double
    let publishOverheadMS: Double
    let confidenceTier: String?
    let confidenceMultiplier: Double
    let concurrencyCap: Int?

    init(
        machineID: String,
        slotCount: Int,
        msPerFrameC1: Double,
        fixedOverheadMS: Double = 0,
        msSource: String,
        curveSource: String,
        txInMS: Double,
        txOutMS: Double = 0,
        publishOverheadMS: Double = 0,
        confidenceTier: String? = nil,
        confidenceMultiplier: Double = 1.0,
        concurrencyCap: Int? = nil
    ) {
        self.machineID = machineID
        self.slotCount = slotCount
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.msSource = msSource
        self.curveSource = curveSource
        self.txInMS = txInMS
        self.txOutMS = txOutMS
        self.publishOverheadMS = publishOverheadMS
        self.confidenceTier = confidenceTier
        self.confidenceMultiplier = confidenceMultiplier
        self.concurrencyCap = concurrencyCap
    }

    enum CodingKeys: String, CodingKey {
        case machineID = "machine_id"
        case slotCount = "slot_count"
        case msPerFrameC1 = "ms_per_frame_c1"
        case fixedOverheadMS = "fixed_overhead_ms"
        case msSource = "ms_source"
        case curveSource = "curve_source"
        case txInMS = "tx_in_ms"
        case txOutMS = "tx_out_ms"
        case publishOverheadMS = "publish_overhead_ms"
        case confidenceTier = "confidence_tier"
        case confidenceMultiplier = "confidence_multiplier"
        case concurrencyCap = "concurrency_cap"
    }
}

extension ThunderboltCAModelInputRow {
    init(sharedRow: CATopologyModelInputRow) {
        self.init(
            machineID: sharedRow.machineID,
            slotCount: sharedRow.slotCount,
            msPerFrameC1: sharedRow.msPerFrameC1,
            fixedOverheadMS: sharedRow.fixedOverheadMS,
            msSource: sharedRow.msSource,
            curveSource: sharedRow.curveSource,
            txInMS: sharedRow.txInMS,
            txOutMS: sharedRow.txOutMS,
            publishOverheadMS: sharedRow.publishOverheadMS,
            confidenceTier: sharedRow.confidenceTier?.rawValue,
            confidenceMultiplier: sharedRow.confidenceMultiplier,
            concurrencyCap: sharedRow.concurrencyCap
        )
    }
}

struct ThunderboltCAAdaptationRow: Sendable, Encodable {
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

extension ThunderboltCAAdaptationRow {
    init(sharedRow: CABenchmarkRuntimeAdaptationRow) {
        self.init(
            machineID: sharedRow.machineID,
            completions: sharedRow.completions,
            initialMSPerFrameC1: sharedRow.initialMSPerFrameC1,
            finalMSPerFrameC1: sharedRow.finalMSPerFrameC1
        )
    }
}

struct ThunderboltCAPredictionSample: Sendable, Encodable {
    let machineID: String
    let decisionAtSeconds: Double
    let predictedSlotReadyMS: Double?
    let predictedStartMS: Double?
    let predictedDoneMS: Double?
    let actualStartMS: Double?
    let actualDoneMS: Double?
    let waited: Bool
    let success: Bool
    let executorMismatch: Bool

    enum CodingKeys: String, CodingKey {
        case machineID = "machine_id"
        case decisionAtSeconds = "decision_at_seconds"
        case predictedSlotReadyMS = "predicted_slot_ready_ms"
        case predictedStartMS = "predicted_start_ms"
        case predictedDoneMS = "predicted_done_ms"
        case actualStartMS = "actual_start_ms"
        case actualDoneMS = "actual_done_ms"
        case waited
        case success
        case executorMismatch = "executor_mismatch"
    }
}

struct ThunderboltCASolverTelemetryRow: Sendable, Encodable {
    let nodesVisited: Int
    let prunedByPickCount: Int
    let prunedByMakespan: Int
    let prunedByCompletionSum: Int
    let incumbentUpdates: Int
    let maxDepth: Int
    let solverWallMS: Double

    enum CodingKeys: String, CodingKey {
        case nodesVisited = "nodes_visited"
        case prunedByPickCount = "pruned_by_pick_count"
        case prunedByMakespan = "pruned_by_makespan"
        case prunedByCompletionSum = "pruned_by_completion_sum"
        case incumbentUpdates = "incumbent_updates"
        case maxDepth = "max_depth"
        case solverWallMS = "solver_wall_ms"
    }
}

struct ThunderboltCAObservability: Sendable, Encodable {
    let policy: CASchedulerPolicy
    let modelInputs: [ThunderboltCAModelInputRow]
    let adaptation: [ThunderboltCAAdaptationRow]
    let predictions: [ThunderboltCAPredictionSample]
    var solverTelemetry: [ThunderboltCASolverTelemetryRow] = []

    enum CodingKeys: String, CodingKey {
        case policy
        case modelInputs = "model_inputs"
        case adaptation
        case predictions
        case solverTelemetry = "solver_telemetry"
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(policy.rawValue, forKey: .policy)
        try container.encode(modelInputs, forKey: .modelInputs)
        try container.encode(adaptation, forKey: .adaptation)
        try container.encode(predictions, forKey: .predictions)
        try container.encode(solverTelemetry, forKey: .solverTelemetry)
    }
}

struct ThunderboltCAObservedRun: Sendable, Encodable {
    let result: ThunderboltCARunResult
    let observability: ThunderboltCAObservability
}

struct ThunderboltCASlotOverrides: Sendable {
    let localSlots: Int?
    let remoteSlotsByHost: [String: Int]
}

struct ThunderboltCARemoteModelDecision: Sendable {
    let host: String
    let machineID: String?
    let msPerFrameC1: Double?
    let fixedOverheadMS: Double?
    let degradationCurve: [CADegradationPoint]
    let txInMS: Double
    let txOutMS: Double
    let publishOverheadMS: Double
    let msSource: String
    let curveSource: String
    let exclusionReason: String?
    let fallbackActive: Bool
    let confidenceTier: String?
    let confidenceMultiplier: Double
    let concurrencyCap: Int?
}

extension ThunderboltCARemoteModelDecision {
    init(sharedDecision: CARemoteModelDecision) {
        self.init(
            host: sharedDecision.host,
            machineID: sharedDecision.machineID,
            msPerFrameC1: sharedDecision.msPerFrameC1,
            fixedOverheadMS: sharedDecision.fixedOverheadMS,
            degradationCurve: sharedDecision.degradationCurve,
            txInMS: sharedDecision.txInMS,
            txOutMS: sharedDecision.txOutMS,
            publishOverheadMS: sharedDecision.publishOverheadMS,
            msSource: sharedDecision.msSource,
            curveSource: sharedDecision.curveSource,
            exclusionReason: sharedDecision.exclusionReason,
            fallbackActive: sharedDecision.fallbackActive,
            confidenceTier: sharedDecision.confidenceTier?.rawValue,
            confidenceMultiplier: sharedDecision.confidenceMultiplier,
            concurrencyCap: sharedDecision.concurrencyCap
        )
    }
}

struct ThunderboltCARemoteCoverageRow: Sendable {
    let host: String
    let reachableSlots: Int
    let executableSlots: Int
    let modeledSlots: Int
    let msSource: String
    let curveSource: String
    let confidenceTier: String?
    let confidenceMultiplier: Double
    let concurrencyCap: Int?
    let note: String

    init(
        host: String,
        reachableSlots: Int,
        executableSlots: Int? = nil,
        modeledSlots: Int,
        msSource: String,
        curveSource: String,
        confidenceTier: String? = nil,
        confidenceMultiplier: Double = 1.0,
        concurrencyCap: Int? = nil,
        note: String
    ) {
        self.host = host
        self.reachableSlots = reachableSlots
        self.executableSlots = executableSlots ?? reachableSlots
        self.modeledSlots = modeledSlots
        self.msSource = msSource
        self.curveSource = curveSource
        self.confidenceTier = confidenceTier
        self.confidenceMultiplier = confidenceMultiplier
        self.concurrencyCap = concurrencyCap
        self.note = note
    }
}

extension ThunderboltCARemoteCoverageRow {
    init(sharedRow: CATopologyModelCoverageRow) {
        self.init(
            host: sharedRow.host,
            reachableSlots: sharedRow.reachableSlots,
            executableSlots: sharedRow.executableSlots,
            modeledSlots: sharedRow.modeledSlots,
            msSource: sharedRow.msSource,
            curveSource: sharedRow.curveSource,
            confidenceTier: sharedRow.confidenceTier?.rawValue,
            confidenceMultiplier: sharedRow.confidenceMultiplier,
            concurrencyCap: sharedRow.concurrencyCap,
            note: sharedRow.note
        )
    }
}

struct ThunderboltCAModelDiagnostics: Sendable {
    let mode: ThunderboltCAModelMode
    let coverageRows: [ThunderboltCARemoteCoverageRow]
    let strictExclusions: [String]
    let reachableWorkerCount: Int
    let reachableSlotCount: Int
    let modeledWorkerCount: Int
    let modeledSlotCount: Int
    let fallbackActive: Bool
    let localPriorGap: Bool
    let remotePriorGap: Bool
    let localExecutableSlotCount: Int
    let remoteExecutableSlotCount: Int
    let totalExecutableSlotCount: Int
    let exactPriorSlotCount: Int
    let hardwareCompatiblePriorSlotCount: Int
    let capabilityBackedSlotCount: Int
    let localFallbackSlotCount: Int

    init(
        mode: ThunderboltCAModelMode,
        coverageRows: [ThunderboltCARemoteCoverageRow],
        strictExclusions: [String],
        reachableWorkerCount: Int,
        reachableSlotCount: Int,
        modeledWorkerCount: Int,
        modeledSlotCount: Int,
        fallbackActive: Bool,
        localPriorGap: Bool,
        remotePriorGap: Bool,
        localExecutableSlotCount: Int? = nil,
        remoteExecutableSlotCount: Int? = nil,
        totalExecutableSlotCount: Int? = nil,
        exactPriorSlotCount: Int = 0,
        hardwareCompatiblePriorSlotCount: Int = 0,
        capabilityBackedSlotCount: Int = 0,
        localFallbackSlotCount: Int = 0
    ) {
        self.mode = mode
        self.coverageRows = coverageRows
        self.strictExclusions = strictExclusions
        self.reachableWorkerCount = reachableWorkerCount
        self.reachableSlotCount = reachableSlotCount
        self.modeledWorkerCount = modeledWorkerCount
        self.modeledSlotCount = modeledSlotCount
        self.fallbackActive = fallbackActive
        self.localPriorGap = localPriorGap
        self.remotePriorGap = remotePriorGap
        self.localExecutableSlotCount = localExecutableSlotCount ?? 0
        self.remoteExecutableSlotCount = remoteExecutableSlotCount ?? reachableSlotCount
        self.totalExecutableSlotCount = totalExecutableSlotCount ?? (self.localExecutableSlotCount + self.remoteExecutableSlotCount)
        self.exactPriorSlotCount = exactPriorSlotCount
        self.hardwareCompatiblePriorSlotCount = hardwareCompatiblePriorSlotCount
        self.capabilityBackedSlotCount = capabilityBackedSlotCount
        self.localFallbackSlotCount = localFallbackSlotCount
    }
}

extension ThunderboltCAModelDiagnostics {
    init(sharedDiagnostics: CATopologyModelDiagnostics) {
        let mode: ThunderboltCAModelMode = switch sharedDiagnostics.mode {
        case .strict:
            .strict
        case .auto:
            .auto
        }
        self.init(
            mode: mode,
            coverageRows: sharedDiagnostics.coverageRows.map { ThunderboltCARemoteCoverageRow(sharedRow: $0) },
            strictExclusions: sharedDiagnostics.strictExclusions,
            reachableWorkerCount: sharedDiagnostics.reachableWorkerCount,
            reachableSlotCount: sharedDiagnostics.reachableSlotCount,
            modeledWorkerCount: sharedDiagnostics.modeledWorkerCount,
            modeledSlotCount: sharedDiagnostics.modeledSlotCount,
            fallbackActive: sharedDiagnostics.fallbackActive,
            localPriorGap: sharedDiagnostics.localPriorGap,
            remotePriorGap: sharedDiagnostics.remotePriorGap,
            localExecutableSlotCount: sharedDiagnostics.localExecutableSlotCount,
            remoteExecutableSlotCount: sharedDiagnostics.remoteExecutableSlotCount,
            totalExecutableSlotCount: sharedDiagnostics.totalExecutableSlotCount,
            exactPriorSlotCount: sharedDiagnostics.exactPriorSlotCount,
            hardwareCompatiblePriorSlotCount: sharedDiagnostics.hardwareCompatiblePriorSlotCount,
            capabilityBackedSlotCount: sharedDiagnostics.capabilityBackedSlotCount,
            localFallbackSlotCount: sharedDiagnostics.localFallbackSlotCount
        )
    }
}

struct ThunderboltCARunSetup: Sendable {
    let port: Int
    let connectTimeout: Int
    let videos: [MediaFile]
    let videoCosts: [CAResolvedVideoCost]
    let priorTable: BenchmarkPriorTable
    let localSignature: String
    let localSlotCount: Int
    let localMSPerFrameC1: Double
    let localFixedOverheadMS: Double
    let sourceHashes: [String: String]
    let slots: [ThunderboltCASlot]
    let machineProfiles: [ThunderboltCAMachineProfile]
    let slotBindings: [ThunderboltCASlotBinding]
    let machineIndexByHost: [String: Int]
    let modelInputs: [ThunderboltCAModelInputRow]
    let diagnostics: ThunderboltCAModelDiagnostics
    let reachableWorkers: [ThunderboltBoundWorkerSpec]
    let workerCapsByHost: [String: WorkerCaps]

    init(
        port: Int,
        connectTimeout: Int,
        videos: [MediaFile],
        videoCosts: [CAResolvedVideoCost],
        priorTable: BenchmarkPriorTable,
        localSignature: String,
        localSlotCount: Int,
        localMSPerFrameC1: Double,
        localFixedOverheadMS: Double = 0,
        sourceHashes: [String: String],
        slots: [ThunderboltCASlot],
        machineProfiles: [ThunderboltCAMachineProfile],
        slotBindings: [ThunderboltCASlotBinding],
        machineIndexByHost: [String: Int],
        modelInputs: [ThunderboltCAModelInputRow],
        diagnostics: ThunderboltCAModelDiagnostics,
        reachableWorkers: [ThunderboltBoundWorkerSpec],
        workerCapsByHost: [String: WorkerCaps]
    ) {
        precondition(
            videoCosts.count == videos.count,
            "ThunderboltCARunSetup requires one carried video cost per video"
        )
        self.port = port
        self.connectTimeout = connectTimeout
        self.videos = videos
        self.videoCosts = videoCosts
        self.priorTable = priorTable
        self.localSignature = localSignature
        self.localSlotCount = localSlotCount
        self.localMSPerFrameC1 = localMSPerFrameC1
        self.localFixedOverheadMS = localFixedOverheadMS
        self.sourceHashes = sourceHashes
        self.slots = slots
        self.machineProfiles = machineProfiles
        self.slotBindings = slotBindings
        self.machineIndexByHost = machineIndexByHost
        self.modelInputs = modelInputs
        self.diagnostics = diagnostics
        self.reachableWorkers = reachableWorkers
        self.workerCapsByHost = workerCapsByHost
    }
}

enum ThunderboltShowdownPreflightClassification: String, Sendable {
    case healthy = "healthy"
    case localPriorGap = "local-prior-gap"
    case remotePriorGap = "remote-prior-gap"
    case localAndRemotePriorGap = "local-prior-gap+remote-prior-gap"
}

extension ThunderboltShowdownPreflightClassification {
    init(sharedClassification: BenchmarkPriorPreflightClassification) {
        switch sharedClassification {
        case .healthy:
            self = .healthy
        case .localPriorGap:
            self = .localPriorGap
        case .remotePriorGap:
            self = .remotePriorGap
        case .localAndRemotePriorGap:
            self = .localAndRemotePriorGap
        }
    }
}

struct ThunderboltShowdownModeledWorker: Sendable, Equatable {
    let signature: String
    let slots: Int
}

extension ThunderboltShowdownModeledWorker {
    var sharedPriorModeledWorker: BenchmarkPriorModeledWorker {
        BenchmarkPriorModeledWorker(signature: signature, slots: slots)
    }
}

enum ThunderboltPriorWriteOutcome {
    case skippedPolicyOff
    case skippedInsufficientSignal
    case skippedExistingCanonical
    case candidateWritten(String)
    case canonicalWritten(String)
    case promoted(String)
    case candidateRejected(String, String)
    case failed(any Error)
}

struct ThunderboltShowdownPriorPromotionDecision: Sendable, Equatable {
    let shouldPromote: Bool
    let reason: String
    let missingModeledSignatures: [String]
    let currentRemoteWorkerCoverage: Int
    let candidateRemoteWorkerCoverage: Int
    let currentRemoteSlotCoverage: Int
    let candidateRemoteSlotCoverage: Int
    let currentLocalPriorValid: Bool
    let candidateLocalPriorValid: Bool
    let localPriorValidityImproved: Bool
    let candidateCorpusAtLeastAsStrong: Bool
    let forceApplied: Bool
}

extension ThunderboltShowdownPriorPromotionDecision {
    init(sharedDecision: BenchmarkPriorPromotionDecision) {
        self.init(
            shouldPromote: sharedDecision.shouldPromote,
            reason: sharedDecision.reason,
            missingModeledSignatures: sharedDecision.missingModeledSignatures,
            currentRemoteWorkerCoverage: sharedDecision.currentRemoteWorkerCoverage,
            candidateRemoteWorkerCoverage: sharedDecision.candidateRemoteWorkerCoverage,
            currentRemoteSlotCoverage: sharedDecision.currentRemoteSlotCoverage,
            candidateRemoteSlotCoverage: sharedDecision.candidateRemoteSlotCoverage,
            currentLocalPriorValid: sharedDecision.currentLocalPriorValid,
            candidateLocalPriorValid: sharedDecision.candidateLocalPriorValid,
            localPriorValidityImproved: sharedDecision.localPriorValidityImproved,
            candidateCorpusAtLeastAsStrong: sharedDecision.candidateCorpusAtLeastAsStrong,
            forceApplied: sharedDecision.forceApplied
        )
    }
}

enum ThunderboltRemoteMaintenanceSampleInvalidationReason: String, Sendable, Equatable {
    case nonIsolatedProbe = "non-isolated-probe"
    case unsuccessfulRemoteProbe = "unsuccessful-remote-probe"
    case localFallback = "local-fallback"
    case executorMismatch = "executor-mismatch"
    case missingWorkerSignature = "missing-worker-signature"
    case invalidConcurrency = "invalid-concurrency"
    case missingProcessTime = "missing-process-time"
}

struct ThunderboltRemoteMaintenanceRepresentativeVideo: Sendable {
    let video: MediaFile
    let frameCount: Double
}

struct ThunderboltRemoteMaintenancePreparedSample: Sendable {
    let video: MediaFile
    let frameCount: Double
    let sha256: String
}

struct ThunderboltRemoteMaintenanceTelemetrySample: Sendable, Equatable {
    let host: String
    let workerSignature: String?
    let concurrency: Int
    let isolated: Bool
    let actualExecutor: String
    let processNanos: UInt64
    let txInMS: Double?
    let txOutMS: Double?
    let publishOverheadMS: Double?
    let videoPath: String
    let frameCount: Double
    let invalidationReason: ThunderboltRemoteMaintenanceSampleInvalidationReason?

    var validForPriorGeneration: Bool {
        guard invalidationReason == nil,
              isolated,
              concurrency > 0,
              processNanos > 0,
              let workerSignature,
              !workerSignature.isEmpty,
              actualExecutor == host else {
            return false
        }
        return true
    }

    var validForAffinePriorGeneration: Bool {
        validForPriorGeneration && !videoPath.isEmpty && frameCount > 0
    }
}

struct ThunderboltRemoteMaintenancePriorEligibility: Sendable, Equatable {
    let workerEligible: Bool
    let eligibleConcurrencies: Set<Int>
}

struct ThunderboltCAModelBuildResult: Sendable {
    let machineProfiles: [ThunderboltCAMachineProfile]
    let slotBindings: [ThunderboltCASlotBinding]
    let machineIndexByHost: [String: Int]
    let modelInputs: [ThunderboltCAModelInputRow]
    let diagnostics: ThunderboltCAModelDiagnostics
}

extension ThunderboltCAModelBuildResult {
    init(sharedBuildResult: CATopologyModelBuildResult) {
        self.init(
            machineProfiles: sharedBuildResult.machineProfiles.map { ThunderboltCAMachineProfile(sharedProfile: $0) },
            slotBindings: sharedBuildResult.slotBindings.map { ThunderboltCASlotBinding(sharedBinding: $0) },
            machineIndexByHost: sharedBuildResult.machineIndexByHost,
            modelInputs: sharedBuildResult.modelInputs.map { ThunderboltCAModelInputRow(sharedRow: $0) },
            diagnostics: ThunderboltCAModelDiagnostics(sharedDiagnostics: sharedBuildResult.diagnostics)
        )
    }
}

extension ThunderboltCASlot {
    var sharedTopologyModelSlot: CATopologyModelSlot {
        switch self {
        case .local(let index):
            .local(index: index)
        case .remote(let worker, let index):
            .remote(host: worker.host, index: index)
        }
    }
}
