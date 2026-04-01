import Foundation
import KikoMediaCore

private func sharedTopologyModelRemoteWorkerInputs(
    reachableWorkers: [ThunderboltBoundWorkerSpec],
    workerCaps: [WorkerCaps?],
    port: Int,
    remoteLiveMSPerFrameC1ByHost: [String: Double],
    remoteTxInEstimateMSByHost: [String: Double],
    remoteTxOutEstimateMSByHost: [String: Double],
    remotePublishOverheadEstimateMSByHost: [String: Double]
) -> [CATopologyModelRemoteWorkerInput] {
    var capsByHost: [String: WorkerCaps?] = [:]
    capsByHost.reserveCapacity(reachableWorkers.count)
    for (worker, caps) in zip(reachableWorkers, workerCaps) {
        capsByHost[worker.host] = caps
    }

    return reachableWorkers.map { worker in
        let caps = capsByHost[worker.host] ?? nil
        return CATopologyModelRemoteWorkerInput(
            host: worker.host,
            port: port,
            workerSignature: caps?.workerSignature,
            caps: caps,
            liveMSPerFrameC1: remoteLiveMSPerFrameC1ByHost[worker.host],
            transferOverheadEstimateMS: remoteTxInEstimateMSByHost[worker.host],
            txOutEstimateMS: remoteTxOutEstimateMSByHost[worker.host],
            publishOverheadEstimateMS: remotePublishOverheadEstimateMSByHost[worker.host],
            slots: (1...max(0, worker.slots)).map { slotIndex in
                CATopologyModelRemoteSlotInput(
                    slotIndex: slotIndex,
                    isExecutable: true
                )
            }
        )
    }
}

private func resolveSharedThunderboltCARemoteModelDecision(
    host: String,
    port: Int,
    mode: ThunderboltCAModelMode,
    caps: WorkerCaps?,
    priorTable: BenchmarkPriorTable,
    remoteTxInEstimateMS: Double? = nil,
    remoteTxOutEstimateMS: Double? = nil,
    remotePublishOverheadEstimateMS: Double? = nil,
    localMSPerFrameC1: Double,
    localFixedOverheadMS: Double = 0,
    localCurve: [CADegradationPoint]
) -> CARemoteModelDecision {
    CARemoteModelDecisionKernel.resolve(
        host: host,
        port: port,
        mode: mode.sharedRemoteDecisionMode,
        workerSignature: caps?.workerSignature,
        caps: caps,
        priorTable: priorTable,
        remoteTxInEstimateMS: remoteTxInEstimateMS,
        remoteTxOutEstimateMS: remoteTxOutEstimateMS,
        remotePublishOverheadEstimateMS: remotePublishOverheadEstimateMS,
        localMSPerFrameC1: localMSPerFrameC1,
        localFixedOverheadMS: localFixedOverheadMS,
        localCurve: localCurve,
        liveMSPerFrameC1: nil
    )
}

func resolveThunderboltCARemoteModelDecision(
    host: String,
    port: Int,
    mode: ThunderboltCAModelMode,
    caps: WorkerCaps?,
    priorTable: BenchmarkPriorTable,
    remoteTxInEstimateMS: Double? = nil,
    remoteTxOutEstimateMS: Double? = nil,
    remotePublishOverheadEstimateMS: Double? = nil,
    localMSPerFrameC1: Double,
    localFixedOverheadMS: Double = 0,
    localCurve: [CADegradationPoint]
) -> ThunderboltCARemoteModelDecision {
    let sharedDecision = resolveSharedThunderboltCARemoteModelDecision(
        host: host,
        port: port,
        mode: mode,
        caps: caps,
        priorTable: priorTable,
        remoteTxInEstimateMS: remoteTxInEstimateMS,
        remoteTxOutEstimateMS: remoteTxOutEstimateMS,
        remotePublishOverheadEstimateMS: remotePublishOverheadEstimateMS,
        localMSPerFrameC1: localMSPerFrameC1,
        localFixedOverheadMS: localFixedOverheadMS,
        localCurve: localCurve
    )
    return ThunderboltCARemoteModelDecision(sharedDecision: sharedDecision)
}

func buildThunderboltCAModelProfiles(
    mode: ThunderboltCAModelMode,
    port: Int,
    slots: [ThunderboltCASlot],
    reachableWorkers: [ThunderboltBoundWorkerSpec],
    workerCaps: [WorkerCaps?],
    priorTable: BenchmarkPriorTable,
    remoteLiveMSPerFrameC1ByHost: [String: Double] = [:],
    remoteTxInEstimateMSByHost: [String: Double],
    remoteTxOutEstimateMSByHost: [String: Double] = [:],
    remotePublishOverheadEstimateMSByHost: [String: Double] = [:],
    localMSPerFrameC1: Double,
    localFixedOverheadMS: Double = 0,
    localMSSource: String,
    localCurve: [CADegradationPoint],
    localCurveSource: String
) -> ThunderboltCAModelBuildResult {
    let sharedBuildResult = CATopologyModelBuilder.build(
        mode: mode.sharedRemoteDecisionMode,
        localSlotCount: slots.reduce(into: 0) { partial, slot in
            if case .local = slot {
                partial += 1
            }
        },
        localProfile: CATopologyModelLocalProfile(
            machineID: "local",
            msPerFrameC1: localMSPerFrameC1,
            fixedOverheadMS: localFixedOverheadMS,
            degradationCurve: localCurve,
            msSource: localMSSource,
            curveSource: localCurveSource
        ),
        priorTable: priorTable,
        remoteWorkers: sharedTopologyModelRemoteWorkerInputs(
            reachableWorkers: reachableWorkers,
            workerCaps: workerCaps,
            port: port,
            remoteLiveMSPerFrameC1ByHost: remoteLiveMSPerFrameC1ByHost,
            remoteTxInEstimateMSByHost: remoteTxInEstimateMSByHost,
            remoteTxOutEstimateMSByHost: remoteTxOutEstimateMSByHost,
            remotePublishOverheadEstimateMSByHost: remotePublishOverheadEstimateMSByHost
        )
    )
    return ThunderboltCAModelBuildResult(sharedBuildResult: sharedBuildResult)
}
