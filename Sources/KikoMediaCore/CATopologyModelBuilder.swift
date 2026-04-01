import Foundation

package struct CATopologyModelRemoteSlotInput: Sendable {
    package let slotIndex: Int
    package let isExecutable: Bool

    package init(slotIndex: Int, isExecutable: Bool) {
        self.slotIndex = slotIndex
        self.isExecutable = isExecutable
    }
}

package struct CATopologyModelRemoteWorkerInput: Sendable {
    package let host: String
    package let port: Int
    package let workerSignature: String?
    package let caps: WorkerCaps?
    package let liveMSPerFrameC1: Double?
    package let transferOverheadEstimateMS: Double?
    package let txOutEstimateMS: Double?
    package let publishOverheadEstimateMS: Double?
    package let slots: [CATopologyModelRemoteSlotInput]

    package init(
        host: String,
        port: Int,
        workerSignature: String? = nil,
        caps: WorkerCaps? = nil,
        liveMSPerFrameC1: Double? = nil,
        transferOverheadEstimateMS: Double? = nil,
        txOutEstimateMS: Double? = nil,
        publishOverheadEstimateMS: Double? = nil,
        slots: [CATopologyModelRemoteSlotInput]
    ) {
        self.host = host
        self.port = port
        self.workerSignature = workerSignature
        self.caps = caps
        self.liveMSPerFrameC1 = liveMSPerFrameC1
        self.transferOverheadEstimateMS = transferOverheadEstimateMS
        self.txOutEstimateMS = txOutEstimateMS
        self.publishOverheadEstimateMS = publishOverheadEstimateMS
        self.slots = slots
    }
}

extension ThunderboltDispatcher.CASlotSnapshot {
    package var sharedTopologyModelRemoteSlotInput: CATopologyModelRemoteSlotInput {
        CATopologyModelRemoteSlotInput(
            slotIndex: slotIndex + 1,
            isExecutable: !isDown
        )
    }
}

extension ThunderboltDispatcher.CAWorkerSnapshot {
    package var sharedTopologyModelRemoteWorkerInput: CATopologyModelRemoteWorkerInput {
        CATopologyModelRemoteWorkerInput(
            host: host,
            port: port,
            workerSignature: workerSignature,
            caps: caps,
            liveMSPerFrameC1: liveMSPerFrameC1,
            transferOverheadEstimateMS: transferOverheadEstimateMS,
            txOutEstimateMS: txOutEstimateMS,
            publishOverheadEstimateMS: publishOverheadEstimateMS,
            slots: slots.map(\.sharedTopologyModelRemoteSlotInput)
        )
    }
}

package enum CATopologyModelBuilder {
    package static func build(
        mode: CARemoteModelDecisionMode,
        localSlotCount: Int,
        localProfile: CATopologyModelLocalProfile,
        priorTable: BenchmarkPriorTable,
        remoteWorkers: [CATopologyModelRemoteWorkerInput]
    ) -> CATopologyModelBuildResult {
        let sanitizedLocalSlotCount = max(1, localSlotCount)
        let localSlots = (1...sanitizedLocalSlotCount).map { CATopologyModelSlot.local(index: $0) }

        var decisionsByHost: [String: CARemoteModelDecision] = [:]
        decisionsByHost.reserveCapacity(remoteWorkers.count)

        var topologyWorkers: [CATopologyModelWorker] = []
        topologyWorkers.reserveCapacity(remoteWorkers.count)

        var slots = localSlots
        slots.reserveCapacity(
            localSlots.count +
                remoteWorkers.reduce(0) { partial, worker in
                    partial + worker.slots.reduce(into: 0) { upSlots, slot in
                        if slot.isExecutable {
                            upSlots += 1
                        }
                    }
                }
        )

        for worker in remoteWorkers {
            let executableSlots = worker.slots.reduce(into: 0) { upSlots, slot in
                if slot.isExecutable {
                    upSlots += 1
                }
            }
            topologyWorkers.append(
                CATopologyModelWorker(
                    host: worker.host,
                    reachableSlots: worker.slots.count,
                    executableSlots: executableSlots
                )
            )
            for slot in worker.slots where slot.isExecutable {
                slots.append(.remote(host: worker.host, index: slot.slotIndex))
            }
            decisionsByHost[worker.host] = CARemoteModelDecisionKernel.resolve(
                host: worker.host,
                port: worker.port,
                mode: mode,
                workerSignature: worker.workerSignature,
                caps: worker.caps,
                priorTable: priorTable,
                remoteTxInEstimateMS: worker.transferOverheadEstimateMS,
                remoteTxOutEstimateMS: worker.txOutEstimateMS,
                remotePublishOverheadEstimateMS: worker.publishOverheadEstimateMS,
                localMSPerFrameC1: localProfile.msPerFrameC1,
                localFixedOverheadMS: localProfile.fixedOverheadMS,
                localCurve: localProfile.degradationCurve,
                liveMSPerFrameC1: worker.liveMSPerFrameC1
            )
        }

        return CATopologyModelAssembly.build(
            mode: mode,
            localProfile: localProfile,
            slots: slots,
            reachableWorkers: topologyWorkers,
            decisionsByHost: decisionsByHost
        )
    }
}
