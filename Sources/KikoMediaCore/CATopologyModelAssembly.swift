import Foundation

package struct CATopologyModelWorker: Sendable {
    package let host: String
    package let reachableSlots: Int
    package let executableSlots: Int

    package init(host: String, slots: Int) {
        self.init(host: host, reachableSlots: slots, executableSlots: slots)
    }

    package init(host: String, reachableSlots: Int, executableSlots: Int) {
        self.host = host
        self.reachableSlots = max(0, reachableSlots)
        self.executableSlots = max(0, min(reachableSlots, executableSlots))
    }
}

package enum CATopologyModelSlot: Sendable {
    case local(index: Int)
    case remote(host: String, index: Int)
}

package struct CATopologyModelLocalProfile: Sendable {
    package let machineID: String
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let degradationCurve: [CADegradationPoint]
    package let msSource: String
    package let curveSource: String

    package init(
        machineID: String,
        msPerFrameC1: Double,
        fixedOverheadMS: Double = 0,
        degradationCurve: [CADegradationPoint],
        msSource: String,
        curveSource: String
    ) {
        self.machineID = machineID
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.degradationCurve = degradationCurve
        self.msSource = msSource
        self.curveSource = curveSource
    }
}

package struct CATopologyModelMachineProfile: Sendable {
    package let id: String
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let degradationCurve: [CADegradationPoint]
    package let txInMS: Double
    package let txOutMS: Double
    package let publishOverheadMS: Double
    package let modeledConcurrencyCap: Int?
}

package struct CATopologyModelSlotBinding: Sendable {
    package let machineIndex: Int
    package let slotID: String
}

package struct CATopologyModelInputRow: Sendable {
    package let machineID: String
    package let slotCount: Int
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let msSource: String
    package let curveSource: String
    package let txInMS: Double
    package let txOutMS: Double
    package let publishOverheadMS: Double
    package let confidenceTier: CAMachineConfidenceTier?
    package let confidenceMultiplier: Double
    package let concurrencyCap: Int?

    package init(
        machineID: String,
        slotCount: Int,
        msPerFrameC1: Double,
        fixedOverheadMS: Double,
        msSource: String,
        curveSource: String,
        txInMS: Double,
        txOutMS: Double,
        publishOverheadMS: Double,
        confidenceTier: CAMachineConfidenceTier? = nil,
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
}

package struct CATopologyModelCoverageRow: Sendable {
    package let host: String
    package let reachableSlots: Int
    package let executableSlots: Int
    package let modeledSlots: Int
    package let msSource: String
    package let curveSource: String
    package let confidenceTier: CAMachineConfidenceTier?
    package let confidenceMultiplier: Double
    package let concurrencyCap: Int?
    package let note: String

    package init(
        host: String,
        reachableSlots: Int,
        executableSlots: Int,
        modeledSlots: Int,
        msSource: String,
        curveSource: String,
        confidenceTier: CAMachineConfidenceTier?,
        confidenceMultiplier: Double,
        concurrencyCap: Int?,
        note: String
    ) {
        self.host = host
        self.reachableSlots = reachableSlots
        self.executableSlots = executableSlots
        self.modeledSlots = modeledSlots
        self.msSource = msSource
        self.curveSource = curveSource
        self.confidenceTier = confidenceTier
        self.confidenceMultiplier = confidenceMultiplier
        self.concurrencyCap = concurrencyCap
        self.note = note
    }
}

package struct CATopologyModelDiagnostics: Sendable {
    package let mode: CARemoteModelDecisionMode
    package let coverageRows: [CATopologyModelCoverageRow]
    package let strictExclusions: [String]
    package let reachableWorkerCount: Int
    package let reachableSlotCount: Int
    package let modeledWorkerCount: Int
    package let modeledSlotCount: Int
    package let fallbackActive: Bool
    package let localPriorGap: Bool
    package let remotePriorGap: Bool
    package let localExecutableSlotCount: Int
    package let remoteExecutableSlotCount: Int
    package let totalExecutableSlotCount: Int
    package let exactPriorSlotCount: Int
    package let hardwareCompatiblePriorSlotCount: Int
    package let capabilityBackedSlotCount: Int
    package let localFallbackSlotCount: Int

    package init(
        mode: CARemoteModelDecisionMode,
        coverageRows: [CATopologyModelCoverageRow],
        strictExclusions: [String],
        reachableWorkerCount: Int,
        reachableSlotCount: Int,
        modeledWorkerCount: Int,
        modeledSlotCount: Int,
        fallbackActive: Bool,
        localPriorGap: Bool,
        remotePriorGap: Bool,
        localExecutableSlotCount: Int,
        remoteExecutableSlotCount: Int,
        totalExecutableSlotCount: Int,
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
        self.localExecutableSlotCount = localExecutableSlotCount
        self.remoteExecutableSlotCount = remoteExecutableSlotCount
        self.totalExecutableSlotCount = totalExecutableSlotCount
        self.exactPriorSlotCount = exactPriorSlotCount
        self.hardwareCompatiblePriorSlotCount = hardwareCompatiblePriorSlotCount
        self.capabilityBackedSlotCount = capabilityBackedSlotCount
        self.localFallbackSlotCount = localFallbackSlotCount
    }
}

package struct CATopologyModelBuildResult: Sendable {
    package let machineProfiles: [CATopologyModelMachineProfile]
    package let slotBindings: [CATopologyModelSlotBinding]
    package let machineIndexByHost: [String: Int]
    package let modelInputs: [CATopologyModelInputRow]
    package let diagnostics: CATopologyModelDiagnostics
}

package enum CATopologyModelAssembly {
    package static func build(
        mode: CARemoteModelDecisionMode,
        localProfile: CATopologyModelLocalProfile,
        slots: [CATopologyModelSlot],
        reachableWorkers: [CATopologyModelWorker],
        decisionsByHost: [String: CARemoteModelDecision]
    ) -> CATopologyModelBuildResult {
        var machineProfiles: [CATopologyModelMachineProfile] = [
            CATopologyModelMachineProfile(
                id: localProfile.machineID,
                msPerFrameC1: localProfile.msPerFrameC1,
                fixedOverheadMS: CAProfileAndFallbackMath.resolvedFixedOverheadMS(localProfile.fixedOverheadMS),
                degradationCurve: localProfile.degradationCurve,
                txInMS: 0,
                txOutMS: 0,
                publishOverheadMS: 0,
                modeledConcurrencyCap: nil
            )
        ]
        var machineMSSources: [String] = [localProfile.msSource]
        var machineCurveSources: [String] = [localProfile.curveSource]
        var machineConfidenceTiers: [CAMachineConfidenceTier?] = [nil]
        var machineConfidenceMultipliers: [Double] = [1.0]
        var machineConcurrencyCaps: [Int?] = [nil]
        var machineIndexByHost: [String: Int] = [:]
        var slotBindings: [CATopologyModelSlotBinding] = []
        slotBindings.reserveCapacity(slots.count)

        var coverageRows: [CATopologyModelCoverageRow] = []
        coverageRows.reserveCapacity(reachableWorkers.count)
        var strictExclusions: [String] = []
        strictExclusions.reserveCapacity(reachableWorkers.count)
        var fallbackActive = false

        var exactPriorSlotCount = 0
        var hardwareCompatiblePriorSlotCount = 0
        var capabilityBackedSlotCount = 0
        var localFallbackSlotCount = 0

        for worker in reachableWorkers {
            let decision = decisionsByHost[worker.host] ?? missingDecision(host: worker.host)
            let reachableSlots = worker.reachableSlots
            let executableSlots = worker.executableSlots
            let modeledSlots = decision.machineID == nil ? 0 : executableSlots
            let note: String = {
                if let reason = decision.exclusionReason {
                    return reason
                }
                guard let tier = decision.confidenceTier else {
                    return "unmodeled"
                }
                switch tier {
                case .exactPrior:
                    return "exact prior"
                case .hardwareCompatiblePrior:
                    return "hardware-compatible prior"
                case .capabilityBacked:
                    return "capability-backed"
                case .localFallback:
                    return "local fallback"
                }
            }()
            coverageRows.append(
                CATopologyModelCoverageRow(
                    host: worker.host,
                    reachableSlots: reachableSlots,
                    executableSlots: executableSlots,
                    modeledSlots: modeledSlots,
                    msSource: decision.msSource,
                    curveSource: decision.curveSource,
                    confidenceTier: decision.confidenceTier,
                    confidenceMultiplier: decision.confidenceMultiplier,
                    concurrencyCap: decision.concurrencyCap,
                    note: note
                )
            )

            if mode == .strict,
               executableSlots > 0,
               modeledSlots == 0,
               let reason = decision.exclusionReason {
                strictExclusions.append("\(worker.host): \(reason)")
            }
            if decision.fallbackActive, modeledSlots > 0 {
                fallbackActive = true
            }
            switch decision.confidenceTier {
            case .exactPrior?:
                exactPriorSlotCount += modeledSlots
            case .hardwareCompatiblePrior?:
                hardwareCompatiblePriorSlotCount += modeledSlots
            case .capabilityBacked?:
                capabilityBackedSlotCount += modeledSlots
            case .localFallback?:
                localFallbackSlotCount += modeledSlots
            case nil:
                break
            }
        }

        for slot in slots {
            switch slot {
            case .local(let index):
                slotBindings.append(
                    CATopologyModelSlotBinding(
                        machineIndex: 0,
                        slotID: "local#s\(index)"
                    )
                )
            case .remote(let host, let index):
                guard let decision = decisionsByHost[host],
                      let machineID = decision.machineID,
                      let msPerFrameC1 = decision.msPerFrameC1,
                      !decision.degradationCurve.isEmpty else {
                    continue
                }
                if machineIndexByHost[host] == nil {
                    machineProfiles.append(
                        CATopologyModelMachineProfile(
                            id: machineID,
                            msPerFrameC1: msPerFrameC1,
                            fixedOverheadMS: decision.fixedOverheadMS ?? 0,
                            degradationCurve: decision.degradationCurve,
                            txInMS: decision.txInMS,
                            txOutMS: decision.txOutMS,
                            publishOverheadMS: decision.publishOverheadMS,
                            modeledConcurrencyCap: decision.concurrencyCap
                        )
                    )
                    machineMSSources.append(decision.msSource)
                    machineCurveSources.append(decision.curveSource)
                    machineConfidenceTiers.append(decision.confidenceTier)
                    machineConfidenceMultipliers.append(decision.confidenceMultiplier)
                    machineConcurrencyCaps.append(decision.concurrencyCap)
                    machineIndexByHost[host] = machineProfiles.count - 1
                }
                let machineIndex = machineIndexByHost[host] ?? 0
                slotBindings.append(
                    CATopologyModelSlotBinding(
                        machineIndex: machineIndex,
                        slotID: "\(host)#s\(index)"
                    )
                )
            }
        }

        var slotCountsByMachine = Array(repeating: 0, count: machineProfiles.count)
        for binding in slotBindings where slotCountsByMachine.indices.contains(binding.machineIndex) {
            slotCountsByMachine[binding.machineIndex] += 1
        }
        let modelInputs = machineProfiles.enumerated().map { index, machine in
            CATopologyModelInputRow(
                machineID: machine.id,
                slotCount: slotCountsByMachine.indices.contains(index) ? slotCountsByMachine[index] : 0,
                msPerFrameC1: machine.msPerFrameC1,
                fixedOverheadMS: machine.fixedOverheadMS,
                msSource: machineMSSources.indices.contains(index) ? machineMSSources[index] : "unknown",
                curveSource: machineCurveSources.indices.contains(index) ? machineCurveSources[index] : "unknown",
                txInMS: machine.txInMS,
                txOutMS: machine.txOutMS,
                publishOverheadMS: machine.publishOverheadMS,
                confidenceTier: machineConfidenceTiers.indices.contains(index) ? machineConfidenceTiers[index] : nil,
                confidenceMultiplier: machineConfidenceMultipliers.indices.contains(index)
                    ? machineConfidenceMultipliers[index]
                    : 1.0,
                concurrencyCap: machineConcurrencyCaps.indices.contains(index) ? machineConcurrencyCaps[index] : nil
            )
        }

        let reachableSlotCount = coverageRows.reduce(0) { partial, row in
            partial + row.reachableSlots
        }
        let executableRemoteSlotCount = coverageRows.reduce(0) { partial, row in
            partial + row.executableSlots
        }
        let modeledSlotCount = coverageRows.reduce(0) { partial, row in
            partial + row.modeledSlots
        }
        let modeledWorkerCount = coverageRows.filter { $0.modeledSlots > 0 }.count
        let localExecutableSlotCount = slotBindings.reduce(into: 0) { partial, binding in
            if binding.machineIndex == 0 {
                partial += 1
            }
        }
        let localPriorGap = localProfile.msSource != "prior(local)" || localProfile.curveSource != "prior(local)"
        let remotePriorGap = coverageRows.contains { row in
            row.executableSlots > 0 && row.confidenceTier != .exactPrior
        } || reachableWorkers.contains { worker in
            guard worker.executableSlots > 0,
                  let decision = decisionsByHost[worker.host] else {
                return false
            }
            return decision.usesLegacyAffineHeuristic
        }

        return CATopologyModelBuildResult(
            machineProfiles: machineProfiles,
            slotBindings: slotBindings,
            machineIndexByHost: machineIndexByHost,
            modelInputs: modelInputs,
            diagnostics: CATopologyModelDiagnostics(
                mode: mode,
                coverageRows: coverageRows,
                strictExclusions: strictExclusions,
                reachableWorkerCount: coverageRows.count,
                reachableSlotCount: reachableSlotCount,
                modeledWorkerCount: modeledWorkerCount,
                modeledSlotCount: modeledSlotCount,
                fallbackActive: fallbackActive,
                localPriorGap: localPriorGap,
                remotePriorGap: remotePriorGap,
                localExecutableSlotCount: localExecutableSlotCount,
                remoteExecutableSlotCount: executableRemoteSlotCount,
                totalExecutableSlotCount: localExecutableSlotCount + executableRemoteSlotCount,
                exactPriorSlotCount: exactPriorSlotCount,
                hardwareCompatiblePriorSlotCount: hardwareCompatiblePriorSlotCount,
                capabilityBackedSlotCount: capabilityBackedSlotCount,
                localFallbackSlotCount: localFallbackSlotCount
            )
        )
    }

    private static func missingDecision(host: String) -> CARemoteModelDecision {
        CARemoteModelDecision(
            host: host,
            machineID: nil,
            msPerFrameC1: nil,
            fixedOverheadMS: nil,
            degradationCurve: [],
            txInMS: 0,
            txOutMS: 0,
            publishOverheadMS: 0,
            msSource: "-",
            curveSource: "-",
            exclusionReason: "missing remote model decision",
            fallbackActive: false,
            confidenceTier: nil,
            confidenceMultiplier: 0,
            concurrencyCap: nil,
            usesLegacyAffineHeuristic: false
        )
    }
}
