import Foundation

package enum CARemoteModelDecisionMode: String, CaseIterable, Sendable {
    case strict
    case auto
}

package enum CAMachineConfidenceTier: String, CaseIterable, Sendable {
    case exactPrior
    case hardwareCompatiblePrior
    case capabilityBacked
    case localFallback

    package var multiplier: Double {
        switch self {
        case .exactPrior:
            1.00
        case .hardwareCompatiblePrior:
            1.15
        case .capabilityBacked:
            1.25
        case .localFallback:
            1.35
        }
    }

    package var concurrencyCap: Int? {
        switch self {
        case .exactPrior:
            nil
        case .hardwareCompatiblePrior, .capabilityBacked, .localFallback:
            1
        }
    }

    package var isLowerConfidence: Bool {
        self != .exactPrior
    }
}

package struct CARemoteModelDecision: Sendable {
    package let host: String
    package let machineID: String?
    package let msPerFrameC1: Double?
    package let fixedOverheadMS: Double?
    package let degradationCurve: [CADegradationPoint]
    package let txInMS: Double
    package let txOutMS: Double
    package let publishOverheadMS: Double
    package let msSource: String
    package let curveSource: String
    package let exclusionReason: String?
    package let fallbackActive: Bool
    package let confidenceTier: CAMachineConfidenceTier?
    package let confidenceMultiplier: Double
    package let concurrencyCap: Int?
    package let usesLegacyAffineHeuristic: Bool

    package init(
        host: String,
        machineID: String?,
        msPerFrameC1: Double?,
        fixedOverheadMS: Double?,
        degradationCurve: [CADegradationPoint],
        txInMS: Double,
        txOutMS: Double,
        publishOverheadMS: Double,
        msSource: String,
        curveSource: String,
        exclusionReason: String?,
        fallbackActive: Bool,
        confidenceTier: CAMachineConfidenceTier?,
        confidenceMultiplier: Double,
        concurrencyCap: Int?,
        usesLegacyAffineHeuristic: Bool
    ) {
        self.host = host
        self.machineID = machineID
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.degradationCurve = degradationCurve
        self.txInMS = txInMS
        self.txOutMS = txOutMS
        self.publishOverheadMS = publishOverheadMS
        self.msSource = msSource
        self.curveSource = curveSource
        self.exclusionReason = exclusionReason
        self.fallbackActive = fallbackActive
        self.confidenceTier = confidenceTier
        self.confidenceMultiplier = confidenceMultiplier
        self.concurrencyCap = concurrencyCap
        self.usesLegacyAffineHeuristic = usesLegacyAffineHeuristic
    }
}

package enum CARemoteModelDecisionKernel {
    private struct BaseModel {
        let tier: CAMachineConfidenceTier
        let msPerFrameC1: Double
        let fixedOverheadMS: Double
        let degradationCurve: [CADegradationPoint]
        let msSource: String
        let curveSource: String
        let usesLegacyAffineHeuristic: Bool
    }

    package static func resolve(
        host: String,
        port: Int,
        mode: CARemoteModelDecisionMode,
        workerSignature: String? = nil,
        caps: WorkerCaps?,
        priorTable: BenchmarkPriorTable,
        remoteTxInEstimateMS: Double? = nil,
        remoteTxOutEstimateMS: Double? = nil,
        remotePublishOverheadEstimateMS: Double? = nil,
        localMSPerFrameC1: Double,
        localFixedOverheadMS: Double = 0,
        localCurve: [CADegradationPoint],
        liveMSPerFrameC1: Double? = nil
    ) -> CARemoteModelDecision {
        let _ = mode

        let signature = workerSignature ?? caps?.workerSignature
        let machineID = "\(host):\(port)"
        let txInMS = max(0, remoteTxInEstimateMS ?? 0)
        let txOutMS = max(0, remoteTxOutEstimateMS ?? 0)
        let publishOverheadMS = max(0, remotePublishOverheadEstimateMS ?? 0)

        guard let baseModel = resolveBaseModel(
            signature: signature,
            caps: caps,
            priorTable: priorTable,
            localMSPerFrameC1: localMSPerFrameC1,
            localFixedOverheadMS: localFixedOverheadMS,
            localCurve: localCurve,
            liveMSPerFrameC1: liveMSPerFrameC1
        ) else {
            let signatureText = signature ?? "<missing>"
            return CARemoteModelDecision(
                host: host,
                machineID: nil,
                msPerFrameC1: nil,
                fixedOverheadMS: nil,
                degradationCurve: [],
                txInMS: txInMS,
                txOutMS: txOutMS,
                publishOverheadMS: publishOverheadMS,
                msSource: "-",
                curveSource: "-",
                exclusionReason: "no usable remote model for signature \(signatureText)",
                fallbackActive: false,
                confidenceTier: nil,
                confidenceMultiplier: 0,
                concurrencyCap: nil,
                usesLegacyAffineHeuristic: false
            )
        }

        let multiplier = baseModel.tier.multiplier
        let resolvedCurve = if baseModel.tier.isLowerConfidence {
            CAProfileAndFallbackMath.conservativeComparableDegradationCurve(
                from: baseModel.degradationCurve
            )
        } else {
            baseModel.degradationCurve
        }

        return CARemoteModelDecision(
            host: host,
            machineID: machineID,
            msPerFrameC1: baseModel.msPerFrameC1 * multiplier,
            fixedOverheadMS: baseModel.fixedOverheadMS * multiplier,
            degradationCurve: resolvedCurve,
            txInMS: txInMS,
            txOutMS: txOutMS,
            publishOverheadMS: publishOverheadMS,
            msSource: baseModel.msSource,
            curveSource: baseModel.curveSource,
            exclusionReason: nil,
            fallbackActive: baseModel.tier != .exactPrior,
            confidenceTier: baseModel.tier,
            confidenceMultiplier: multiplier,
            concurrencyCap: baseModel.tier.concurrencyCap,
            usesLegacyAffineHeuristic: baseModel.usesLegacyAffineHeuristic
        )
    }

    private static func resolveBaseModel(
        signature: String?,
        caps: WorkerCaps?,
        priorTable: BenchmarkPriorTable,
        localMSPerFrameC1: Double,
        localFixedOverheadMS: Double,
        localCurve: [CADegradationPoint],
        liveMSPerFrameC1: Double?
    ) -> BaseModel? {
        let exactMachine = priorTable.exactMachine(signature: signature)
        let exactModel = exactMachine.flatMap { machine in
            priorBackedModel(
                machine: machine,
                tier: .exactPrior,
                msSource: "prior(remote)",
                curveSource: "prior(remote)",
                liveMSPerFrameC1: liveMSPerFrameC1,
                preferLiveOverlay: true
            )
        }
        if let exactModel {
            return exactModel
        }

        let compatibleMachine = priorTable.hardwareCompatibleMachine(signature: signature)
        let compatibleModel = compatibleMachine.flatMap { machine in
            priorBackedModel(
                machine: machine,
                tier: .hardwareCompatiblePrior,
                msSource: "prior(hardware-compatible)",
                curveSource: "prior(hardware-compatible)",
                liveMSPerFrameC1: liveMSPerFrameC1,
                preferLiveOverlay: false
            )
        }
        if let compatibleModel {
            return compatibleModel
        }

        let capsCurve = capabilityCurve(from: caps)
        let capsMS = CAProfileAndFallbackMath.validMSPerFrameC1(
            liveMSPerFrameC1 ?? caps?.msPerFrameC1
        )
        if let capsMS, !capsCurve.isEmpty {
            return BaseModel(
                tier: .capabilityBacked,
                msPerFrameC1: capsMS,
                fixedOverheadMS: 0,
                degradationCurve: capsCurve,
                msSource: "caps(remote)",
                curveSource: "caps(remote)",
                usesLegacyAffineHeuristic: false
            )
        }

        guard let localMS = CAProfileAndFallbackMath.validMSPerFrameC1(localMSPerFrameC1),
              !localCurve.isEmpty else {
            return nil
        }
        return BaseModel(
            tier: .localFallback,
            msPerFrameC1: localMS,
            fixedOverheadMS: CAProfileAndFallbackMath.resolvedFixedOverheadMS(localFixedOverheadMS),
            degradationCurve: localCurve,
            msSource: "fallback(local-c1)",
            curveSource: "fallback(local-curve)",
            usesLegacyAffineHeuristic: false
        )
    }

    private static func priorBackedModel(
        machine: BenchmarkPriorMachine,
        tier: CAMachineConfidenceTier,
        msSource: String,
        curveSource: String,
        liveMSPerFrameC1: Double?,
        preferLiveOverlay: Bool
    ) -> BaseModel? {
        guard let affineModel = CAProfileAndFallbackMath.resolvedRemoteAffineModel(from: machine) else {
            return nil
        }
        let curve = CAProfileAndFallbackMath.degradationCurve(from: machine)
        guard !curve.isEmpty else { return nil }

        let liveMS = CAProfileAndFallbackMath.validMSPerFrameC1(liveMSPerFrameC1)
        let resolvedMSPerFrameC1: Double
        if preferLiveOverlay, let liveMS {
            resolvedMSPerFrameC1 = liveMS
        } else if let liveMS {
            resolvedMSPerFrameC1 = max(affineModel.msPerFrameC1, liveMS)
        } else {
            resolvedMSPerFrameC1 = affineModel.msPerFrameC1
        }

        return BaseModel(
            tier: tier,
            msPerFrameC1: resolvedMSPerFrameC1,
            fixedOverheadMS: affineModel.fixedOverheadMS,
            degradationCurve: curve,
            msSource: msSource,
            curveSource: curveSource,
            usesLegacyAffineHeuristic: affineModel.source == .legacyHeuristic
        )
    }

    private static func capabilityCurve(from caps: WorkerCaps?) -> [CADegradationPoint] {
        let priorCellsCurve = CAProfileAndFallbackMath.degradationCurve(from: caps?.priorCells)
        if !priorCellsCurve.isEmpty {
            return priorCellsCurve
        }
        let rawCurve = CAProfileAndFallbackMath.degradationCurve(from: caps?.degradationCurve)
        if !rawCurve.isEmpty {
            return rawCurve
        }
        return []
    }
}
