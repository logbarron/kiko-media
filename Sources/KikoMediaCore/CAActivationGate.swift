package enum CAActivationGate {
    package enum Failure: Sendable, Equatable {
        case noWorkers
        case missingPriorArtifact
        case invalidPriorArtifact
        case invalidLocalPrior
        case strictTickV2Rejected
    }

    package enum PriorArtifactState: Sendable, Equatable {
        case missing
        case invalid
        case loaded
    }

    package enum Decision: Sendable, Equatable {
        case enabled
        case disabled(Failure)

        package var isEnabled: Bool {
            if case .enabled = self {
                return true
            }
            return false
        }
    }

    package static func resolveLocalPriorProfile(
        priorTable: BenchmarkPriorTable?,
        videoTranscodePreset: String
    ) -> CAValidatedPriorProfile? {
        guard let priorTable,
              let signature = localWorkerSignature(videoTranscodePreset: videoTranscodePreset) else {
            return nil
        }
        if let exactProfile = CAProfileAndFallbackMath.priorProfile(
            forSignature: signature,
            in: priorTable
        ) {
            return exactProfile
        }
        let compatibleMachine = priorTable.hardwareCompatibleMachine(signature: signature)
        return CAProfileAndFallbackMath.validatedPriorProfile(from: compatibleMachine)
    }

    package static func evaluate(
        workersPresent: Bool,
        priorArtifactState: PriorArtifactState,
        localPriorProfile: CAValidatedPriorProfile?,
        strictTickV2Accepted: Bool? = nil
    ) -> Decision {
        guard workersPresent else {
            return .disabled(.noWorkers)
        }
        guard priorArtifactState != .missing else {
            return .disabled(.missingPriorArtifact)
        }
        guard priorArtifactState != .invalid else {
            return .disabled(.invalidPriorArtifact)
        }
        guard localPriorProfile != nil else {
            return .disabled(.invalidLocalPrior)
        }
        guard strictTickV2Accepted != false else {
            return .disabled(.strictTickV2Rejected)
        }
        return .enabled
    }

    private static func localWorkerSignature(videoTranscodePreset: String) -> String? {
        let caps = WorkerCaps.detectLocal()
        return WorkerSignatureBuilder.make(
            chipName: caps.chipName,
            performanceCores: caps.performanceCores,
            efficiencyCores: caps.efficiencyCores,
            videoEncodeEngines: caps.videoEncodeEngines,
            preset: videoTranscodePreset,
            osVersion: caps.osVersion
        )
    }
}
