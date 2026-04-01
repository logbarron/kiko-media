package enum BenchmarkPriorPreflightClassification: String, Sendable {
    case healthy = "healthy"
    case localPriorGap = "local-prior-gap"
    case remotePriorGap = "remote-prior-gap"
    case localAndRemotePriorGap = "local-prior-gap+remote-prior-gap"

    package static func classify(
        localPriorGap: Bool,
        remotePriorGap: Bool
    ) -> BenchmarkPriorPreflightClassification {
        if localPriorGap && remotePriorGap {
            return .localAndRemotePriorGap
        }
        if localPriorGap {
            return .localPriorGap
        }
        if remotePriorGap {
            return .remotePriorGap
        }
        return .healthy
    }
}

package struct BenchmarkPriorModeledWorker: Sendable, Equatable {
    package let signature: String
    package let slots: Int

    package init(signature: String, slots: Int) {
        self.signature = signature
        self.slots = slots
    }
}

package struct BenchmarkPriorPromotionDecision: Sendable, Equatable {
    package let shouldPromote: Bool
    package let reason: String
    package let missingModeledSignatures: [String]
    package let currentRemoteWorkerCoverage: Int
    package let candidateRemoteWorkerCoverage: Int
    package let currentRemoteSlotCoverage: Int
    package let candidateRemoteSlotCoverage: Int
    package let currentLocalPriorValid: Bool
    package let candidateLocalPriorValid: Bool
    package let localPriorValidityImproved: Bool
    package let candidateCorpusAtLeastAsStrong: Bool
    package let forceApplied: Bool

    package init(
        shouldPromote: Bool,
        reason: String,
        missingModeledSignatures: [String],
        currentRemoteWorkerCoverage: Int,
        candidateRemoteWorkerCoverage: Int,
        currentRemoteSlotCoverage: Int,
        candidateRemoteSlotCoverage: Int,
        currentLocalPriorValid: Bool,
        candidateLocalPriorValid: Bool,
        localPriorValidityImproved: Bool,
        candidateCorpusAtLeastAsStrong: Bool,
        forceApplied: Bool
    ) {
        self.shouldPromote = shouldPromote
        self.reason = reason
        self.missingModeledSignatures = missingModeledSignatures
        self.currentRemoteWorkerCoverage = currentRemoteWorkerCoverage
        self.candidateRemoteWorkerCoverage = candidateRemoteWorkerCoverage
        self.currentRemoteSlotCoverage = currentRemoteSlotCoverage
        self.candidateRemoteSlotCoverage = candidateRemoteSlotCoverage
        self.currentLocalPriorValid = currentLocalPriorValid
        self.candidateLocalPriorValid = candidateLocalPriorValid
        self.localPriorValidityImproved = localPriorValidityImproved
        self.candidateCorpusAtLeastAsStrong = candidateCorpusAtLeastAsStrong
        self.forceApplied = forceApplied
    }
}

package enum BenchmarkPriorPolicyKernel {
    package static func evaluatePromotion(
        localSignature: String,
        currentLocalPriorValid: Bool,
        candidateLocalPriorValid: Bool,
        currentModeledWorkers: [BenchmarkPriorModeledWorker],
        candidateModeledWorkers: [BenchmarkPriorModeledWorker],
        currentCorpusSummary: BenchmarkPriorCorpusSummary? = nil,
        candidateCorpusSummary: BenchmarkPriorCorpusSummary? = nil,
        showdownComparatorPass: Bool? = nil,
        requireComparator: Bool = false,
        force: Bool
    ) -> BenchmarkPriorPromotionDecision {
        let localPriorValidityImproved = !currentLocalPriorValid && candidateLocalPriorValid

        var currentSignatures = Set(currentModeledWorkers.map(\.signature))
        var candidateSignatures = Set(candidateModeledWorkers.map(\.signature))
        if currentLocalPriorValid {
            currentSignatures.insert(localSignature)
        }
        if candidateLocalPriorValid {
            candidateSignatures.insert(localSignature)
        }
        let missingModeledSignatures = currentSignatures.subtracting(candidateSignatures).sorted()
        let candidateCorpusAtLeastAsStrong: Bool = {
            guard let currentCorpusSummary,
                  let candidateCorpusSummary else {
                return true
            }
            return candidateCorpusSummary.videoCount >= currentCorpusSummary.videoCount
                && candidateCorpusSummary.totalBytes >= currentCorpusSummary.totalBytes
        }()

        let currentRemoteWorkerCoverage = currentModeledWorkers.count
        let candidateRemoteWorkerCoverage = candidateModeledWorkers.count
        let currentRemoteSlotCoverage = currentModeledWorkers.reduce(0) { $0 + $1.slots }
        let candidateRemoteSlotCoverage = candidateModeledWorkers.reduce(0) { $0 + $1.slots }
        let improvedCoverage =
            candidateRemoteSlotCoverage > currentRemoteSlotCoverage
            || candidateRemoteWorkerCoverage > currentRemoteWorkerCoverage
            || localPriorValidityImproved

        if candidateRemoteWorkerCoverage < currentRemoteWorkerCoverage {
            return decision(
                shouldPromote: false,
                reason: "remote worker coverage regressed",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: false
            )
        }

        if !missingModeledSignatures.isEmpty {
            return decision(
                shouldPromote: false,
                reason: "candidate drops currently modeled signatures",
                missingModeledSignatures: missingModeledSignatures,
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: false
            )
        }

        if candidateRemoteSlotCoverage < currentRemoteSlotCoverage {
            return decision(
                shouldPromote: false,
                reason: "remote slot coverage regressed",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: false
            )
        }

        if currentLocalPriorValid && !candidateLocalPriorValid {
            return decision(
                shouldPromote: false,
                reason: "local prior validity regressed",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: false
            )
        }

        if !candidateCorpusAtLeastAsStrong {
            return decision(
                shouldPromote: false,
                reason: "weaker corpus than canonical",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: false,
                forceApplied: false
            )
        }

        if requireComparator, !force, showdownComparatorPass != true {
            return decision(
                shouldPromote: false,
                reason: "showdown comparator rejected candidate",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: false
            )
        }

        if requireComparator {
            return decision(
                shouldPromote: true,
                reason: force ? "force promote enabled" : "showdown comparator passed",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: force
            )
        }

        if improvedCoverage {
            return decision(
                shouldPromote: true,
                reason: "candidate improves strict coverage",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: false
            )
        }

        if force {
            return decision(
                shouldPromote: true,
                reason: "force promote enabled",
                missingModeledSignatures: [],
                currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
                candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
                currentRemoteSlotCoverage: currentRemoteSlotCoverage,
                candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
                currentLocalPriorValid: currentLocalPriorValid,
                candidateLocalPriorValid: candidateLocalPriorValid,
                localPriorValidityImproved: localPriorValidityImproved,
                candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
                forceApplied: true
            )
        }

        return decision(
            shouldPromote: false,
            reason: "candidate does not improve strict coverage",
            missingModeledSignatures: [],
            currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
            candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
            currentRemoteSlotCoverage: currentRemoteSlotCoverage,
            candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
            currentLocalPriorValid: currentLocalPriorValid,
            candidateLocalPriorValid: candidateLocalPriorValid,
            localPriorValidityImproved: localPriorValidityImproved,
            candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
            forceApplied: false
        )
    }

    private static func decision(
        shouldPromote: Bool,
        reason: String,
        missingModeledSignatures: [String],
        currentRemoteWorkerCoverage: Int,
        candidateRemoteWorkerCoverage: Int,
        currentRemoteSlotCoverage: Int,
        candidateRemoteSlotCoverage: Int,
        currentLocalPriorValid: Bool,
        candidateLocalPriorValid: Bool,
        localPriorValidityImproved: Bool,
        candidateCorpusAtLeastAsStrong: Bool,
        forceApplied: Bool
    ) -> BenchmarkPriorPromotionDecision {
        BenchmarkPriorPromotionDecision(
            shouldPromote: shouldPromote,
            reason: reason,
            missingModeledSignatures: missingModeledSignatures,
            currentRemoteWorkerCoverage: currentRemoteWorkerCoverage,
            candidateRemoteWorkerCoverage: candidateRemoteWorkerCoverage,
            currentRemoteSlotCoverage: currentRemoteSlotCoverage,
            candidateRemoteSlotCoverage: candidateRemoteSlotCoverage,
            currentLocalPriorValid: currentLocalPriorValid,
            candidateLocalPriorValid: candidateLocalPriorValid,
            localPriorValidityImproved: localPriorValidityImproved,
            candidateCorpusAtLeastAsStrong: candidateCorpusAtLeastAsStrong,
            forceApplied: forceApplied
        )
    }
}
