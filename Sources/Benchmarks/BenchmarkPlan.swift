import Foundation
import KikoMediaCore

enum BurstSearchStrategy: Sendable {
    case bruteForce
    case optimized(topK: Int = 10)
}

enum CASchedulerPolicy: String, Sendable {
    case fifo
    case complexityAware = "ca"
}

enum CAArrivalProfile: String, CaseIterable, Sendable {
    case allAtOnce = "all-at-once"
    case burst_1_20_5_5_1 = "burst-1-20-5-5-1"
    case trickle
}

enum ThunderboltRunType: Sendable {
    case full
    case burstSweep
    case showdown
}

enum ThunderboltShowdownScope: Sendable {
    case allProfiles
    case singleProfile(CAArrivalProfile)
}

enum ThunderboltCAModelMode: String, CaseIterable, Sendable {
    case strict
    case auto

    var sharedRemoteDecisionMode: CARemoteModelDecisionMode {
        switch self {
        case .strict:
            .strict
        case .auto:
            .auto
        }
    }
}

enum ThunderboltPriorUpdatePolicy: String, CaseIterable, Sendable {
    case off
    case candidateOnly
    case promoteGuarded
    case promoteForce
}

struct BenchmarkPlan: Sendable {
    var mediaFolder: String? = nil
    // `comparison` is opt-in; keep the default suite stable.
    var components: [BenchmarkComponent] = BenchmarkComponent.allCases.filter { $0 != .comparison }

    var videoPreset: String = defaultVideoPreset
    var videoTimeoutSeconds: Int = defaultVideoTimeoutSeconds

    var ssdPath: String? = nil
    var keepSSDBenchArtifacts: Bool = false
    // nil = legacy/default behavior (CLI), true/false = explicit wizard choice.
    var updateProductionPriorFromRun: Bool? = nil

    var tbWorkers: String? = nil

    var jsonMode: Bool = false
    var sweepMode: BurstSearchStrategy = .optimized()
    // Wizard-only: do not surface the default as completed until the prompt has run.
    var hasChosenSweepMode: Bool = false
    var thunderboltRunType: ThunderboltRunType = .full
    var thunderboltShowdownScope: ThunderboltShowdownScope = .allProfiles
    var thunderboltSlotOverrides: ThunderboltCASlotOverrides? = nil
    var thunderboltCAModelMode: ThunderboltCAModelMode = .auto
    // Stored as one policy so wizard, CLI, and runtime stay in sync.
    var thunderboltPriorUpdatePolicy: ThunderboltPriorUpdatePolicy = .off
    var caSchedulerPolicy: CASchedulerPolicy? = nil
    var caArrivalProfile: CAArrivalProfile? = nil
    var caRawOutputPath: String? = nil
    var caSummaryOutputPath: String? = nil
    var runCAAcceptance: Bool = false
    var caAcceptanceOutputPath: String? = nil

    var runLimitFinder: Bool = false
    var limitConfig: LimitFinderConfig = LimitFinderConfig()

    var reportDirectory: String = "bench-results"

    var requiresMediaFolder: Bool {
        components.contains { BenchmarkCatalog.spec(for: $0).requiresMediaFolder }
    }

    var requiresSSDPath: Bool {
        components.contains { BenchmarkCatalog.spec(for: $0).requiresSSDPath }
    }

    // Compatibility shims for legacy CLI parsing while the runtime migrates.
    var refreshPriorBeforeShowdown: Bool {
        get {
            thunderboltPriorUpdatePolicy != .off
        }
        set {
            guard !newValue else {
                if thunderboltPriorUpdatePolicy == .off {
                    thunderboltPriorUpdatePolicy = .candidateOnly
                }
                return
            }
            thunderboltPriorUpdatePolicy = .off
        }
    }

    var promotePrior: Bool {
        get {
            switch thunderboltPriorUpdatePolicy {
            case .promoteGuarded, .promoteForce:
                return true
            case .off, .candidateOnly:
                return false
            }
        }
        set {
            guard !newValue else {
                if thunderboltPriorUpdatePolicy == .off || thunderboltPriorUpdatePolicy == .candidateOnly {
                    thunderboltPriorUpdatePolicy = .promoteGuarded
                }
                return
            }
            switch thunderboltPriorUpdatePolicy {
            case .promoteGuarded, .promoteForce:
                thunderboltPriorUpdatePolicy = .candidateOnly
            case .off, .candidateOnly:
                break
            }
        }
    }

    var forcePromote: Bool {
        get {
            thunderboltPriorUpdatePolicy == .promoteForce
        }
        set {
            guard !newValue else {
                thunderboltPriorUpdatePolicy = .promoteForce
                return
            }
            if thunderboltPriorUpdatePolicy == .promoteForce {
                thunderboltPriorUpdatePolicy = .promoteGuarded
            }
        }
    }
}
