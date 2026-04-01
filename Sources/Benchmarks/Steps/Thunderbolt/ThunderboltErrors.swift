import Foundation

enum ThunderboltBenchmarkJSONError: Error, CustomStringConvertible {
    case noVideos
    case workersNotConfigured
    case invalidWorkers(String)
    case noBridgeSources
    case workerBindingIssues([ThunderboltWorkerBindingIssue])
    case workerUnreachable(host: String, port: Int)
    case noBurstConfigs
    case invariantViolation(String)
    case caStrictRemoteExclusion([String])
    case encodingFailed

    var description: String {
        switch self {
        case .noVideos:
            return "no video files in media folder"
        case .workersNotConfigured:
            return "TB_WORKERS is not configured (environment or com.kiko.media.plist)"
        case .invalidWorkers(let raw):
            return "TB_WORKERS has no valid worker entries: \(raw)"
        case .noBridgeSources:
            return "no local bridge route available for configured workers"
        case .workerBindingIssues(let issues):
            let details = issues.map { "\($0.worker.host): \($0.reason)" }.joined(separator: "; ")
            return "source-bind unavailable for configured workers (\(details))"
        case .workerUnreachable(let host, let port):
            return "worker unreachable: \(host):\(port)"
        case .noBurstConfigs:
            return "could not compute thunderbolt burst configurations"
        case .invariantViolation(let reason):
            return "benchmark JSON invariant violation (\(reason))"
        case .caStrictRemoteExclusion(let reasons):
            let details = reasons.joined(separator: "; ")
            return "strict CA model mode excluded reachable remotes (\(details)); rerun showdown with --ca-model-mode auto or refresh remote benchmark-prior coverage"
        case .encodingFailed:
            return "could not encode JSON payload"
        }
    }
}

enum ThunderboltCAAcceptanceError: Error, CustomStringConvertible {
    case failedProfiles([String])
    case inconsistentJobCount(profile: String, expected: Int, fifoActual: Int, caActual: Int)

    var description: String {
        switch self {
        case .failedProfiles(let profiles):
            return "CA acceptance failed for profile(s): \(profiles.joined(separator: ", "))"
        case .inconsistentJobCount(let profile, let expected, let fifoActual, let caActual):
            return "CA acceptance corpus mismatch for profile \(profile): expected \(expected) jobs, fifo=\(fifoActual), ca=\(caActual)"
        }
    }
}

enum ThunderboltShowdownPriorMaintenanceError: Error, CustomStringConvertible {
    case noLocalBurstData
    case candidateLoadFailed(String)

    var description: String {
        switch self {
        case .noLocalBurstData:
            return "could not build local-only prior candidate from burst measurements"
        case .candidateLoadFailed(let path):
            return "failed to load prior candidate at \(path)"
        }
    }
}
