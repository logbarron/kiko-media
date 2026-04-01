import Foundation

enum BenchmarkError: Error, CustomStringConvertible {
    case mediaFolderNotFound(String)
    case emptyMediaFolder(String)
    case memoryGuardExceeded(
        stage: String,
        detail: String?,
        currentMB: Int,
        warningMB: Int,
        limitMB: Int,
        peakMB: Int
    )

    var description: String {
        switch self {
        case .mediaFolderNotFound(let p):
            return "Media folder not found: \(p)"
        case .emptyMediaFolder(let p):
            return "No media files found in: \(p)"
        case let .memoryGuardExceeded(stage, detail, currentMB, warningMB, limitMB, peakMB):
            var message = "memory guard triggered in stage '\(stage)': \(currentMB)MB (warning \(warningMB)MB, limit \(limitMB)MB, peak \(peakMB)MB)"
            if let detail, !detail.isEmpty {
                message += " [\(detail)]"
            }
            message += ". Adjust BENCH_MEMORY_LIMIT_MB (0 disables) or run a narrower stage."
            return message
        }
    }
}
