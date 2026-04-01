import Foundation
import OSLog

extension MediaProcessor {
    func detectAssetType(path: String) async -> Asset.AssetType? {
        if ImageProcessor.isImage(path: path) {
            return .image
        }
        if await VideoProcessor.isVideo(sourcePath: path) {
            return .video
        }
        return nil
    }

    func cleanupDerivedFiles(uploadId: String, assetType: Asset.AssetType) {
        let fm = FileManager.default
        let thumbPath = "\(config.thumbsDir)/\(uploadId).jpg"
        let previewExt = assetType == .video ? "mp4" : "jpg"
        let previewPath = "\(config.previewsDir)/\(uploadId).\(previewExt)"

        try? fm.removeItem(atPath: thumbPath)
        try? fm.removeItem(atPath: previewPath)
    }

    func cancelledWithCleanup(uploadId: String, assetType: Asset.AssetType) -> Bool {
        guard Task.isCancelled else { return false }
        Logger.kiko.info("Processing cancelled for \(uploadId)")
        cleanupDerivedFiles(uploadId: uploadId, assetType: assetType)
        return true
    }

    @discardableResult
    func updateStatusLogged(id: String, status: Asset.AssetStatus) async -> Bool {
        do {
            if let statusUpdateOverride {
                try await statusUpdateOverride(id, status)
            } else {
                try await database.updateStatus(id: id, status: status)
            }
            recordStatusTransition(id: id, status: status)
            return true
        } catch {
            Logger.kiko.error("DB status update failed for \(id) to \(status.rawValue): \(error)")
            return false
        }
    }

    func updateTerminalStatusLogged(
        id: String,
        status: Asset.AssetStatus
    ) async -> Bool {
        let retryDelaysNanos: [UInt64] = [0, 50_000_000, 200_000_000]
        for (attempt, delay) in retryDelaysNanos.enumerated() {
            if delay > 0 {
                try? await Task.sleep(nanoseconds: delay)
            }
            if await updateStatusLogged(id: id, status: status) {
                return true
            }
            if attempt == retryDelaysNanos.count - 1 {
                Logger.kiko.error(
                    "Terminal status persistence exhausted retries for \(id, privacy: .public) => \(status.rawValue, privacy: .public)"
                )
            }
        }
        return false
    }

    func markCompleteTerminalLogged(id: String, timestamp: String) async -> Bool {
        let retryDelaysNanos: [UInt64] = [0, 50_000_000, 200_000_000]
        for (attempt, delay) in retryDelaysNanos.enumerated() {
            if delay > 0 {
                try? await Task.sleep(nanoseconds: delay)
            }
            do {
                if let markCompleteOverride {
                    try await markCompleteOverride(id, timestamp)
                } else {
                    try await database.markComplete(id: id, timestamp: timestamp)
                }
                return true
            } catch {
                Logger.kiko.error("DB markComplete failed for \(id): \(error)")
                if attempt == retryDelaysNanos.count - 1 {
                    Logger.kiko.error(
                        "Terminal complete persistence exhausted retries for \(id, privacy: .public)"
                    )
                }
            }
        }
        return false
    }

    func currentTimestamp() -> String {
        DateUtils.exifTimestamp(from: Date())
    }
}
