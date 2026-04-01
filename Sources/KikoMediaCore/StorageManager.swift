import Foundation

package struct StorageManager: Sendable {
    package let externalSSDPath: String
    package let sha256BufferSize: Int

    package init(externalSSDPath: String, sha256BufferSize: Int) {
        self.externalSSDPath = externalSSDPath
        self.sha256BufferSize = sha256BufferSize
    }

    package func archiveOriginal(sourcePath: String, assetId: String, originalName: String) async -> ArchiveResult {
        let fm = FileManager.default

        let ext = (originalName as NSString).pathExtension.lowercased()
        let filename = ext.isEmpty ? assetId : "\(assetId).\(ext)"

        guard VolumeUtils.isMounted(volumeContainingPath: externalSSDPath) else {
            return .ssdUnavailable
        }

        let internalChecksum: String
        do {
            internalChecksum = try calculateSHA256(path: sourcePath)
        } catch {
            return .failed("Failed to calculate checksum of upload: \(error.localizedDescription)")
        }

        let externalPath = "\(externalSSDPath)/\(filename)"
        let temporaryPath = "\(externalPath).partial-\(UUID().uuidString)"
        defer { try? fm.removeItem(atPath: temporaryPath) }

        do {
            try fm.createDirectory(atPath: externalSSDPath, withIntermediateDirectories: true)
            try fm.copyItem(atPath: sourcePath, toPath: temporaryPath)
        } catch {
            return .ssdWriteFailed(checksum: internalChecksum, reason: error.localizedDescription)
        }

        let externalChecksum: String
        do {
            externalChecksum = try calculateSHA256(path: temporaryPath)
        } catch {
            return .verificationFailed(
                internalPath: sourcePath,
                externalPath: externalPath,
                reason: "Cannot calculate checksum of external copy"
            )
        }

        if internalChecksum != externalChecksum {
            return .checksumMismatch(
                internalPath: sourcePath,
                externalPath: externalPath,
                internalChecksum: internalChecksum,
                externalChecksum: externalChecksum
            )
        }

        do {
            if fm.fileExists(atPath: externalPath) {
                _ = try fm.replaceItemAt(
                    URL(fileURLWithPath: externalPath),
                    withItemAt: URL(fileURLWithPath: temporaryPath),
                    backupItemName: nil,
                    options: []
                )
            } else {
                try fm.moveItem(atPath: temporaryPath, toPath: externalPath)
            }
        } catch {
            return .ssdWriteFailed(checksum: internalChecksum, reason: "Failed to finalize archive copy: \(error.localizedDescription)")
        }

        return .success(externalPath: externalPath, checksum: internalChecksum)
    }

    private func calculateSHA256(path: String) throws -> String {
        try SHA256Utility.calculateSHA256(path: path, bufferSize: sha256BufferSize)
    }

}

package enum ArchiveResult: Sendable {
    case success(externalPath: String, checksum: String)
    case ssdUnavailable
    case ssdWriteFailed(checksum: String, reason: String)
    case checksumMismatch(internalPath: String, externalPath: String, internalChecksum: String, externalChecksum: String)
    case verificationFailed(internalPath: String, externalPath: String, reason: String)
    case failed(String)

    package var isSafelyStored: Bool {
        switch self {
        case .success:
            return true
        case .ssdUnavailable, .ssdWriteFailed, .failed, .checksumMismatch, .verificationFailed:
            return false
        }
    }

    package var logMessage: String {
        switch self {
        case .success(let path, let checksum):
            return "Archived to \(path) (SHA256: \(checksum.prefix(16))...)"
        case .ssdUnavailable:
            return "SSD unavailable - will retry on restart"
        case .ssdWriteFailed(_, let reason):
            return "SSD write failed (\(reason)) - will retry on restart"
        case .checksumMismatch(_, _, let internal_, let external):
            return "ERROR: Checksum mismatch! Internal: \(internal_.prefix(16))..., External: \(external.prefix(16))..."
        case .verificationFailed(_, _, let reason):
            return "WARNING: Verification failed (\(reason)), discarded archive attempt"
        case .failed(let reason):
            return "ERROR: Archive failed - \(reason)"
        }
    }
}
