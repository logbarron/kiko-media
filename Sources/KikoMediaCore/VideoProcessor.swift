import Foundation
import AVFoundation

private struct SendableSession: @unchecked Sendable {
    let session: AVAssetExportSession
}

package struct VideoRuntimeProbeResult: Sendable {
    package let runtimeEstimateSeconds: Double
    package let frameCount: Double
    package let durationSeconds: Double
}

package enum VideoProcessor {
    private static let runtimeEstimateFallbackFPS = 24.0
    private static let runtimeEstimateBaselineFPS = 30.0
    private static let runtimeEstimateMinScale = 0.5
    private static let runtimeEstimateMaxScale = 2.0

    private enum ExportRaceResult {
        case exportCompleted(Result<Void, Error>)
        case timeoutElapsed
        case cancelled
    }

    private static func makeAsset(url: URL, mimeType: String?) -> AVURLAsset {
        if let mimeType {
            return AVURLAsset(url: url, options: [AVURLAssetOverrideMIMETypeKey: mimeType])
        }
        return AVURLAsset(url: url)
    }

    package static func generateThumbnail(
        sourcePath: String,
        outputPath: String,
        size: Int,
        time captureTime: Double,
        quality: Double,
        mimeType: String? = nil
    ) async throws {
        let sourceURL = URL(fileURLWithPath: sourcePath)
        let asset = makeAsset(url: sourceURL, mimeType: mimeType)

        let generator = AVAssetImageGenerator(asset: asset)
        generator.appliesPreferredTrackTransform = true
        generator.maximumSize = CGSize(width: size, height: size)

        let duration = try await asset.load(.duration)
        let time = CMTime(seconds: min(captureTime, duration.seconds), preferredTimescale: 600)

        let cgImage = try await generator.image(at: time).image

        try autoreleasepool {
            try ImageUtils.saveAsJPEG(image: cgImage, path: outputPath, quality: quality)
        }
    }

    package static func transcode(sourcePath: String, outputPath: String, timeoutSeconds: Int, preset: String, mimeType: String? = nil) async throws {
        let sourceURL = URL(fileURLWithPath: sourcePath)
        let outputURL = URL(fileURLWithPath: outputPath)

        try FileManager.default.createDirectory(
            at: outputURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        try? FileManager.default.removeItem(at: outputURL)

        let asset = makeAsset(url: sourceURL, mimeType: mimeType)

        let videoTracks = try await asset.loadTracks(withMediaType: .video)
        guard !videoTracks.isEmpty else {
            throw VideoProcessorError.noVideoTrack
        }

        guard let session = AVAssetExportSession(
            asset: asset,
            presetName: preset
        ) else {
            throw VideoProcessorError.cannotCreateExportSession
        }

        session.shouldOptimizeForNetworkUse = true

        let sendable = SendableSession(session: session)

        do {
            try await runExportWithTimeout(
                timeoutSeconds: timeoutSeconds,
                export: {
                    try await sendable.session.export(to: outputURL, as: .mp4)
                },
                cancelExport: {
                    sendable.session.cancelExport()
                }
            )
        } catch {
            try? FileManager.default.removeItem(at: outputURL)
            throw error
        }
    }

    package static func runExportWithTimeout(
        timeoutSeconds: Int,
        export: @Sendable @escaping () async throws -> Void,
        cancelExport: @Sendable @escaping () -> Void
    ) async throws {
        try await withThrowingTaskGroup(of: ExportRaceResult.self) { group in
            group.addTask {
                do {
                    try await export()
                    return .exportCompleted(.success(()))
                } catch {
                    return .exportCompleted(.failure(error))
                }
            }

            group.addTask {
                do {
                    try await Task.sleep(for: .seconds(timeoutSeconds))
                    return .timeoutElapsed
                } catch is CancellationError {
                    return .cancelled
                }
            }

            var didTimeout = false

            while let result = try await group.next() {
                switch result {
                case let .exportCompleted(exportResult):
                    group.cancelAll()

                    if didTimeout {
                        switch exportResult {
                        case let .failure(error) where !(error is CancellationError):
                            throw error
                        default:
                            throw VideoProcessorError.timeout
                        }
                    }

                    try exportResult.get()
                    return

                case .timeoutElapsed:
                    guard !didTimeout else { continue }
                    didTimeout = true
                    cancelExport()

                case .cancelled:
                    continue
                }
            }

            if didTimeout {
                throw VideoProcessorError.timeout
            }
        }
    }

    package static func isVideo(sourcePath: String, mimeType: String? = nil) async -> Bool {
        let sourceURL = URL(fileURLWithPath: sourcePath)
        let asset = makeAsset(url: sourceURL, mimeType: mimeType)
        guard let videoTracks = try? await asset.loadTracks(withMediaType: .video) else {
            return false
        }
        return !videoTracks.isEmpty
    }

    package static func probeRuntimeEstimate(sourcePath: String, mimeType: String? = nil) async -> VideoRuntimeProbeResult? {
        let sourceURL = URL(fileURLWithPath: sourcePath)
        let asset = makeAsset(url: sourceURL, mimeType: mimeType)

        guard let duration = try? await asset.load(.duration).seconds,
              duration.isFinite,
              duration > 0 else {
            return nil
        }
        guard let track = try? await asset.loadTracks(withMediaType: .video).first else {
            return nil
        }

        let nominalFPS = (try? await track.load(.nominalFrameRate)).map(Double.init)
        let fps = resolvedRuntimeEstimateFPS(nominalFPS)
        let runtimeScale = runtimeEstimateScaleForHardwareEncoder(fps: fps)
        let runtimeEstimateSeconds = max(0.1, duration * runtimeScale)
        let frameCount = duration * fps
        guard frameCount.isFinite, frameCount > 0 else {
            return nil
        }
        return VideoRuntimeProbeResult(
            runtimeEstimateSeconds: runtimeEstimateSeconds,
            frameCount: frameCount,
            durationSeconds: duration
        )
    }

    package static func probeRuntimeEstimateSeconds(sourcePath: String, mimeType: String? = nil) async -> Double? {
        await probeRuntimeEstimate(sourcePath: sourcePath, mimeType: mimeType)?.runtimeEstimateSeconds
    }

    package static func runtimeEstimateScaleForHardwareEncoder(fps: Double) -> Double {
        let safeFPS = fps.isFinite && fps > 0 ? fps : runtimeEstimateFallbackFPS
        return max(
            runtimeEstimateMinScale,
            min(runtimeEstimateMaxScale, safeFPS / runtimeEstimateBaselineFPS)
        )
    }

    package static func extractTimestamp(sourcePath: String, mimeType: String? = nil) async -> String? {
        let sourceURL = URL(fileURLWithPath: sourcePath)
        let asset = makeAsset(url: sourceURL, mimeType: mimeType)

        var date: Date?

        if let creationDate = try? await asset.load(.creationDate),
           let d = try? await creationDate.load(.dateValue) {
            date = d
        }

        if date == nil, let metadata = try? await asset.load(.metadata) {
            for item in metadata {
                if let key = item.commonKey, key == .commonKeyCreationDate,
                   let d = try? await item.load(.dateValue) {
                    date = d
                    break
                }
            }
        }

        guard let date else { return nil }

        return DateUtils.exifTimestamp(from: date)
    }

    private static func resolvedRuntimeEstimateFPS(_ nominalFPS: Double?) -> Double {
        guard let nominalFPS, nominalFPS.isFinite, nominalFPS > 0 else {
            return runtimeEstimateFallbackFPS
        }
        return nominalFPS
    }
}

package enum VideoProcessorError: Error {
    case noVideoTrack
    case cannotCreateExportSession
    case timeout
}
