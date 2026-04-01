import Foundation
import OSLog
import UniformTypeIdentifiers

extension MediaProcessor {
    private static let completionPersistenceStartupRetryLimit = 1

    func process(job: ProcessingJob) async {
        await Self.$processingTopologyContext.withValue(UUID()) {
            await self.processWithTopologyContext(job: job)
        }
    }

    private func processWithTopologyContext(job: ProcessingJob) async {
        let uploadId = job.uploadId
        let filePath = job.filePath
        Logger.kiko.info("Processing \(uploadId): \(job.originalName)")

        await updateStatusLogged(id: uploadId, status: .processing)

        let assetType = job.assetType
        do {
            try await database.updateType(id: uploadId, type: assetType)
        } catch {
            Logger.kiko.error("DB type update failed for \(uploadId): \(error)")
            // Do not continue with stale DB metadata; leave state for startup recovery.
            return
        }
        guard !cancelledWithCleanup(uploadId: uploadId, assetType: assetType) else { return }

        let timestamp: String
        let thumbnailSuccess: Bool

        switch assetType {
        case .image:
            let previewSuccess: Bool
            (thumbnailSuccess, previewSuccess, timestamp) = await processImage(
                uploadId: uploadId,
                filePath: filePath
            )
            guard previewSuccess else {
                guard !cancelledWithCleanup(uploadId: uploadId, assetType: assetType) else { return }
                Logger.kiko.error("Image preview failed for \(uploadId)")
                cleanupDerivedFiles(uploadId: uploadId, assetType: assetType)
                _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                return
            }
        case .video:
            let mimeType: String?
            if URL(fileURLWithPath: filePath).pathExtension.isEmpty {
                let ext = (job.originalName as NSString).pathExtension.lowercased()
                mimeType = ext.isEmpty ? nil : UTType(filenameExtension: ext)?.preferredMIMEType
            } else {
                mimeType = nil
            }
            guard let resolvedFrameCount = Self.dispatchFrameCount(for: job),
                  resolvedFrameCount.isFinite,
                  resolvedFrameCount > 0 else {
                Logger.kiko.error("Missing resolved video cost for \(uploadId)")
                cleanupDerivedFiles(uploadId: uploadId, assetType: assetType)
                _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                return
            }
            if let resolvedVideoCost = job.resolvedVideoCost {
                Self.videoDispatchObserver?(uploadId, resolvedFrameCount, resolvedVideoCost)
            }
            let routingDirective = selectedVideoRoutingDirective(for: uploadId)
            let videoResult = await processVideo(
                uploadId: uploadId,
                filePath: filePath,
                originalName: job.originalName,
                mimeType: mimeType,
                routingDirective: routingDirective,
                frameCount: resolvedFrameCount
            )
            thumbnailSuccess = videoResult.thumb
            let previewSuccess = videoResult.preview
            timestamp = videoResult.timestamp
            guard previewSuccess else {
                guard !cancelledWithCleanup(uploadId: uploadId, assetType: assetType) else { return }
                if videoResult.transientRemoteRetry {
                    await requeueForTransientRemoteFailure(job: job)
                    return
                }
                Logger.kiko.error("Video transcode failed for \(uploadId)")
                cleanupDerivedFiles(uploadId: uploadId, assetType: assetType)
                _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                return
            }
        }

        guard thumbnailSuccess else {
            guard !cancelledWithCleanup(uploadId: uploadId, assetType: assetType) else { return }
            Logger.kiko.error("Thumbnail generation failed for \(uploadId)")
            cleanupDerivedFiles(uploadId: uploadId, assetType: assetType)
            _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
            return
        }

        guard !cancelledWithCleanup(uploadId: uploadId, assetType: assetType) else { return }

        if !job.isRepair {
            let archiveOriginal = self.archiveOriginal
            let originalName = job.originalName
            let launchObserver = Self.processingLaunchObserver
            let archiveResult = await Task.detached(priority: .userInitiated) {
                Self.emitProcessingLaunchEvent(.archiveDetached, observer: launchObserver)
                return await archiveOriginal(filePath, uploadId, originalName)
            }.value
            Logger.kiko.info("Archive result for \(uploadId): \(archiveResult.logMessage)")

            guard archiveResult.isSafelyStored else {
                if case .failed = archiveResult {
                    Logger.kiko.error("Failed to archive \(uploadId) - needs manual review")
                    cleanupDerivedFiles(uploadId: uploadId, assetType: assetType)
                    _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                } else if case .checksumMismatch = archiveResult {
                    Logger.kiko.error("Checksum mismatch for \(uploadId) - discarded SSD copy, keeping upload for review")
                    _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                } else if case .verificationFailed = archiveResult {
                    Logger.kiko.error("Verification failed for \(uploadId) - discarded SSD copy, keeping upload for review")
                    _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                } else {
                    Logger.kiko.warning("Archive pending for \(uploadId) - will retry on restart")
                }
                return
            }
        }

        guard !cancelledWithCleanup(uploadId: uploadId, assetType: assetType) else { return }

        if let restoreStatus = job.restoreStatus {
            guard await updateTerminalStatusLogged(id: uploadId, status: restoreStatus) else {
                return
            }
            Logger.kiko.info("Repaired \(uploadId), restored status to \(restoreStatus.rawValue)")
        } else {
            guard await markCompleteTerminalLogged(id: uploadId, timestamp: timestamp) else {
                await handleCompletionPersistenceFailure(job: job)
                return
            }
            do {
                let asset = try await database.getAsset(id: uploadId)
                recordSchedulingSuccessIfNeeded(uploadId: uploadId, asset: asset)
                Logger.kiko.info("Completed \(uploadId)")
            } catch {
                Logger.kiko.error("Completed \(uploadId) but failed to load asset for metrics: \(error)")
            }
        }

        if !job.isRepair {
            try? FileManager.default.removeItem(atPath: filePath)
            try? FileManager.default.removeItem(atPath: "\(filePath).info")
        }
    }

    private func requeueForTransientRemoteFailure(job: ProcessingJob) async {
        cleanupDerivedFiles(uploadId: job.uploadId, assetType: job.assetType)
        guard await updateStatusLogged(id: job.uploadId, status: .queued) else {
            clearTransientRemoteExclusion(uploadId: job.uploadId)
            Logger.kiko.error(
                "Failed to persist queued status for \(job.uploadId, privacy: .public); aborting transient requeue"
            )
            return
        }
        // Defer enqueue until the original task reaches jobCompleted and clears activeJobs.
        deferTransientRequeue(job)
        Logger.kiko.info(
            "Deferred transient requeue for \(job.uploadId, privacy: .public) until in-flight slot clears"
        )
    }

    private func handleCompletionPersistenceFailure(job: ProcessingJob) async {
        guard !job.isRepair else {
            Logger.kiko.error(
                "Completion persistence failed for repair \(job.uploadId, privacy: .public); retaining source for manual review"
            )
            return
        }

        do {
            let retryCount = try await database.reEnqueueForRetry(id: job.uploadId)
            let retryLimit = Self.completionPersistenceStartupRetryLimit
            if retryCount <= retryLimit {
                Logger.kiko.warning(
                    "Completion persistence failed for \(job.uploadId, privacy: .public); retained upload for startup retry (\(retryCount)/\(retryLimit))"
                )
                return
            }

            Logger.kiko.error(
                "Completion persistence failed for \(job.uploadId, privacy: .public) and exceeded startup retry limit (\(retryCount) > \(retryLimit)); marking failed and retaining upload for manual review"
            )
            guard await updateTerminalStatusLogged(id: job.uploadId, status: .failed) else {
                Logger.kiko.error(
                    "Failed to persist terminal failed status after completion retry exhaustion for \(job.uploadId, privacy: .public); upload retained in queued state for manual intervention"
                )
                return
            }
        } catch {
            Logger.kiko.error(
                "Completion persistence failed for \(job.uploadId, privacy: .public) and queued retry persistence failed: \(error)"
            )
        }
    }

    private func processImage(uploadId: String, filePath: String) async -> (thumb: Bool, preview: Bool, timestamp: String) {
        let thumbPath = "\(config.thumbsDir)/\(uploadId).jpg"
        let previewPath = "\(config.previewsDir)/\(uploadId).jpg"
        let thumbSize = config.thumbnailSize
        let thumbQuality = config.thumbnailQuality
        let prevSize = config.previewSize
        let prevQuality = config.previewQuality
        let maxPx = config.maxImagePixels
        let maxDim = config.maxImageDimension
        let maxRatio = config.maxCompressionRatio

        async let thumbResult: Bool = {
            Self.emitProcessingLaunchEvent(.imageThumbnail)
            do {
                try ImageProcessor.generateThumbnail(
                    sourcePath: filePath, outputPath: thumbPath,
                    size: thumbSize, quality: thumbQuality,
                    maxPixels: maxPx, maxDimension: maxDim, maxCompressionRatio: maxRatio
                )
                return true
            } catch {
                Logger.kiko.error("Thumbnail failed for \(uploadId): \(error)")
                return false
            }
        }()

        async let previewResult: Bool = {
            Self.emitProcessingLaunchEvent(.imagePreview)
            do {
                try ImageProcessor.generatePreview(
                    sourcePath: filePath, outputPath: previewPath,
                    size: prevSize, quality: prevQuality,
                    maxPixels: maxPx, maxDimension: maxDim, maxCompressionRatio: maxRatio
                )
                return true
            } catch {
                Logger.kiko.error("Preview failed for \(uploadId): \(error)")
                return false
            }
        }()

        async let timestampResult: String? = {
            Self.emitProcessingLaunchEvent(.imageTimestamp)
            return ImageProcessor.extractTimestamp(sourcePath: filePath)
        }()

        let (thumbSuccess, previewSuccess, ts) = await (thumbResult, previewResult, timestampResult)
        return (thumbSuccess, previewSuccess, ts ?? currentTimestamp())
    }

    private func processVideo(
        uploadId: String,
        filePath: String,
        originalName: String,
        mimeType: String? = nil,
        routingDirective: VideoRoutingDirective,
        frameCount: Double
    ) async -> (thumb: Bool, preview: Bool, timestamp: String, transientRemoteRetry: Bool) {
        let thumbPath = "\(config.thumbsDir)/\(uploadId).jpg"
        let previewPath = "\(config.previewsDir)/\(uploadId).mp4"
        let vtSize = config.videoThumbnailSize
        let vtTime = config.videoThumbnailTime
        let vtQuality = config.videoThumbnailQuality
        let vtTimeout = config.videoTranscodeTimeout
        let vtPreset = config.videoTranscodePreset

        if case .remote(let workerIndex, let slotIndex, _) = routingDirective,
           remoteVideoDispatchOverride != nil || thunderboltDispatcher != nil {
            beginRemoteVideoDispatch(uploadId: uploadId)
            defer { endRemoteVideoDispatch(uploadId: uploadId) }

            let timestampTask = Task<String?, Never>(priority: .userInitiated) {
                Self.emitProcessingLaunchEvent(.videoTimestamp)
                return await VideoProcessor.extractTimestamp(sourcePath: filePath, mimeType: mimeType)
            }

            if let override = remoteVideoDispatchOverride {
                if let result = await override(uploadId, filePath, originalName, mimeType) {
                    applyRemoteDispatchOutcome(
                        uploadId: uploadId,
                        routingDirective: routingDirective,
                        outcome: .success
                    )
                    let ts = await timestampTask.value
                    return (result.thumb, result.preview, ts ?? currentTimestamp(), false)
                }
            } else if let dispatcher = thunderboltDispatcher {
                let executionSampleModel = await remoteExecutionSampleModelForDispatch(
                    workerIndex: workerIndex
                )
                let dispatchResult = await dispatcher.dispatch(
                    uploadId: uploadId,
                    filePath: filePath,
                    originalName: originalName,
                    mimeType: mimeType,
                    targetWorkerIndex: workerIndex,
                    targetSlotIndex: slotIndex,
                    frameCount: frameCount,
                    successfulExecutionSampleModel: executionSampleModel
                )
                switch dispatchResult {
                case .success:
                    applyRemoteDispatchOutcome(
                        uploadId: uploadId,
                        routingDirective: routingDirective,
                        outcome: .success
                    )
                    let ts = await timestampTask.value
                    return (true, true, ts ?? currentTimestamp(), false)
                case .transientRetry(let slotHealthDown):
                    applyRemoteDispatchOutcome(
                        uploadId: uploadId,
                        routingDirective: routingDirective,
                        outcome: .transientRetry(slotHealthDown: slotHealthDown)
                    )
                    timestampTask.cancel()
                    return (false, false, currentTimestamp(), true)
                case .permanentFailure:
                    applyRemoteDispatchOutcome(
                        uploadId: uploadId,
                        routingDirective: routingDirective,
                        outcome: .permanentFailure
                    )
                    break
                case .fallbackLocal:
                    applyRemoteDispatchOutcome(
                        uploadId: uploadId,
                        routingDirective: routingDirective,
                        outcome: .fallbackLocal
                    )
                    break
                }
            }

            timestampTask.cancel()
            applyRemoteDispatchOutcome(
                uploadId: uploadId,
                routingDirective: routingDirective,
                outcome: .unavailable
            )
            Logger.kiko.info("Thunderbolt dispatch failed/unavailable for \(uploadId, privacy: .public), processing locally")
        } else {
            applyRemoteDispatchOutcome(
                uploadId: uploadId,
                routingDirective: routingDirective,
                outcome: .fallbackLocal
            )
        }

        markLocalVideoRuntimeStart(uploadId: uploadId)

        if let localOverride = localVideoProcessingOverride {
            let result = await localOverride(uploadId, filePath, mimeType)
            return (result.thumb, result.preview, result.timestamp, false)
        }

        let localProcessStartNanos = DispatchTime.now().uptimeNanoseconds

        async let thumbResult: Bool = {
            Self.emitProcessingLaunchEvent(.videoThumbnail)
            do {
                try await VideoProcessor.generateThumbnail(
                    sourcePath: filePath, outputPath: thumbPath,
                    size: vtSize, time: vtTime, quality: vtQuality,
                    mimeType: mimeType
                )
                return true
            } catch {
                Logger.kiko.error("Video thumbnail failed for \(uploadId): \(error)")
                return false
            }
        }()

        async let previewResult: Bool = {
            Self.emitProcessingLaunchEvent(.videoPreview)
            do {
                try await VideoProcessor.transcode(
                    sourcePath: filePath, outputPath: previewPath,
                    timeoutSeconds: vtTimeout, preset: vtPreset,
                    mimeType: mimeType
                )
                return true
            } catch {
                Logger.kiko.error("Video transcode failed for \(uploadId): \(error)")
                return false
            }
        }()

        async let timestampResult: String? = {
            Self.emitProcessingLaunchEvent(.videoTimestamp)
            return await VideoProcessor.extractTimestamp(sourcePath: filePath, mimeType: mimeType)
        }()

        let (thumbSuccess, previewSuccess) = await (thumbResult, previewResult)
        let localProcessEndNanos = DispatchTime.now().uptimeNanoseconds
        let localProcessNanos = localProcessEndNanos >= localProcessStartNanos
            ? localProcessEndNanos - localProcessStartNanos
            : 0
        let ts = await timestampResult
        if thumbSuccess, previewSuccess, frameCount.isFinite, frameCount > 0 {
            updateLocalLiveMSPerFrame(processNanos: localProcessNanos, frameCount: frameCount)
        }
        return (thumbSuccess, previewSuccess, ts ?? currentTimestamp(), false)
    }
}
