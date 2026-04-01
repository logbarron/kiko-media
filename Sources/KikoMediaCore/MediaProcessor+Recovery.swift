import Foundation
import OSLog
import UniformTypeIdentifiers

extension MediaProcessor {
    @TaskLocal
    static var rebuildInsertFailureOverride: (@Sendable (String) -> Bool)?

    package static func withRebuildInsertFailureOverride<T>(
        _ override: @escaping @Sendable (String) -> Bool,
        operation: () throws -> T
    ) rethrows -> T {
        try $rebuildInsertFailureOverride.withValue(override) {
            try operation()
        }
    }

    package static func withRebuildInsertFailureOverride<T>(
        _ override: @escaping @Sendable (String) -> Bool,
        operation: () async throws -> T
    ) async rethrows -> T {
        try await $rebuildInsertFailureOverride.withValue(override) {
            try await operation()
        }
    }

    private func loadKnownRecoveryAsset(id: String) async throws -> Asset? {
        let retryDelaysNanos: [UInt64] = [0, 50_000_000, 200_000_000]

        for (attempt, delay) in retryDelaysNanos.enumerated() {
            if delay > 0 {
                try? await Task.sleep(nanoseconds: delay)
            }

            do {
                return try await database.getAsset(id: id)
            } catch {
                if attempt == retryDelaysNanos.count - 1 {
                    throw error
                }
                Logger.kiko.warning(
                    "Recovery: failed loading known upload \(id, privacy: .public), retrying: \(error)"
                )
            }
        }

        return nil
    }

    package func recoverIncomplete() async {
        Logger.kiko.info("Starting crash recovery scan...")

        let fm = FileManager.default
        let files: [String]
        do {
            files = try fm.contentsOfDirectory(atPath: config.uploadDir)
        } catch {
            Logger.kiko.error("Recovery: failed to list uploads directory, continuing with orphan sweep: \(error)")
            files = []
        }

        let uploadIds = files.filter { !$0.hasPrefix(".") && !$0.hasSuffix(".info") && Asset.isValidId($0) }

        if uploadIds.isEmpty {
            Logger.kiko.info("No upload files to recover")
        }

        let knownIds: Set<String>
        do {
            knownIds = try await database.getExistingIds(from: uploadIds)
        } catch {
            Logger.kiko.error("Failed to get asset IDs from database: \(error)")
            return
        }

        var recovered = 0
        for filename in uploadIds {

            let uploadId = filename  // tusd uses upload ID as filename
            let filePath = "\(config.uploadDir)/\(filename)"

            if knownIds.contains(uploadId) {
                let asset: Asset
                do {
                    guard let loadedAsset = try await loadKnownRecoveryAsset(id: uploadId) else {
                        Logger.kiko.error(
                            "Recovery: \(uploadId, privacy: .public) was reported as known but could not be loaded, marking failed"
                        )
                        _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                        continue
                    }
                    asset = loadedAsset
                } catch {
                    Logger.kiko.error(
                        "Recovery: failed loading known upload \(uploadId, privacy: .public) after retries, marking failed: \(error)"
                    )
                    _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                    continue
                }

                switch asset.status {
                case .complete, .moderated:
                    Logger.kiko.info("Recovery: \(uploadId) already complete/moderated, cleaning up upload file")
                    try? fm.removeItem(atPath: filePath)
                    try? fm.removeItem(atPath: "\(filePath).info")
                case .queued, .processing:
                    Logger.kiko.info("Recovery: \(uploadId) was interrupted, re-queuing")
                    guard let assetType = await detectAssetType(path: filePath) else {
                        Logger.kiko.error("Recovery: \(uploadId) type detection failed, marking failed")
                        _ = await updateTerminalStatusLogged(id: uploadId, status: .failed)
                        continue
                    }
                    await enqueueRecoveryJob(
                        uploadId: uploadId,
                        originalName: asset.originalName,
                        filePath: filePath,
                        assetType: assetType,
                        arrivalAtSeconds: asset.createdAt.timeIntervalSince1970
                    )
                    recovered += 1
                case .failed:
                    Logger.kiko.info("Recovery: \(uploadId) previously failed, skipping")
                }
            } else {
                Logger.kiko.info("Recovery: \(uploadId) not in database, adding as new")
                let infoPath = "\(config.uploadDir)/\(uploadId).info"

                guard let infoData = fm.contents(atPath: infoPath),
                      let info = try? JSONDecoder().decode(TusdUpload.self, from: infoData) else {
                    Logger.kiko.warning("Recovery: \(uploadId) missing or invalid .info metadata, skipping")
                    continue
                }

                if let infoId = info.id, infoId != uploadId {
                    Logger.kiko.warning("Recovery: \(uploadId) .info ID mismatch (\(infoId)), skipping")
                    continue
                }

                guard info.sizeIsDeferred != true else {
                    Logger.kiko.warning("Recovery: \(uploadId) has deferred size in .info, skipping")
                    continue
                }

                guard let expectedSize = info.size, expectedSize >= 0 else {
                    Logger.kiko.warning("Recovery: \(uploadId) missing size in .info, skipping")
                    continue
                }

                let actualSize: Int64
                do {
                    let attrs = try fm.attributesOfItem(atPath: filePath)
                    guard let sizeNumber = attrs[.size] as? NSNumber else {
                        Logger.kiko.warning("Recovery: \(uploadId) could not read file size, skipping")
                        continue
                    }
                    actualSize = sizeNumber.int64Value
                } catch {
                    Logger.kiko.warning("Recovery: \(uploadId) failed reading file size: \(error)")
                    continue
                }

                guard actualSize == expectedSize else {
                    Logger.kiko.warning("Recovery: \(uploadId) size mismatch (file=\(actualSize), info=\(expectedSize)), skipping")
                    continue
                }

                guard let assetType = await detectAssetType(path: filePath) else {
                    Logger.kiko.warning("Recovery: \(uploadId) unsupported media type, skipping")
                    continue
                }

                let rawOriginalName: String
                if let filename = info.metaData?["filename"], !filename.isEmpty {
                    rawOriginalName = filename
                } else {
                    rawOriginalName = "recovered_\(uploadId)"
                }
                let originalName = Asset.sanitizedOriginalName(rawOriginalName)

                do {
                    _ = try await database.insertQueued(id: uploadId, type: assetType, originalName: originalName)
                    await enqueueRecoveryJob(
                        uploadId: uploadId,
                        originalName: originalName,
                        filePath: filePath,
                        assetType: assetType
                    )
                    recovered += 1
                } catch {
                    Logger.kiko.error("Recovery: Failed to insert \(uploadId): \(error)")
                }
            }
        }

        do {
            let unfinishedAssets = try await database.getUnfinishedAssets()
            for asset in unfinishedAssets where asset.status == .queued {
                let uploadPath = "\(config.uploadDir)/\(asset.id)"
                guard !fm.fileExists(atPath: uploadPath) else { continue }
                Logger.kiko.error(
                    "Recovery: queued asset \(asset.id, privacy: .public) is missing upload payload, marking failed"
                )
                _ = await updateTerminalStatusLogged(id: asset.id, status: .failed)
            }
        } catch {
            Logger.kiko.error("Recovery: failed to query unfinished assets for orphan sweep: \(error)")
        }

        Logger.kiko.info("Recovery complete: \(recovered) jobs queued")
        await restartQueuesAfterRecovery()
    }

    package func verifyDerivedArtifacts() async {
        Logger.kiko.info("Verifying derived artifacts...")

        let fm = FileManager.default
        let ssdMounted = VolumeUtils.isMounted(volumeContainingPath: config.externalSSDPath)
        let moderatedIds: Set<String>?
        do {
            moderatedIds = try moderationMarkers.allMarked()
        } catch {
            Logger.kiko.error("Failed to read moderation markers; failing closed by preserving DB moderation state: \(error)")
            moderatedIds = nil
        }
        let assets: [Asset]
        do {
            assets = try await database.getVerifiableAssets()
        } catch {
            Logger.kiko.error("Failed to query assets for verification: \(error)")
            return
        }

        // Marker files are the durable moderation source of truth across DB loss/rebuild.
        // Reconcile complete/moderated DB states to markers on startup to heal crash-window drift.
        if let moderatedIds {
            let terminalStates = assets.compactMap { asset -> Database.ModerationState? in
                guard asset.status == .complete || asset.status == .moderated else {
                    return nil
                }
                return Database.ModerationState(id: asset.id, status: asset.status)
            }
            _ = await reconcileTerminalModerationState(
                terminalStates: terminalStates,
                moderatedIds: moderatedIds,
                context: "Artifact verification"
            )
        } else {
            Logger.kiko.warning("Skipping moderation reconciliation because moderation markers are unreadable")
        }

        var repairCount = 0
        for asset in assets {
            let markerStatus: Asset.AssetStatus
            if let moderatedIds {
                markerStatus = moderatedIds.contains(asset.id) ? .moderated : .complete
            } else if asset.status == .moderated {
                markerStatus = .moderated
            } else if asset.status == .complete {
                markerStatus = .complete
            } else {
                // Marker source unavailable. For stranded processing assets, restore to moderated to avoid
                // accidentally exposing content that may have been moderated.
                markerStatus = .moderated
            }

            // .processing with upload file handled by recoverIncomplete()
            if asset.status == .processing {
                let uploadPath = "\(config.uploadDir)/\(asset.id)"
                if fm.fileExists(atPath: uploadPath) {
                    continue
                }
            }

            let thumbPath = "\(config.thumbsDir)/\(asset.id).jpg"
            let previewExt = asset.type == .video ? "mp4" : "jpg"
            let previewPath = "\(config.previewsDir)/\(asset.id).\(previewExt)"

            let thumbExists = fm.fileExists(atPath: thumbPath)
            let previewExists = fm.fileExists(atPath: previewPath)
            let thumbValid = thumbExists && ImageProcessor.isImage(path: thumbPath)
            let previewValid: Bool
            if asset.type == .image {
                previewValid = previewExists && ImageProcessor.isImage(path: previewPath)
            } else {
                if previewExists {
                    previewValid = await VideoProcessor.isVideo(sourcePath: previewPath)
                } else {
                    previewValid = false
                }
            }

            if thumbValid && previewValid {
                if asset.status == .processing {
                    let restoreStatus: Asset.AssetStatus = markerStatus
                    Logger.kiko.info("Restoring stranded \(asset.id) to \(restoreStatus.rawValue)")
                    _ = await updateTerminalStatusLogged(id: asset.id, status: restoreStatus)
                }
                continue
            }

            Logger.kiko.warning("Missing or invalid derived files for \(asset.id): thumbExists=\(thumbExists), thumbValid=\(thumbValid), previewExists=\(previewExists), previewValid=\(previewValid)")
            let ext = (asset.originalName as NSString).pathExtension.lowercased()
            let ssdFilename = ext.isEmpty ? asset.id : "\(asset.id).\(ext)"
            let ssdPath = "\(config.externalSSDPath)/\(ssdFilename)"

            guard fm.fileExists(atPath: ssdPath) else {
                if ssdMounted {
                    Logger.kiko.error("Cannot repair \(asset.id): original not found on SSD")
                    _ = await updateTerminalStatusLogged(id: asset.id, status: .failed)
                } else {
                    Logger.kiko.warning("Cannot repair \(asset.id): SSD not mounted, skipping")
                }
                continue
            }

            let restoreStatus = markerStatus

            await updateStatusLogged(id: asset.id, status: .processing)
            await enqueueRecoveryJob(
                uploadId: asset.id,
                originalName: asset.originalName,
                filePath: ssdPath,
                assetType: asset.type,
                arrivalAtSeconds: asset.createdAt.timeIntervalSince1970,
                isRepair: true,
                restoreStatus: restoreStatus
            )
            repairCount += 1
        }

        Logger.kiko.info("Artifact verification complete: \(repairCount) repairs queued")

        if repairCount > 0 {
            await restartQueuesAfterRecovery()
        }
    }

    private func reconcileTerminalModerationState(
        terminalStates: [Database.ModerationState],
        moderatedIds: Set<String>,
        context: String
    ) async -> (updated: Int, failed: Int) {
        var updated = 0
        var failed = 0

        for state in terminalStates {
            let markerStatus: Asset.AssetStatus = moderatedIds.contains(state.id) ? .moderated : .complete
            guard state.status != markerStatus else { continue }

            Logger.kiko.warning(
                "\(context): reconciling moderation drift for \(state.id): db=\(state.status.rawValue), marker=\(markerStatus.rawValue)"
            )
            if await updateTerminalStatusLogged(id: state.id, status: markerStatus) {
                updated += 1
            } else {
                failed += 1
            }
        }

        if updated > 0 {
            Logger.kiko.info("\(context): moderation reconciliation complete: \(updated) assets updated")
        }
        if failed > 0 {
            Logger.kiko.error("\(context): moderation reconciliation failed for \(failed) assets; moderation state may be stale")
        }

        return (updated: updated, failed: failed)
    }

    package func rebuildFromSSD() async {
        Logger.kiko.info("Starting database rebuild from SSD...")

        let fm = FileManager.default
        let ssdPath = config.externalSSDPath

        let mountCheck = Self.rebuildMountCheckOverride
        let ssdMounted = mountCheck?(config.externalSSDPath) ?? VolumeUtils.isMounted(volumeContainingPath: config.externalSSDPath)
        guard ssdMounted else {
            Logger.kiko.error("Rebuild failed: SSD not mounted at \(ssdPath)")
            return
        }

        guard let fileEnumerator = fm.enumerator(
            at: URL(fileURLWithPath: ssdPath, isDirectory: true),
            includingPropertiesForKeys: nil,
            options: [.skipsSubdirectoryDescendants],
            errorHandler: { url, error in
                Logger.kiko.warning("Rebuild: skipping unreadable path \(url.path): \(error.localizedDescription)")
                return true
            }
        ) else {
            Logger.kiko.error("Rebuild failed: cannot enumerate SSD directory")
            return
        }

        let moderatedIds: Set<String>
        do {
            moderatedIds = try moderationMarkers.allMarked()
        } catch {
            Logger.kiko.error("Rebuild failed: cannot read moderation markers (failing closed): \(error)")
            return
        }
        Logger.kiko.info("Rebuild: \(moderatedIds.count) moderated assets found in markers")

        typealias RebuildResult = (id: String, originalName: String, assetType: Asset.AssetType, timestamp: String)
        let rebuildParallelism = config.maxConcurrentRebuildProbes

        let rebuildStats = await withTaskGroup(
            of: RebuildResult?.self,
            returning: (inserted: Int, skipped: Int, failed: Int).self
        ) { group in
            let rebuildObserver = Self.rebuildProbeConcurrencyObserver
            var inFlight = 0
            var inserted = 0
            var skipped = 0
            var failed = 0

            func insertRebuildResult(_ rebuildResult: RebuildResult) async -> (inserted: Int, skipped: Int, failed: Int) {
                if let shouldFailInsert = Self.rebuildInsertFailureOverride,
                   shouldFailInsert(rebuildResult.id) {
                    Logger.kiko.error("Rebuild: forced insert failure for \(rebuildResult.id)")
                    return (inserted: 0, skipped: 0, failed: 1)
                }

                let status: Asset.AssetStatus = moderatedIds.contains(rebuildResult.id) ? .moderated : .complete
                do {
                    let wasInserted = try await database.insertComplete(
                        id: rebuildResult.id,
                        type: rebuildResult.assetType,
                        timestamp: rebuildResult.timestamp,
                        originalName: rebuildResult.originalName,
                        status: status
                    )
                    return wasInserted ? (inserted: 1, skipped: 0, failed: 0) : (inserted: 0, skipped: 1, failed: 0)
                } catch {
                    Logger.kiko.error("Rebuild: failed to insert \(rebuildResult.id): \(error)")
                    return (inserted: 0, skipped: 0, failed: 1)
                }
            }

            while let fileURL = fileEnumerator.nextObject() as? URL {
                let filename = fileURL.lastPathComponent
                guard !filename.hasPrefix(".") else { continue }

                let fullPath = fileURL.path
                group.addTask {
                    let fm = FileManager.default
                    var isDirectory: ObjCBool = false
                    guard fm.fileExists(atPath: fullPath, isDirectory: &isDirectory),
                          !isDirectory.boolValue else {
                        return nil
                    }

                    guard let candidate = await Self.rebuildCandidate(fullPath: fullPath) else {
                        return nil
                    }

                    let timestamp = await Self.rebuildTimestamp(
                        fullPath: fullPath,
                        assetType: candidate.assetType
                    )
                    return (
                        id: candidate.id,
                        originalName: candidate.originalName,
                        assetType: candidate.assetType,
                        timestamp: timestamp
                    )
                }
                inFlight += 1
                Self.emitRebuildProbeConcurrencyEvent(
                    phase: .scheduled,
                    inFlight: inFlight,
                    cap: rebuildParallelism,
                    observer: rebuildObserver
                )

                if inFlight >= rebuildParallelism {
                    if let completedTaskResult = await group.next(),
                       let rebuildResult = completedTaskResult {
                        let delta = await insertRebuildResult(rebuildResult)
                        inserted += delta.inserted
                        skipped += delta.skipped
                        failed += delta.failed
                    }
                    inFlight -= 1
                    Self.emitRebuildProbeConcurrencyEvent(
                        phase: .completed,
                        inFlight: inFlight,
                        cap: rebuildParallelism,
                        observer: rebuildObserver
                    )
                }
            }

            while inFlight > 0 {
                if let completedTaskResult = await group.next(),
                   let rebuildResult = completedTaskResult {
                    let delta = await insertRebuildResult(rebuildResult)
                    inserted += delta.inserted
                    skipped += delta.skipped
                    failed += delta.failed
                }
                inFlight -= 1
                Self.emitRebuildProbeConcurrencyEvent(
                    phase: .completed,
                    inFlight: inFlight,
                    cap: rebuildParallelism,
                    observer: rebuildObserver
                )
            }

            return (inserted: inserted, skipped: skipped, failed: failed)
        }

        let terminalStates: [Database.ModerationState]
        let existingAssetIds: Set<String>
        do {
            terminalStates = try await database.getTerminalModerationStates()
            existingAssetIds = try await database.getAllAssetIds()
        } catch {
            Logger.kiko.error("Rebuild: failed to query moderation reconciliation state: \(error)")
            Logger.kiko.info(
                "Rebuild complete: \(rebuildStats.inserted) inserted, \(rebuildStats.skipped) skipped, \(rebuildStats.failed) failed"
            )
            return
        }

        let reconciliation = await reconcileTerminalModerationState(
            terminalStates: terminalStates,
            moderatedIds: moderatedIds,
            context: "Rebuild"
        )

        let markersToKeep = moderatedIds.intersection(existingAssetIds)
        if rebuildStats.failed > 0 {
            Logger.kiko.warning(
                "Rebuild: skipping stale moderation marker pruning due to \(rebuildStats.failed) rebuild insert failures (fail-closed)"
            )
        } else {
            do {
                let markerPruneResult = try moderationMarkers.pruneUntracked(keeping: markersToKeep)
                if markerPruneResult.removed > 0 {
                    Logger.kiko.info("Rebuild: pruned \(markerPruneResult.removed) stale moderation markers")
                }
                if markerPruneResult.failed > 0 {
                    Logger.kiko.error(
                        "Rebuild: failed to prune \(markerPruneResult.failed) stale moderation markers; marker/DB mismatch may remain"
                    )
                }
            } catch {
                Logger.kiko.error("Rebuild: stale moderation marker pruning failed: \(error)")
            }
        }

        Logger.kiko.info(
            "Rebuild complete: \(rebuildStats.inserted) inserted, \(rebuildStats.skipped) skipped, \(rebuildStats.failed) failed, \(reconciliation.updated) reconciled, \(reconciliation.failed) reconciliation failures"
        )
    }

    package static func rebuildCandidate(fullPath: String) async -> (id: String, originalName: String, assetType: Asset.AssetType)? {
        let fileURL = URL(fileURLWithPath: fullPath)
        let id = fileURL.deletingPathExtension().lastPathComponent
        guard Asset.isValidId(id) else {
            return nil
        }

        let ext = fileURL.pathExtension.lowercased()
        guard ext.isEmpty || isAllowedRebuildExtension(ext) else {
            return nil
        }

        if ImageProcessor.isImage(path: fullPath) {
            return (id: id, originalName: fileURL.lastPathComponent, assetType: .image)
        }

        guard await VideoProcessor.isVideo(sourcePath: fullPath) else {
            return nil
        }

        return (id: id, originalName: fileURL.lastPathComponent, assetType: .video)
    }

    private static func isAllowedRebuildExtension(_ ext: String) -> Bool {
        guard let type = UTType(filenameExtension: ext) else {
            return false
        }
        return type.conforms(to: .image) || type.conforms(to: .movie)
    }

    private static func rebuildTimestamp(fullPath: String, assetType: Asset.AssetType) async -> String {
        if assetType == .image {
            return ImageProcessor.extractTimestamp(sourcePath: fullPath)
                ?? fileModificationTime(fullPath)
                ?? DateUtils.exifTimestamp(from: Date())
        }

        return await VideoProcessor.extractTimestamp(sourcePath: fullPath)
            ?? fileModificationTime(fullPath)
            ?? DateUtils.exifTimestamp(from: Date())
    }

    private static func fileModificationTime(_ path: String) -> String? {
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: path),
              let modDate = attrs[.modificationDate] as? Date else {
            return nil
        }
        return DateUtils.exifTimestamp(from: modDate)
    }
}
