import Darwin
import Foundation
import Hummingbird
import KikoMediaCore
import OSLog
import ServiceLifecycle
import UnixSignals

private enum RuntimeHealthError: Error {
    case watchdogDatabaseHealthCheckFailed
}

package struct KikoMediaAppRuntime {
    package static func run() async throws {
        let config: Config
        do {
            config = try Config.load()
        } catch let error as VideoTranscodePresetConfigurationError {
            Logger.kiko.error("Refusing startup: \(error.localizedDescription)")
            throw error
        } catch let error as VideoSchedulerPolicyConfigurationError {
            Logger.kiko.error("Refusing startup: \(error.localizedDescription)")
            throw error
        } catch {
            Logger.kiko.error("Refusing startup due to configuration load error: \(error.localizedDescription)")
            throw error
        }
        DateUtils.configure(eventTimezone: config.eventTimezone)
        try ensureDirectories(config: config)

        let (database, needsRebuild) = try await initializeDatabase(config: config)

        let moderationMarkers = ModerationMarkers(
            baseDir: URL(fileURLWithPath: config.moderatedDir)
        )

        let workers = config.thunderboltWorkers
        func makeThunderboltDispatcher(
            complexityAwareSchedulingEnabled: Bool,
            benchmarkPriorTable: BenchmarkPriorTable = BenchmarkPriorTable()
        ) -> ThunderboltDispatcher {
            ThunderboltDispatcher(
                workers: workers,
                port: config.tbPort,
                connectTimeout: config.tbConnectTimeout,
                thumbsDir: config.thumbsDir,
                previewsDir: config.previewsDir,
                sha256BufferSize: config.sha256BufferSize,
                complexityAwareSchedulingEnabled: complexityAwareSchedulingEnabled,
                benchmarkPriorTable: benchmarkPriorTable,
                videoTranscodePreset: config.videoTranscodePreset,
                onRetryIncrement: { uploadId in
                    do {
                        return try await database.incrementRetryCount(id: uploadId)
                    } catch {
                        Logger.kiko.error(
                            "Failed to persist retryCount increment for \(uploadId, privacy: .public): \(error.localizedDescription)"
                        )
                        return nil
                    }
                },
                onRetrySeed: { uploadId in
                    do {
                        return max(0, try await database.getAsset(id: uploadId)?.retryCount ?? 0)
                    } catch {
                        Logger.kiko.error(
                            "Failed to load durable retryCount seed for \(uploadId, privacy: .public): \(error.localizedDescription)"
                        )
                        return 0
                    }
                }
            )
        }

        let thunderboltDispatcher: ThunderboltDispatcher?
        let benchmarkPriorTable: BenchmarkPriorTable?
        let benchmarkPriorArtifactState: CAActivationGate.PriorArtifactState?
        let complexityAwareSchedulingEnabled: Bool

        switch config.videoSchedulerPolicy {
        case .auto:
            let (loadedPriorTable, priorLoadResult) = loadBenchmarkPriorTable(config: config)
            let loadedPriorArtifactState = priorArtifactState(from: priorLoadResult)
            let localPriorProfile = CAActivationGate.resolveLocalPriorProfile(
                priorTable: loadedPriorTable,
                videoTranscodePreset: config.videoTranscodePreset
            )
            var activationDecision = CAActivationGate.evaluate(
                workersPresent: !workers.isEmpty,
                priorArtifactState: loadedPriorArtifactState,
                localPriorProfile: localPriorProfile
            )
            benchmarkPriorTable = loadedPriorTable
            benchmarkPriorArtifactState = loadedPriorArtifactState

            if !workers.isEmpty {
                if let bridgeIP = firstBridgeIPv4() {
                    if case .disabled(.missingPriorArtifact) = activationDecision {
                        Logger.kiko.warning(
                            "Complexity-aware scheduling disabled: benchmark-prior.json v2 not found; running FIFO policy"
                        )
                    } else if case .disabled(.invalidPriorArtifact) = activationDecision {
                        switch priorLoadResult {
                        case .invalid:
                            Logger.kiko.warning(
                                "Complexity-aware scheduling disabled: benchmark-prior.json is invalid or unreadable; running FIFO policy"
                            )
                        case .unsupportedVersion(let version):
                            Logger.kiko.warning(
                                "Complexity-aware scheduling disabled: benchmark-prior.json version \(version, privacy: .public) is unsupported; regenerate it and rerun FIFO policy"
                            )
                        case .missing, .loaded:
                            break
                        }
                    } else if case .disabled(.invalidLocalPrior) = activationDecision {
                        Logger.kiko.warning(
                            "Complexity-aware scheduling disabled: local machine prior entry missing/invalid; running FIFO policy"
                        )
                    }
                    if activationDecision.isEnabled {
                        let missingTickV2Workers = workersMissingTickV2(
                            workers: workers,
                            port: config.tbPort,
                            connectTimeoutMS: config.tbConnectTimeout
                        )
                        if !missingTickV2Workers.isEmpty {
                            activationDecision = CAActivationGate.evaluate(
                                workersPresent: !workers.isEmpty,
                                priorArtifactState: loadedPriorArtifactState,
                                localPriorProfile: localPriorProfile,
                                strictTickV2Accepted: false
                            )
                            Logger.kiko.warning(
                                "Complexity-aware scheduling disabled: all TB workers must be source-route reachable and report tick_version=2; failing workers: \(missingTickV2Workers.joined(separator: ","), privacy: .public)"
                            )
                        }
                    }
                    thunderboltDispatcher = makeThunderboltDispatcher(
                        complexityAwareSchedulingEnabled: activationDecision.isEnabled,
                        benchmarkPriorTable: loadedPriorTable ?? BenchmarkPriorTable()
                    )
                    let totalSlots = workers.reduce(0) { $0 + $1.slots }
                    Logger.kiko.info(
                        "Thunderbolt offload enabled: \(workers.count) worker(s), \(totalSlots) remote slot(s), bridge IP: \(bridgeIP)"
                    )
                    if activationDecision.isEnabled {
                        Logger.kiko.info("Complexity-aware scheduling enabled via TB_WORKERS + prior v2 + strict tick v2 gating")
                    }
                } else {
                    Logger.kiko.warning("TB_WORKERS configured but no bridge interface found — Thunderbolt offload disabled; complexity-aware scheduling disabled")
                    thunderboltDispatcher = nil
                }
            } else {
                thunderboltDispatcher = nil
            }

            complexityAwareSchedulingEnabled = thunderboltDispatcher != nil && activationDecision.isEnabled

        case .fifo:
            benchmarkPriorTable = nil
            benchmarkPriorArtifactState = nil
            complexityAwareSchedulingEnabled = false

            if !workers.isEmpty {
                if let bridgeIP = firstBridgeIPv4() {
                    thunderboltDispatcher = makeThunderboltDispatcher(complexityAwareSchedulingEnabled: false)
                    let totalSlots = workers.reduce(0) { $0 + $1.slots }
                    Logger.kiko.info(
                        "Thunderbolt offload enabled: \(workers.count) worker(s), \(totalSlots) remote slot(s), bridge IP: \(bridgeIP)"
                    )
                    Logger.kiko.info("Video scheduler policy 'fifo': forcing FIFO dequeue while keeping Thunderbolt offload enabled")
                } else {
                    Logger.kiko.warning("TB_WORKERS configured but no bridge interface found — Thunderbolt offload disabled; video scheduler policy 'fifo' will run local FIFO")
                    thunderboltDispatcher = nil
                }
            } else {
                Logger.kiko.info("Video scheduler policy 'fifo': no TB_WORKERS configured; running local FIFO policy")
                thunderboltDispatcher = nil
            }

        case .none:
            benchmarkPriorTable = nil
            benchmarkPriorArtifactState = nil
            complexityAwareSchedulingEnabled = false
            thunderboltDispatcher = nil

            if workers.isEmpty {
                Logger.kiko.info("Video scheduler policy 'none': running local FIFO policy")
            } else {
                Logger.kiko.info("Video scheduler policy 'none': Thunderbolt offload disabled by config; running local FIFO policy")
            }
        }

        let processor = MediaProcessor(
            config: config,
            database: database,
            moderationMarkers: moderationMarkers,
            thunderboltDispatcher: thunderboltDispatcher,
            benchmarkPriorTable: benchmarkPriorTable,
            priorArtifactState: benchmarkPriorArtifactState,
            complexityAwareSchedulingEnabled: complexityAwareSchedulingEnabled
        )

        await processor.setRecomputeSignal { trigger in
            Logger.kiko.debug("Recompute trigger received: \(trigger.rawValue, privacy: .public)")
        }

        if let thunderboltDispatcher {
            await thunderboltDispatcher.setRecomputeSignal { trigger in
                Task {
                    await processor.requestRecomputeFromDispatcher(trigger: trigger)
                }
            }
        }

        if needsRebuild {
            if VolumeUtils.isMounted(volumeContainingPath: config.externalSSDPath) {
                await processor.rebuildFromSSD()
            } else {
                Logger.kiko.warning("Skipping rebuild: SSD not mounted")
            }
        }

        await processor.recoverIncomplete()
        await processor.verifyDerivedArtifacts()

        let fileServer = FileServer(
            thumbsDir: config.thumbsDir,
            previewsDir: config.previewsDir,
            database: database,
            cacheControl: config.cacheControl
        )

        let turnstileVerifier: TurnstileVerifier
        let sessionCookie: SessionCookie

        do {
            (turnstileVerifier, sessionCookie) = try configureSessionGating(config: config)
            Logger.kiko.info("Turnstile protection enabled")
        } catch let error as SessionGatingConfigurationError {
            Logger.kiko.error("Refusing startup: \(error.localizedDescription)")
            throw error
        } catch {
            Logger.kiko.error("Refusing startup due to session gating setup error: \(error.localizedDescription)")
            throw error
        }

        let webhookHandler = WebhookHandler(
            database: database,
            processor: processor,
            uploadDir: config.uploadDir,
            sessionCookie: sessionCookie,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes,
            queueFullRetryAfterSeconds: config.webhookRetryAfterSeconds
        )
        let heartRevisionTracker = HeartRevisionTracker()

        let publicRouter = RouterBuilders.buildPublicRouter(
            database: database,
            fileServer: fileServer,
            sessionCookie: sessionCookie,
            turnstileVerifier: turnstileVerifier,
            heartRevisionTracker: heartRevisionTracker,
            gateSecret: config.gateSecret,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes
        )

        let internalRouter = RouterBuilders.buildInternalRouter(
            database: database,
            fileServer: fileServer,
            moderationMarkers: moderationMarkers,
            webhookHandler: webhookHandler,
            internalAuthSecret: config.internalAuthSecret,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes
        )

        let publicApp = Application(
            router: publicRouter,
            configuration: .init(address: .hostname(config.bindAddress, port: config.publicPort))
        )

        let internalApp = Application(
            router: internalRouter,
            configuration: .init(address: .hostname("127.0.0.1", port: config.internalPort))
        )

        Logger.kiko.info("kiko-media starting: public on \(config.bindAddress):\(config.publicPort), internal on 127.0.0.1:\(config.internalPort)")

        let serviceGroup = ServiceGroup(
            configuration: .init(
                services: [publicApp, internalApp],
                gracefulShutdownSignals: [.sigterm, .sigint],
                logger: publicApp.logger
            )
        )

        let watchdog = Task { () -> Bool in
            while true {
                do {
                    try await Task.sleep(for: .seconds(config.healthCheckInterval))
                } catch {
                    return false
                }
                do {
                    _ = try await database.getTotalAssetCount()
                } catch {
                    Logger.kiko.error("Watchdog: DB health check failed: \(error)")
                    await serviceGroup.triggerGracefulShutdown()
                    return true
                }
            }
        }

        let serviceError: (any Error)?
        do {
            try await serviceGroup.run()
            serviceError = nil
        } catch {
            serviceError = error
        }
        watchdog.cancel()
        let watchdogTriggeredShutdown = await watchdog.value
        await thunderboltDispatcher?.shutdown()
        await processor.shutdown()
        await turnstileVerifier.shutdown()
        if let serviceError { throw serviceError }
        if watchdogTriggeredShutdown { throw RuntimeHealthError.watchdogDatabaseHealthCheckFailed }
    }

    private static func firstBridgeIPv4() -> String? {
        var addrs: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&addrs) == 0, let first = addrs else { return nil }
        defer { freeifaddrs(first) }

        var cursor: UnsafeMutablePointer<ifaddrs>? = first
        while let addr = cursor {
            defer { cursor = addr.pointee.ifa_next }

            let name = String(cString: addr.pointee.ifa_name)
            guard name.hasPrefix("bridge"),
                  let socketAddress = addr.pointee.ifa_addr,
                  socketAddress.pointee.sa_family == UInt8(AF_INET) else {
                continue
            }

            var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
            if getnameinfo(
                socketAddress,
                socklen_t(socketAddress.pointee.sa_len),
                &hostname,
                socklen_t(hostname.count),
                nil,
                0,
                NI_NUMERICHOST
            ) == 0 {
                let bytes = hostname.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }
                return String(decoding: bytes, as: UTF8.self)
            }
        }
        return nil
    }

    private static func loadBenchmarkPriorTable(
        config: Config
    ) -> (table: BenchmarkPriorTable?, loadResult: BenchmarkPriorArtifact.LoadResult) {
        let priorPath = config.benchmarkPriorPath
        switch BenchmarkPriorArtifact.loadResult(fromPath: priorPath) {
        case .loaded(let artifact):
            Logger.kiko.info(
                "Loaded benchmark prior artifact from \(priorPath, privacy: .public) with \(artifact.machines.count) machine profile(s)"
            )
            return (BenchmarkPriorTable(artifact: artifact), .loaded(artifact))
        case .missing:
            Logger.kiko.warning(
                "No benchmark prior artifact at \(priorPath, privacy: .public); complexity-aware scheduling gate will remain off"
            )
            return (nil, .missing)
        case .invalid:
            Logger.kiko.warning(
                "Benchmark prior artifact at \(priorPath, privacy: .public) is invalid or unreadable; complexity-aware scheduling gate will remain off"
            )
            return (nil, .invalid)
        case .unsupportedVersion(let version):
            Logger.kiko.warning(
                "Benchmark prior artifact at \(priorPath, privacy: .public) uses unsupported version \(version, privacy: .public); complexity-aware scheduling gate will remain off"
            )
            return (nil, .unsupportedVersion(version))
        }
    }

    private static func priorArtifactState(
        from loadResult: BenchmarkPriorArtifact.LoadResult
    ) -> CAActivationGate.PriorArtifactState {
        switch loadResult {
        case .missing:
            return .missing
        case .invalid, .unsupportedVersion:
            return .invalid
        case .loaded:
            return .loaded
        }
    }

    private static func workersMissingTickV2(
        workers: [Config.ThunderboltWorker],
        port: Int,
        connectTimeoutMS: Int
    ) -> [String] {
        let bridgeSources = ThunderboltDispatcher.discoverBridgeSources()
        return workers.compactMap { worker in
            guard let sourceIP = ThunderboltDispatcher.sourceIPForWorkerHost(
                worker.host,
                bridgeSources: bridgeSources
            ) else {
                return worker.host
            }
            guard let caps = ThunderboltTransport.queryCapabilities(
                host: worker.host,
                port: port,
                timeoutMS: connectTimeoutMS,
                sourceIP: sourceIP
            ),
            TickProtocolGate.isAccepted(version: caps.tickVersion, complexityAwareSchedulingEnabled: true) else {
                return worker.host
            }
            return nil
        }
    }
}
