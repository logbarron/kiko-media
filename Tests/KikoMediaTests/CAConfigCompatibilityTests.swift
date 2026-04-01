import Testing
@testable import KikoMediaCore

@Suite("CA config compatibility")
struct CAConfigCompatibilityTests {
    @Test("no TB workers keeps FIFO policy")
    func noTBWorkers_policyIsFIFO() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeConfig(env: env, tbWorkers: "")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-config-no-workers"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }

    @Test("TB workers with prior enables complexity-aware policy")
    func tbWorkersPresent_withPrior_policyIsComplexityAware() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeConfig(env: env, tbWorkers: "10.0.0.2:1")
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-config-with-prior"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true
        )

        #expect(await processor.videoSelectionPolicy == .complexityAware)
    }

    @Test("TB workers without prior keeps FIFO policy")
    func tbWorkersPresent_noPrior_policyIsFIFO() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeConfig(env: env, tbWorkers: "10.0.0.2:1")
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-config-no-prior"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            complexityAwareSchedulingEnabled: true
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }

    @Test("explicit override can force FIFO with valid workers and prior")
    func explicitOverride_canForceFIFO_withValidWorkers() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeConfig(env: env, tbWorkers: "10.0.0.2:1")
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-config-explicit-fifo"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: false
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }

    @Test("scheduler policy fifo forces FIFO with valid workers and prior")
    func schedulerPolicyFIFO_forcesFIFO_withValidWorkers() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeConfig(env: env, tbWorkers: "10.0.0.2:1", videoSchedulerPolicy: .fifo)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-config-policy-fifo"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }

    @Test("scheduler policy none forces FIFO with valid workers and prior")
    func schedulerPolicyNone_forcesFIFO_withValidWorkers() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeConfig(env: env, tbWorkers: "10.0.0.2:1", videoSchedulerPolicy: .none)
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-config-policy-none"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: true
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }

    @Test("tick v2 gate disables CA when workers are missing v2")
    func tickV2Gate_disablesCA_whenWorkersMissingV2() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = makeConfig(env: env, tbWorkers: "10.0.0.2:1")
        let priorTable = makeLocalComplexityAwarePriorTable(config: config)
        let gatePasses = TickProtocolGate.isAccepted(version: 1, complexityAwareSchedulingEnabled: true)
        #expect(!gatePasses)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-ca-config-tick-gate"))
        let processor = MediaProcessor(
            config: config,
            database: env.database,
            moderationMarkers: markers,
            benchmarkPriorTable: priorTable,
            complexityAwareSchedulingEnabled: gatePasses
        )

        #expect(await processor.videoSelectionPolicy == .fifo)
    }
}

private func makeConfig(
    env: TestEnv,
    tbWorkers: String,
    videoSchedulerPolicy: VideoSchedulerPolicy = .auto
) -> Config {
    Config(
        publicPort: env.config.publicPort,
        internalPort: env.config.internalPort,
        bindAddress: env.config.bindAddress,
        uploadDir: env.config.uploadDir,
        thumbsDir: env.config.thumbsDir,
        previewsDir: env.config.previewsDir,
        logsDir: env.config.logsDir,
        moderatedDir: env.config.moderatedDir,
        externalSSDPath: env.config.externalSSDPath,
        databasePath: env.config.databasePath,
        turnstileSecret: env.config.turnstileSecret,
        sessionHmacSecret: env.config.sessionHmacSecret,
        videoSchedulerPolicy: videoSchedulerPolicy,
        videoTranscodePreset: env.config.videoTranscodePreset,
        tbWorkers: tbWorkers
    )
}
