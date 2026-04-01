import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Terminal status persistence hardening")
struct TerminalStatusPersistenceTests {
    @Test("terminal failed status retries and eventually persists")
    func terminalFailedStatusRetriesAndPersists() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "terminal-status-retry-001"
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: "\(uploadID).mov")
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data(repeating: 0x61, count: 512).write(to: URL(fileURLWithPath: uploadPath))

        let injector = StatusWriteInjector(
            database: env.database,
            targetID: uploadID,
            targetStatus: .failed,
            forcedFailuresRemaining: 2
        )
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-terminal-retry"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetID, _ in
                .success(externalPath: "/tmp/\(assetID)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { _, _, _ in
                (thumb: true, preview: false, timestamp: "2026:02:28 12:00:00")
            },
            statusUpdateOverride: { id, status in
                try await injector.update(id: id, status: status)
            }
        )

        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath,
            assetType: .video
        ))

        let reachedFailed = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: uploadID)
            return asset?.status == .failed
        }
        #expect(reachedFailed)
        #expect(await injector.attempts == 3)
    }

    @Test("terminal failed status exhaustion leaves recoverable state")
    func terminalFailedStatusExhaustionLeavesRecoverableState() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "terminal-status-exhaust-001"
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: "\(uploadID).mov")
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data(repeating: 0x62, count: 512).write(to: URL(fileURLWithPath: uploadPath))

        let injector = StatusWriteInjector(
            database: env.database,
            targetID: uploadID,
            targetStatus: .failed,
            forcedFailuresRemaining: 10
        )
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-terminal-exhaust"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetID, _ in
                .success(externalPath: "/tmp/\(assetID)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { _, _, _ in
                (thumb: true, preview: false, timestamp: "2026:02:28 12:00:00")
            },
            statusUpdateOverride: { id, status in
                try await injector.update(id: id, status: status)
            }
        )

        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath,
            assetType: .video
        ))

        let reachedAttempts = try await waitUntil(timeoutSeconds: 4) {
            await injector.attempts >= 3
        }
        #expect(reachedAttempts)

        let asset = try await env.database.getAsset(id: uploadID)
        #expect(asset?.status == .processing)
        #expect(FileManager.default.fileExists(atPath: uploadPath))
        #expect(await injector.attempts == 3)
    }

    @Test("terminal complete retries and eventually persists")
    func terminalCompleteRetriesAndPersists() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "terminal-complete-retry-001"
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: "\(uploadID).mov")
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data(repeating: 0x63, count: 1024).write(to: URL(fileURLWithPath: uploadPath))

        let injector = MarkCompleteInjector(database: env.database, targetID: uploadID, forcedFailuresRemaining: 2)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-complete-retry"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetID, _ in
                .success(externalPath: "/tmp/\(assetID)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { _, _, _ in
                (thumb: true, preview: true, timestamp: "2026:02:28 12:00:00")
            },
            markCompleteOverride: { id, timestamp in
                try await injector.markComplete(id: id, timestamp: timestamp)
            }
        )

        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath,
            assetType: .video
        ))

        let reachedComplete = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: uploadID)
            return asset?.status == .complete
        }
        #expect(reachedComplete)

        let cleanedUp = try await waitUntil(timeoutSeconds: 4) {
            !FileManager.default.fileExists(atPath: uploadPath) &&
                !FileManager.default.fileExists(atPath: "\(uploadPath).info")
        }
        #expect(cleanedUp)

        let asset = try await env.database.getAsset(id: uploadID)
        #expect(asset?.completedAt != nil)
        #expect(!FileManager.default.fileExists(atPath: uploadPath))
        #expect(!FileManager.default.fileExists(atPath: "\(uploadPath).info"))
        #expect(await injector.attempts == 3)
    }

    @Test("terminal complete exhaustion queues bounded startup retry state")
    func terminalCompleteExhaustionQueuesBoundedStartupRetryState() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "terminal-complete-exhaust-001"
        _ = try await env.database.insertQueued(id: uploadID, type: .video, originalName: "\(uploadID).mov")
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data(repeating: 0x64, count: 1024).write(to: URL(fileURLWithPath: uploadPath))

        let injector = MarkCompleteInjector(database: env.database, targetID: uploadID, forcedFailuresRemaining: 10)
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-complete-exhaust"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            archiveOriginal: { _, assetID, _ in
                .success(externalPath: "/tmp/\(assetID)", checksum: "test-checksum")
            },
            localVideoProcessingOverride: { _, _, _ in
                (thumb: true, preview: true, timestamp: "2026:02:28 12:00:00")
            },
            markCompleteOverride: { id, timestamp in
                try await injector.markComplete(id: id, timestamp: timestamp)
            }
        )

        #expect(await processor.enqueue(
            uploadId: uploadID,
            originalName: "\(uploadID).mov",
            filePath: uploadPath,
            assetType: .video
        ))

        let reachedQueuedRetryState = try await waitUntil(timeoutSeconds: 4) {
            let asset = try await env.database.getAsset(id: uploadID)
            return asset?.status == .queued && asset?.retryCount == 1
        }
        #expect(reachedQueuedRetryState)

        let asset = try await env.database.getAsset(id: uploadID)
        #expect(asset?.status == .queued)
        #expect(asset?.retryCount == 1)
        #expect(FileManager.default.fileExists(atPath: uploadPath))
        #expect(await injector.attempts == 3)
    }

    @Test("terminal moderated status retries and eventually persists")
    func terminalModeratedStatusRetriesAndPersists() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "terminal-moderated-retry-001"
        _ = try await env.database.insertQueued(id: uploadID, type: .image, originalName: "\(uploadID).jpg")

        let injector = StatusWriteInjector(
            database: env.database,
            targetID: uploadID,
            targetStatus: .moderated,
            forcedFailuresRemaining: 2
        )
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-status-retry"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            statusUpdateOverride: { id, status in
                try await injector.update(id: id, status: status)
            }
        )

        let persisted = await processor.updateTerminalStatusLogged(id: uploadID, status: .moderated)
        #expect(persisted)
        #expect(await injector.attempts == 3)

        let asset = try await env.database.getAsset(id: uploadID)
        #expect(asset?.status == .moderated)
        #expect(asset?.completedAt != nil)
    }

    @Test("terminal moderated status exhaustion leaves non-terminal state")
    func terminalModeratedStatusExhaustionLeavesNonTerminalState() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "terminal-moderated-exhaust-001"
        _ = try await env.database.insertQueued(id: uploadID, type: .image, originalName: "\(uploadID).jpg")

        let injector = StatusWriteInjector(
            database: env.database,
            targetID: uploadID,
            targetStatus: .moderated,
            forcedFailuresRemaining: 10
        )
        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-status-exhaust"))
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: markers,
            statusUpdateOverride: { id, status in
                try await injector.update(id: id, status: status)
            }
        )

        let persisted = await processor.updateTerminalStatusLogged(id: uploadID, status: .moderated)
        #expect(!persisted)
        #expect(await injector.attempts == 3)

        let asset = try await env.database.getAsset(id: uploadID)
        #expect(asset?.status == .queued)
        #expect(asset?.completedAt == nil)
    }
}

private enum ForcedTerminalWriteError: Error {
    case forced
}

private actor StatusWriteInjector {
    private let database: Database
    private let targetID: String
    private let targetStatus: Asset.AssetStatus
    private var remainingForcedFailures: Int
    private(set) var attempts = 0

    init(
        database: Database,
        targetID: String,
        targetStatus: Asset.AssetStatus,
        forcedFailuresRemaining: Int
    ) {
        self.database = database
        self.targetID = targetID
        self.targetStatus = targetStatus
        self.remainingForcedFailures = max(0, forcedFailuresRemaining)
    }

    func update(id: String, status: Asset.AssetStatus) async throws {
        if id == targetID, status == targetStatus {
            attempts += 1
            if remainingForcedFailures > 0 {
                remainingForcedFailures -= 1
                throw ForcedTerminalWriteError.forced
            }
        }
        try await database.updateStatus(id: id, status: status)
    }
}

private actor MarkCompleteInjector {
    private let database: Database
    private let targetID: String
    private var remainingForcedFailures: Int
    private(set) var attempts = 0

    init(database: Database, targetID: String, forcedFailuresRemaining: Int) {
        self.database = database
        self.targetID = targetID
        self.remainingForcedFailures = max(0, forcedFailuresRemaining)
    }

    func markComplete(id: String, timestamp: String) async throws {
        if id == targetID {
            attempts += 1
            if remainingForcedFailures > 0 {
                remainingForcedFailures -= 1
                throw ForcedTerminalWriteError.forced
            }
        }
        try await database.markComplete(id: id, timestamp: timestamp)
    }
}

private func waitUntil(
    timeoutSeconds: TimeInterval,
    pollEveryMillis: UInt64 = 25,
    condition: @escaping @Sendable () async throws -> Bool
) async throws -> Bool {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        if try await condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(Int(pollEveryMillis)))
    }
    return try await condition()
}
