import Testing
import Foundation
import CryptoKit
import Hummingbird
import HummingbirdTesting
import NIOCore
@testable import KikoMediaCore
@testable import KikoMediaApp

// MARK: - Moderation API

@Suite("Moderation API")
struct ModerationRouteTests {

    @Test("PATCH sets asset status to moderated and creates marker file")
    func moderate() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "mod-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/mod-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .noContent)
            }
        }

        // Verify in DB
        let asset = try await env.database.getAsset(id: "mod-001")
        #expect(asset?.status == .moderated)

        // Verify marker file created in temp dir
        let markerPath = env.tempDir.appendingPathComponent("moderated/mod-001").path
        #expect(FileManager.default.fileExists(atPath: markerPath))
    }

    @Test("PATCH restores asset status to complete and removes marker file")
    func unmoderate() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "unmod-001")
        try await env.database.updateStatus(id: "unmod-001", status: .moderated)

        // Pre-create marker file (simulates prior moderation)
        let markerDir = env.tempDir.appendingPathComponent("moderated")
        try FileManager.default.createDirectory(at: markerDir, withIntermediateDirectories: true)
        FileManager.default.createFile(atPath: markerDir.appendingPathComponent("unmod-001").path, contents: nil)

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/unmod-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"complete"}"#)
            ) { response in
                #expect(response.status == .noContent)
            }
        }

        let asset = try await env.database.getAsset(id: "unmod-001")
        #expect(asset?.status == .complete)

        // Verify marker file removed
        let markerPath = markerDir.appendingPathComponent("unmod-001").path
        #expect(!FileManager.default.fileExists(atPath: markerPath))
    }

    @Test("PATCH on queued asset returns 400")
    func moderateQueuedFails() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "q-001", type: .image, originalName: "q.jpg")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/q-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("PATCH on nonexistent asset returns 404")
    func moderateNotFound() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/ghost",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("PATCH with invalid JSON returns 400")
    func moderateInvalidJSON() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "bad-json-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/bad-json-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: "not json")
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("PATCH with oversized JSON body returns 413")
    func moderateOversizedJSON() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "oversize-json-001")

        let oversizedStatus = String(repeating: "a", count: RequestBodyLimits.defaultJSONMaxBytes + 1024)
        let oversizedPayload = #"{"status":"\#(oversizedStatus)"}"#

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/oversize-json-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: oversizedPayload)
            ) { response in
                #expect(response.status == .contentTooLarge)
            }
        }
    }

    @Test("PATCH with invalid status value returns 400")
    func moderateInvalidStatus() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "bad-status-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/bad-status-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"deleted"}"#)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Moderation is idempotent (moderating an already-moderated asset succeeds)")
    func moderateIdempotent() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "idem-001")
        try await env.database.updateStatus(id: "idem-001", status: .moderated)

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/idem-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .noContent)
            }
        }
    }
}

// MARK: - Moderation Marker Atomicity

@Suite("Moderation Marker Atomicity")
struct ModerationMarkerAtomicityTests {
    private enum ForcedStatusUpdateError: Error {
        case failed
    }

    /// Build an internal router with a custom ModerationMarkers instance
    private func internalRouter(
        env: TestEnv,
        moderationMarkers: ModerationMarkers,
        updateAssetStatus: (@Sendable (_ id: String, _ status: Asset.AssetStatus) async throws -> Void)? = nil
    ) -> Router<BasicRequestContext> {
        let fileServer = FileServer(
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            database: env.database,
            cacheControl: "public, max-age=31536000, immutable"
        )
        let processor = MediaProcessor(
            config: env.config,
            database: env.database,
            moderationMarkers: moderationMarkers
        )
        let webhookHandler = WebhookHandler(
            database: env.database,
            processor: processor,
            uploadDir: env.uploadDir,
            sessionCookie: nil
        )
        return RouterBuilders.buildInternalRouter(
            database: env.database,
            fileServer: fileServer,
            moderationMarkers: moderationMarkers,
            webhookHandler: webhookHandler,
            internalAuthSecret: routeTestsInternalAuthSecret,
            updateAssetStatus: updateAssetStatus
        )
    }

    @Test("Moderation PATCH returns 500 when marker directory is unwritable, DB unchanged")
    func markerFailureBlocksModeration() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "atom-001")

        // Create a regular file where the moderated directory should be,
        // so ensureDirectory() / createFile will fail
        let markerDirPath = env.tempDir.appendingPathComponent("broken-moderated")
        FileManager.default.createFile(atPath: markerDirPath.path, contents: Data("block".utf8))
        let brokenMarkers = ModerationMarkers(baseDir: markerDirPath)

        let app = Application(router: internalRouter(env: env, moderationMarkers: brokenMarkers))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/atom-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .internalServerError)
            }
        }

        // DB must remain unchanged -- still .complete
        let asset = try await env.database.getAsset(id: "atom-001")
        #expect(asset?.status == .complete)
    }

    @Test("Moderation PATCH rolls marker back when DB update fails (moderate path)")
    func markerRollbackOnStatusUpdateFailureModerate() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "atom-db-fail-mod-001"
        try await env.insertCompleteImageAsset(id: id)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        let app = Application(router: internalRouter(
            env: env,
            moderationMarkers: markers,
            updateAssetStatus: { _, _ in throw ForcedStatusUpdateError.failed }
        ))

        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/\(id)",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .internalServerError)
            }
        }

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .complete)
        #expect(try !markers.allMarked().contains(id))
    }

    @Test("Moderation PATCH rolls marker back when DB update fails (unmoderate path)")
    func markerRollbackOnStatusUpdateFailureUnmoderate() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let id = "atom-db-fail-unmod-001"
        try await env.insertCompleteImageAsset(id: id)
        try await env.database.updateStatus(id: id, status: .moderated)

        let markers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated"))
        try markers.mark(id)

        let app = Application(router: internalRouter(
            env: env,
            moderationMarkers: markers,
            updateAssetStatus: { _, _ in throw ForcedStatusUpdateError.failed }
        ))

        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/\(id)",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"complete"}"#)
            ) { response in
                #expect(response.status == .internalServerError)
            }
        }

        let asset = try await env.database.getAsset(id: id)
        #expect(asset?.status == .moderated)
        #expect(try markers.allMarked().contains(id))
    }

    @Test("ModerationMarkers.mark() throws ModerationMarkerError.writeFailed when baseDir is unwritable")
    func markThrowsOnUnwritableDir() throws {
        // Use /dev/null as baseDir -- can't create subdirectories under a device node
        let markers = ModerationMarkers(baseDir: URL(fileURLWithPath: "/dev/null"))
        #expect {
            try markers.mark("test-id")
        } throws: { error in
            guard let markerError = error as? ModerationMarkerError else { return false }
            if case .writeFailed = markerError { return true }
            return false
        }
    }

    @Test("ModerationMarkers.unmark() throws invalidId for bad ID")
    func unmarkThrowsInvalidId() throws {
        let markers = ModerationMarkers(baseDir: URL(fileURLWithPath: "/tmp"))
        #expect {
            try markers.unmark("../escape")
        } throws: { error in
            guard let markerError = error as? ModerationMarkerError else { return false }
            if case .invalidId = markerError { return true }
            return false
        }
    }

    @Test("Happy path: moderate creates marker file then updates DB")
    func moderateCreatesMarkerAndUpdatesDB() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "happy-mod-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/happy-mod-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .noContent)
            }
        }

        let markerPath = env.tempDir.appendingPathComponent("moderated/happy-mod-001").path
        #expect(FileManager.default.fileExists(atPath: markerPath))
        let asset = try await env.database.getAsset(id: "happy-mod-001")
        #expect(asset?.status == .moderated)
    }

    @Test("Happy path: unmoderate removes marker file then updates DB")
    func unmoderateRemovesMarkerAndUpdatesDB() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "happy-unmod-001")

        // First moderate it
        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/happy-unmod-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .noContent)
            }

            // Verify marker exists before unmoderation
            let markerPath = env.tempDir.appendingPathComponent("moderated/happy-unmod-001").path
            #expect(FileManager.default.fileExists(atPath: markerPath))

            // Now unmoderate
            try await client.execute(
                uri: "/api/assets/happy-unmod-001",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"complete"}"#)
            ) { response in
                #expect(response.status == .noContent)
            }
        }

        let markerPath = env.tempDir.appendingPathComponent("moderated/happy-unmod-001").path
        #expect(!FileManager.default.fileExists(atPath: markerPath))
        let asset = try await env.database.getAsset(id: "happy-unmod-001")
        #expect(asset?.status == .complete)
    }
}

// MARK: - Route Separation (Security Boundary)

@Suite("Route Separation")
struct RouteSeparationTests {

    @Test("Public router does NOT have POST /hooks/upload-complete")
    func publicHasNoWebhook() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"test","Size":1024}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                // Security boundary: webhook must not exist on the public listener.
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("Public router does NOT have PATCH /api/assets/{id}")
    func publicHasNoModeration() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "sep-001")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/sep-001",
                method: .patch,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                // Security boundary: this route must not exist on the public listener.
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("Internal router does NOT gate on session cookies")
    func internalNoSessionGating() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "ungated-001")
        let sessionCookie = SessionCookie(
            config: env.config,
            hmacSecret: "01234567890123456789012345678901"
        )

        // Configure webhook session validation and verify it does not leak onto internal gallery/file routes.
        let app = Application(
            router: env.internalRouter(
                sessionCookie: sessionCookie,
                internalAuthSecret: routeTestsInternalAuthSecret
            )
        )
        try await app.test(.router) { client in
            // No cookie header - should still work as long as internal Authorization is valid.
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: routeTestsInternalAuthSecret]
            ) { response in
                #expect(response.status == .ok)
            }
            try await client.execute(
                uri: "/api/thumbs/ungated-001",
                method: .get,
                headers: [.authorization: routeTestsInternalAuthSecret]
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }
}

// MARK: - Internal Auth Guard

@Suite("Internal Auth Guard")
struct InternalAuthGuardTests {
    private let internalAuthSecret = "test-internal-auth-secret"

    @Test("Internal gallery fails closed when internal auth secret is missing")
    func internalGalleryRejectsMissingConfiguredSecret() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: nil))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: internalAuthSecret]
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Internal gallery fails closed when internal auth secret is empty")
    func internalGalleryRejectsEmptyConfiguredSecret() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: ""))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: internalAuthSecret]
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Internal gallery rejects missing Authorization header")
    func internalGalleryRejectsMissingAuthorization() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: internalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Internal thumbs rejects missing Authorization header")
    func internalThumbsRejectsMissingAuthorization() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "internal-auth-thumbs-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: internalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/internal-auth-thumbs-001", method: .get) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Internal preview rejects missing Authorization header")
    func internalPreviewRejectsMissingAuthorization() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "internal-auth-preview-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: internalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/internal-auth-preview-001", method: .get) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Internal moderation patch rejects missing Authorization header")
    func internalModerationRejectsMissingAuthorization() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "internal-auth-mod-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: internalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/internal-auth-mod-001",
                method: .patch,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Internal gallery rejects wrong Authorization header")
    func internalGalleryRejectsWrongAuthorization() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: internalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: "wrong-secret"]
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Internal gallery does not accept session HMAC secret when internal auth secret differs")
    func internalGalleryRejectsSessionHmacSecretWhenDifferent() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let sessionHmacSecret = "01234567890123456789012345678901"
        let app = Application(router: env.internalRouter(internalAuthSecret: internalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: sessionHmacSecret]
            ) { response in
                #expect(response.status == .unauthorized)
            }

            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: internalAuthSecret]
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }

    @Test("Internal gallery accepts correct Authorization header")
    func internalGalleryAcceptsCorrectAuthorization() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: internalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: internalAuthSecret]
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }

    @Test("Internal webhook route remains available without internal Authorization")
    func internalWebhookUnaffectedByInternalAuthGuard() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: nil))
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-receive","Event":{"Upload":{"ID":"test"}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }
}

// MARK: - Route-Layer ID Validation

@Suite("Route ID Validation")
struct RouteIdValidationTests {

    // Note: IDs containing "/" are split by the router before reaching the handler,
    // so we test with IDs that are invalid per Asset.isValidId but form a single path segment.

    @Test("Public thumbs rejects invalid ID with 400", arguments: [
        "..", ".hidden", "abc..def",
    ])
    func publicThumbsInvalidId(id: String) async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/\(id)", method: .get) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Public preview rejects invalid ID with 400", arguments: [
        "..", ".hidden", "abc..def",
    ])
    func publicPreviewInvalidId(id: String) async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/\(id)", method: .get) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Internal thumbs rejects invalid ID with 400", arguments: [
        "..", ".hidden", "abc..def",
    ])
    func internalThumbsInvalidId(id: String) async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/thumbs/\(id)",
                method: .get,
                headers: [.authorization: routeTestsInternalAuthSecret]
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Internal preview rejects invalid ID with 400", arguments: [
        "..", ".hidden", "abc..def",
    ])
    func internalPreviewInvalidId(id: String) async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/preview/\(id)",
                method: .get,
                headers: [.authorization: routeTestsInternalAuthSecret]
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Internal moderation PATCH rejects invalid ID with 400", arguments: [
        "..", ".hidden", "abc..def",
    ])
    func internalModerationInvalidId(id: String) async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/\(id)",
                method: .patch,
                headers: [.contentType: "application/json", .authorization: routeTestsInternalAuthSecret],
                body: ByteBuffer(string: #"{"status":"moderated"}"#)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }
}
