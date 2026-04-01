import Testing
import Foundation
import CryptoKit
import Hummingbird
import HummingbirdTesting
import NIOCore
@testable import KikoMediaCore
@testable import KikoMediaApp

// MARK: - Webhook Integration

@Suite("Webhook Integration")
struct WebhookRouteTests {

    @Test("Valid webhook creates asset in database")
    func webhookInsertsAsset() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "webhook-test-001"
        // Create an actual image file at the upload path
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)", width: 100, height: 100)

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":1024,"Offset":1024,"MetaData":{"filename":"photo.jpg","filetype":"image/jpeg"},"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
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

        // Verify asset was inserted into DB
        let asset = try await env.database.getAsset(id: uploadId)
        #expect(asset != nil)
        #expect(asset?.originalName == "photo.jpg")
    }

    @Test("Webhook sanitizes original filename before database insert")
    func webhookSanitizesOriginalName() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "webhook-sanitize-001"
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)", width: 100, height: 100)

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":1024,"Offset":1024,"MetaData":{"filename":"<script>alert(1)</script>/photo.jpg"},"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
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

        let asset = try await env.database.getAsset(id: uploadId)
        #expect(asset?.originalName == "_script_alert(1)__script__photo.jpg")
    }

    @Test("Webhook with missing upload ID returns 400")
    func webhookMissingId() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"Size":1024}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook with missing Upload object returns 400")
    func webhookMissingUpload() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook with path traversal ID is rejected")
    func webhookPathTraversal() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"../../../etc/passwd","Size":1024,"Offset":1024,"Storage":{"Path":"\(env.uploadDir)/../../../etc/passwd"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Duplicate webhook is idempotent (second call succeeds without duplicating)")
    func webhookIdempotent() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "idem-webhook-001"
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)")

        let payload = """
        {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":1024,"Offset":1024,"MetaData":{"filename":"photo.jpg"},"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
        """

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            // First call
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .ok)
            }

            // Second call (duplicate webhook)
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .ok) // Should not fail
            }
        }

        // Still only one asset in DB
        let count = try await env.database.getTotalAssetCount()
        #expect(count == 1)
    }

    @Test("Pre-create rejects without cookie when session gating is enabled")
    func preCreateRejectsMissingCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: "test-secret-for-precreate")
        let app = Application(router: env.internalRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            // Pre-create payload without Cookie in HTTPRequest headers
            let payload = """
            {"Type":"pre-create","Event":{"Upload":{"Size":1024,"MetaData":{"filename":"photo.jpg"}},"HTTPRequest":{"Method":"POST","URI":"/files/","Header":{}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Pre-create accepts with valid cookie when session gating is enabled")
    func preCreateAcceptsValidCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: "test-secret-for-precreate")
        let value = cookie.create()
        let app = Application(router: env.internalRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            let payload = """
            {"Type":"pre-create","Event":{"Upload":{"Size":1024,"MetaData":{"filename":"photo.jpg"}},"HTTPRequest":{"Method":"POST","URI":"/files/","Header":{"Cookie":["kiko_session=\(value)"]}}}}
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

    @Test("Pre-create works without gating when session gating is disabled")
    func preCreateNoGating() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter(sessionCookie: nil))
        try await app.test(.router) { client in
            let payload = """
            {"Type":"pre-create","Event":{"Upload":{"Size":1024}}}
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

// MARK: - Webhook Edge Cases

@Suite("Webhook Edge Cases")
struct WebhookEdgeCaseTests {

    @Test("Webhook rejects when file not found at storage path")
    func webhookFileNotFound() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"missing-file-001","Size":1024,"Offset":1024,"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/missing-file-001"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook rejects storage path outside upload directory")
    func webhookPathOutsideUploads() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        // Create a file outside the upload dir to ensure the path check (not file existence) is what rejects it
        let outsidePath = env.tempDir.appendingPathComponent("outside-file").path
        try Data("test".utf8).write(to: URL(fileURLWithPath: outsidePath))

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"legit-id","Size":1024,"Offset":1024,"Storage":{"Type":"filestore","Path":"\(outsidePath)"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook rejects storage path symlink escaping upload directory")
    func webhookPathSymlinkEscape() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let outsidePath = env.tempDir.appendingPathComponent("outside-symlink-target").path
        try Data("test".utf8).write(to: URL(fileURLWithPath: outsidePath))

        let symlinkPath = "\(env.uploadDir)/symlink-escape-link"
        try FileManager.default.createSymbolicLink(atPath: symlinkPath, withDestinationPath: outsidePath)

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"symlink-escape-001","Size":4,"Offset":4,"Storage":{"Type":"filestore","Path":"\(symlinkPath)"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook rejects upload-complete with missing offset")
    func webhookMissingOffset() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "missing-offset-001"
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)")

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":1024,"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook rejects upload-complete with size offset mismatch")
    func webhookSizeOffsetMismatch() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "size-offset-mismatch-001"
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)")

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":1024,"Offset":1000,"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook rejects upload-complete with zero-byte size")
    func webhookZeroByteUpload() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "zero-byte-upload-001"
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)")

        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":0,"Offset":0,"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook returns 503 with Retry-After when processor queue is full")
    func webhookQueueFullBackpressure() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "queue-full-upload-001"
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)")

        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            uploadDir: env.config.uploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            externalSSDPath: env.config.externalSSDPath,
            databasePath: env.config.databasePath,
            webhookRetryAfterSeconds: 17,
            turnstileSecret: nil,
            sessionHmacSecret: nil,
            maxPendingWebhookJobs: 0
        )
        let moderationMarkers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backpressure"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: moderationMarkers)
        let fileServer = FileServer(
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            database: env.database,
            cacheControl: "public, max-age=31536000, immutable"
        )
        let webhookHandler = WebhookHandler(
            database: env.database,
            processor: processor,
            uploadDir: env.uploadDir,
            sessionCookie: nil,
            queueFullRetryAfterSeconds: config.webhookRetryAfterSeconds
        )
        let app = Application(router: RouterBuilders.buildInternalRouter(
            database: env.database,
            fileServer: fileServer,
            moderationMarkers: moderationMarkers,
            webhookHandler: webhookHandler
        ))

        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":1024,"Offset":1024,"MetaData":{"filename":"photo.jpg"},"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .serviceUnavailable)
                #expect(response.headers[.retryAfter] == "\(config.webhookRetryAfterSeconds)")
            }
        }

        #expect(try await env.database.assetExists(id: uploadId) == false)
    }

    @Test("Webhook duplicate remains idempotent when queue is full")
    func webhookQueueFullDuplicateIdempotent() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadId = "queue-full-duplicate-001"
        try TestImage.writeJPEG(to: "\(env.uploadDir)/\(uploadId)")
        _ = try await env.database.insertQueued(id: uploadId, type: .image, originalName: "photo.jpg")

        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            uploadDir: env.config.uploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            externalSSDPath: env.config.externalSSDPath,
            databasePath: env.config.databasePath,
            webhookRetryAfterSeconds: 17,
            turnstileSecret: nil,
            sessionHmacSecret: nil,
            maxPendingWebhookJobs: 0
        )
        let moderationMarkers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-backpressure-duplicate"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: moderationMarkers)
        let fileServer = FileServer(
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            database: env.database,
            cacheControl: "public, max-age=31536000, immutable"
        )
        let webhookHandler = WebhookHandler(
            database: env.database,
            processor: processor,
            uploadDir: env.uploadDir,
            sessionCookie: nil,
            queueFullRetryAfterSeconds: config.webhookRetryAfterSeconds
        )
        let app = Application(router: RouterBuilders.buildInternalRouter(
            database: env.database,
            fileServer: fileServer,
            moderationMarkers: moderationMarkers,
            webhookHandler: webhookHandler
        ))

        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-finish","Event":{"Upload":{"ID":"\(uploadId)","Size":1024,"Offset":1024,"MetaData":{"filename":"photo.jpg"},"Storage":{"Type":"filestore","Path":"\(env.uploadDir)/\(uploadId)"}}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.retryAfter] == nil)
            }
        }

        #expect(try await env.database.getTotalAssetCount() == 1)
        let existing = try await env.database.getAsset(id: uploadId)
        #expect(existing?.status == .queued)
    }

    @Test("Unknown webhook hook type is silently accepted")
    func webhookUnknownType() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            let payload = """
            {"Type":"post-receive","Event":{"Upload":{"ID":"test"}}}
            """
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }

    @Test("Webhook rejects invalid JSON body")
    func webhookInvalidJSON() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        let app = Application(router: env.internalRouter())
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/hooks/upload-complete", method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: "not json at all")
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Webhook rejects oversized JSON body with 413")
    func webhookOversizedJSON() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.internalRouter())
        let oversizedType = String(repeating: "a", count: RequestBodyLimits.defaultJSONMaxBytes + 1024)
        let payload = #"{"Type":"\#(oversizedType)","Event":{"Upload":{"ID":"x"}}}"#

        try await app.test(.router) { client in
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .contentTooLarge)
            }
        }
    }

    @Test("Webhook honors JSON_MAX_BODY_BYTES override")
    func webhookHonorsJSONMaxBodyBytesOverride() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            uploadDir: env.config.uploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            externalSSDPath: env.config.externalSSDPath,
            databasePath: env.config.databasePath,
            jsonMaxBodyBytes: 512,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )
        let moderationMarkers = ModerationMarkers(baseDir: env.tempDir.appendingPathComponent("moderated-json-cap"))
        let processor = MediaProcessor(config: config, database: env.database, moderationMarkers: moderationMarkers)
        let fileServer = FileServer(
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            database: env.database,
            cacheControl: "public, max-age=31536000, immutable"
        )
        let webhookHandler = WebhookHandler(
            database: env.database,
            processor: processor,
            uploadDir: env.uploadDir,
            sessionCookie: nil,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes
        )
        let app = Application(router: RouterBuilders.buildInternalRouter(
            database: env.database,
            fileServer: fileServer,
            moderationMarkers: moderationMarkers,
            webhookHandler: webhookHandler,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes
        ))

        let oversizedType = String(repeating: "a", count: config.jsonMaxBodyBytes + 256)
        let payload = #"{"Type":"\#(oversizedType)","Event":{"Upload":{"ID":"x"}}}"#

        try await app.test(.router) { client in
            try await client.execute(
                uri: "/hooks/upload-complete",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: payload)
            ) { response in
                #expect(response.status == .contentTooLarge)
            }
        }
    }
}

// MARK: - Heart Endpoint

@Suite("Heart Endpoint")
struct HeartRouteTests {

    private let hmacSecret = "test-secret-key-for-session-gating"

    @Test("POST /api/assets/{id}/heart returns 200 with incremented count")
    func heartIncrementsCount() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "heart-test-001")

        let heartRevisionTracker = HeartRevisionTracker()
        let (router, cookie) = env.gatedPublicRouter(heartRevisionTracker: heartRevisionTracker)
        let cookieValue = cookie.create()
        let app = Application(router: router)
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/heart-test-001/heart",
                method: .post,
                headers: [.cookie: "kiko_session=\(cookieValue)"]
            ) { response in
                #expect(response.status == .ok)
                let body = try JSONDecoder().decode(TestHeartResponse.self, from: response.body)
                #expect(body.heartCount == 1)
            }
            try await client.execute(
                uri: "/api/assets/heart-test-001/heart",
                method: .post,
                headers: [.cookie: "kiko_session=\(cookieValue)"]
            ) { response in
                let body = try JSONDecoder().decode(TestHeartResponse.self, from: response.body)
                #expect(body.heartCount == 2)
            }

            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.cookie: "kiko_session=\(cookieValue)"]
            ) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.heartRevision == 2)
            }
        }
    }

    @Test("POST /api/assets/{id}/heart on non-existent asset returns 404")
    func heartNonExistentReturns404() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let (router, cookie) = env.gatedPublicRouter()
        let cookieValue = cookie.create()
        let app = Application(router: router)
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/does-not-exist/heart",
                method: .post,
                headers: [.cookie: "kiko_session=\(cookieValue)"]
            ) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("POST /api/assets/{id}/heart without session cookie returns 401")
    func heartWithoutSessionReturns401() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "heart-noauth")

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/heart-noauth/heart",
                method: .post
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("POST /api/assets/{id}/heart on non-complete asset returns 404")
    func heartNonCompleteReturns404() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "heart-queued", type: .image, originalName: "photo.jpg")

        let (router, cookie) = env.gatedPublicRouter()
        let cookieValue = cookie.create()
        let app = Application(router: router)
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/assets/heart-queued/heart",
                method: .post,
                headers: [.cookie: "kiko_session=\(cookieValue)"]
            ) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("GET /api/gallery?sort=hearts returns assets sorted by heartCount DESC")
    func gallerySortByHearts() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "popular", timestamp: "2025:02:05 10:00:00")
        try await env.insertCompleteImageAsset(id: "recent", timestamp: "2025:02:05 14:00:00")
        _ = try await env.database.incrementHeartCount(id: "popular")
        _ = try await env.database.incrementHeartCount(id: "popular")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery?sort=hearts", method: .get) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.count == 2)
                #expect(gallery.assets[0].id == "popular")
                #expect(gallery.assets[0].heartCount == 2)
                #expect(gallery.assets[1].id == "recent")
                #expect(gallery.assets[1].heartCount == 0)
            }
        }
    }

    @Test("heartCount field present in gallery response")
    func heartCountInGalleryResponse() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "hc-gallery")

        let app = Application(router: env.publicRouter(heartRevisionTracker: HeartRevisionTracker(initialValue: 7)))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.first?.heartCount == 0)
                #expect(gallery.heartRevision == 7)
            }
        }
    }

    @Test("internal gallery response omits heartRevision on the wire")
    func internalGalleryOmitsHeartRevision() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "internal-gallery")

        let app = Application(router: env.internalRouter(internalAuthSecret: "internal-secret"))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: "internal-secret"]
            ) { response in
                let object = try JSONSerialization.jsonObject(with: response.body) as? [String: Any]
                #expect(object?["heartRevision"] == nil)
            }
        }
    }

    @Test("POST /api/heart-counts returns counts only for complete and moderated assets")
    func heartCountsReturnsVisibleAndModeratedOnly() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "heart-count-complete")
        try await env.insertCompleteImageAsset(id: "heart-count-moderated")
        _ = try await env.database.incrementHeartCount(id: "heart-count-complete")
        _ = try await env.database.incrementHeartCount(id: "heart-count-complete")
        _ = try await env.database.incrementHeartCount(id: "heart-count-moderated")
        try await env.database.updateStatus(id: "heart-count-moderated", status: .moderated)
        _ = try await env.database.insertQueued(id: "heart-count-queued", type: .image, originalName: "queued.jpg")

        let (router, cookie) = env.gatedPublicRouter()
        let cookieValue = cookie.create()
        let app = Application(router: router)
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/heart-counts",
                method: .post,
                headers: [
                    .cookie: "kiko_session=\(cookieValue)",
                    .contentType: "application/json",
                ],
                body: ByteBuffer(
                    string: #"{"ids":["heart-count-complete","heart-count-moderated","heart-count-queued","missing-id","heart-count-complete"]}"#
                )
            ) { response in
                #expect(response.status == .ok)
                let body = try JSONDecoder().decode(TestHeartCountsResponse.self, from: response.body)
                #expect(body.heartCounts["heart-count-complete"] == 2)
                #expect(body.heartCounts["heart-count-moderated"] == 1)
                #expect(body.heartCounts["heart-count-queued"] == nil)
                #expect(body.heartCounts["missing-id"] == nil)
                #expect(body.heartCounts.count == 2)
            }
        }
    }

    @Test("POST /api/heart-counts without session cookie returns 401")
    func heartCountsWithoutSessionReturns401() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "heart-counts-noauth")

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/heart-counts",
                method: .post,
                headers: [.contentType: "application/json"],
                body: ByteBuffer(string: #"{"ids":["heart-counts-noauth"]}"#)
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("POST /api/heart-counts rejects invalid IDs")
    func heartCountsRejectsInvalidIds() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let (router, cookie) = env.gatedPublicRouter()
        let cookieValue = cookie.create()
        let app = Application(router: router)
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/heart-counts",
                method: .post,
                headers: [
                    .cookie: "kiko_session=\(cookieValue)",
                    .contentType: "application/json",
                ],
                body: ByteBuffer(string: #"{"ids":["../bad-id"]}"#)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("POST /api/heart-counts rejects requests above 500 IDs")
    func heartCountsRejectsRequestsAbove500Ids() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let ids = (0..<501).map { "asset-\($0)" }
        let payload = try JSONSerialization.data(withJSONObject: ["ids": ids])
        let (router, cookie) = env.gatedPublicRouter()
        let cookieValue = cookie.create()
        let app = Application(router: router)
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/heart-counts",
                method: .post,
                headers: [
                    .cookie: "kiko_session=\(cookieValue)",
                    .contentType: "application/json",
                ],
                body: ByteBuffer(data: payload)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }
}
