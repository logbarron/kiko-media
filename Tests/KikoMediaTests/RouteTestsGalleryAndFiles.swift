import Testing
import Foundation
import CryptoKit
import Hummingbird
import HummingbirdTesting
import NIOCore
@testable import KikoMediaCore
@testable import KikoMediaApp

// MARK: - Gallery API

@Suite("Gallery API")
struct GalleryRouteTests {

    @Test("Gallery returns correct JSON shape with assets and total")
    func galleryShape() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "gallery-001", timestamp: "2025:02:05 12:00:00")
        try await env.insertCompleteImageAsset(id: "gallery-002", timestamp: "2025:02:05 13:00:00")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                #expect(response.status == .ok)
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.total == 2)
                #expect(gallery.assets.count == 2)
                // Public gallery should NOT include status field
                #expect(gallery.assets.allSatisfy { $0.status == nil })
            }
        }
    }

    @Test("Gallery pagination with ?limit works")
    func galleryLimit() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        for i in 0..<5 {
            try await env.insertCompleteImageAsset(
                id: "page-\(String(format: "%03d", i))",
                timestamp: "2025:02:05 \(String(format: "%02d", i)):00:00"
            )
        }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery?limit=2", method: .get) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.count == 2)
                #expect(gallery.total == 5) // total is still full count
            }
        }
    }

    @Test("Gallery pagination with ?offset works")
    func galleryOffset() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        for i in 0..<5 {
            try await env.insertCompleteImageAsset(
                id: "off-\(String(format: "%03d", i))",
                timestamp: "2025:02:05 \(String(format: "%02d", i)):00:00"
            )
        }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery?limit=100&offset=3", method: .get) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.count == 2) // 5 total, offset 3, so 2 remaining
            }
        }
    }

    @Test("Gallery pagination offset beyond total returns empty assets with stable total")
    func galleryOffsetBeyondTotal() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        for i in 0..<3 {
            _ = try await env.database.insertComplete(
                id: "off-beyond-\(String(format: "%03d", i))",
                type: .image,
                timestamp: "2025:02:05 \(String(format: "%02d", i)):00:00",
                originalName: "off-beyond-\(i).jpg",
                status: .complete
            )
        }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery?limit=2&offset=99", method: .get) { response in
                #expect(response.status == .ok)
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.isEmpty)
                #expect(gallery.total == 3)
            }
        }
    }

    @Test("Public gallery excludes moderated assets")
    func publicExcludesModerated() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "visible", timestamp: "2025:02:05 12:00:00")
        // Insert then moderate
        _ = try await env.database.insertQueued(id: "hidden", type: .image, originalName: "hidden.jpg")
        try await env.database.markComplete(id: "hidden", timestamp: "2025:02:05 13:00:00")
        try await env.database.updateStatus(id: "hidden", status: .moderated)

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.total == 1)
                #expect(gallery.assets.first?.id == "visible")
            }
        }
    }

    @Test("Empty gallery returns zero total and empty array")
    func emptyGallery() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.total == 0)
                #expect(gallery.assets.isEmpty)
            }
        }
    }

    @Test("Negative limit is clamped to 1, not treated as unlimited")
    func negativeLimitClamped() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        for i in 0..<5 {
            try await env.insertCompleteImageAsset(
                id: "neg-\(String(format: "%03d", i))",
                timestamp: "2025:02:05 \(String(format: "%02d", i)):00:00"
            )
        }
        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery?limit=-1", method: .get) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.count == 1) // clamped to 1, not 5
                #expect(gallery.total == 5)
            }
        }
    }

    @Test("Non-integer limit parameter falls back to default")
    func nonIntegerLimit() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        for i in 0..<3 {
            try await env.insertCompleteImageAsset(
                id: "abc-\(String(format: "%03d", i))",
                timestamp: "2025:02:05 \(String(format: "%02d", i)):00:00"
            )
        }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery?limit=abc", method: .get) { response in
                #expect(response.status == .ok)
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.count == 3) // falls back to default limit (100), all 3 returned
                #expect(gallery.total == 3)
            }
        }
    }

    @Test("Oversized limit is clamped to database max page size")
    func oversizedLimitClampedToMax() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let maxLimit = env.database.maxLimit
        let totalAssets = maxLimit + 1

        for i in 0..<totalAssets {
            _ = try await env.database.insertComplete(
                id: "clamp-\(String(format: "%04d", i))",
                type: .image,
                timestamp: "2025:02:06 \(String(format: "%02d", i % 24)):00:00",
                originalName: "clamp-\(i).jpg",
                status: .complete
            )
        }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery?limit=\(maxLimit * 10)", method: .get) { response in
                #expect(response.status == .ok)
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.count == maxLimit)
                #expect(gallery.total == totalAssets)
            }
        }
    }

    @Test("Gallery keeps total aligned with returned assets under concurrent writes")
    func galleryConsistentDuringConcurrentWrites() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "seed-000", timestamp: "2025:02:05 00:00:00")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            for i in 0..<300 {
                let id = "race-\(String(format: "%03d", i))"
                let writer = Task {
                    _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
                    try await env.database.markComplete(
                        id: id,
                        timestamp: "2025:02:06 \(String(format: "%02d", i % 24)):00:00"
                    )
                }

                try await client.execute(uri: "/api/gallery?limit=500", method: .get) { response in
                    #expect(response.status == .ok)
                    let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                    #expect(gallery.total == gallery.assets.count)
                }

                try await writer.value
            }
        }
    }
}

// MARK: - Internal Gallery (Moderation View)

@Suite("Internal Gallery")
struct InternalGalleryTests {

    @Test("Internal gallery includes status field")
    func includesStatus() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "int-001")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: routeTestsInternalAuthSecret]
            ) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.assets.first?.status == "complete")
            }
        }
    }

    @Test("Internal gallery shows both complete and moderated")
    func showsModerationTargets() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "vis-001")

        _ = try await env.database.insertQueued(id: "mod-001", type: .image, originalName: "mod.jpg")
        try await env.database.markComplete(id: "mod-001", timestamp: "2025:02:05 10:00:00")
        try await env.database.updateStatus(id: "mod-001", status: .moderated)

        // Also add a queued asset that should NOT appear
        _ = try await env.database.insertQueued(id: "queued-001", type: .video, originalName: "q.mov")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.authorization: routeTestsInternalAuthSecret]
            ) { response in
                let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                #expect(gallery.total == 2) // complete + moderated only
                let statuses = Set(gallery.assets.compactMap(\.status))
                #expect(statuses == Set(["complete", "moderated"]))
            }
        }
    }

    @Test("Internal gallery keeps total aligned with returned assets under concurrent writes")
    func internalGalleryConsistentDuringConcurrentWrites() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "int-seed-000", timestamp: "2025:02:05 00:00:00")

        let app = Application(router: env.internalRouter(internalAuthSecret: routeTestsInternalAuthSecret))
        try await app.test(.router) { client in
            for i in 0..<300 {
                let id = "int-race-\(String(format: "%03d", i))"
                let writer = Task {
                    _ = try await env.database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
                    try await env.database.markComplete(
                        id: id,
                        timestamp: "2025:02:06 \(String(format: "%02d", i % 24)):00:00"
                    )
                    if i.isMultiple(of: 2) {
                        try await env.database.updateStatus(id: id, status: .moderated)
                    }
                }

                try await client.execute(
                    uri: "/api/gallery?limit=500",
                    method: .get,
                    headers: [.authorization: routeTestsInternalAuthSecret]
                ) { response in
                    #expect(response.status == .ok)
                    let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                    #expect(gallery.total == gallery.assets.count)
                }

                try await writer.value
            }
        }
    }
}

// MARK: - File Serving

@Suite("File Serving")
struct FileServingTests {

    @Test("Thumbnail returns 200 with correct headers")
    func serveThumbnail() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "serve-001")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/serve-001", method: .get) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.contentType] == "image/jpeg")
                #expect(response.headers[.cacheControl] == "public, max-age=31536000, immutable")
                #expect(response.headers[.acceptRanges] == "bytes")
                #expect(response.body.readableBytes > 0)
            }
        }
    }

    @Test("Preview for image asset returns image/jpeg")
    func serveImagePreview() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "img-preview-001")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/img-preview-001", method: .get) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.contentType] == "image/jpeg")
            }
        }
    }

    @Test("Preview for video asset returns video/mp4")
    func serveVideoPreview() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteVideoAsset(id: "vid-preview-001")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/vid-preview-001", method: .get) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.contentType] == "video/mp4")
            }
        }
    }

    @Test("Missing thumbnail returns 404")
    func missingThumbnail() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/nonexistent", method: .get) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("Preview for unknown asset ID returns 404")
    func missingPreview() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/nonexistent", method: .get) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("Thumbnail returns 404 when asset exists in DB but file is missing from disk")
    func thumbnailFileMissing() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await env.insertCompleteImageAsset(id: "orphan-001")
        // Delete the thumbnail file
        try FileManager.default.removeItem(atPath: "\(env.thumbsDir)/orphan-001.jpg")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/orphan-001", method: .get) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("Thumbnail returns 404 for zero-byte file on disk")
    func thumbnailZeroByte() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }
        try await env.insertCompleteImageAsset(id: "empty-001")
        // Overwrite with empty file
        try Data().write(to: URL(fileURLWithPath: "\(env.thumbsDir)/empty-001.jpg"))

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/empty-001", method: .get) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("Thumbnail returns 410 Gone for failed image asset")
    func thumbnailGoneForFailed() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "fail-img-001", type: .image, originalName: "fail.jpg")
        try await env.database.updateStatus(id: "fail-img-001", status: .failed)
        try TestImage.writeJPEG(to: "\(env.thumbsDir)/fail-img-001.jpg")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/fail-img-001", method: .get) { response in
                #expect(response.status == .gone)
            }
        }
    }

    @Test("Preview returns 410 Gone for failed image asset")
    func previewGoneForFailedImage() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "fail-img-002", type: .image, originalName: "fail.jpg")
        try await env.database.updateStatus(id: "fail-img-002", status: .failed)
        try TestImage.writeJPEG(to: "\(env.previewsDir)/fail-img-002.jpg")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/fail-img-002", method: .get) { response in
                #expect(response.status == .gone)
            }
        }
    }

    @Test("Preview returns 410 Gone for failed video asset")
    func previewGoneForFailedVideo() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        _ = try await env.database.insertQueued(id: "fail-vid-001", type: .video, originalName: "fail.mov")
        try await env.database.updateStatus(id: "fail-vid-001", status: .failed)
        try Data(repeating: 0xFF, count: 1024).write(to: URL(fileURLWithPath: "\(env.previewsDir)/fail-vid-001.mp4"))

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/fail-vid-001", method: .get) { response in
                #expect(response.status == .gone)
            }
        }
    }

    @Test("Thumbnail still serves 200 for moderated asset")
    func thumbnailServesModerated() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "mod-serve-001")
        try await env.database.updateStatus(id: "mod-serve-001", status: .moderated)

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/mod-serve-001", method: .get) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.contentType] == "image/jpeg")
            }
        }
    }

    @Test("Preview still serves 200 for moderated asset")
    func previewServesModerated() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "mod-serve-002")
        try await env.database.updateStatus(id: "mod-serve-002", status: .moderated)

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/mod-serve-002", method: .get) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.contentType] == "image/jpeg")
            }
        }
    }

    @Test("Content-Length header is set correctly")
    func contentLength() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "cl-001")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/cl-001", method: .get) { response in
                #expect(response.status == .ok)
                let contentLength = response.headers[.contentLength]
                #expect(contentLength != nil)
                if let cl = contentLength.flatMap(Int.init) {
                    #expect(cl == response.body.readableBytes)
                }
            }
        }
    }
}

