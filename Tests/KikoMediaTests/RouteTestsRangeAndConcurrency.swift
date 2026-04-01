import Testing
import Foundation
import CryptoKit
import Hummingbird
import HummingbirdTesting
import NIOCore
@testable import KikoMediaCore
@testable import KikoMediaApp

// MARK: - Range Requests

@Suite("Range Requests")
struct RangeRequestTests {

    @Test("Valid byte range returns 206 with Content-Range header")
    func validRange() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "range-001")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/thumbs/range-001",
                method: .get,
                headers: [.range: "bytes=0-99"]
            ) { response in
                #expect(response.status == .partialContent)
                let contentRange = response.headers[.contentRange]
                #expect(contentRange != nil)
                #expect(contentRange?.hasPrefix("bytes 0-99/") == true)
                #expect(response.headers[.contentLength] == "100")
                #expect(response.body.readableBytes == 100)
            }
        }
    }

    @Test("Suffix range (bytes=-N) returns last N bytes with 206")
    func suffixRange() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "range-suffix")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/thumbs/range-suffix",
                method: .get,
                headers: [.range: "bytes=-50"]
            ) { response in
                #expect(response.status == .partialContent)
                let contentRange = response.headers[.contentRange]
                #expect(contentRange != nil)
                #expect(response.body.readableBytes == 50)
            }
        }
    }

    @Test("Open-end range (bytes=N-) returns from offset to end with 206")
    func openEndRange() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "range-open")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            // First get full size
            var fullSize = 0
            try await client.execute(uri: "/api/thumbs/range-open", method: .get) { response in
                fullSize = response.body.readableBytes
            }

            // Then request from byte 100 to end
            try await client.execute(
                uri: "/api/thumbs/range-open",
                method: .get,
                headers: [.range: "bytes=100-"]
            ) { response in
                #expect(response.status == .partialContent)
                #expect(response.body.readableBytes == fullSize - 100)
            }
        }
    }

    @Test("Malformed range header returns 416 with bytes */size", arguments: [
        "not-a-range",          // No bytes= prefix
        "bytes=abc-def",        // Non-numeric start/end
        "bytes=0-abc",          // Non-numeric end token
        "bytes=0--1",           // Invalid syntax
        "bytes=0-+1",           // Signed malformed end token
        "bytes=+0-99",          // Signed malformed start token
        "bytes=-+50",           // Signed malformed suffix token
    ])
    func malformedRangeRejected(rangeHeader: String) async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "range-invalid")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            var fullSize = 0
            try await client.execute(uri: "/api/thumbs/range-invalid", method: .get) { response in
                fullSize = response.body.readableBytes
            }

            try await client.execute(
                uri: "/api/thumbs/range-invalid",
                method: .get,
                headers: [.range: rangeHeader]
            ) { response in
                #expect(response.status == .rangeNotSatisfiable)
                #expect(response.headers[.contentRange] == "bytes */\(fullSize)")
                #expect(response.body.readableBytes == 0)
            }
        }
    }

    @Test("Unsatisfiable range returns 416 with bytes */size", arguments: [
        "bytes=100-50",         // Reversed (start > end)
        "bytes=999999-999999",  // Start beyond EOF
    ])
    func unsatisfiableRange(rangeHeader: String) async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteImageAsset(id: "range-oob")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            var fullSize = 0
            try await client.execute(uri: "/api/thumbs/range-oob", method: .get) { response in
                fullSize = response.body.readableBytes
            }

            try await client.execute(
                uri: "/api/thumbs/range-oob",
                method: .get,
                headers: [.range: rangeHeader]
            ) { response in
                #expect(response.status == .rangeNotSatisfiable)
                #expect(response.headers[.contentRange] == "bytes */\(fullSize)")
                #expect(response.body.readableBytes == 0)
            }
        }
    }

    @Test("Range request on video preview returns 206")
    func videoRangeRequest() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        try await env.insertCompleteVideoAsset(id: "range-vid")

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/preview/range-vid",
                method: .get,
                headers: [.range: "bytes=0-99"]
            ) { response in
                #expect(response.status == .partialContent)
                #expect(response.headers[.contentType] == "video/mp4")
                #expect(response.headers[.contentRange] != nil)
            }
        }
    }
}

// MARK: - Concurrency

@Suite("Concurrency")
struct ConcurrencyTests {

    @Test("30 concurrent gallery requests all succeed")
    func concurrentGalleryRequests() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        for i in 0..<5 {
            try await env.insertCompleteImageAsset(
                id: "conc-\(String(format: "%03d", i))",
                timestamp: "2025:02:05 \(String(format: "%02d", i)):00:00"
            )
        }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await withThrowingTaskGroup(of: Void.self) { group in
                for _ in 0..<30 {
                    group.addTask {
                        try await client.execute(uri: "/api/gallery", method: .get) { response in
                            #expect(response.status == .ok)
                            let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                            #expect(gallery.total == 5)
                            #expect(gallery.assets.count == 5)
                        }
                    }
                }
                for try await _ in group {}
            }
        }
    }
}

