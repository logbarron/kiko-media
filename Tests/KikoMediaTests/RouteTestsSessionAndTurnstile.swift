import Testing
import Foundation
import CryptoKit
import Hummingbird
import HummingbirdTesting
import NIOCore
@testable import KikoMediaCore
@testable import KikoMediaApp

// MARK: - Health Endpoint

@Suite("Health Endpoint")
struct HealthRouteTests {

    @Test("GET /health returns 200 with status ok")
    func healthCheck() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let app = Application(router: env.publicRouter())
        try await app.test(.router) { client in
            try await client.execute(uri: "/health", method: .get) { response in
                #expect(response.status == .ok)
                let body = try JSONDecoder().decode(TestHealthResponse.self, from: response.body)
                #expect(body.status == "ok")
            }
        }
    }
}

// MARK: - Session Cookie Gating

@Suite("Session Gating")
struct SessionGatingTests {

    private let hmacSecret = "test-secret-key-for-session-gating"

    @Test("Public gallery rejects request without cookie when session gating is enabled")
    func galleryRejectsMissingCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Public gallery accepts request with valid cookie")
    func galleryAcceptsValidCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let value = cookie.create()
        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.cookie: "kiko_session=\(value)"]
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }

    @Test("Public gallery rejects tampered cookie")
    func galleryRejectsTamperedCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let tampered = cookie.create() + "A"
        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.cookie: "\(cookie.name)=\(tampered)"]
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Public thumbs rejects without cookie when session gating is enabled")
    func thumbsRejectsMissingCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/thumbs/any-id", method: .get) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Public preview rejects without cookie when session gating is enabled")
    func previewRejectsMissingCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/preview/any-id", method: .get) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("Public endpoints work without gating when session gating is disabled")
    func noGatingWhenDisabled() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        // No session cookie validator means public session gating is disabled.
        let app = Application(router: env.publicRouter(sessionCookie: nil))
        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                #expect(response.status == .ok)
            }
        }
    }

    @Test("Expired cookie is rejected")
    func expiredCookieRejected() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        // Build an expired cookie manually
        let expiry = Int(Date().timeIntervalSince1970) - 1
        let expiryData = withUnsafeBytes(of: expiry.bigEndian) { Data($0) }
        let secret = SymmetricKey(data: Data(hmacSecret.utf8))
        let sig = HMAC<SHA256>.authenticationCode(for: expiryData, using: secret)
        let expiryB64 = expiryData.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
        let sigB64 = Data(sig).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
        let expiredValue = "\(expiryB64).\(sigB64)"

        let app = Application(router: env.publicRouter(sessionCookie: cookie))
        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.cookie: "kiko_session=\(expiredValue)"]
            ) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }
}

@Suite("Session Gating Startup Validation")
struct SessionGatingStartupValidationTests {

    private func makeConfig(turnstileSecret: String?, sessionHmacSecret: String?) -> Config {
        Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: "/tmp/kiko-test-uploads",
            thumbsDir: "/tmp/kiko-test-thumbs",
            previewsDir: "/tmp/kiko-test-previews",
            logsDir: "/tmp/kiko-test-logs",
            externalSSDPath: "/tmp/kiko-test-ssd",
            databasePath: "/tmp/kiko-test.db",
            turnstileSecret: turnstileSecret,
            sessionHmacSecret: sessionHmacSecret,
            turnstileExpectedHostname: "photos.example.com",
            turnstileExpectedAction: "kiko_verify",
            turnstileExpectedCData: "kiko_public"
        )
    }

    @Test("Startup validation throws when secrets are missing")
    func missingSecretsFailClosed() async {
        let config = makeConfig(turnstileSecret: nil, sessionHmacSecret: nil)
        do {
            let (verifier, _) = try KikoMediaAppRuntime.configureSessionGating(config: config)
            await verifier.shutdown()
            Issue.record("Expected startup validation to reject missing secrets")
        } catch let error as SessionGatingConfigurationError {
            #expect(error == .missingSecrets)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Startup validation succeeds with valid secrets")
    func validSecretsSucceed() async throws {
        let hmacSecret = String(repeating: "a", count: 32)
        let config = makeConfig(turnstileSecret: "turnstile-secret", sessionHmacSecret: hmacSecret)
        let (verifier, sessionCookie) = try KikoMediaAppRuntime.configureSessionGating(config: config)

        let value = sessionCookie.create()
        #expect(sessionCookie.validate(value))
        await verifier.shutdown()
    }

    @Test("Startup validation throws when TURNSTILE_EXPECTED_HOSTNAME is missing")
    func missingExpectedHostnameFailsClosed() async {
        let hmacSecret = String(repeating: "a", count: 32)
        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: "/tmp/kiko-test-uploads",
            thumbsDir: "/tmp/kiko-test-thumbs",
            previewsDir: "/tmp/kiko-test-previews",
            logsDir: "/tmp/kiko-test-logs",
            externalSSDPath: "/tmp/kiko-test-ssd",
            databasePath: "/tmp/kiko-test.db",
            turnstileSecret: "turnstile-secret",
            sessionHmacSecret: hmacSecret,
            turnstileExpectedHostname: ""
        )

        do {
            let (verifier, _) = try KikoMediaAppRuntime.configureSessionGating(config: config)
            await verifier.shutdown()
            Issue.record("Expected startup validation to reject missing expected hostname")
        } catch let error as SessionGatingConfigurationError {
            #expect(error == .missingTurnstileExpectedHostname)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Startup validation throws when TURNSTILE_EXPECTED_ACTION is missing")
    func missingExpectedActionFailsClosed() async {
        let hmacSecret = String(repeating: "a", count: 32)
        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: "/tmp/kiko-test-uploads",
            thumbsDir: "/tmp/kiko-test-thumbs",
            previewsDir: "/tmp/kiko-test-previews",
            logsDir: "/tmp/kiko-test-logs",
            externalSSDPath: "/tmp/kiko-test-ssd",
            databasePath: "/tmp/kiko-test.db",
            turnstileSecret: "turnstile-secret",
            sessionHmacSecret: hmacSecret,
            turnstileExpectedHostname: "photos.example.com",
            turnstileExpectedAction: "",
            turnstileExpectedCData: "kiko_public"
        )

        do {
            let (verifier, _) = try KikoMediaAppRuntime.configureSessionGating(config: config)
            await verifier.shutdown()
            Issue.record("Expected startup validation to reject missing expected action")
        } catch let error as SessionGatingConfigurationError {
            #expect(error == .missingTurnstileExpectedAction)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Startup validation throws when TURNSTILE_EXPECTED_CDATA is missing")
    func missingExpectedCDataFailsClosed() async {
        let hmacSecret = String(repeating: "a", count: 32)
        let config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: "/tmp/kiko-test-uploads",
            thumbsDir: "/tmp/kiko-test-thumbs",
            previewsDir: "/tmp/kiko-test-previews",
            logsDir: "/tmp/kiko-test-logs",
            externalSSDPath: "/tmp/kiko-test-ssd",
            databasePath: "/tmp/kiko-test.db",
            turnstileSecret: "turnstile-secret",
            sessionHmacSecret: hmacSecret,
            turnstileExpectedHostname: "photos.example.com",
            turnstileExpectedAction: "kiko_verify",
            turnstileExpectedCData: ""
        )

        do {
            let (verifier, _) = try KikoMediaAppRuntime.configureSessionGating(config: config)
            await verifier.shutdown()
            Issue.record("Expected startup validation to reject missing expected cdata")
        } catch let error as SessionGatingConfigurationError {
            #expect(error == .missingTurnstileExpectedCData)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}

// MARK: - Turnstile Verify Endpoint

@Suite("Turnstile Verify Endpoint")
struct TurnstileVerifyRouteTests {

    private let hmacSecret = "test-secret-key-for-session-gating"

    @Test("POST /api/turnstile/verify returns 503 with Retry-After when verifier is overloaded")
    func verifyOverloadBackpressure() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        // Use maxInFlight=0 to force deterministic overload behavior without network calls.
        let config = Config(
            publicPort: env.config.publicPort,
            internalPort: env.config.internalPort,
            uploadDir: env.config.uploadDir,
            thumbsDir: env.config.thumbsDir,
            previewsDir: env.config.previewsDir,
            logsDir: env.config.logsDir,
            externalSSDPath: env.config.externalSSDPath,
            databasePath: env.config.databasePath,
            turnstileSecret: nil,
            sessionHmacSecret: nil,
            turnstileMaxInFlightVerifications: 0,
            turnstileOverloadRetryAfterSeconds: 2
        )

        let cookie = SessionCookie(config: config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(router: env.publicRouter(sessionCookie: cookie, turnstileVerifier: verifier))

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test"}"#)
                ) { response in
                    #expect(response.status == .serviceUnavailable)
                    #expect(response.headers[.retryAfter] == "\(config.turnstileOverloadRetryAfterSeconds)")
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify returns 503 Retry-After and no cookie when verifier is unavailable")
    func verifyReturnsUnavailableWhenTurnstileUnavailable() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                turnstileVerify: { _ in .unavailable(retryAfterSeconds: 7) }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test"}"#)
                ) { response in
                    #expect(response.status == .serviceUnavailable)
                    #expect(response.headers[.retryAfter] == "7")
                    #expect(response.headers[.setCookie] == nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify returns 400 and no cookie for malformed JSON")
    func verifyRejectsMalformedJSONBody() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"#)
                ) { response in
                    #expect(response.status == .badRequest)
                    #expect(response.headers[.setCookie] == nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify returns 413 and no cookie for oversized JSON body")
    func verifyRejectsOversizedJSONBody() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                turnstileVerify: { _ in .success }
            )
        )
        let oversizedToken = String(repeating: "a", count: RequestBodyLimits.defaultJSONMaxBytes + 1024)
        let payload = #"{"token":"\#(oversizedToken)"}"#

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: payload)
                ) { response in
                    #expect(response.status == .contentTooLarge)
                    #expect(response.headers[.setCookie] == nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify returns 400 and no cookie when token is missing")
    func verifyRejectsMissingTokenField() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{}"#)
                ) { response in
                    #expect(response.status == .badRequest)
                    #expect(response.headers[.setCookie] == nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify rejects when gate is enabled and no gate proof is provided")
    func verifyRejectsMissingGateProofWhenEnabled() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                gateSecret: "event-gate-secret",
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test"}"#)
                ) { response in
                    #expect(response.status == .forbidden)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify returns 403 and no cookie when verifier rejects token")
    func verifyRejectsWhenTurnstileReturnsRejected() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                turnstileVerify: { _ in .rejected }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test"}"#)
                ) { response in
                    #expect(response.status == .forbidden)
                    #expect(response.headers[.setCookie] == nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify issues a usable session cookie on success")
    func verifyIssuesUsableSessionCookieOnSuccess() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        actor IssuedCookieStore {
            private var value: String?

            func set(_ value: String?) {
                self.value = value
            }

            func get() -> String? {
                value
            }
        }

        try await env.insertCompleteImageAsset(id: "verify-cookie-001")

        let tokenPayload = #"{"token":"0.success"}"#
        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                turnstileVerify: { _ in .success }
            )
        )

        do {
            let issuedCookieStore = IssuedCookieStore()
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: tokenPayload)
                ) { response in
                    #expect(response.status == .noContent)
                    let setCookieHeader = response.headers[.setCookie]
                    #expect(setCookieHeader != nil)
                    await issuedCookieStore.set(
                        setCookieHeader?
                            .split(separator: ";", maxSplits: 1)
                            .first
                            .map(String.init)
                    )
                }

                let issuedCookiePair = await issuedCookieStore.get()
                guard let issuedCookiePair else {
                    Issue.record("Expected verify route to issue a session cookie")
                    return
                }

                let cookieValue = issuedCookiePair.dropFirst(cookie.name.count + 1)
                #expect(cookie.validate(String(cookieValue)))

                try await client.execute(
                    uri: "/api/gallery",
                    method: .get,
                    headers: [.cookie: issuedCookiePair]
                ) { response in
                    #expect(response.status == .ok)
                    let gallery = try JSONDecoder().decode(TestGalleryResponse.self, from: response.body)
                    #expect(gallery.total == 1)
                    #expect(gallery.assets.first?.id == "verify-cookie-001")
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify accepts canonical gateSecret when gate is enabled")
    func verifyAcceptsGateSecretWhenGateEnabled() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let gateSecret = "event-gate-secret"
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                gateSecret: gateSecret,
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test","gateSecret":"\#(gateSecret)"}"#)
                ) { response in
                    #expect(response.status == .noContent)
                    #expect(response.headers[.setCookie] != nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify accepts legacy password alias when gate is enabled")
    func verifyAcceptsPasswordAliasWhenGateEnabled() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let gateSecret = "event-gate-secret"
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                gateSecret: gateSecret,
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test","password":"\#(gateSecret)"}"#)
                ) { response in
                    #expect(response.status == .noContent)
                    #expect(response.headers[.setCookie] != nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify accepts legacy inviteToken alias when gate is enabled")
    func verifyAcceptsInviteTokenAliasWhenGateEnabled() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let gateSecret = "event-gate-secret"
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                gateSecret: gateSecret,
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test","inviteToken":"\#(gateSecret)"}"#)
                ) { response in
                    #expect(response.status == .noContent)
                    #expect(response.headers[.setCookie] != nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }

    @Test("POST /api/turnstile/verify rejects invalid gate proof when gate is enabled")
    func verifyRejectsInvalidGateProofWhenEnabled() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                gateSecret: "event-gate-secret",
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test","gateSecret":"wrong","password":"wrong","inviteToken":"wrong"}"#)
                ) { response in
                    #expect(response.status == .forbidden)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }


    @Test("POST /api/turnstile/verify keeps Turnstile-only behavior when gate is disabled")
    func verifyAllowsTokenOnlyWhenGateDisabled() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let cookie = SessionCookie(config: env.config, hmacSecret: hmacSecret)
        let verifier = TurnstileVerifier(
            config: env.config,
            secret: "test-turnstile-secret",
            expectedHostname: "photos.example.com",
            expectedAction: "kiko_verify",
            expectedCData: "kiko_public"
        )
        let app = Application(
            router: env.publicRouter(
                sessionCookie: cookie,
                turnstileVerifier: verifier,
                gateSecret: nil,
                turnstileVerify: { _ in .success }
            )
        )

        do {
            try await app.test(.router) { client in
                try await client.execute(
                    uri: "/api/turnstile/verify",
                    method: .post,
                    headers: [.contentType: "application/json"],
                    body: ByteBuffer(string: #"{"token":"0.test"}"#)
                ) { response in
                    #expect(response.status == .noContent)
                    #expect(response.headers[.setCookie] != nil)
                }
            }
        } catch {
            await verifier.shutdown()
            throw error
        }

        await verifier.shutdown()
    }
}
