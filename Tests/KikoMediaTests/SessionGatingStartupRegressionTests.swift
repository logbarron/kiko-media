import Testing
import KikoMediaCore
@testable import KikoMediaApp

@Suite("Session Gating Startup Regression")
struct SessionGatingStartupRegressionTests {

    private func makeConfig(
        turnstileSecret: String? = "turnstile-secret",
        sessionHmacSecret: String?
    ) -> Config {
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

    @Test("Short SESSION_HMAC_SECRET refuses startup")
    func shortHmacSecretRefusesStartup() async {
        let shortSecret = String(repeating: "a", count: 31)
        let config = makeConfig(sessionHmacSecret: shortSecret)

        do {
            let (verifier, _) = try KikoMediaAppRuntime.configureSessionGating(config: config)
            await verifier.shutdown()
            Issue.record("Expected startup validation to reject short SESSION_HMAC_SECRET")
        } catch let error as SessionGatingConfigurationError {
            #expect(error == .hmacSecretTooShort(actualBytes: 31, minimumBytes: 32))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Empty or whitespace secrets refuse startup", arguments: [
        ("", "01234567890123456789012345678901"),
        ("   ", "01234567890123456789012345678901"),
        ("turnstile-secret", ""),
        ("turnstile-secret", String(repeating: " ", count: 32)),
    ])
    func emptyOrWhitespaceSecretsRefuseStartup(turnstileSecret: String, sessionHmacSecret: String) async {
        let config = makeConfig(
            turnstileSecret: turnstileSecret,
            sessionHmacSecret: sessionHmacSecret
        )

        do {
            let (verifier, _) = try KikoMediaAppRuntime.configureSessionGating(config: config)
            await verifier.shutdown()
            Issue.record("Expected startup validation to reject empty/whitespace secrets")
        } catch is SessionGatingConfigurationError {
            // Expected.
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Missing TURNSTILE_SECRET refuses startup")
    func missingTurnstileSecretRefusesStartup() async {
        let config = makeConfig(
            turnstileSecret: nil,
            sessionHmacSecret: "01234567890123456789012345678901"
        )

        do {
            let (verifier, _) = try KikoMediaAppRuntime.configureSessionGating(config: config)
            await verifier.shutdown()
            Issue.record("Expected startup validation to reject missing TURNSTILE_SECRET")
        } catch let error as SessionGatingConfigurationError {
            #expect(error == .missingSecrets)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}
