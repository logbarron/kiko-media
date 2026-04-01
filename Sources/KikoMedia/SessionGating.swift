import Foundation
import KikoMediaCore

enum SessionGatingConfigurationError: Error, Equatable, LocalizedError {
    case missingSecrets
    case hmacSecretTooShort(actualBytes: Int, minimumBytes: Int)
    case missingTurnstileExpectedHostname
    case missingTurnstileExpectedAction
    case missingTurnstileExpectedCData

    var errorDescription: String? {
        switch self {
        case .missingSecrets:
            return "Missing TURNSTILE_SECRET or SESSION_HMAC_SECRET"
        case let .hmacSecretTooShort(actualBytes, minimumBytes):
            return "SESSION_HMAC_SECRET too short (\(actualBytes) bytes, minimum \(minimumBytes))"
        case .missingTurnstileExpectedHostname:
            return "Missing TURNSTILE_EXPECTED_HOSTNAME"
        case .missingTurnstileExpectedAction:
            return "Missing TURNSTILE_EXPECTED_ACTION"
        case .missingTurnstileExpectedCData:
            return "Missing TURNSTILE_EXPECTED_CDATA"
        }
    }
}

extension KikoMediaAppRuntime {
    static func configureSessionGating(config: Config) throws -> (TurnstileVerifier, SessionCookie) {
        guard let rawTurnstileSecret = config.turnstileSecret,
              let rawHmacSecret = config.sessionHmacSecret else {
            throw SessionGatingConfigurationError.missingSecrets
        }

        let turnstileSecret = rawTurnstileSecret.trimmingCharacters(in: .whitespacesAndNewlines)
        let hmacSecret = rawHmacSecret.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !turnstileSecret.isEmpty, !hmacSecret.isEmpty else {
            throw SessionGatingConfigurationError.missingSecrets
        }

        let minimumHmacBytes = 32
        let hmacBytes = hmacSecret.utf8.count
        guard hmacBytes >= minimumHmacBytes else {
            throw SessionGatingConfigurationError.hmacSecretTooShort(
                actualBytes: hmacBytes,
                minimumBytes: minimumHmacBytes
            )
        }

        let expectedHostname = TurnstileVerifier.normalizeHostname(config.turnstileExpectedHostname)
        guard !expectedHostname.isEmpty else {
            throw SessionGatingConfigurationError.missingTurnstileExpectedHostname
        }
        let expectedAction = TurnstileVerifier.normalizeVerificationField(config.turnstileExpectedAction)
        guard !expectedAction.isEmpty else {
            throw SessionGatingConfigurationError.missingTurnstileExpectedAction
        }
        let expectedCData = TurnstileVerifier.normalizeVerificationField(config.turnstileExpectedCData)
        guard !expectedCData.isEmpty else {
            throw SessionGatingConfigurationError.missingTurnstileExpectedCData
        }

        return (
            TurnstileVerifier(
                config: config,
                secret: turnstileSecret,
                expectedHostname: expectedHostname,
                expectedAction: expectedAction,
                expectedCData: expectedCData
            ),
            SessionCookie(config: config, hmacSecret: hmacSecret)
        )
    }
}
