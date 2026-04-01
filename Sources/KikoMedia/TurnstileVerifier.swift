import Foundation
import AsyncHTTPClient
import NIOCore
import OSLog
import KikoMediaCore

enum TurnstileVerificationResult: Sendable, Equatable {
    case success
    case rejected
    case unavailable(retryAfterSeconds: Int)
}

actor TurnstileVerifier {
    private let secret: String
    private let expectedHostname: String
    private let expectedAction: String
    private let expectedCData: String
    private let httpClient: HTTPClient
    private let verifyTimeout: Int
    private let maxResponse: Int
    private let maxInFlightVerifications: Int
    private let overloadRetryAfterSeconds: Int
    private var isShutdown = false
    private var inFlightVerifications = 0

    private static let siteverifyURL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    private static let maxVerifyAttempts = 2
    private static let retryDelayNanoseconds: UInt64 = 200_000_000

    init(config: Config, secret: String, expectedHostname: String, expectedAction: String, expectedCData: String) {
        self.secret = secret
        self.expectedHostname = expectedHostname
        self.expectedAction = expectedAction
        self.expectedCData = expectedCData
        self.verifyTimeout = config.turnstileVerifyTimeout
        self.maxResponse = config.turnstileMaxResponse
        self.maxInFlightVerifications = max(0, config.turnstileMaxInFlightVerifications)
        self.overloadRetryAfterSeconds = max(1, config.turnstileOverloadRetryAfterSeconds)
        self.httpClient = HTTPClient(eventLoopGroupProvider: .singleton)
    }

    func shutdown() async {
        guard !isShutdown else { return }
        isShutdown = true
        do {
            try await httpClient.shutdown().get()
        } catch {
            Logger.kiko.warning("Turnstile HTTP client shutdown error: \(error)")
        }
    }

    func verify(token: String) async -> TurnstileVerificationResult {
        guard !isShutdown else {
            return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
        }

        if Task.isCancelled {
            return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
        }

        if maxInFlightVerifications <= 0 || inFlightVerifications >= maxInFlightVerifications {
            return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
        }

        inFlightVerifications += 1
        defer { inFlightVerifications -= 1 }

        let idempotencyKey = UUID().uuidString

        for attempt in 1...Self.maxVerifyAttempts {
            if Task.isCancelled {
                return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
            }

            do {
                var request = HTTPClientRequest(url: Self.siteverifyURL)
                request.method = .POST
                request.headers.add(name: "Content-Type", value: "application/json")

                let body = SiteverifyRequest(secret: secret, response: token, idempotencyKey: idempotencyKey)
                let bodyData = try JSONEncoder().encode(body)
                request.body = .bytes(bodyData)

                let response = try await httpClient.execute(request, timeout: .seconds(Int64(verifyTimeout)))
                let statusCode = response.status.code
                guard response.status == .ok else {
                    if Self.shouldRetry(statusCode: statusCode, attempt: attempt) {
                        Logger.kiko.warning("Turnstile siteverify transient status \(statusCode), retrying")
                        do {
                            try await Task.sleep(nanoseconds: Self.retryDelayNanoseconds)
                        } catch {
                            return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
                        }
                        continue
                    }
                    Logger.kiko.warning("Turnstile siteverify returned status \(statusCode)")
                    return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
                }

                let responseBody = try await response.body.collect(upTo: maxResponse)
                let result = try JSONDecoder().decode(SiteverifyResponse.self, from: responseBody)

                if !result.success {
                    let codes = result.errorCodes?.joined(separator: ", ") ?? "none"
                    Logger.kiko.info("Turnstile verification failed: \(codes)")
                    return .rejected
                }

                guard let hostname = result.hostname else {
                    Logger.kiko.warning("Turnstile verification failed: missing hostname in siteverify response")
                    return .rejected
                }

                let normalizedHostname = Self.normalizeHostname(hostname)
                guard normalizedHostname == expectedHostname else {
                    Logger.kiko.warning(
                        "Turnstile verification failed: hostname mismatch expected '\(self.expectedHostname)', got '\(normalizedHostname)'"
                    )
                    return .rejected
                }

                guard let action = result.action else {
                    Logger.kiko.warning("Turnstile verification failed: missing action in siteverify response")
                    return .rejected
                }
                let normalizedAction = Self.normalizeVerificationField(action)
                guard normalizedAction == expectedAction else {
                    Logger.kiko.warning(
                        "Turnstile verification failed: action mismatch expected '\(self.expectedAction)', got '\(normalizedAction)'"
                    )
                    return .rejected
                }

                guard let cdata = result.cdata else {
                    Logger.kiko.warning("Turnstile verification failed: missing cdata in siteverify response")
                    return .rejected
                }
                let normalizedCData = Self.normalizeVerificationField(cdata)
                guard normalizedCData == expectedCData else {
                    Logger.kiko.warning(
                        "Turnstile verification failed: cdata mismatch expected '\(self.expectedCData)', got '\(normalizedCData)'"
                    )
                    return .rejected
                }

                return .success
            } catch {
                if error is CancellationError || Task.isCancelled {
                    return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
                }
                if attempt < Self.maxVerifyAttempts {
                    Logger.kiko.warning("Turnstile verification transient error, retrying")
                    do {
                        try await Task.sleep(nanoseconds: Self.retryDelayNanoseconds)
                    } catch {
                        return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
                    }
                    continue
                }
                Logger.kiko.error("Turnstile verification error (details redacted)")
                return .unavailable(retryAfterSeconds: overloadRetryAfterSeconds)
            }
        }

        preconditionFailure("unreachable: all verify attempts exhausted")
    }

    static func normalizeHostname(_ hostname: String) -> String {
        var normalized = hostname.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        while normalized.hasSuffix(".") {
            normalized.removeLast()
        }
        return normalized
    }

    static func normalizeVerificationField(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func shouldRetry(statusCode: UInt, attempt: Int) -> Bool {
        guard attempt < Self.maxVerifyAttempts else { return false }
        return statusCode == 408 || statusCode == 429 || statusCode >= 500
    }
}

private struct SiteverifyRequest: Encodable {
    let secret: String
    let response: String
    let idempotencyKey: String

    enum CodingKeys: String, CodingKey {
        case secret
        case response
        case idempotencyKey = "idempotency_key"
    }
}

private struct SiteverifyResponse: Decodable {
    let success: Bool
    let errorCodes: [String]?
    let challengeTs: String?
    let hostname: String?
    let action: String?
    let cdata: String?

    enum CodingKeys: String, CodingKey {
        case success
        case errorCodes = "error-codes"
        case challengeTs = "challenge_ts"
        case hostname
        case action
        case cdata
    }
}
