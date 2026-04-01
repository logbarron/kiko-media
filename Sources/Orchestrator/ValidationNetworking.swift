import Foundation
import Dispatch
import Darwin

// MARK: - Validation

func validateDomain(_ domain: String) -> Bool {
    let cleaned = domain.hasSuffix(".") ? String(domain.dropLast()) : domain
    let parts = cleaned.split(separator: ".")
    guard parts.count >= 2 else { return false }
    guard let tld = parts.last, tld.count >= 2, tld.allSatisfy({ $0.isLetter }) else { return false }
    return parts.allSatisfy { part in
        !part.isEmpty && part.count <= 63 &&
        part.allSatisfy { $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "-") } &&
        !part.hasPrefix("-") && !part.hasSuffix("-")
    }
}

func validateIPv4(_ ip: String) -> Bool {
    var addr = in_addr()
    return ip.withCString { inet_pton(AF_INET, $0, &addr) == 1 }
}

func validateCloudflareTokenFormat(_ token: String) -> Bool {
    // Keep local validation permissive to avoid false negatives if Cloudflare changes token format.
    // We only reject obviously bad input before attempting API verification.
    guard token.count >= 20 else { return false }
    return !token.contains(where: \.isWhitespace)
}

func validateSessionHmacSecret(_ secret: String) -> Bool {
    // Mirrors app enforcement: < 32 bytes refuses startup.
    secret.utf8.count >= 32
}

func validateGateSecret(_ secret: String) -> Bool {
    secret.utf8.count >= 8
}

// MARK: - Network Verification (Best-Effort)

enum VerificationResult {
    case valid
    case invalid(String)
    case unavailable(String)  // network issues, rate limit, unexpected responses
}


func fetch(_ request: URLRequest) -> Result<(HTTPURLResponse, Data), Error> {
    let sem = DispatchSemaphore(value: 0)
    nonisolated(unsafe) var result: Result<(HTTPURLResponse, Data), Error>?

    let task = URLSession.shared.dataTask(with: request) { data, response, error in
        defer { sem.signal() }
        if let error = error {
            result = .failure(error)
            return
        }
        guard let http = response as? HTTPURLResponse else {
            result = .failure(URLError(.badServerResponse))
            return
        }
        result = .success((http, data ?? Data()))
    }

    task.resume()

    let wait = sem.wait(timeout: .now() + request.timeoutInterval + 1)
    if wait == .timedOut {
        task.cancel()
        return .failure(URLError(.timedOut))
    }
    return result ?? .failure(URLError(.unknown))
}

struct CloudflareTokenVerifyResponse: Decodable {
    struct Result: Decodable {
        let status: String?
    }
    struct APIError: Decodable {
        let code: Int?
        let message: String?
    }

    let success: Bool
    let result: Result?
    let errors: [APIError]?
}

func verifyCloudflareToken(_ token: String) -> VerificationResult {
    guard let url = URL(string: "https://api.cloudflare.com/client/v4/user/tokens/verify") else {
        return .unavailable("internal error (bad verify URL)")
    }

    var req = URLRequest(url: url)
    req.httpMethod = "GET"
    req.timeoutInterval = 10
    req.setValue("application/json", forHTTPHeaderField: "Accept")
    req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

    switch fetch(req) {
    case .failure(let error):
        return .unavailable(error.localizedDescription)
    case .success(let (http, data)):
        if http.statusCode == 429 || (500...599).contains(http.statusCode) {
            return .unavailable("Cloudflare API returned HTTP \(http.statusCode)")
        }

        let decoder = JSONDecoder()
        guard let parsed = try? decoder.decode(CloudflareTokenVerifyResponse.self, from: data) else {
            if (400...499).contains(http.statusCode) {
                return .invalid("request rejected (HTTP \(http.statusCode)); check token format/permissions")
            }
            // If this isn't decodable, the safest thing is to warn and let the user continue.
            return .unavailable("unexpected response format (HTTP \(http.statusCode))")
        }

        if parsed.success {
            if let status = parsed.result?.status, status.lowercased() != "active" {
                return .invalid("token status is '\(status)'")
            }
            return .valid
        }

        let msg = parsed.errors?.first?.message ?? "verification failed"
        if http.statusCode == 200 || (400...499).contains(http.statusCode) {
            return .invalid(msg)
        }
        return .unavailable(msg)
    }
}

struct TurnstileSiteverifyResponse: Decodable {
    let success: Bool
    let errorCodes: [String]?

    enum CodingKeys: String, CodingKey {
        case success
        case errorCodes = "error-codes"
    }
}

struct TurnstileSiteverifyRequest: Encodable {
    let secret: String
    let response: String
}

func verifyTurnstileSecret(_ secret: String) -> VerificationResult {
    guard let url = URL(string: "https://challenges.cloudflare.com/turnstile/v0/siteverify") else {
        return .unavailable("internal error (bad siteverify URL)")
    }

    var req = URLRequest(url: url)
    req.httpMethod = "POST"
    req.timeoutInterval = 10
    req.setValue("application/json", forHTTPHeaderField: "Accept")
    req.setValue("application/json", forHTTPHeaderField: "Content-Type")

    // Use a non-empty dummy response so we can distinguish invalid secret vs invalid response.
    let payload = TurnstileSiteverifyRequest(secret: secret, response: "setup-wizard-sanity-check")
    req.httpBody = try? JSONEncoder().encode(payload)

    switch fetch(req) {
    case .failure(let error):
        return .unavailable(error.localizedDescription)
    case .success(let (http, data)):
        if http.statusCode == 429 || (500...599).contains(http.statusCode) {
            return .unavailable("Turnstile API returned HTTP \(http.statusCode)")
        }

        let decoder = JSONDecoder()
        guard let parsed = try? decoder.decode(TurnstileSiteverifyResponse.self, from: data) else {
            return .unavailable("unexpected response format (HTTP \(http.statusCode))")
        }

        if parsed.success {
            return .valid
        }

        let codes = parsed.errorCodes ?? []
        if codes.contains("invalid-input-secret") || codes.contains("missing-input-secret") {
            return .invalid("invalid secret key")
        }

        // Expected for a dummy response: invalid-input-response (secret looks valid).
        if codes.contains("invalid-input-response") || codes.contains("missing-input-response") {
            return .valid
        }

        // If we can't confidently classify it, warn and let the user proceed.
        if codes.isEmpty {
            return .unavailable("siteverify failed without error codes")
        }
        return .unavailable("siteverify error: \(codes.joined(separator: ", "))")
    }
}
