import Foundation
import CryptoKit
import Hummingbird
import KikoMediaCore

struct SessionCookie: Sendable {
    let name: String
    let ttlSeconds: Int

    private let hmacSecret: SymmetricKey

    init(config: Config, hmacSecret: String) {
        self.name = config.sessionCookieName
        self.ttlSeconds = config.sessionCookieTTL
        self.hmacSecret = SymmetricKey(data: Data(hmacSecret.utf8))
    }

    func create() -> String {
        let expiry = Int(Date().timeIntervalSince1970) + ttlSeconds
        let expiryData = withUnsafeBytes(of: expiry.bigEndian) { Data($0) }
        let expiryB64 = base64urlEncode(expiryData)

        let signature = HMAC<SHA256>.authenticationCode(for: expiryData, using: hmacSecret)
        let signatureB64 = base64urlEncode(Data(signature))

        return "\(expiryB64).\(signatureB64)"
    }

    func validate(_ value: String) -> Bool {
        let parts = value.split(separator: ".")
        guard parts.count == 2 else { return false }

        guard let expiryData = base64urlDecode(String(parts[0])),
              let signatureData = base64urlDecode(String(parts[1])) else {
            return false
        }

        guard HMAC<SHA256>.isValidAuthenticationCode(signatureData, authenticating: expiryData, using: hmacSecret) else {
            return false
        }

        // Data bytes are not guaranteed to be aligned for Int loads.
        guard expiryData.count == MemoryLayout<Int>.size else { return false }
        var expiryU64: UInt64 = 0
        for b in expiryData {
            expiryU64 = (expiryU64 << 8) | UInt64(b)
        }
        guard expiryU64 <= UInt64(Int.max) else { return false }
        let expiry = Int(expiryU64)
        let now = Int(Date().timeIntervalSince1970)

        return now < expiry
    }

    func extractFromRequest(_ request: Request) -> String? {
        guard let cookieHeader = request.headers[.cookie] else { return nil }
        return parseCookieValue(from: cookieHeader)
    }

    func setCookieHeader(value: String) -> String {
        "\(name)=\(value); HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=\(ttlSeconds)"
    }

    private func base64urlEncode(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private func base64urlDecode(_ string: String) -> Data? {
        var s = string
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")

        let remainder = s.count % 4
        if remainder > 0 {
            s += String(repeating: "=", count: 4 - remainder)
        }

        return Data(base64Encoded: s)
    }

    private func parseCookieValue(from header: String) -> String? {
        for part in header.split(separator: ";") {
            let trimmed = part.trimmingCharacters(in: .whitespaces)
            let kv = trimmed.split(separator: "=", maxSplits: 1)
            if kv.count == 2 && kv[0] == name {
                return String(kv[1])
            }
        }
        return nil
    }
}

extension SessionCookie {
    func validateFromTusdHeaders(_ headers: [String: [String]]?) -> Bool {
        guard let headers = headers,
              let cookieValues = headers["Cookie"] ?? headers.first(where: { $0.key.caseInsensitiveCompare("Cookie") == .orderedSame })?.value,
              let cookieHeader = cookieValues.first else {
            return false
        }
        guard let value = parseCookieValue(from: cookieHeader) else { return false }
        return validate(value)
    }
}
