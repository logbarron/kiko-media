import Testing
import Foundation
import CryptoKit
import KikoMediaCore
@testable import KikoMediaApp

@Suite("SessionCookie")
struct SessionCookieTests {

    let cookie: SessionCookie

    init() throws {
        let env = try TestEnv()
        cookie = SessionCookie(config: env.config, hmacSecret: "test-secret-key-for-unit-tests")
    }

    @Test("Create then validate roundtrip succeeds")
    func roundtrip() {
        let value = cookie.create()
        #expect(cookie.validate(value))
    }

    @Test("Tampered signature fails validation")
    func tamperedSignature() {
        let value = cookie.create()
        let parts = value.split(separator: ".")
        var sig = Array(parts[1])
        // Flip the first character (avoids base64url padding-bit no-ops at the end)
        sig[0] = sig[0] == "A" ? "B" : "A"
        let tampered = "\(parts[0]).\(String(sig))"
        #expect(!cookie.validate(tampered))
    }

    @Test("Tampered expiry fails validation")
    func tamperedExpiry() {
        let value = cookie.create()
        let parts = value.split(separator: ".")
        // Replace expiry with different base64url but keep original signature
        let fakeExpiry = "AAAAAAAAAAA"
        let tampered = "\(fakeExpiry).\(parts[1])"
        #expect(!cookie.validate(tampered))
    }

    @Test("Cookie created with different secret fails validation")
    func wrongSecret() throws {
        let env = try TestEnv()
        let other = SessionCookie(config: env.config, hmacSecret: "different-secret")
        let value = other.create()
        #expect(!cookie.validate(value))
    }

    @Test("Malformed inputs return false without crashing")
    func malformedInputs() {
        #expect(!cookie.validate(""))
        #expect(!cookie.validate("nodot"))
        #expect(!cookie.validate("too.many.dots"))
        #expect(!cookie.validate("."))
        #expect(!cookie.validate(".."))
        #expect(!cookie.validate("abc.!!!notbase64!!!"))
        #expect(!cookie.validate("🎉.🎉"))
    }

    @Test("Expired cookie is rejected")
    func expiredCookie() {
        // Manually build a cookie with expiry far in the past (avoid flakiness if wall-clock time adjusts)
        let expiry = Int(Date().timeIntervalSince1970) - 3600
        let expiryData = withUnsafeBytes(of: expiry.bigEndian) { Data($0) }

        // Sign with the same secret to isolate the expiry check
        let secret = SymmetricKey(data: Data("test-secret-key-for-unit-tests".utf8))
        let sig = HMAC<SHA256>.authenticationCode(for: expiryData, using: secret)

        let expiryB64 = base64urlEncode(expiryData)
        let sigB64 = base64urlEncode(Data(sig))
        let value = "\(expiryB64).\(sigB64)"

        #expect(!cookie.validate(value))
    }

    @Test("Tusd header validation with valid cookie")
    func tusdHeadersValid() {
        let value = cookie.create()
        let headers: [String: [String]] = ["Cookie": ["kiko_session=\(value)"]]
        #expect(cookie.validateFromTusdHeaders(headers))
    }

    @Test("Tusd header validation accepts lowercase cookie header key")
    func tusdHeadersLowercaseCookieKey() {
        let value = cookie.create()
        let headers: [String: [String]] = ["cookie": ["kiko_session=\(value)"]]
        #expect(cookie.validateFromTusdHeaders(headers))
    }

    @Test("Tusd header validation rejects missing cookie")
    func tusdHeadersMissing() {
        #expect(!cookie.validateFromTusdHeaders(nil))
        #expect(!cookie.validateFromTusdHeaders([:]))
        #expect(!cookie.validateFromTusdHeaders(["Cookie": []]))
        #expect(!cookie.validateFromTusdHeaders(["Cookie": ["other=abc"]]))
    }

    @Test("Set-Cookie header has correct format and attributes")
    func setCookieHeaderFormat() {
        let value = cookie.create()
        let header = cookie.setCookieHeader(value: value)
        #expect(header.contains("kiko_session="))
        #expect(header.contains("HttpOnly"))
        #expect(header.contains("Secure"))
        #expect(header.contains("SameSite=Lax"))
        #expect(header.contains("Path=/"))
        #expect(header.contains("Max-Age=14400"))
    }

    @Test("Multiple cookies in header are parsed correctly")
    func multiCookieParsing() {
        let value = cookie.create()
        let headers: [String: [String]] = [
            "Cookie": ["other_cookie=xyz; kiko_session=\(value); another=abc"]
        ]
        #expect(cookie.validateFromTusdHeaders(headers))
    }

    @Test("Truncated expiry data (fewer than 8 bytes) fails validation")
    func truncatedExpiry() {
        // "AA" base64url-decodes to 1 byte (0x00), which is fewer than MemoryLayout<Int>.size (8)
        #expect(!cookie.validate("AA.BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"))
    }

    @Test("Expiry greater than Int.max is rejected")
    func expiryGreaterThanIntMax() {
        let overflowExpiry = UInt64(Int.max) + 1
        let expiryData = withUnsafeBytes(of: overflowExpiry.bigEndian) { Data($0) }

        // Sign with the same secret so this specifically exercises the Int overflow guard.
        let secret = SymmetricKey(data: Data("test-secret-key-for-unit-tests".utf8))
        let sig = HMAC<SHA256>.authenticationCode(for: expiryData, using: secret)

        let value = "\(base64urlEncode(expiryData)).\(base64urlEncode(Data(sig)))"
        #expect(!cookie.validate(value))
    }

    @Test("Truncated signature data fails validation")
    func truncatedSignature() {
        let expiryData = withUnsafeBytes(of: Int(1).bigEndian) { Data($0) }
        // "AA" decodes to a single byte, which is too short to be a valid SHA-256 HMAC.
        let value = "\(base64urlEncode(expiryData)).AA"
        #expect(!cookie.validate(value))
    }

    // MARK: - Base64URL helpers (duplicated from SessionCookie since they're private)

    private func base64urlEncode(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}
