import Testing
@testable import KikoMediaCore

@Suite("Asset ID Validation")
struct AssetValidationTests {

    @Test("Valid IDs are accepted")
    func validIds() {
        #expect(Asset.isValidId("abc123"))
        #expect(Asset.isValidId("abc-123_file"))
        #expect(Asset.isValidId("file.txt"))
        #expect(Asset.isValidId("a"))
        #expect(Asset.isValidId("UPPER-case-MIX"))
        #expect(Asset.isValidId("550e8400-e29b-41d4-a716-446655440000"))
    }

    @Test("Empty ID is rejected")
    func emptyId() {
        #expect(!Asset.isValidId(""))
    }

    @Test("Hidden file prefix is rejected", arguments: [
        ".hidden", ".", "..", ".env",
    ])
    func hiddenFile(id: String) {
        #expect(!Asset.isValidId(id))
    }

    @Test("Path traversal sequences are rejected", arguments: [
        "../etc/passwd", "../../secret", "./hidden",
        "path/../../escape", "..", "abc..def",
        "path/to/file", "/absolute",
    ])
    func pathTraversal(id: String) {
        #expect(!Asset.isValidId(id))
    }

    @Test("Backslash is rejected", arguments: [
        "path\\file", "\\",
    ])
    func backslash(id: String) {
        #expect(!Asset.isValidId(id))
    }

    @Test("Null byte is rejected", arguments: [
        "file\0name", "\0",
    ])
    func nullByte(id: String) {
        #expect(!Asset.isValidId(id))
    }

    @Test("Whitespace is rejected", arguments: [
        "file name", " leading", "trailing ", "tab\tname", "line\nname", "carriage\rname",
    ])
    func whitespace(id: String) {
        #expect(!Asset.isValidId(id))
    }

    @Test("Control characters are rejected", arguments: [
        "file\u{0001}name", "\u{001F}", "\u{007F}",
    ])
    func controlCharacters(id: String) {
        #expect(!Asset.isValidId(id))
    }

    @Test("Asset ID max length is enforced")
    func idMaxLength() {
        let maxLengthId = String(repeating: "a", count: 128)
        let tooLongId = String(repeating: "a", count: 129)

        #expect(Asset.isValidId(maxLengthId))
        #expect(!Asset.isValidId(tooLongId))
    }

    @Test("Original filename sanitizer replaces unsafe characters")
    func sanitizeOriginalNameUnsafeCharacters() {
        let sanitized = Asset.sanitizedOriginalName("  <script>alert(1)</script>/file\\name.jpg\n")
        #expect(sanitized == "_script_alert(1)__script__file_name.jpg")
    }

    @Test("Original filename sanitizer falls back for empty input")
    func sanitizeOriginalNameEmptyFallback() {
        #expect(Asset.sanitizedOriginalName(nil) == "unknown")
        #expect(Asset.sanitizedOriginalName(" \n\t ") == "unknown")
    }

    @Test("Original filename sanitizer falls back for control-character-only input")
    func sanitizeOriginalNameControlOnlyFallback() {
        let raw = "\u{0000}\u{0001}\u{0002}\n\r\t"
        #expect(Asset.sanitizedOriginalName(raw) == "unknown")
    }

    @Test("Original filename sanitizer limits max length")
    func sanitizeOriginalNameLengthCap() {
        let longName = String(repeating: "a", count: 300) + ".jpg"
        let sanitized = Asset.sanitizedOriginalName(longName)
        #expect(sanitized.count == 255)
    }
}
