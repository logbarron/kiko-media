import Testing
@testable import KikoMediaCore
@testable import KikoMediaApp

@Suite("ConfigTBConnectTimeout")
struct ConfigTBConnectTimeoutTests {
    private let key = "TB_CONNECT_TIMEOUT"
    private let defaultPreset = Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? "AVAssetExportPreset1920x1080"

    private func withTBConnectTimeout<T: Sendable>(
        _ value: String?,
        _ body: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try await TestEnvironment.withEnvironment([
            "TB_CONNECT_TIMEOUT": value,
            "VIDEO_TRANSCODE_PRESET": defaultPreset,
        ], body)
    }

    @Test("Config.load converts legacy second-based TB_CONNECT_TIMEOUT to milliseconds")
    func configLoadConvertsLegacySecondsToMilliseconds() async throws {
        try await withTBConnectTimeout("5") {
            let config = try Config.load()
            #expect(config.tbConnectTimeout == 5_000)
        }
    }

    @Test("Config.load preserves explicit millisecond TB_CONNECT_TIMEOUT values")
    func configLoadPreservesMilliseconds() async throws {
        try await withTBConnectTimeout("750") {
            let config = try Config.load()
            #expect(config.tbConnectTimeout == 750)
        }
    }

    @Test("Config.load falls back for malformed or out-of-range TB_CONNECT_TIMEOUT", arguments: [
        "not-a-number",
        "31",
    ])
    func configLoadFallsBackForMalformedOrOutOfRange(rawValue: String) async throws {
        let fallback = Config.intDefaults[key]!.fallback
        try await withTBConnectTimeout(rawValue) {
            let config = try Config.load()
            #expect(config.tbConnectTimeout == fallback)
        }
    }
}
