import Testing
import Foundation
@testable import KikoMediaCore
@testable import KikoMediaApp

private func expectedExifTimestamp(for date: Date, timeZone: TimeZone) -> String {
    let formatter = DateFormatter()
    formatter.calendar = Calendar(identifier: .gregorian)
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.timeZone = timeZone
    formatter.dateFormat = "yyyy:MM:dd HH:mm:ss"
    return formatter.string(from: date)
}

@Suite("Date Formatting", Testing.ParallelizationTrait.serialized)
struct DateUtilsTests {
    init() {
        DateUtils.configure(eventTimezone: "")
    }

    @Test("EXIF timestamp format is YYYY:MM:DD HH:MM:SS")
    func formatPattern() {
        let date = Date(timeIntervalSince1970: 0) // 1970-01-01 UTC
        let result = DateUtils.exifTimestamp(from: date)

        // Should match the pattern regardless of timezone
        let parts = result.split(separator: " ")
        #expect(parts.count == 2)

        let dateParts = parts[0].split(separator: ":")
        #expect(dateParts.count == 3) // YYYY:MM:DD

        let timeParts = parts[1].split(separator: ":")
        #expect(timeParts.count == 3) // HH:MM:SS
    }

    @Test("EXIF timestamp has correct field widths")
    func fieldWidths() {
        let result = DateUtils.exifTimestamp(from: Date())

        // Full format is exactly 19 characters: "YYYY:MM:DD HH:MM:SS"
        #expect(result.count == 19)

        // Space at position 10
        let chars = Array(result)
        #expect(chars[10] == " ")

        // Colons at positions 4, 7, 13, 16
        #expect(chars[4] == ":")
        #expect(chars[7] == ":")
        #expect(chars[13] == ":")
        #expect(chars[16] == ":")
    }

    @Test("Timestamps are lexicographically sortable")
    func sortable() {
        let earlier = Date(timeIntervalSince1970: 1_000_000)
        let later = Date(timeIntervalSince1970: 2_000_000)

        let ts1 = DateUtils.exifTimestamp(from: earlier)
        let ts2 = DateUtils.exifTimestamp(from: later)

        #expect(ts1 < ts2)
    }

    @Test("Different dates produce different strings")
    func differentDates() {
        let date1 = Date(timeIntervalSince1970: 0)         // 1970-01-01
        let date2 = Date(timeIntervalSince1970: 86400 * 365 * 30) // ~2000-01-01

        let ts1 = DateUtils.exifTimestamp(from: date1)
        let ts2 = DateUtils.exifTimestamp(from: date2)

        #expect(ts1 != ts2)
    }

    @Test("Invalid EVENT_TIMEZONE falls back to system default")
    func invalidTimezone() {
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let expected = expectedExifTimestamp(for: date, timeZone: .current)

        DateUtils.configure(eventTimezone: "Not_A_Valid_Zone")
        let result = DateUtils.exifTimestamp(from: date)
        #expect(result == expected)
    }

    @Test("Empty EVENT_TIMEZONE uses system default")
    func emptyTimezone() {
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let expected = expectedExifTimestamp(for: date, timeZone: .current)

        DateUtils.configure(eventTimezone: "")
        let result = DateUtils.exifTimestamp(from: date)
        #expect(result == expected)
    }

    @Test("Valid EVENT_TIMEZONE uses IANA timezone for concrete timestamp")
    func validTimezoneConcreteTimestamp() {
        DateUtils.configure(eventTimezone: "America/New_York")
        let date = Date(timeIntervalSince1970: 0) // 1970-01-01 00:00:00 UTC
        let result = DateUtils.exifTimestamp(from: date)

        #expect(result == "1969:12:31 19:00:00")
    }

}

// MARK: - Config Validation

@Suite("Config Validation")
struct ConfigValidationTests {
    private func withEnvironmentCleared<T: Sendable>(
        _ keys: [String],
        _ body: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try await TestEnvironment.withEnvironmentCleared(keys, body)
    }

    private func withEnvironment<T: Sendable>(
        _ overrides: [String: String?],
        _ body: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try await TestEnvironment.withEnvironment(overrides, body)
    }

    @Test("Config.load derives runtime paths from BASE_DIRECTORY")
    func configLoadDerivesRuntimePathsFromBaseDirectory() async throws {
        let baseDirectory = "~/tmp/kiko-config-paths-\(UUID().uuidString)"
        let defaultPreset = Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? "AVAssetExportPreset1920x1080"

        try await withEnvironment([
            "BASE_DIRECTORY": baseDirectory,
            "VIDEO_TRANSCODE_PRESET": defaultPreset,
        ]) {
            let config = try Config.load()
            let expandedBase = URL(fileURLWithPath: NSString(string: baseDirectory).expandingTildeInPath)

            #expect(config.uploadDir == expandedBase.appendingPathComponent("uploads").path)
            #expect(config.thumbsDir == expandedBase.appendingPathComponent("thumbs").path)
            #expect(config.previewsDir == expandedBase.appendingPathComponent("previews").path)
            #expect(config.logsDir == expandedBase.appendingPathComponent("logs").path)
            #expect(config.moderatedDir == expandedBase.appendingPathComponent("moderated").path)
            #expect(config.databasePath == expandedBase.appendingPathComponent("metadata.db").path)
        }
    }

    @Test("Config.load derives benchmark-prior path from BASE_DIRECTORY")
    func configLoadDerivesBenchmarkPriorPathFromBaseDirectory() async throws {
        let baseDirectory = "~/tmp/kiko-config-prior-\(UUID().uuidString)"
        let defaultPreset = Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? "AVAssetExportPreset1920x1080"

        try await withEnvironment([
            "BASE_DIRECTORY": baseDirectory,
            "VIDEO_TRANSCODE_PRESET": defaultPreset,
        ]) {
            let config = try Config.load()
            let expandedBase = NSString(string: baseDirectory).expandingTildeInPath
            let expectedPath = Config.benchmarkPriorPath(baseDirectoryPath: expandedBase)

            #expect(config.baseDirectoryPath == expandedBase)
            #expect(config.benchmarkPriorPath == expectedPath)
        }
    }

    @Test("Config.load reads INTERNAL_AUTH_SECRET independently")
    func configLoadReadsInternalAuthSecret() async throws {
        let internalSecret = "internal-auth-secret-\(UUID().uuidString)"
        let defaultPreset = Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? "AVAssetExportPreset1920x1080"

        try await withEnvironment([
            "VIDEO_TRANSCODE_PRESET": defaultPreset,
            "INTERNAL_AUTH_SECRET": internalSecret,
        ]) {
            let config = try Config.load()
            #expect(config.internalAuthSecret == internalSecret)
        }
    }

    @Test("Config.load uses defaults.env values when env vars are absent")
    func configLoadUsesDefaultsEnv() async throws {
        let keys = [
            "PUBLIC_PORT", "INTERNAL_PORT", "BIND_ADDRESS", "BASE_DIRECTORY", "EXTERNAL_SSD_PATH",
            "HEALTH_CHECK_INTERVAL", "JSON_MAX_BODY_BYTES", "WEBHOOK_RETRY_AFTER_SECONDS", "TURNSTILE_SECRET", "SESSION_HMAC_SECRET", "INTERNAL_AUTH_SECRET",
            "MAX_CONCURRENT_IMAGES", "MAX_CONCURRENT_VIDEOS", "MAX_CONCURRENT_REBUILD_PROBES", "THUMBNAIL_SIZE", "THUMBNAIL_QUALITY",
            "PREVIEW_SIZE", "PREVIEW_QUALITY", "VIDEO_THUMBNAIL_SIZE", "VIDEO_THUMBNAIL_TIME",
            "VIDEO_THUMBNAIL_QUALITY", "VIDEO_TRANSCODE_TIMEOUT", "VIDEO_TRANSCODE_PRESET",
            "MAX_IMAGE_PIXELS", "MAX_IMAGE_DIMENSION", "MAX_COMPRESSION_RATIO",
            "SQLITE_BUSY_TIMEOUT", "SQLITE_CACHE_SIZE", "DEFAULT_PAGE_SIZE", "MAX_PAGE_SIZE",
            "MAX_PAGE_OFFSET", "SQL_BATCH_SIZE", "SESSION_COOKIE_TTL", "SESSION_COOKIE_NAME",
            "TURNSTILE_VERIFY_TIMEOUT", "TURNSTILE_MAX_RESPONSE", "TURNSTILE_MAX_INFLIGHT_VERIFICATIONS", "TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS",
            "TURNSTILE_EXPECTED_HOSTNAME", "TURNSTILE_EXPECTED_ACTION", "TURNSTILE_EXPECTED_CDATA", "CACHE_CONTROL", "EVENT_TIMEZONE",
            "MAX_PENDING_WEBHOOK_JOBS", "QUEUE_COMPACTION_THRESHOLD", "SHA256_BUFFER_SIZE",
        ]

        try await withEnvironmentCleared(keys) {
            let config = try Config.load()
            let expectedBaseDir = NSString(string: Config.stringDefaults["BASE_DIRECTORY"]!).expandingTildeInPath
            let expectedBaseURL = URL(fileURLWithPath: expectedBaseDir)

            #expect(config.publicPort == Config.intDefaults["PUBLIC_PORT"]!.fallback)
            #expect(config.internalPort == Config.intDefaults["INTERNAL_PORT"]!.fallback)
            #expect(config.jsonMaxBodyBytes == Config.intDefaults["JSON_MAX_BODY_BYTES"]!.fallback)
            #expect(config.webhookRetryAfterSeconds == Config.intDefaults["WEBHOOK_RETRY_AFTER_SECONDS"]!.fallback)
            #expect(config.maxConcurrentImages == Config.intDefaults["MAX_CONCURRENT_IMAGES"]!.fallback)
            #expect(config.maxConcurrentVideos == Config.intDefaults["MAX_CONCURRENT_VIDEOS"]!.fallback)
            #expect(config.maxConcurrentRebuildProbes == Config.intDefaults["MAX_CONCURRENT_REBUILD_PROBES"]!.fallback)
            #expect(config.maxPendingWebhookJobs == Config.intDefaults["MAX_PENDING_WEBHOOK_JOBS"]!.fallback)
            #expect(config.sessionCookieTTL == Config.intDefaults["SESSION_COOKIE_TTL"]!.fallback)
            #expect(config.turnstileMaxInFlightVerifications == Config.intDefaults["TURNSTILE_MAX_INFLIGHT_VERIFICATIONS"]!.fallback)
            #expect(config.turnstileOverloadRetryAfterSeconds == Config.intDefaults["TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS"]!.fallback)
            #expect(config.sha256BufferSize == Config.intDefaults["SHA256_BUFFER_SIZE"]!.fallback)
            #expect(config.internalAuthSecret == nil)
            #expect(config.maxCompressionRatio == Config.doubleDefaults["MAX_COMPRESSION_RATIO"]!.fallback)
            #expect(config.uploadDir == expectedBaseURL.appendingPathComponent("uploads").path)
            #expect(config.databasePath == expectedBaseURL.appendingPathComponent("metadata.db").path)
        }
    }
}
