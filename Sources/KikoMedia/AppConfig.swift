import AVFoundation
import Foundation
import KikoMediaCore
import OSLog

extension Config {
    private static func resolveTBConnectTimeoutMS(from env: [String: String]) -> Int {
        let key = "TB_CONNECT_TIMEOUT"
        guard let spec = intDefaults[key] else {
            preconditionFailure("Missing int default for key \(key)")
        }
        let fallback = spec.fallback
        guard let raw = env[key]?.trimmingCharacters(in: .whitespacesAndNewlines),
              !raw.isEmpty else {
            return fallback
        }

        guard let parsed = Int(raw) else {
            Logger.kiko.warning("Invalid integer for \(key)='\(raw)', using default \(fallback)")
            return fallback
        }

        // Backward compatibility: historical values used seconds with range 1...30.
        if (1...30).contains(parsed) {
            let converted = parsed * 1_000
            Logger.kiko.warning(
                "Interpreting legacy \(key)=\(parsed)s as \(converted)ms; update env to explicit milliseconds"
            )
            return converted
        }

        if let range = spec.range, !range.contains(parsed) {
            Logger.kiko.warning("\(key)=\(parsed) outside range \(range), using default \(fallback)")
            return fallback
        }

        return parsed
    }

    static func validatedVideoTranscodePreset(
        _ preset: String,
        supportedPresets: [String] = AVAssetExportSession.allExportPresets()
    ) throws -> String {
        let uniqueSupportedPresets = Set(supportedPresets)
        guard uniqueSupportedPresets.contains(preset) else {
            throw VideoTranscodePresetConfigurationError.unsupportedPreset(
                preset: preset,
                supportedPresets: uniqueSupportedPresets.sorted()
            )
        }
        return preset
    }

    static func validatedVideoSchedulerPolicy(
        _ rawValue: String
    ) throws -> VideoSchedulerPolicy {
        guard let policy = VideoSchedulerPolicy(trimmedRawValue: rawValue) else {
            throw VideoSchedulerPolicyConfigurationError.unsupportedPolicy(
                policy: rawValue,
                supportedPolicies: VideoSchedulerPolicy.allCases.map(\.rawValue)
            )
        }
        return policy
    }

    static func load() throws -> Config {
        let env = ProcessInfo.processInfo.environment

        let baseDirRaw = envString("BASE_DIRECTORY")
        let expanded = NSString(string: baseDirRaw).expandingTildeInPath
        let baseDir = URL(fileURLWithPath: expanded)
        let videoTranscodePreset = try validatedVideoTranscodePreset(envString("VIDEO_TRANSCODE_PRESET"))
        let videoSchedulerPolicy = try validatedVideoSchedulerPolicy(envString("VIDEO_SCHEDULER_POLICY"))

        return Config(
            publicPort: envInt("PUBLIC_PORT"),
            internalPort: envInt("INTERNAL_PORT"),
            bindAddress: envString("BIND_ADDRESS"),
            uploadDir: baseDir.appendingPathComponent("uploads").path,
            thumbsDir: baseDir.appendingPathComponent("thumbs").path,
            previewsDir: baseDir.appendingPathComponent("previews").path,
            logsDir: baseDir.appendingPathComponent("logs").path,
            moderatedDir: baseDir.appendingPathComponent("moderated").path,
            externalSSDPath: envString("EXTERNAL_SSD_PATH"),
            databasePath: baseDir.appendingPathComponent("metadata.db").path,
            healthCheckInterval: envInt("HEALTH_CHECK_INTERVAL"),
            jsonMaxBodyBytes: envInt("JSON_MAX_BODY_BYTES"),
            webhookRetryAfterSeconds: envInt("WEBHOOK_RETRY_AFTER_SECONDS"),
            turnstileSecret: env["TURNSTILE_SECRET"],
            sessionHmacSecret: env["SESSION_HMAC_SECRET"],
            internalAuthSecret: env["INTERNAL_AUTH_SECRET"],
            gateSecret: env["GATE_SECRET"],
            maxConcurrentImages: envInt("MAX_CONCURRENT_IMAGES"),
            maxConcurrentVideos: envInt("MAX_CONCURRENT_VIDEOS"),
            videoSchedulerPolicy: videoSchedulerPolicy,
            maxConcurrentRebuildProbes: envInt("MAX_CONCURRENT_REBUILD_PROBES"),
            thumbnailSize: envInt("THUMBNAIL_SIZE"),
            thumbnailQuality: envDouble("THUMBNAIL_QUALITY"),
            previewSize: envInt("PREVIEW_SIZE"),
            previewQuality: envDouble("PREVIEW_QUALITY"),
            videoThumbnailSize: envInt("VIDEO_THUMBNAIL_SIZE"),
            videoThumbnailTime: envDouble("VIDEO_THUMBNAIL_TIME"),
            videoThumbnailQuality: envDouble("VIDEO_THUMBNAIL_QUALITY"),
            videoTranscodeTimeout: envInt("VIDEO_TRANSCODE_TIMEOUT"),
            videoTranscodePreset: videoTranscodePreset,
            tbWorkers: envString("TB_WORKERS"),
            tbPort: envInt("TB_PORT"),
            tbConnectTimeout: resolveTBConnectTimeoutMS(from: env),
            maxImagePixels: envInt("MAX_IMAGE_PIXELS"),
            maxImageDimension: envInt("MAX_IMAGE_DIMENSION"),
            maxCompressionRatio: envDouble("MAX_COMPRESSION_RATIO"),
            sqliteBusyTimeout: envInt("SQLITE_BUSY_TIMEOUT"),
            sqliteCacheSize: envInt("SQLITE_CACHE_SIZE"),
            defaultPageSize: envInt("DEFAULT_PAGE_SIZE"),
            maxPageSize: envInt("MAX_PAGE_SIZE"),
            maxPageOffset: envInt("MAX_PAGE_OFFSET"),
            sqlBatchSize: envInt("SQL_BATCH_SIZE"),
            sessionCookieTTL: envInt("SESSION_COOKIE_TTL"),
            sessionCookieName: envString("SESSION_COOKIE_NAME"),
            turnstileVerifyTimeout: envInt("TURNSTILE_VERIFY_TIMEOUT"),
            turnstileMaxResponse: envInt("TURNSTILE_MAX_RESPONSE"),
            turnstileMaxInFlightVerifications: envInt("TURNSTILE_MAX_INFLIGHT_VERIFICATIONS"),
            turnstileOverloadRetryAfterSeconds: envInt("TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS"),
            turnstileExpectedHostname: envString("TURNSTILE_EXPECTED_HOSTNAME"),
            turnstileExpectedAction: envString("TURNSTILE_EXPECTED_ACTION"),
            turnstileExpectedCData: envString("TURNSTILE_EXPECTED_CDATA"),
            cacheControl: envString("CACHE_CONTROL"),
            eventTimezone: envString("EVENT_TIMEZONE"),
            maxPendingWebhookJobs: envInt("MAX_PENDING_WEBHOOK_JOBS"),
            queueCompactionThreshold: envInt("QUEUE_COMPACTION_THRESHOLD"),
            sha256BufferSize: envInt("SHA256_BUFFER_SIZE")
        )
    }
}

enum VideoTranscodePresetConfigurationError: Error, Equatable, LocalizedError {
    case unsupportedPreset(preset: String, supportedPresets: [String])

    var errorDescription: String? {
        switch self {
        case let .unsupportedPreset(preset, supportedPresets):
            let supportedList = supportedPresets.isEmpty
                ? "(none reported by AVFoundation)"
                : supportedPresets.joined(separator: ", ")
            return "Unsupported VIDEO_TRANSCODE_PRESET '\(preset)'. Supported presets on this host: \(supportedList)"
        }
    }
}

enum VideoSchedulerPolicyConfigurationError: Error, Equatable, LocalizedError {
    case unsupportedPolicy(policy: String, supportedPolicies: [String])

    var errorDescription: String? {
        switch self {
        case let .unsupportedPolicy(policy, supportedPolicies):
            return "Unsupported VIDEO_SCHEDULER_POLICY '\(policy)'. Supported policies: \(supportedPolicies.joined(separator: ", "))"
        }
    }
}
