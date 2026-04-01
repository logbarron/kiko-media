import Foundation
import OSLog

package enum VideoSchedulerPolicy: String, Sendable, CaseIterable {
    case auto
    case fifo
    case none

    package init?(trimmedRawValue rawValue: String) {
        self.init(rawValue: rawValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased())
    }
}

package struct Config: Sendable {
    package struct ThunderboltWorker: Sendable, Equatable {
        package let host: String
        package let slots: Int
    }

    // Operational
    package let publicPort: Int
    package let internalPort: Int
    package let bindAddress: String
    package let uploadDir: String
    package let thumbsDir: String
    package let previewsDir: String
    package let logsDir: String
    package let moderatedDir: String
    package let externalSSDPath: String
    package let databasePath: String
    package let healthCheckInterval: Int
    package let jsonMaxBodyBytes: Int
    package let webhookRetryAfterSeconds: Int
    package let turnstileSecret: String?
    package let sessionHmacSecret: String?
    package let internalAuthSecret: String?
    package let gateSecret: String?

    // Processing
    package let maxConcurrentImages: Int
    package let maxConcurrentVideos: Int
    package let videoSchedulerPolicy: VideoSchedulerPolicy
    package let maxConcurrentRebuildProbes: Int
    package let thumbnailSize: Int
    package let thumbnailQuality: Double
    package let previewSize: Int
    package let previewQuality: Double
    package let videoThumbnailSize: Int
    package let videoThumbnailTime: Double
    package let videoThumbnailQuality: Double
    package let videoTranscodeTimeout: Int
    package let videoTranscodePreset: String
    package let thunderboltWorkers: [ThunderboltWorker]
    package let tbPort: Int
    package let tbConnectTimeout: Int // milliseconds

    // Security
    package let maxImagePixels: Int
    package let maxImageDimension: Int
    package let maxCompressionRatio: Double

    // Database
    package let sqliteBusyTimeout: Int
    package let sqliteCacheSize: Int
    package let defaultPageSize: Int
    package let maxPageSize: Int
    package let maxPageOffset: Int
    package let sqlBatchSize: Int

    // Session
    package let sessionCookieTTL: Int
    package let sessionCookieName: String
    package let turnstileVerifyTimeout: Int
    package let turnstileMaxResponse: Int
    package let turnstileMaxInFlightVerifications: Int
    package let turnstileOverloadRetryAfterSeconds: Int
    package let turnstileExpectedHostname: String
    package let turnstileExpectedAction: String
    package let turnstileExpectedCData: String

    // Caching
    package let cacheControl: String

    // Event
    package let eventTimezone: String

    // Internal
    package let maxPendingWebhookJobs: Int
    package let queueCompactionThreshold: Int
    package let sha256BufferSize: Int

    // MARK: - Default Lookup

    private static func defaultIntValue(_ key: String) -> Int {
        guard let spec = intDefaults[key] else {
            preconditionFailure("Missing int default for key \(key)")
        }
        return spec.fallback
    }

    private static func defaultDoubleValue(_ key: String) -> Double {
        guard let spec = doubleDefaults[key] else {
            preconditionFailure("Missing double default for key \(key)")
        }
        return spec.fallback
    }

    private static func defaultStringValue(_ key: String) -> String {
        guard let value = stringDefaults[key] else {
            preconditionFailure("Missing string default for key \(key)")
        }
        return value
    }

    package init(
        publicPort: Int,
        internalPort: Int,
        bindAddress: String = Config.defaultStringValue("BIND_ADDRESS"),
        uploadDir: String,
        thumbsDir: String,
        previewsDir: String,
        logsDir: String,
        moderatedDir: String = "",
        externalSSDPath: String,
        databasePath: String,
        healthCheckInterval: Int = Config.defaultIntValue("HEALTH_CHECK_INTERVAL"),
        jsonMaxBodyBytes: Int = Config.defaultIntValue("JSON_MAX_BODY_BYTES"),
        webhookRetryAfterSeconds: Int = Config.defaultIntValue("WEBHOOK_RETRY_AFTER_SECONDS"),
        turnstileSecret: String?,
        sessionHmacSecret: String?,
        internalAuthSecret: String? = nil,
        gateSecret: String? = nil,
        maxConcurrentImages: Int = Config.defaultIntValue("MAX_CONCURRENT_IMAGES"),
        maxConcurrentVideos: Int = Config.defaultIntValue("MAX_CONCURRENT_VIDEOS"),
        videoSchedulerPolicy: VideoSchedulerPolicy = .auto,
        maxConcurrentRebuildProbes: Int = Config.defaultIntValue("MAX_CONCURRENT_REBUILD_PROBES"),
        thumbnailSize: Int = Config.defaultIntValue("THUMBNAIL_SIZE"),
        thumbnailQuality: Double = Config.defaultDoubleValue("THUMBNAIL_QUALITY"),
        previewSize: Int = Config.defaultIntValue("PREVIEW_SIZE"),
        previewQuality: Double = Config.defaultDoubleValue("PREVIEW_QUALITY"),
        videoThumbnailSize: Int = Config.defaultIntValue("VIDEO_THUMBNAIL_SIZE"),
        videoThumbnailTime: Double = Config.defaultDoubleValue("VIDEO_THUMBNAIL_TIME"),
        videoThumbnailQuality: Double = Config.defaultDoubleValue("VIDEO_THUMBNAIL_QUALITY"),
        videoTranscodeTimeout: Int = Config.defaultIntValue("VIDEO_TRANSCODE_TIMEOUT"),
        videoTranscodePreset: String = Config.defaultStringValue("VIDEO_TRANSCODE_PRESET"),
        tbWorkers: String = Config.defaultStringValue("TB_WORKERS"),
        tbPort: Int = Config.defaultIntValue("TB_PORT"),
        tbConnectTimeout: Int = Config.defaultIntValue("TB_CONNECT_TIMEOUT"),
        maxImagePixels: Int = Config.defaultIntValue("MAX_IMAGE_PIXELS"),
        maxImageDimension: Int = Config.defaultIntValue("MAX_IMAGE_DIMENSION"),
        maxCompressionRatio: Double = Config.defaultDoubleValue("MAX_COMPRESSION_RATIO"),
        sqliteBusyTimeout: Int = Config.defaultIntValue("SQLITE_BUSY_TIMEOUT"),
        sqliteCacheSize: Int = Config.defaultIntValue("SQLITE_CACHE_SIZE"),
        defaultPageSize: Int = Config.defaultIntValue("DEFAULT_PAGE_SIZE"),
        maxPageSize: Int = Config.defaultIntValue("MAX_PAGE_SIZE"),
        maxPageOffset: Int = Config.defaultIntValue("MAX_PAGE_OFFSET"),
        sqlBatchSize: Int = Config.defaultIntValue("SQL_BATCH_SIZE"),
        sessionCookieTTL: Int = Config.defaultIntValue("SESSION_COOKIE_TTL"),
        sessionCookieName: String = Config.defaultStringValue("SESSION_COOKIE_NAME"),
        turnstileVerifyTimeout: Int = Config.defaultIntValue("TURNSTILE_VERIFY_TIMEOUT"),
        turnstileMaxResponse: Int = Config.defaultIntValue("TURNSTILE_MAX_RESPONSE"),
        turnstileMaxInFlightVerifications: Int = Config.defaultIntValue("TURNSTILE_MAX_INFLIGHT_VERIFICATIONS"),
        turnstileOverloadRetryAfterSeconds: Int = Config.defaultIntValue("TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS"),
        turnstileExpectedHostname: String = Config.defaultStringValue("TURNSTILE_EXPECTED_HOSTNAME"),
        turnstileExpectedAction: String = Config.defaultStringValue("TURNSTILE_EXPECTED_ACTION"),
        turnstileExpectedCData: String = Config.defaultStringValue("TURNSTILE_EXPECTED_CDATA"),
        cacheControl: String = Config.defaultStringValue("CACHE_CONTROL"),
        eventTimezone: String = Config.defaultStringValue("EVENT_TIMEZONE"),
        maxPendingWebhookJobs: Int = Config.defaultIntValue("MAX_PENDING_WEBHOOK_JOBS"),
        queueCompactionThreshold: Int = Config.defaultIntValue("QUEUE_COMPACTION_THRESHOLD"),
        sha256BufferSize: Int = Config.defaultIntValue("SHA256_BUFFER_SIZE")
    ) {
        self.publicPort = publicPort
        self.internalPort = internalPort
        self.bindAddress = bindAddress
        self.uploadDir = uploadDir
        self.thumbsDir = thumbsDir
        self.previewsDir = previewsDir
        self.logsDir = logsDir
        self.moderatedDir = moderatedDir
        self.externalSSDPath = externalSSDPath
        self.databasePath = databasePath
        self.healthCheckInterval = healthCheckInterval
        self.jsonMaxBodyBytes = jsonMaxBodyBytes
        self.webhookRetryAfterSeconds = webhookRetryAfterSeconds
        self.turnstileSecret = turnstileSecret
        self.sessionHmacSecret = sessionHmacSecret
        self.internalAuthSecret = internalAuthSecret
        self.gateSecret = gateSecret
        self.maxConcurrentImages = maxConcurrentImages
        self.maxConcurrentVideos = maxConcurrentVideos
        self.videoSchedulerPolicy = videoSchedulerPolicy
        self.maxConcurrentRebuildProbes = maxConcurrentRebuildProbes
        self.thumbnailSize = thumbnailSize
        self.thumbnailQuality = thumbnailQuality
        self.previewSize = previewSize
        self.previewQuality = previewQuality
        self.videoThumbnailSize = videoThumbnailSize
        self.videoThumbnailTime = videoThumbnailTime
        self.videoThumbnailQuality = videoThumbnailQuality
        self.videoTranscodeTimeout = videoTranscodeTimeout
        self.videoTranscodePreset = videoTranscodePreset
        self.thunderboltWorkers = Self.parseThunderboltWorkers(tbWorkers)
        self.tbPort = tbPort
        self.tbConnectTimeout = tbConnectTimeout
        self.maxImagePixels = maxImagePixels
        self.maxImageDimension = maxImageDimension
        self.maxCompressionRatio = maxCompressionRatio
        self.sqliteBusyTimeout = sqliteBusyTimeout
        self.sqliteCacheSize = sqliteCacheSize
        self.defaultPageSize = defaultPageSize
        self.maxPageSize = maxPageSize
        self.maxPageOffset = maxPageOffset
        self.sqlBatchSize = sqlBatchSize
        self.sessionCookieTTL = sessionCookieTTL
        self.sessionCookieName = sessionCookieName
        self.turnstileVerifyTimeout = turnstileVerifyTimeout
        self.turnstileMaxResponse = turnstileMaxResponse
        self.turnstileMaxInFlightVerifications = turnstileMaxInFlightVerifications
        self.turnstileOverloadRetryAfterSeconds = turnstileOverloadRetryAfterSeconds
        self.turnstileExpectedHostname = turnstileExpectedHostname
        self.turnstileExpectedAction = turnstileExpectedAction
        self.turnstileExpectedCData = turnstileExpectedCData
        self.cacheControl = cacheControl
        self.eventTimezone = eventTimezone
        self.maxPendingWebhookJobs = maxPendingWebhookJobs
        self.queueCompactionThreshold = queueCompactionThreshold
        self.sha256BufferSize = sha256BufferSize
    }

    // MARK: - Defaults

    package struct IntSpec { package let fallback: Int; package let range: ClosedRange<Int>? }
    package struct DoubleSpec { package let fallback: Double; package let range: ClosedRange<Double>? }

    // MARK: - Env Var Helpers

    package static func envInt(_ key: String) -> Int {
        guard let spec = intDefaults[key] else {
            preconditionFailure("Unknown int config key: \(key)")
        }
        guard let raw = ProcessInfo.processInfo.environment[key] else { return spec.fallback }
        guard let value = Int(raw) else {
            Logger.kiko.warning("Invalid integer for \(key)='\(raw)', using default \(spec.fallback)")
            return spec.fallback
        }
        if let range = spec.range, !range.contains(value) {
            Logger.kiko.warning("\(key)=\(value) outside range \(range), using default \(spec.fallback)")
            return spec.fallback
        }
        return value
    }

    package static func envDouble(_ key: String) -> Double {
        guard let spec = doubleDefaults[key] else {
            preconditionFailure("Unknown double config key: \(key)")
        }
        guard let raw = ProcessInfo.processInfo.environment[key] else { return spec.fallback }
        guard let value = Double(raw) else {
            Logger.kiko.warning("Invalid double for \(key)='\(raw)', using default \(spec.fallback)")
            return spec.fallback
        }
        if let range = spec.range, !range.contains(value) {
            Logger.kiko.warning("\(key)=\(value) outside range \(range), using default \(spec.fallback)")
            return spec.fallback
        }
        return value
    }

    package static func envString(_ key: String) -> String {
        guard let fallback = stringDefaults[key] else {
            preconditionFailure("Unknown string config key: \(key)")
        }
        guard let raw = ProcessInfo.processInfo.environment[key], !raw.isEmpty else { return fallback }
        return raw
    }

    package static let benchmarkPriorArtifactFilename = "benchmark-prior.json"

    package static func benchmarkPriorPath(baseDirectoryPath: String) -> String {
        URL(fileURLWithPath: baseDirectoryPath)
            .appendingPathComponent(benchmarkPriorArtifactFilename)
            .path
    }

    package var baseDirectoryPath: String {
        URL(fileURLWithPath: uploadDir)
            .deletingLastPathComponent()
            .path
    }

    package var benchmarkPriorPath: String {
        Self.benchmarkPriorPath(baseDirectoryPath: baseDirectoryPath)
    }

    package static func parseThunderboltWorkers(_ raw: String) -> [ThunderboltWorker] {
        guard !raw.isEmpty else { return [] }
        return raw.split(separator: ",").compactMap { entry in
            let trimmedEntry = entry.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmedEntry.isEmpty else { return nil }

            let parts = trimmedEntry.split(separator: ":", omittingEmptySubsequences: false)
            guard parts.count == 2 else { return nil }

            let host = String(parts[0]).trimmingCharacters(in: .whitespacesAndNewlines)
            let slotsRaw = String(parts[1]).trimmingCharacters(in: .whitespacesAndNewlines)
            guard !host.isEmpty, let slots = Int(slotsRaw), slots > 0 else { return nil }

            return ThunderboltWorker(host: host, slots: slots)
        }
    }
}
