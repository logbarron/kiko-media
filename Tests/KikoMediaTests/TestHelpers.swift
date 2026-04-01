import Foundation
import CoreGraphics
import ImageIO
import UniformTypeIdentifiers
import Hummingbird
import HummingbirdTesting
import NIOCore
@testable import KikoMediaCore
@testable import KikoMediaApp

// MARK: - Process Environment Helpers

enum TestEnvironment {
    private static let gate = DispatchSemaphore(value: 1)

    static func withEnvironment<T>(
        _ overrides: [String: String?],
        _ body: () throws -> T
    ) rethrows -> T {
        let keys = Array(overrides.keys)
        return try withLockedEnvironment(keys: keys) {
            apply(overrides: overrides)
            return try body()
        }
    }

    static func withEnvironment<T: Sendable>(
        _ overrides: [String: String?],
        _ body: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        let keys = Array(Set(overrides.keys))
        await acquireLock()
        let originalValues = snapshot(keys: keys)
        apply(overrides: overrides)
        do {
            let result = try await body()
            restore(values: originalValues)
            releaseLock()
            return result
        } catch {
            restore(values: originalValues)
            releaseLock()
            throw error
        }
    }

    static func withEnvironmentCleared<T>(
        _ keys: [String],
        _ body: () throws -> T
    ) rethrows -> T {
        var overrides: [String: String?] = [:]
        overrides.reserveCapacity(keys.count)
        for key in keys {
            overrides[key] = nil
        }
        return try withEnvironment(overrides, body)
    }

    static func withEnvironmentCleared<T: Sendable>(
        _ keys: [String],
        _ body: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        var overrides: [String: String?] = [:]
        overrides.reserveCapacity(keys.count)
        for key in keys {
            overrides[key] = nil
        }
        return try await withEnvironment(overrides, body)
    }

    private static func withLockedEnvironment<T>(
        keys: [String],
        _ body: () throws -> T
    ) rethrows -> T {
        gate.wait()
        defer { gate.signal() }

        let uniqueKeys = Array(Set(keys))
        let originalValues = snapshot(keys: uniqueKeys)
        defer { restore(values: originalValues) }
        return try body()
    }

    private static func acquireLock() async {
        await withCheckedContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                gate.wait()
                continuation.resume()
            }
        }
    }

    private static func releaseLock() {
        gate.signal()
    }

    private static func snapshot(keys: [String]) -> [String: String?] {
        let env = ProcessInfo.processInfo.environment
        var values: [String: String?] = [:]
        values.reserveCapacity(keys.count)
        for key in keys {
            values[key] = env[key]
        }
        return values
    }

    private static func restore(values: [String: String?]) {
        for (key, value) in values {
            if let value {
                setenv(key, value, 1)
            } else {
                unsetenv(key)
            }
        }
    }

    private static func apply(overrides: [String: String?]) {
        for (key, value) in overrides {
            if let value {
                setenv(key, value, 1)
            } else {
                unsetenv(key)
            }
        }
    }
}

// MARK: - Test Environment

/// Self-contained test environment with temp directories and database.
/// Each test should create its own TestEnv for isolation.
struct TestEnv {
    let tempDir: URL
    let database: Database
    let thumbsDir: String
    let previewsDir: String
    let uploadDir: String
    let config: Config

    init() throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-test-\(UUID().uuidString)")
        let fm = FileManager.default
        try fm.createDirectory(at: tempDir, withIntermediateDirectories: true)

        thumbsDir = tempDir.appendingPathComponent("thumbs").path
        previewsDir = tempDir.appendingPathComponent("previews").path
        uploadDir = tempDir.appendingPathComponent("uploads").path
        let logsDir = tempDir.appendingPathComponent("logs").path
        let dbPath = tempDir.appendingPathComponent("test.db").path

        for dir in [thumbsDir, previewsDir, uploadDir, logsDir] {
            try fm.createDirectory(atPath: dir, withIntermediateDirectories: true)
        }

        database = try Database(
            path: dbPath,
            busyTimeout: 5000,
            cacheSize: -20000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10_000,
            sqlBatchSize: 500
        )
        config = Config(
            publicPort: 0,
            internalPort: 0,
            uploadDir: uploadDir,
            thumbsDir: thumbsDir,
            previewsDir: previewsDir,
            logsDir: logsDir,
            externalSSDPath: "/tmp/nonexistent-ssd",
            databasePath: dbPath,
            turnstileSecret: nil,
            sessionHmacSecret: nil
        )

        DateUtils.configure(eventTimezone: config.eventTimezone)
    }

    func cleanup() {
        try? FileManager.default.removeItem(at: tempDir)
    }

    /// Insert a complete image asset with test thumbnail and preview files on disk
    func insertCompleteImageAsset(id: String, timestamp: String = "2025:02:05 12:00:00") async throws {
        _ = try await database.insertQueued(id: id, type: .image, originalName: "\(id).jpg")
        try await database.markComplete(id: id, timestamp: timestamp)
        try TestImage.writeJPEG(to: "\(thumbsDir)/\(id).jpg", width: 512, height: 512)
        try TestImage.writeJPEG(to: "\(previewsDir)/\(id).jpg", width: 1440, height: 1080)
    }

    /// Insert a complete video asset with test thumbnail and dummy preview file
    func insertCompleteVideoAsset(id: String, timestamp: String = "2025:02:05 12:00:00") async throws {
        _ = try await database.insertQueued(id: id, type: .video, originalName: "\(id).mov")
        try await database.markComplete(id: id, timestamp: timestamp)
        try TestImage.writeJPEG(to: "\(thumbsDir)/\(id).jpg", width: 512, height: 512)
        // FileServer doesn't validate file content, just reads and serves bytes
        try Data(repeating: 0xFF, count: 1024).write(to: URL(fileURLWithPath: "\(previewsDir)/\(id).mp4"))
    }
}

func makeResolvedVideoCost(
    frameCount: Double?,
    durationSeconds: Double? = nil,
    runtimeSeconds: Double? = nil,
    confidence: EstimateConfidence = .low,
    localMSPerFrameC1: Double? = 1.0,
    localFixedOverheadMS: Double = 0
) -> CAResolvedVideoCost {
    CAProfileAndFallbackMath.resolveVideoCost(
        frameCount: frameCount,
        durationSeconds: durationSeconds,
        runtimeSeconds: runtimeSeconds,
        confidence: confidence,
        runtimeSourceWhenPresent: .estimatedProcessingRuntime,
        localMSPerFrameC1: localMSPerFrameC1,
        localFixedOverheadMS: localFixedOverheadMS
    )
}

func makeResolvedVideoCosts(
    frameCounts: [Double],
    localMSPerFrameC1: Double? = 1.0,
    localFixedOverheadMS: Double = 0
) -> [CAResolvedVideoCost] {
    frameCounts.map { frameCount in
        makeResolvedVideoCost(
            frameCount: frameCount,
            localMSPerFrameC1: localMSPerFrameC1,
            localFixedOverheadMS: localFixedOverheadMS
        )
    }
}

func makeDefaultResolvedVideoCost() -> CAResolvedVideoCost {
    makeResolvedVideoCost(frameCount: nil, localMSPerFrameC1: nil)
}

func makeDefaultResolvedVideoCosts(count: Int) -> [CAResolvedVideoCost] {
    (0..<count).map { _ in makeDefaultResolvedVideoCost() }
}

func makeLocalComplexityAwarePriorTable(
    config: Config,
    msPerFrameC1: Double = 1.0
) -> BenchmarkPriorTable {
    let caps = WorkerCaps.detectLocal()
    guard let signature = WorkerSignatureBuilder.make(
        chipName: caps.chipName,
        performanceCores: caps.performanceCores,
        efficiencyCores: caps.efficiencyCores,
        videoEncodeEngines: caps.videoEncodeEngines,
        preset: config.videoTranscodePreset,
        osVersion: caps.osVersion
    ) else {
        return BenchmarkPriorTable()
    }

    let machine = BenchmarkPriorMachine(
        signature: signature,
        chipName: caps.chipName ?? "test-chip",
        performanceCores: caps.performanceCores ?? 1,
        efficiencyCores: caps.efficiencyCores ?? 0,
        videoEncodeEngines: caps.videoEncodeEngines ?? 1,
        osVersion: caps.osVersion ?? "0.0",
        transcodePreset: config.videoTranscodePreset,
        msPerFrameC1: max(0.001, msPerFrameC1),
        avgCorpusFrameCount: 60 * 24,
        cells: [
            BenchmarkPriorCell(
                concurrency: 1,
                videosPerMin: 60,
                msPerVideoP50: 1_000,
                msPerVideoP95: 1_200,
                degradationRatio: 1.0
            ),
            BenchmarkPriorCell(
                concurrency: 2,
                videosPerMin: 90,
                msPerVideoP50: 1_300,
                msPerVideoP95: 1_600,
                degradationRatio: 1.3
            )
        ]
    )
    return BenchmarkPriorTable(machines: [machine])
}

// MARK: - Router Builders

extension TestEnv {
    /// Build a public router using the shared production route builders
    func publicRouter(
        sessionCookie: SessionCookie? = nil,
        turnstileVerifier: TurnstileVerifier? = nil,
        gateSecret: String? = nil,
        turnstileVerify: (@Sendable (String) async -> TurnstileVerificationResult)? = nil,
        heartRevisionTracker: HeartRevisionTracker = HeartRevisionTracker()
    ) -> Router<BasicRequestContext> {
        let fileServer = FileServer(thumbsDir: thumbsDir, previewsDir: previewsDir, database: database, cacheControl: "public, max-age=31536000, immutable")

        return RouterBuilders.buildPublicRouter(
            database: database,
            fileServer: fileServer,
            sessionCookie: sessionCookie,
            turnstileVerifier: turnstileVerifier,
            heartRevisionTracker: heartRevisionTracker,
            gateSecret: gateSecret,
            turnstileVerify: turnstileVerify,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes
        )
    }

    /// Build a public router with session gating enabled and return the cookie helper.
    /// This gives tests a canonical, non-bypass path for auth-protected endpoints.
    func gatedPublicRouter(
        hmacSecret: String = "01234567890123456789012345678901",
        heartRevisionTracker: HeartRevisionTracker = HeartRevisionTracker()
    ) -> (router: Router<BasicRequestContext>, cookie: SessionCookie) {
        let cookie = SessionCookie(config: config, hmacSecret: hmacSecret)
        let router = publicRouter(sessionCookie: cookie, heartRevisionTracker: heartRevisionTracker)
        return (router, cookie)
    }

    /// Build an internal router using the shared production route builders
    func internalRouter(
        sessionCookie: SessionCookie? = nil,
        internalAuthSecret: String? = nil
    ) -> Router<BasicRequestContext> {
        let fileServer = FileServer(thumbsDir: thumbsDir, previewsDir: previewsDir, database: database, cacheControl: "public, max-age=31536000, immutable")
        let moderationMarkers = ModerationMarkers(baseDir: tempDir.appendingPathComponent("moderated"))
        let processor = MediaProcessor(config: config, database: database, moderationMarkers: moderationMarkers)
        let webhookHandler = WebhookHandler(
            database: database, processor: processor,
            uploadDir: uploadDir,
            sessionCookie: sessionCookie,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes,
            queueFullRetryAfterSeconds: config.webhookRetryAfterSeconds
        )

        return RouterBuilders.buildInternalRouter(
            database: database,
            fileServer: fileServer,
            moderationMarkers: moderationMarkers,
            webhookHandler: webhookHandler,
            internalAuthSecret: internalAuthSecret,
            jsonMaxBodyBytes: config.jsonMaxBodyBytes
        )
    }
}

// MARK: - Synthetic Test Image Generation

enum TestImage {
    /// Create a CGImage with a gradient pattern.
    /// Uses varied pixel data so the JPEG compresses at a realistic ratio,
    /// avoiding false positives from the decompression bomb detector.
    static func make(width: Int = 100, height: Int = 100) -> CGImage {
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        let bytesPerRow = width * 4
        let data = UnsafeMutablePointer<UInt8>.allocate(capacity: height * bytesPerRow)
        defer { data.deallocate() }

        // Fill with a diagonal gradient + per-pixel variation
        for y in 0..<height {
            for x in 0..<width {
                let offset = y * bytesPerRow + x * 4
                let r = UInt8((x * 255) / max(width - 1, 1))
                let g = UInt8((y * 255) / max(height - 1, 1))
                let b = UInt8(((x + y) * 127) / max(width + height - 2, 1))
                data[offset]     = r
                data[offset + 1] = g
                data[offset + 2] = b
                data[offset + 3] = 255 // alpha
            }
        }

        let context = CGContext(
            data: data,
            width: width, height: height,
            bitsPerComponent: 8, bytesPerRow: bytesPerRow,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        )!
        return context.makeImage()!
    }

    /// Write a test JPEG file at the given path
    static func writeJPEG(to path: String, width: Int = 100, height: Int = 100, quality: CGFloat = 0.85) throws {
        let image = make(width: width, height: height)
        try ImageUtils.saveAsJPEG(image: image, path: path, quality: quality)
    }

    /// Write a test JPEG with embedded EXIF metadata (for stripping verification)
    static func writeJPEGWithEXIF(to path: String, width: Int = 500, height: Int = 500) throws {
        let image = make(width: width, height: height)
        let url = URL(fileURLWithPath: path) as CFURL
        guard let destination = CGImageDestinationCreateWithURL(url, "public.jpeg" as CFString, 1, nil) else {
            throw NSError(domain: "TestImage", code: 1, userInfo: [NSLocalizedDescriptionKey: "Cannot create image destination"])
        }
        let exifDict: [CFString: Any] = [
            kCGImagePropertyExifDateTimeOriginal: "2025:01:15 14:30:00",
            kCGImagePropertyExifLensMake: "TestLens",
        ]
        let properties: [CFString: Any] = [
            kCGImagePropertyExifDictionary: exifDict,
            kCGImageDestinationLossyCompressionQuality: 0.85 as CGFloat,
        ]
        CGImageDestinationAddImage(destination, image, properties as CFDictionary)
        guard CGImageDestinationFinalize(destination) else {
            throw NSError(domain: "TestImage", code: 2, userInfo: [NSLocalizedDescriptionKey: "Cannot finalize image"])
        }
    }

    /// Read pixel dimensions from a JPEG/image file
    static func dimensions(at path: String) -> (width: Int, height: Int)? {
        let url = URL(fileURLWithPath: path)
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil),
              let props = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
              let w = props[kCGImagePropertyPixelWidth] as? Int,
              let h = props[kCGImagePropertyPixelHeight] as? Int else {
            return nil
        }
        return (w, h)
    }
}

// MARK: - Decodable Test Response Models

struct TestGalleryResponse: Decodable {
    let assets: [TestGalleryAsset]
    let total: Int
    let heartRevision: Int?
}

struct TestGalleryAsset: Decodable {
    let id: String
    let type: String
    let status: String?
    let heartCount: Int
}

struct TestHeartResponse: Decodable {
    let heartCount: Int
}

struct TestHeartCountsResponse: Decodable {
    let heartCounts: [String: Int]
}

struct TestHealthResponse: Decodable {
    let status: String
}

enum TestRepositoryRoot {
    static func resolve(from filePath: String = #filePath, sentinels: [String]) throws -> URL {
        let startDirectory = URL(fileURLWithPath: filePath).deletingLastPathComponent().path
        if let found = discover(startingAt: startDirectory, sentinels: sentinels) {
            return found
        }

        throw NSError(
            domain: "TestRepositoryRoot",
            code: 1,
            userInfo: [
                NSLocalizedDescriptionKey: "Could not locate repository root from \(filePath) using sentinels \(sentinels)"
            ]
        )
    }

    private static func discover(
        startingAt directory: String,
        sentinels: [String],
        fileManager: FileManager = .default
    ) -> URL? {
        var current = URL(fileURLWithPath: directory).standardizedFileURL
        while true {
            let path = current.path
            let isRoot = sentinels.allSatisfy { sentinel in
                fileManager.fileExists(atPath: "\(path)/\(sentinel)")
            }
            if isRoot {
                return current
            }

            let parent = current.deletingLastPathComponent()
            if parent.path == path {
                return nil
            }
            current = parent
        }
    }
}
