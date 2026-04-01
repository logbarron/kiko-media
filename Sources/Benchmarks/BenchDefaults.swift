import Foundation
import KikoMediaCore

enum BenchDefaults {
    // Derived from Config defaults to avoid benchmark/runtime drift.
    private static func intFallback(_ key: String) -> Int {
        guard let spec = Config.intDefaults[key] else {
            preconditionFailure("Missing int fallback for \(key)")
        }
        return spec.fallback
    }

    private static func doubleFallback(_ key: String) -> Double {
        guard let spec = Config.doubleDefaults[key] else {
            preconditionFailure("Missing double fallback for \(key)")
        }
        return spec.fallback
    }

    static let maxImagePixels = intFallback("MAX_IMAGE_PIXELS")
    static let maxImageDimension = intFallback("MAX_IMAGE_DIMENSION")
    static let maxCompressionRatio = doubleFallback("MAX_COMPRESSION_RATIO")

    static let sqliteBusyTimeout = intFallback("SQLITE_BUSY_TIMEOUT")
    static let sqliteCacheSize = intFallback("SQLITE_CACHE_SIZE")
    static let defaultPageSize = intFallback("DEFAULT_PAGE_SIZE")
    static let maxPageSize = intFallback("MAX_PAGE_SIZE")
    static let maxPageOffset = intFallback("MAX_PAGE_OFFSET")
    static let sqlBatchSize = intFallback("SQL_BATCH_SIZE")

    static let sha256BufferSize = intFallback("SHA256_BUFFER_SIZE")
}

func makeBenchmarkDatabase(path: String) throws -> Database {
    try Database(
        path: path,
        busyTimeout: BenchDefaults.sqliteBusyTimeout,
        cacheSize: BenchDefaults.sqliteCacheSize,
        defaultPageSize: BenchDefaults.defaultPageSize,
        maxPageSize: BenchDefaults.maxPageSize,
        maxPageOffset: BenchDefaults.maxPageOffset,
        sqlBatchSize: BenchDefaults.sqlBatchSize
    )
}
