import Foundation

enum BenchmarkComponent: String, CaseIterable, Sendable {
    case image
    case video
    case sha256
    case db
    case archive
    case thunderbolt
    case pipeline
    case comparison
}

struct BenchmarkComponentSpec: Sendable {
    let id: BenchmarkComponent
    let title: String
    let detail: String
    let requiresMediaFolder: Bool
    let requiresSSDPath: Bool
    /// Human hint only (not a guarantee). Intended for wizards/help output.
    let expectedRuntime: String
}

enum BenchmarkCatalog {
    static let components: [BenchmarkComponentSpec] = [
        BenchmarkComponentSpec(
            id: .image,
            title: "Image processing",
            detail: "Thumbnail (512px), preview (1440px), timestamps, peak memory.",
            requiresMediaFolder: true,
            requiresSSDPath: false,
            expectedRuntime: "varies (image count)"
        ),
        BenchmarkComponentSpec(
            id: .video,
            title: "Video processing",
            detail: "Analysis, decode-only, transcode, thumbnails.",
            requiresMediaFolder: true,
            requiresSSDPath: false,
            expectedRuntime: "varies (video duration)"
        ),
        BenchmarkComponentSpec(
            id: .sha256,
            title: "SHA256 hashing",
            detail: "Hashing throughput over your media files.",
            requiresMediaFolder: true,
            requiresSSDPath: false,
            expectedRuntime: "varies (media folder size)"
        ),
        BenchmarkComponentSpec(
            id: .db,
            title: "Database",
            detail: "SQLite write/read scenarios (synthetic).",
            requiresMediaFolder: false,
            requiresSSDPath: false,
            expectedRuntime: "short"
        ),
        BenchmarkComponentSpec(
            id: .archive,
            title: "Archive to external SSD",
            detail: "Copy + SHA256 verify to an external drive. Writes a temporary bench folder.",
            requiresMediaFolder: true,
            requiresSSDPath: true,
            expectedRuntime: "varies (media folder size + SSD)"
        ),
        BenchmarkComponentSpec(
            id: .thunderbolt,
            title: "Thunderbolt offload",
            detail: "Worker connectivity, transport throughput, round-trip, burst sweep.",
            requiresMediaFolder: true,
            requiresSSDPath: false,
            expectedRuntime: "varies (video count × worker count)"
        ),
        BenchmarkComponentSpec(
            id: .pipeline,
            title: "Pipeline benches",
            detail: "Concurrency sweeps + realistic pipeline (DB + SHA + processing + optional SSD archive).",
            requiresMediaFolder: true,
            requiresSSDPath: false,
            expectedRuntime: "varies"
        ),
        BenchmarkComponentSpec(
            id: .comparison,
            title: "Comparison benches",
            detail: "Curves for tradeoffs (JPEG quality, thumbnail size, buffer sizes, presets).",
            requiresMediaFolder: true,
            requiresSSDPath: false,
            expectedRuntime: "varies"
        ),
    ]

    static func spec(for id: BenchmarkComponent) -> BenchmarkComponentSpec {
        // Safe because `components` is the single source of truth.
        components.first(where: { $0.id == id })!
    }
}
