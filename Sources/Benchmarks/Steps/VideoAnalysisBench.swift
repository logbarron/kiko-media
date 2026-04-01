import Foundation
import AVFoundation
import CoreMedia
import KikoMediaCore

struct VideoInfo: Sendable {
    let codec: String
    let width: Int
    let height: Int
    let fps: Float
    let duration: Double
    let estimatedFrames: Int
}

func codecName(_ type: CMVideoCodecType) -> String {
    switch type {
    case kCMVideoCodecType_H264: return "H.264"
    case kCMVideoCodecType_HEVC: return "HEVC"
    case kCMVideoCodecType_MPEG4Video: return "MPEG4"
    default:
        var big = type.bigEndian
        return withUnsafeBytes(of: &big) { buf in
            String(buf.compactMap { $0 >= 0x20 && $0 < 0x7F ? Character(UnicodeScalar($0)) : nil })
        }
    }
}

func getVideoInfo(path: String) async throws -> VideoInfo {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    let tracks = try await asset.loadTracks(withMediaType: .video)
    guard let track = tracks.first else { throw VideoProcessorError.noVideoTrack }
    let size = try await track.load(.naturalSize)
    let fps = try await track.load(.nominalFrameRate)
    let descs = try await track.load(.formatDescriptions)
    let duration = try await asset.load(.duration)
    let codecType = descs.first.map { CMFormatDescriptionGetMediaSubType($0) } ?? 0
    return VideoInfo(
        codec: codecName(codecType),
        width: Int(size.width),
        height: Int(size.height),
        fps: fps,
        duration: duration.seconds,
        estimatedFrames: fps > 0 ? Int(Double(fps) * duration.seconds) : 0
    )
}

func benchmarkVideoAnalysis(corpus: [MediaFile]) async throws {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Video Source Analysis")
    let videos = corpus.filter { $0.type == .video }
    guard !videos.isEmpty else { BenchOutput.line("  No video files in media folder, skipping"); return }
    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "File", width: 14),
        BenchmarkRuntimeTableColumn(header: "Codec", width: 6),
        BenchmarkRuntimeTableColumn(header: "Resolution", width: 11),
        BenchmarkRuntimeTableColumn(header: "FPS", width: 5, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Frames", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Duration", width: 8, alignment: .right),
    ]

    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for file in videos {
        let info = try await getVideoInfo(path: file.path)
        BenchmarkRuntimeRenderer.printTableRow(
            [
                file.description,
                info.codec,
                "\(info.width)x\(info.height)",
                String(format: "%.0f", info.fps),
                "\(info.estimatedFrames)",
                String(format: "%.1fs", info.duration),
            ],
            columns: columns
        )
    }
}
