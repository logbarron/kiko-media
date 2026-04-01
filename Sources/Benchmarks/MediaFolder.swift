import Foundation
import KikoMediaCore
import UniformTypeIdentifiers

enum MediaType: String, Sendable {
    case image
    case video
}

struct MediaFile: Sendable {
    let path: String
    let name: String
    let type: MediaType
    let sizeBytes: Int

    var description: String {
        let sizeMB = Double(sizeBytes) / (1024 * 1024)
        let ext = URL(fileURLWithPath: path).pathExtension.uppercased()
        if sizeMB >= 1 {
            return "\(ext) \(String(format: "%.1f", sizeMB))MB"
        }
        return "\(ext) \(sizeBytes / 1024)KB"
    }
}

struct MediaFolderSummary: Sendable {
    let imageCount: Int
    let videoCount: Int
    let totalBytes: Int64

    var summaryLine: String {
        let mb = totalBytes / (1024 * 1024)
        return "\(imageCount) images, \(videoCount) videos (\(mb)MB)"
    }
}

func isImage(path: String) -> Bool {
    ImageProcessor.isImage(path: path)
}

func isVideo(path: String) -> Bool {
    let ext = URL(fileURLWithPath: path).pathExtension
    guard !ext.isEmpty, let type = UTType(filenameExtension: ext) else {
        return false
    }
    return type.conforms(to: .movie) || type.conforms(to: .video)
}

func loadMediaFolder(path: String) throws -> [MediaFile] {
    let fm = FileManager.default
    guard let files = try? fm.contentsOfDirectory(atPath: path) else {
        throw BenchmarkError.mediaFolderNotFound(path)
    }

    var corpus: [MediaFile] = []
    for filename in files.sorted() {
        guard !filename.hasPrefix(".") else { continue }

        let fullPath = "\(path)/\(filename)"
        var isDir: ObjCBool = false
        guard fm.fileExists(atPath: fullPath, isDirectory: &isDir), !isDir.boolValue else { continue }

        guard let attrs = try? fm.attributesOfItem(atPath: fullPath),
              let size = attrs[.size] as? Int, size > 0 else { continue }

        if isImage(path: fullPath) {
            corpus.append(MediaFile(path: fullPath, name: filename, type: .image, sizeBytes: size))
        } else if isVideo(path: fullPath) {
            corpus.append(MediaFile(path: fullPath, name: filename, type: .video, sizeBytes: size))
        }
    }

    guard !corpus.isEmpty else {
        throw BenchmarkError.emptyMediaFolder(path)
    }

    return corpus
}

func summarizeMediaFolder(path: String) throws -> MediaFolderSummary {
    let files = try loadMediaFolder(path: path)
    let imageCount = files.filter { $0.type == .image }.count
    let videoCount = files.filter { $0.type == .video }.count
    let totalBytes = files.reduce(into: Int64(0)) { $0 += Int64($1.sizeBytes) }
    return MediaFolderSummary(imageCount: imageCount, videoCount: videoCount, totalBytes: totalBytes)
}
