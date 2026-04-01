import Foundation
import HTTPTypes
import Hummingbird
import KikoMediaCore
import NIOCore
import NIOPosix

struct FileServer: Sendable {
    let thumbsDir: String
    let previewsDir: String
    let database: Database
    let fileIO: FileIO
    let cacheControl: String

    init(thumbsDir: String, previewsDir: String, database: Database, cacheControl: String) {
        self.thumbsDir = thumbsDir
        self.previewsDir = previewsDir
        self.database = database
        self.fileIO = FileIO(threadPool: .singleton)
        self.cacheControl = cacheControl
    }

    private func resolvedAsset(id: String) async throws -> Asset {
        guard let asset = try await database.getAsset(id: id) else {
            throw HTTPError(.notFound)
        }
        guard asset.status != .failed else { throw HTTPError(.gone) }
        return asset
    }

    func serveThumbnail(_ request: Request, context: some RequestContext, id: String) async throws -> Response {
        _ = try await resolvedAsset(id: id)
        let path = "\(thumbsDir)/\(id).jpg"
        guard isPathWithinDirectory(path: path, directory: thumbsDir) else {
            throw HTTPError(.badRequest)
        }
        return try await serveFile(path: path, contentType: "image/jpeg", request: request, context: context)
    }

    func servePreview(_ request: Request, context: some RequestContext, id: String) async throws -> Response {
        let asset = try await resolvedAsset(id: id)

        let (path, contentType): (String, String)
        switch asset.type {
        case .image:
            path = "\(previewsDir)/\(id).jpg"
            contentType = "image/jpeg"
        case .video:
            path = "\(previewsDir)/\(id).mp4"
            contentType = "video/mp4"
        }

        guard isPathWithinDirectory(path: path, directory: previewsDir) else {
            throw HTTPError(.badRequest)
        }

        return try await serveFile(path: path, contentType: contentType, request: request, context: context)
    }

    func isPathWithinDirectory(path: String, directory: String) -> Bool {
        let resolvedPath = URL(fileURLWithPath: path).resolvingSymlinksInPath().path
        let resolvedDir = URL(fileURLWithPath: directory).resolvingSymlinksInPath().path
        return resolvedPath.hasPrefix(resolvedDir + "/")
    }

    private enum RangeParseResult {
        case valid(ClosedRange<Int>)
        case unsatisfiable
        case invalid
    }

    private func serveFile(path: String, contentType: String, request: Request, context: some RequestContext) async throws -> Response {
        let fileSize: Int
        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: path)
            fileSize = attributes[.size] as? Int ?? 0
        } catch {
            throw HTTPError(.notFound)
        }

        guard fileSize > 0 else {
            throw HTTPError(.notFound)
        }

        var headers = HTTPFields()
        headers[.contentType] = contentType
        headers[.cacheControl] = cacheControl
        headers[.acceptRanges] = "bytes"
        headers[.xContentTypeOptions] = "nosniff"

        if let rangeHeader = request.headers[.range] {
            switch parseRange(rangeHeader, fileSize: fileSize) {
            case .valid(let range):
                headers[.contentLength] = String(range.count)
                headers[.contentRange] = "bytes \(range.lowerBound)-\(range.upperBound)/\(fileSize)"
                let body = try await fileIO.loadFile(path: path, range: range, context: context)
                return Response(status: .partialContent, headers: headers, body: body)
            case .unsatisfiable, .invalid:
                headers[.contentRange] = "bytes */\(fileSize)"
                return Response(status: .rangeNotSatisfiable, headers: headers)
            }
        }

        headers[.contentLength] = String(fileSize)
        let body = try await fileIO.loadFile(path: path, context: context)
        return Response(status: .ok, headers: headers, body: body)
    }

    private func parseRange(_ header: String, fileSize: Int) -> RangeParseResult {
        guard header.hasPrefix("bytes=") else { return .invalid }
        let spec = header.dropFirst(6)
        let parts = spec.split(separator: "-", omittingEmptySubsequences: false)
        guard parts.count == 2 else { return .invalid }

        func isASCIIDigits(_ value: String) -> Bool {
            !value.isEmpty && value.utf8.allSatisfy { $0 >= 48 && $0 <= 57 }
        }

        let startStr = String(parts[0])
        let endStr = String(parts[1])

        if startStr.isEmpty {
            guard isASCIIDigits(endStr), let suffix = Int(endStr), suffix > 0 else { return .invalid }
            return .valid(max(0, fileSize - suffix)...(fileSize - 1))
        }

        guard isASCIIDigits(startStr), let start = Int(startStr) else { return .invalid }
        let end: Int
        if endStr.isEmpty {
            end = fileSize - 1
        } else {
            guard isASCIIDigits(endStr), let parsedEnd = Int(endStr) else { return .invalid }
            end = min(parsedEnd, fileSize - 1)
        }
        guard start <= end else { return .unsatisfiable }
        guard start < fileSize else { return .unsatisfiable }
        return .valid(start...end)
    }
}
