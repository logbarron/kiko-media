import Foundation
import Hummingbird
import KikoMediaCore
import OSLog

struct TusdHookRequest: Decodable, Sendable {
    let type: String
    let event: TusdHookEvent

    enum CodingKeys: String, CodingKey {
        case type = "Type"
        case event = "Event"
    }
}

struct TusdHookEvent: Decodable, Sendable {
    let upload: TusdUpload?
    let httpRequest: TusdHTTPRequest?

    enum CodingKeys: String, CodingKey {
        case upload = "Upload"
        case httpRequest = "HTTPRequest"
    }
}

struct TusdHTTPRequest: Decodable, Sendable {
    let method: String?
    let uri: String?
    let remoteAddr: String?
    let header: [String: [String]]?

    enum CodingKeys: String, CodingKey {
        case method = "Method"
        case uri = "URI"
        case remoteAddr = "RemoteAddr"
        case header = "Header"
    }
}

struct WebhookHandler: Sendable {
    static let defaultQueueFullRetryAfterSeconds = 5

    let database: Database
    let processor: MediaProcessor
    let uploadDir: String
    let sessionCookie: SessionCookie?
    let jsonMaxBodyBytes: Int
    let queueFullRetryAfterSeconds: Int

    init(
        database: Database,
        processor: MediaProcessor,
        uploadDir: String,
        sessionCookie: SessionCookie?,
        jsonMaxBodyBytes: Int = RequestBodyLimits.defaultJSONMaxBytes,
        queueFullRetryAfterSeconds: Int = Self.defaultQueueFullRetryAfterSeconds
    ) {
        self.database = database
        self.processor = processor
        self.uploadDir = uploadDir
        self.sessionCookie = sessionCookie
        self.jsonMaxBodyBytes = jsonMaxBodyBytes
        self.queueFullRetryAfterSeconds = queueFullRetryAfterSeconds
    }

    func handleHook(_ request: Request, context _: some RequestContext) async throws -> Response {
        let hook = try await decodeJSONBody(
            TusdHookRequest.self,
            from: request,
            maxBytes: jsonMaxBodyBytes
        ) { error in
            Logger.kiko.warning("Webhook: invalid JSON body: \(error)")
        }

        let status: HTTPResponse.Status
        switch hook.type {
        case "pre-create":
            status = try handlePreCreate(hook: hook)
        case "post-finish":
            status = try await handleUploadComplete(hook: hook)
        default:
            Logger.kiko.info("tusd hook received: \(hook.type) (ignored)")
            status = .ok
        }

        return Response(
            status: status,
            headers: [.contentType: "application/json"],
            body: .init(byteBuffer: .init(string: "{}"))
        )
    }

    private func handlePreCreate(hook: TusdHookRequest) throws -> HTTPResponse.Status {
        Logger.kiko.info("tusd hook received: pre-create")

        if let cookie = sessionCookie {
            let headers = hook.event.httpRequest?.header
            guard cookie.validateFromTusdHeaders(headers) else {
                Logger.kiko.warning("pre-create rejected: invalid or missing session cookie")
                throw HTTPError(.unauthorized)
            }
            Logger.kiko.info("pre-create: session cookie valid")
        }

        return .ok
    }

    private func handleUploadComplete(hook: TusdHookRequest) async throws -> HTTPResponse.Status {
        guard let upload = hook.event.upload, let uploadId = upload.id else {
            Logger.kiko.error("Webhook missing upload ID")
            throw HTTPError(.badRequest)
        }

        guard Asset.isValidId(uploadId) else {
            Logger.kiko.warning("Security: Rejected invalid upload ID: \(uploadId)")
            throw HTTPError(.badRequest)
        }

        guard let uploadSize = upload.size, let uploadOffset = upload.offset else {
            Logger.kiko.warning("Webhook rejected: missing size/offset for id=\(uploadId)")
            throw HTTPError(.badRequest)
        }

        guard upload.sizeIsDeferred != true else {
            Logger.kiko.warning("Webhook rejected: deferred size at completion for id=\(uploadId)")
            throw HTTPError(.badRequest)
        }

        guard uploadSize > 0, uploadOffset >= 0 else {
            Logger.kiko.warning("Webhook rejected: non-positive size or negative offset for id=\(uploadId)")
            throw HTTPError(.badRequest)
        }

        guard uploadOffset == uploadSize else {
            Logger.kiko.warning("Webhook rejected: offset/size mismatch for id=\(uploadId)")
            throw HTTPError(.badRequest)
        }

        let originalName = Asset.sanitizedOriginalName(upload.metaData?["filename"])
        let loggedName = sanitizeFilenameForLog(originalName)
        let filePath = upload.storage?.path ?? "\(uploadDir)/\(uploadId)"

        let resolvedPath = URL(fileURLWithPath: filePath).resolvingSymlinksInPath().path
        let resolvedUploadDir = URL(fileURLWithPath: uploadDir).resolvingSymlinksInPath().path
        guard resolvedPath.hasPrefix(resolvedUploadDir + "/") else {
            Logger.kiko.warning("Security: Rejected path outside upload directory for id=\(uploadId)")
            throw HTTPError(.badRequest)
        }

        guard FileManager.default.fileExists(atPath: resolvedPath) else {
            Logger.kiko.error("File not found for id=\(uploadId)")
            throw HTTPError(.badRequest)
        }

        let canAcceptWebhookEnqueue = await processor.canAcceptWebhookBackpressure()
        if !canAcceptWebhookEnqueue {
            if try await database.assetExists(id: uploadId) {
                Logger.kiko.info("Asset \(uploadId) already exists while queue is full or processor is shutting down, skipping")
                return .ok
            }

            Logger.kiko.warning("Queue full or shutting down: rejecting \(uploadId) before DB insert")
            throw HTTPError(
                .serviceUnavailable,
                headers: [.retryAfter: "\(queueFullRetryAfterSeconds)"]
            )
        }

        Logger.kiko.info("Webhook received: id=\(uploadId), name=\(loggedName)")

        let assetType: Asset.AssetType = ImageProcessor.isImage(path: resolvedPath) ? .image : .video
        let wasInserted = try await database.insertQueued(id: uploadId, type: assetType, originalName: originalName)

        if wasInserted {
            let enqueueAccepted = await processor.enqueueWebhookAsset(
                uploadId: uploadId,
                originalName: originalName,
                filePath: resolvedPath,
                assetType: assetType
            )
            if enqueueAccepted {
                Logger.kiko.info("Asset \(uploadId) queued for processing")
            } else {
                do {
                    let rolledBack = try await database.deleteQueued(id: uploadId)
                    if rolledBack {
                        Logger.kiko.warning("Queue full or shutting down: rolled back queued asset \(uploadId)")
                    } else {
                        Logger.kiko.warning("Queue full or shutting down for \(uploadId); DB row no longer queued during rollback")
                    }
                } catch {
                    Logger.kiko.error("Failed to roll back queued asset \(uploadId) after enqueue rejection: \(error)")
                }

                throw HTTPError(
                    .serviceUnavailable,
                    headers: [.retryAfter: "\(queueFullRetryAfterSeconds)"]
                )
            }
        } else {
            Logger.kiko.info("Asset \(uploadId) already exists, skipping")
        }

        return .ok
    }

    private func sanitizeFilenameForLog(_ rawName: String) -> String {
        var sanitized = ""
        sanitized.reserveCapacity(rawName.count)

        for scalar in rawName.unicodeScalars {
            if CharacterSet.controlCharacters.contains(scalar) {
                sanitized.append(" ")
            } else if scalar.value == 0x2F || scalar.value == 0x5C {
                sanitized.append("_")
            } else {
                sanitized.unicodeScalars.append(scalar)
            }
        }

        let trimmed = sanitized.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "unknown" }

        let maxLoggedNameLength = 120
        guard trimmed.count > maxLoggedNameLength else { return trimmed }
        return String(trimmed.prefix(maxLoggedNameLength)) + "..."
    }
}
