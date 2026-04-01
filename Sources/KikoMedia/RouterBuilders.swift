import Hummingbird
import Foundation
import CryptoKit
import KikoMediaCore

enum RouterBuilders {
    private static let maxHeartCountBatchIDs = 500

    private static func validatedAssetId(_ context: BasicRequestContext) throws -> String {
        let id = try context.parameters.require("id")
        guard Asset.isValidId(id) else { throw HTTPError(.badRequest) }
        return id
    }

    private static func constantTimeEquals(_ lhs: String, _ rhs: String) -> Bool {
        let left = Data(SHA256.hash(data: Data(lhs.utf8)))
        let right = Data(SHA256.hash(data: Data(rhs.utf8)))
        var diff: UInt8 = 0
        for (a, b) in zip(left, right) {
            diff |= a ^ b
        }
        return diff == 0
    }

    private static func registerAssetRoutes(
        on router: Router<BasicRequestContext>,
        fileServer: FileServer,
        requireSession: (@Sendable (Request) throws -> Void)? = nil
    ) {
        router.get("/api/thumbs/{id}") { request, context in
            try requireSession?(request)
            let id = try RouterBuilders.validatedAssetId(context)
            return try await fileServer.serveThumbnail(request, context: context, id: id)
        }

        router.get("/api/preview/{id}") { request, context in
            try requireSession?(request)
            let id = try RouterBuilders.validatedAssetId(context)
            return try await fileServer.servePreview(request, context: context, id: id)
        }
    }

    static func buildPublicRouter(
        database: Database,
        fileServer: FileServer,
        sessionCookie: SessionCookie?,
        turnstileVerifier: TurnstileVerifier?,
        heartRevisionTracker: HeartRevisionTracker,
        gateSecret: String? = nil,
        turnstileVerify: (@Sendable (String) async -> TurnstileVerificationResult)? = nil,
        jsonMaxBodyBytes: Int = RequestBodyLimits.defaultJSONMaxBytes
    ) -> Router<BasicRequestContext> {
        let router = Router()

        router.get("/health") { _, _ in
            _ = try await database.getTotalAssetCount()
            return HealthResponse(status: "ok")
        }

        if let verifier = turnstileVerifier, let cookie = sessionCookie {
            let verifyToken = turnstileVerify ?? { token in
                await verifier.verify(token: token)
            }

            router.post("/api/turnstile/verify") { request, _ in
                let payload = try await decodeJSONBody(
                    TurnstileVerifyRequest.self,
                    from: request,
                    maxBytes: jsonMaxBodyBytes
                )

                switch await verifyToken(payload.token) {
                case .success:
                    break
                case .rejected:
                    throw HTTPError(.forbidden)
                case let .unavailable(retryAfterSeconds):
                    throw HTTPError(
                        .serviceUnavailable,
                        headers: [.retryAfter: "\(retryAfterSeconds)"]
                    )
                }

                if let gateSecret, !gateSecret.isEmpty {
                    guard payload.gateSecretCandidates.contains(where: { candidate in
                        constantTimeEquals(candidate, gateSecret)
                    }) else {
                        throw HTTPError(.forbidden)
                    }
                }

                let cookieValue = cookie.create()
                var response = Response(status: .noContent)
                response.headers[.setCookie] = cookie.setCookieHeader(value: cookieValue)
                return response
            }
        }

        let requireSession: @Sendable (Request) throws -> Void = { request in
            if let cookie = sessionCookie {
                guard let cookieValue = cookie.extractFromRequest(request),
                      cookie.validate(cookieValue) else {
                    throw HTTPError(.unauthorized)
                }
            }
        }

        router.get("/api/gallery") { request, _ in
            try requireSession(request)
            let limit = request.uri.queryParameters.get("limit", as: Int.self)
            let offset = request.uri.queryParameters.get("offset", as: Int.self)
            let sortByHearts = request.uri.queryParameters.get("sort") == "hearts"
            let gallery = try await database.getGalleryAssetsAndCount(limit: limit, offset: offset, sortByHearts: sortByHearts)
            let heartRevision = await heartRevisionTracker.current()
            return GalleryResponse(
                assets: gallery.assets.map { $0.toGalleryAsset() },
                total: gallery.total,
                heartRevision: heartRevision
            )
        }

        router.post("/api/assets/{id}/heart") { request, context in
            try requireSession(request)
            let id = try RouterBuilders.validatedAssetId(context)
            do {
                let newCount = try await database.incrementHeartCount(id: id)
                _ = await heartRevisionTracker.bump()
                return HeartResponse(heartCount: newCount)
            } catch is DatabaseWriteError {
                throw HTTPError(.notFound)
            }
        }

        router.post("/api/heart-counts") { request, _ in
            try requireSession(request)
            let payload = try await decodeJSONBody(
                HeartCountsRequest.self,
                from: request,
                maxBytes: jsonMaxBodyBytes
            )

            guard payload.ids.count <= RouterBuilders.maxHeartCountBatchIDs else {
                throw HTTPError(.badRequest)
            }

            var uniqueIds: [String] = []
            uniqueIds.reserveCapacity(payload.ids.count)
            var seenIds = Set<String>()
            seenIds.reserveCapacity(payload.ids.count)

            for id in payload.ids {
                guard Asset.isValidId(id) else {
                    throw HTTPError(.badRequest)
                }
                if seenIds.insert(id).inserted {
                    uniqueIds.append(id)
                }
            }

            let heartCounts = try await database.getHeartCounts(ids: uniqueIds)
            return HeartCountsResponse(heartCounts: heartCounts)
        }

        RouterBuilders.registerAssetRoutes(on: router, fileServer: fileServer, requireSession: requireSession)

        return router
    }

    static func buildInternalRouter(
        database: Database,
        fileServer: FileServer,
        moderationMarkers: ModerationMarkers,
        webhookHandler: WebhookHandler,
        internalAuthSecret: String? = nil,
        updateAssetStatus: (@Sendable (_ id: String, _ status: Asset.AssetStatus) async throws -> Void)? = nil,
        jsonMaxBodyBytes: Int = RequestBodyLimits.defaultJSONMaxBytes
    ) -> Router<BasicRequestContext> {
        let router = Router()
        let persistModerationStatus: @Sendable (_ id: String, _ status: Asset.AssetStatus) async throws -> Void = updateAssetStatus ?? { id, status in
            try await database.updateStatus(id: id, status: status)
        }
        let requireInternalAuthorization: @Sendable (Request) throws -> Void = { request in
            guard let secret = internalAuthSecret, !secret.isEmpty else {
                // Fail closed: protected internal routes must never become unauthenticated
                // due to missing shared-secret configuration.
                throw HTTPError(.unauthorized)
            }
            guard let provided = request.headers[.authorization],
                  RouterBuilders.constantTimeEquals(provided, secret) else {
                throw HTTPError(.unauthorized)
            }
        }

        router.post("/hooks/upload-complete") { request, context in
            try await webhookHandler.handleHook(request, context: context)
        }

        router.get("/api/gallery") { request, _ in
            try requireInternalAuthorization(request)
            let limit = request.uri.queryParameters.get("limit", as: Int.self)
            let offset = request.uri.queryParameters.get("offset", as: Int.self)
            let gallery = try await database.getModerationAssetsAndCount(limit: limit, offset: offset)
            return GalleryResponse(
                assets: gallery.assets.map { $0.toGalleryAsset(status: $0.status) },
                total: gallery.total,
                heartRevision: nil
            )
        }

        router.patch("/api/assets/{id}") { request, context in
            try requireInternalAuthorization(request)
            let id = try RouterBuilders.validatedAssetId(context)
            let payload = try await decodeJSONBody(
                ModerationRequest.self,
                from: request,
                maxBytes: jsonMaxBodyBytes
            )

            guard let asset = try await database.getAsset(id: id) else {
                throw HTTPError(.notFound)
            }

            guard asset.status == .complete || asset.status == .moderated else {
                throw HTTPError(.badRequest)
            }

            let previousStatus = asset.status
            let newStatus: Asset.AssetStatus = payload.status == .moderated ? .moderated : .complete

            do {
                if newStatus == .moderated {
                    try moderationMarkers.mark(id)
                } else {
                    try moderationMarkers.unmark(id)
                }
            } catch {
                throw HTTPError(.internalServerError)
            }

            do {
                try await persistModerationStatus(id, newStatus)
            } catch {
                do {
                    if previousStatus == .moderated {
                        try moderationMarkers.mark(id)
                    } else {
                        try moderationMarkers.unmark(id)
                    }
                } catch {
                    throw HTTPError(.internalServerError)
                }
                throw HTTPError(.internalServerError)
            }

            return HTTPResponse.Status.noContent
        }

        RouterBuilders.registerAssetRoutes(on: router, fileServer: fileServer, requireSession: requireInternalAuthorization)

        return router
    }
}
