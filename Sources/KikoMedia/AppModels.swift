import Foundation
import Hummingbird
import KikoMediaCore

struct HealthResponse: ResponseCodable {
    let status: String
}

struct GalleryAsset: Encodable, Sendable {
    let id: String
    let type: String
    let status: String?
    let heartCount: Int
}

struct HeartResponse: ResponseEncodable {
    let heartCount: Int
}

struct GalleryResponse: ResponseEncodable {
    let assets: [GalleryAsset]
    let total: Int
    let heartRevision: Int?
}

struct HeartCountsRequest: Decodable, Sendable {
    let ids: [String]
}

struct HeartCountsResponse: ResponseEncodable {
    let heartCounts: [String: Int]
}

enum ModerationStatus: String, Decodable, Sendable {
    case complete
    case moderated
}

struct ModerationRequest: Decodable, Sendable {
    let status: ModerationStatus
}

struct TurnstileVerifyRequest: Decodable, Sendable {
    let token: String
    let gateSecret: String?
    let password: String?
    let inviteToken: String?

    var gateSecretCandidates: [String] {
        [gateSecret, password, inviteToken].compactMap { value in
            guard let value, !value.isEmpty else { return nil }
            return value
        }
    }
}

extension Asset {
    func toGalleryAsset(status: AssetStatus? = nil) -> GalleryAsset {
        GalleryAsset(id: id, type: type.rawValue, status: status?.rawValue, heartCount: heartCount)
    }
}
