import Foundation

package struct TusdUpload: Decodable, Sendable {
    package let id: String?
    package let size: Int64?
    package let offset: Int64?
    package let sizeIsDeferred: Bool?
    package let metaData: [String: String]?
    package let storage: TusdStorage?
    package let infoPath: String?

    enum CodingKeys: String, CodingKey {
        case id = "ID"
        case size = "Size"
        case offset = "Offset"
        case sizeIsDeferred = "SizeIsDeferred"
        case metaData = "MetaData"
        case storage = "Storage"
        case infoPath = "InfoPath"
    }
}

package struct TusdStorage: Decodable, Sendable {
    package let type: String?
    package let path: String?
    package let infoPath: String?
    package let bucket: String?
    package let key: String?

    enum CodingKeys: String, CodingKey {
        case type = "Type"
        case path = "Path"
        case infoPath = "InfoPath"
        case bucket = "Bucket"
        case key = "Key"
    }
}

