import Testing
import Foundation
import KikoMediaCore
@testable import KikoMediaApp

@Suite("Webhook Payload Decoding")
struct WebhookPayloadTests {

    @Test("Decode valid post-finish payload")
    func postFinish() throws {
        let json = """
        {
            "Type": "post-finish",
            "Event": {
                "Upload": {
                    "ID": "abc123def456",
                    "Size": 12345678,
                    "Offset": 12345678,
                    "MetaData": {
                        "filename": "IMG_1234.HEIC",
                        "filetype": "image/heic"
                    },
                    "Storage": {
                        "Type": "filestore",
                        "Path": "/Users/test/uploads/abc123def456"
                    }
                }
            }
        }
        """
        let data = Data(json.utf8)
        let hook = try JSONDecoder().decode(TusdHookRequest.self, from: data)

        #expect(hook.type == "post-finish")
        #expect(hook.event.upload?.id == "abc123def456")
        #expect(hook.event.upload?.size == 12345678)
        #expect(hook.event.upload?.metaData?["filename"] == "IMG_1234.HEIC")
        #expect(hook.event.upload?.metaData?["filetype"] == "image/heic")
        #expect(hook.event.upload?.storage?.type == "filestore")
        #expect(hook.event.upload?.storage?.path == "/Users/test/uploads/abc123def456")
    }

    @Test("Decode valid pre-create payload")
    func preCreate() throws {
        let json = """
        {
            "Type": "pre-create",
            "Event": {
                "Upload": {
                    "Size": 5242880,
                    "SizeIsDeferred": false,
                    "MetaData": {
                        "filename": "video.mp4",
                        "filetype": "video/mp4"
                    }
                },
                "HTTPRequest": {
                    "Method": "POST",
                    "URI": "/files/",
                    "RemoteAddr": "127.0.0.1:54321",
                    "Header": {
                        "Cookie": ["kiko_session=abc.def"]
                    }
                }
            }
        }
        """
        let data = Data(json.utf8)
        let hook = try JSONDecoder().decode(TusdHookRequest.self, from: data)

        #expect(hook.type == "pre-create")
        #expect(hook.event.upload?.id == nil) // pre-create has no ID yet
        #expect(hook.event.upload?.size == 5242880)
        #expect(hook.event.httpRequest?.method == "POST")
        #expect(hook.event.httpRequest?.header?["Cookie"]?.first == "kiko_session=abc.def")
    }

    @Test("Decode minimal payload (optional fields missing)")
    func minimalPayload() throws {
        let json = """
        {
            "Type": "post-finish",
            "Event": {
                "Upload": {
                    "ID": "minimal-001"
                }
            }
        }
        """
        let data = Data(json.utf8)
        let hook = try JSONDecoder().decode(TusdHookRequest.self, from: data)

        #expect(hook.type == "post-finish")
        #expect(hook.event.upload?.id == "minimal-001")
        #expect(hook.event.upload?.metaData == nil)
        #expect(hook.event.upload?.storage == nil)
        #expect(hook.event.httpRequest == nil)
    }

    @Test("CodingKeys map PascalCase JSON to camelCase Swift")
    func codingKeys() throws {
        let json = """
        {
            "Type": "post-finish",
            "Event": {
                "Upload": {
                    "ID": "test",
                    "SizeIsDeferred": true,
                    "InfoPath": "/tmp/test.info"
                }
            }
        }
        """
        let data = Data(json.utf8)
        let hook = try JSONDecoder().decode(TusdHookRequest.self, from: data)

        #expect(hook.event.upload?.sizeIsDeferred == true)
        #expect(hook.event.upload?.infoPath == "/tmp/test.info")
    }

    @Test("Invalid JSON throws DecodingError")
    func invalidJSON() {
        let data = Data("not json".utf8)
        #expect {
            try JSONDecoder().decode(TusdHookRequest.self, from: data)
        } throws: { error in
            error is DecodingError
        }
    }

    @Test("Missing required field (Type) throws DecodingError")
    func missingRequiredField() {
        let json = """
        {"Event": {"Upload": {"ID": "test"}}}
        """
        #expect {
            try JSONDecoder().decode(TusdHookRequest.self, from: Data(json.utf8))
        } throws: { error in
            error is DecodingError
        }
    }

    @Test("Missing required field (Event) throws DecodingError")
    func missingEvent() {
        let json = """
        {"Type": "post-finish"}
        """
        #expect {
            try JSONDecoder().decode(TusdHookRequest.self, from: Data(json.utf8))
        } throws: { error in
            error is DecodingError
        }
    }

    @Test("Missing Upload for post-finish payload decodes with nil upload")
    func missingUploadForPostFinish() {
        let json = """
        {"Type": "post-finish", "Event": {}}
        """
        let hook = try? JSONDecoder().decode(TusdHookRequest.self, from: Data(json.utf8))
        #expect(hook != nil)
        #expect(hook?.event.upload == nil)
    }

    @Test("Unknown Type value is preserved for forward-compatible handling")
    func unknownType() {
        let json = """
        {"Type": "post-receive", "Event": {"Upload": {"ID": "test"}}}
        """
        let hook = try? JSONDecoder().decode(TusdHookRequest.self, from: Data(json.utf8))
        #expect(hook != nil)
        #expect(hook?.type == "post-receive")
        #expect(hook?.event.upload?.id == "test")
    }

    @Test("Malformed Storage shape throws DecodingError")
    func malformedStorageShape() {
        let json = """
        {
            "Type": "post-finish",
            "Event": {
                "Upload": {
                    "ID": "abc123def456",
                    "Storage": {
                        "Type": "filestore",
                        "Path": 42
                    }
                }
            }
        }
        """
        #expect {
            try JSONDecoder().decode(TusdHookRequest.self, from: Data(json.utf8))
        } throws: { error in
            error is DecodingError
        }
    }

    @Test("Malformed HTTPRequest header shape throws DecodingError")
    func malformedHTTPRequestHeaderShape() {
        let json = """
        {
            "Type": "pre-create",
            "Event": {
                "Upload": {
                    "Size": 5242880
                },
                "HTTPRequest": {
                    "Method": "POST",
                    "URI": "/files/",
                    "Header": {
                        "Cookie": "kiko_session=abc.def"
                    }
                }
            }
        }
        """
        #expect {
            try JSONDecoder().decode(TusdHookRequest.self, from: Data(json.utf8))
        } throws: { error in
            error is DecodingError
        }
    }
}
