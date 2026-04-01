import Testing
import Hummingbird
import HummingbirdTesting
@testable import KikoMediaCore
@testable import KikoMediaApp

@Suite("Test Helper Security")
struct TestHelpersSecurityTests {
    @Test("gatedPublicRouter rejects requests without session cookie")
    func gatedPublicRouterRejectsMissingCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let gated = env.gatedPublicRouter()
        let app = Application(router: gated.router)

        try await app.test(.router) { client in
            try await client.execute(uri: "/api/gallery", method: .get) { response in
                #expect(response.status == .unauthorized)
            }
        }
    }

    @Test("gatedPublicRouter accepts valid session cookie")
    func gatedPublicRouterAcceptsValidCookie() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let gated = env.gatedPublicRouter()
        let app = Application(router: gated.router)
        let cookieName = gated.cookie.name
        let cookieValue = gated.cookie.create()

        try await app.test(.router) { client in
            try await client.execute(
                uri: "/api/gallery",
                method: .get,
                headers: [.cookie: "\(cookieName)=\(cookieValue)"]
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }
}
