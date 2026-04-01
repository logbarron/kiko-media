import Foundation
import Hummingbird

@inline(__always)
func decodeJSONBody<T: Decodable>(
    _ type: T.Type,
    from request: Request,
    maxBytes: Int,
    onDecodeFailure: ((Error) -> Void)? = nil
) async throws -> T {
    do {
        let body = try await request.body.collect(upTo: maxBytes)
        return try JSONDecoder().decode(T.self, from: body)
    } catch let error as any HTTPResponseError {
        throw error
    } catch {
        onDecodeFailure?(error)
        throw HTTPError(.badRequest)
    }
}
