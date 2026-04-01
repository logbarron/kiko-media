import Foundation

actor BenchOutputCaptureGate {
    static let shared = BenchOutputCaptureGate()
    private var locked = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    private func acquire() async {
        if !locked {
            locked = true
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    private func release() {
        if let next = waiters.first {
            waiters.removeFirst()
            next.resume()
            return
        }
        locked = false
    }

    func withExclusive<T>(_ operation: @Sendable () throws -> T) async rethrows -> T {
        await acquire()
        defer { release() }
        return try operation()
    }

    func withExclusive<T>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
        await acquire()
        defer { release() }
        return try await operation()
    }
}
