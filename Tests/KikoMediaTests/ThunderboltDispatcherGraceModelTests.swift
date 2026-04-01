import Darwin
import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Thunderbolt dispatcher grace model", Testing.ParallelizationTrait.serialized)
struct ThunderboltDispatcherGraceModelTests {
    @Test("grace path avoids tight polling connect loop")
    func gracePathAvoidsTightPolling() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "tb-grace-model-001"
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data(repeating: 0x61, count: 256).write(to: URL(fileURLWithPath: uploadPath))

        let counter = ConnectAttemptCounter()
        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6552,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                counter.increment()
                return nil
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let clock = ContinuousClock()
        let started = clock.now
        let result = await dispatcher.dispatch(
            uploadId: uploadID,
            filePath: uploadPath,
            originalName: "\(uploadID).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )
        let elapsed = clock.now - started
        let elapsedSeconds = Double(elapsed.components.seconds)
            + (Double(elapsed.components.attoseconds) / 1_000_000_000_000_000_000)

        #expect(result == .transientRetry(slotHealthDown: true))
        #expect(elapsedSeconds >= 1.2, "Grace timeout should wait approximately 1.5s before retry path")

        let attempts = counter.value()
        #expect(attempts <= 10, "Non-blocking grace model should avoid ~100ms polling loops")

        await dispatcher.shutdown()
    }

    @Test("grace recovery within window skips durable retry increment")
    func graceRecoverySkipsRetryIncrement() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "tb-grace-model-002"
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data(repeating: 0x62, count: 256).write(to: URL(fileURLWithPath: uploadPath))

        let workerPort = try reserveLoopbackPort()
        let retryCounter = AsyncCounter()
        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: workerPort,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            bridgeSourcesOverride: [bridge],
            queryCapabilitiesOverride: { _, _, _, _ in nil },
            onRetryIncrement: { _ in
                await retryCounter.increment()
                return 1
            }
        )

        let clock = ContinuousClock()
        let started = clock.now
        let dispatchTask = Task {
            await dispatcher.dispatch(
                uploadId: uploadID,
                filePath: uploadPath,
                originalName: "\(uploadID).mov",
                mimeType: "video/quicktime",
                targetWorkerIndex: 0,
                targetSlotIndex: 0
            )
        }

        let markedDown = try await waitUntil(timeoutSeconds: 1) {
            await dispatcher.dispatchCapacity() == 0
        }
        #expect(markedDown)

        let server = try LoopbackServer(port: workerPort)
        try server.start()
        defer { server.stop() }

        let result = await dispatchTask.value
        let elapsed = clock.now - started
        let elapsedSeconds = Double(elapsed.components.seconds)
            + (Double(elapsed.components.attoseconds) / 1_000_000_000_000_000_000)

        #expect(result == .transientRetry(slotHealthDown: false))
        #expect(elapsedSeconds < 4.0, "Recovered grace path should return well before long backoff probe windows")
        #expect(await retryCounter.value() == 0)

        await dispatcher.shutdown()
    }
}

private final class ConnectAttemptCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count: Int = 0

    func increment() {
        lock.lock()
        count += 1
        lock.unlock()
    }

    func value() -> Int {
        lock.lock()
        let current = count
        lock.unlock()
        return current
    }
}

private actor AsyncCounter {
    private var count = 0

    func increment() {
        count += 1
    }

    func value() -> Int {
        count
    }
}

private func waitUntil(
    timeoutSeconds: TimeInterval,
    pollEveryMillis: UInt64 = 25,
    condition: @escaping @Sendable () async throws -> Bool
) async throws -> Bool {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        if try await condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(Int(pollEveryMillis)))
    }
    return try await condition()
}

private func reserveLoopbackPort() throws -> Int {
    let fd = socket(AF_INET, SOCK_STREAM, 0)
    guard fd >= 0 else { throw POSIXError(.ENOTSOCK) }
    defer { Darwin.close(fd) }

    var one: Int32 = 1
    _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, socklen_t(MemoryLayout<Int32>.size))

    var address = sockaddr_in()
    address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = in_port_t(0).bigEndian
    address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

    let bindRC = withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
            Darwin.bind(fd, socketAddress, socklen_t(MemoryLayout<sockaddr_in>.size))
        }
    }
    guard bindRC == 0 else { throw POSIXError(.EADDRNOTAVAIL) }

    var bound = sockaddr_in()
    var len = socklen_t(MemoryLayout<sockaddr_in>.size)
    let nameRC = withUnsafeMutablePointer(to: &bound) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
            getsockname(fd, socketAddress, &len)
        }
    }
    guard nameRC == 0 else { throw POSIXError(.EINVAL) }
    return Int(UInt16(bigEndian: bound.sin_port))
}

private final class LoopbackServer {
    private let port: Int
    private var listenFD: Int32 = -1
    private var acceptTask: Task<Void, Never>?

    init(port: Int) throws {
        self.port = port
    }

    func start() throws {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw POSIXError(.ENOTSOCK) }

        var one: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, socklen_t(MemoryLayout<Int32>.size))
        _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))

        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = in_port_t(UInt16(port)).bigEndian
        address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

        let bindRC = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
                Darwin.bind(fd, socketAddress, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindRC == 0 else {
            Darwin.close(fd)
            throw POSIXError(.EADDRINUSE)
        }

        guard listen(fd, 8) == 0 else {
            Darwin.close(fd)
            throw POSIXError(.ECONNREFUSED)
        }

        listenFD = fd
        acceptTask = Task.detached(priority: .utility) { [fd] in
            while !Task.isCancelled {
                var clientAddress = sockaddr_in()
                var clientLength = socklen_t(MemoryLayout<sockaddr_in>.size)
                let clientFD = withUnsafeMutablePointer(to: &clientAddress) { pointer in
                    pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
                        accept(fd, socketAddress, &clientLength)
                    }
                }
                if clientFD >= 0 {
                    Darwin.close(clientFD)
                    continue
                }
                if errno == EINTR {
                    continue
                }
                if errno == EBADF || errno == EINVAL {
                    return
                }
                try? await Task.sleep(for: .milliseconds(10))
            }
        }
    }

    func stop() {
        acceptTask?.cancel()
        acceptTask = nil
        if listenFD >= 0 {
            Darwin.close(listenFD)
            listenFD = -1
        }
    }
}
