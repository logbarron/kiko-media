import CryptoKit
import Darwin
import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Thunderbolt dispatcher tick stream framing", Testing.ParallelizationTrait.serialized)
struct ThunderboltDispatcherTickStreamTests {
    @Test("late running tick after complete is skipped before response header parse")
    func lateRunningTickAfterCompleteIsIgnored() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "tb-tick-framing-late-running-001"
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data().write(to: URL(fileURLWithPath: uploadPath))

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let bridge = loopbackBridgeSource()
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6550,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let workerStart = WorkerStartBarrier()
        let workerTask = Task.detached(priority: .userInitiated) {
            await workerStart.signalStarted()
            defer { Darwin.close(pair.serverFD) }
            guard let request = readDispatchRequest(fd: pair.serverFD),
                  let context = parseTickContext(fromWorkerMime: request.mime) else {
                return
            }

            let running = ProgressTickV2(
                status: .running,
                jobHandle: context.jobHandle,
                sessionID: context.sessionID,
                errorClass: .none,
                progress: 0.30,
                elapsedMS: 120,
                estRemainingMS: 800
            ).encode()
            let complete = ProgressTickV2(
                status: .complete,
                jobHandle: context.jobHandle,
                sessionID: context.sessionID,
                errorClass: .none,
                progress: 1.0,
                elapsedMS: 900,
                estRemainingMS: 0
            ).encode()
            // This late frame is the desync trigger: coordinator has already exited tick loop.
            let lateRunning = ProgressTickV2(
                status: .running,
                jobHandle: context.jobHandle,
                sessionID: context.sessionID,
                errorClass: .none,
                progress: 1.0,
                elapsedMS: 901,
                estRemainingMS: 0
            ).encode()

            _ = writeAll(fd: pair.serverFD, data: running)
            _ = writeAll(fd: pair.serverFD, data: complete)
            _ = writeAll(fd: pair.serverFD, data: lateRunning)

            let previewPayload = Data("preview".utf8)
            let thumbPayload = Data("thumb".utf8)
            let header = makeResponseHeader(
                status: 0x01,
                processNanos: 123_456_789,
                previewPayload: previewPayload,
                thumbPayload: thumbPayload
            )
            _ = writeAll(fd: pair.serverFD, data: header)
            _ = writeAll(fd: pair.serverFD, data: previewPayload)
            _ = writeAll(fd: pair.serverFD, data: thumbPayload)
        }
        await workerStart.waitUntilStarted()

        let result = await dispatcher.dispatch(
            uploadId: uploadID,
            filePath: uploadPath,
            originalName: "\(uploadID).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )

        #expect(result == .success)
        await dispatcher.shutdown()
        _ = await workerTask.result
    }

    @Test("hard timeout after tick phase fails before peer closes stalled response")
    func responseReadTimesOutBeforePeerCloseAfterTickComplete() async throws {
        let env = try TestEnv()
        defer { env.cleanup() }

        let uploadID = "tb-tick-framing-stall-001"
        let uploadPath = "\(env.uploadDir)/\(uploadID)"
        try Data().write(to: URL(fileURLWithPath: uploadPath))

        let pair = try makeSocketPair()
        let fdSequence = FDSequence(values: [pair.clientFD])
        let bridge = loopbackBridgeSource()
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6550,
            connectTimeout: 500,
            thumbsDir: env.thumbsDir,
            previewsDir: env.previewsDir,
            sha256BufferSize: env.config.sha256BufferSize,
            complexityAwareSchedulingEnabled: true,
            bridgeSourcesOverride: [bridge],
            connectOverride: { _, _, _, _ in
                fdSequence.next()
            },
            queryCapabilitiesOverride: { _, _, _, _ in nil }
        )

        let holdOpenNanos: UInt64 = 8_000_000_000
        let workerStart = WorkerStartBarrier()
        let peerCloseBoundary = PeerCloseBoundary()
        let workerTask = Task.detached(priority: .userInitiated) {
            await workerStart.signalStarted()
            defer { Darwin.close(pair.serverFD) }
            guard let request = readDispatchRequest(fd: pair.serverFD),
                  let context = parseTickContext(fromWorkerMime: request.mime) else {
                return
            }

            let running = ProgressTickV2(
                status: .running,
                jobHandle: context.jobHandle,
                sessionID: context.sessionID,
                errorClass: .none,
                progress: 0.25,
                elapsedMS: 100,
                estRemainingMS: 900
            ).encode()
            let complete = ProgressTickV2(
                status: .complete,
                jobHandle: context.jobHandle,
                sessionID: context.sessionID,
                errorClass: .none,
                progress: 1.0,
                elapsedMS: 950,
                estRemainingMS: 0
            ).encode()

            _ = writeAll(fd: pair.serverFD, data: running)
            _ = writeAll(fd: pair.serverFD, data: complete)

            // Hold connection open with no response header bytes long enough to
            // force the coordinator's post-tick read timeout path.
            try? await Task.sleep(nanoseconds: holdOpenNanos)
            await peerCloseBoundary.markReached()
        }
        await workerStart.waitUntilStarted()

        let started = Date()
        let result = await dispatcher.dispatch(
            uploadId: uploadID,
            filePath: uploadPath,
            originalName: "\(uploadID).mov",
            mimeType: "video/quicktime",
            targetWorkerIndex: 0,
            targetSlotIndex: 0
        )
        let elapsed = Date().timeIntervalSince(started)
        let staleTickTimeoutSeconds = Double(ThunderboltDispatcher.slotTickStalenessTimeoutMS) / 1_000
        let reachedPeerCloseBoundary = await peerCloseBoundary.hasReached()

        #expect(result == .transientRetry(slotHealthDown: true))
        #expect(
            elapsed > staleTickTimeoutSeconds * 1.5,
            "Expected post-tick read timeout to outlive the per-tick staleness window"
        )
        #expect(
            reachedPeerCloseBoundary == false,
            "Expected coordinator-side timeout before the worker reached its own close boundary"
        )
        await dispatcher.shutdown()
        _ = await workerTask.result
    }
}

private struct SocketPair {
    let clientFD: Int32
    let serverFD: Int32
}

private struct DispatchRequest {
    let fileSize: Int
    let mime: String
}

private struct TickContext {
    let jobHandle: UInt32
    let sessionID: UInt32
}

private final class FDSequence: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [Int32?]

    init(values: [Int32?]) {
        self.values = values
    }

    func next() -> Int32? {
        lock.lock()
        defer { lock.unlock() }
        guard !values.isEmpty else { return nil }
        return values.removeFirst()
    }
}

private actor WorkerStartBarrier {
    private var started = false
    private var continuation: CheckedContinuation<Void, Never>?

    func signalStarted() {
        started = true
        continuation?.resume()
        continuation = nil
    }

    func waitUntilStarted() async {
        if started { return }
        await withCheckedContinuation { continuation in
            self.continuation = continuation
        }
    }
}

private actor PeerCloseBoundary {
    private var reached = false

    func markReached() {
        reached = true
    }

    func hasReached() -> Bool {
        reached
    }
}

private func loopbackBridgeSource() -> ThunderboltDispatcher.BridgeSource {
    ThunderboltDispatcher.BridgeSource(
        name: "bridge-test",
        ip: "127.0.0.1",
        network: 0x7F00_0000,
        mask: 0xFF00_0000
    )
}

private func makeSocketPair() throws -> SocketPair {
    var descriptors = [Int32](repeating: -1, count: 2)
    guard socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0 else {
        throw POSIXError(.ENOTSOCK)
    }

    var one: Int32 = 1
    for fd in descriptors {
        _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
    }
    var readTimeout = timeval(tv_sec: 5, tv_usec: 0)
    _ = withUnsafePointer(to: &readTimeout) { timeoutPointer in
        setsockopt(
            descriptors[1],
            SOL_SOCKET,
            SO_RCVTIMEO,
            timeoutPointer,
            socklen_t(MemoryLayout<timeval>.size)
        )
    }
    return SocketPair(clientFD: descriptors[0], serverFD: descriptors[1])
}

private func readDispatchRequest(fd: Int32) -> DispatchRequest? {
    // Header prefix: fileSize(8) + sha(64) + nameLen(2)
    guard let prefix = readExactly(fd: fd, count: 74) else { return nil }
    let fileSize = prefix.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: 0, as: UInt64.self).bigEndian)
    }
    guard fileSize >= 0 else { return nil }

    let nameLen = prefix.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: 72, as: UInt16.self).bigEndian)
    }
    guard nameLen >= 0 else { return nil }
    guard let nameAndMimeLen = readExactly(fd: fd, count: nameLen + 2) else { return nil }
    let mimeLen = nameAndMimeLen.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: nameLen, as: UInt16.self).bigEndian)
    }
    guard mimeLen >= 0 else { return nil }
    guard let mimeData = readExactly(fd: fd, count: mimeLen),
          let mime = String(data: mimeData, encoding: .utf8) else {
        return nil
    }
    if fileSize > 0 {
        guard readExactly(fd: fd, count: fileSize) != nil else { return nil }
    }
    return DispatchRequest(fileSize: fileSize, mime: mime)
}

private func parseTickContext(fromWorkerMime mime: String) -> TickContext? {
    let tag = "#kiko-v2:"
    guard let tagRange = mime.range(of: tag) else { return nil }
    let metadata = mime[tagRange.upperBound...]
    var parsedHandle: UInt32?
    var parsedSession: UInt32?

    for pair in metadata.split(separator: ",") {
        let parts = pair.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
        guard parts.count == 2 else { continue }
        let key = parts[0].trimmingCharacters(in: .whitespacesAndNewlines)
        let value = parts[1].trimmingCharacters(in: .whitespacesAndNewlines)
        if key == "h", let handle = UInt32(value) {
            parsedHandle = handle
        } else if key == "s", let session = UInt32(value) {
            parsedSession = session
        }
    }

    guard let parsedHandle, let parsedSession else { return nil }
    return TickContext(jobHandle: parsedHandle, sessionID: parsedSession)
}

private func makeResponseHeader(
    status: UInt8,
    processNanos: UInt64,
    previewPayload: Data,
    thumbPayload: Data
) -> Data {
    let previewSHA = SHA256.hash(data: previewPayload).map { String(format: "%02x", $0) }.joined()
    let thumbSHA = SHA256.hash(data: thumbPayload).map { String(format: "%02x", $0) }.joined()

    var header = Data(capacity: 145)
    header.append(status)

    var processNanosBE = processNanos.bigEndian
    header.append(Data(bytes: &processNanosBE, count: MemoryLayout<UInt64>.size))

    var previewSizeBE = UInt32(previewPayload.count).bigEndian
    header.append(Data(bytes: &previewSizeBE, count: MemoryLayout<UInt32>.size))
    header.append(Data(previewSHA.utf8))

    var thumbSizeBE = UInt32(thumbPayload.count).bigEndian
    header.append(Data(bytes: &thumbSizeBE, count: MemoryLayout<UInt32>.size))
    header.append(Data(thumbSHA.utf8))

    return header
}

private func readExactly(fd: Int32, count: Int) -> Data? {
    guard count >= 0 else { return nil }
    guard count > 0 else { return Data() }
    var data = Data(count: count)

    let ok = data.withUnsafeMutableBytes { raw -> Bool in
        guard let base = raw.baseAddress else { return false }
        var offset = 0
        while offset < count {
            let bytesRead = Darwin.read(fd, base.advanced(by: offset), count - offset)
            if bytesRead > 0 {
                offset += bytesRead
                continue
            }
            if bytesRead == 0 { return false }
            if bytesRead < 0, errno == EINTR { continue }
            return false
        }
        return true
    }
    return ok ? data : nil
}

private func writeAll(fd: Int32, data: Data) -> Bool {
    data.withUnsafeBytes { raw -> Bool in
        guard let base = raw.baseAddress else { return true }
        var sent = 0
        while sent < data.count {
            let bytesWritten = Darwin.write(fd, base.advanced(by: sent), data.count - sent)
            if bytesWritten > 0 {
                sent += bytesWritten
                continue
            }
            if bytesWritten < 0, errno == EINTR { continue }
            return false
        }
        return true
    }
}
