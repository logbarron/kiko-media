import Foundation
import CryptoKit
import Darwin
import Testing
@testable import KikoMediaCore

@Suite("Thunderbolt transport")
struct ThunderboltTransportTests {
    @Test("sendHeader rejects malformed sha256 input before writing")
    func sendHeaderRejectsMalformedSHA256Input() {
        var pipeFDs: [Int32] = [0, 0]
        guard Darwin.pipe(&pipeFDs) == 0 else {
            Issue.record("Failed to create pipe")
            return
        }

        let readFD = pipeFDs[0]
        var writeFD = pipeFDs[1]
        defer {
            if readFD >= 0 {
                Darwin.close(readFD)
            }
            if writeFD >= 0 {
                Darwin.close(writeFD)
            }
        }

        #expect(
            !ThunderboltTransport.sendHeader(
                fd: writeFD,
                fileSize: 1,
                sha256Hex: "not-hex",
                name: "a.jpg",
                mime: "image/jpeg"
            )
        )

        Darwin.close(writeFD)
        writeFD = -1

        var byte: UInt8 = 0
        #expect(Darwin.read(readFD, &byte, 1) == 0)
    }

    @Test("parseResponseHeader rejects truncated payload")
    func parseResponseHeaderRejectsTruncatedPayload() {
        #expect(ThunderboltTransport.parseResponseHeader(Data(count: 144)) == nil)
    }

    @Test("queryCapabilities accepts payload size at 32768-byte upper bound")
    func queryCapabilitiesAcceptsUpperBoundPayload() async throws {
        let port = try reserveLoopbackPort()
        let payload = makeCapabilitiesJSONPayload(totalBytes: 32_768)
        #expect(payload.count == 32_768)

        let response = makeCapabilitiesResponse(prevSize: payload.count, payload: payload)
        let server = try OneShotCapabilitiesServer(port: port, response: response)
        try server.start()
        defer { server.stop() }

        try? await Task.sleep(for: .milliseconds(30))
        let caps = ThunderboltTransport.queryCapabilities(
            host: "127.0.0.1",
            port: port,
            timeoutMS: 500,
            sourceIP: nil
        )

        #expect(caps != nil)
        #expect(caps?.tickVersion == 2)
    }

    @Test("queryCapabilities rejects payload size above 32768-byte upper bound")
    func queryCapabilitiesRejectsPayloadAboveUpperBound() async throws {
        let port = try reserveLoopbackPort()
        let response = makeCapabilitiesResponse(prevSize: 32_769, payload: Data())
        let server = try OneShotCapabilitiesServer(port: port, response: response)
        try server.start()
        defer { server.stop() }

        try? await Task.sleep(for: .milliseconds(30))
        let caps = ThunderboltTransport.queryCapabilities(
            host: "127.0.0.1",
            port: port,
            timeoutMS: 500,
            sourceIP: nil
        )

        #expect(caps == nil)
    }
}

private func makeCapabilitiesJSONPayload(totalBytes: Int) -> Data {
    let prefix = #"{"tick_version":2,"pad":""#
    let suffix = #""}"#
    let padCount = totalBytes - prefix.utf8.count - suffix.utf8.count
    precondition(padCount >= 0, "Requested payload size too small")
    return Data((prefix + String(repeating: "a", count: padCount) + suffix).utf8)
}

private func makeCapabilitiesResponse(prevSize: Int, payload: Data) -> Data {
    let zeroSHA = String(repeating: "0", count: 64)
    let payloadSHA = SHA256.hash(data: payload).map { String(format: "%02x", $0) }.joined()

    var response = Data(capacity: 145 + payload.count)
    response.append(0x03) // capabilities status

    var processNanosBE = UInt64(0).bigEndian
    response.append(Data(bytes: &processNanosBE, count: MemoryLayout<UInt64>.size))

    var prevSizeBE = UInt32(prevSize).bigEndian
    response.append(Data(bytes: &prevSizeBE, count: MemoryLayout<UInt32>.size))

    response.append(Data(payloadSHA.utf8))

    var thumbSizeBE = UInt32(0).bigEndian
    response.append(Data(bytes: &thumbSizeBE, count: MemoryLayout<UInt32>.size))

    response.append(Data(zeroSHA.utf8))

    response.append(payload)
    return response
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

private final class OneShotCapabilitiesServer {
    private let port: Int
    private let response: Data
    private var listenFD: Int32 = -1

    init(port: Int, response: Data) throws {
        self.port = port
        self.response = response
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

        guard listen(fd, 1) == 0 else {
            Darwin.close(fd)
            throw POSIXError(.ECONNREFUSED)
        }

        listenFD = fd
        let response = self.response
        DispatchQueue.global(qos: .userInitiated).async {
            var clientAddress = sockaddr_in()
            var clientLength = socklen_t(MemoryLayout<sockaddr_in>.size)
            let clientFD = withUnsafeMutablePointer(to: &clientAddress) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
                    Darwin.accept(fd, socketAddress, &clientLength)
                }
            }
            guard clientFD >= 0 else { return }
            defer { Darwin.close(clientFD) }

            // Synchronize with client write path: consume the probe request header first.
            guard drainCapabilitiesProbeRequest(fd: clientFD) else { return }
            _ = writeAll(fd: clientFD, data: response)
        }
    }

    func stop() {
        if listenFD >= 0 {
            pokeLoopbackPort(port: port)
            _ = Darwin.shutdown(listenFD, SHUT_RDWR)
            Darwin.close(listenFD)
            listenFD = -1
        }
    }
}

private func pokeLoopbackPort(port: Int) {
    let fd = socket(AF_INET, SOCK_STREAM, 0)
    guard fd >= 0 else { return }
    defer { Darwin.close(fd) }

    var one: Int32 = 1
    _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))

    var address = sockaddr_in()
    address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = in_port_t(UInt16(port)).bigEndian
    address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

    _ = withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
            Darwin.connect(fd, socketAddress, socklen_t(MemoryLayout<sockaddr_in>.size))
        }
    }
}

private func drainCapabilitiesProbeRequest(fd: Int32) -> Bool {
    guard let prefix = readExactly(fd: fd, count: 8 + 64 + 2) else { return false }
    let nameLength = prefix.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: 8 + 64, as: UInt16.self).bigEndian)
    }
    guard nameLength >= 0 else { return false }

    guard let nameAndMimeLength = readExactly(fd: fd, count: nameLength + 2) else { return false }
    let mimeLength = nameAndMimeLength.withUnsafeBytes { raw in
        Int(raw.loadUnaligned(fromByteOffset: nameLength, as: UInt16.self).bigEndian)
    }
    guard mimeLength >= 0 else { return false }

    return readExactly(fd: fd, count: mimeLength) != nil
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
            let written = Darwin.write(fd, base.advanced(by: sent), data.count - sent)
            if written > 0 {
                sent += written
                continue
            }
            if written < 0, errno == EINTR {
                continue
            }
            return false
        }
        return true
    }
}
