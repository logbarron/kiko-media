import Foundation
import Darwin
import CryptoKit

package enum ThunderboltTransport {
    private static let capabilitiesReadTimeoutMS: Int32 = 250
    private static let sendfileWritablePollTimeoutMS: Int32 = 25

    package static func connect(host: String, port: Int, timeoutMS: Int, sourceIP: String?) -> Int32? {
        var hints = addrinfo(
            ai_flags: AI_ADDRCONFIG,
            ai_family: AF_INET,
            ai_socktype: SOCK_STREAM,
            ai_protocol: IPPROTO_TCP,
            ai_addrlen: 0,
            ai_canonname: nil,
            ai_addr: nil,
            ai_next: nil
        )
        var result: UnsafeMutablePointer<addrinfo>?
        let resolveRC = getaddrinfo(host, String(port), &hints, &result)
        guard resolveRC == 0, let start = result else { return nil }
        defer { freeaddrinfo(start) }

        var sourceAddress: sockaddr_in?
        if let sourceIP, !sourceIP.isEmpty {
            var parsed = sockaddr_in()
            parsed.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
            parsed.sin_family = sa_family_t(AF_INET)
            parsed.sin_port = in_port_t(0).bigEndian
            let sourceRC = sourceIP.withCString { cstr in
                inet_pton(AF_INET, cstr, &parsed.sin_addr)
            }
            guard sourceRC == 1 else { return nil }
            sourceAddress = parsed
        }

        let timeoutMillis = Int32(max(timeoutMS, 1))
        var current: UnsafeMutablePointer<addrinfo>? = start
        while let ai = current {
            defer { current = ai.pointee.ai_next }

            let fd = socket(ai.pointee.ai_family, ai.pointee.ai_socktype, ai.pointee.ai_protocol)
            guard fd >= 0 else { continue }

            var one: Int32 = 1
            _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))

            if var sourceAddress {
                let bindRC = withUnsafePointer(to: &sourceAddress) { pointer in
                    pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
                        Darwin.bind(fd, socketAddress, socklen_t(MemoryLayout<sockaddr_in>.size))
                    }
                }
                guard bindRC == 0 else {
                    Darwin.close(fd)
                    continue
                }
            }

            let originalFlags = fcntl(fd, F_GETFL, 0)
            guard originalFlags >= 0 else {
                Darwin.close(fd)
                continue
            }
            guard fcntl(fd, F_SETFL, originalFlags | O_NONBLOCK) == 0 else {
                Darwin.close(fd)
                continue
            }

            let connectRC = Darwin.connect(fd, ai.pointee.ai_addr, ai.pointee.ai_addrlen)
            if connectRC == 0 {
                _ = fcntl(fd, F_SETFL, originalFlags)
                return fd
            }

            if errno == EINPROGRESS {
                var pollDescriptor = pollfd(fd: fd, events: Int16(POLLOUT), revents: 0)
                var pollRC: Int32 = -1
                repeat {
                    pollRC = poll(&pollDescriptor, 1, timeoutMillis)
                } while pollRC < 0 && errno == EINTR

                if pollRC == 1 {
                    var socketError: Int32 = 0
                    var socketErrorLength = socklen_t(MemoryLayout<Int32>.size)
                    _ = getsockopt(fd, SOL_SOCKET, SO_ERROR, &socketError, &socketErrorLength)
                    if socketError == 0 {
                        _ = fcntl(fd, F_SETFL, originalFlags)
                        return fd
                    }
                }
            }

            Darwin.close(fd)
        }

        return nil
    }

    package static func sendHeader(fd: Int32, fileSize: Int, sha256Hex: String, name: String, mime: String) -> Bool {
        guard fileSize >= 0 else { return false }

        let shaBytes = Array(sha256Hex.utf8)
        guard shaBytes.count == 64, shaBytes.allSatisfy(Self.isHexByte) else { return false }

        let nameBytes = Data(name.utf8)
        let mimeBytes = Data(mime.utf8)
        guard nameBytes.count <= Int(UInt16.max), mimeBytes.count <= Int(UInt16.max) else { return false }

        var header = Data(capacity: 8 + 64 + 2 + nameBytes.count + 2 + mimeBytes.count)
        var fileSizeBE = UInt64(fileSize).bigEndian
        header.append(Data(bytes: &fileSizeBE, count: 8))
        header.append(contentsOf: shaBytes)

        var nameLenBE = UInt16(nameBytes.count).bigEndian
        header.append(Data(bytes: &nameLenBE, count: 2))
        header.append(nameBytes)

        var mimeLenBE = UInt16(mimeBytes.count).bigEndian
        header.append(Data(bytes: &mimeLenBE, count: 2))
        header.append(mimeBytes)

        return writeAll(fd: fd, data: header)
    }

    package static func sendFileData(fd: Int32, filePath: String, fileSize: Int) -> Bool {
        guard fileSize >= 0 else { return false }

        let fileFD = Darwin.open(filePath, O_RDONLY)
        guard fileFD >= 0 else { return false }
        defer { Darwin.close(fileFD) }

        var offset: off_t = 0
        var remaining: off_t = off_t(fileSize)
        while remaining > 0 {
            var chunkLength: off_t = remaining
            let sendRC = sendfile(fileFD, fd, offset, &chunkLength, nil, 0)
            offset += chunkLength
            remaining -= chunkLength

            if sendRC == 0, remaining == 0 { break }
            if sendRC == 0 { continue }
            if errno == EAGAIN {
                if chunkLength == 0 {
                    guard waitUntilWritable(fd: fd, timeoutMS: sendfileWritablePollTimeoutMS) else {
                        return false
                    }
                }
                continue
            }
            if errno == EINTR {
                continue
            }
            return false
        }

        return remaining == 0
    }

    package static func readResponseHeader(fd: Int32, timeoutMS: Int32? = nil) -> (
        status: UInt8,
        processNanos: UInt64,
        prevSize: Int,
        prevSHA256: String,
        thumbSize: Int,
        thumbSHA256: String
    )? {
        guard let header = readExactly(fd: fd, count: 145, readTimeoutMS: timeoutMS) else { return nil }
        return parseResponseHeader(header)
    }

    package static func parseResponseHeader(_ header: Data) -> (
        status: UInt8,
        processNanos: UInt64,
        prevSize: Int,
        prevSHA256: String,
        thumbSize: Int,
        thumbSHA256: String
    )? {
        guard header.count == 145 else { return nil }

        let status = header[0]
        let processNanos = header.withUnsafeBytes { raw in
            raw.loadUnaligned(fromByteOffset: 1, as: UInt64.self).bigEndian
        }
        let prevSize = header.withUnsafeBytes { raw in
            Int(raw.loadUnaligned(fromByteOffset: 9, as: UInt32.self).bigEndian)
        }
        let thumbSize = header.withUnsafeBytes { raw in
            Int(raw.loadUnaligned(fromByteOffset: 77, as: UInt32.self).bigEndian)
        }

        guard let prevSHA256 = String(data: header.subdata(in: 13..<77), encoding: .utf8),
              let thumbSHA256 = String(data: header.subdata(in: 81..<145), encoding: .utf8),
              prevSHA256.utf8.count == 64,
              thumbSHA256.utf8.count == 64
        else {
            return nil
        }

        return (status, processNanos, prevSize, prevSHA256, thumbSize, thumbSHA256)
    }

    package static func readBytes(fd: Int32, count: Int, timeoutMS: Int32? = nil) -> Data? {
        readExactly(fd: fd, count: count, readTimeoutMS: timeoutMS)
    }

    package static func readTickFrameV2(fd: Int32, timeoutMS: Int32? = nil) -> Data? {
        readExactly(fd: fd, count: ProgressTickV2.encodedByteCount, readTimeoutMS: timeoutMS)
    }

    package static func setReadTimeout(fd: Int32, timeoutMS: Int32) {
        let sanitized = max(Int32(1), timeoutMS)
        let tvSec = __darwin_time_t(sanitized / 1_000)
        let tvUSec = __darwin_suseconds_t((sanitized % 1_000) * 1_000)
        var tv = timeval(tv_sec: tvSec, tv_usec: tvUSec)
        _ = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
    }

    package static func readToFile(fd: Int32, count: Int, path: String) -> Bool {
        guard count >= 0 else { return false }

        let fileURL = URL(fileURLWithPath: path)
        let fm = FileManager.default
        do {
            try fm.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            if fm.fileExists(atPath: path) {
                try fm.removeItem(atPath: path)
            }
            guard fm.createFile(atPath: path, contents: nil) else { return false }

            let handle = try FileHandle(forWritingTo: fileURL)
            defer { try? handle.close() }

            var remaining = count
            var buffer = [UInt8](repeating: 0, count: 1_048_576)
            while remaining > 0 {
                let wanted = min(remaining, buffer.count)
                let bytesRead = buffer.withUnsafeMutableBytes { raw -> Int in
                    guard let base = raw.baseAddress else { return -1 }
                    return Darwin.read(fd, base, wanted)
                }
                if bytesRead > 0 {
                    try handle.write(contentsOf: Data(buffer.prefix(bytesRead)))
                    remaining -= bytesRead
                    continue
                }
                if bytesRead == 0 { return false }
                if errno == EINTR { continue }
                return false
            }
        } catch {
            return false
        }

        return true
    }

    package static func closeConnection(fd: Int32) {
        Darwin.close(fd)
    }

    package static func queryCapabilities(
        host: String,
        port: Int,
        timeoutMS: Int,
        sourceIP: String?
    ) -> WorkerCaps? {
        guard let fd = connect(host: host, port: port, timeoutMS: timeoutMS, sourceIP: sourceIP) else {
            return nil
        }
        defer { closeConnection(fd: fd) }

        let capsTimeoutMS = max(Int32(1), capabilitiesReadTimeoutMS)
        let tvSec = __darwin_time_t(capsTimeoutMS / 1_000)
        let tvUSec = __darwin_suseconds_t((capsTimeoutMS % 1_000) * 1_000)
        var tv = timeval(tv_sec: tvSec, tv_usec: tvUSec)
        _ = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))

        let zeroSHA = String(repeating: "0", count: 64)
        guard sendHeader(
            fd: fd,
            fileSize: 0,
            sha256Hex: zeroSHA,
            name: "__kiko_caps__",
            mime: "application/x-kiko-caps+json"
        ) else {
            return nil
        }

        guard let response = readResponseHeader(fd: fd) else { return nil }
        guard response.status == 0x03 else { return nil }
        guard (1...32_768).contains(response.prevSize) else { return nil }
        guard response.thumbSize == 0 else { return nil }
        guard response.thumbSHA256 == zeroSHA else { return nil }

        guard let payloadData = readExactly(fd: fd, count: response.prevSize) else { return nil }

        let payloadSHA = SHA256.hash(data: payloadData)
            .map { String(format: "%02x", $0) }.joined()
        guard payloadSHA == response.prevSHA256.lowercased() else { return nil }

        return try? JSONDecoder().decode(WorkerCaps.self, from: payloadData)
    }

    private static func writeAll(fd: Int32, data: Data) -> Bool {
        guard !data.isEmpty else { return true }

        return data.withUnsafeBytes { raw in
            guard let base = raw.baseAddress else { return false }
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

    private static func readExactly(fd: Int32, count: Int, readTimeoutMS: Int32? = nil) -> Data? {
        guard count >= 0 else { return nil }
        guard count > 0 else { return Data() }

        var data = Data(count: count)
        let ok = data.withUnsafeMutableBytes { raw in
            guard let base = raw.baseAddress else { return false }
            var offset = 0
            let readDeadlineNanos: UInt64? = readTimeoutMS.map { timeoutMS in
                let sanitized = max(Int32(0), timeoutMS)
                return DispatchTime.now().uptimeNanoseconds + UInt64(sanitized) * 1_000_000
            }
            while offset < count {
                if let readTimeoutMS {
                    let timeoutForPollMS: Int32
                    if let readDeadlineNanos {
                        let nowNanos = DispatchTime.now().uptimeNanoseconds
                        guard nowNanos < readDeadlineNanos else {
                            return false
                        }
                        let remainingNanos = readDeadlineNanos - nowNanos
                        let remainingMillis = Int32(min(remainingNanos / 1_000_000, UInt64(Int32.max)))
                        timeoutForPollMS = max(1, remainingMillis)
                    } else {
                        timeoutForPollMS = readTimeoutMS
                    }

                    guard waitUntilReadable(fd: fd, timeoutMS: timeoutForPollMS) else {
                        return false
                    }
                }
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

    private static func waitUntilReadable(fd: Int32, timeoutMS: Int32) -> Bool {
        waitUntil(
            fd: fd,
            timeoutMS: timeoutMS,
            events: Int16(POLLIN),
            readyMask: Int16(POLLIN | POLLHUP | POLLERR)
        )
    }

    private static func waitUntilWritable(fd: Int32, timeoutMS: Int32) -> Bool {
        waitUntil(
            fd: fd,
            timeoutMS: timeoutMS,
            events: Int16(POLLOUT),
            readyMask: Int16(POLLOUT | POLLHUP | POLLERR)
        )
    }

    private static func waitUntil(
        fd: Int32,
        timeoutMS: Int32,
        events: Int16,
        readyMask: Int16
    ) -> Bool {
        guard timeoutMS >= 0 else { return false }
        var pollDescriptor = pollfd(fd: fd, events: events, revents: 0)
        let deadlineNanos = DispatchTime.now().uptimeNanoseconds + UInt64(timeoutMS) * 1_000_000
        var remainingTimeoutMS = timeoutMS

        while true {
            let result = Darwin.poll(&pollDescriptor, 1, remainingTimeoutMS)
            if result > 0 {
                return (pollDescriptor.revents & readyMask) != 0
            }
            if result == 0 {
                return false
            }
            if errno == EINTR {
                let nowNanos = DispatchTime.now().uptimeNanoseconds
                guard nowNanos < deadlineNanos else {
                    return false
                }
                let remainingNanos = deadlineNanos - nowNanos
                let remainingMillis = Int32(min(remainingNanos / 1_000_000, UInt64(Int32.max)))
                remainingTimeoutMS = max(1, remainingMillis)
                continue
            }
            return false
        }
    }

    private static func isHexByte(_ byte: UInt8) -> Bool {
        (byte >= 48 && byte <= 57) || (byte >= 65 && byte <= 70) || (byte >= 97 && byte <= 102)
    }
}
