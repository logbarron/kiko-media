import Foundation
import CryptoKit
import Darwin

package enum SHA256Utility {
    private static let hexDigits = Array("0123456789abcdef".utf8)

    package static func calculateSHA256(path: String, bufferSize: Int = 1_048_576) throws -> String {
        let chunkSize = max(4_096, bufferSize)
        let fd = open(path, O_RDONLY)
        guard fd >= 0 else {
            throw POSIXError(.init(rawValue: errno) ?? .EIO)
        }
        defer { Darwin.close(fd) }

        let buffer = UnsafeMutableRawPointer.allocate(byteCount: chunkSize, alignment: 64)
        defer { buffer.deallocate() }

        var hasher = SHA256()

        while true {
            let bytesRead = Darwin.read(fd, buffer, chunkSize)
            if bytesRead > 0 {
                let raw = UnsafeRawBufferPointer(start: buffer, count: bytesRead)
                hasher.update(bufferPointer: raw)
                continue
            }
            if bytesRead == 0 {
                break
            }
            if errno == EINTR {
                continue
            }
            throw POSIXError(.init(rawValue: errno) ?? .EIO)
        }

        let digest = hasher.finalize()
        var encoded = [UInt8]()
        encoded.reserveCapacity(64)
        for byte in digest {
            encoded.append(Self.hexDigits[Int(byte >> 4)])
            encoded.append(Self.hexDigits[Int(byte & 0x0F)])
        }
        return String(decoding: encoded, as: UTF8.self)
    }
}
