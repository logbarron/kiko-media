import Testing
import Foundation
import CryptoKit
import Darwin
@testable import KikoMediaCore

// Memory-sensitive assertions are noisy under concurrent suite activity.
@Suite("SHA256 Utility", Testing.ParallelizationTrait.serialized)
struct SHA256UtilityTests {
    @Test("Streaming hash matches CryptoKit digest across buffer sizes")
    func digestMatchesAcrossBufferSizes() throws {
        let fixture = try SHA256Fixture(byteCount: 6 * 1024 * 1024 + 137)
        defer { fixture.cleanup() }

        let expected = SHA256.hash(data: fixture.data).map { String(format: "%02x", $0) }.joined()
        let sizes = [4 * 1024, 64 * 1024, 512 * 1024, 1_048_576, 2 * 1_048_576]

        for size in sizes {
            let digest = try SHA256Utility.calculateSHA256(path: fixture.path, bufferSize: size)
            #expect(digest == expected, "Digest mismatch for buffer size \(size)")
        }
    }

    @Test("Repeated hashing stays memory bounded")
    func repeatedHashingMemoryBounded() throws {
        let fixture = try SHA256Fixture(byteCount: 8 * 1024 * 1024 + 7)
        defer { fixture.cleanup() }

        let warmupIterations = 12
        for _ in 0..<warmupIterations {
            _ = try SHA256Utility.calculateSHA256(path: fixture.path, bufferSize: 64 * 1024)
        }

        let baseline = physFootprintBytes()
        var peak = baseline

        let iterations = 160
        for index in 0..<iterations {
            _ = try SHA256Utility.calculateSHA256(path: fixture.path, bufferSize: 64 * 1024)
            if index.isMultiple(of: 8) {
                peak = max(peak, physFootprintBytes())
            }
        }

        let growthBytes = peak > baseline ? (peak - baseline) : 0
        let growthMB = Double(growthBytes) / (1024 * 1024)
        #expect(
            growthBytes < 160 * 1024 * 1024,
            "Hashing growth should stay bounded; observed +\(String(format: "%.1f", growthMB)) MB"
        )
    }

}

private struct SHA256Fixture {
    let tempDir: URL
    let path: String
    let data: Data

    init(byteCount: Int) throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-sha-fixture-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        path = tempDir.appendingPathComponent("payload.bin").path

        var bytes = Data(count: byteCount)
        bytes.withUnsafeMutableBytes { raw in
            guard let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return }
            for index in 0..<byteCount {
                base[index] = UInt8(truncatingIfNeeded: (index &* 31) &+ 17)
            }
        }
        data = bytes
        try data.write(to: URL(fileURLWithPath: path), options: .atomic)
    }

    func cleanup() {
        try? FileManager.default.removeItem(at: tempDir)
    }
}

private func physFootprintBytes() -> UInt64 {
    var info = task_vm_info_data_t()
    var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
    let result = withUnsafeMutablePointer(to: &info) { pointer in
        pointer.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { rebound in
            task_info(
                mach_task_self_,
                task_flavor_t(TASK_VM_INFO),
                rebound,
                &count
            )
        }
    }

    guard result == KERN_SUCCESS else { return 0 }
    return UInt64(info.phys_footprint)
}
