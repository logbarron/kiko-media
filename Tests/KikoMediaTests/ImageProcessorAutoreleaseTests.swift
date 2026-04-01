import Testing
import Foundation
import Darwin
@testable import KikoMediaCore

// Memory-sensitive assertions are noisy under concurrent suite activity.
@Suite("Image Processor Autorelease Hardening", Testing.ParallelizationTrait.serialized)
struct ImageProcessorAutoreleaseTests {

    @Test("Metadata probes stay behaviorally stable across repeated calls")
    func metadataProbeBehaviorRemainsStable() throws {
        let fixture = try MetadataProbeFixture()
        defer { fixture.cleanup() }

        for _ in 0..<200 {
            #expect(ImageProcessor.extractTimestamp(sourcePath: fixture.imagePath) == fixture.expectedTimestamp)
            #expect(ImageProcessor.isImage(path: fixture.imagePath))

            #expect(ImageProcessor.extractTimestamp(sourcePath: fixture.textPath) == nil)
            #expect(!ImageProcessor.isImage(path: fixture.textPath))
        }
    }

    @Test("Metadata probes avoid unbounded memory growth under repeated calls")
    func metadataProbeMemoryBounded() throws {
        let fixture = try MetadataProbeFixture()
        defer { fixture.cleanup() }

        let warmup = 20
        for _ in 0..<warmup {
            _ = ImageProcessor.extractTimestamp(sourcePath: fixture.imagePath)
            _ = ImageProcessor.isImage(path: fixture.imagePath)
        }

        let baseline = physFootprintBytes()
        var peak = baseline

        for iteration in 0..<600 {
            _ = ImageProcessor.extractTimestamp(sourcePath: fixture.imagePath)
            _ = ImageProcessor.isImage(path: fixture.imagePath)
            _ = ImageProcessor.extractTimestamp(sourcePath: fixture.textPath)
            _ = ImageProcessor.isImage(path: fixture.textPath)

            if iteration.isMultiple(of: 20) {
                peak = max(peak, physFootprintBytes())
            }
        }

        let growthBytes = peak > baseline ? (peak - baseline) : 0
        let growthMB = Double(growthBytes) / (1024.0 * 1024.0)
        #expect(
            growthBytes < 192 * 1024 * 1024,
            "Memory growth should stay bounded for metadata probes; observed +\(String(format: "%.1f", growthMB)) MB"
        )
    }
}

private struct MetadataProbeFixture {
    let tempDir: URL
    let imagePath: String
    let textPath: String
    let expectedTimestamp = "2025:01:15 14:30:00"

    init() throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-metadata-probe-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)

        imagePath = tempDir.appendingPathComponent("source.jpg").path
        textPath = tempDir.appendingPathComponent("not-image.txt").path

        try TestImage.writeJPEGWithEXIF(to: imagePath, width: 2600, height: 1800)
        try Data("definitely not image data".utf8).write(to: URL(fileURLWithPath: textPath))
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
