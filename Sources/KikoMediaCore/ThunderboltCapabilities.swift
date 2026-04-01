import Foundation
import IOKit

package struct WorkerCaps: Sendable, Decodable {
    package let totalCores: Int?
    package let videoEncodeEngines: Int?
    package let chipName: String?
    package let performanceCores: Int?
    package let efficiencyCores: Int?
    package let videoDecodeEngines: Int?
    package let osVersion: String?
    package let workerSignature: String?
    package let priorCells: [BenchmarkPriorCell]?
    package let msPerFrameC1: Double?
    package let degradationCurve: [Int: Double]?
    package let tickVersion: Int?

    enum CodingKeys: String, CodingKey {
        case totalCores = "total_cores"
        case videoEncodeEngines = "video_encode_engines"
        case chipName = "chip_name"
        case performanceCores = "performance_cores"
        case efficiencyCores = "efficiency_cores"
        case videoDecodeEngines = "video_decode_engines"
        case osVersion = "os_version"
        case workerSignature = "worker_signature"
        case priorCells = "prior_cells"
        case msPerFrameC1 = "ms_per_frame_c1"
        case degradationCurve = "degradation_curve"
        case tickVersion = "tick_version"
    }

    package static func detectLocal() -> WorkerCaps {
        WorkerCaps(
            totalCores: detectSysctlInt("hw.physicalcpu"),
            videoEncodeEngines: detectIOKitCount("ave2"),
            chipName: detectSysctlString("machdep.cpu.brand_string"),
            performanceCores: detectSysctlInt("hw.perflevel0.physicalcpu"),
            efficiencyCores: detectSysctlInt("hw.perflevel1.physicalcpu"),
            videoDecodeEngines: detectIOKitCount("avd"),
            osVersion: WorkerSignatureBuilder.normalizedOS(ProcessInfo.processInfo.operatingSystemVersion),
            workerSignature: nil,
            priorCells: nil,
            msPerFrameC1: nil,
            degradationCurve: nil,
            tickVersion: Int(ProgressTickV2.version)
        )
    }
}

package enum ThunderboltCapabilities {
    package static func sweepCeiling(totalCores: Int, videoEncodeEngines: Int) -> Int {
        let engines = max(1, videoEncodeEngines)
        let cores = max(1, totalCores)
        return min(cores, engines * 2 + 1)
    }
}

// MARK: - Detection helpers

private func detectSysctlInt(_ name: String) -> Int? {
    var value: Int = 0
    var size = MemoryLayout<Int>.size
    guard sysctlbyname(name, &value, &size, nil, 0) == 0, value >= 1 else { return nil }
    return value
}

private func detectSysctlString(_ name: String) -> String? {
    var size: Int = 0
    guard sysctlbyname(name, nil, &size, nil, 0) == 0, size > 0 else { return nil }
    var buffer = [CChar](repeating: 0, count: size)
    guard sysctlbyname(name, &buffer, &size, nil, 0) == 0 else { return nil }
    let result = String(decoding: buffer.prefix(while: { $0 != 0 }).map { UInt8(bitPattern: $0) }, as: UTF8.self)
    return result.isEmpty ? nil : result
}

private func detectIOKitCount(_ deviceName: String) -> Int? {
    var iterator: io_iterator_t = 0
    let matching = IOServiceNameMatching(deviceName)
    let kr = IOServiceGetMatchingServices(kIOMainPortDefault, matching, &iterator)
    guard kr == KERN_SUCCESS else { return nil }
    defer { IOObjectRelease(iterator) }

    var count = 0
    while case let service = IOIteratorNext(iterator), service != 0 {
        count += 1
        IOObjectRelease(service)
    }
    guard count >= 1 else { return nil }
    return count
}
