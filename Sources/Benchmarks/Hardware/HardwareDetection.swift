import Foundation
import IOKit
import VideoToolbox

struct HardwareProfile: Sendable {
    let chipName: String
    let performanceCores: Int
    let efficiencyCores: Int
    let totalCores: Int
    let memoryGB: Int
    /// Physical video encode engines on the SoC (1 for M4 base/Pro, 2 for Max, 4 for Ultra).
    /// Detected via IOKit AppleAVE2Driver instance count.
    let videoEncodeEngines: Int
    /// All HW-accelerated codec entries from VTCopyVideoEncoderList (informational).
    let hwEncoderNames: [String]

    var summary: String {
        var parts = [chipName]
        if performanceCores > 0 && efficiencyCores > 0 {
            parts.append("\(totalCores) cores (\(performanceCores)P+\(efficiencyCores)E)")
        } else {
            parts.append("\(totalCores) cores")
        }
        parts.append("\(memoryGB)GB")
        parts.append("\(videoEncodeEngines) encode engine\(videoEncodeEngines == 1 ? "" : "s")")
        return parts.joined(separator: ", ")
    }

    static func detect() -> HardwareProfile {
        let pCores = sysctlInt("hw.perflevel0.physicalcpu") ?? 0
        let eCores = sysctlInt("hw.perflevel1.physicalcpu") ?? 0
        let total = sysctlInt("hw.physicalcpu") ?? ProcessInfo.processInfo.processorCount
        let memBytes = sysctlInt64("hw.memsize") ?? Int64(ProcessInfo.processInfo.physicalMemory)
        let memGB = Int(memBytes / (1024 * 1024 * 1024))
        let chip = sysctlString("machdep.cpu.brand_string") ?? "Unknown"
        let engines = countVideoEncodeEngines()
        let hwNames = listHWEncoderNames()

        return HardwareProfile(
            chipName: chip,
            performanceCores: pCores,
            efficiencyCores: eCores,
            totalCores: total,
            memoryGB: memGB,
            videoEncodeEngines: engines,
            hwEncoderNames: hwNames
        )
    }
}

// MARK: - sysctl helpers

private func sysctlInt(_ name: String) -> Int? {
    var value: Int = 0
    var size = MemoryLayout<Int>.size
    let result = sysctlbyname(name, &value, &size, nil, 0)
    return result == 0 ? value : nil
}

private func sysctlInt64(_ name: String) -> Int64? {
    var value: Int64 = 0
    var size = MemoryLayout<Int64>.size
    let result = sysctlbyname(name, &value, &size, nil, 0)
    return result == 0 ? value : nil
}

private func sysctlString(_ name: String) -> String? {
    var size: Int = 0
    guard sysctlbyname(name, nil, &size, nil, 0) == 0, size > 0 else { return nil }
    var buffer = [CChar](repeating: 0, count: size)
    guard sysctlbyname(name, &buffer, &size, nil, 0) == 0 else { return nil }
    let truncated = buffer.prefix(while: { $0 != 0 }).map { UInt8(bitPattern: $0) }
    return String(decoding: truncated, as: UTF8.self)
}

// MARK: - Video encode engine detection (IOKit)

private func countVideoEncodeEngines() -> Int {
    var iterator: io_iterator_t = 0
    let matching = IOServiceNameMatching("ave2")
    let kr = IOServiceGetMatchingServices(kIOMainPortDefault, matching, &iterator)
    guard kr == KERN_SUCCESS else { return 1 }
    defer { IOObjectRelease(iterator) }

    var count = 0
    while case let service = IOIteratorNext(iterator), service != 0 {
        count += 1
        IOObjectRelease(service)
    }
    return max(count, 1)
}

// MARK: - HW encoder names (VTCopyVideoEncoderList, informational)

private func listHWEncoderNames() -> [String] {
    var list: CFArray?
    let status = VTCopyVideoEncoderList(nil, &list)
    guard status == noErr, let encoders = list as? [[String: Any]] else {
        return []
    }

    var names: [String] = []
    for encoder in encoders {
        let isHW = encoder[kVTVideoEncoderList_IsHardwareAccelerated as String] as? Bool ?? false
        if isHW {
            let name = encoder[kVTVideoEncoderList_DisplayName as String] as? String ?? "Unknown"
            names.append(name)
        }
    }
    return names
}
