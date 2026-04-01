import Foundation
import Darwin

// MARK: - Timing

func measure(_ block: () throws -> Void) rethrows -> Duration {
    let clock = ContinuousClock()
    let start = clock.now
    try block()
    return clock.now - start
}

func measureAsync(_ block: () async throws -> Void) async rethrows -> Duration {
    let clock = ContinuousClock()
    let start = clock.now
    try await block()
    return clock.now - start
}

// MARK: - Statistics

struct Stats {
    let count: Int
    let p50: Double
    let p95: Double
    let min: Double
    let max: Double
    let mean: Double

    init(_ values: [Double]) {
        let sorted = values.sorted()
        count = sorted.count
        min = sorted.first ?? 0
        max = sorted.last ?? 0
        mean = sorted.isEmpty ? 0 : sorted.reduce(0, +) / Double(sorted.count)
        p50 = percentile(sorted, 0.50)
        p95 = percentile(sorted, 0.95)
    }

    var summary: String {
        "p50: \(fmt(p50))  p95: \(fmt(p95))  min: \(fmt(min))  max: \(fmt(max))  n=\(count)"
    }
}

private func percentile(_ sorted: [Double], _ p: Double) -> Double {
    guard !sorted.isEmpty else { return 0 }
    let index = p * Double(sorted.count - 1)
    let lower = Int(index)
    let upper = Swift.min(lower + 1, sorted.count - 1)
    let fraction = index - Double(lower)
    return sorted[lower] + fraction * (sorted[upper] - sorted[lower])
}

func fmt(_ seconds: Double) -> String {
    if seconds < 0.001 { return String(format: "%.2fus", seconds * 1_000_000) }
    if seconds < 1.0 { return String(format: "%.1fms", seconds * 1000) }
    return String(format: "%.2fs", seconds)
}

func fmtDuration(_ d: Duration) -> String {
    fmt(d.seconds)
}

extension Duration {
    var seconds: Double {
        let (s, a) = components
        return Double(s) + Double(a) / 1_000_000_000_000_000_000
    }
}

// MARK: - Resource Monitoring

func getMemoryMB() -> Int {
    var info = task_vm_info_data_t()
    var count = mach_msg_type_number_t(
        MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size
    )
    let result = withUnsafeMutablePointer(to: &info) {
        $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
            task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count)
        }
    }
    guard result == KERN_SUCCESS else { return 0 }
    return Int(info.phys_footprint) / (1024 * 1024)
}

func getCPUPercent() -> Double {
    var threadList: thread_act_array_t?
    var threadCount: mach_msg_type_number_t = 0
    let result = task_threads(mach_task_self_, &threadList, &threadCount)
    guard result == KERN_SUCCESS, let threads = threadList else { return 0 }
    defer {
        let size = vm_size_t(MemoryLayout<thread_t>.size) * vm_size_t(threadCount)
        vm_deallocate(mach_task_self_, vm_address_t(bitPattern: threads), size)
    }
    var total: Double = 0
    for i in 0..<Int(threadCount) {
        var info = thread_basic_info()
        var infoCount = mach_msg_type_number_t(
            MemoryLayout<thread_basic_info_data_t>.size / MemoryLayout<integer_t>.size
        )
        let kr = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: Int(infoCount)) {
                thread_info(threads[i], thread_flavor_t(THREAD_BASIC_INFO), $0, &infoCount)
            }
        }
        guard kr == KERN_SUCCESS else { continue }
        if info.flags & TH_FLAGS_IDLE != 0 { continue }
        total += Double(info.cpu_usage) / Double(TH_USAGE_SCALE) * 100.0
    }
    return total
}

func getThermalState() -> String {
    switch ProcessInfo.processInfo.thermalState {
    case .nominal: return "nominal"
    case .fair: return "fair"
    case .serious: return "SERIOUS"
    case .critical: return "CRITICAL"
    @unknown default: return "unknown"
    }
}

// MARK: - Rendering Row Helpers

func printRow(_ label: String, _ detail: String, _ stats: Stats) {
    BenchmarkRuntimeRenderer.printStatsRow(label, detail, stats)
}

func printRow(_ label: String, _ value: String) {
    BenchmarkRuntimeRenderer.printValueRow(label, value)
}

extension String {
    static func * (lhs: String, rhs: Int) -> String {
        String(repeating: lhs, count: rhs)
    }
}
