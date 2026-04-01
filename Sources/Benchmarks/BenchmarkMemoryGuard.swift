import Foundation

struct BenchmarkMemoryGuardConfiguration: Sendable {
    let limitMB: Int
    let warningMB: Int
    let physicalMemoryMB: Int
    let source: String
}

private final class BenchmarkMemoryGuardImpl: @unchecked Sendable {
    private struct State: Sendable {
        let config: BenchmarkMemoryGuardConfiguration
        var warned = false
        var peakMB = 0
    }

    private let lock = NSLock()
    private var state: State?
    private var didBootstrap = false
    private var disabledReason = "not configured"

    func bootstrap() -> BenchmarkMemoryGuardConfiguration? {
        lock.lock()
        if didBootstrap {
            let config = state?.config
            lock.unlock()
            return config
        }
        didBootstrap = true

        let processInfo = ProcessInfo.processInfo
        let physicalMemoryMB = max(1, Int(processInfo.physicalMemory / (1024 * 1024)))
        let fallbackLimitMB = Self.defaultLimitMB(physicalMemoryMB: physicalMemoryMB)
        let envRaw = processInfo.environment["BENCH_MEMORY_LIMIT_MB"]?.trimmingCharacters(in: .whitespacesAndNewlines)

        let config: BenchmarkMemoryGuardConfiguration?
        if let envRaw, !envRaw.isEmpty {
            if let parsed = Int(envRaw) {
                if parsed == 0 {
                    disabledReason = "BENCH_MEMORY_LIMIT_MB=0"
                    config = nil
                } else if parsed > 0 {
                    let warningMB = max(1, Int(Double(parsed) * 0.85))
                    config = BenchmarkMemoryGuardConfiguration(
                        limitMB: parsed,
                        warningMB: warningMB,
                        physicalMemoryMB: physicalMemoryMB,
                        source: "BENCH_MEMORY_LIMIT_MB"
                    )
                } else {
                    let warningMB = max(1, Int(Double(fallbackLimitMB) * 0.85))
                    config = BenchmarkMemoryGuardConfiguration(
                        limitMB: fallbackLimitMB,
                        warningMB: warningMB,
                        physicalMemoryMB: physicalMemoryMB,
                        source: "default (invalid BENCH_MEMORY_LIMIT_MB=\(envRaw))"
                    )
                }
            } else {
                let warningMB = max(1, Int(Double(fallbackLimitMB) * 0.85))
                config = BenchmarkMemoryGuardConfiguration(
                    limitMB: fallbackLimitMB,
                    warningMB: warningMB,
                    physicalMemoryMB: physicalMemoryMB,
                    source: "default (invalid BENCH_MEMORY_LIMIT_MB=\(envRaw))"
                )
            }
        } else {
            let warningMB = max(1, Int(Double(fallbackLimitMB) * 0.85))
            config = BenchmarkMemoryGuardConfiguration(
                limitMB: fallbackLimitMB,
                warningMB: warningMB,
                physicalMemoryMB: physicalMemoryMB,
                source: "default (80% of physical memory)"
            )
        }

        if let config {
            state = State(config: config, warned: false, peakMB: getMemoryMB())
        } else {
            state = nil
        }
        lock.unlock()

        return config
    }

    func statusSummary() -> String {
        lock.lock()
        defer { lock.unlock() }

        if let config = state?.config {
            return "enabled (\(config.limitMB)MB hard cap, \(config.warningMB)MB warning)"
        }
        return "disabled (\(disabledReason))"
    }

    func peakMB() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return state?.peakMB ?? getMemoryMB()
    }

    func checkpoint(stage: String, detail: String?) throws {
        let memoryMB = getMemoryMB()

        var warningContext: (warningMB: Int, limitMB: Int)?
        var failure: BenchmarkError?

        lock.lock()
        if var state {
            if memoryMB > state.peakMB {
                state.peakMB = memoryMB
            }

            if !state.warned, memoryMB >= state.config.warningMB {
                state.warned = true
                warningContext = (state.config.warningMB, state.config.limitMB)
            }

            if memoryMB >= state.config.limitMB {
                failure = .memoryGuardExceeded(
                    stage: stage,
                    detail: detail,
                    currentMB: memoryMB,
                    warningMB: state.config.warningMB,
                    limitMB: state.config.limitMB,
                    peakMB: state.peakMB
                )
            }
            self.state = state
        }
        lock.unlock()

        if let warningContext {
            BenchmarkRuntimeRenderer.printField(
                "Memory warning",
                "\(memoryMB)MB reached guardrail in \(stage) (warn \(warningContext.warningMB)MB, stop \(warningContext.limitMB)MB)",
                semantic: .warning
            )
            if let detail, !detail.isEmpty {
                BenchmarkRuntimeRenderer.printDetail(detail)
            }
        }

        if let failure {
            throw failure
        }
    }

    private static func defaultLimitMB(physicalMemoryMB: Int) -> Int {
        let reserveMB = 512
        let percentageLimit = Int(Double(physicalMemoryMB) * 0.80)
        return max(1024, min(max(1024, physicalMemoryMB - reserveMB), percentageLimit))
    }
}

enum BenchmarkMemoryGuard {
    private static let impl = BenchmarkMemoryGuardImpl()

    static func bootstrap() -> BenchmarkMemoryGuardConfiguration? {
        impl.bootstrap()
    }

    static func statusSummary() -> String {
        impl.statusSummary()
    }

    static func peakMB() -> Int {
        impl.peakMB()
    }

    static func checkpoint(stage: String, detail: String? = nil) throws {
        try impl.checkpoint(stage: stage, detail: detail)
    }
}
