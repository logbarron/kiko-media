import Foundation

enum BenchmarkInterruptError: Error, CustomStringConvertible {
    case interrupted

    var description: String {
        "run interrupted by SIGINT (Ctrl-C)"
    }
}

final class BenchmarkInterruptState: @unchecked Sendable {
    private let lock = NSLock()
    private var interrupted = false

    @discardableResult
    func requestInterrupt() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if interrupted {
            return false
        }
        interrupted = true
        return true
    }

    var isInterrupted: Bool {
        lock.lock()
        defer { lock.unlock() }
        return interrupted
    }

    func throwIfInterrupted() throws {
        if Task.isCancelled || isInterrupted {
            throw BenchmarkInterruptError.interrupted
        }
    }
}

func isBenchmarkInterrupted(_ error: Error, interruptState: BenchmarkInterruptState?) -> Bool {
    if error is BenchmarkInterruptError {
        return true
    }
    if error is CancellationError {
        return true
    }
    if let interruptState, interruptState.isInterrupted {
        return true
    }
    return false
}
