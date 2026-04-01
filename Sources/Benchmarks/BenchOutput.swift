import Darwin
import Foundation

// Swift 6 strict concurrency rejects mutable static state by default.
// Keep the BenchOutput API but store mutable state in a locked singleton.
private final class BenchOutputImpl: @unchecked Sendable {
    private let lock = NSLock()
    private var reportHandle: FileHandle?
    private var reportURL: URL?
    private var reportLineBuffer = ""
    private var useStderr = false

    func setUseStderr(_ enabled: Bool) {
        lock.lock()
        defer { lock.unlock() }
        useStderr = enabled
    }

    func startReport(reportDirectory: String) throws -> URL {
        let fm = FileManager.default
        let cwd = fm.currentDirectoryPath
        let expandedReportDirectory = NSString(string: reportDirectory).expandingTildeInPath
        let resultsDir = expandedReportDirectory.hasPrefix("/")
            ? URL(fileURLWithPath: expandedReportDirectory, isDirectory: true).standardizedFileURL
            : URL(fileURLWithPath: cwd, isDirectory: true)
                .appendingPathComponent(expandedReportDirectory, isDirectory: true)
                .standardizedFileURL
        try fm.createDirectory(at: resultsDir, withIntermediateDirectories: true)

        let filename = "\(timestampForFilename()).md"
        let url = resultsDir.appendingPathComponent(filename)
        fm.createFile(atPath: url.path, contents: nil)

        let handle = try FileHandle(forWritingTo: url)

        lock.lock()
        if let existing = reportHandle {
            try? existing.close()
        }
        reportHandle = handle
        reportURL = url
        reportLineBuffer.removeAll(keepingCapacity: false)
        lock.unlock()

        return url
    }

    func finishReport() {
        lock.lock()
        defer { lock.unlock() }
        if let handle = reportHandle {
            try? handle.synchronize()
            try? handle.close()
        }
        reportHandle = nil
        reportURL = nil
        reportLineBuffer.removeAll(keepingCapacity: false)
    }

    func currentReportPath() -> String? {
        lock.lock()
        defer { lock.unlock() }
        return reportURL?.path
    }

    func line(_ text: String = "") {
        write(text + "\n")
    }

    func write(_ text: String) {
        lock.lock()
        defer { lock.unlock() }

        writeToTerminal(text)

        guard let handle = reportHandle else { return }

        // Report (plain text markdown, without transient carriage-return rewrites).
        let reportText = sanitizedReportText(text)
        if !reportText.isEmpty, let data = reportText.data(using: .utf8) {
            // Best-effort; benchmark output should still flow to console.
            try? handle.write(contentsOf: data)
        }
    }

    func writeTerminalOnly(_ text: String) {
        lock.lock()
        defer { lock.unlock() }

        guard supportsTerminalOnlyWritesLocked() else { return }
        writeToTerminal(text)
    }

    func supportsTerminalOnlyWrites() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return supportsTerminalOnlyWritesLocked()
    }

    private func sanitizedReportText(_ text: String) -> String {
        let stripped = stripANSI(text)
        guard !stripped.isEmpty else { return "" }

        var output = ""
        output.reserveCapacity(stripped.count)

        for character in stripped {
            switch character {
            case "\r":
                // Transient status updates overwrite the current terminal line.
                reportLineBuffer.removeAll(keepingCapacity: true)
            case "\n":
                output.append(reportLineBuffer)
                output.append("\n")
                reportLineBuffer.removeAll(keepingCapacity: true)
            default:
                reportLineBuffer.append(character)
            }
        }

        return output
    }

    private func stripANSI(_ text: String) -> String {
        var output = ""
        var index = text.startIndex

        while index < text.endIndex {
            if text[index] == "\u{1B}" {
                let next = text.index(after: index)
                if next < text.endIndex, text[next] == "[" {
                    // CSI: ESC [ ... final-byte
                    var cursor = text.index(after: next)
                    while cursor < text.endIndex {
                        let scalar = text[cursor].unicodeScalars.first?.value ?? 0
                        if (0x40...0x7E).contains(scalar) {
                            cursor = text.index(after: cursor)
                            break
                        }
                        cursor = text.index(after: cursor)
                    }
                    index = cursor
                    continue
                }
                if next < text.endIndex, text[next] == "]" {
                    // OSC: ESC ] ... BEL or ST (ESC \)
                    var cursor = text.index(after: next)
                    while cursor < text.endIndex {
                        if text[cursor] == "\u{07}" {
                            cursor = text.index(after: cursor)
                            break
                        }
                        if text[cursor] == "\u{1B}" {
                            let maybeST = text.index(after: cursor)
                            if maybeST < text.endIndex, text[maybeST] == "\\" {
                                cursor = text.index(after: maybeST)
                                break
                            }
                        }
                        cursor = text.index(after: cursor)
                    }
                    index = cursor
                    continue
                }
            }

            output.append(text[index])
            index = text.index(after: index)
        }

        return output
    }

    private func timestampForFilename() -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone.current
        formatter.dateFormat = "yyyy-MM-dd_HHmmss"
        return formatter.string(from: Date())
    }

    private func supportsTerminalOnlyWritesLocked() -> Bool {
        let stream = useStderr ? stderr : stdout
        return isatty(fileno(stream)) == 1
    }

    private func writeToTerminal(_ text: String) {
        let stream = useStderr ? stderr : stdout
        _ = text.withCString { cString in
            fputs(cString, stream)
        }
        fflush(stream)
    }
}

enum BenchOutput {
    private static let impl = BenchOutputImpl()

    static func startReport(reportDirectory: String = "bench-results") throws -> URL {
        try impl.startReport(reportDirectory: reportDirectory)
    }

    static func finishReport() { impl.finishReport() }
    static func currentReportPath() -> String? { impl.currentReportPath() }
    static func line(_ text: String = "") { impl.line(text) }
    static func write(_ text: String) { impl.write(text) }
    static func writeTerminalOnly(_ text: String) { impl.writeTerminalOnly(text) }
    static func supportsTerminalOnlyWrites() -> Bool { impl.supportsTerminalOnlyWrites() }
    static func redirectToStderr(_ enabled: Bool) { impl.setUseStderr(enabled) }
}
