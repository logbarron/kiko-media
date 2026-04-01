import Darwin
import Foundation

public enum TerminalUIPrimitives {
    public typealias Reader = () -> String?
    public typealias Writer = (_ text: String, _ terminator: String) -> Void

    public static func stdoutWriter(_ text: String, _ terminator: String) {
        Swift.print(text, terminator: terminator)
    }

    public static let isColorEnabled = isatty(fileno(stdout)) != 0

    private static func ansi(_ code: String) -> String {
        isColorEnabled ? "\u{1b}[\(code)m" : ""
    }

    public static let bold = ansi("1")
    public static let dim = ansi("2")
    public static let reset = ansi("0")
    public static let red = ansi("31")
    public static let green = ansi("32")
    public static let yellow = ansi("33")

    public static let listItemIndent = "    "
    public static let listDetailIndent = "       "

    public static func setTerminalTitle(
        _ title: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        guard isColorEnabled else { return }
        writer("\u{1b}]0;\(title)\u{7}", "")
        fflush(stdout)
    }

    public static func clearScreen(
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        if isColorEnabled {
            writer("\u{1b}[2J\u{1b}[H", "")
            fflush(stdout)
        } else {
            writer("\n---\n", "\n")
        }
    }

    public static func redraw(
        title: String,
        subtitle: String,
        completed: [(String, String)],
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        setTerminalTitle(title, writer: writer)
        clearScreen(writer: writer)
        writer(
            """

      \(bold)\(title)\(reset)
      \(dim)\(subtitle)
      Press Ctrl-C at any time to cancel.\(reset)

    """,
            "\n"
        )
        for (label, value) in completed {
            writer(formatFieldLine(label: label, value: value), "\n")
        }
        if !completed.isEmpty {
            writer("", "\n")
        }
    }

    public static func formatStepLine(_ number: Int, of total: Int, _ text: String) -> String {
        "  \(bold)Step \(number) of \(total)\(reset) \(dim)·\(reset) \(bold)\(text)\(reset)"
    }

    public static func formatStepLine(_ number: Int, _ text: String) -> String {
        "  \(bold)Step \(number)\(reset) \(dim)·\(reset) \(bold)\(text)\(reset)"
    }

    public static func formatSuccessLine(_ text: String) -> String {
        formatStatusLine(symbol: "✓", color: green, text: text)
    }

    public static func formatErrorLine(_ text: String) -> String {
        formatStatusLine(symbol: "✗", color: red, text: text)
    }

    public static func formatWarningLine(_ text: String) -> String {
        formatStatusLine(symbol: "!", color: yellow, text: text)
    }

    public static func formatHintLine(_ text: String) -> String {
        "  \(dim)\(text)\(reset)"
    }

    public static func formatStatusLine(symbol: String, color: String, text: String) -> String {
        "  \(color)\(symbol)\(reset) \(text)"
    }

    public static func formatFieldLine(label: String, value: String) -> String {
        "  \(dim)\(label):\(reset) \(value)"
    }

    public static func formatBodyLine(_ text: String) -> String {
        "  \(text)"
    }

    public static func formatActionMenuLines(_ number: Int, title: String, detail: String) -> (titleLine: String, detailLine: String) {
        let titleLine = "\(listItemIndent)\(bold)\(number).\(reset) \(title)"
        let detailLine = "\(listDetailIndent)\(dim)\(detail)\(reset)"
        return (titleLine, detailLine)
    }

    public static func printStep(
        _ number: Int,
        of total: Int,
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatStepLine(number, of: total, text), "\n")
    }

    public static func printStep(
        _ number: Int,
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatStepLine(number, text), "\n")
    }

    public static func printSuccess(
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatSuccessLine(text), "\n")
    }

    public static func printError(
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatErrorLine(text), "\n")
    }

    public static func printWarning(
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatWarningLine(text), "\n")
    }

    public static func printHint(
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatHintLine(text), "\n")
    }

    public static func printSectionTitle(
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer("", "\n")
        writer("  \(bold)\(text)\(reset)", "\n")
        writer("", "\n")
    }

    public static func printSubsectionTitle(
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer("  \(bold)\(text)\(reset)", "\n")
    }

    public static func printField(
        _ label: String,
        value: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatFieldLine(label: label, value: value), "\n")
    }

    public static func printBody(
        _ text: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        writer(formatBodyLine(text), "\n")
    }

    public static func printActionMenuItem(
        _ number: Int,
        title: String,
        detail: String,
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        let lines = formatActionMenuLines(number, title: title, detail: detail)
        writer(lines.titleLine, "\n")
        writer(lines.detailLine, "\n")
    }

    public static func formatPrompt(_ message: String, default defaultValue: String? = nil) -> String {
        if let def = defaultValue {
            return "  \(bold)\(message)\(reset) \(dim)(\(def))\(reset): "
        }
        return "  \(bold)\(message)\(reset): "
    }

    public static func prompt(
        _ message: String,
        default defaultValue: String? = nil,
        readInput: Reader = { readLine() },
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) -> String {
        writer(formatPrompt(message, default: defaultValue), "")
        fflush(stdout)
        let input = readInput()?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return input.isEmpty ? (defaultValue ?? "") : input
    }

    public static func promptRequired(
        _ message: String,
        readInput: Reader = { readLine() },
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) -> String {
        while true {
            let value = prompt(message, readInput: readInput, writer: writer)
            if !value.isEmpty { return value }
            printError("This field is required.", writer: writer)
        }
    }

    public static func confirmSuffix(defaultYes: Bool?) -> String {
        if let defaultYes {
            return defaultYes ? " (Y/n)" : " (y/N)"
        }
        return " (y/n)"
    }

    public static func resolveConfirmResponse(_ raw: String, defaultYes: Bool?) -> Bool? {
        let response = raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if response.isEmpty, let defaultYes { return defaultYes }
        if response == "y" || response == "yes" { return true }
        if response == "n" || response == "no" { return false }
        return nil
    }

    public static func confirm(
        _ message: String,
        defaultYes: Bool? = nil,
        readInput: Reader = { readLine() },
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) -> Bool {
        while true {
            let response = prompt(
                message + dim + confirmSuffix(defaultYes: defaultYes) + reset,
                readInput: readInput,
                writer: writer
            )
            if let decision = resolveConfirmResponse(response, defaultYes: defaultYes) {
                return decision
            }
            printError("Enter y or n.", writer: writer)
        }
    }

    public static func waitForEnter(
        _ message: String = "Press Enter to continue",
        readInput: Reader = { readLine() },
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) {
        printHint(message, writer: writer)
        _ = readInput()
    }

    public static func normalizePathInput(_ raw: String) -> String {
        var s = raw.trimmingCharacters(in: .whitespacesAndNewlines)

        if (s.hasPrefix("\"") && s.hasSuffix("\"")) || (s.hasPrefix("'") && s.hasSuffix("'")) {
            s = String(s.dropFirst().dropLast())
        }

        // Terminal drag/drop commonly inserts shell-escaped paths (e.g. "/Users/me/My\\ Folder").
        // This is not a shell, so unescape common escaped characters.
        s = s.replacingOccurrences(of: "\\ ", with: " ")
        s = s.replacingOccurrences(of: "\\(", with: "(")
        s = s.replacingOccurrences(of: "\\)", with: ")")
        s = s.replacingOccurrences(of: "\\[", with: "[")
        s = s.replacingOccurrences(of: "\\]", with: "]")
        s = s.replacingOccurrences(of: "\\&", with: "&")
        s = s.replacingOccurrences(of: "\\'", with: "'")
        s = s.replacingOccurrences(of: "\\\"", with: "\"")

        // Expand ~ for interactive input (the shell won't do it here).
        s = (s as NSString).expandingTildeInPath
        return s
    }

    public static func discoverExternalVolumes(
        fileManager: FileManager = .default
    ) -> [URL] {
        guard let volumes = fileManager.mountedVolumeURLs(
            includingResourceValuesForKeys: [.volumeNameKey, .volumeIsRemovableKey],
            options: .skipHiddenVolumes
        ) else { return [] }

        let systemPrefixes = [
            "/System", "/Volumes/Macintosh HD", "/Volumes/Preboot",
            "/Volumes/Recovery", "/Volumes/VM", "/Volumes/Update",
        ]

        return volumes.filter { url in
            let path = url.path
            guard path.hasPrefix("/Volumes/"),
                  !systemPrefixes.contains(where: { path.hasPrefix($0) }) else {
                return false
            }
            do {
                let values = try url.resourceValues(forKeys: [.isVolumeKey])
                return values.isVolume == true
            } catch {
                return false
            }
        }
    }

    public static func pickExternalSSDPath(
        volumes: [URL] = discoverExternalVolumes(),
        formatBytes: (Int64) -> String,
        manualPathPrompt: String,
        manualPathExamples: [String],
        selectionHint: String? = nil,
        selectedVolumePath: (URL) -> String,
        readInput: Reader = { readLine() },
        writer: Writer = TerminalUIPrimitives.stdoutWriter
    ) -> String {
        func promptManualPath() -> String {
            printHint("Tip: drag a folder from Finder into this terminal to paste the path.", writer: writer)
            if !manualPathExamples.isEmpty {
                printHint("Examples: \(manualPathExamples.joined(separator: "  or  "))", writer: writer)
            }
            writer("", "\n")
            return normalizePathInput(
                promptRequired(manualPathPrompt, readInput: readInput, writer: writer)
            )
        }

        if volumes.isEmpty {
            printWarning("No external volumes detected.", writer: writer)
            printHint("Mount your SSD and re-run, or enter the path manually.", writer: writer)
            writer("", "\n")
            return promptManualPath()
        }

        if let selectionHint, !selectionHint.isEmpty {
            printHint(selectionHint, writer: writer)
            writer("", "\n")
        }

        for (index, volume) in volumes.enumerated() {
            var sizeInfo = ""
            if let values = try? volume.resourceValues(forKeys: [.volumeTotalCapacityKey, .volumeAvailableCapacityKey]),
               let total = values.volumeTotalCapacity,
               let free = values.volumeAvailableCapacity {
                sizeInfo = " \(dim)(\(formatBytes(Int64(total))), \(formatBytes(Int64(free))) free)\(reset)"
            }
            writer("\(listItemIndent)\(bold)\(index + 1)\(reset)  \(volume.path)\(sizeInfo)", "\n")
        }
        writer("\(listItemIndent)\(dim)0\(reset)  Enter a path manually", "\n")
        writer("", "\n")

        while true {
            let choice = prompt("Which drive", readInput: readInput, writer: writer)
            if let selection = Int(choice) {
                if selection == 0 {
                    return promptManualPath()
                }
                if (1...volumes.count).contains(selection) {
                    return selectedVolumePath(volumes[selection - 1])
                }
            }
            printError("Enter a number from the list above.", writer: writer)
        }
    }
}
