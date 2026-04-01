import Darwin
import Foundation
import KikoMediaCore

enum ConsoleUI {
    // MARK: - ANSI Colors (4-bit only — works on every terminal)

    static let isColorEnabled = TerminalUIPrimitives.isColorEnabled
    static let bold = TerminalUIPrimitives.bold
    static let dim = TerminalUIPrimitives.dim
    static let reset = TerminalUIPrimitives.reset
    static let red = TerminalUIPrimitives.red
    static let green = TerminalUIPrimitives.green
    static let yellow = TerminalUIPrimitives.yellow

    // MARK: - Screen Management

    static func setTerminalTitle(_ title: String) {
        TerminalUIPrimitives.setTerminalTitle(title)
    }

    static func clearScreen() {
        TerminalUIPrimitives.clearScreen()
    }

    static func redraw(title: String, subtitle: String, completed: [(String, String)]) {
        TerminalUIPrimitives.redraw(title: title, subtitle: subtitle, completed: completed)
    }

    // MARK: - Output Helpers

    static let listItemIndent = TerminalUIPrimitives.listItemIndent
    static let listDetailIndent = TerminalUIPrimitives.listDetailIndent

    static func printStep(_ number: Int, of total: Int, _ text: String) {
        TerminalUIPrimitives.printStep(number, of: total, text)
    }

    static func printStep(_ number: Int, _ text: String) {
        TerminalUIPrimitives.printStep(number, text)
    }

    static func printSuccess(_ text: String) {
        TerminalUIPrimitives.printSuccess(text)
    }

    static func printError(_ text: String) {
        TerminalUIPrimitives.printError(text)
    }

    static func printWarning(_ text: String) {
        TerminalUIPrimitives.printWarning(text)
    }

    static func printHint(_ text: String) {
        TerminalUIPrimitives.printHint(text)
    }

    static func printSectionTitle(_ text: String) {
        BenchmarkRuntimeRenderer.printSectionTitle(text)
    }

    static func printBody(_ text: String) {
        BenchmarkRuntimeRenderer.printBody(text)
    }

    static func printField(_ label: String, value: String) {
        BenchmarkRuntimeRenderer.printField(label, value)
    }

    static func printSubsectionTitle(_ text: String) {
        BenchmarkRuntimeRenderer.printSubsectionTitle(text, includeTrailingBlankLine: false)
    }

    static func printActionMenuItem(_ number: Int, title: String, detail: String) {
        TerminalUIPrimitives.printActionMenuItem(number, title: title, detail: detail)
    }

    // MARK: - Input Helpers

    static func prompt(_ message: String, default defaultValue: String? = nil) -> String {
        TerminalUIPrimitives.prompt(message, default: defaultValue)
    }

    static func promptRequired(_ message: String) -> String {
        TerminalUIPrimitives.promptRequired(message)
    }

    static func confirm(
        _ message: String,
        defaultYes: Bool? = nil,
        readInput: TerminalUIPrimitives.Reader = { readLine() },
        writer: TerminalUIPrimitives.Writer = TerminalUIPrimitives.stdoutWriter
    ) -> Bool {
        TerminalUIPrimitives.confirm(
            message,
            defaultYes: defaultYes,
            readInput: readInput,
            writer: writer
        )
    }

    /// Returns true if the user wants to change the value, false to keep it.
    static func keepOrChange(label: String, current: String, display: String? = nil) -> Bool {
        while true {
            printField("Current \(label)", value: display ?? current)
            Swift.print("  \(bold)Action\(reset) \(dim)(Enter=keep, c=change)\(reset): ", terminator: "")
            fflush(stdout)

            let input = (readLine() ?? "")
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .lowercased()

            if input.isEmpty { return false }
            if input == "c" || input == "change" {
                Swift.print("")
                return true
            }

            printError("Press Enter to keep, or type c to change.")
        }
    }

    static func waitForEnter(_ message: String = "Press Enter to continue") {
        TerminalUIPrimitives.waitForEnter(message)
    }

    // MARK: - Path Input Normalization

    static func normalizePathInput(_ raw: String) -> String {
        TerminalUIPrimitives.normalizePathInput(raw)
    }

    // MARK: - External SSD Picker

    static func formatBytes(_ bytes: Int64) -> String {
        BenchmarkByteFormatter.format(bytes)
    }

    static func pickSSDPath() -> String {
        TerminalUIPrimitives.pickExternalSSDPath(
            formatBytes: formatBytes,
            manualPathPrompt: "SSD path",
            manualPathExamples: ["/Volumes/MySSD", "/Volumes/MyDrive/bench-folder"],
            selectionHint: "Bench artifacts are written under <ssd path>/bench-results/<run-id>/ and removed after the run.",
            selectedVolumePath: { $0.path }
        )
    }
}
