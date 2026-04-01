import Foundation
import Darwin
import KikoMediaCore

// MARK: - ANSI Colors (4-bit only — works on every terminal)

let isColorEnabled = TerminalUIPrimitives.isColorEnabled
let bold = TerminalUIPrimitives.bold
let dim = TerminalUIPrimitives.dim
let reset = TerminalUIPrimitives.reset
let red = TerminalUIPrimitives.red
let green = TerminalUIPrimitives.green
let yellow = TerminalUIPrimitives.yellow

// MARK: - Screen Management

func setTerminalTitle(_ title: String) {
    TerminalUIPrimitives.setTerminalTitle(title)
}

func clearScreen() {
    TerminalUIPrimitives.clearScreen()
}

func redraw(_ completed: [(String, String)], subtitle: String = "This wizard configures your deployment.") {
    TerminalUIPrimitives.redraw(
        title: "kiko-media orchestrator",
        subtitle: subtitle,
        completed: completed
    )
}

// MARK: - Output Helpers

let listItemIndent = TerminalUIPrimitives.listItemIndent
let listDetailIndent = TerminalUIPrimitives.listDetailIndent

func printStep(_ number: Int, of total: Int, _ text: String) {
    TerminalUIPrimitives.printStep(number, of: total, text)
}


func printSuccess(_ text: String) {
    TerminalUIPrimitives.printSuccess(text)
}

func printError(_ text: String) {
    TerminalUIPrimitives.printError(text)
}

func printWarning(_ text: String) {
    TerminalUIPrimitives.printWarning(text)
}

func printHint(_ text: String) {
    TerminalUIPrimitives.printHint(text)
}

func printSectionTitle(_ text: String) {
    TerminalUIPrimitives.printSectionTitle(text)
}

func printCompactSectionTitle(_ text: String) {
    TerminalUIPrimitives.printSubsectionTitle(text)
}

func printField(_ label: String, _ value: String) {
    TerminalUIPrimitives.printField(label, value: value)
}

func printBody(_ text: String) {
    TerminalUIPrimitives.printBody(text)
}

func printNumberedItems(_ items: [String], start: Int = 1, indent: String = listItemIndent) {
    for (offset, item) in items.enumerated() {
        let number = start + offset
        print("\(indent)\(dim)\(number).\(reset) \(item)")
    }
}

func printActionMenuItem(_ number: Int, title: String, detail: String) {
    TerminalUIPrimitives.printActionMenuItem(number, title: title, detail: detail)
}

func printManualCommands(_ title: String, commands: [String]) {
    printSectionTitle(title)
    for command in commands {
        print("\(listItemIndent)\(bold)\(command)\(reset)")
    }
    print()
}

// MARK: - Input Helpers

func prompt(_ message: String, default defaultValue: String? = nil) -> String {
    TerminalUIPrimitives.prompt(message, default: defaultValue)
}

func promptRequired(_ message: String) -> String {
    TerminalUIPrimitives.promptRequired(message)
}

func confirm(
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
func keepOrChange(
    label: String,
    current: String,
    display: String? = nil,
    actionHint: String = "(Enter=keep, c=change)",
    showCurrentLine: Bool = true
) -> Bool {
    while true {
        if showCurrentLine {
            print("  \(dim)Current \(label):\(reset) \(display ?? current)")
        }
        print("  \(bold)Action\(reset) \(dim)\(actionHint)\(reset): ", terminator: "")
        fflush(stdout)

        let input = (readLine() ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()

        if input.isEmpty { return false }
        if input == "c" || input == "change" {
            print()
            return true
        }

        printError("Press Enter to keep, or type c to change.")
    }
}

func maskedPreview(_ value: String, visiblePrefix: Int = 12) -> String {
    guard !value.isEmpty else { return "(empty)" }
    guard value.count > visiblePrefix else { return value }
    return "\(value.prefix(visiblePrefix))..."
}

func promptValidSessionHmacSecret() -> String {
    while true {
        let input = promptRequired("Session HMAC secret")
        if validateSessionHmacSecret(input) {
            return input
        }
        printError("Secret too short. Minimum is 32 bytes.")
    }
}

func promptValidGateSecret() -> String {
    printHint("Minimum 8 characters.")
    while true {
        let input = promptRequired("Gate secret")
        if validateGateSecret(input) {
            return input
        }
        printError("Too short. Minimum 8 characters.")
    }
}

func waitForEnter(_ message: String = "Press Enter to continue") {
    TerminalUIPrimitives.waitForEnter(message)
}


func promptValidCloudflareToken() -> String {
    while true {
        let input = promptRequired("Cloudflare API token")
        if validateCloudflareTokenFormat(input) {
            return input
        }
        printError("Token format looks invalid. Paste the full Cloudflare API token.")
    }
}
