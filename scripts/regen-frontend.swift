import Foundation
import Darwin

let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
let scriptURL = URL(fileURLWithPath: CommandLine.arguments[0], relativeTo: cwd).standardizedFileURL
let repoRoot = scriptURL.deletingLastPathComponent().deletingLastPathComponent()

let process = Process()
process.executableURL = URL(fileURLWithPath: "/usr/bin/swift")
process.currentDirectoryURL = repoRoot
process.arguments = ["run", "orchestrator", "--internal-regen-frontend"] + Array(CommandLine.arguments.dropFirst())

do {
    try process.run()
    process.waitUntilExit()
    exit(process.terminationStatus)
} catch {
    fputs("Failed to run frontend regeneration: \(error)\n", stderr)
    exit(1)
}
