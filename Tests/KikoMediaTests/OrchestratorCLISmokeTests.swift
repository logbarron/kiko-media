import Foundation
import Testing

@Suite("Orchestrator CLI Smoke")
struct OrchestratorCLISmokeTests {
    private static let repoRootSentinels = [
        "Package.swift",
        "deploy/Caddyfile.template",
    ]

    private static let requiredCommands: Set<String> = [
        "<base>",
        "--status",
        "--start",
        "--restart",
        "--shutdown",
        "--thunderbolt",
        "--tb-status",
        "--help",
    ]

    struct CommandResult {
        let status: Int32
        let stdout: String
        let stderr: String
    }

    enum SmokeError: Error {
        case processLaunchFailed(String)
    }

    @Test("orchestrator --help command surface is stable")
    func orchestratorHelpCommandSurfaceStable() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let orchestratorHelp = try Self.runOrchestrator(repoRoot: repoRoot, arguments: ["--help"])

        #expect(orchestratorHelp.status == 0)
        let commandFlags = Self.extractCommandFlags(from: orchestratorHelp)
        let missing = Self.requiredCommands.subtracting(commandFlags)
        #expect(
            missing.isEmpty,
            "Missing required orchestrator commands: \(missing.sorted().joined(separator: ", "))"
        )
    }

    @Test("orchestrator --help groups --status under Services")
    func orchestratorHelpStatusGrouping() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let orchestratorHelp = try Self.runOrchestrator(repoRoot: repoRoot, arguments: ["--help"])

        #expect(orchestratorHelp.status == 0)
        let groups = Self.extractHelpGroupItems(from: orchestratorHelp)
        #expect(groups["Services"]?.contains("--status") == true)
        #expect(groups["Setup"]?.contains("--status") != true)
    }

    @Test("orchestrator --status exits successfully")
    func orchestratorStatusCommandRuns() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let result = try Self.runOrchestrator(repoRoot: repoRoot, arguments: ["--status"])

        #expect(result.status == 0)
        #expect((result.stdout + result.stderr).contains("kiko-media"))
    }

    @Test("orchestrator unknown argument returns usage error")
    func orchestratorUnknownArgumentFailsWithUsageExit() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let result = try Self.runOrchestrator(repoRoot: repoRoot, arguments: ["--definitely-not-a-real-flag"])

        #expect(result.status == 2)
        #expect((result.stdout + result.stderr).contains("Unknown argument"))
    }

    private static func resolveRepoRoot() throws -> String {
        try TestRepositoryRoot.resolve(
            from: #filePath,
            sentinels: repoRootSentinels
        ).path
    }

    private static func orchestratorExecutable(repoRoot: String) -> String? {
        let candidates = [
            "\(repoRoot)/.build/debug/orchestrator",
            "\(repoRoot)/.build/release/orchestrator",
            "\(repoRoot)/.build/arm64-apple-macosx/debug/orchestrator",
            "\(repoRoot)/.build/arm64-apple-macosx/release/orchestrator",
        ]
        let fileManager = FileManager.default
        for candidate in candidates where fileManager.isExecutableFile(atPath: candidate) {
            return candidate
        }
        return nil
    }

    private static func runOrchestrator(repoRoot: String, arguments: [String]) throws -> CommandResult {
        if let executable = orchestratorExecutable(repoRoot: repoRoot) {
            return try runCommand(
                executable: executable,
                arguments: arguments,
                workingDirectory: repoRoot
            )
        }

        let swiftRunArguments = ["run", "orchestrator"] + arguments
        return try runCommand(
            executable: "/usr/bin/swift",
            arguments: swiftRunArguments,
            workingDirectory: repoRoot
        )
    }

    private static func runCommand(
        executable: String,
        arguments: [String],
        workingDirectory: String
    ) throws -> CommandResult {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        process.currentDirectoryURL = URL(fileURLWithPath: workingDirectory)

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        do {
            try process.run()
        } catch {
            throw SmokeError.processLaunchFailed("\(executable) \(arguments.joined(separator: " "))")
        }

        process.waitUntilExit()

        let stdoutData = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        let stderrData = stderrPipe.fileHandleForReading.readDataToEndOfFile()

        return CommandResult(
            status: process.terminationStatus,
            stdout: String(data: stdoutData, encoding: .utf8) ?? "",
            stderr: String(data: stderrData, encoding: .utf8) ?? ""
        )
    }

    private static func extractCommandFlags(from result: CommandResult) -> Set<String> {
        let combinedOutput = result.stdout + "\n" + result.stderr
        let stripped = combinedOutput.replacing(/\x1b\[[0-9;]*m/, with: "")
        let lines = stripped
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map(String.init)

        var flags = Set<String>()
        var inUsageSection = false

        for raw in lines {
            let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)

            if trimmed == "Usage" {
                inUsageSection = true
                continue
            }

            if inUsageSection {
                if trimmed.isEmpty { continue }
                let usageToken = trimmed.split(separator: " ", maxSplits: 1).first.map(String.init) ?? ""
                if usageToken == "orchestrator" || usageToken.hasSuffix("/orchestrator") {
                    flags.insert("<base>")
                }
                inUsageSection = false
            }

            let token = trimmed.split(separator: " ", maxSplits: 1).first.map(String.init) ?? trimmed
            if token == "(none)" {
                flags.insert("<base>")
            } else if token.hasPrefix("--") {
                flags.insert(token)
            }
        }

        return flags
    }

    private static func extractHelpGroupItems(from result: CommandResult) -> [String: Set<String>] {
        let combinedOutput = result.stdout + "\n" + result.stderr
        let stripped = combinedOutput.replacing(/\x1b\[[0-9;]*m/, with: "")
        let lines = stripped
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map(String.init)

        let headings: Set<String> = ["Setup", "Services", "Workers", "Help"]
        var groups: [String: Set<String>] = [:]
        var currentHeading: String?

        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty { continue }

            if headings.contains(trimmed) {
                currentHeading = trimmed
                if groups[trimmed] == nil {
                    groups[trimmed] = []
                }
                continue
            }

            guard let heading = currentHeading else { continue }
            if line.hasPrefix("    --") {
                let flag = trimmed.split(separator: " ", maxSplits: 1).first.map(String.init) ?? trimmed
                groups[heading, default: []].insert(flag)
                continue
            }

            if !line.hasPrefix(" ") {
                currentHeading = nil
            }
        }

        return groups
    }
}
