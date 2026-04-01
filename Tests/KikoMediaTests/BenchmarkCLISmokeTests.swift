import Foundation
import Testing
@testable import benchmarks

@Suite("Benchmark CLI Smoke")
struct BenchmarkCLISmokeTests {
    private static let repoRootSentinels = [
        "Package.swift",
        "Sources/Benchmarks/Main.swift",
    ]

    struct CommandResult {
        let status: Int32
        let stdout: String
        let stderr: String
    }

    enum SmokeError: Error {
        case processLaunchFailed(String)
    }

    @Test("benchmark --help includes wizard mode")
    func benchmarkHelpIncludesWizardMode() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let help = try Self.runBenchmark(repoRoot: repoRoot, arguments: ["--help"])

        #expect(help.status == 0)
        let flags = Self.extractCommandFlags(from: help)
        #expect(flags.contains("--wizard"), "Default help is missing --wizard")
    }

    @Test("benchmark --help-advanced remains available")
    func benchmarkAdvancedHelpStillWorks() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let advancedHelp = try Self.runBenchmark(repoRoot: repoRoot, arguments: ["--help-advanced"])

        #expect(advancedHelp.status == 0)
        let flags = Self.extractCommandFlags(from: advancedHelp)
        #expect(flags.contains("--wizard"), "Advanced help should include --wizard")
        #expect(flags.contains("--tb-workers"), "Advanced help should include Thunderbolt options")
    }

    @Test("benchmark --list stays aligned with benchmark catalog")
    func benchmarkListMatchesCatalog() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let list = try Self.runBenchmark(repoRoot: repoRoot, arguments: ["--list"])
        #expect(list.status == 0)

        let output = Self.stripANSI(list.stdout + "\n" + list.stderr)
        for spec in BenchmarkCatalog.components where spec.id != .thunderbolt {
            #expect(output.contains("    \(spec.id.rawValue)"))
            #expect(output.contains(spec.detail))
        }
        if let thunderboltSpec = BenchmarkCatalog.components.first(where: { $0.id == .thunderbolt }) {
            #expect(output.contains("    thunderbolt"))
            #expect(output.contains("(requires external Macs) \(thunderboltSpec.detail)"))
        }
    }

    @Test("benchmark --help includes usage and reference entries")
    func benchmarkHelpIncludesUsageAndReferenceEntries() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let help = try Self.runBenchmark(repoRoot: repoRoot, arguments: ["--help"])
        #expect(help.status == 0)

        let output = Self.stripANSI(help.stdout + "\n" + help.stderr)
        #expect(!output.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        #expect(output.contains("Usage"))

        let flags = Self.extractCommandFlags(from: help)
        #expect(flags.contains("--list"), "Default help is missing --list")
        #expect(flags.contains("--help-advanced"), "Default help is missing --help-advanced")
        #expect(flags.contains("--stage"), "Default help is missing --stage")
    }

    private static func resolveRepoRoot() throws -> String {
        try TestRepositoryRoot.resolve(
            from: #filePath,
            sentinels: repoRootSentinels
        ).path
    }

    private static func benchmarkExecutable(repoRoot: String) -> String? {
        let candidates = [
            "\(repoRoot)/.build/debug/benchmark",
            "\(repoRoot)/.build/debug/benchmarks",
            "\(repoRoot)/.build/arm64-apple-macosx/debug/benchmark",
            "\(repoRoot)/.build/arm64-apple-macosx/debug/benchmarks",
            "\(repoRoot)/.build/release/benchmark",
            "\(repoRoot)/.build/release/benchmarks",
            "\(repoRoot)/.build/arm64-apple-macosx/release/benchmark",
            "\(repoRoot)/.build/arm64-apple-macosx/release/benchmarks",
        ]
        let fileManager = FileManager.default
        for candidate in candidates where fileManager.isExecutableFile(atPath: candidate) {
            return candidate
        }
        return nil
    }

    private static func runBenchmark(repoRoot: String, arguments: [String]) throws -> CommandResult {
        if let executable = benchmarkExecutable(repoRoot: repoRoot) {
            return try runCommand(
                executable: executable,
                arguments: arguments,
                workingDirectory: repoRoot
            )
        }

        let swiftRunArguments = ["run", "benchmark"] + arguments
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
        let stripped = stripANSI(combinedOutput)
        let lines = stripped
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map(String.init)

        var flags = Set<String>()
        for raw in lines {
            let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            let token = trimmed.split(separator: " ", maxSplits: 1).first.map(String.init) ?? trimmed
            if token.hasPrefix("--") {
                flags.insert(token)
            }
        }
        return flags
    }

    private static func stripANSI(_ value: String) -> String {
        value.replacing(/\x1b\[[0-9;]*m/, with: "")
    }
}
