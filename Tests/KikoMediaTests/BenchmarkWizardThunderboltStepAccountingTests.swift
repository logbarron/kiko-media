import Foundation
import Testing

@Suite("Benchmark Wizard Thunderbolt step accounting", Testing.ParallelizationTrait.serialized)
struct BenchmarkWizardThunderboltStepAccountingTests {
    private static let repoRootSentinels = [
        "Package.swift",
        "Sources/Benchmarks/Main.swift",
    ]

    struct CommandResult {
        let status: Int32
        let stdout: String
        let stderr: String
    }

    enum WizardTestError: Error {
        case processLaunchFailed(String)
    }

    @Test("burst path uses dynamic total without skipped placeholders")
    func burstPathUsesDynamicTotalWithoutSkippedPlaceholders() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let mediaFolder = try Self.makeWizardMediaFolder()
        defer { try? FileManager.default.removeItem(at: mediaFolder) }

        let script = [
            "5", // Run mode: single stage
            "6", // Stage: thunderbolt
            mediaFolder.path,
            "",  // Workers: keep defaults
            "1", // Thunderbolt run type: burst sweep
            "",  // Sweep mode: keep default
            "4", // Run plan action: exit
        ].joined(separator: "\n") + "\n"

        let result = try Self.runWizard(
            repoRoot: repoRoot,
            input: script,
            environment: ["TB_WORKERS": "127.0.0.1:1"]
        )
        #expect(result.status == 0)

        let output = Self.stripANSI(result.stdout + "\n" + result.stderr)
        let stepLines = Self.extractStepLines(from: output)
        let runTypePromptBlock = Self.lastRedrawBlock(
            containingAll: ["Step 4", "Thunderbolt Run Type"],
            in: output
        )
        let sweepModePromptBlock = Self.lastRedrawBlock(
            containingAll: ["Step 5 of 5", "Thunderbolt Sweep Mode"],
            in: output
        )
        let runPlan = Self.lastRunPlanBlock(from: output)

        #expect(
            stepLines.contains(where: { $0.contains("Step 5 of 5") && $0.contains("Thunderbolt Sweep Mode") })
        )
        #expect(!runTypePromptBlock.contains("Sweep mode:"))
        #expect(!sweepModePromptBlock.contains("Sweep mode:"))
        #expect(runPlan.contains("Sweep mode"))
        #expect(!stepLines.contains(where: { $0.contains("Advanced Options") }))
        #expect(!output.contains("Skipped for burst sweep mode."))
    }

    @Test("showdown path uses dynamic 8-step total")
    func showdownPathUsesDynamicEightStepTotal() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let mediaFolder = try Self.makeWizardMediaFolder()
        defer { try? FileManager.default.removeItem(at: mediaFolder) }

        let script = [
            "5", // Run mode: single stage
            "6", // Stage: thunderbolt
            mediaFolder.path,
            "",  // Workers: keep defaults
            "2", // Thunderbolt run type: showdown
            "",  // Slot overrides: use defaults
            "",  // CA model mode: keep default
            "",  // Prior update policy: keep default off
            "",  // Showdown scope: keep default all profiles
            "4", // Run plan action: exit
        ].joined(separator: "\n") + "\n"

        let result = try Self.runWizard(
            repoRoot: repoRoot,
            input: script,
            environment: ["TB_WORKERS": "127.0.0.1:1"]
        )
        #expect(result.status == 0)

        let output = Self.stripANSI(result.stdout + "\n" + result.stderr)
        let stepLines = Self.extractStepLines(from: output)

        #expect(
            stepLines.contains(where: { $0.contains("Step 5 of 8") && $0.contains("CA Slot Overrides (This Run Only)") })
        )
        #expect(stepLines.contains(where: { $0.contains("Step 6 of 8") && $0.contains("CA Model Mode") }))
        #expect(stepLines.contains(where: { $0.contains("Step 7 of 8") && $0.contains("Scheduler Model Update") }))
        #expect(stepLines.contains(where: { $0.contains("Step 8 of 8") && $0.contains("CA Profiles") }))
        #expect(!stepLines.contains(where: { $0.contains("Advanced Options") }))
        #expect(!output.contains("Skipped for burst sweep mode."))
    }

    @Test("full run type exposes full CA configuration steps")
    func fullRunTypeExposesFullCAConfigurationSteps() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let mediaFolder = try Self.makeWizardMediaFolder()
        defer { try? FileManager.default.removeItem(at: mediaFolder) }

        let script = [
            "5", // Run mode: single stage
            "6", // Stage: thunderbolt
            mediaFolder.path,
            "",  // Workers: keep defaults
            "3", // Thunderbolt run type: full
            "",  // Sweep mode: keep default
            "",  // CA model mode: keep default
            "",  // Prior update policy: keep default off
            "",  // Showdown scope: keep default all profiles
            "4", // Run plan action: exit
        ].joined(separator: "\n") + "\n"

        let result = try Self.runWizard(
            repoRoot: repoRoot,
            input: script,
            environment: ["TB_WORKERS": "127.0.0.1:1"]
        )
        #expect(result.status == 0)

        let output = Self.stripANSI(result.stdout + "\n" + result.stderr)
        let stepLines = Self.extractStepLines(from: output)
        let runPlan = Self.lastRunPlanBlock(from: output)

        #expect(output.contains("Full stage (burst + showdown)"))
        #expect(output.contains("Thunderbolt run type (1-3)"))
        #expect(stepLines.contains(where: { $0.contains("Step 5 of 8") && $0.contains("Thunderbolt Sweep Mode") }))
        #expect(stepLines.contains(where: { $0.contains("Step 6 of 8") && $0.contains("CA Model Mode") }))
        #expect(stepLines.contains(where: { $0.contains("Step 7 of 8") && $0.contains("Scheduler Model Update") }))
        #expect(stepLines.contains(where: { $0.contains("Step 8 of 8") && $0.contains("CA Profiles") }))
        #expect(runPlan.contains("Thunderbolt mode"))
        #expect(runPlan.contains("CA model mode"))
        #expect(runPlan.contains("Model update"))
        #expect(runPlan.contains("CA profiles"))
    }

    @Test("edit settings preserves full run type when thunderbolt stage is reselected")
    func editSettingsPreservesFullRunTypeWhenThunderboltStageIsReselected() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let mediaFolder = try Self.makeWizardMediaFolder()
        defer { try? FileManager.default.removeItem(at: mediaFolder) }

        let script = [
            "5", // Run mode: single stage
            "6", // Stage: thunderbolt
            mediaFolder.path,
            "",  // Workers: keep defaults
            "3", // Thunderbolt run type: full
            "",  // Sweep mode: keep default
            "",  // CA model mode: keep default
            "",  // Prior update policy: keep default off
            "",  // Showdown scope: keep default all profiles
            "2", // Run plan action: edit settings
            "c", // Stage: change
            "6", // Stage: thunderbolt again
            "",  // Media folder: keep
            "",  // Workers: keep
            "",  // Run type: keep current
            "",  // Sweep mode: keep current
            "",  // CA model mode: keep current
            "",  // Prior update policy: keep current
            "",  // CA profiles: keep current
            "4", // Run plan action: exit
        ].joined(separator: "\n") + "\n"

        let result = try Self.runWizard(
            repoRoot: repoRoot,
            input: script,
            environment: ["TB_WORKERS": "127.0.0.1:1"]
        )
        #expect(result.status == 0)

        let output = Self.stripANSI(result.stdout + "\n" + result.stderr)
        let runPlan = Self.lastRunPlanBlock(from: output)

        #expect(runPlan.contains("Thunderbolt mode: full stage"))
        #expect(!runPlan.contains("Thunderbolt mode: showdown"))
    }

    @Test("showdown single-profile prompt keeps dynamic step context")
    func showdownSingleProfilePromptKeepsDynamicStepContext() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let mediaFolder = try Self.makeWizardMediaFolder()
        defer { try? FileManager.default.removeItem(at: mediaFolder) }

        let script = [
            "5", // Run mode: single stage
            "6", // Stage: thunderbolt
            mediaFolder.path,
            "",  // Workers: keep defaults
            "2", // Thunderbolt run type: showdown
            "",  // Slot overrides: use defaults
            "",  // CA model mode: keep default
            "",  // Prior update policy: keep default off
            "2", // Showdown scope: single profile pair
            "1", // Profile: all-at-once
            "4", // Run plan action: exit
        ].joined(separator: "\n") + "\n"

        let result = try Self.runWizard(
            repoRoot: repoRoot,
            input: script,
            environment: ["TB_WORKERS": "127.0.0.1:1"]
        )
        #expect(result.status == 0)

        let output = Self.stripANSI(result.stdout + "\n" + result.stderr)
        let stepLines = Self.extractStepLines(from: output)
        let singleProfilePromptBlock = Self.lastRedrawBlock(
            containingAll: ["Step 8 of 8", "Select Profile"],
            in: output
        )

        #expect(stepLines.contains(where: { $0.contains("Step 8 of 8") && $0.contains("CA Profiles") }))
        #expect(stepLines.contains(where: { $0.contains("Step 8 of 8") && $0.contains("Select Profile") }))
        #expect(singleProfilePromptBlock.contains("Mode: single stage"))
        #expect(singleProfilePromptBlock.contains("Stage: thunderbolt"))
        #expect(singleProfilePromptBlock.contains("Media folder:"))
        #expect(singleProfilePromptBlock.contains("Workers:"))
        #expect(!singleProfilePromptBlock.contains("Sweep mode:"))
        #expect(!output.contains("Step 6b"))
    }

    @Test("run plan output shows thunderbolt execution settings truthfully")
    func runPlanOutputShowsThunderboltExecutionSettingsTruthfully() throws {
        let repoRoot = try Self.resolveRepoRoot()
        let mediaFolder = try Self.makeWizardMediaFolder()
        defer { try? FileManager.default.removeItem(at: mediaFolder) }

        let script = [
            "5", // Run mode: single stage
            "6", // Stage: thunderbolt
            mediaFolder.path,
            "",  // Workers: keep defaults
            "2", // Thunderbolt run type: showdown
            "",  // Slot overrides: use defaults
            "",  // CA model mode: keep default
            "",  // Prior update policy: keep default off
            "",  // Showdown scope: keep default all profiles
            "4", // Run plan action: exit
        ].joined(separator: "\n") + "\n"

        let result = try Self.runWizard(
            repoRoot: repoRoot,
            input: script,
            environment: ["TB_WORKERS": "127.0.0.1:1"]
        )
        #expect(result.status == 0)

        let output = Self.stripANSI(result.stdout + "\n" + result.stderr)
        let runPlan = Self.lastRunPlanBlock(from: output)
        #expect(runPlan.contains("Workers"))
        #expect(runPlan.contains("Thunderbolt mode"))
        #expect(runPlan.contains("CA model mode"))
        #expect(runPlan.contains("Model update"))
        #expect(runPlan.contains("CA profiles"))
        #expect(runPlan.contains("CA slot overrides"))
        #expect(!runPlan.contains("Sweep mode"))
        #expect(!runPlan.contains("Policy order"))
        #expect(!runPlan.contains("Workers source"))
        #expect(!runPlan.contains("TB mode"))
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

    private static func runWizard(
        repoRoot: String,
        input: String,
        environment: [String: String]
    ) throws -> CommandResult {
        if let executable = benchmarkExecutable(repoRoot: repoRoot) {
            return try runCommand(
                executable: executable,
                arguments: ["--wizard"],
                workingDirectory: repoRoot,
                input: input,
                environment: environment
            )
        }

        return try runCommand(
            executable: "/usr/bin/swift",
            arguments: ["run", "benchmark", "--wizard"],
            workingDirectory: repoRoot,
            input: input,
            environment: environment
        )
    }

    private static func runCommand(
        executable: String,
        arguments: [String],
        workingDirectory: String,
        input: String,
        environment: [String: String]
    ) throws -> CommandResult {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        process.currentDirectoryURL = URL(fileURLWithPath: workingDirectory)

        var processEnvironment = ProcessInfo.processInfo.environment
        for (key, value) in environment {
            processEnvironment[key] = value
        }
        process.environment = processEnvironment

        let stdinPipe = Pipe()
        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardInput = stdinPipe
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        do {
            try process.run()
        } catch {
            throw WizardTestError.processLaunchFailed("\(executable) \(arguments.joined(separator: " "))")
        }

        if let data = input.data(using: .utf8) {
            stdinPipe.fileHandleForWriting.write(data)
        }
        try? stdinPipe.fileHandleForWriting.close()

        process.waitUntilExit()

        let stdoutData = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        let stderrData = stderrPipe.fileHandleForReading.readDataToEndOfFile()

        return CommandResult(
            status: process.terminationStatus,
            stdout: String(data: stdoutData, encoding: .utf8) ?? "",
            stderr: String(data: stderrData, encoding: .utf8) ?? ""
        )
    }

    private static func makeWizardMediaFolder() throws -> URL {
        let folder = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-wizard-media-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        let video = folder.appendingPathComponent("video.mp4")
        try Data("wizard-test-video".utf8).write(to: video)
        return folder
    }

    private static func stripANSI(_ value: String) -> String {
        value.replacing(/\x1b\[[0-9;]*m/, with: "")
    }

    private static func extractStepLines(from value: String) -> [String] {
        value
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { $0.contains("Step ") }
    }

    private static func lastRunPlanBlock(from value: String) -> String {
        guard let runPlanStart = value.range(of: "Run Plan", options: .backwards) else {
            return value
        }

        let tail = value[runPlanStart.lowerBound...]
        if let actionStart = tail.range(of: "Choose an action") {
            return String(tail[..<actionStart.lowerBound])
        }
        return String(tail)
    }

    private static func lastRedrawBlock(containingAll needles: [String], in value: String) -> String {
        for block in value.components(separatedBy: "kiko-media benchmarks").reversed() {
            if needles.allSatisfy({ block.contains($0) }) {
                return "kiko-media benchmarks" + block
            }
        }
        return value
    }
}
