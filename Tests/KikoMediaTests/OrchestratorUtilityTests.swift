import CryptoKit
import Foundation
import Testing
@testable import KikoMediaCore
@testable import Orchestrator

@Suite("Orchestrator validation networking")
struct OrchestratorValidationNetworkingTests {
    @Test("validation helpers enforce local format contracts")
    func validationHelpersEnforceLocalFormatContracts() {
        #expect(validateDomain("example.com"))
        #expect(validateDomain("photos.event.example.org"))
        #expect(!validateDomain("localhost"))
        #expect(!validateDomain("-bad.example.com"))

        #expect(validateIPv4("127.0.0.1"))
        #expect(!validateIPv4("999.0.0.1"))

        #expect(validateCloudflareTokenFormat(String(repeating: "a", count: 20)))
        #expect(!validateCloudflareTokenFormat("short-token"))
        #expect(!validateCloudflareTokenFormat("token with spaces"))

        #expect(validateSessionHmacSecret(String(repeating: "s", count: 32)))
        #expect(!validateSessionHmacSecret(String(repeating: "s", count: 31)))

        #expect(validateGateSecret("12345678"))
        #expect(!validateGateSecret("1234567"))
    }
}

@Suite("Orchestrator advanced config validation")
struct OrchestratorAdvancedConfigValidationTests {
    @Test("scheduler policy validation and normalization use canonical values")
    func schedulerPolicyValidationAndNormalizationUseCanonicalValues() {
        let spec = DefaultSpec(
            type: "string",
            defaultValue: "auto",
            min: "",
            max: "",
            label: "Video scheduler policy",
            description: "Production video scheduling policy"
        )

        #expect(validateSpec(" FIFO ", envVar: "VIDEO_SCHEDULER_POLICY", spec: spec))
        #expect(!validateSpec("balanced", envVar: "VIDEO_SCHEDULER_POLICY", spec: spec))
        #expect(normalizedAdvancedValue(" FIFO \n", for: "VIDEO_SCHEDULER_POLICY") == VideoSchedulerPolicy.fifo.rawValue)
    }
}

@Suite("Orchestrator file generation utilities")
struct OrchestratorFileGenerationUtilityTests {
    @Test("processTemplate escapes XML values and rejects unreplaced placeholders")
    func processTemplateEscapesXMLValuesAndRejectsUnreplacedPlaceholders() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-filegen-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let templatePath = tempDir.appendingPathComponent("template.txt").path
        try "value=__VALUE__".write(toFile: templatePath, atomically: true, encoding: .utf8)

        let rendered = try processTemplate(
            at: templatePath,
            replacements: ["__VALUE__": "a&b<c>\"'"],
            xmlEscapeValues: true
        )

        #expect(rendered == "value=a&amp;b&lt;c&gt;&quot;&apos;")
        try assertNoUnreplacedPlaceholders(rendered, in: templatePath)
        #expect(throws: SetupWizardError.self) {
            try assertNoUnreplacedPlaceholders("left=__MISSING__", in: templatePath)
        }
    }

    @Test("csp hash helper hashes a single inline tag and rejects ambiguous matches")
    func cspHashHelperRejectsAmbiguousInlineTags() throws {
        let html = "<style>body{color:red;}</style><script>console.log('x')</script>"
        let styleHash = try cspHashForInlineTag("style", in: html)
        let expectedStyleHash = "sha256-\(Data(SHA256.hash(data: Data("body{color:red;}".utf8))).base64EncodedString())"

        #expect(styleHash == expectedStyleHash)
        #expect(throws: SetupWizardError.self) {
            try cspHashForInlineTag("style", in: "<style>a</style><style>b</style>")
        }
    }

    @Test("required advanced value helpers enforce key presence")
    func requiredAdvancedValueHelpersEnforceKeyPresence() throws {
        #expect(try requiredAdvancedValue("BASE_DIRECTORY", from: ["BASE_DIRECTORY": "/tmp/kiko"]) == "/tmp/kiko")

        let selected = try requiredAdvancedValues(
            ["A", "B"],
            from: ["A": "1", "B": "2"]
        )
        #expect(selected["A"] == "1")
        #expect(selected["B"] == "2")

        #expect(throws: SetupWizardError.self) {
            _ = try requiredAdvancedValue("MISSING", from: [:])
        }
    }
}

@Suite("Orchestrator dependency install utilities")
struct OrchestratorDependencyInstallTests {
    private final class StreamingWriterStub {
        var writes: [(String, String)] = []

        func write(_ text: String, _ terminator: String) {
            writes.append((text, terminator))
        }

        var combinedOutput: String {
            writes.map { $0.0 + $0.1 }.joined()
        }
    }

    @Test("process runners capture stdout stderr and status codes")
    func processRunnersCaptureStdoutStderrAndStatusCodes() {
        let success = runProcess(executable: "/bin/sh", arguments: ["-c", "printf 'ok'"])
        #expect(success.exitCode == 0)
        #expect(success.output == "ok")

        let failure = runProcess(
            executable: "/bin/sh",
            arguments: ["-c", "printf 'out'; printf 'err' 1>&2; exit 7"]
        )
        #expect(failure.exitCode == 7)
        #expect(failure.output.contains("out"))
        #expect(failure.output.contains("err"))

        let stdoutOnly = runProcessStdoutOnly(
            executable: "/bin/sh",
            arguments: ["-c", "printf 'stdout'; printf 'stderr' 1>&2"]
        )
        #expect(stdoutOnly.exitCode == 0)
        #expect(stdoutOnly.output == "stdout")
    }

    @Test("process runners pass environment overrides to child processes")
    func processRunnersPassEnvironmentOverridesToChildProcesses() {
        let baseDirectoryOverride = "/tmp/kiko-env-\(UUID().uuidString)"

        let envPassthrough = runProcess(
            executable: "/bin/sh",
            arguments: ["-c", "test -n \"$PATH\"; printenv BASE_DIRECTORY"],
            environmentOverrides: ["BASE_DIRECTORY": baseDirectoryOverride]
        )
        #expect(envPassthrough.exitCode == 0)
        #expect(envPassthrough.output == baseDirectoryOverride)

        let envPassthroughStdoutOnly = runProcessStdoutOnly(
            executable: "/bin/sh",
            arguments: ["-c", "test -n \"$PATH\"; printenv BASE_DIRECTORY"],
            environmentOverrides: ["BASE_DIRECTORY": baseDirectoryOverride]
        )
        #expect(envPassthroughStdoutOnly.exitCode == 0)
        #expect(envPassthroughStdoutOnly.output == baseDirectoryOverride)
    }

    @Test("streaming process runner emits formatted lines and flushes trailing output")
    func streamingProcessRunnerEmitsFormattedLinesAndFlushesTrailingOutput() {
        let writer = StreamingWriterStub()
        let streamed = runProcessStreaming(
            executable: "/bin/sh",
            arguments: ["-c", "printf 'line-one\\n'; printf 'line-two' 1>&2; printf '\\npartial'"],
            writer: { writer.write($0, $1) }
        )

        #expect(streamed == 0)
        #expect(
            writer.combinedOutput
                == "    \(dim)line-one\(reset)\n    \(dim)line-two\(reset)\n    \(dim)partial\(reset)\n"
        )
    }
}
