import Foundation
import Testing
@testable import KikoMediaCore

@Suite("External SSD Picker")
struct ExternalSSDPickerTests {
    private final class IOStub {
        var inputs: [String]
        var writes: [(String, String)] = []

        init(inputs: [String]) {
            self.inputs = inputs
        }

        func read() -> String? {
            guard !inputs.isEmpty else { return nil }
            return inputs.removeFirst()
        }

        func write(_ text: String, _ terminator: String) {
            writes.append((text, terminator))
        }

        var combinedOutput: String {
            writes.map { $0.0 + $0.1 }.joined()
        }
    }

    @Test("orchestrator semantics: selecting a drive resolves to /originals")
    func orchestratorSelectionAppendsOriginals() {
        let io = IOStub(inputs: ["1"])

        let selectedPath = TerminalUIPrimitives.pickExternalSSDPath(
            volumes: [URL(fileURLWithPath: "/Volumes/EventSSD", isDirectory: true)],
            formatBytes: { _ in "500 GB" },
            manualPathPrompt: "SSD path",
            manualPathExamples: ["/Volumes/MySSD/originals", "/Volumes/MyDrive/originals"],
            selectionHint: "An /originals folder will be created on the drive you pick.",
            selectedVolumePath: { $0.appendingPathComponent("originals").path },
            readInput: { io.read() },
            writer: { io.write($0, $1) }
        )

        #expect(selectedPath == "/Volumes/EventSSD/originals")
        #expect(io.combinedOutput.contains("Which drive"))
        #expect(!io.combinedOutput.contains("Press Enter to use"))
    }

    @Test("benchmark semantics: selecting a drive resolves to drive path")
    func benchmarkSelectionUsesDrivePath() {
        let io = IOStub(inputs: ["1"])

        let selectedPath = TerminalUIPrimitives.pickExternalSSDPath(
            volumes: [URL(fileURLWithPath: "/Volumes/BenchSSD", isDirectory: true)],
            formatBytes: { _ in "1 TB" },
            manualPathPrompt: "SSD path",
            manualPathExamples: ["/Volumes/MySSD", "/Volumes/MyDrive/bench-folder"],
            selectionHint: "Bench artifacts are written under <ssd path>/bench-results/<run-id>/ and removed after the run.",
            selectedVolumePath: { $0.path },
            readInput: { io.read() },
            writer: { io.write($0, $1) }
        )

        #expect(selectedPath == "/Volumes/BenchSSD")
        #expect(io.combinedOutput.contains("bench-results/<run-id>"))
        #expect(!io.combinedOutput.contains("Press Enter to use"))
    }

    @Test("manual entry path is required and normalized")
    func manualPathIsRequiredAndNormalized() {
        let io = IOStub(inputs: ["", "~/Bench\\ SSD"])
        let home = FileManager.default.homeDirectoryForCurrentUser.path

        let selectedPath = TerminalUIPrimitives.pickExternalSSDPath(
            volumes: [],
            formatBytes: { _ in "0 GB" },
            manualPathPrompt: "SSD path",
            manualPathExamples: ["/Volumes/MySSD", "/Volumes/MyDrive/bench-folder"],
            selectionHint: nil,
            selectedVolumePath: { $0.path },
            readInput: { io.read() },
            writer: { io.write($0, $1) }
        )

        #expect(selectedPath == "\(home)/Bench SSD")
        #expect(io.combinedOutput.contains("No external volumes detected."))
        #expect(io.combinedOutput.contains("This field is required."))
    }
}
