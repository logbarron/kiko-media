import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Terminal UI Primitives")
struct TerminalUIPrimitivesTests {
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

    @Test("confirm retries invalid input and accepts explicit yes/no")
    func confirmRetriesInvalidInput() {
        let io = IOStub(inputs: ["maybe", "YeS"])
        let accepted = TerminalUIPrimitives.confirm(
            "Proceed",
            readInput: { io.read() },
            writer: { io.write($0, $1) }
        )

        #expect(accepted)
        #expect(io.combinedOutput.contains("Enter y or n."))
    }

    @Test("confirm supports enter-to-default when default is provided")
    func confirmDefaultHandling() {
        let defaultNo = IOStub(inputs: [""])
        let rejected = TerminalUIPrimitives.confirm(
            "Include archive stage",
            defaultYes: false,
            readInput: { defaultNo.read() },
            writer: { defaultNo.write($0, $1) }
        )
        #expect(!rejected)
        #expect(defaultNo.combinedOutput.contains(" (y/N)"))

        let defaultYes = IOStub(inputs: [""])
        let accepted = TerminalUIPrimitives.confirm(
            "Keep trying",
            defaultYes: true,
            readInput: { defaultYes.read() },
            writer: { defaultYes.write($0, $1) }
        )
        #expect(accepted)
        #expect(defaultYes.combinedOutput.contains(" (Y/n)"))
    }

    @Test("style helpers preserve success error and hint formatting")
    func styleHelperFormatting() {
        #expect(
            TerminalUIPrimitives.formatSuccessLine("done")
                == "  \(TerminalUIPrimitives.green)✓\(TerminalUIPrimitives.reset) done"
        )
        #expect(
            TerminalUIPrimitives.formatErrorLine("failed")
                == "  \(TerminalUIPrimitives.red)✗\(TerminalUIPrimitives.reset) failed"
        )
        #expect(
            TerminalUIPrimitives.formatHintLine("tip")
                == "  \(TerminalUIPrimitives.dim)tip\(TerminalUIPrimitives.reset)"
        )
    }

    @Test("action menu formatting helper matches shared layout")
    func actionMenuFormattingHelper() {
        let lines = TerminalUIPrimitives.formatActionMenuLines(
            2,
            title: "Use defaults",
            detail: "Skip advanced editing."
        )

        #expect(
            lines.titleLine
                == "\(TerminalUIPrimitives.listItemIndent)\(TerminalUIPrimitives.bold)2.\(TerminalUIPrimitives.reset) Use defaults"
        )
        #expect(
            lines.detailLine
                == "\(TerminalUIPrimitives.listDetailIndent)\(TerminalUIPrimitives.dim)Skip advanced editing.\(TerminalUIPrimitives.reset)"
        )
    }

    @Test("path normalization unescapes drag-drop input and expands tilde")
    func pathNormalization() {
        let home = FileManager.default.homeDirectoryForCurrentUser.path
        let normalized = TerminalUIPrimitives.normalizePathInput("~/My\\ Folder/Bench\\ Data")
        #expect(normalized == "\(home)/My Folder/Bench Data")
    }
}
