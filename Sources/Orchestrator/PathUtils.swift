import Foundation
import KikoMediaCore

func expandTildePath(_ path: String) -> String {
    NSString(string: path).expandingTildeInPath
}

func normalizePathInput(_ raw: String) -> String {
    TerminalUIPrimitives.normalizePathInput(raw)
}
