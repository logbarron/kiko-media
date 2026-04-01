#!/usr/bin/env swift
import Foundation

struct DefaultsEntry {
    let lineNumber: Int
    let key: String
    let type: String
    let defaultValue: String
    let min: String
    let max: String
}

enum GeneratorError: Error, CustomStringConvertible {
    case cannotLocateRepositoryRoot
    case cannotReadDefaultsEnv(path: String)
    case malformedLine(lineNumber: Int, line: String)
    case duplicateKey(lineNumber: Int, key: String)
    case invalidInt(lineNumber: Int, key: String, value: String)
    case invalidDouble(lineNumber: Int, key: String, value: String)

    var description: String {
        switch self {
        case .cannotLocateRepositoryRoot:
            return "Could not locate repository root (expected Package.swift and deploy/defaults.env)."
        case let .cannotReadDefaultsEnv(path):
            return "Could not read defaults file at \(path)."
        case let .malformedLine(lineNumber, line):
            return "Malformed defaults.env line \(lineNumber): \(line)"
        case let .duplicateKey(lineNumber, key):
            return "Duplicate defaults.env key '\(key)' at line \(lineNumber)."
        case let .invalidInt(lineNumber, key, value):
            return "Invalid int value for key '\(key)' at line \(lineNumber): \(value)"
        case let .invalidDouble(lineNumber, key, value):
            return "Invalid double value for key '\(key)' at line \(lineNumber): \(value)"
        }
    }
}

private func locateRepositoryRoot(startingAt url: URL) -> URL? {
    var current = url
    let fm = FileManager.default
    while true {
        let packagePath = current.appendingPathComponent("Package.swift").path
        let defaultsPath = current.appendingPathComponent("deploy/defaults.env").path
        if fm.fileExists(atPath: packagePath), fm.fileExists(atPath: defaultsPath) {
            return current
        }
        let parent = current.deletingLastPathComponent()
        if parent.path == current.path {
            return nil
        }
        current = parent
    }
}

private func parseDefaultsFile(at path: String) throws -> [DefaultsEntry] {
    guard let content = try? String(contentsOfFile: path, encoding: .utf8) else {
        throw GeneratorError.cannotReadDefaultsEnv(path: path)
    }

    var entries: [DefaultsEntry] = []
    var seenKeys = Set<String>()

    for (index, rawLine) in content.split(separator: "\n", omittingEmptySubsequences: false).enumerated() {
        let lineNumber = index + 1
        let line = String(rawLine)
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        if trimmed.isEmpty || trimmed.hasPrefix("#") {
            continue
        }

        let parts = trimmed.split(separator: "|", maxSplits: 6, omittingEmptySubsequences: false).map(String.init)
        guard parts.count == 7 else {
            throw GeneratorError.malformedLine(lineNumber: lineNumber, line: line)
        }
        let key = parts[0]
        guard !seenKeys.contains(key) else {
            throw GeneratorError.duplicateKey(lineNumber: lineNumber, key: key)
        }
        seenKeys.insert(key)
        entries.append(
            DefaultsEntry(
                lineNumber: lineNumber,
                key: key,
                type: parts[1],
                defaultValue: parts[2],
                min: parts[3],
                max: parts[4]
            )
        )
    }

    return entries
}

private func escapedSwiftString(_ value: String) -> String {
    value
        .replacingOccurrences(of: "\\", with: "\\\\")
        .replacingOccurrences(of: "\"", with: "\\\"")
}

private func formatRange(min: String, max: String) -> String {
    if min.isEmpty || max.isEmpty {
        return "nil"
    }
    return "\(min)...\(max)"
}

private func renderSource(entries: [DefaultsEntry]) throws -> String {
    var intEntries: [String] = []
    var doubleEntries: [String] = []
    var stringEntries: [String] = []

    for entry in entries {
        let lineNumber = entry.lineNumber
        switch entry.type {
        case "int":
            guard let fallback = Int(entry.defaultValue) else {
                throw GeneratorError.invalidInt(lineNumber: lineNumber, key: entry.key, value: entry.defaultValue)
            }
            if !entry.min.isEmpty, Int(entry.min) == nil {
                throw GeneratorError.invalidInt(lineNumber: lineNumber, key: entry.key, value: entry.min)
            }
            if !entry.max.isEmpty, Int(entry.max) == nil {
                throw GeneratorError.invalidInt(lineNumber: lineNumber, key: entry.key, value: entry.max)
            }
            intEntries.append(
                "        \"\(entry.key)\": IntSpec(fallback: \(fallback), range: \(formatRange(min: entry.min, max: entry.max))),"
            )
        case "double":
            guard Double(entry.defaultValue) != nil else {
                throw GeneratorError.invalidDouble(lineNumber: lineNumber, key: entry.key, value: entry.defaultValue)
            }
            if !entry.min.isEmpty, Double(entry.min) == nil {
                throw GeneratorError.invalidDouble(lineNumber: lineNumber, key: entry.key, value: entry.min)
            }
            if !entry.max.isEmpty, Double(entry.max) == nil {
                throw GeneratorError.invalidDouble(lineNumber: lineNumber, key: entry.key, value: entry.max)
            }
            doubleEntries.append(
                "        \"\(entry.key)\": DoubleSpec(fallback: \(entry.defaultValue), range: \(formatRange(min: entry.min, max: entry.max))),"
            )
        case "string":
            stringEntries.append(
                "        \"\(entry.key)\": \"\(escapedSwiftString(entry.defaultValue))\","
            )
        default:
            throw GeneratorError.malformedLine(lineNumber: lineNumber, line: "\(entry.key)|\(entry.type)|...")
        }
    }

    var lines: [String] = []
    lines.append("// This file is generated by scripts/generate_config_defaults.swift from deploy/defaults.env.")
    lines.append("// Do not edit this file manually.")
    lines.append("import Foundation")
    lines.append("")
    lines.append("extension Config {")
    lines.append("    package static let intDefaults: [String: IntSpec] = [")
    lines.append(contentsOf: intEntries)
    lines.append("    ]")
    lines.append("")
    lines.append("    package static let doubleDefaults: [String: DoubleSpec] = [")
    lines.append(contentsOf: doubleEntries)
    lines.append("    ]")
    lines.append("")
    lines.append("    package static let stringDefaults: [String: String] = [")
    lines.append(contentsOf: stringEntries)
    lines.append("    ]")
    lines.append("}")
    lines.append("")
    return lines.joined(separator: "\n")
}

let shouldCheckOnly = CommandLine.arguments.contains("--check")

do {
    let currentDirectoryURL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
    guard let repoRoot = locateRepositoryRoot(startingAt: currentDirectoryURL) else {
        throw GeneratorError.cannotLocateRepositoryRoot
    }

    let defaultsPath = repoRoot.appendingPathComponent("deploy/defaults.env").path
    let outputPath = repoRoot.appendingPathComponent("Sources/KikoMediaCore/ConfigDefaults.generated.swift").path

    let entries = try parseDefaultsFile(at: defaultsPath)
    let source = try renderSource(entries: entries)

    if shouldCheckOnly {
        let current = (try? String(contentsOfFile: outputPath, encoding: .utf8)) ?? ""
        if current == source {
            print("Config defaults generated file is up to date.")
            exit(0)
        }
        fputs("Config defaults generated file is out of date. Run: swift scripts/generate_config_defaults.swift\n", stderr)
        exit(1)
    }

    let current = try? String(contentsOfFile: outputPath, encoding: .utf8)
    if current != source {
        try source.write(toFile: outputPath, atomically: true, encoding: .utf8)
        print("Wrote \(outputPath)")
    } else {
        print("No changes needed (\(outputPath) is already up to date).")
    }
} catch {
    fputs("Error: \(error)\n", stderr)
    exit(1)
}
