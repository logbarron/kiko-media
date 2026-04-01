import Foundation
import Dispatch
import Darwin
import Security
import KikoMediaCore

// MARK: - Repo Root Discovery + Invocation

let repoRootSentinelPaths = [
    "Package.swift",
    "deploy/Caddyfile.template",
]

func isRepoRootDirectory(_ path: String, fileManager: FileManager = .default) -> Bool {
    repoRootSentinelPaths.allSatisfy { sentinel in
        fileManager.fileExists(atPath: "\(path)/\(sentinel)")
    }
}

func normalizeSearchDirectory(_ rawPath: String, fileManager: FileManager = .default) -> String? {
    let trimmed = rawPath.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return nil }

    let expanded = expandTildePath(trimmed)
    let absoluteURL: URL
    if expanded.hasPrefix("/") {
        absoluteURL = URL(fileURLWithPath: expanded)
    } else {
        absoluteURL = URL(fileURLWithPath: expanded, relativeTo: URL(fileURLWithPath: fileManager.currentDirectoryPath))
    }
    let absolutePath = absoluteURL.standardizedFileURL.path

    var isDirectory: ObjCBool = false
    if fileManager.fileExists(atPath: absolutePath, isDirectory: &isDirectory), isDirectory.boolValue {
        return absolutePath
    }
    return URL(fileURLWithPath: absolutePath).deletingLastPathComponent().path
}

func discoverRepoRoot(startingAt directory: String, fileManager: FileManager = .default) -> String? {
    var current = URL(fileURLWithPath: directory).standardizedFileURL
    while true {
        let path = current.path
        if isRepoRootDirectory(path, fileManager: fileManager) {
            return path
        }

        let parent = current.deletingLastPathComponent()
        if parent.path == path {
            return nil
        }
        current = parent
    }
}

func discoverRepoRoot() -> String? {
    let fm = FileManager.default
    var candidates: [String] = [fm.currentDirectoryPath]
    if let arg0 = CommandLine.arguments.first {
        candidates.append(arg0)
    }
    if let executablePath = Bundle.main.executablePath {
        candidates.append(executablePath)
    }
    // Fallback only; primary discovery is sentinel-based.
    candidates.append(#filePath)

    var seen = Set<String>()
    for rawCandidate in candidates {
        guard let directory = normalizeSearchDirectory(rawCandidate, fileManager: fm) else {
            continue
        }
        if !seen.insert(directory).inserted {
            continue
        }
        if let root = discoverRepoRoot(startingAt: directory, fileManager: fm) {
            return root
        }
    }

    return nil
}

func setupInvocationBase(repoRoot _: String?) -> String {
    "swift run orchestrator"
}

func repoRootDiscoveryErrorMessage(attemptedCommand: String?) -> String {
    guard let attemptedCommand, !attemptedCommand.isEmpty else {
        return "Cannot find kiko-media repo root."
    }
    return "Cannot find kiko-media repo root for command: \(attemptedCommand)"
}


// MARK: - Entry Point

func resolveHomeAndRepoRoot(attemptedCommand: String? = nil) -> (home: String, repoRoot: String) {
    let fm = FileManager.default
    let home = fm.homeDirectoryForCurrentUser.path

    guard let repoRoot = discoverRepoRoot() else {
        printError(repoRootDiscoveryErrorMessage(attemptedCommand: attemptedCommand))
        printHint("Run from inside the repo and invoke with: swift run orchestrator")
        exit(1)
    }

    return (home: home, repoRoot: repoRoot)
}

runSetupCLIOrWizard()
