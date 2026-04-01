import Darwin
import Foundation

enum WipeError: LocalizedError {
    case missingPlist(String)
    case invalidPlist(String)
    case missingExternalSSDPath
    case missingSSDDirectory(String)
    case unsafeSSDPath(String)
    case serviceStopFailed(String, String)

    var errorDescription: String? {
        switch self {
        case .missingPlist(let path):
            return "Missing LaunchAgent plist: \(path)"
        case .invalidPlist(let path):
            return "Could not read EnvironmentVariables from \(path)"
        case .missingExternalSSDPath:
            return "EXTERNAL_SSD_PATH is missing from com.kiko.media.plist"
        case .missingSSDDirectory(let path):
            return "Configured SSD archive directory is missing: \(path)"
        case .unsafeSSDPath(let path):
            return "Refusing to wipe unsafe SSD path: \(path)"
        case .serviceStopFailed(let service, let reason):
            return "Failed to stop \(service): \(reason)"
        }
    }
}

struct RuntimePaths {
    let baseDir: String
    let ssdPath: String
}

let fileManager = FileManager.default
let home = fileManager.homeDirectoryForCurrentUser.path
let mediaPlistPath = "\(home)/Library/LaunchAgents/com.kiko.media.plist"

func expandTilde(_ path: String) -> String {
    NSString(string: path).expandingTildeInPath
}

func loadEnvironment(from plistPath: String) throws -> [String: String] {
    guard fileManager.fileExists(atPath: plistPath) else {
        throw WipeError.missingPlist(plistPath)
    }

    let data = try Data(contentsOf: URL(fileURLWithPath: plistPath))
    var format = PropertyListSerialization.PropertyListFormat.xml
    guard let root = try PropertyListSerialization.propertyList(
        from: data,
        options: [],
        format: &format
    ) as? [String: Any],
    let env = root["EnvironmentVariables"] as? [String: Any] else {
        throw WipeError.invalidPlist(plistPath)
    }

    var resolved: [String: String] = [:]
    resolved.reserveCapacity(env.count)
    for (key, value) in env {
        if let string = value as? String {
            resolved[key] = string
        }
    }
    return resolved
}

func resolveRuntimePaths() throws -> RuntimePaths {
    let env = try loadEnvironment(from: mediaPlistPath)

    let baseDirRaw = env["BASE_DIRECTORY"]?.trimmingCharacters(in: .whitespacesAndNewlines)
    let baseDir = expandTilde(baseDirRaw?.isEmpty == false ? baseDirRaw! : "~/Documents/kiko-media")

    guard let ssdRaw = env["EXTERNAL_SSD_PATH"]?.trimmingCharacters(in: .whitespacesAndNewlines),
          !ssdRaw.isEmpty else {
        throw WipeError.missingExternalSSDPath
    }

    let ssdPath = expandTilde(ssdRaw)
    let components = URL(fileURLWithPath: ssdPath).standardizedFileURL.path
        .split(separator: "/", omittingEmptySubsequences: true)
    guard components.count >= 3, components.first == "Volumes" else {
        throw WipeError.unsafeSSDPath(ssdPath)
    }

    var isDirectory: ObjCBool = false
    guard fileManager.fileExists(atPath: ssdPath, isDirectory: &isDirectory), isDirectory.boolValue else {
        throw WipeError.missingSSDDirectory(ssdPath)
    }

    return RuntimePaths(baseDir: baseDir, ssdPath: ssdPath)
}

@discardableResult
func runProcess(_ executable: String, _ arguments: [String]) -> (status: Int32, output: String) {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = arguments

    let pipe = Pipe()
    process.standardOutput = pipe
    process.standardError = pipe

    do {
        try process.run()
    } catch {
        return (1, error.localizedDescription)
    }

    process.waitUntilExit()
    let data = pipe.fileHandleForReading.readDataToEndOfFile()
    let output = String(data: data, encoding: .utf8) ?? ""
    return (process.terminationStatus, output)
}

func launchAgentPath(_ plist: String) -> String {
    "\(home)/Library/LaunchAgents/\(plist)"
}

func serviceIsLoaded(_ label: String) -> Bool {
    let uid = getuid()
    let result = runProcess("/bin/launchctl", ["print", "gui/\(uid)/\(label)"])
    return result.status == 0
}

func stopService(label: String, plist: String) throws {
    guard serviceIsLoaded(label) else { return }

    let uid = getuid()
    let result = runProcess(
        "/bin/launchctl",
        ["bootout", "gui/\(uid)", launchAgentPath(plist)]
    )
    guard result.status == 0 else {
        let summary = result.output.split(separator: "\n").first.map(String.init) ?? "launchctl bootout failed"
        throw WipeError.serviceStopFailed(label, summary)
    }
}

func stopServices() throws {
    try stopService(label: "com.kiko.media", plist: "com.kiko.media.plist")
    try stopService(label: "com.kiko.tusd", plist: "com.kiko.tusd.plist")
    try stopService(label: "com.kiko.caddy", plist: "com.kiko.caddy.plist")
}

func clearDirectoryIfPresent(_ path: String) throws {
    var isDirectory: ObjCBool = false
    guard fileManager.fileExists(atPath: path, isDirectory: &isDirectory) else { return }
    guard isDirectory.boolValue else { return }

    let entries = try fileManager.contentsOfDirectory(atPath: path)
    for entry in entries {
        try fileManager.removeItem(atPath: "\(path)/\(entry)")
    }
}

func clearRequiredDirectory(_ path: String) throws {
    var isDirectory: ObjCBool = false
    guard fileManager.fileExists(atPath: path, isDirectory: &isDirectory), isDirectory.boolValue else {
        throw WipeError.missingSSDDirectory(path)
    }
    try clearDirectoryIfPresent(path)
}

func removeFileIfPresent(_ path: String) throws {
    guard fileManager.fileExists(atPath: path) else { return }
    try fileManager.removeItem(atPath: path)
}

func wipeMedia(at paths: RuntimePaths) throws {
    try clearDirectoryIfPresent("\(paths.baseDir)/uploads")
    try clearDirectoryIfPresent("\(paths.baseDir)/thumbs")
    try clearDirectoryIfPresent("\(paths.baseDir)/previews")
    try clearDirectoryIfPresent("\(paths.baseDir)/moderated")
    try removeFileIfPresent("\(paths.baseDir)/metadata.db")
    try removeFileIfPresent("\(paths.baseDir)/metadata.db-wal")
    try removeFileIfPresent("\(paths.baseDir)/metadata.db-shm")
    try clearRequiredDirectory(paths.ssdPath)
}

do {
    let paths = try resolveRuntimePaths()

    print("This will stop kiko-media services and permanently remove test media from:")
    print("  \(paths.baseDir)/uploads")
    print("  \(paths.baseDir)/thumbs")
    print("  \(paths.baseDir)/previews")
    print("  \(paths.baseDir)/moderated")
    print("  \(paths.baseDir)/metadata.db*")
    print("  \(paths.ssdPath)")
    print()
    print("Logs, config files, plists, and deploy artifacts are preserved.")
    print("Type WIPE to continue:", terminator: " ")
    fflush(stdout)

    let confirmation = readLine(strippingNewline: true)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    guard confirmation == "WIPE" else {
        fputs("Aborted.\n", stderr)
        exit(1)
    }

    try stopServices()
    try wipeMedia(at: paths)
    print("Media wipe complete. Services remain stopped.")
} catch {
    fputs("Media wipe failed: \(error.localizedDescription)\n", stderr)
    exit(1)
}
