import Foundation

// MARK: - Benchmark Utilities (used by Steps/, Pipeline/, and guardrails)

actor CPUCollector {
    private var values: [Double] = []

    func add(_ v: Double) {
        values.append(v)
    }

    func snapshot() -> [Double] {
        values
    }
}

struct SSDBenchmarkArtifactsPath: Sendable {
    let ssdBase: String
    let resultsDir: String
    let runDir: String
    let benchDir: String
    let runId: String
    let leaf: String
}

func makeBenchmarkRunID() -> String {
    ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "-")
}

func makeSSDBenchmarkArtifactsPath(
    ssdBase: String,
    leaf: String,
    runId: String = makeBenchmarkRunID()
) -> SSDBenchmarkArtifactsPath {
    let normalizedBase = URL(fileURLWithPath: ssdBase, isDirectory: true).standardizedFileURL.path
    let normalizedLeaf = (leaf as NSString).lastPathComponent
    let resultsDir = "\(normalizedBase)/bench-results"
    let runDir = "\(resultsDir)/\(runId)"
    let benchDir = "\(runDir)/\(normalizedLeaf)"
    return SSDBenchmarkArtifactsPath(
        ssdBase: normalizedBase,
        resultsDir: resultsDir,
        runDir: runDir,
        benchDir: benchDir,
        runId: runId,
        leaf: normalizedLeaf
    )
}

func cleanupSSDBenchmarkArtifacts(_ paths: SSDBenchmarkArtifactsPath) {
    let fm = FileManager.default
    if (paths.benchDir as NSString).lastPathComponent == paths.leaf,
       isDirectChild(path: paths.benchDir, of: paths.runDir) {
        try? fm.removeItem(atPath: paths.benchDir)
    }
    pruneIfEmptyDirectory(paths.runDir, expectedParent: paths.resultsDir, expectedName: paths.runId, fileManager: fm)
    pruneIfEmptyDirectory(paths.resultsDir, expectedParent: paths.ssdBase, expectedName: "bench-results", fileManager: fm)
}

private func pruneIfEmptyDirectory(
    _ path: String,
    expectedParent: String,
    expectedName: String,
    fileManager: FileManager
) {
    guard (path as NSString).lastPathComponent == expectedName else { return }
    guard isDirectChild(path: path, of: expectedParent) else { return }

    var isDir: ObjCBool = false
    guard fileManager.fileExists(atPath: path, isDirectory: &isDir), isDir.boolValue else { return }
    guard let children = try? fileManager.contentsOfDirectory(atPath: path), children.isEmpty else { return }

    try? fileManager.removeItem(atPath: path)
}

private func isDirectChild(path: String, of parent: String) -> Bool {
    let childComponents = URL(fileURLWithPath: path).standardizedFileURL.pathComponents
    let parentComponents = URL(fileURLWithPath: parent).standardizedFileURL.pathComponents
    guard childComponents.count == parentComponents.count + 1 else { return false }
    return zip(parentComponents, childComponents).allSatisfy(==)
}

func makeTempDir(_ prefix: String) -> String {
    let path = NSTemporaryDirectory() + "kiko-bench-\(prefix)-\(UUID().uuidString)"
    try? FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true)
    return path
}

func cleanup(_ path: String) {
    try? FileManager.default.removeItem(atPath: path)
}
