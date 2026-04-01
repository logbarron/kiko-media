import Foundation
import Darwin

typealias ProcessOutputWriter = (_ text: String, _ terminator: String) -> Void

// MARK: - Binary Downloads

private func captureProcessOutput(stdoutPipe: Pipe, stderrPipe: Pipe) -> (stdout: Data, stderr: Data) {
    let stdoutFD = stdoutPipe.fileHandleForReading.fileDescriptor
    let stderrFD = stderrPipe.fileHandleForReading.fileDescriptor

    var stdoutData = Data()
    var stderrData = Data()
    var stdoutOpen = true
    var stderrOpen = true
    var buffer = [UInt8](repeating: 0, count: 4096)

    while stdoutOpen || stderrOpen {
        var descriptors: [pollfd] = []
        if stdoutOpen {
            descriptors.append(
                pollfd(
                    fd: stdoutFD,
                    events: Int16(POLLIN) | Int16(POLLHUP) | Int16(POLLERR),
                    revents: 0
                )
            )
        }
        if stderrOpen {
            descriptors.append(
                pollfd(
                    fd: stderrFD,
                    events: Int16(POLLIN) | Int16(POLLHUP) | Int16(POLLERR),
                    revents: 0
                )
            )
        }
        if descriptors.isEmpty { break }

        let pollResult = descriptors.withUnsafeMutableBufferPointer { ptr in
            poll(ptr.baseAddress, nfds_t(ptr.count), -1)
        }
        if pollResult < 0 {
            if errno == EINTR { continue }
            break
        }

        var index = 0
        if stdoutOpen {
            let events = descriptors[index].revents
            index += 1
            if events != 0 {
                let readCount = Darwin.read(stdoutFD, &buffer, buffer.count)
                if readCount > 0 {
                    stdoutData.append(contentsOf: buffer.prefix(readCount))
                } else if readCount == 0 || (events & (Int16(POLLHUP) | Int16(POLLERR))) != 0 {
                    stdoutOpen = false
                } else if readCount < 0, errno != EINTR {
                    stdoutOpen = false
                }
            }
        }

        if stderrOpen {
            let events = descriptors[index].revents
            if events != 0 {
                let readCount = Darwin.read(stderrFD, &buffer, buffer.count)
                if readCount > 0 {
                    stderrData.append(contentsOf: buffer.prefix(readCount))
                } else if readCount == 0 || (events & (Int16(POLLHUP) | Int16(POLLERR))) != 0 {
                    stderrOpen = false
                } else if readCount < 0, errno != EINTR {
                    stderrOpen = false
                }
            }
        }
    }

    return (stdoutData, stderrData)
}

func runProcess(
    executable: String,
    arguments: [String],
    workingDirectory: String? = nil,
    environmentOverrides: [String: String]? = nil,
    capture: Bool = true
) -> (exitCode: Int32, output: String) {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = arguments
    if let wd = workingDirectory {
        process.currentDirectoryURL = URL(fileURLWithPath: wd)
    }
    if let environmentOverrides, !environmentOverrides.isEmpty {
        process.environment = ProcessInfo.processInfo.environment.merging(environmentOverrides) { _, new in new }
    }

    if capture {
        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe
        do {
            try process.run()
            let captured = captureProcessOutput(stdoutPipe: stdoutPipe, stderrPipe: stderrPipe)
            process.waitUntilExit()
            let stdout = String(data: captured.stdout, encoding: .utf8)?
                .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            let stderr = String(data: captured.stderr, encoding: .utf8)?
                .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            var output = stdout

            // Keep success behavior unchanged; on failure include stderr diagnostics.
            if process.terminationStatus != 0 {
                if output.isEmpty {
                    output = stderr
                } else if !stderr.isEmpty {
                    output += "\n\(stderr)"
                }
            }
            return (process.terminationStatus, output)
        } catch {
            return (-1, "")
        }
    } else {
        do {
            try process.run()
            process.waitUntilExit()
        } catch {
            return (-1, "")
        }
        return (process.terminationStatus, "")
    }
}

func runProcessStdoutOnly(
    executable: String,
    arguments: [String],
    workingDirectory: String? = nil,
    environmentOverrides: [String: String]? = nil
) -> (exitCode: Int32, output: String) {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = arguments
    if let wd = workingDirectory {
        process.currentDirectoryURL = URL(fileURLWithPath: wd)
    }
    if let environmentOverrides, !environmentOverrides.isEmpty {
        process.environment = ProcessInfo.processInfo.environment.merging(environmentOverrides) { _, new in new }
    }

    let stdoutPipe = Pipe()
    process.standardOutput = stdoutPipe

    do {
        try process.run()
        let outData = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        let stdout = String(data: outData, encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return (process.terminationStatus, stdout)
    } catch {
        return (-1, "")
    }
}

@discardableResult
func runProcessStreaming(
    executable: String,
    arguments: [String],
    workingDirectory: String? = nil,
    indent: String = listItemIndent,
    writer: ProcessOutputWriter = { text, terminator in
        Swift.print(text, terminator: terminator)
    }
) -> Int32 {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = arguments
    if let wd = workingDirectory {
        process.currentDirectoryURL = URL(fileURLWithPath: wd)
    }

    // Keep streamed output visually "inside" the wizard UI.
    let pipe = Pipe()
    process.standardOutput = pipe
    process.standardError = pipe

    do {
        try process.run()
    } catch {
        return -1
    }

    let handle = pipe.fileHandleForReading
    var buffer = ""

    while true {
        let data = handle.availableData
        if data.isEmpty { break } // EOF
        guard let chunk = String(data: data, encoding: .utf8), !chunk.isEmpty else { continue }
        buffer += chunk

        // Flush full lines as they arrive.
        while let nl = buffer.firstIndex(of: "\n") {
            let line = String(buffer[..<nl])
            buffer.removeSubrange(...nl)
            writer("\(indent)\(dim)\(line)\(reset)", "\n")
        }
    }

    // Flush any trailing partial line.
    if !buffer.isEmpty {
        writer("\(indent)\(dim)\(buffer)\(reset)", "\n")
    }

    process.waitUntilExit()
    return process.terminationStatus
}

func ensureCaddy(home: String) -> Bool {
    let fm = FileManager.default
    let caddyPath = "\(home)/bin/caddy"

    if fm.isExecutableFile(atPath: caddyPath) {
        let (exitCode, output) = runProcess(executable: caddyPath, arguments: ["version"])
        if exitCode == 0 {
            let version = output.isEmpty ? "installed" : firstLine(output)
            printSuccess("Caddy found: \(version)")
            if !keepOrChange(
                label: "installed Caddy",
                current: version,
                actionHint: "(Enter=keep installed, c=download latest)",
                showCurrentLine: false
            ) {
                return true
            }
        }
    }

    print()
    printHint("Downloading Caddy with Cloudflare DNS plugin...")
    print()

    let (exitCode, _) = runProcess(
        executable: "/usr/bin/curl",
        arguments: ["-fL", "--progress-bar",
                    "https://caddyserver.com/api/download?os=darwin&arch=arm64&p=github.com%2Fcaddy-dns%2Fcloudflare",
                    "-o", caddyPath],
        capture: false
    )

    guard exitCode == 0 else {
        printError("Caddy download failed.")
        printManualCommands("Manual install:", commands: [
            "curl -L \"https://caddyserver.com/api/download?os=darwin&arch=arm64&p=github.com%2Fcaddy-dns%2Fcloudflare\" \\",
            "  -o ~/bin/caddy",
            "chmod +x ~/bin/caddy",
        ])
        return false
    }

    do {
        try fm.setAttributes([.posixPermissions: 0o755], ofItemAtPath: caddyPath)
    } catch {
        printWarning("Could not chmod 755: \(caddyPath)")
    }

    let (verifyExit, verifyOutput) = runProcess(executable: caddyPath, arguments: ["version"])
    if verifyExit == 0 {
        print()
        let version = verifyOutput.isEmpty ? "installed" : firstLine(verifyOutput)
        printSuccess("Caddy installed: \(version)")
        return true
    } else {
        print()
        printError("Caddy installed but version check failed.")
        return false
    }
}

private struct GitHubReleaseAsset: Decodable {
    let name: String
    let browserDownloadURL: String
    let digest: String?

    enum CodingKeys: String, CodingKey {
        case name
        case browserDownloadURL = "browser_download_url"
        case digest
    }
}

private struct GitHubLatestRelease: Decodable {
    let tagName: String
    let assets: [GitHubReleaseAsset]

    enum CodingKeys: String, CodingKey {
        case tagName = "tag_name"
        case assets
    }
}

private let hexCharacterSet = CharacterSet(charactersIn: "0123456789abcdefABCDEF")

private func normalizedSHA256(_ raw: String) -> String? {
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    guard trimmed.count == 64,
          trimmed.unicodeScalars.allSatisfy({ hexCharacterSet.contains($0) }) else {
        return nil
    }
    return trimmed.lowercased()
}

private func sha256FromDigestField(_ digest: String?) -> String? {
    guard let digest else { return nil }
    let trimmed = digest.trimmingCharacters(in: .whitespacesAndNewlines)
    let prefix = "sha256:"
    if trimmed.lowercased().hasPrefix(prefix) {
        return normalizedSHA256(String(trimmed.dropFirst(prefix.count)))
    }
    return normalizedSHA256(trimmed)
}

private func selectTusdDarwinArm64ZipAsset(from assets: [GitHubReleaseAsset]) -> GitHubReleaseAsset? {
    assets.first { $0.name.lowercased().contains("darwin_arm64.zip") }
}

private func selectChecksumAssets(from assets: [GitHubReleaseAsset]) -> [GitHubReleaseAsset] {
    func rank(name: String) -> Int {
        let lower = name.lowercased()
        if lower.contains("sha256sum") { return 0 }
        if lower.contains("sha256") { return 1 }
        if lower.contains("checksum") { return 2 }
        return 3
    }

    return assets
        .filter {
            let lower = $0.name.lowercased()
            return lower.contains("sha256") || lower.contains("checksum")
        }
        .sorted {
            let lhsRank = rank(name: $0.name)
            let rhsRank = rank(name: $1.name)
            if lhsRank != rhsRank { return lhsRank < rhsRank }
            return $0.name.count < $1.name.count
        }
}

private func checksumFileNameMatches(_ checksumEntry: String, expectedAssetName: String) -> Bool {
    var normalized = checksumEntry.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !normalized.isEmpty else { return false }

    if normalized.hasPrefix("*") {
        normalized.removeFirst()
    }
    if normalized.hasPrefix("./") {
        normalized.removeFirst(2)
    }

    return URL(fileURLWithPath: normalized).lastPathComponent == expectedAssetName
}

private func parseSHA256Checksum(from body: String, forAssetName assetName: String) -> String? {
    for rawLine in body.split(separator: "\n", omittingEmptySubsequences: false) {
        let line = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)
        if line.isEmpty || line.hasPrefix("#") {
            continue
        }

        if line.uppercased().hasPrefix("SHA256 ("),
           let closeParen = line.firstIndex(of: ")"),
           let equalsIndex = line.lastIndex(of: "=") {
            let filenameStart = line.index(line.startIndex, offsetBy: 8)
            let fileName = String(line[filenameStart..<closeParen]).trimmingCharacters(in: .whitespacesAndNewlines)
            let hashRaw = String(line[line.index(after: equalsIndex)...]).trimmingCharacters(in: .whitespacesAndNewlines)
            if checksumFileNameMatches(fileName, expectedAssetName: assetName),
               let checksum = normalizedSHA256(hashRaw) {
                return checksum
            }
        }

        let fields = line.split(
            maxSplits: 1,
            omittingEmptySubsequences: true,
            whereSeparator: { $0 == " " || $0 == "\t" }
        )
        if fields.count == 2,
           let checksum = normalizedSHA256(String(fields[0])),
           checksumFileNameMatches(String(fields[1]), expectedAssetName: assetName) {
            return checksum
        }
    }

    return nil
}

private func resolveTusdChecksum(
    zipAsset: GitHubReleaseAsset,
    allAssets: [GitHubReleaseAsset]
) -> (checksum: String, source: String)? {
    if let digestChecksum = sha256FromDigestField(zipAsset.digest) {
        return (checksum: digestChecksum, source: "GitHub asset digest")
    }

    for checksumAsset in selectChecksumAssets(from: allAssets) {
        let (exitCode, checksumBody) = runProcess(
            executable: "/usr/bin/curl",
            arguments: ["-sfL", checksumAsset.browserDownloadURL]
        )
        guard exitCode == 0 else {
            continue
        }

        if let checksum = parseSHA256Checksum(from: checksumBody, forAssetName: zipAsset.name) {
            return (checksum: checksum, source: "release checksum asset \(checksumAsset.name)")
        }
    }

    return nil
}

private func sha256ForFile(at path: String) -> String? {
    let (exitCode, output) = runProcess(
        executable: "/usr/bin/shasum",
        arguments: ["-a", "256", path]
    )
    guard exitCode == 0 else {
        return nil
    }

    guard let hashToken = output.split(whereSeparator: { $0 == " " || $0 == "\t" }).first else {
        return nil
    }

    return normalizedSHA256(String(hashToken))
}

func ensureTusd(home: String) -> Bool {
    let fm = FileManager.default
    let tusdPath = "\(home)/bin/tusd"

    if fm.isExecutableFile(atPath: tusdPath) {
        let (exitCode, output) = runProcess(executable: tusdPath, arguments: ["-version"])
        if exitCode == 0 {
            let version = output.isEmpty ? "installed" : firstLine(output)
            printSuccess("tusd found: \(version)")
            if !keepOrChange(
                label: "installed tusd",
                current: version,
                actionHint: "(Enter=keep installed, c=download latest)",
                showCurrentLine: false
            ) {
                return true
            }
        }
    }

    print()
    printHint("Fetching latest tusd release info...")

    let (apiExit, apiOutput) = runProcess(
        executable: "/usr/bin/curl",
        arguments: ["-sfL", "https://api.github.com/repos/tus/tusd/releases/latest"]
    )

    guard apiExit == 0,
          let jsonData = apiOutput.data(using: .utf8),
          let release = try? JSONDecoder().decode(GitHubLatestRelease.self, from: jsonData),
          let darwinAsset = selectTusdDarwinArm64ZipAsset(from: release.assets) else {
        printError("Could not fetch tusd release info from GitHub.")
        printManualCommands("Manual install:", commands: [
            "VERSION=$(curl -s https://api.github.com/repos/tus/tusd/releases/latest \\",
            "  | grep '\"tag_name\"' | cut -d'\"' -f4 | tr -d 'v')",
            "curl -L \"https://github.com/tus/tusd/releases/download/v${VERSION}/tusd_darwin_arm64.zip\" -o tusd.zip",
            "shasum -a 256 tusd.zip  # compare with official release checksum before unzip",
            "unzip tusd.zip && mv tusd_darwin_arm64/tusd ~/bin/tusd && chmod +x ~/bin/tusd",
            "rm -rf tusd.zip tusd_darwin_arm64",
        ])
        return false
    }

    guard let checksumInfo = resolveTusdChecksum(zipAsset: darwinAsset, allAssets: release.assets) else {
        printError("Could not resolve a verifiable tusd checksum from release metadata/assets.")
        printHint("Refusing automatic install to avoid unverified binary execution.")
        printManualCommands("Manual install (verify checksum first):", commands: [
            "VERSION=$(curl -s https://api.github.com/repos/tus/tusd/releases/latest \\",
            "  | grep '\"tag_name\"' | cut -d'\"' -f4 | tr -d 'v')",
            "curl -L \"https://github.com/tus/tusd/releases/download/v${VERSION}/tusd_darwin_arm64.zip\" -o tusd.zip",
            "shasum -a 256 tusd.zip  # compare with official checksum before unzip",
            "unzip tusd.zip && mv tusd_darwin_arm64/tusd ~/bin/tusd && chmod +x ~/bin/tusd",
            "rm -rf tusd.zip tusd_darwin_arm64",
        ])
        return false
    }

    let tagName = release.tagName
    let downloadURL = darwinAsset.browserDownloadURL

    printHint("Latest release: \(tagName)")
    printHint("Checksum source: \(checksumInfo.source)")
    print()
    printHint("Downloading tusd...")
    print()

    let tmpDir = NSTemporaryDirectory() + "kiko-tusd-\(ProcessInfo.processInfo.globallyUniqueString)"
    do {
        try fm.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
    } catch {
        printError("Could not create temp directory.")
        return false
    }
    defer { try? fm.removeItem(atPath: tmpDir) }

    let zipPath = "\(tmpDir)/tusd.zip"
    let extractDir = "\(tmpDir)/tusd_extract"

    let (dlExit, _) = runProcess(
        executable: "/usr/bin/curl",
        arguments: ["-fL", "--progress-bar", downloadURL, "-o", zipPath],
        capture: false
    )

    guard dlExit == 0 else {
        printError("tusd download failed.")
        printManualCommands("Manual install:", commands: [
            "curl -L \"\(downloadURL)\" -o tusd.zip",
            "shasum -a 256 tusd.zip  # compare with official release checksum before unzip",
            "unzip tusd.zip && mv tusd_darwin_arm64/tusd ~/bin/tusd && chmod +x ~/bin/tusd",
            "rm -rf tusd.zip tusd_darwin_arm64",
        ])
        return false
    }

    guard let downloadedChecksum = sha256ForFile(at: zipPath) else {
        printError("Could not compute SHA256 for downloaded tusd archive.")
        return false
    }

    guard downloadedChecksum == checksumInfo.checksum else {
        printError("tusd checksum verification failed.")
        printHint("Expected: \(checksumInfo.checksum)")
        printHint("Actual:   \(downloadedChecksum)")
        return false
    }
    printSuccess("tusd checksum verified.")

    let (unzipExit, _) = runProcess(
        executable: "/usr/bin/unzip",
        arguments: ["-o", zipPath, "-d", extractDir],
        capture: false
    )

    guard unzipExit == 0 else {
        print()
        printError("Failed to extract tusd zip.")
        return false
    }

    let extractedBinary = "\(extractDir)/tusd_darwin_arm64/tusd"
    guard fm.fileExists(atPath: extractedBinary) else {
        printError("Expected binary not found in extracted archive.")
        return false
    }

    do {
        if fm.fileExists(atPath: tusdPath) {
            try fm.removeItem(atPath: tusdPath)
        }
        try fm.moveItem(atPath: extractedBinary, toPath: tusdPath)
    } catch {
        printError("Could not install tusd binary: \(error.localizedDescription)")
        return false
    }

    do {
        try fm.setAttributes([.posixPermissions: 0o755], ofItemAtPath: tusdPath)
    } catch {
        printWarning("Could not chmod 755: \(tusdPath)")
    }

    let (verifyExit, verifyOutput) = runProcess(executable: tusdPath, arguments: ["-version"])
    if verifyExit == 0 {
        print()
        let version = verifyOutput.isEmpty ? "installed" : firstLine(verifyOutput)
        printSuccess("tusd installed: \(version)")
        return true
    } else {
        print()
        printError("tusd installed but version check failed.")
        return false
    }
}

func offerBuild(repoRoot: String) -> Bool {
    let fm = FileManager.default
    let binaryPath = "\(repoRoot)/.build/release/KikoMedia"

    if fm.fileExists(atPath: binaryPath) {
        printSuccess("kiko-media binary already exists.")
        if !confirm("Rebuild?") {
            return true
        }
    } else if !confirm("Build kiko-media now? (First build may take 2+ minutes.)") {
        return false
    }

    print()
    printHint("Building kiko-media (release)...")
    print()

    let exitCode = runProcessStreaming(
        executable: "/usr/bin/swift",
        arguments: ["build", "-c", "release"],
        workingDirectory: repoRoot
    )

    if exitCode == 0 {
        print()
        printSuccess("Build succeeded.")
        return true
    } else {
        print()
        printError("Build failed. Run manually: cd \(repoRoot) && swift build -c release")
        return false
    }
}
