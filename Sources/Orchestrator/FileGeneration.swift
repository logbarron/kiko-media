import Foundation
import CryptoKit

// MARK: - XML Escaping

func xmlEscape(_ str: String) -> String {
    str.replacingOccurrences(of: "&", with: "&amp;")
       .replacingOccurrences(of: "<", with: "&lt;")
       .replacingOccurrences(of: ">", with: "&gt;")
       .replacingOccurrences(of: "\"", with: "&quot;")
       .replacingOccurrences(of: "'", with: "&apos;")
}

// MARK: - Template Processing

func processTemplate(at path: String, replacements: [String: String], xmlEscapeValues: Bool = false) throws -> String {
    var content = try String(contentsOfFile: path, encoding: .utf8)
    for (placeholder, value) in replacements {
        let escaped = xmlEscapeValues ? xmlEscape(value) : value
        content = content.replacingOccurrences(of: placeholder, with: escaped)
    }
    return content
}

func writeFile(_ content: String, to path: String) throws {
    let url = URL(fileURLWithPath: path)
    let dir = url.deletingLastPathComponent()
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    try content.write(to: url, atomically: true, encoding: .utf8)
}

func chmod600(_ path: String) {
    do {
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: path)
    } catch {
        printWarning("Could not chmod 600: \(path)")
    }
}

enum SetupWizardError: LocalizedError {
    case missingRequiredValue(String)
    case missingRequiredFile(String)
    case unreplacedPlaceholders(path: String, placeholders: [String])
    case cspHashingFailed(tag: String, reason: String)
    case invalidGeneratedArtifact(path: String, reason: String)

    var errorDescription: String? {
        switch self {
        case .missingRequiredValue(let key):
            return "Missing required value: \(key)"
        case .missingRequiredFile(let path):
            return "Missing required file: \(path)"
        case .unreplacedPlaceholders(let path, let placeholders):
            let list = placeholders.sorted().joined(separator: ", ")
            return "Template contains unreplaced placeholders in \(path): \(list)"
        case .cspHashingFailed(let tag, let reason):
            return "Unable to compute CSP hash for <\(tag)>: \(reason)"
        case .invalidGeneratedArtifact(let path, let reason):
            return "Invalid generated artifact at \(path): \(reason)"
        }
    }
}

func requiredAdvancedValue(_ key: String, from advanced: [String: String]) throws -> String {
    guard let value = advanced[key] else {
        throw SetupWizardError.missingRequiredValue(key)
    }
    return value
}

func requiredAdvancedValues(_ keys: [String], from advanced: [String: String]) throws -> [String: String] {
    var values: [String: String] = [:]
    values.reserveCapacity(keys.count)
    for key in keys {
        values[key] = try requiredAdvancedValue(key, from: advanced)
    }
    return values
}

let placeholderRegex = try! NSRegularExpression(pattern: "__[A-Z0-9_]+__", options: [])

func assertNoUnreplacedPlaceholders(_ content: String, in path: String) throws {
    let range = NSRange(content.startIndex..., in: content)
    let matches = placeholderRegex.matches(in: content, options: [], range: range)
    guard !matches.isEmpty else { return }

    var placeholders = Set<String>()
    placeholders.reserveCapacity(matches.count)
    for match in matches {
        if let r = Range(match.range, in: content) {
            placeholders.insert(String(content[r]))
        }
    }
    throw SetupWizardError.unreplacedPlaceholders(path: path, placeholders: Array(placeholders))
}

func cspHashForInlineTag(_ tag: String, in html: String) throws -> String {
    // CSP hashes must be computed from the exact bytes between the opening and closing tags.
    let pattern = "<\(tag)>(.*?)</\(tag)>"
    let regex = try NSRegularExpression(pattern: pattern, options: [.dotMatchesLineSeparators])
    let range = NSRange(html.startIndex..., in: html)
    let matches = regex.matches(in: html, options: [], range: range)
    guard matches.count == 1 else {
        throw SetupWizardError.cspHashingFailed(tag: tag, reason: "expected 1 inline <\(tag)> tag, found \(matches.count)")
    }
    guard let bodyRange = Range(matches[0].range(at: 1), in: html) else {
        throw SetupWizardError.cspHashingFailed(tag: tag, reason: "internal error extracting match")
    }

    let body = String(html[bodyRange])
    let digest = SHA256.hash(data: Data(body.utf8))
    let b64 = Data(digest).base64EncodedString()
    return "sha256-\(b64)"
}

struct AdvancedTemplateContext {
    let baseDir: String
    let placeholders: [String: String]
}

struct FrontendArtifacts {
    let indexContent: String
    let caddyfileContent: String
}

struct FrontendRegenerationResult {
    let indexChanged: Bool
    let caddyfileChanged: Bool
}

func makeAdvancedTemplateContext(from advanced: [String: String]) throws -> AdvancedTemplateContext {
    let baseDirRaw = try requiredAdvancedValue("BASE_DIRECTORY", from: advanced)
    let baseDir = expandTildePath(baseDirRaw)
    var placeholders: [String: String] = [:]
    placeholders.reserveCapacity(advanced.count)
    for (key, value) in advanced {
        let expandedValue = key == "BASE_DIRECTORY" ? baseDir : value
        placeholders["__\(key)__"] = expandedValue
    }
    return AdvancedTemplateContext(baseDir: baseDir, placeholders: placeholders)
}

func renderFrontendArtifacts(
    advanced: [String: String],
    advancedContext: AdvancedTemplateContext,
    gateSecret: String,
    turnstileSitekey: String,
    domain: String,
    wireguardBindIP: String,
    repoRoot: String
) throws -> FrontendArtifacts {
    var indexReplacements = advancedContext.placeholders
    indexReplacements["__GATE_ENABLED__"] = gateSecret.isEmpty ? "false" : "true"
    indexReplacements["__TURNSTILE_SITEKEY__"] = turnstileSitekey
    indexReplacements["__TURNSTILE_ACTION__"] = try requiredAdvancedValue(
        "TURNSTILE_EXPECTED_ACTION",
        from: advanced
    )
    indexReplacements["__TURNSTILE_CDATA__"] = try requiredAdvancedValue(
        "TURNSTILE_EXPECTED_CDATA",
        from: advanced
    )
    let indexContent = try processTemplate(
        at: "\(repoRoot)/deploy/index.html.template",
        replacements: indexReplacements
    )
    try assertNoUnreplacedPlaceholders(indexContent, in: "deploy/index.html.template")

    let cspStyleHash = try cspHashForInlineTag("style", in: indexContent)
    let cspScriptHash = try cspHashForInlineTag("script", in: indexContent)

    var caddyfileReplacements = advancedContext.placeholders
    caddyfileReplacements["__DOMAIN__"] = domain
    caddyfileReplacements["__WIREGUARD_BIND_IP__"] = wireguardBindIP
    caddyfileReplacements["__BASE_DIRECTORY__"] = advancedContext.baseDir
    caddyfileReplacements["__REPO_DIR__"] = repoRoot
    caddyfileReplacements["__CSP_STYLE_HASH__"] = cspStyleHash
    caddyfileReplacements["__CSP_SCRIPT_HASH__"] = cspScriptHash
    let caddyfileContent = try processTemplate(
        at: "\(repoRoot)/deploy/Caddyfile.template",
        replacements: caddyfileReplacements
    )
    try assertNoUnreplacedPlaceholders(caddyfileContent, in: "deploy/Caddyfile.template")

    return FrontendArtifacts(indexContent: indexContent, caddyfileContent: caddyfileContent)
}

func requiredFileContents(at path: String, displayName: String) throws -> String {
    guard FileManager.default.fileExists(atPath: path) else {
        throw SetupWizardError.missingRequiredFile(displayName)
    }
    return try String(contentsOfFile: path, encoding: .utf8)
}

func parseJavaScriptStringConstant(_ name: String, from content: String, path: String) throws -> String {
    let marker = "const \(name) = "
    let matches = content
        .split(separator: "\n", omittingEmptySubsequences: false)
        .map(String.init)
        .compactMap { line -> String? in
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmed.hasPrefix(marker), trimmed.hasSuffix(";") else { return nil }
            let rawValue = String(trimmed.dropFirst(marker.count).dropLast())
            guard rawValue.count >= 2, rawValue.first == "'", rawValue.last == "'" else { return nil }
            return String(rawValue.dropFirst().dropLast())
        }

    guard matches.count == 1, let value = matches.first, !value.isEmpty else {
        throw SetupWizardError.invalidGeneratedArtifact(
            path: path,
            reason: "could not parse \(name) from generated JavaScript constant"
        )
    }
    return value
}

func parseJavaScriptIntConstant(_ name: String, from content: String, path: String) throws -> String {
    let marker = "const \(name) = "
    let matches = content
        .split(separator: "\n", omittingEmptySubsequences: false)
        .map(String.init)
        .compactMap { line -> String? in
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmed.hasPrefix(marker), trimmed.hasSuffix(";") else { return nil }
            let rawValue = String(trimmed.dropFirst(marker.count).dropLast()).trimmingCharacters(in: .whitespaces)
            guard !rawValue.isEmpty, Int(rawValue) != nil else { return nil }
            return rawValue
        }

    guard matches.count == 1, let value = matches.first else {
        throw SetupWizardError.invalidGeneratedArtifact(
            path: path,
            reason: "could not parse integer constant \(name)"
        )
    }
    return value
}

func parseJavaScriptIntProperty(_ name: String, from content: String, path: String) throws -> String {
    let marker = "\(name):"
    let matches = content
        .split(separator: "\n", omittingEmptySubsequences: false)
        .map(String.init)
        .compactMap { line -> String? in
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmed.hasPrefix(marker), trimmed.hasSuffix(",") else { return nil }
            let rawValue = String(trimmed.dropFirst(marker.count).dropLast()).trimmingCharacters(in: .whitespaces)
            guard !rawValue.isEmpty, Int(rawValue) != nil else { return nil }
            return rawValue
        }

    guard matches.count == 1, let value = matches.first else {
        throw SetupWizardError.invalidGeneratedArtifact(
            path: path,
            reason: "could not parse integer property \(name)"
        )
    }
    return value
}

let generatedFrontendIndexBindings: [(key: String, source: String, isProperty: Bool)] = [
    ("UPLOAD_CHUNK_SIZE_BYTES", "CHUNK_SIZE", false),
    ("TUSD_MAX_SIZE", "MAX_FILE_SIZE", false),
    ("PARALLEL_UPLOADS", "PARALLEL_UPLOADS", false),
    ("UPLOAD_RETRY_BASE_MS", "UPLOAD_RETRY_BASE_MS", false),
    ("UPLOAD_RETRY_MAX_MS", "UPLOAD_RETRY_MAX_MS", false),
    ("UPLOAD_RETRY_STEPS", "UPLOAD_RETRY_STEPS", false),
    ("POLL_MAX_INFLIGHT", "maxInflight", true),
    ("GALLERY_POLL_BASE_MS", "GALLERY_POLL_BASE_MS", false),
    ("GALLERY_POLL_MAX_MS", "GALLERY_POLL_MAX_MS", false),
    ("PHOTO_THUMB_POLL_BASE_MS", "PHOTO_THUMB_POLL_BASE_MS", false),
    ("PHOTO_THUMB_POLL_MAX_MS", "PHOTO_THUMB_POLL_MAX_MS", false),
    ("PHOTO_PREVIEW_POLL_BASE_MS", "PHOTO_PREVIEW_POLL_BASE_MS", false),
    ("PHOTO_PREVIEW_POLL_MAX_MS", "PHOTO_PREVIEW_POLL_MAX_MS", false),
    ("VIDEO_PREVIEW_EARLY_BASE_MS", "VIDEO_PREVIEW_EARLY_BASE_MS", false),
    ("VIDEO_PREVIEW_EARLY_MAX_MS", "VIDEO_PREVIEW_EARLY_MAX_MS", false),
    ("VIDEO_PREVIEW_LATE_MS", "VIDEO_PREVIEW_LATE_MS", false),
    ("VIDEO_PREVIEW_EARLY_WINDOW_MS", "VIDEO_PREVIEW_EARLY_WINDOW_MS", false),
]

func mergedAdvancedValuesForFrontendRegeneration(
    persistedEnv: [String: String],
    indexContent: String,
    path: String
) throws -> [String: String] {
    var advanced = persistedEnv
    for binding in generatedFrontendIndexBindings where advanced[binding.key] == nil {
        let value = try (
            binding.isProperty
            ? parseJavaScriptIntProperty(binding.source, from: indexContent, path: path)
            : parseJavaScriptIntConstant(binding.source, from: indexContent, path: path)
        )
        advanced[binding.key] = value
    }
    return advanced
}

func parsePublicSiteConfiguration(from caddyContent: String, path: String) throws -> (domain: String, wireguardBindIP: String) {
    let lines = caddyContent
        .split(separator: "\n", omittingEmptySubsequences: false)
        .map(String.init)

    guard let siteIndex = lines.firstIndex(where: {
        let trimmed = $0.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.hasPrefix("https://") && trimmed.hasSuffix("{")
    }) else {
        throw SetupWizardError.invalidGeneratedArtifact(path: path, reason: "missing public site block")
    }

    let siteLine = lines[siteIndex].trimmingCharacters(in: .whitespacesAndNewlines)
    let prefix = "https://"
    let suffix = ":8443 {"
    guard siteLine.hasPrefix(prefix), siteLine.hasSuffix(suffix) else {
        throw SetupWizardError.invalidGeneratedArtifact(path: path, reason: "unexpected public site line format")
    }
    let domain = String(siteLine.dropFirst(prefix.count).dropLast(suffix.count))
    guard !domain.isEmpty else {
        throw SetupWizardError.invalidGeneratedArtifact(path: path, reason: "public domain is empty")
    }

    for line in lines.dropFirst(siteIndex + 1) {
        let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed == "}" { break }
        if trimmed.hasPrefix("bind ") {
            let bindIP = String(trimmed.dropFirst("bind ".count))
            guard !bindIP.isEmpty else {
                throw SetupWizardError.invalidGeneratedArtifact(path: path, reason: "public bind IP is empty")
            }
            return (domain, bindIP)
        }
    }

    throw SetupWizardError.invalidGeneratedArtifact(path: path, reason: "missing bind line in public site block")
}

@discardableResult
func writeFileIfChanged(_ content: String, to path: String) throws -> Bool {
    if let existing = try? String(contentsOfFile: path, encoding: .utf8), existing == content {
        return false
    }
    try writeFile(content, to: path)
    return true
}

func regenerateFrontendArtifacts(repoRoot: String, home: String) throws -> FrontendRegenerationResult {
    let persistedEnv = loadMediaLaunchAgentEnvironment(home: home)
    guard !persistedEnv.isEmpty else {
        throw SetupWizardError.missingRequiredFile("~/Library/LaunchAgents/com.kiko.media.plist")
    }

    let indexPath = "\(repoRoot)/deploy/index.html"
    let caddyfilePath = "\(repoRoot)/deploy/Caddyfile"
    let existingIndexContent = try requiredFileContents(at: indexPath, displayName: "deploy/index.html")
    let existingCaddyfileContent = try requiredFileContents(at: caddyfilePath, displayName: "deploy/Caddyfile")

    let turnstileSitekey = try parseJavaScriptStringConstant(
        "TURNSTILE_SITEKEY",
        from: existingIndexContent,
        path: "deploy/index.html"
    )
    let advanced = try mergedAdvancedValuesForFrontendRegeneration(
        persistedEnv: persistedEnv,
        indexContent: existingIndexContent,
        path: "deploy/index.html"
    )
    let publicSite = try parsePublicSiteConfiguration(
        from: existingCaddyfileContent,
        path: "deploy/Caddyfile"
    )
    let advancedContext = try makeAdvancedTemplateContext(from: advanced)
    let frontendArtifacts = try renderFrontendArtifacts(
        advanced: advanced,
        advancedContext: advancedContext,
        gateSecret: advanced["GATE_SECRET"] ?? "",
        turnstileSitekey: turnstileSitekey,
        domain: publicSite.domain,
        wireguardBindIP: publicSite.wireguardBindIP,
        repoRoot: repoRoot
    )

    let indexChanged = try writeFileIfChanged(frontendArtifacts.indexContent, to: indexPath)
    if indexChanged {
        printSuccess("Updated deploy/index.html")
    } else {
        printHint("deploy/index.html unchanged.")
    }

    let caddyfileChanged = try writeFileIfChanged(frontendArtifacts.caddyfileContent, to: caddyfilePath)
    if caddyfileChanged {
        printSuccess("Updated deploy/Caddyfile")
    } else {
        printHint("deploy/Caddyfile unchanged.")
    }

    return FrontendRegenerationResult(indexChanged: indexChanged, caddyfileChanged: caddyfileChanged)
}

// MARK: - File Generation

func generateFiles(answers: Answers, repoRoot: String, home: String) throws {
    let fm = FileManager.default
    let advancedContext = try makeAdvancedTemplateContext(from: answers.advanced)
    let baseDir = advancedContext.baseDir
    let advancedPlaceholders = advancedContext.placeholders

    // Build media plist replacements from advanced dict + secrets
    var mediaReplacements: [String: String] = [
        "__REPO_DIR__": repoRoot,
        "__TURNSTILE_SECRET__": answers.turnstileSecret,
        "__SESSION_HMAC_SECRET__": answers.sessionHmacSecret,
        "__INTERNAL_AUTH_SECRET__": answers.internalAuthSecret,
        "__GATE_SECRET__": answers.gateSecret,
        "__EXTERNAL_SSD_PATH__": answers.ssdPath,
    ]
    for (placeholder, value) in advancedPlaceholders {
        mediaReplacements[placeholder] = value
    }

    let frontendArtifacts = try renderFrontendArtifacts(
        advanced: answers.advanced,
        advancedContext: advancedContext,
        gateSecret: answers.gateSecret,
        turnstileSitekey: answers.turnstileSitekey,
        domain: answers.domain,
        wireguardBindIP: answers.wireguardBindIP,
        repoRoot: repoRoot
    )
    try writeFile(frontendArtifacts.indexContent, to: "\(repoRoot)/deploy/index.html")
    printSuccess("deploy/index.html")
    try writeFile(frontendArtifacts.caddyfileContent, to: "\(repoRoot)/deploy/Caddyfile")
    printSuccess("deploy/Caddyfile")

    // 3. com.kiko.caddy.plist (XML — escape values)
    let caddyPlist = try processTemplate(
        at: "\(repoRoot)/deploy/launchd/com.kiko.caddy.plist",
        replacements: [
            "__HOME__": home,
            "__REPO_DIR__": repoRoot,
            "__CLOUDFLARE_API_TOKEN__": answers.cloudflareToken,
            "__INTERNAL_AUTH_SECRET__": answers.internalAuthSecret,
            "__BASE_DIRECTORY__": baseDir,
        ],
        xmlEscapeValues: true
    )
    let launchAgentsDir = "\(home)/Library/LaunchAgents"
    try assertNoUnreplacedPlaceholders(caddyPlist, in: "deploy/launchd/com.kiko.caddy.plist")
    let caddyPlistPath = "\(launchAgentsDir)/com.kiko.caddy.plist"
    try writeFile(caddyPlist, to: caddyPlistPath)
    chmod600(caddyPlistPath)
    printSuccess("~/Library/LaunchAgents/com.kiko.caddy.plist")

    // 4. com.kiko.tusd.plist (XML — escape values)
    var tusdPlistReplacements = advancedPlaceholders
    tusdPlistReplacements["__HOME__"] = home
    tusdPlistReplacements["__BASE_DIRECTORY__"] = baseDir
    let tusdPlist = try processTemplate(
        at: "\(repoRoot)/deploy/launchd/com.kiko.tusd.plist",
        replacements: tusdPlistReplacements,
        xmlEscapeValues: true
    )
    try assertNoUnreplacedPlaceholders(tusdPlist, in: "deploy/launchd/com.kiko.tusd.plist")
    let tusdPlistPath = "\(launchAgentsDir)/com.kiko.tusd.plist"
    try writeFile(tusdPlist, to: tusdPlistPath)
    chmod600(tusdPlistPath)
    printSuccess("~/Library/LaunchAgents/com.kiko.tusd.plist")

    // 5. com.kiko.media.plist (XML — escape values)
    let mediaPlist = try processTemplate(
        at: "\(repoRoot)/deploy/launchd/com.kiko.media.plist",
        replacements: mediaReplacements,
        xmlEscapeValues: true
    )
    try assertNoUnreplacedPlaceholders(mediaPlist, in: "deploy/launchd/com.kiko.media.plist")
    let mediaPlistPath = "\(launchAgentsDir)/com.kiko.media.plist"
    try writeFile(mediaPlist, to: mediaPlistPath)
    chmod600(mediaPlistPath)
    printSuccess("~/Library/LaunchAgents/com.kiko.media.plist")

    // ── Create directories ──

    print()

    let dirs = [
        "\(baseDir)/uploads",
        "\(baseDir)/thumbs",
        "\(baseDir)/previews",
        "\(baseDir)/logs",
        "\(baseDir)/moderated",
    ]

    for dir in dirs {
        if !fm.fileExists(atPath: dir) {
            try fm.createDirectory(atPath: dir, withIntermediateDirectories: true)
            printSuccess("Created \(dir.replacingOccurrences(of: home, with: "~"))")
        }
    }

    // SSD originals directory
    if fm.isWritableFile(atPath: URL(fileURLWithPath: answers.ssdPath).deletingLastPathComponent().path) {
        if !fm.fileExists(atPath: answers.ssdPath) {
            try fm.createDirectory(atPath: answers.ssdPath, withIntermediateDirectories: true)
            printSuccess("Created \(answers.ssdPath)")
        }
    }

}
