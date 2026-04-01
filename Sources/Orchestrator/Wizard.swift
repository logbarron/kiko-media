import Foundation
import Dispatch
import Darwin
import Security
import KikoMediaCore

// MARK: - Defaults File Parser

struct DefaultSpec {
    let type: String
    let defaultValue: String
    let min: String
    let max: String
    let label: String
    let description: String
}

/// Parse deploy/defaults.env — single source of truth for all configurable env vars.
func parseDefaults(_ path: String) -> [String: DefaultSpec] {
    guard let content = try? String(contentsOfFile: path, encoding: .utf8) else { return [:] }
    var result: [String: DefaultSpec] = [:]
    for line in content.split(separator: "\n") {
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        if trimmed.isEmpty || trimmed.hasPrefix("#") { continue }
        let parts = trimmed.split(separator: "|", maxSplits: 6, omittingEmptySubsequences: false).map(String.init)
        if parts.count == 7 {
            result[parts[0]] = DefaultSpec(
                type: parts[1], defaultValue: parts[2],
                min: parts[3], max: parts[4],
                label: parts[5], description: parts[6]
            )
        }
    }
    return result
}

/// Validate a value against its spec from defaults.env.
func validateSpec(_ value: String, envVar: String? = nil, spec: DefaultSpec) -> Bool {
    switch spec.type {
    case "int":
        guard let n = Int(value) else { return false }
        if !spec.min.isEmpty, !spec.max.isEmpty,
           let lo = Int(spec.min), let hi = Int(spec.max) {
            return (lo...hi).contains(n)
        }
        return true
    case "double":
        guard let n = Double(value) else { return false }
        if !spec.min.isEmpty, !spec.max.isEmpty,
           let lo = Double(spec.min), let hi = Double(spec.max) {
            return (lo...hi).contains(n)
        }
        return true
    case "string":
        if envVar == "VIDEO_SCHEDULER_POLICY" {
            return VideoSchedulerPolicy(trimmedRawValue: value) != nil
        }
        return !value.isEmpty
    default:
        return false
    }
}

func normalizedAdvancedValue(_ value: String, for envVar: String) -> String {
    if envVar == "VIDEO_SCHEDULER_POLICY" {
        return value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }
    return value
}

// MARK: - Formatting

func formatBytes(_ bytes: Int64) -> String {
    let gb = Double(bytes) / 1_000_000_000
    if gb >= 1000 {
        return String(format: "%.1f TB", gb / 1000)
    }
    return String(format: "%.0f GB", gb)
}

// MARK: - Secret Generation

func generateHmacSecret() -> String? {
    var bytes = [UInt8](repeating: 0, count: 32)
    let status = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
    guard status == errSecSuccess else { return nil }
    return Data(bytes).base64EncodedString()
}

// MARK: - Collected Answers

struct Answers {
    let domain: String
    let wireguardBindIP: String
    let cloudflareToken: String
    let turnstileSecret: String
    let turnstileSitekey: String
    let sessionHmacSecret: String
    let internalAuthSecret: String
    let gateSecret: String
    let ssdPath: String
    let eventTimezone: String
    var advanced: [String: String]  // all env vars from defaults.env
}

// MARK: - SSD Picker

func pickSSDPath() -> String {
    TerminalUIPrimitives.pickExternalSSDPath(
        formatBytes: formatBytes,
        manualPathPrompt: "SSD path",
        manualPathExamples: ["/Volumes/MySSD/originals", "/Volumes/MyDrive/originals"],
        selectionHint: "An /originals folder will be created on the drive you pick.",
        selectedVolumePath: { $0.appendingPathComponent("originals").path }
    )
}

// MARK: - Timezone Picker

let timezoneQuickPickOptions: [(label: String, identifier: String)] = [
    ("US Eastern", "America/New_York"),
    ("US Pacific", "America/Los_Angeles"),
    ("UTC", "Etc/UTC"),
    ("London", "Europe/London"),
    ("Tokyo", "Asia/Tokyo"),
]

func pickTimezone() -> String {
    for (index, option) in timezoneQuickPickOptions.enumerated() {
        print("\(listItemIndent)\(bold)\(index + 1)\(reset)  \(option.label)  \(dim)(\(option.identifier))\(reset)")
    }
    print("\(listItemIndent)\(dim)0\(reset)  Enter manually")
    print()
    printHint("Global IANA zones are supported (e.g. Europe/Paris, Asia/Singapore).")
    printHint("Press Enter for system default.")
    print()
    while true {
        let tzChoice = prompt("Which timezone")
        if tzChoice.isEmpty {
            return ""
        } else if let n = Int(tzChoice), (1...timezoneQuickPickOptions.count).contains(n) {
            return timezoneQuickPickOptions[n - 1].identifier
        } else if tzChoice == "0" {
            printHint("Use Continent/City format, e.g. Europe/London, Asia/Tokyo")
            let custom = promptRequired("Timezone identifier")
            if TimeZone(identifier: custom) != nil {
                return custom
            } else {
                printWarning("'\(custom)' is not recognized.")
                if confirm("Use it anyway") {
                    return custom
                }
            }
        }
        printError("Enter a number from the list (0-\(timezoneQuickPickOptions.count)), or press Enter for system default.")
    }
}

let defaultModerationURL = "http://localhost:8080"

func moderationUIURL(from caddyfileContent: String) -> String? {
    let pattern = #"localhost:(\d+)"#
    guard let regex = try? NSRegularExpression(pattern: pattern) else { return nil }
    let range = NSRange(caddyfileContent.startIndex..<caddyfileContent.endIndex, in: caddyfileContent)
    guard let match = regex.firstMatch(in: caddyfileContent, range: range),
          let portRange = Range(match.range(at: 1), in: caddyfileContent)
    else {
        return nil
    }
    return "http://localhost:\(caddyfileContent[portRange])"
}

func resolveModerationUIURL(repoRoot: String) -> String {
    let caddyCandidates = [
        "\(repoRoot)/deploy/Caddyfile",
        "\(repoRoot)/deploy/Caddyfile.template",
    ]
    for path in caddyCandidates {
        if let content = try? String(contentsOfFile: path, encoding: .utf8),
           let url = moderationUIURL(from: content) {
            return url
        }
    }
    return defaultModerationURL
}


// MARK: - Main Wizard

func runWizard() throws {
    let fm = FileManager.default
    let (home, repoRoot) = resolveHomeAndRepoRoot(attemptedCommand: setupInvocationBase(repoRoot: nil))

    // Load defaults from deploy/defaults.env
    let defaults = parseDefaults("\(repoRoot)/deploy/defaults.env")
    guard !defaults.isEmpty else {
        printError("Cannot read deploy/defaults.env")
        exit(1)
    }

    runStartMenu(home: home, repoRoot: repoRoot)

    // ── Binary Downloads ──
    redraw([])

    let binDir = "\(home)/bin"
    if !fm.fileExists(atPath: binDir) {
        try fm.createDirectory(atPath: binDir, withIntermediateDirectories: true)
    }

    printSectionTitle("Dependency Check")
    printBody("kiko-media requires Caddy and tusd.")
    printBody("If they are already installed, press Enter to keep current versions.")
    print()

    let caddyOk = ensureCaddy(home: home)
    print()
    let tusdOk = ensureTusd(home: home)
    print()

    if !caddyOk || !tusdOk {
        if !confirm("Continue with setup? (Binaries aren't needed until you start services.)") {
            print("\n  \(dim)Cancelled.\(reset)\n")
            exit(0)
        }
        print()
    }

    let totalBasicSteps = 11

    // Answers persist across loops so defaults work on start-over
    var domain = ""
    var wireguardIP = ""
    var cloudflareToken = ""
    var turnstileSitekey = ""
    var turnstileSecret = ""
    var sessionSecret = ""
    var internalAuthSecret = ""
    var gateSecret = ""
    var ssdPath = ""
    var eventTimezone = ""
    var advancedOverrides: [String: String] = [:]
    var shouldConfigureThunderbolt = false
    var benchmarkResult: PipelineDelegatedRecommendation? = nil

    while true {
        var completed: [(String, String)] = []
        benchmarkResult = nil

        // ── Step 1: Domain ──

        redraw(completed)
        printStep(1, of: totalBasicSteps, "Domain")
        print()
        printBody("The domain guests will use to access the gallery.")
        printHint("e.g. photos.example.com")
        print()
        if domain.isEmpty || keepOrChange(label: "domain", current: domain) {
            while true {
                let input = promptRequired("Domain")
                if validateDomain(input) { domain = input; break }
                printError("Enter a valid domain, e.g. photos.example.com")
            }
        }
        completed.append(("Step 1 · Domain", domain))

        // ── Step 2: WireGuard ──

        redraw(completed)
        printStep(2, of: totalBasicSteps, "WireGuard")
        print()
        printBody("This is your Mac's Address in the vps-tunnel config in the WireGuard app.")
        printHint("Caddy binds to this IP so it's only reachable through the tunnel.")
        printHint("Haven't set up WireGuard yet? Follow docs/runbook.md section 4.6 first.")
        printHint("e.g. 10.0.0.2")
        print()
        if wireguardIP.isEmpty || keepOrChange(label: "IP", current: wireguardIP) {
            while true {
                let input = promptRequired("WireGuard IP")
                if validateIPv4(input) { wireguardIP = input; break }
                printError("Enter a valid IPv4 address, e.g. 10.0.0.2")
            }
        }
        completed.append(("Step 2 · WireGuard", wireguardIP))

        // ── Step 3: Cloudflare API Token ──

        redraw(completed)
        printStep(3, of: totalBasicSteps, "Cloudflare API Token")
        print()
        printBody("Caddy uses this token to get TLS certificates for your domain.")
        print()
        printNumberedItems([
            "Go to \(bold)https://dash.cloudflare.com/profile/api-tokens\(reset)",
            "Click 'Create Token'",
            "Choose 'Create Custom Token'",
            "Permissions: Zone \(dim)→\(reset) Zone \(dim)→\(reset) Read, Zone \(dim)→\(reset) DNS \(dim)→\(reset) Edit",
            "Zone Resources: Include \(dim)→\(reset) Specific zone \(dim)→\(reset) your domain",
            "Create the token and copy it",
        ])
        print()
        if cloudflareToken.isEmpty || keepOrChange(label: "token", current: cloudflareToken, display: maskedPreview(cloudflareToken)) {
            cloudflareToken = promptValidCloudflareToken()
        }
        while true {
            if !validateCloudflareTokenFormat(cloudflareToken) {
                cloudflareToken = promptValidCloudflareToken()
            }

            print()
            printHint("Verifying Cloudflare token...")
            switch verifyCloudflareToken(cloudflareToken) {
            case .valid:
                printSuccess("Cloudflare token verified.")
                break
            case .invalid(let reason):
                printError("Cloudflare token invalid: \(reason)")
                cloudflareToken = promptValidCloudflareToken()
                continue
            case .unavailable(let reason):
                // Network issues should warn (not fail) so offline setup still works.
                printWarning("Could not verify Cloudflare token (\(reason)).")
                if confirm("Continue without verifying this token") {
                    break
                }
                cloudflareToken = promptValidCloudflareToken()
                continue
            }
            break
        }
        completed.append(("Step 3 · Cloudflare Token", "token saved"))

        // ── Step 4: Turnstile ──

        redraw(completed)
        printStep(4, of: totalBasicSteps, "Turnstile")
        print()
        printBody("Cloudflare bot protection. Create a Turnstile widget, get Site and Secret keys.")
        print()
        printNumberedItems([
            "Open your Cloudflare dashboard and go to:",
        ])
        print("\(listDetailIndent)Protect & Connect \(dim)→\(reset) Application Security \(dim)→\(reset) Turnstile")
        printNumberedItems([
            "Click 'Add widget'",
            "Widget name: something descriptive \(dim)(e.g. kiko-media)\(reset)",
            "Click 'Add a custom hostname' and enter your domain",
        ], start: 2)
        print("\(listDetailIndent)\(dim)This should match Step 1:\(reset) \(bold)\(domain)\(reset)")
        printNumberedItems([
            "Click 'Add', then select Widget Mode: \(bold)Managed\(reset)",
            "Create the widget",
            "Copy keys below when prompted",
        ], start: 5)
        print()
        var needsTurnstileInput = turnstileSitekey.isEmpty || turnstileSecret.isEmpty
        if !needsTurnstileInput {
            let display = "site \(maskedPreview(turnstileSitekey)), secret \(maskedPreview(turnstileSecret))"
            needsTurnstileInput = keepOrChange(label: "Turnstile keys", current: "saved", display: display)
        }
        if needsTurnstileInput {
            turnstileSitekey = promptRequired("Site key")
            turnstileSecret = promptRequired("Secret key")
        }
        while true {
            print()
            printHint("Sanity-checking Turnstile secret key...")
            switch verifyTurnstileSecret(turnstileSecret) {
            case .valid:
                printSuccess("Turnstile secret key looks valid.")
                break
            case .invalid(let reason):
                printError("Turnstile secret key invalid: \(reason)")
                turnstileSecret = promptRequired("Secret key")
                continue
            case .unavailable(let reason):
                printWarning("Could not verify Turnstile secret key (\(reason)).")
                if confirm("Continue without verifying this secret key") {
                    break
                }
                turnstileSecret = promptRequired("Secret key")
                continue
            }
            break
        }
        completed.append(("Step 4 · Turnstile", "keys saved"))

        // ── Step 5: Event Gate (Optional) ──

        redraw(completed)
        printStep(5, of: totalBasicSteps, "Event Gate")
        print()
        printBody("Optional extra check at Turnstile verify: require an event code or invite token.")
        printHint("Leave disabled for Turnstile-only behavior.")
        print()

        if gateSecret.isEmpty {
            if confirm("Enable event gate") {
                gateSecret = promptValidGateSecret()
            }
        } else {
            let display = maskedPreview(gateSecret)
            if keepOrChange(label: "gate secret", current: gateSecret, display: display) {
                if confirm("Update gate secret") {
                    gateSecret = promptValidGateSecret()
                } else if confirm("Disable event gate") {
                    gateSecret = ""
                }
            }
        }

        completed.append(("Step 5 · Event Gate", gateSecret.isEmpty ? "disabled" : "enabled"))

        // ── Step 6: Session Secret ──

        redraw(completed)
        printStep(6, of: totalBasicSteps, "Session Secret")
        print()
        printBody("Used to sign session cookies. Keep this stable unless you want to invalidate all sessions.")
        printHint("A separate internal auth secret is also generated for moderation API routing.")
        print()

        var shouldRegenerate = sessionSecret.isEmpty
        var sessionSecretAction = "kept"
        if !sessionSecret.isEmpty {
            let preview = maskedPreview(sessionSecret)
            shouldRegenerate = keepOrChange(label: "secret", current: sessionSecret, display: preview)
        }

        if shouldRegenerate {
            if let generated = generateHmacSecret() {
                sessionSecret = generated
                sessionSecretAction = "generated"
                printSuccess("Generated. This will be saved to the launchd plist.")
            } else {
                printWarning("Could not auto-generate a secret (system RNG failure).")
                printHint("Generate one with: openssl rand -base64 32")
                sessionSecret = promptValidSessionHmacSecret()
                sessionSecretAction = "updated"
            }
        }

        while !validateSessionHmacSecret(sessionSecret) {
            printWarning("Current secret is shorter than 32 bytes. kiko-media will refuse startup.")
            if confirm("Regenerate a new secret now") {
                if let generated = generateHmacSecret() {
                    sessionSecret = generated
                    sessionSecretAction = "generated"
                    printSuccess("Generated. This will be saved to the launchd plist.")
                    break
                }
                printWarning("Could not auto-generate a secret (system RNG failure).")
                printHint("Generate one with: openssl rand -base64 32")
            }
            sessionSecret = promptValidSessionHmacSecret()
            sessionSecretAction = "updated"
        }

        var internalAuthSecretAction = "kept"
        if internalAuthSecret.isEmpty {
            if let generated = generateHmacSecret() {
                internalAuthSecret = generated
                internalAuthSecretAction = "generated"
                printSuccess("Generated dedicated internal auth secret for moderation routes.")
            } else {
                printWarning("Could not auto-generate internal auth secret (system RNG failure).")
                printHint("Generate one with: openssl rand -base64 32")
                internalAuthSecret = promptValidSessionHmacSecret()
                internalAuthSecretAction = "updated"
            }
        }

        while !validateSessionHmacSecret(internalAuthSecret) {
            printWarning("Internal auth secret is shorter than 32 bytes.")
            if confirm("Regenerate internal auth secret now") {
                if let generated = generateHmacSecret() {
                    internalAuthSecret = generated
                    internalAuthSecretAction = "generated"
                    printSuccess("Generated dedicated internal auth secret.")
                    break
                }
                printWarning("Could not auto-generate internal auth secret (system RNG failure).")
                printHint("Generate one with: openssl rand -base64 32")
            }
            internalAuthSecret = promptValidSessionHmacSecret()
            internalAuthSecretAction = "updated"
        }

        print()
        waitForEnter()
        completed.append(("Step 6 · Session Secret", "session \(sessionSecretAction), internal \(internalAuthSecretAction)"))

        // ── Step 7: External SSD ──

        redraw(completed)
        printStep(7, of: totalBasicSteps, "External SSD")
        print()
        printBody("Originals are archived to an external drive for safekeeping.")
        print()
        if ssdPath.isEmpty || keepOrChange(label: "path", current: ssdPath) {
            ssdPath = pickSSDPath()
        }

        while true {
            // Validate SSD path
            let ssdURL = URL(fileURLWithPath: ssdPath)
            let ssdParent = ssdURL.deletingLastPathComponent().path

            if fm.fileExists(atPath: ssdPath), !fm.isWritableFile(atPath: ssdPath) {
                printError("SSD path exists but is not writable: \(ssdPath)")
                printHint("Pick a different location or fix permissions, then try again.")
                print()
                ssdPath = pickSSDPath()
                continue
            }

            if !fm.fileExists(atPath: ssdPath), !fm.isWritableFile(atPath: ssdParent) {
                printWarning("Cannot verify path. Make sure the volume is mounted before starting services.")
                if confirm("Use this path anyway") {
                    break
                }
                print()
                ssdPath = pickSSDPath()
                continue
            }

            break
        }
        completed.append(("Step 7 · External SSD", ssdPath))

        // ── Step 8: Event Timezone ──

        redraw(completed)
        printStep(8, of: totalBasicSteps, "Event Timezone")
        print()
        printBody("Photo timestamps are formatted in this timezone.")
        print()
        if eventTimezone.isEmpty || keepOrChange(label: "timezone", current: eventTimezone) {
            eventTimezone = pickTimezone()
        }
        completed.append(("Step 8 · Timezone", eventTimezone.isEmpty ? "system default" : eventTimezone))

        // ── Step 9: Benchmark ──

        redraw(completed)
        printStep(9, of: totalBasicSteps, "Benchmark")
        print()
        printBody("Run benchmarks to find optimal image and video concurrency for this Mac.")
        printHint("Takes a few minutes. Requires a folder with sample images and videos.")
        print()
        if confirm("Benchmark your system") {
            printHint("Drag a folder from Finder, or type the path.")
            printHint("Needs sample images and videos from a real event.")
            print()
            var mediaPath = ""
            while true {
                mediaPath = normalizePathInput(promptRequired("Media folder"))
                var isDir: ObjCBool = false
                guard fm.fileExists(atPath: mediaPath, isDirectory: &isDir), isDir.boolValue else {
                    redraw(completed)
                    printStep(9, of: totalBasicSteps, "Benchmark")
                    printError("Not a valid directory: \(mediaPath)")
                    print()
                    printHint("Drag a folder from Finder, or type the path.")
                    print()
                    continue
                }

                // Mirror benchmark classification: non-image files are treated as videos.
                let files = (try? fm.contentsOfDirectory(atPath: mediaPath)) ?? []
                var imageCount = 0
                var videoCount = 0
                for name in files {
                    guard !name.hasPrefix(".") else { continue }

                    let fullPath = "\(mediaPath)/\(name)"
                    var entryIsDir: ObjCBool = false
                    guard fm.fileExists(atPath: fullPath, isDirectory: &entryIsDir), !entryIsDir.boolValue else {
                        continue
                    }
                    guard let attrs = try? fm.attributesOfItem(atPath: fullPath),
                          let size = attrs[.size] as? Int,
                          size > 0 else {
                        continue
                    }

                    if ImageProcessor.isImage(path: fullPath) {
                        imageCount += 1
                    } else {
                        videoCount += 1
                    }
                }

                if imageCount == 0 || videoCount == 0 {
                    redraw(completed)
                    printStep(9, of: totalBasicSteps, "Benchmark")
                    if imageCount == 0 && videoCount == 0 {
                        printError("No supported media files found in: \(mediaPath)")
                    } else if imageCount == 0 {
                        printError("No images found in: \(mediaPath)")
                    } else {
                        printError("No videos found in: \(mediaPath)")
                    }
                    printHint("Folder needs both images and videos for the concurrency sweep.")
                    printHint("Detected: \(imageCount) image(s), \(videoCount) video(s).")
                    print()
                    continue
                }
                break
            }

            print()
            let result = runDelegatedPipelineBenchmark(repoRoot: repoRoot, mediaPath: mediaPath, showProgress: true)
            print()
            switch result {
            case .success(let rec):
                benchmarkResult = rec
                printSuccess("Benchmark complete.")
                print()
                print("  \(dim)Images:\(reset) \(bold)\(rec.imageConcurrency)\(reset) concurrent")
                print("  \(dim)Videos:\(reset) \(bold)\(rec.videoConcurrency)\(reset) concurrent")
            case .fallback(let reason):
                printWarning("Benchmark did not produce a result: \(reason)")
                printHint("Using defaults. You can change them in Advanced Config.")
            }
            print()
            waitForEnter()
        }
        let benchmarkStatus: String
        if let rec = benchmarkResult {
            benchmarkStatus = "images \(rec.imageConcurrency), videos \(rec.videoConcurrency)"
        } else {
            benchmarkStatus = "defaults"
        }
        completed.append(("Step 9 · Benchmark", benchmarkStatus))

        if let rec = benchmarkResult {
            advancedOverrides["MAX_CONCURRENT_IMAGES"] = String(rec.imageConcurrency)
            advancedOverrides["MAX_CONCURRENT_VIDEOS"] = String(rec.videoConcurrency)
        }

        // ── Step 10: Advanced Configuration ──

        redraw(completed)
        printStep(10, of: totalBasicSteps, "Advanced Configuration")
        print()
        printBody("Tune processing limits, security thresholds, ports, and more.")
        if benchmarkResult != nil {
            printHint("Adjust from benchmark findings.")
        } else {
            printHint("Defaults work well for most deployments.")
        }
        print()

        // Build advanced dict: start from defaults, apply any previous overrides
        var advanced: [String: String] = [:]
        for (key, spec) in defaults {
            advanced[key] = advancedOverrides[key] ?? spec.defaultValue
        }

        var answers = Answers(
            domain: domain,
            wireguardBindIP: wireguardIP,
            cloudflareToken: cloudflareToken,
            turnstileSecret: turnstileSecret,
            turnstileSitekey: turnstileSitekey,
            sessionHmacSecret: sessionSecret,
            internalAuthSecret: internalAuthSecret,
            gateSecret: gateSecret,
            ssdPath: ssdPath,
            eventTimezone: eventTimezone,
            advanced: advanced
        )

        let advancedEffectiveDefaults = effectiveAdvancedDefaultOverrides(
            domain: domain,
            ssdPath: ssdPath,
            eventTimezone: eventTimezone
        )

        // Sync basic fields that overlap with advanced
        answers.advanced["EVENT_TIMEZONE"] = eventTimezone
        answers.advanced["EXTERNAL_SSD_PATH"] = ssdPath
        answers.advanced["TURNSTILE_EXPECTED_HOSTNAME"] = domain

        if confirm("Configure advanced options?") {
            runAdvancedConfig(
                &answers,
                defaults: defaults,
                effectiveDefaultOverrides: advancedEffectiveDefaults,
                completedSteps: completed
            )
        }

        // Save overrides for re-run
        advancedOverrides = answers.advanced

        let advChanges = countAdvancedChanges(
            advancedValues: answers.advanced,
            defaults: defaults,
            effectiveDefaultOverrides: advancedEffectiveDefaults
        )
        completed.append(("Step 10 · Advanced", advChanges == 0 ? "defaults" : "\(advChanges) changes"))

        // ── Step 11: Thunderbolt Offload ──

        redraw(completed)
        printStep(11, of: totalBasicSteps, "Thunderbolt Offload")
        print()
        printBody("Optional: prepare the worker artifact, benchmark workers, and write Thunderbolt worker settings.")
        printHint("Production runtime still depends on VIDEO_SCHEDULER_POLICY and startup readiness gates.")
        print()
        shouldConfigureThunderbolt = confirm("Do you have Thunderbolt-connected worker Macs")
        completed.append(("Step 11 · Thunderbolt", shouldConfigureThunderbolt ? "enabled" : "skipped"))

        // ── Generate ──

        redraw(completed)
        printSectionTitle("Review")
        if confirm("Generate config files with these values") {
            redraw(completed)
            printSectionTitle("Generate")
            try generateFiles(answers: answers, repoRoot: repoRoot, home: home)

            if shouldConfigureThunderbolt {
                print()
                let tbExit = runThunderboltCommand(repoRoot: repoRoot, home: home)
                if tbExit != 0 {
                    printWarning("Thunderbolt flow did not complete successfully.")
                    if !confirm("Continue setup without Thunderbolt offload configuration") {
                        redraw(completed)
                        printSectionTitle("Setup Complete")
                        printBody("\(dim)Config files were generated. Services were not restarted.\(reset)")
                        print()
                        return
                    }
                }
            }

            print()
            waitForEnter()

            redraw(completed)
            printSectionTitle("Build")
            let buildReady = offerBuild(repoRoot: repoRoot)
            if !buildReady {
                print()
                printWarning("kiko-media release binary is not ready.")
                printHint("Services may fail to start without .build/release/KikoMedia.")
                if confirm("Stop here without restarting services") {
                    redraw(completed)
                    printSectionTitle("Setup Complete")
                    printBody("\(dim)Config files were generated. Services were not restarted.\(reset)")
                    print()
                    return
                }
                if !confirm("Continue and attempt restart anyway") {
                    redraw(completed)
                    printSectionTitle("Setup Complete")
                    printBody("\(dim)Config files were generated. Services were not restarted.\(reset)")
                    print()
                    return
                }
            }

            redraw(completed)
            printSectionTitle("Flush")
            printHint("Deletes runtime derived state: metadata.db, thumbs/, previews/.")
            var shouldFlush = confirm("Flush runtime derived state before restart")
            if shouldFlush {
                print()
                printWarning("Flush is destructive and cannot be undone.")
                printHint("Preserved: uploads/, moderated/, archived originals on SSD.")
                print()
                if !confirm("Confirm flush now") {
                    shouldFlush = false
                }
            }
            if shouldFlush && !fm.fileExists(atPath: answers.ssdPath) {
                printError("Cannot flush: SSD path is not available: \(answers.ssdPath)")
                if confirm("Continue with restart only (no flush)") {
                    shouldFlush = false
                } else {
                    redraw(completed)
                    printSectionTitle("Setup Complete")
                    printBody("\(dim)Config files were generated. Services were not restarted.\(reset)")
                    print()
                    return
                }
            }

            redraw(completed)
            printSectionTitle("Applying Service Lifecycle")
            _ = enableSleepPrevention()
            bootoutAllServices(home: home)

            var flushOK = true
            if shouldFlush {
                let baseDir = try expandedBaseDirectory(from: answers)
                printHint("Flushing metadata + derived files in \(baseDir)")
                flushOK = flushRuntimeState(baseDir: baseDir)
            }

            print()
            let startOK = bootstrapAllServices(home: home)

            let publicPort = answers.advanced["PUBLIC_PORT"] ?? "3001"
            if !startOK || (shouldFlush && !flushOK) {
                print()
                waitForEnter("Press Enter for summary")
            }
            redraw(completed)
            printSectionTitle("Setup Complete")
            if shouldFlush {
                if flushOK {
                    printBody("\(dim)Flush:\(reset) metadata + derived files reset; uploads/moderation preserved")
                } else {
                    printBody("\(yellow)!\(reset) Flush had errors; review messages above")
                }
                print()
            }
            if startOK {
                printBody("\(bold)Verify:\(reset)")
            } else {
                printBody("\(yellow)!\(reset) One or more services failed to start. Retry manually:")
                print("\(listItemIndent)\(bold)launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.caddy.plist\(reset)")
                print("\(listItemIndent)\(bold)launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.tusd.plist\(reset)")
                print("\(listItemIndent)\(bold)launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist\(reset)")
                print()
                printBody("\(bold)Then verify:\(reset)")
            }
            print("\(listItemIndent)\(bold)curl http://127.0.0.1:\(publicPort)/health\(reset)")
            print("\(listItemIndent)\(bold)\(resolveModerationUIURL(repoRoot: repoRoot))\(reset) \(dim)(moderation UI)\(reset)")
            print()
            printBody("\(dim)See docs/runbook.md §§ 5, 9, and 6 for startup, restart, and moderation details.\(reset)")
            print()

            return
        }
        print()
        if confirm("Start over to make changes") {
            continue
        }
        print("\n  \(dim)Cancelled. No files were changed.\(reset)\n")
        exit(0)
    }
}
