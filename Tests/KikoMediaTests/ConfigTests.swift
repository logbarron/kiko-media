import Testing
import Foundation
@testable import KikoMediaCore
@testable import KikoMediaApp
@testable import Orchestrator

private func withEnvValue<T: Sendable>(
    _ key: String,
    _ value: String?,
    _ body: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await TestEnvironment.withEnvironment([key: value], body)
}

private enum DefaultsEnvFixture {
    enum SpecType: String {
        case int
        case double
        case string
    }

    struct Spec {
        let type: SpecType
        let defaultValue: String
        let min: String
        let max: String
    }

    static func loadSpecs() throws -> [String: Spec] {
        let defaultsURL = try TestRepositoryRoot.resolve(
            from: #filePath,
            sentinels: ["Package.swift"]
        ).appendingPathComponent("deploy/defaults.env")
        let content = try String(contentsOf: defaultsURL, encoding: .utf8)

        var result: [String: Spec] = [:]
        for line in content.split(separator: "\n", omittingEmptySubsequences: false) {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty || trimmed.hasPrefix("#") { continue }

            let parts = trimmed
                .split(separator: "|", maxSplits: 6, omittingEmptySubsequences: false)
                .map(String.init)
            #expect(parts.count == 7, "Malformed defaults line: \(trimmed)")
            guard parts.count == 7 else { continue }

            guard let type = SpecType(rawValue: parts[1]) else {
                #expect(Bool(false), "Unsupported defaults type '\(parts[1])' for key \(parts[0])")
                continue
            }
            result[parts[0]] = Spec(
                type: type,
                defaultValue: parts[2],
                min: parts[3],
                max: parts[4]
            )
        }
        return result
    }

    static func intRange(from spec: Spec) -> ClosedRange<Int>? {
        guard !spec.min.isEmpty, !spec.max.isEmpty else { return nil }
        guard let lower = Int(spec.min), let upper = Int(spec.max) else { return nil }
        return lower...upper
    }

    static func doubleRange(from spec: Spec) -> ClosedRange<Double>? {
        guard !spec.min.isEmpty, !spec.max.isEmpty else { return nil }
        guard let lower = Double(spec.min), let upper = Double(spec.max) else { return nil }
        return lower...upper
    }

}

private enum TemplateWiringFixture {
    struct GeneratedSetupArtifacts {
        let repoRoot: URL
        let indexContent: String
        let caddyPlistContent: String
        let mediaPlistContent: String
    }

    static let templatePaths = [
        "deploy/index.html.template",
        "deploy/Caddyfile.template",
        "deploy/launchd/com.kiko.caddy.plist",
        "deploy/launchd/com.kiko.tusd.plist",
        "deploy/launchd/com.kiko.media.plist",
        "deploy/worker.swift.template",
    ]

    static let explicitAliasSourceToPlaceholder: [String: String] = [
        "TURNSTILE_EXPECTED_ACTION": "TURNSTILE_ACTION",
        "TURNSTILE_EXPECTED_CDATA": "TURNSTILE_CDATA",
    ]

    static let allowedCustomTemplateKeys: Set<String> = [
        "CLOUDFLARE_API_TOKEN",
        "CSP_SCRIPT_HASH",
        "CSP_STYLE_HASH",
        "DOMAIN",
        "GATE_ENABLED",
        "GATE_SECRET",
        "HOME",
        "INTERNAL_AUTH_SECRET",
        "REPO_DIR",
        "SESSION_HMAC_SECRET",
        "TURNSTILE_ACTION",
        "TURNSTILE_CDATA",
        "TURNSTILE_SECRET",
        "TURNSTILE_SITEKEY",
        "WIREGUARD_BIND_IP",
        "WORK_DIR",
    ]

    static func repoRoot() throws -> URL {
        try TestRepositoryRoot.resolve(
            from: #filePath,
            sentinels: ["Package.swift"]
        )
    }

    static func placeholderKeys(in relativePath: String, repoRoot: URL) throws -> Set<String> {
        let fileURL = repoRoot.appendingPathComponent(relativePath)
        let content = try String(contentsOf: fileURL, encoding: .utf8)
        let pattern = "__([A-Z0-9_]+)__"
        let regex = try NSRegularExpression(pattern: pattern)
        let range = NSRange(content.startIndex..<content.endIndex, in: content)
        let matches = regex.matches(in: content, options: [], range: range)

        var keys = Set<String>()
        keys.reserveCapacity(matches.count)
        for match in matches {
            guard let keyRange = Range(match.range(at: 1), in: content) else { continue }
            keys.insert(String(content[keyRange]))
        }
        return keys
    }

    static func allTemplatePlaceholderKeys() throws -> Set<String> {
        let root = try repoRoot()
        var keys = Set<String>()
        for path in templatePaths {
            keys.formUnion(try placeholderKeys(in: path, repoRoot: root))
        }
        return keys
    }

    static func templateContent(at relativePath: String) throws -> String {
        let root = try repoRoot()
        let fileURL = root.appendingPathComponent(relativePath)
        return try String(contentsOf: fileURL, encoding: .utf8)
    }

    static func copyDeployDirectory(to repoRoot: URL) throws {
        let source = try self.repoRoot().appendingPathComponent("deploy", isDirectory: true)
        let destination = repoRoot.appendingPathComponent("deploy", isDirectory: true)
        try FileManager.default.copyItem(at: source, to: destination)
    }

    static func defaultsValues(overrides: [String: String] = [:]) throws -> [String: String] {
        var values = Dictionary(
            uniqueKeysWithValues: try DefaultsEnvFixture.loadSpecs().map { ($0.key, $0.value.defaultValue) }
        )
        for (key, value) in overrides {
            values[key] = value
        }
        return values
    }

    static func generateSetupArtifacts(
        gateSecret: String = "invite-only",
        turnstileAction: String = "guest-upload",
        turnstileCData: String = "event-2026",
        sessionHmacSecret: String = String(repeating: "s", count: 32),
        internalAuthSecret: String = "internal-auth-secret"
    ) throws -> GeneratedSetupArtifacts {
        let fm = FileManager.default
        let tempRepo = fm.temporaryDirectory.appendingPathComponent("kiko-config-filegen-\(UUID().uuidString)")
        try fm.createDirectory(at: tempRepo, withIntermediateDirectories: true)

        try copyDeployDirectory(to: tempRepo)
        let home = tempRepo.appendingPathComponent("home")
        let ssdRoot = tempRepo.appendingPathComponent("ssd")
        try fm.createDirectory(at: home, withIntermediateDirectories: true)
        try fm.createDirectory(at: ssdRoot, withIntermediateDirectories: true)

        let answers = Answers(
            domain: "example.com",
            wireguardBindIP: "10.0.0.2",
            cloudflareToken: String(repeating: "a", count: 40),
            turnstileSecret: "turnstile-secret",
            turnstileSitekey: "turnstile-sitekey",
            sessionHmacSecret: sessionHmacSecret,
            internalAuthSecret: internalAuthSecret,
            gateSecret: gateSecret,
            ssdPath: ssdRoot.appendingPathComponent("originals").path,
            eventTimezone: "",
            advanced: try defaultsValues(overrides: [
                "BASE_DIRECTORY": tempRepo.appendingPathComponent("runtime").path,
                "TURNSTILE_EXPECTED_ACTION": turnstileAction,
                "TURNSTILE_EXPECTED_CDATA": turnstileCData,
            ])
        )
        try generateFiles(answers: answers, repoRoot: tempRepo.path, home: home.path)

        let launchAgents = home.appendingPathComponent("Library/LaunchAgents", isDirectory: true)
        return GeneratedSetupArtifacts(
            repoRoot: tempRepo,
            indexContent: try String(
                contentsOf: tempRepo.appendingPathComponent("deploy/index.html"),
                encoding: .utf8
            ),
            caddyPlistContent: try String(
                contentsOf: launchAgents.appendingPathComponent("com.kiko.caddy.plist"),
                encoding: .utf8
            ),
            mediaPlistContent: try String(
                contentsOf: launchAgents.appendingPathComponent("com.kiko.media.plist"),
                encoding: .utf8
            )
        )
    }
}

// MARK: - defaults.env Source of Truth

@Suite("Config Defaults Source of Truth")
struct ConfigDefaultsSourceOfTruthTests {
    @Test("int defaults and ranges come from deploy/defaults.env")
    func intDefaultsMatchDefaultsEnv() throws {
        let specs = try DefaultsEnvFixture.loadSpecs()
        let expectedKeys = Set(
            specs
                .filter { $0.value.type == .int }
                .map(\.key)
        )
        #expect(Set(Config.intDefaults.keys) == expectedKeys)

        for key in expectedKeys.sorted() {
            guard let fileSpec = specs[key],
                  let fallback = Int(fileSpec.defaultValue),
                  let runtimeSpec = Config.intDefaults[key] else {
                #expect(Bool(false), "Missing or invalid int spec for \(key)")
                continue
            }
            #expect(runtimeSpec.fallback == fallback, "\(key) fallback mismatch")
            #expect(runtimeSpec.range == DefaultsEnvFixture.intRange(from: fileSpec), "\(key) range mismatch")
        }
    }

    @Test("double defaults and ranges come from deploy/defaults.env")
    func doubleDefaultsMatchDefaultsEnv() throws {
        let specs = try DefaultsEnvFixture.loadSpecs()
        let expectedKeys = Set(
            specs
                .filter { $0.value.type == .double }
                .map(\.key)
        )
        #expect(Set(Config.doubleDefaults.keys) == expectedKeys)

        for key in expectedKeys.sorted() {
            guard let fileSpec = specs[key],
                  let fallback = Double(fileSpec.defaultValue),
                  let runtimeSpec = Config.doubleDefaults[key] else {
                #expect(Bool(false), "Missing or invalid double spec for \(key)")
                continue
            }
            #expect(runtimeSpec.fallback == fallback, "\(key) fallback mismatch")
            #expect(runtimeSpec.range == DefaultsEnvFixture.doubleRange(from: fileSpec), "\(key) range mismatch")
        }
    }

    @Test("string defaults come from deploy/defaults.env")
    func stringDefaultsMatchDefaultsEnv() throws {
        let specs = try DefaultsEnvFixture.loadSpecs()
        let expected = Dictionary(
            uniqueKeysWithValues: specs
                .filter { $0.value.type == .string }
                .map { ($0.key, $0.value.defaultValue) }
        )
        #expect(Config.stringDefaults == expected)
    }
}

@Suite("Orchestrator Template Wiring")
struct OrchestratorTemplateWiringTests {
    @Test("defaults.env keys are wired to templates or explicit aliases")
    func defaultsKeysWiredToTemplatesOrAliases() throws {
        let specs = try DefaultsEnvFixture.loadSpecs()
        let defaultsKeys = Set(specs.keys)
        let templateKeys = try TemplateWiringFixture.allTemplatePlaceholderKeys()
        let explicitAliasSourceKeys = Set(TemplateWiringFixture.explicitAliasSourceToPlaceholder.keys)

        let missing = defaultsKeys
            .subtracting(templateKeys)
            .subtracting(explicitAliasSourceKeys)
        #expect(
            missing.isEmpty,
            "defaults.env keys missing template wiring: \(missing.sorted().joined(separator: ", "))"
        )

        for aliasPlaceholder in TemplateWiringFixture.explicitAliasSourceToPlaceholder.values.sorted() {
            #expect(
                templateKeys.contains(aliasPlaceholder),
                "Missing alias placeholder __\(aliasPlaceholder)__ in deploy templates"
            )
        }
    }

    @Test("template custom placeholders are from approved dynamic set")
    func templateCustomPlaceholdersApproved() throws {
        let specs = try DefaultsEnvFixture.loadSpecs()
        let defaultsKeys = Set(specs.keys)
        let templateKeys = try TemplateWiringFixture.allTemplatePlaceholderKeys()
        let allowedCustom = TemplateWiringFixture.allowedCustomTemplateKeys
        let unexpected = templateKeys.subtracting(defaultsKeys).subtracting(allowedCustom)

        #expect(
            unexpected.isEmpty,
            "Unexpected custom placeholders: \(unexpected.sorted().joined(separator: ", "))"
        )
    }

    @Test("FileGeneration maps Turnstile alias placeholders")
    func fileGenerationMapsTurnstileAliasPlaceholders() throws {
        let action = "guest-upload"
        let cdata = "event-2026"
        let artifacts = try TemplateWiringFixture.generateSetupArtifacts(
            turnstileAction: action,
            turnstileCData: cdata
        )
        defer { try? FileManager.default.removeItem(at: artifacts.repoRoot) }

        #expect(artifacts.indexContent.contains("const TURNSTILE_ACTION = '\(action)';"))
        #expect(artifacts.indexContent.contains("const TURNSTILE_CDATA = '\(cdata)';"))
        #expect(!artifacts.indexContent.contains("__TURNSTILE_ACTION__"))
        #expect(!artifacts.indexContent.contains("__TURNSTILE_CDATA__"))
    }

    @Test("worker template uses canonical placeholder naming for thunderbolt settings")
    func workerTemplateUsesCanonicalThunderboltPlaceholderNaming() throws {
        let workerTemplateKeys = try TemplateWiringFixture.placeholderKeys(
            in: "deploy/worker.swift.template",
            repoRoot: TemplateWiringFixture.repoRoot()
        )
        let expectedKeys: Set<String> = [
            "TB_PORT",
            "WORK_DIR",
            "VIDEO_TRANSCODE_PRESET",
            "VIDEO_THUMBNAIL_SIZE",
            "VIDEO_THUMBNAIL_TIME",
            "VIDEO_THUMBNAIL_QUALITY",
            "VIDEO_TRANSCODE_TIMEOUT",
        ]
        #expect(workerTemplateKeys == expectedKeys)

        let legacyKeys: Set<String> = [
            "PORT",
            "PRESET",
            "THUMB_SIZE",
            "THUMB_TIME",
            "THUMB_QUALITY",
            "TRANSCODE_TIMEOUT",
        ]
        #expect(workerTemplateKeys.isDisjoint(with: legacyKeys))
    }

    @Test("Thunderbolt worker generation maps canonical worker placeholders")
    func thunderboltWorkerGenerationMapsCanonicalWorkerPlaceholders() throws {
        let repoRoot = try TemplateWiringFixture.repoRoot().path
        let settings = try #require(loadThunderboltWorkerTemplateSettings(repoRoot: repoRoot))

        let rendered = try processTemplate(
            at: "\(repoRoot)/deploy/worker.swift.template",
            replacements: thunderboltWorkerTemplateReplacements(settings: settings)
        )

        try assertNoUnreplacedPlaceholders(rendered, in: "deploy/worker.swift.template")
        #expect(rendered.contains("let port: UInt16 = \(settings.port)"))
        #expect(rendered.contains("let defaultWorkDir = \"\(settings.workDirPrefix)\""))
        #expect(rendered.contains("let transcodePreset = \"\(settings.transcodePreset)\""))
        #expect(rendered.contains("let thumbMaxSize = \(settings.thumbSize)"))
        #expect(rendered.contains("let thumbTime: Double = \(settings.thumbTime)"))
        #expect(rendered.contains("let thumbQuality: Double = \(settings.thumbQuality)"))
        #expect(rendered.contains("let transcodeTimeout = \(settings.transcodeTimeout)"))
    }

    @Test("FileGeneration derives GATE_ENABLED from gateSecret emptiness")
    func fileGenerationDerviesGateEnabled() throws {
        let gated = try TemplateWiringFixture.generateSetupArtifacts(gateSecret: "invite-only")
        defer { try? FileManager.default.removeItem(at: gated.repoRoot) }
        #expect(gated.indexContent.contains("const GATE_ENABLED = true;"))

        let open = try TemplateWiringFixture.generateSetupArtifacts(gateSecret: "")
        defer { try? FileManager.default.removeItem(at: open.repoRoot) }
        #expect(open.indexContent.contains("const GATE_ENABLED = false;"))
    }

    @Test("Caddy template injects internal auth header on privileged upstreams")
    func caddyTemplateInjectsInternalAuthHeader() throws {
        let caddyfile = try TemplateWiringFixture.templateContent(at: "deploy/Caddyfile.template")
        let pattern = #"header_up Authorization \{env\.INTERNAL_AUTH_SECRET\}"#
        let regex = try NSRegularExpression(pattern: pattern)
        let range = NSRange(caddyfile.startIndex..<caddyfile.endIndex, in: caddyfile)
        let matchCount = regex.numberOfMatches(in: caddyfile, range: range)
        #expect(
            matchCount >= 4,
            "Expected at least 4 Authorization header_up injections (internal API routes), found \(matchCount)"
        )
        #expect(!caddyfile.contains("{env.SESSION_HMAC_SECRET}"))
    }

    @Test("FileGeneration wires session and internal auth secrets for launchd plists")
    func fileGenerationWiresSessionAndInternalSecretsForLaunchdPlists() throws {
        let sessionSecret = String(repeating: "s", count: 32)
        let internalAuthSecret = "internal-auth-secret"
        let artifacts = try TemplateWiringFixture.generateSetupArtifacts(
            sessionHmacSecret: sessionSecret,
            internalAuthSecret: internalAuthSecret
        )
        defer { try? FileManager.default.removeItem(at: artifacts.repoRoot) }

        #expect(artifacts.mediaPlistContent.contains("<key>SESSION_HMAC_SECRET</key>"))
        #expect(artifacts.mediaPlistContent.contains("<string>\(sessionSecret)</string>"))
        #expect(artifacts.mediaPlistContent.contains("<key>INTERNAL_AUTH_SECRET</key>"))
        #expect(artifacts.mediaPlistContent.contains("<string>\(internalAuthSecret)</string>"))
        #expect(artifacts.caddyPlistContent.contains("<key>INTERNAL_AUTH_SECRET</key>"))
        #expect(artifacts.caddyPlistContent.contains("<string>\(internalAuthSecret)</string>"))
    }

    @Test("FileGeneration wires GATE_SECRET for media launchd plist")
    func fileGenerationWiresGateSecretForMedia() throws {
        let gateSecret = "invite-only"
        let artifacts = try TemplateWiringFixture.generateSetupArtifacts(gateSecret: gateSecret)
        defer { try? FileManager.default.removeItem(at: artifacts.repoRoot) }

        #expect(artifacts.mediaPlistContent.contains("<key>GATE_SECRET</key>"))
        #expect(artifacts.mediaPlistContent.contains("<string>\(gateSecret)</string>"))
    }

    @Test("media launchd template exposes GATE_SECRET environment variable")
    func mediaLaunchdTemplateExposesGateSecret() throws {
        let mediaPlist = try TemplateWiringFixture.templateContent(at: "deploy/launchd/com.kiko.media.plist")
        #expect(mediaPlist.contains("<key>GATE_SECRET</key>"))
        #expect(mediaPlist.contains("__GATE_SECRET__"))
    }
}

// MARK: - Env Var Override

@Suite("Config Env Var Override")
struct ConfigEnvVarOverrideTests {

    @Test("Int env var override works")
    func intOverride() async throws {
        let key = "PUBLIC_PORT"
        try await withEnvValue(key, "9999") {
            #expect(Config.envInt(key) == 9999)
        }
    }

    @Test("Double env var override works")
    func doubleOverride() async throws {
        let key = "THUMBNAIL_QUALITY"
        try await withEnvValue(key, "0.50") {
            #expect(Config.envDouble(key) == 0.50)
        }
    }

    @Test("String env var override works")
    func stringOverride() async throws {
        let key = "BIND_ADDRESS"
        try await withEnvValue(key, "0.0.0.0") {
            #expect(Config.envString(key) == "0.0.0.0")
        }
    }
}

// MARK: - Invalid Env Var Fallback

@Suite("Config Invalid Env Var Fallback")
struct ConfigInvalidFallbackTests {

    @Test("Invalid int env var falls back to default")
    func invalidIntFallback() async throws {
        let key = "PUBLIC_PORT"
        try await withEnvValue(key, "abc") {
            #expect(Config.envInt(key) == Config.intDefaults[key]!.fallback)
        }
    }

    @Test("Invalid double env var falls back to default")
    func invalidDoubleFallback() async throws {
        let key = "THUMBNAIL_QUALITY"
        try await withEnvValue(key, "not-a-number") {
            #expect(Config.envDouble(key) == Config.doubleDefaults[key]!.fallback)
        }
    }

    @Test("Empty string env var falls back to default")
    func emptyStringFallback() async throws {
        let key = "BIND_ADDRESS"
        try await withEnvValue(key, "") {
            #expect(Config.envString(key) == Config.stringDefaults[key]!)
        }
    }
}

// MARK: - Out-of-Range Fallback

@Suite("Config Out-of-Range Fallback")
struct ConfigOutOfRangeFallbackTests {

    @Test("Int below range falls back to default")
    func intBelowRange() async throws {
        let key = "PUBLIC_PORT"
        try await withEnvValue(key, "0") {
            #expect(Config.envInt(key) == Config.intDefaults[key]!.fallback)
        }
    }

    @Test("Int above range falls back to default")
    func intAboveRange() async throws {
        let key = "PUBLIC_PORT"
        try await withEnvValue(key, "70000") {
            #expect(Config.envInt(key) == Config.intDefaults[key]!.fallback)
        }
    }

    @Test("Double below range falls back to default")
    func doubleBelowRange() async throws {
        let key = "MAX_COMPRESSION_RATIO"
        try await withEnvValue(key, "0.5") {
            #expect(Config.envDouble(key) == Config.doubleDefaults[key]!.fallback)
        }
    }

    @Test("Double above range falls back to default")
    func doubleAboveRange() async throws {
        let key = "THUMBNAIL_QUALITY"
        try await withEnvValue(key, "1.5") {
            #expect(Config.envDouble(key) == Config.doubleDefaults[key]!.fallback)
        }
    }

    @Test("Int with nil range accepts any value")
    func intNilRangeAcceptsAny() async throws {
        let key = "SQLITE_CACHE_SIZE"
        try await withEnvValue(key, "-999999") {
            #expect(Config.envInt(key) == -999999)
        }
    }
}

// MARK: - String Defaults Regression

@Suite("Config String Defaults Regression")
struct ConfigStringDefaultsRegressionTests {

    @Test("Critical string keys have non-empty defaults", arguments: [
        "BASE_DIRECTORY",
        "BIND_ADDRESS",
        "SESSION_COOKIE_NAME",
        "VIDEO_TRANSCODE_PRESET",
    ])
    func criticalStringDefaultsNotEmpty(key: String) {
        let value = Config.stringDefaults[key]
        #expect(value != nil)
        #expect(value != "")
    }
}

// MARK: - No Unintended Zero Fallback

@Suite("Config No Zero Fallback")
struct ConfigNoZeroFallbackTests {

    @Test("No int key that should be positive has fallback of zero")
    func noUnintendedZeroFallback() {
        let allowedZeroKeys: Set<String> = []
        for (key, spec) in Config.intDefaults where !allowedZeroKeys.contains(key) {
            #expect(spec.fallback != 0, "Key \(key) has unexpected zero fallback")
        }
    }
}

// MARK: - Video Transcode Preset Validation

@Suite("Config Video Transcode Preset Validation")
struct ConfigVideoTranscodePresetValidationTests {

    @Test("Valid VIDEO_TRANSCODE_PRESET is accepted")
    func validPresetAccepted() throws {
        let supportedPresets = [
            "AVAssetExportPreset1280x720",
            "AVAssetExportPreset1920x1080",
        ]

        let preset = try Config.validatedVideoTranscodePreset(
            "AVAssetExportPreset1920x1080",
            supportedPresets: supportedPresets
        )
        #expect(preset == "AVAssetExportPreset1920x1080")
    }

    @Test("Invalid VIDEO_TRANSCODE_PRESET fails closed with supported list")
    func invalidPresetFailsClosed() {
        let supportedPresets = [
            "AVAssetExportPreset1920x1080",
            "AVAssetExportPreset1280x720",
            "AVAssetExportPreset1920x1080",
        ]

        #expect {
            _ = try Config.validatedVideoTranscodePreset(
                "AVAssetExportPresetTypo",
                supportedPresets: supportedPresets
            )
        } throws: { error in
            guard let configError = error as? VideoTranscodePresetConfigurationError else {
                return false
            }
            guard case let .unsupportedPreset(preset, available) = configError else {
                return false
            }
            return preset == "AVAssetExportPresetTypo" &&
                available == ["AVAssetExportPreset1280x720", "AVAssetExportPreset1920x1080"]
        }
    }
}

// MARK: - Video Scheduler Policy Validation

@Suite("Config Video Scheduler Policy Validation")
struct ConfigVideoSchedulerPolicyValidationTests {
    @Test("Valid VIDEO_SCHEDULER_POLICY is accepted and normalized")
    func validPolicyAcceptedAndNormalized() throws {
        let policy = try Config.validatedVideoSchedulerPolicy(" FIFO \n")
        #expect(policy == .fifo)
    }

    @Test("Invalid VIDEO_SCHEDULER_POLICY fails closed with supported list")
    func invalidPolicyFailsClosed() {
        #expect {
            _ = try Config.validatedVideoSchedulerPolicy("balanced")
        } throws: { error in
            guard let configError = error as? VideoSchedulerPolicyConfigurationError else {
                return false
            }
            guard case let .unsupportedPolicy(policy, supportedPolicies) = configError else {
                return false
            }
            return policy == "balanced" &&
                supportedPolicies == ["auto", "fifo", "none"]
        }
    }

    @Test("Config.load parses VIDEO_SCHEDULER_POLICY")
    func configLoadParsesVideoSchedulerPolicy() async throws {
        let defaultPreset = Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? "AVAssetExportPreset1920x1080"
        try await TestEnvironment.withEnvironment([
            "VIDEO_SCHEDULER_POLICY": "none",
            "VIDEO_TRANSCODE_PRESET": defaultPreset,
        ]) {
            let config = try Config.load()
            #expect(config.videoSchedulerPolicy == .none)
        }
    }

    @Test("Config.load rejects invalid VIDEO_SCHEDULER_POLICY")
    func configLoadRejectsInvalidVideoSchedulerPolicy() async throws {
        let defaultPreset = Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? "AVAssetExportPreset1920x1080"
        let _: Void = try await TestEnvironment.withEnvironment([
            "VIDEO_SCHEDULER_POLICY": "balanced",
            "VIDEO_TRANSCODE_PRESET": defaultPreset,
        ]) {
            #expect(throws: VideoSchedulerPolicyConfigurationError.self) {
                _ = try Config.load()
            }
        }
    }
}

// MARK: - Thunderbolt Worker Parsing

@Suite("Config Thunderbolt Worker Parsing")
struct ConfigThunderboltWorkerParsingTests {
    @Test("Parser returns typed workers for valid entries")
    func parserValidEntries() {
        let workers = Config.parseThunderboltWorkers("192.168.100.10:2,worker.local:4")
        #expect(
            workers == [
                Config.ThunderboltWorker(host: "192.168.100.10", slots: 2),
                Config.ThunderboltWorker(host: "worker.local", slots: 4),
            ]
        )
    }

    @Test("Parser trims whitespace and drops invalid entries")
    func parserDropsInvalidEntries() {
        let workers = Config.parseThunderboltWorkers(" 192.168.100.10 : 2 ,bad,:3,host:-1,host:0,host:abc,worker.local:5 ")
        #expect(
            workers == [
                Config.ThunderboltWorker(host: "192.168.100.10", slots: 2),
                Config.ThunderboltWorker(host: "worker.local", slots: 5),
            ]
        )
    }

    @Test("Config init parses TB_WORKERS into typed workers")
    func configInitParsesWorkers() {
        let config = Config(
            publicPort: 3001,
            internalPort: 3002,
            uploadDir: "/tmp/uploads",
            thumbsDir: "/tmp/thumbs",
            previewsDir: "/tmp/previews",
            logsDir: "/tmp/logs",
            externalSSDPath: "",
            databasePath: "/tmp/metadata.db",
            turnstileSecret: nil,
            sessionHmacSecret: nil,
            tbWorkers: "192.168.100.10:2,192.168.100.11:3"
        )

        #expect(
            config.thunderboltWorkers == [
                Config.ThunderboltWorker(host: "192.168.100.10", slots: 2),
                Config.ThunderboltWorker(host: "192.168.100.11", slots: 3),
            ]
        )
    }

    @Test("Config.load uses the same typed TB_WORKERS parse path")
    func configLoadParsesWorkers() async throws {
        let defaultPreset = Config.stringDefaults["VIDEO_TRANSCODE_PRESET"] ?? "AVAssetExportPreset1920x1080"
        try await TestEnvironment.withEnvironment([
            "TB_WORKERS": "192.168.100.10:2, 192.168.100.11:3",
            "VIDEO_TRANSCODE_PRESET": defaultPreset,
        ]) {
            let config = try Config.load()
            #expect(
                config.thunderboltWorkers == [
                    Config.ThunderboltWorker(host: "192.168.100.10", slots: 2),
                    Config.ThunderboltWorker(host: "192.168.100.11", slots: 3),
                ]
            )
        }
    }
}
