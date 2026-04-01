import Foundation
import Darwin

// MARK: - Service Lifecycle

struct LaunchdService {
    let label: String
    let plist: String

    var launchdLabel: String {
        plist.replacingOccurrences(of: ".plist", with: "")
    }
}

let stopOrder: [LaunchdService] = [
    .init(label: "kiko-media", plist: "com.kiko.media.plist"),
    .init(label: "tusd", plist: "com.kiko.tusd.plist"),
    .init(label: "caddy", plist: "com.kiko.caddy.plist"),
]

let startOrder: [LaunchdService] = [
    .init(label: "caddy", plist: "com.kiko.caddy.plist"),
    .init(label: "tusd", plist: "com.kiko.tusd.plist"),
    .init(label: "kiko-media", plist: "com.kiko.media.plist"),
]

func launchAgentPath(home: String, plist: String) -> String {
    "\(home)/Library/LaunchAgents/\(plist)"
}

func firstLine(_ output: String) -> String {
    output.split(separator: "\n").first.map(String.init) ?? ""
}

func isLaunchAgentLoaded(_ label: String) -> Bool {
    let uid = getuid()
    let (exitCode, _) = runProcess(
        executable: "/bin/launchctl",
        arguments: ["print", "gui/\(uid)/\(label)"]
    )
    return exitCode == 0
}

func loadedKikoServices() -> [LaunchdService] {
    stopOrder.filter { isLaunchAgentLoaded($0.launchdLabel) }
}

let caffeinateJobLabel = "com.kiko.media.caffeinate"

func isSleepPreventionEnabled() -> Bool {
    isLaunchAgentLoaded(caffeinateJobLabel)
}

@discardableResult
func enableSleepPrevention() -> Bool {
    if isSleepPreventionEnabled() { return true }

    let (exitCode, output) = runProcess(
        executable: "/bin/launchctl",
        arguments: ["submit", "-l", caffeinateJobLabel, "--", "/usr/bin/caffeinate", "-s"]
    )
    if exitCode == 0 {
        printSuccess("Sleep prevention enabled.")
        return true
    }

    let summary = firstLine(output)
    if summary.isEmpty {
        printWarning("Could not enable sleep prevention.")
    } else {
        printWarning("Could not enable sleep prevention: \(summary)")
    }
    printHint("Manual: caffeinate -s")
    return false
}

@discardableResult
func disableSleepPrevention() -> Bool {
    if !isSleepPreventionEnabled() { return true }

    let (exitCode, output) = runProcess(
        executable: "/bin/launchctl",
        arguments: ["remove", caffeinateJobLabel]
    )
    if exitCode == 0 {
        printSuccess("Sleep prevention disabled.")
        return true
    }

    let summary = firstLine(output)
    if summary.isEmpty {
        printWarning("Could not disable sleep prevention.")
    } else {
        printWarning("Could not disable sleep prevention: \(summary)")
    }
    printHint("Manual: launchctl remove \(caffeinateJobLabel)")
    return false
}

enum ConfigurationState {
    case missing
    case partial(found: Int, total: Int, missing: [String])
    case complete

    var canStartServices: Bool {
        if case .complete = self { return true }
        return false
    }
}

struct ConfigArtifact {
    let path: String
    let name: String
}

func expectedConfigArtifacts(home: String, repoRoot: String) -> [ConfigArtifact] {
    [
        .init(path: "\(repoRoot)/deploy/index.html", name: "deploy/index.html"),
        .init(path: "\(repoRoot)/deploy/Caddyfile", name: "deploy/Caddyfile"),
        .init(path: launchAgentPath(home: home, plist: "com.kiko.caddy.plist"), name: "com.kiko.caddy.plist"),
        .init(path: launchAgentPath(home: home, plist: "com.kiko.tusd.plist"), name: "com.kiko.tusd.plist"),
        .init(path: launchAgentPath(home: home, plist: "com.kiko.media.plist"), name: "com.kiko.media.plist"),
    ]
}

func detectConfigurationState(home: String, repoRoot: String) -> ConfigurationState {
    let fm = FileManager.default
    let artifacts = expectedConfigArtifacts(home: home, repoRoot: repoRoot)
    let found = artifacts.filter { fm.fileExists(atPath: $0.path) }
    if found.isEmpty { return .missing }
    if found.count == artifacts.count { return .complete }

    let missing = artifacts
        .filter { !fm.fileExists(atPath: $0.path) }
        .map(\.name)
    return .partial(found: found.count, total: artifacts.count, missing: missing)
}

func printServiceState(_ loaded: [LaunchdService]) {
    if loaded.isEmpty {
        printField("Services", "not running.")
        return
    }

    printField("Services", "running (\(loaded.count) of \(stopOrder.count)).")
    for service in loaded {
        print("\(listItemIndent)\(dim)-\(reset) \(service.launchdLabel)")
    }
}

func printSleepPreventionState(enabled: Bool? = nil) {
    let isEnabled = enabled ?? isSleepPreventionEnabled()
    if isEnabled {
        printField("Sleep prevention", "enabled \(dim)(caffeinate -s)\(reset).")
    } else {
        printField("Sleep prevention", "off.")
    }
}

func printConfigurationState(_ state: ConfigurationState) {
    switch state {
    case .missing:
        printField("Configuration files", "not found yet.")
    case .complete:
        printField("Configuration files", "ready.")
    case .partial(let found, let total, let missing):
        printField("Configuration files", "partial (\(found) of \(total) found).")
        let preview = missing.prefix(3).joined(separator: ", ")
        if !preview.isEmpty {
            printHint("Missing: \(preview)")
        }
    }
}

func bootoutAllServices(home: String) {
    let uid = getuid()
    for service in stopOrder {
        if !isLaunchAgentLoaded(service.launchdLabel) {
            printHint("\(service.label) already stopped.")
            continue
        }
        let plistPath = launchAgentPath(home: home, plist: service.plist)
        let (exitCode, output) = runProcess(
            executable: "/bin/launchctl",
            arguments: ["bootout", "gui/\(uid)", plistPath]
        )
        if exitCode == 0 {
            printSuccess("Stopped \(service.label)")
        } else {
            let summary = firstLine(output)
            if summary.isEmpty {
                printWarning("Could not stop \(service.label). \(dim)(continuing)\(reset)")
                continue
            }
            printWarning("Could not stop \(service.label): \(summary) \(dim)(continuing)\(reset)")
        }
    }
}

@discardableResult
func reloadCaddyIfRunning() -> Bool {
    let caddyLabel = "com.kiko.caddy"
    guard isLaunchAgentLoaded(caddyLabel) else {
        printHint("caddy is not running. Updated files are on disk; no reload was needed.")
        return true
    }

    let uid = getuid()
    let (exitCode, output) = runProcess(
        executable: "/bin/launchctl",
        arguments: ["kickstart", "-k", "gui/\(uid)/\(caddyLabel)"]
    )
    if exitCode == 0 {
        printSuccess("Reloaded caddy")
        return true
    }

    printError("Failed to reload caddy (exit \(exitCode))")
    let summary = firstLine(output)
    if !summary.isEmpty {
        printHint(summary)
    }
    return false
}

@discardableResult
func shutdownServices(home: String, exitAfter: Bool, disableSleepPreventionOnCleanShutdown: Bool) -> Bool {
    shutdownServices(
        home: home,
        exitAfter: exitAfter,
        disableSleepPreventionOnCleanShutdown: disableSleepPreventionOnCleanShutdown,
        printLifecycleHeader: true
    )
}

@discardableResult
func shutdownServices(
    home: String,
    exitAfter: Bool,
    disableSleepPreventionOnCleanShutdown: Bool,
    printLifecycleHeader: Bool
) -> Bool {
    if printLifecycleHeader {
        printSectionTitle("Applying Service Lifecycle")
    }
    bootoutAllServices(home: home)

    let remaining = loadedKikoServices()
    print()
    if remaining.isEmpty {
        printSuccess("Shutdown complete. All kiko-media services are unloaded.")
        if disableSleepPreventionOnCleanShutdown {
            _ = disableSleepPrevention()
        }
        print()
    } else {
        printWarning("Some services are still loaded.")
        for service in remaining {
            printHint(service.launchdLabel)
        }
        let commands = stopOrder.map { "launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/\($0.plist)" }
        printManualCommands("Manual stop:", commands: commands)
    }

    let cleanShutdown = remaining.isEmpty
    if exitAfter {
        printSectionTitle("Shutdown Complete")
        printBody("\(dim)Re-run the wizard when you are ready to configure or start services.\(reset)")
        print()
        exit(0)
    }

    return cleanShutdown
}

private func promptMenuSelection(maxOption: Int) -> Int {
    while true {
        let action = prompt("Action (1-\(maxOption))")
        guard let selection = Int(action), (1 ... maxOption).contains(selection) else {
            printError("Enter a number from the menu.")
            continue
        }
        return selection
    }
}

private enum IdleMenuAction {
    case startServices
    case runGuidedConfiguration
    case disableSleepPrevention
    case exit
}

private func promptIdleMenuAction(canStartServices: Bool, sleepEnabled: Bool) -> IdleMenuAction {
    printCompactSectionTitle("Choose an action")
    var option = 1

    let startOption: Int? = canStartServices ? option : nil
    if canStartServices {
        printActionMenuItem(option, title: "Start services", detail: "Bootstrap caddy, tusd, and kiko-media now.")
        option += 1
    }

    let guidedOption = option
    let guidedDetail = canStartServices
        ? "Open setup to review or update configuration."
        : "Open setup to create or update deployment config."
    printActionMenuItem(option, title: "Run guided configuration", detail: guidedDetail)
    option += 1

    let disableSleepOption: Int? = sleepEnabled ? option : nil
    if sleepEnabled {
        printActionMenuItem(option, title: "Disable sleep prevention", detail: "Allow the Mac to sleep again.")
        option += 1
    }

    let exitOption = option
    printActionMenuItem(option, title: "Exit", detail: "Quit setup and leave services unchanged.")
    print()

    while true {
        let n = promptMenuSelection(maxOption: exitOption)
        if let startOption, n == startOption {
            return .startServices
        }
        if n == guidedOption {
            return .runGuidedConfiguration
        }
        if let disableSleepOption, n == disableSleepOption {
            return .disableSleepPrevention
        }
        if n == exitOption {
            return .exit
        }
    }
}

private func openGuidedConfiguration() {
    printHint("Opening guided configuration.")
    print()
}

private func exitStartMenu() -> Never {
    print("\n  \(dim)Exited.\(reset)\n")
    exit(0)
}

func runStartMenu(home: String, repoRoot: String) {
    while true {
        let loaded = loadedKikoServices()
        let configState = detectConfigurationState(home: home, repoRoot: repoRoot)
        let sleepEnabled = isSleepPreventionEnabled()

        redraw([])
        printSectionTitle("Welcome")
        printBody("This wizard helps you configure and manage your kiko-media deployment.")
        printBody("Flow: dependency check, guided configuration, file generation, service control.")
        print()
        printConfigurationState(configState)
        printServiceState(loaded)
        if !loaded.isEmpty || sleepEnabled {
            printSleepPreventionState(enabled: sleepEnabled)
        }
        print()

        if !loaded.isEmpty && !sleepEnabled {
            printWarning("Sleep prevention is off while services are running.")
            printHint("Enable it below to keep the Mac awake on event day. \(dim)(caffeinate -s)\(reset)")
            print()
        }

        if loaded.isEmpty {
            switch promptIdleMenuAction(canStartServices: configState.canStartServices, sleepEnabled: sleepEnabled) {
            case .startServices:
                printSectionTitle("Applying Service Lifecycle")
                _ = enableSleepPrevention()
                let startOK = bootstrapAllServices(home: home)
                print()
                if startOK {
                    printSuccess("Services started.")
                } else {
                    printWarning("One or more services failed to start.")
                    let commands = startOrder.map { "launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/\($0.plist)" }
                    printManualCommands("Manual start:", commands: commands)
                }
                printSectionTitle("Done")
                printBody("\(dim)Re-run the wizard to update configuration or shut down services.\(reset)")
                print()
                exit(0)
            case .runGuidedConfiguration:
                openGuidedConfiguration()
                return
            case .disableSleepPrevention:
                printSectionTitle("Sleep Prevention")
                _ = disableSleepPrevention()
                waitForEnter()
                continue
            case .exit:
                exitStartMenu()
            }
        }

        printCompactSectionTitle("Choose an action")
        var option = 1
        let enableSleepOption = (!sleepEnabled)
        if enableSleepOption {
            printActionMenuItem(option, title: "Enable sleep prevention", detail: "Keep the Mac awake while services run. \(dim)(caffeinate -s)\(reset)")
            option += 1
        }
        let stopAndExitOption = option
        printActionMenuItem(option, title: "Stop services and exit", detail: "Shut down caddy, tusd, and kiko-media, then quit setup.")
        option += 1
        let guidedStopOption = option
        printActionMenuItem(option, title: "Run guided configuration (stop services first)", detail: "Stop services before setup, then restart cleanly after changes.")
        option += 1
        let guidedKeepOption = option
        printActionMenuItem(option, title: "Run guided configuration (keep services running)", detail: "Continue setup without stopping currently running services.")
        option += 1
        let exitOption = option
        printActionMenuItem(option, title: "Exit", detail: "Quit setup and leave services unchanged.")
        print()

        while true {
            let n = promptMenuSelection(maxOption: exitOption)

            if enableSleepOption, n == 1 {
                printSectionTitle("Sleep Prevention")
                _ = enableSleepPrevention()
                waitForEnter()
                break
            }

            switch n {
            case stopAndExitOption:
                _ = shutdownServices(home: home, exitAfter: true, disableSleepPreventionOnCleanShutdown: true)
            case guidedStopOption:
                let cleanShutdown = shutdownServices(home: home, exitAfter: false, disableSleepPreventionOnCleanShutdown: false)
                if !cleanShutdown && !confirm("Continue configuration even though some services are still loaded") {
                    exitStartMenu()
                }
                openGuidedConfiguration()
                return
            case guidedKeepOption:
                openGuidedConfiguration()
                return
            case exitOption:
                exitStartMenu()
            default:
                printError("Enter a number from the menu.")
            }
        }
    }
}

func bootstrapAllServices(home: String) -> Bool {
    let uid = getuid()
    let fm = FileManager.default
    var success = true

    for service in startOrder {
        let plistPath = launchAgentPath(home: home, plist: service.plist)
        guard fm.fileExists(atPath: plistPath) else {
            printError("Missing LaunchAgent: \(plistPath)")
            success = false
            continue
        }

        if isLaunchAgentLoaded(service.launchdLabel) {
            printHint("\(service.label) already loaded (skipping)")
            continue
        }

        let (enableExit, enableOutput) = runProcess(
            executable: "/bin/launchctl",
            arguments: ["enable", "gui/\(uid)/\(service.launchdLabel)"]
        )
        if enableExit != 0 {
            let summary = firstLine(enableOutput)
            if !summary.isEmpty {
                printHint("enable \(service.label): \(summary) (continuing)")
            }
        }

        let (exitCode, output) = runProcess(
            executable: "/bin/launchctl",
            arguments: ["bootstrap", "gui/\(uid)", plistPath]
        )
        if exitCode == 0 {
            printSuccess("Started \(service.label)")
        } else {
            printError("Failed to start \(service.label) (exit \(exitCode))")
            let summary = firstLine(output)
            if !summary.isEmpty {
                printHint(summary)
            }
            success = false
        }
    }

    return success
}

func expandedBaseDirectory(from answers: Answers) throws -> String {
    let baseDirRaw = try requiredAdvancedValue("BASE_DIRECTORY", from: answers.advanced)
    return expandTildePath(baseDirRaw)
}

@discardableResult
func removeFileIfPresent(_ path: String) -> Bool {
    let fm = FileManager.default
    guard fm.fileExists(atPath: path) else { return true }
    do {
        try fm.removeItem(atPath: path)
        printSuccess("Deleted \(path)")
        return true
    } catch {
        printError("Failed deleting \(path): \(error.localizedDescription)")
        return false
    }
}

@discardableResult
func clearDirectoryContents(_ path: String) -> Bool {
    let fm = FileManager.default
    do {
        if !fm.fileExists(atPath: path) {
            try fm.createDirectory(atPath: path, withIntermediateDirectories: true)
            printSuccess("Created \(path)")
            return true
        }
        let entries = try fm.contentsOfDirectory(atPath: path)
        for entry in entries {
            try fm.removeItem(atPath: "\(path)/\(entry)")
        }
        printSuccess("Cleared \(path)")
        return true
    } catch {
        printError("Failed clearing \(path): \(error.localizedDescription)")
        return false
    }
}

func flushRuntimeState(baseDir: String) -> Bool {
    var ok = true
    ok = removeFileIfPresent("\(baseDir)/metadata.db") && ok
    ok = removeFileIfPresent("\(baseDir)/metadata.db-wal") && ok
    ok = removeFileIfPresent("\(baseDir)/metadata.db-shm") && ok
    ok = clearDirectoryContents("\(baseDir)/thumbs") && ok
    ok = clearDirectoryContents("\(baseDir)/previews") && ok
    return ok
}
