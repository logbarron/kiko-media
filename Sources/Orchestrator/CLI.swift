import Foundation
import Darwin

// MARK: - CLI

private func printHelpGroup(_ title: String, _ items: [(String, String)]) {
    printCompactSectionTitle(title)
    for (flag, desc) in items {
        print("    \(flag.padding(toLength: 18, withPad: " ", startingAt: 0))\(dim)\(desc)\(reset)")
    }
    print()
}

func printSetupHelp(repoRoot: String?) {
    let base = setupInvocationBase(repoRoot: repoRoot)
    print()
    printCompactSectionTitle("kiko-media orchestrator")
    printHint("Configure, deploy, and manage kiko-media services.")
    print()
    printCompactSectionTitle("Usage")
    printBody("  orchestrator")
    if base != "orchestrator" {
        printHint("Run from the repo with: \(base)")
    }
    print()
    printHelpGroup("Services", [
        ("--status",        "Show service and configuration status"),
        ("--start",         "Start all services (enables sleep prevention)"),
        ("--restart",       "Stop then start all services"),
        ("--stop",          "Stop all services (disables sleep prevention)"),
        ("--shutdown",      "Stop and persistently disable all services"),
    ])
    printHelpGroup("Workers", [
        ("--thunderbolt",   "Configure Thunderbolt workers"),
        ("--tb-status",     "Show Thunderbolt worker status and runtime context"),
    ])
    printHelpGroup("Help", [
        ("--help",          "Show this help"),
    ])
}

enum SetupCLICommand: String {
    case status
    case tbStatus = "tb-status"
    case internalRegenFrontend = "internal-regen-frontend"
    case start
    case stop
    case shutdown
    case restart
    case thunderbolt
}

struct SetupCLIParseResult {
    let command: SetupCLICommand?
    let error: String?
}

func parseAndValidateCLIArgs(_ args: [String]) -> SetupCLIParseResult {
    let known: Set<String> = [
        "--status", "--tb-status", "--internal-regen-frontend", "--start", "--stop", "--shutdown", "--restart", "--thunderbolt",
        "status", "tb-status", "internal-regen-frontend", "start", "stop", "shutdown", "restart", "thunderbolt",
    ]

    for arg in args {
        if !known.contains(arg) {
            return SetupCLIParseResult(command: nil, error: "Unknown argument: \(arg)")
        }
    }

    if let first = args.first, let direct = SetupCLICommand(rawValue: first) {
        if args.count != 1 {
            return SetupCLIParseResult(command: nil, error: "Unexpected extra arguments: \(args.dropFirst().joined(separator: " "))")
        }
        return SetupCLIParseResult(command: direct, error: nil)
    }

    var commands: [SetupCLICommand] = []
    if args.contains("--status") { commands.append(.status) }
    if args.contains("--tb-status") { commands.append(.tbStatus) }
    if args.contains("--internal-regen-frontend") { commands.append(.internalRegenFrontend) }
    if args.contains("--start") { commands.append(.start) }
    if args.contains("--stop") { commands.append(.stop) }
    if args.contains("--shutdown") { commands.append(.shutdown) }
    if args.contains("--restart") { commands.append(.restart) }
    if args.contains("--thunderbolt") { commands.append(.thunderbolt) }

    if commands.isEmpty {
        return SetupCLIParseResult(command: nil, error: nil)
    }
    if commands.count > 1 {
        return SetupCLIParseResult(command: nil, error: "Choose only one command: --status, --tb-status, --start, --stop, --shutdown, --restart, or --thunderbolt.")
    }
    if args.count != 1 {
        let flag = "--" + commands[0].rawValue
        var extras = args
        if let idx = extras.firstIndex(of: flag) {
            extras.remove(at: idx)
        }
        return SetupCLIParseResult(command: nil, error: "Unexpected extra arguments: \(extras.joined(separator: " "))")
    }

    return SetupCLIParseResult(command: commands[0], error: nil)
}

func runCLICommand(_ command: SetupCLICommand) -> Never {
    let attemptedCommand = "\(setupInvocationBase(repoRoot: nil)) --\(command.rawValue)"
    let (home, repoRoot) = resolveHomeAndRepoRoot(attemptedCommand: attemptedCommand)

    switch command {
    case .status:
        let dashboard = StatusDashboard(home: home, repoRoot: repoRoot)
        dashboard.start()
        if !isColorEnabled {
            exit(0)
        }
        let closeDashboard: @Sendable () -> Void = {
            dashboard.stop()
            print("\u{1b}[H\u{1b}[2J\u{1b}[3J", terminator: "")
            print("Status dashboard closed.")
            fflush(stdout)
            exit(0)
        }
        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, SIG_IGN)
        let sigINTSource = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
        sigINTSource.setEventHandler(handler: closeDashboard)
        sigINTSource.resume()
        let sigTERMSource = DispatchSource.makeSignalSource(signal: SIGTERM, queue: .main)
        sigTERMSource.setEventHandler(handler: closeDashboard)
        sigTERMSource.resume()
        dispatchMain()

    case .tbStatus:
        let exitCode = runThunderboltStatusCommand(repoRoot: repoRoot, home: home)
        exit(exitCode == 0 ? 0 : 1)

    case .internalRegenFrontend:
        printSectionTitle("Frontend Regeneration")
        do {
            let result = try regenerateFrontendArtifacts(repoRoot: repoRoot, home: home)
            print()
            if !result.indexChanged && !result.caddyfileChanged {
                printHint("No frontend artifacts changed.")
                exit(0)
            }
            if result.caddyfileChanged {
                let reloadOK = reloadCaddyIfRunning()
                exit(reloadOK ? 0 : 1)
            }
            printHint("Caddy reload not needed.")
            exit(0)
        } catch {
            printError("Frontend regeneration failed: \(error.localizedDescription)")
            printHint("Use the full setup wizard or manual config updates for sitekey, secrets, domain, bind IP, or Turnstile action/cdata changes.")
            exit(1)
        }

    case .stop:
        let ok = stopServices(home: home, exitAfter: false, disableSleepPreventionOnCleanStop: true)
        print()
        printThunderboltStopWorkerIndependenceNote()
        exit(ok ? 0 : 1)

    case .shutdown:
        let ok = disableAllServices(home: home)
        print()
        printThunderboltStopWorkerIndependenceNote()
        exit(ok ? 0 : 1)

    case .start:
        let configState = detectConfigurationState(home: home, repoRoot: repoRoot)
        guard configState.canStartServices else {
            printSectionTitle("Status")
            printConfigurationState(configState)
            printServiceState(loadedKikoServices())
            printSleepPreventionState()
            print()
            printError("Cannot start services: configuration files are not ready.")
            exit(1)
        }

        printSectionTitle("Applying Service Lifecycle")
        _ = enableSleepPrevention()
        let startOK = bootstrapAllServices(home: home)
        print()
        if startOK {
            printSuccess("Services started.")
            print()
            runThunderboltStartAdvisoryProbe(repoRoot: repoRoot, home: home)
            print()
            exit(0)
        }

        printWarning("One or more services failed to start.")
        let enableCommands = startOrder.map { "launchctl enable gui/$(id -u)/\($0.launchdLabel)" }
        let bootstrapCommands = startOrder.map { "launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/\($0.plist)" }
        printManualCommands("Manual start:", commands: enableCommands + bootstrapCommands)
        printHint("See docs/runbook.md for troubleshooting.")
        print()
        exit(1)

    case .restart:
        let configState = detectConfigurationState(home: home, repoRoot: repoRoot)
        guard configState.canStartServices else {
            printSectionTitle("Status")
            printConfigurationState(configState)
            printServiceState(loadedKikoServices())
            printSleepPreventionState()
            print()
            printError("Cannot restart services: configuration files are not ready.")
            exit(1)
        }

        printSectionTitle("Applying Service Lifecycle")
        _ = enableSleepPrevention()

        print()
        printCompactSectionTitle("Stop phase")
        print()
        let cleanStop = stopServices(
            home: home,
            exitAfter: false,
            disableSleepPreventionOnCleanStop: false,
            printLifecycleHeader: false
        )
        print()
        printThunderboltStopWorkerIndependenceNote()
        if !cleanStop {
            print()
            printWarning("Stop incomplete; not restarting.")
            exit(1)
        }

        print()
        printCompactSectionTitle("Start phase")
        print()
        let startOK = bootstrapAllServices(home: home)
        print()
        if startOK {
            printSuccess("Services started.")
            print()
            runThunderboltStartAdvisoryProbe(repoRoot: repoRoot, home: home)
            print()
            exit(0)
        }

        printWarning("One or more services failed to start.")
        let enableCommands = startOrder.map { "launchctl enable gui/$(id -u)/\($0.launchdLabel)" }
        let bootstrapCommands = startOrder.map { "launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/\($0.plist)" }
        printManualCommands("Manual start:", commands: enableCommands + bootstrapCommands)
        printHint("See docs/runbook.md for troubleshooting.")
        print()
        exit(1)

    case .thunderbolt:
        let exitCode = runThunderboltCommand(repoRoot: repoRoot, home: home)
        exit(exitCode == 0 ? 0 : 1)
    }
}

func runSetupCLIOrWizard() {
    let cliArgs = Array(CommandLine.arguments.dropFirst())
    let discoveredRepoRoot = discoverRepoRoot()

    if cliArgs.contains("--help") || cliArgs.contains("-h") || cliArgs.first == "help" {
        printSetupHelp(repoRoot: discoveredRepoRoot)
        exit(0)
    }

    let parsed = parseAndValidateCLIArgs(cliArgs)
    if let error = parsed.error {
        printError(error)
        printSetupHelp(repoRoot: discoveredRepoRoot)
        exit(2)
    }

    if let cmd = parsed.command {
        runCLICommand(cmd)
    }

    do {
        try runWizard()
    } catch {
        printError("Setup failed: \(error.localizedDescription)")
        exit(1)
    }
}
