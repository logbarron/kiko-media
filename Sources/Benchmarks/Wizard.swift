import Foundation
import KikoMediaCore

struct BenchmarkWizard {
    private struct MediaFolderChoice {
        let path: String
        let summary: String
    }

    private struct ThunderboltWorkerDefault {
        let host: String
        let slots: Int
    }

    private static let headerTitle = "kiko-media benchmarks"
    private static let headerSubtitle = "Writes a report to bench-results/<timestamp>.md."

    private enum RunMode: Int {
        case profile = 1
        case extend = 2
        case tune = 3
        case stressTest = 4
        case singleStage = 5
        case exit = 6
    }

    private enum WizardMode: Sendable {
        case profile
        case extend
        case singleStage
        case limitFinder
    }

    private enum MediaFolderExpectation {
        case any
        case needsImages
        case needsVideos
        case needsImagesAndVideos
    }

    private func redraw(completed: [(String, String)]) {
        ConsoleUI.redraw(
            title: Self.headerTitle,
            subtitle: Self.headerSubtitle,
            completed: completed
        )
    }

    func run() throws -> BenchmarkPlan {
        menu: while true {
            let mode = promptForRunMode()

            switch mode {
            case .profile, .extend:
                var plan = BenchmarkPlan()
                plan.runLimitFinder = false
                plan.components = mode == .extend ? extendComponents() : profileComponents()
                let wizardMode: WizardMode = mode == .extend ? .extend : .profile

                var mediaChoice: MediaFolderChoice? = nil

                var step = 1

                let expectation = mediaExpectation(mode: wizardMode, plan: plan)
                mediaChoice = promptForMediaFolder(
                    completed: completedItems(mode: wizardMode, plan: plan, media: mediaChoice),
                    expectation: expectation,
                    defaultPath: plan.mediaFolder,
                    stepNumber: step
                )
                step += 1
                plan.mediaFolder = mediaChoice?.path

                // Extend: archive SSD setup
                if plan.components.contains(.archive) {
                    plan.ssdPath = promptForExternalSSD(
                        completed: completedItems(mode: wizardMode, plan: plan, media: mediaChoice),
                        stepNumber: step
                    )
                    step += 1
                }

                // Extend: thunderbolt worker setup
                if wizardMode == .extend {
                    promptForThunderboltWorkers(
                        completed: completedItems(mode: wizardMode, plan: plan, media: mediaChoice),
                        plan: &plan,
                        stepNumber: step
                    )
                    step += 1

                    if plan.components.contains(.thunderbolt) {
                        plan.sweepMode = promptForSweepMode(
                            completed: completedItems(mode: wizardMode, plan: plan, media: mediaChoice),
                            currentMode: plan.sweepMode,
                            stepNumber: step
                        )
                        plan.hasChosenSweepMode = true
                        step += 1

                        plan.thunderboltCAModelMode = promptForThunderboltCAModelMode(
                            completed: completedItems(mode: wizardMode, plan: plan, media: mediaChoice),
                            current: plan.thunderboltCAModelMode,
                            stepNumber: step,
                            totalSteps: nil
                        )
                        step += 1

                        plan.thunderboltPriorUpdatePolicy = promptForThunderboltPriorUpdatePolicy(
                            completed: completedItems(mode: wizardMode, plan: plan, media: mediaChoice),
                            current: plan.thunderboltPriorUpdatePolicy,
                            stepNumber: step,
                            totalSteps: nil
                        )
                        step += 1
                    }
                }

                if shouldOfferAdvancedVideoOptions(plan) {
                    try maybeConfigureAdvancedVideoOptions(
                        plan: &plan,
                        completed: completedItems(mode: wizardMode, plan: plan, media: mediaChoice),
                        stepNumber: step
                    )
                    step += 1
                }

                if let final = try reviewPlan(mode: wizardMode, plan: &plan, media: &mediaChoice) {
                    return final
                }
                continue menu

            case .tune:
                var plan = BenchmarkPlan()
                plan.runLimitFinder = false
                plan.components = [.comparison]

                var mediaChoice: MediaFolderChoice? = nil

                let expectation = mediaExpectation(mode: .singleStage, plan: plan)
                mediaChoice = promptForMediaFolder(
                    completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                    expectation: expectation,
                    defaultPath: plan.mediaFolder,
                    stepNumber: 1,
                    totalSteps: 1
                )
                plan.mediaFolder = mediaChoice?.path

                if let final = try reviewPlan(mode: .singleStage, plan: &plan, media: &mediaChoice) {
                    return final
                }
                continue menu

            case .singleStage:
                var plan = BenchmarkPlan()
                plan.runLimitFinder = false
                plan.components = []

                var mediaChoice: MediaFolderChoice? = nil

                var step = 1
                let component = promptForComponent(
                    completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                    stepNumber: step
                )
                step += 1
                plan.components = [component]

                if component == .thunderbolt {
                    let expectation = mediaExpectation(mode: .singleStage, plan: plan)
                    mediaChoice = promptForMediaFolder(
                        completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                        expectation: expectation,
                        defaultPath: plan.mediaFolder,
                        stepNumber: step
                    )
                    plan.mediaFolder = mediaChoice?.path
                    step += 1

                    promptForThunderboltSingleStageWorkers(
                        completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                        plan: &plan,
                        stepNumber: step
                    )
                    step += 1

                    plan.thunderboltRunType = promptForThunderboltRunType(
                        completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                        current: plan.thunderboltRunType,
                        stepNumber: step
                    )
                    step += 1
                    let thunderboltTotalSteps = totalEditSteps(mode: .singleStage, plan: plan)

                    switch plan.thunderboltRunType {
                    case .showdown:
                        promptForThunderboltCASlotOverrides(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            plan: &plan,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        step += 1
                        plan.thunderboltCAModelMode = promptForThunderboltCAModelMode(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            current: plan.thunderboltCAModelMode,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        step += 1
                        plan.thunderboltPriorUpdatePolicy = promptForThunderboltPriorUpdatePolicy(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            current: plan.thunderboltPriorUpdatePolicy,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        step += 1
                        plan.thunderboltShowdownScope = promptForThunderboltShowdownScope(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            current: plan.thunderboltShowdownScope,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        step += 1
                    case .full:
                        plan.thunderboltSlotOverrides = nil
                        plan.sweepMode = promptForSweepMode(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            currentMode: plan.sweepMode,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        plan.hasChosenSweepMode = true
                        step += 1
                        plan.thunderboltCAModelMode = promptForThunderboltCAModelMode(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            current: plan.thunderboltCAModelMode,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        step += 1
                        plan.thunderboltPriorUpdatePolicy = promptForThunderboltPriorUpdatePolicy(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            current: plan.thunderboltPriorUpdatePolicy,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        step += 1
                        plan.thunderboltShowdownScope = promptForThunderboltShowdownScope(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            current: plan.thunderboltShowdownScope,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        step += 1
                    case .burstSweep:
                        plan.thunderboltSlotOverrides = nil
                        plan.thunderboltCAModelMode = .auto
                        plan.thunderboltPriorUpdatePolicy = .off
                        plan.thunderboltShowdownScope = .allProfiles
                        plan.sweepMode = promptForSweepMode(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            currentMode: plan.sweepMode,
                            stepNumber: step,
                            totalSteps: thunderboltTotalSteps
                        )
                        plan.hasChosenSweepMode = true
                        step += 1
                    }

                    try maybeConfigureAdvancedVideoOptions(
                        plan: &plan,
                        completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                        stepNumber: step,
                        totalSteps: thunderboltTotalSteps
                    )

                    if let final = try reviewPlan(mode: .singleStage, plan: &plan, media: &mediaChoice) {
                        return final
                    }
                    continue menu
                }

                let totalSteps = 1
                    + (plan.requiresMediaFolder ? 1 : 0)
                    + (shouldPromptForExternalSSD(mode: .singleStage, plan: plan) ? 1 : 0)
                    + (shouldOfferAdvancedVideoOptions(plan) ? 1 : 0)

                if plan.requiresMediaFolder {
                    let expectation = mediaExpectation(mode: .singleStage, plan: plan)
                    mediaChoice = promptForMediaFolder(
                        completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                        expectation: expectation,
                        defaultPath: plan.mediaFolder,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                    step += 1
                    plan.mediaFolder = mediaChoice?.path
                }

                if shouldPromptForExternalSSD(mode: .singleStage, plan: plan) {
                    if isSingleStagePipeline(mode: .singleStage, plan: plan) {
                        plan.ssdPath = promptForOptionalPipelineSSD(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            currentPath: plan.ssdPath,
                            stepNumber: step,
                            totalSteps: totalSteps
                        )
                        if plan.ssdPath == nil {
                            plan.keepSSDBenchArtifacts = false
                        }
                    } else {
                        plan.ssdPath = promptForExternalSSD(
                            completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                            stepNumber: step,
                            totalSteps: totalSteps
                        )
                    }
                    step += 1
                }

                if shouldOfferAdvancedVideoOptions(plan) {
                    try maybeConfigureAdvancedVideoOptions(
                        plan: &plan,
                        completed: completedItems(mode: .singleStage, plan: plan, media: mediaChoice),
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                    step += 1
                }

                if let final = try reviewPlan(mode: .singleStage, plan: &plan, media: &mediaChoice) {
                    return final
                }
                continue menu

            case .stressTest:
                var plan = BenchmarkPlan()
                plan.runLimitFinder = true
                plan.components = []

                var mediaChoice: MediaFolderChoice? = nil

                var step = 1
                let workload = promptForLimitWorkload(
                    completed: completedItems(mode: .limitFinder, plan: plan, media: mediaChoice),
                    currentWorkload: plan.limitConfig.workload,
                    stepNumber: step
                )
                step += 1
                plan.limitConfig.workload = workload
                let totalSteps = shouldOfferAdvancedVideoOptions(plan) ? 3 : 2

                let expectation = mediaExpectation(mode: .limitFinder, plan: plan)
                mediaChoice = promptForMediaFolder(
                    completed: completedItems(mode: .limitFinder, plan: plan, media: mediaChoice),
                    expectation: expectation,
                    defaultPath: plan.mediaFolder,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                step += 1
                plan.mediaFolder = mediaChoice?.path

                if shouldOfferAdvancedVideoOptions(plan) {
                    try maybeConfigureAdvancedVideoOptions(
                        plan: &plan,
                        completed: completedItems(mode: .limitFinder, plan: plan, media: mediaChoice),
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                    step += 1
                }

                if let final = try reviewPlan(mode: .limitFinder, plan: &plan, media: &mediaChoice) {
                    return final
                }
                continue menu

            case .exit:
                Foundation.exit(0)
            }
        }
    }

    // MARK: - Run Mode

    private func promptForRunMode() -> RunMode {
        redraw(completed: [])

        ConsoleUI.printSubsectionTitle("Usage")
        ConsoleUI.printBody("  swift run -c release benchmark  \(ConsoleUI.dim)accurate numbers\(ConsoleUI.reset)")
        ConsoleUI.printBody("  swift run benchmark             \(ConsoleUI.dim)debug, not accurate\(ConsoleUI.reset)")

        ConsoleUI.printSectionTitle("Run Mode")
        ConsoleUI.printActionMenuItem(
            RunMode.profile.rawValue,
            title: "Profile",
            detail: "Measure your Mac and find optimal concurrency settings."
        )
        ConsoleUI.printHint("\(ConsoleUI.listDetailIndent)Stages: image, video, sha256, db, pipeline.")
        ConsoleUI.printActionMenuItem(
            RunMode.extend.rawValue,
            title: "Extended",
            detail: "Profile plus external hardware setup and benchmarking."
        )
        ConsoleUI.printHint("\(ConsoleUI.listDetailIndent)Adds: archive (external SSD), thunderbolt (worker Macs).")
        ConsoleUI.printActionMenuItem(
            RunMode.tune.rawValue,
            title: "Tune",
            detail: "Parameter tradeoff curves (JPEG quality, thumbnail size, buffer, preset)."
        )
        ConsoleUI.printActionMenuItem(
            RunMode.stressTest.rawValue,
            title: "Stress test",
            detail: "Ramp concurrency until failure. Find the breaking point."
        )
        ConsoleUI.printActionMenuItem(
            RunMode.singleStage.rawValue,
            title: "Single stage",
            detail: "Pick one stage to run."
        )
        ConsoleUI.printActionMenuItem(
            RunMode.exit.rawValue,
            title: "Exit",
            detail: "Quit without running benchmarks."
        )
        Swift.print("")

        while true {
            let raw = ConsoleUI.prompt("Action (1-6)")
            guard let n = Int(raw), let mode = RunMode(rawValue: n) else {
                ConsoleUI.printError("Enter a number from the menu.")
                continue
            }
            return mode
        }
    }

    private func profileComponents() -> [BenchmarkComponent] {
        [.image, .video, .sha256, .db, .pipeline]
    }

    private func extendComponents() -> [BenchmarkComponent] {
        [.image, .video, .sha256, .db, .archive, .thunderbolt, .pipeline]
    }

    // MARK: - Inputs

    private func printPromptTitle(_ text: String, stepNumber: Int?, totalSteps: Int?) {
        if let stepNumber {
            if let totalSteps {
                ConsoleUI.printStep(stepNumber, of: totalSteps, text)
            } else {
                ConsoleUI.printStep(stepNumber, text)
            }
            Swift.print("")
        } else {
            ConsoleUI.printSectionTitle(text)
        }
    }

    private func promptForArchiveInclusion(
        completed: [(String, String)],
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) -> Bool {
        redraw(completed: completed)

        printPromptTitle("Archive Stage (Optional)", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printHint("Copies originals to an external SSD and SHA256-verifies the copy.")
        ConsoleUI.printHint("Writes under: <ssd folder>/bench-results/<run-id>/archive")
        ConsoleUI.printHint("By default, that bench folder is removed after the run.")
        Swift.print("")

        return ConsoleUI.confirm("Include archive stage", defaultYes: false)
    }

    private func promptForComponent(
        completed: [(String, String)],
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) -> BenchmarkComponent {
        redraw(completed: completed)

        printPromptTitle("Stages", stepNumber: stepNumber, totalSteps: totalSteps)
        for (i, spec) in BenchmarkCatalog.components.enumerated() {
            ConsoleUI.printActionMenuItem(
                i + 1,
                title: "\(spec.id.rawValue) · \(spec.title) · \(spec.expectedRuntime)",
                detail: spec.detail
            )
        }
        Swift.print("")

        while true {
            let choice = ConsoleUI.prompt("Stage (1-\(BenchmarkCatalog.components.count))")
            if let n = Int(choice), (1...BenchmarkCatalog.components.count).contains(n) {
                return BenchmarkCatalog.components[n - 1].id
            }
            ConsoleUI.printError("Enter a number from 1 to \(BenchmarkCatalog.components.count).")
        }
    }

    private func promptForMediaFolder(
        completed: [(String, String)],
        expectation: MediaFolderExpectation,
        defaultPath: String?,
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) -> MediaFolderChoice {
        while true {
            redraw(completed: completed)
            printPromptTitle("Media Folder", stepNumber: stepNumber, totalSteps: totalSteps)
            ConsoleUI.printHint("Choose a folder containing image/video files (non-recursive).")
            ConsoleUI.printHint("This folder is never modified.")
            Swift.print("")
            ConsoleUI.printHint("Tip: drag a folder from Finder into this terminal to paste the path.")
            ConsoleUI.printHint("Examples: ~/Pictures/bench-media  or  /Users/you/Pictures/bench-media")
            Swift.print("")

            let rawInput: String
            if let defaultPath, !defaultPath.isEmpty {
                rawInput = ConsoleUI.prompt("Media folder", default: defaultPath)
            } else {
                rawInput = ConsoleUI.promptRequired("Media folder")
            }

            let input = ConsoleUI.normalizePathInput(rawInput)
            do {
                let summary = try summarizeMediaFolder(path: input)
                if let msg = mediaExpectationFailureMessage(expectation: expectation, summary: summary) {
                    ConsoleUI.printError(msg)
                    Swift.print("")
                    continue
                }
                ConsoleUI.printSuccess("Found \(summary.summaryLine).")
                return MediaFolderChoice(path: input, summary: summary.summaryLine)
            } catch {
                ConsoleUI.printError("\(error)")
                Swift.print("")
            }
        }
    }

    private func promptForExternalSSD(
        completed: [(String, String)],
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) -> String {
        redraw(completed: completed)
        printPromptTitle("External SSD", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printHint("Copies your media files to an external SSD and measures write speed.")
        ConsoleUI.printHint("Files are verified with SHA-256 and cleaned up after the run.")
        Swift.print("")

        let ssdPath = ConsoleUI.pickSSDPath()
        ConsoleUI.printSuccess("Selected: \(ssdPath)")
        return ssdPath
    }

    private func promptForOptionalPipelineSSD(
        completed: [(String, String)],
        currentPath: String?,
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) -> String? {
        redraw(completed: completed)
        printPromptTitle("External SSD (Optional)", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printHint("Adds SSD archive copy + SHA-256 verification inside realistic pipeline.")
        ConsoleUI.printHint("Leave disabled to keep archive skipped behavior.")
        Swift.print("")

        if let currentPath, !currentPath.isEmpty {
            if !ConsoleUI.keepOrChange(label: "external SSD folder", current: currentPath) {
                return currentPath
            }
            if ConsoleUI.confirm("Disable external SSD for realistic pipeline archive", defaultYes: false) {
                return nil
            }
            Swift.print("")
            return promptForExternalSSD(
                completed: completed,
                stepNumber: stepNumber,
                totalSteps: totalSteps
            )
        }

        guard ConsoleUI.confirm("Use external SSD for realistic pipeline archive", defaultYes: false) else {
            return nil
        }
        Swift.print("")
        return promptForExternalSSD(
            completed: completed,
            stepNumber: stepNumber,
            totalSteps: totalSteps
        )
    }

    private func promptForLimitWorkload(
        completed: [(String, String)],
        currentWorkload: LimitWorkload,
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) -> LimitWorkload {
        redraw(completed: completed)

        printPromptTitle("Workload", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printActionMenuItem(1, title: "mixed", detail: "Images + videos together.")
        ConsoleUI.printActionMenuItem(2, title: "image", detail: "Images only.")
        ConsoleUI.printActionMenuItem(3, title: "video", detail: "Videos only.")
        Swift.print("")

        while true {
            let raw = ConsoleUI.prompt("Workload (1-3)")
            switch raw {
            case "1":
                return .mixed
            case "2":
                return .image
            case "3":
                return .video
            default:
                ConsoleUI.printError("Enter 1, 2, or 3.")
            }
        }
    }

    private func promptForSweepMode(
        completed: [(String, String)],
        currentMode: BurstSearchStrategy,
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) -> BurstSearchStrategy {
        redraw(completed: completed)
        printPromptTitle("Thunderbolt Sweep Mode", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printActionMenuItem(1, title: "smart", detail: "Optimized search (faster, recommended).")
        ConsoleUI.printActionMenuItem(2, title: "exhaustive", detail: "Brute-force all combinations (slowest).")
        Swift.print("")

        let defaultChoice: String
        switch currentMode {
        case .optimized:
            defaultChoice = "1"
        case .bruteForce:
            defaultChoice = "2"
        }

        while true {
            let raw = ConsoleUI.prompt("Sweep mode (1-2)", default: defaultChoice)
            switch raw {
            case "1":
                return .optimized()
            case "2":
                return .bruteForce
            default:
                ConsoleUI.printError("Enter 1 or 2.")
            }
        }
    }

    private func promptForThunderboltRunType(
        completed: [(String, String)],
        current: ThunderboltRunType,
        stepNumber: Int,
        totalSteps: Int? = nil
    ) -> ThunderboltRunType {
        redraw(completed: completed)
        printPromptTitle("Thunderbolt Run Type", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printActionMenuItem(
            1,
            title: "Burst sweep + leaderboard",
            detail: "Concurrency slot estimation."
        )
        ConsoleUI.printActionMenuItem(
            2,
            title: "FIFO vs CA showdown",
            detail: "Estimate improvement with Thunderbolt workers."
        )
        ConsoleUI.printActionMenuItem(
            3,
            title: "Full stage (burst + showdown)",
            detail: "Run burst sweep first, then FIFO vs CA showdown."
        )
        Swift.print("")

        let defaultChoice: String
        switch current {
        case .burstSweep:
            defaultChoice = "1"
        case .showdown:
            defaultChoice = "2"
        case .full:
            defaultChoice = "3"
        }

        while true {
            let raw = ConsoleUI.prompt("Thunderbolt run type (1-3)", default: defaultChoice)
            switch raw {
            case "1":
                return .burstSweep
            case "2":
                return .showdown
            case "3":
                return .full
            default:
                ConsoleUI.printError("Enter 1, 2, or 3.")
            }
        }
    }

    private func promptForThunderboltCAModelMode(
        completed: [(String, String)],
        current: ThunderboltCAModelMode,
        stepNumber: Int,
        totalSteps: Int?
    ) -> ThunderboltCAModelMode {
        redraw(completed: completed)
        printPromptTitle("CA Model Mode", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printActionMenuItem(
            1,
            title: "auto (recommended)",
            detail: "Uses prior first, then capabilities, then local fallback so remotes stay modeled."
        )
        ConsoleUI.printActionMenuItem(
            2,
            title: "strict (parity)",
            detail: "Prior-only parity with production CA; fails if any reachable remote is excluded."
        )
        Swift.print("")

        let defaultChoice = current == .auto ? "1" : "2"
        while true {
            let raw = ConsoleUI.prompt("CA model mode (1-2)", default: defaultChoice)
            switch raw {
            case "1":
                return .auto
            case "2":
                return .strict
            default:
                ConsoleUI.printError("Enter 1 or 2.")
            }
        }
    }

    private func promptForThunderboltPriorUpdatePolicy(
        completed: [(String, String)],
        current: ThunderboltPriorUpdatePolicy,
        stepNumber: Int,
        totalSteps: Int?
    ) -> ThunderboltPriorUpdatePolicy {
        redraw(completed: completed)
        printPromptTitle("Scheduler Model Update", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printHint("Choose whether this run should refresh the saved speed model used by CA scheduling.")
        Swift.print("")
        ConsoleUI.printActionMenuItem(1, title: "Don't update", detail: "Run the benchmark without saving a new model.")
        ConsoleUI.printActionMenuItem(2, title: "Save for review", detail: "Save a new model from this run for review, but keep the current model in use.")
        ConsoleUI.printActionMenuItem(3, title: "Promote if safe", detail: "Replace the current model if the new one looks safe, even if CA only ties FIFO.")
        ConsoleUI.printActionMenuItem(4, title: "Promote only if CA wins", detail: "Replace the current model only if the new one looks safe and CA beats FIFO in the showdown.")
        Swift.print("")

        let defaultChoice: String
        switch current {
        case .off:
            defaultChoice = "1"
        case .candidateOnly:
            defaultChoice = "2"
        case .promoteForce:
            defaultChoice = "3"
        case .promoteGuarded:
            defaultChoice = "4"
        }

        while true {
            let raw = ConsoleUI.prompt("Model update (1-4)", default: defaultChoice)
            switch raw {
            case "1":
                return .off
            case "2":
                return .candidateOnly
            case "3":
                return .promoteForce
            case "4":
                return .promoteGuarded
            default:
                ConsoleUI.printError("Enter 1, 2, 3, or 4.")
            }
        }
    }

    private func promptForThunderboltSingleStageWorkers(
        completed: [(String, String)],
        plan: inout BenchmarkPlan,
        stepNumber: Int,
        totalSteps: Int? = nil
    ) {
        while true {
            redraw(completed: completed)
            printPromptTitle("Thunderbolt Workers", stepNumber: stepNumber, totalSteps: totalSteps)
            ConsoleUI.printHint("Offloads video processing to worker Macs via Thunderbolt bridge.")
            Swift.print("")

            let bridges = ThunderboltDispatcher.discoverBridgeSources()
            if bridges.isEmpty {
                ConsoleUI.printWarning("No Thunderbolt bridges detected on this Mac.")
            } else {
                printDetectedBridges(bridges)
            }

            let defaults = resolveThunderboltWorkerDefaults()
            let configuredHosts = defaults.workers.map(\.host).joined(separator: ",")
            if !configuredHosts.isEmpty {
                ConsoleUI.printField(
                    "Workers configured",
                    value: "\(configuredHosts) \(ConsoleUI.dim)(from \(defaults.source))\(ConsoleUI.reset)"
                )
            } else {
                ConsoleUI.printField("Workers configured", value: "(none)")
            }

            let currentRaw = trimmedNonEmpty(plan.tbWorkers) ?? defaults.raw
            let currentWorkers = currentRaw.map { Config.parseThunderboltWorkers($0) } ?? []
            let currentHosts = currentWorkers.map(\.host).joined(separator: ",")
            ConsoleUI.printField("Current workers", value: currentHosts.isEmpty ? "(none)" : currentHosts)
            Swift.print("")

            let action = ConsoleUI.prompt("Action (Enter=keep, c=change)")
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .lowercased()
            if action.isEmpty {
                if let currentRaw, !currentRaw.isEmpty {
                    plan.tbWorkers = currentRaw
                    return
                }
                ConsoleUI.printError("No workers configured yet. Type c to set hosts.")
                continue
            }
            guard action == "c" else {
                ConsoleUI.printError("Press Enter to keep current workers or c to change.")
                continue
            }

            let defaultsByHost = workerSlotsByHost(
                defaults.workers + currentWorkers.map { ThunderboltWorkerDefault(host: $0.host, slots: $0.slots) }
            )
            let input = ConsoleUI.prompt(
                "Workers (host[,host...])",
                default: currentHosts.isEmpty ? nil : currentHosts
            )
            let hosts = parseWorkerHostList(input)
            guard !hosts.isEmpty else {
                ConsoleUI.printError("Enter at least one host.")
                continue
            }

            plan.tbWorkers = hosts
                .map { host in
                    let slots = max(1, defaultsByHost[host] ?? 1)
                    return "\(host):\(slots)"
                }
                .joined(separator: ",")
            return
        }
    }

    private func promptForThunderboltCASlotOverrides(
        completed: [(String, String)],
        plan: inout BenchmarkPlan,
        stepNumber: Int,
        totalSteps: Int
    ) {
        redraw(completed: completed)
        printPromptTitle("CA Slot Overrides (This Run Only)", stepNumber: stepNumber, totalSteps: totalSteps)

        let workerDefaults = workerDefaultsFromPlan(plan.tbWorkers)
        let localDefault = resolveLocalCASlotsDefaultForWizard()

        ConsoleUI.printHint("Detected defaults:")
        ConsoleUI.printBody("  local: \(localDefault) (from MAX_CONCURRENT_VIDEOS)")
        for worker in workerDefaults {
            ConsoleUI.printBody("  \(worker.host): \(worker.slots)")
        }
        Swift.print("")
        ConsoleUI.printHint("Press Enter to use defaults, or type c to change.")

        let action: String = {
            while true {
                let candidate = ConsoleUI.prompt("Action (Enter/c)")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                    .lowercased()
                if candidate.isEmpty || candidate == "c" {
                    return candidate
                }
                ConsoleUI.printError("Press Enter to use defaults, or type c to change.")
            }
        }()

        var localSlots = localDefault
        var remoteByHost = workerSlotsByHost(workerDefaults)

        if action == "c" {
            localSlots = promptForSlotValue("Local slots", defaultValue: localSlots, range: 1...64)
            for worker in workerDefaults {
                let value = promptForSlotValue("\(worker.host) slots", defaultValue: worker.slots, range: 0...16)
                remoteByHost[worker.host] = value
            }
        }

        plan.thunderboltSlotOverrides = ThunderboltCASlotOverrides(
            localSlots: localSlots,
            remoteSlotsByHost: remoteByHost
        )
    }

    private func promptForThunderboltShowdownScope(
        completed: [(String, String)],
        current: ThunderboltShowdownScope,
        stepNumber: Int,
        totalSteps: Int
    ) -> ThunderboltShowdownScope {
        redraw(completed: completed)
        printPromptTitle("CA Profiles", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printActionMenuItem(
            1,
            title: "All profiles",
            detail: "Runs FIFO -> CA and CA -> FIFO for all-at-once, burst-1-20-5-5-1, trickle."
        )
        ConsoleUI.printActionMenuItem(
            2,
            title: "Single profile pair",
            detail: "Runs order-neutral pair for one selected profile."
        )
        Swift.print("")

        let defaultChoice: String
        switch current {
        case .allProfiles:
            defaultChoice = "1"
        case .singleProfile:
            defaultChoice = "2"
        }

        while true {
            let scope = ConsoleUI.prompt("Scope (1-2)", default: defaultChoice)
            switch scope {
            case "1":
                return .allProfiles
            case "2":
                let selected = promptForThunderboltSingleProfile(
                    completed: completed,
                    defaultProfile: defaultProfileForShowdownScope(current),
                    stepNumber: stepNumber,
                    totalSteps: totalSteps
                )
                return .singleProfile(selected)
            default:
                ConsoleUI.printError("Enter 1 or 2.")
            }
        }
    }

    private func promptForThunderboltSingleProfile(
        completed: [(String, String)],
        defaultProfile: CAArrivalProfile,
        stepNumber: Int,
        totalSteps: Int
    ) -> CAArrivalProfile {
        while true {
            redraw(completed: completed)
            printPromptTitle("Select Profile", stepNumber: stepNumber, totalSteps: totalSteps)
            ConsoleUI.printActionMenuItem(1, title: CAArrivalProfile.allAtOnce.rawValue, detail: "")
            ConsoleUI.printActionMenuItem(2, title: CAArrivalProfile.burst_1_20_5_5_1.rawValue, detail: "")
            ConsoleUI.printActionMenuItem(3, title: CAArrivalProfile.trickle.rawValue, detail: "")
            Swift.print("")

            let defaultChoice: String
            switch defaultProfile {
            case .allAtOnce:
                defaultChoice = "1"
            case .burst_1_20_5_5_1:
                defaultChoice = "2"
            case .trickle:
                defaultChoice = "3"
            }
            let raw = ConsoleUI.prompt("Profile (1-3)", default: defaultChoice)
            switch raw {
            case "1":
                return .allAtOnce
            case "2":
                return .burst_1_20_5_5_1
            case "3":
                return .trickle
            default:
                ConsoleUI.printError("Enter 1, 2, or 3.")
            }
        }
    }

    private func parseWorkerHostList(_ raw: String) -> [String] {
        let candidates = raw
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        var seen = Set<String>()
        var hosts: [String] = []
        for host in candidates where seen.insert(host).inserted {
            hosts.append(host)
        }
        return hosts
    }

    private func resolveThunderboltWorkerDefaults() -> (workers: [ThunderboltWorkerDefault], source: String, raw: String?) {
        if let envRaw = trimmedNonEmpty(ProcessInfo.processInfo.environment["TB_WORKERS"]) {
            let workers = Config.parseThunderboltWorkers(envRaw).map { ThunderboltWorkerDefault(host: $0.host, slots: $0.slots) }
            return (workers, "environment", envRaw)
        }
        let plist = loadBenchmarkMediaLaunchAgentEnvironment()
        if let plistRaw = trimmedNonEmpty(plist["TB_WORKERS"]) {
            let workers = Config.parseThunderboltWorkers(plistRaw).map { ThunderboltWorkerDefault(host: $0.host, slots: $0.slots) }
            return (workers, "com.kiko.media.plist", plistRaw)
        }
        return ([], "not configured", nil)
    }

    private func workerDefaultsFromPlan(_ raw: String?) -> [ThunderboltWorkerDefault] {
        guard let raw = trimmedNonEmpty(raw) else { return [] }
        return Config.parseThunderboltWorkers(raw).map { ThunderboltWorkerDefault(host: $0.host, slots: $0.slots) }
    }

    private func workerSlotsByHost(_ workers: [ThunderboltWorkerDefault]) -> [String: Int] {
        workers.reduce(into: [String: Int]()) { partial, worker in
            partial[worker.host] = worker.slots
        }
    }

    private func promptForSlotValue(_ label: String, defaultValue: Int, range: ClosedRange<Int>) -> Int {
        while true {
            let raw = ConsoleUI.prompt(label, default: "\(defaultValue)")
            guard let value = Int(raw), range.contains(value) else {
                ConsoleUI.printError("Enter an integer in \(range.lowerBound)...\(range.upperBound).")
                continue
            }
            return value
        }
    }

    private func resolveLocalCASlotsDefaultForWizard() -> Int {
        if let envRaw = trimmedNonEmpty(ProcessInfo.processInfo.environment["MAX_CONCURRENT_VIDEOS"]),
           let envValue = Int(envRaw),
           envValue > 0 {
            return envValue
        }
        let plist = loadBenchmarkMediaLaunchAgentEnvironment()
        if let plistRaw = trimmedNonEmpty(plist["MAX_CONCURRENT_VIDEOS"]),
           let plistValue = Int(plistRaw),
           plistValue > 0 {
            return plistValue
        }
        return max(1, Config.intDefaults["MAX_CONCURRENT_VIDEOS"]?.fallback ?? 2)
    }

    private func defaultProfileForShowdownScope(_ scope: ThunderboltShowdownScope) -> CAArrivalProfile {
        switch scope {
        case .allProfiles:
            return .allAtOnce
        case .singleProfile(let profile):
            return profile
        }
    }

    // MARK: - Review

    private func reviewPlan(
        mode: WizardMode,
        plan: inout BenchmarkPlan,
        media: inout MediaFolderChoice?
    ) throws -> BenchmarkPlan? {
        while true {
            // Keep this screen compact: the run plan itself is the summary.
            redraw(completed: [])
            ConsoleUI.printSubsectionTitle("Run Plan")
            Swift.print("")
            printRunPlan(mode: mode, plan: plan, media: media)

            Swift.print("")
            ConsoleUI.printSubsectionTitle("Choose an action")
            ConsoleUI.printActionMenuItem(1, title: "Run now", detail: "Start benchmarks and write the report.")
            ConsoleUI.printActionMenuItem(2, title: "Edit settings", detail: "Change stages/paths/video settings.")
            ConsoleUI.printActionMenuItem(3, title: "Back", detail: "Return to run mode selection.")
            ConsoleUI.printActionMenuItem(4, title: "Exit", detail: "Quit without running benchmarks.")
            Swift.print("")

            let raw = ConsoleUI.prompt("Action (1-4)")
            guard let n = Int(raw) else {
                ConsoleUI.printError("Enter a number from the menu.")
                continue
            }

            switch n {
            case 1:
                if shouldPromptForProductionPriorUpdate(plan) {
                    plan.updateProductionPriorFromRun = promptForProductionPriorUpdate()
                } else {
                    plan.updateProductionPriorFromRun = nil
                }
                return plan
            case 2:
                try editSettings(mode: mode, plan: &plan, media: &media)
            case 3:
                return nil
            case 4:
                Foundation.exit(0)
            default:
                ConsoleUI.printError("Enter 1, 2, 3, or 4.")
            }
        }
    }

    private func editSettings(mode: WizardMode, plan: inout BenchmarkPlan, media: inout MediaFolderChoice?) throws {
        var step = 1

        var totalSteps = totalEditSteps(mode: mode, plan: plan)

        // Single stage: stage selection
        if mode == .singleStage {
            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Stage", stepNumber: step, totalSteps: totalSteps)

            let current = plan.components.first?.rawValue ?? "(none)"
            if ConsoleUI.keepOrChange(label: "stage", current: current) {
                let component = promptForComponent(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                plan.components = [component]

                if !shouldPromptForExternalSSD(mode: mode, plan: plan) {
                    plan.ssdPath = nil
                    plan.keepSSDBenchArtifacts = false
                }
                if !plan.requiresMediaFolder {
                    plan.mediaFolder = nil
                    media = nil
                }
                if component != .thunderbolt {
                    plan.thunderboltRunType = .full
                    plan.thunderboltShowdownScope = .allProfiles
                    plan.thunderboltSlotOverrides = nil
                    plan.thunderboltCAModelMode = .auto
                    plan.thunderboltPriorUpdatePolicy = .off
                }

                totalSteps = totalEditSteps(mode: mode, plan: plan)
            }
            step += 1
        }

        // Limit finder: workload
        if mode == .limitFinder {
            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Workload", stepNumber: step, totalSteps: totalSteps)

            let current = plan.limitConfig.workload.rawValue
            if ConsoleUI.keepOrChange(label: "workload", current: current) {
                let workload = promptForLimitWorkload(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    currentWorkload: plan.limitConfig.workload,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                plan.limitConfig.workload = workload
                totalSteps = totalEditSteps(mode: mode, plan: plan)
            }
            step += 1
        }

        // Media folder
        if plan.runLimitFinder || plan.requiresMediaFolder {
            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Media Folder", stepNumber: step, totalSteps: totalSteps)
            ConsoleUI.printHint("Choose a folder containing image/video files (non-recursive).")
            ConsoleUI.printHint("This folder is never modified.")
            Swift.print("")
            ConsoleUI.printHint("Tip: drag a folder from Finder into this terminal to paste the path.")
            ConsoleUI.printHint("Examples: ~/Pictures/bench-media  or  /Users/you/Pictures/bench-media")
            Swift.print("")

            let current = media?.path ?? plan.mediaFolder ?? ""
            if current.isEmpty || ConsoleUI.keepOrChange(label: "media folder", current: current) {
                let expectation = mediaExpectation(mode: mode, plan: plan)
                let choice = promptForMediaFolder(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    expectation: expectation,
                    defaultPath: current.isEmpty ? nil : current,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                media = choice
                plan.mediaFolder = choice.path
            }
            step += 1
        }

        if mode == .singleStage, plan.components.first == .thunderbolt {
            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Thunderbolt Workers", stepNumber: step, totalSteps: totalSteps)
            let currentHosts = hostsOnlyWorkersLabel(plan.tbWorkers)
            if currentHosts.isEmpty || ConsoleUI.keepOrChange(label: "workers", current: currentHosts) {
                promptForThunderboltSingleStageWorkers(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    plan: &plan,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
            }
            totalSteps = totalEditSteps(mode: mode, plan: plan)
            step += 1

            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Thunderbolt Run Type", stepNumber: step, totalSteps: totalSteps)
            if ConsoleUI.keepOrChange(label: "run type", current: thunderboltRunTypeLabel(plan.thunderboltRunType)) {
                plan.thunderboltRunType = promptForThunderboltRunType(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    current: plan.thunderboltRunType,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                if plan.thunderboltRunType != .showdown {
                    plan.thunderboltSlotOverrides = nil
                }
                if plan.thunderboltRunType == .burstSweep {
                    plan.thunderboltShowdownScope = .allProfiles
                    plan.thunderboltCAModelMode = .auto
                    plan.thunderboltPriorUpdatePolicy = .off
                }
            }
            totalSteps = totalEditSteps(mode: mode, plan: plan)
            step += 1

            switch plan.thunderboltRunType {
            case .showdown:
                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("CA Slot Overrides (This Run Only)", stepNumber: step, totalSteps: totalSteps)
                let currentSlots = plan.thunderboltSlotOverrides.map(thunderboltSlotOverrideLabel) ?? "(not set)"
                if plan.thunderboltSlotOverrides == nil || ConsoleUI.keepOrChange(label: "slot overrides", current: currentSlots) {
                    promptForThunderboltCASlotOverrides(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        plan: &plan,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
                step += 1

                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("CA Model Mode", stepNumber: step, totalSteps: totalSteps)
                if ConsoleUI.keepOrChange(label: "model mode", current: thunderboltCAModelModeLabel(plan.thunderboltCAModelMode)) {
                    plan.thunderboltCAModelMode = promptForThunderboltCAModelMode(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        current: plan.thunderboltCAModelMode,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
                step += 1

                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("Scheduler Model Update", stepNumber: step, totalSteps: totalSteps)
                if ConsoleUI.keepOrChange(
                    label: "model update",
                    current: thunderboltPriorUpdatePolicyLabel(plan.thunderboltPriorUpdatePolicy)
                ) {
                    plan.thunderboltPriorUpdatePolicy = promptForThunderboltPriorUpdatePolicy(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        current: plan.thunderboltPriorUpdatePolicy,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
                step += 1

                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("CA Profiles", stepNumber: step, totalSteps: totalSteps)
                if ConsoleUI.keepOrChange(label: "scope", current: thunderboltShowdownScopeLabel(plan.thunderboltShowdownScope)) {
                    plan.thunderboltShowdownScope = promptForThunderboltShowdownScope(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        current: plan.thunderboltShowdownScope,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
                step += 1

            case .full:
                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("Thunderbolt Sweep Mode", stepNumber: step, totalSteps: totalSteps)
                if !plan.hasChosenSweepMode {
                    plan.sweepMode = promptForSweepMode(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        currentMode: plan.sweepMode,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                    plan.hasChosenSweepMode = true
                } else if ConsoleUI.keepOrChange(label: "sweep mode", current: sweepModeLabel(plan.sweepMode)) {
                    plan.sweepMode = promptForSweepMode(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        currentMode: plan.sweepMode,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                    plan.hasChosenSweepMode = true
                }
                step += 1

                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("CA Model Mode", stepNumber: step, totalSteps: totalSteps)
                if ConsoleUI.keepOrChange(label: "model mode", current: thunderboltCAModelModeLabel(plan.thunderboltCAModelMode)) {
                    plan.thunderboltCAModelMode = promptForThunderboltCAModelMode(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        current: plan.thunderboltCAModelMode,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
                step += 1

                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("Scheduler Model Update", stepNumber: step, totalSteps: totalSteps)
                if ConsoleUI.keepOrChange(
                    label: "model update",
                    current: thunderboltPriorUpdatePolicyLabel(plan.thunderboltPriorUpdatePolicy)
                ) {
                    plan.thunderboltPriorUpdatePolicy = promptForThunderboltPriorUpdatePolicy(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        current: plan.thunderboltPriorUpdatePolicy,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
                step += 1

                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("CA Profiles", stepNumber: step, totalSteps: totalSteps)
                if ConsoleUI.keepOrChange(label: "profiles", current: thunderboltShowdownScopeLabel(plan.thunderboltShowdownScope)) {
                    plan.thunderboltShowdownScope = promptForThunderboltShowdownScope(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        current: plan.thunderboltShowdownScope,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
                step += 1

            case .burstSweep:
                plan.thunderboltPriorUpdatePolicy = .off
                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("Thunderbolt Sweep Mode", stepNumber: step, totalSteps: totalSteps)
                if !plan.hasChosenSweepMode {
                    plan.sweepMode = promptForSweepMode(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        currentMode: plan.sweepMode,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                    plan.hasChosenSweepMode = true
                } else if ConsoleUI.keepOrChange(label: "sweep mode", current: sweepModeLabel(plan.sweepMode)) {
                    plan.sweepMode = promptForSweepMode(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        currentMode: plan.sweepMode,
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                    plan.hasChosenSweepMode = true
                }
                step += 1
            }
        }

        // External SSD
        if shouldPromptForExternalSSD(mode: mode, plan: plan) {
            if isSingleStagePipeline(mode: mode, plan: plan) {
                plan.ssdPath = promptForOptionalPipelineSSD(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    currentPath: plan.ssdPath,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                if plan.ssdPath == nil {
                    plan.keepSSDBenchArtifacts = false
                }
            } else {
                redraw(completed: completedItems(mode: mode, plan: plan, media: media))
                printPromptTitle("External SSD", stepNumber: step, totalSteps: totalSteps)
                ConsoleUI.printHint("Copies your media files to an external SSD and measures write speed.")
                ConsoleUI.printHint("Files are verified with SHA-256 and cleaned up after the run.")
                Swift.print("")

                let currentSSD = plan.ssdPath ?? ""
                if currentSSD.isEmpty || ConsoleUI.keepOrChange(label: "external SSD folder", current: currentSSD) {
                    plan.ssdPath = promptForExternalSSD(
                        completed: completedItems(mode: mode, plan: plan, media: media),
                        stepNumber: step,
                        totalSteps: totalSteps
                    )
                }
            }
            step += 1
        }

        // Extend: thunderbolt workers
        if mode == .extend, plan.components.contains(.thunderbolt) {
            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Thunderbolt Workers", stepNumber: step, totalSteps: totalSteps)
            ConsoleUI.printHint("Offloads video processing to worker Macs via Thunderbolt bridge.")
            Swift.print("")

            let currentWorkers = plan.tbWorkers ?? ""
            if currentWorkers.isEmpty || ConsoleUI.keepOrChange(label: "workers", current: currentWorkers) {
                promptForThunderboltWorkers(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    plan: &plan,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
            }
            totalSteps = totalEditSteps(mode: mode, plan: plan)
            step = min(step + 1, totalSteps)
        }

        if mode == .extend, plan.components.contains(.thunderbolt) {
            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Thunderbolt Sweep Mode", stepNumber: step, totalSteps: totalSteps)
            ConsoleUI.printHint("Controls how burst worker/local slot combinations are explored.")
            Swift.print("")

            let currentSweepMode = sweepModeLabel(plan.sweepMode)
            if !plan.hasChosenSweepMode {
                plan.sweepMode = promptForSweepMode(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    currentMode: plan.sweepMode,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                plan.hasChosenSweepMode = true
            } else if ConsoleUI.keepOrChange(label: "sweep mode", current: currentSweepMode) {
                plan.sweepMode = promptForSweepMode(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    currentMode: plan.sweepMode,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
                plan.hasChosenSweepMode = true
            }
            step += 1

            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("CA Model Mode", stepNumber: step, totalSteps: totalSteps)
            if ConsoleUI.keepOrChange(label: "model mode", current: thunderboltCAModelModeLabel(plan.thunderboltCAModelMode)) {
                plan.thunderboltCAModelMode = promptForThunderboltCAModelMode(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    current: plan.thunderboltCAModelMode,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
            }
            step += 1

            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Scheduler Model Update", stepNumber: step, totalSteps: totalSteps)
            if ConsoleUI.keepOrChange(
                label: "model update",
                current: thunderboltPriorUpdatePolicyLabel(plan.thunderboltPriorUpdatePolicy)
            ) {
                plan.thunderboltPriorUpdatePolicy = promptForThunderboltPriorUpdatePolicy(
                    completed: completedItems(mode: mode, plan: plan, media: media),
                    current: plan.thunderboltPriorUpdatePolicy,
                    stepNumber: step,
                    totalSteps: totalSteps
                )
            }
            step += 1
        }

        // Advanced options (video preset/timeout)
        if shouldOfferAdvancedVideoOptions(plan) {
            redraw(completed: completedItems(mode: mode, plan: plan, media: media))
            printPromptTitle("Advanced Options", stepNumber: step, totalSteps: totalSteps)
            ConsoleUI.printHint("Adjusts AVFoundation video preset and timeout.")
            Swift.print("")

            if ConsoleUI.keepOrChange(label: "video preset", current: plan.videoPreset) {
                plan.videoPreset = ConsoleUI.promptRequired("Video preset")
            }
            Swift.print("")
            if ConsoleUI.keepOrChange(label: "video timeout (sec)", current: "\(plan.videoTimeoutSeconds)") {
                while true {
                    let raw = ConsoleUI.promptRequired("Video timeout (sec)")
                    if let n = Int(raw), n > 0 {
                        plan.videoTimeoutSeconds = n
                        break
                    }
                    ConsoleUI.printError("Enter a positive integer.")
                }
            }
        }
    }

    private func maybeConfigureAdvancedVideoOptions(
        plan: inout BenchmarkPlan,
        completed: [(String, String)],
        stepNumber: Int? = nil,
        totalSteps: Int? = nil
    ) throws {
        guard shouldOfferAdvancedVideoOptions(plan) else { return }

        redraw(completed: completed)
        printPromptTitle("Advanced Options", stepNumber: stepNumber, totalSteps: totalSteps)
        ConsoleUI.printHint("Adjusts AVFoundation video preset and timeout.")
        Swift.print("")

        guard ConsoleUI.confirm("Change video preset/timeout", defaultYes: false) else { return }
        Swift.print("")

        if ConsoleUI.keepOrChange(label: "video preset", current: plan.videoPreset) {
            plan.videoPreset = ConsoleUI.promptRequired("Video preset")
        }
        Swift.print("")
        if ConsoleUI.keepOrChange(label: "video timeout (sec)", current: "\(plan.videoTimeoutSeconds)") {
            while true {
                let raw = ConsoleUI.promptRequired("Video timeout (sec)")
                if let n = Int(raw), n > 0 {
                    plan.videoTimeoutSeconds = n
                    break
                }
                ConsoleUI.printError("Enter a positive integer.")
            }
        }
    }

    private func shouldPromptForProductionPriorUpdate(_ plan: BenchmarkPlan) -> Bool {
        !plan.runLimitFinder && plan.components.contains(.pipeline)
    }

    private func promptForProductionPriorUpdate() -> Bool {
        redraw(completed: [])
        ConsoleUI.printSubsectionTitle("Pre-Run Check · Production Prior")
        Swift.print("")
        ConsoleUI.printBody("This run can update benchmark-prior.json.")
        ConsoleUI.printBody("That file is the CA performance baseline used for production scheduling.")
        Swift.print("")
        ConsoleUI.printBody("If you update it now, these benchmark results become the new baseline.")
        ConsoleUI.printBody("For production-quality results, use a large, diverse video corpus.")
        Swift.print("")

        let shouldUpdate = ConsoleUI.confirm(
            "Update production prior baseline from this run",
            defaultYes: false
        )
        Swift.print("")
        if shouldUpdate {
            ConsoleUI.printSuccess("This run will update the production prior baseline.")
        } else {
            ConsoleUI.printHint("Keeping current production prior baseline unchanged.")
        }
        Swift.print("")
        return shouldUpdate
    }

    private func shouldOfferAdvancedVideoOptions(_ plan: BenchmarkPlan) -> Bool {
        if plan.runLimitFinder {
            return plan.limitConfig.workload != .image
        }
        let wanted = Set(plan.components)
        return wanted.contains(.video) || wanted.contains(.pipeline)
    }

    private func isSingleStagePipeline(mode: WizardMode, plan: BenchmarkPlan) -> Bool {
        guard mode == .singleStage, plan.components.count == 1 else { return false }
        return plan.components.first == .pipeline
    }

    private func shouldPromptForExternalSSD(mode: WizardMode, plan: BenchmarkPlan) -> Bool {
        if plan.runLimitFinder { return false }
        if plan.requiresSSDPath { return true }
        return isSingleStagePipeline(mode: mode, plan: plan)
    }

    private func shouldShowExternalSSDInRunPlan(mode: WizardMode, plan: BenchmarkPlan) -> Bool {
        if plan.runLimitFinder { return false }
        if plan.components.contains(.archive) { return true }
        return isSingleStagePipeline(mode: mode, plan: plan)
    }

    private func printRunPlan(mode: WizardMode, plan: BenchmarkPlan, media: MediaFolderChoice?) {
        #if DEBUG
        ConsoleUI.printWarning("Debug build — numbers will be slow. Use: swift run -c release benchmark")
        #else
        ConsoleUI.printField("Build", value: "release")
        #endif

        if plan.runLimitFinder {
            ConsoleUI.printField("Mode", value: "limit finder (\(plan.limitConfig.workload.rawValue))")
            ConsoleUI.printField(
                "Ramp",
                value: "start \(plan.limitConfig.startLoad), step \(plan.limitConfig.stepLoad), max \(plan.limitConfig.maxLoad)"
            )
            ConsoleUI.printField(
                "Stop thresholds",
                value: "failure>\(plan.limitConfig.errorRateThreshold), p95>\(plan.limitConfig.timeoutThresholdSeconds)s, mem>=\(plan.limitConfig.memoryCapMB)MB, thermal>=\(plan.limitConfig.thermalThreshold)"
            )
        } else {
            ConsoleUI.printField("Mode", value: modeLabel(mode))
            let wanted = Set(plan.components)
            let ordered = BenchmarkComponent.allCases.filter { wanted.contains($0) }
            let stageNames = ordered.map { $0.rawValue }.joined(separator: ", ")
            ConsoleUI.printField("Stages", value: stageNames.isEmpty ? "(none)" : stageNames)
        }

        if let media {
            ConsoleUI.printField("Media folder", value: "\(media.path) (\(media.summary))")
        } else if let folder = plan.mediaFolder, !folder.isEmpty {
            ConsoleUI.printField("Media folder", value: folder)
        }

        if shouldShowExternalSSDInRunPlan(mode: mode, plan: plan) {
            let ssd = (plan.ssdPath?.isEmpty == false) ? (plan.ssdPath ?? "") : "(not set)"
            ConsoleUI.printField("External SSD folder", value: ssd)
            if let ssdPath = plan.ssdPath, !ssdPath.isEmpty {
                let benchLeaf = plan.components.contains(.archive) ? "archive" : "realistic-pipeline"
                ConsoleUI.printHint("    Writes: \(ssdPath)/bench-results/<run-id>/\(benchLeaf)")
            }
        }

        if !plan.runLimitFinder, let tbWorkers = plan.tbWorkers, !tbWorkers.isEmpty {
            ConsoleUI.printField("Workers", value: tbWorkers)
        }
        if !plan.runLimitFinder, plan.components.contains(.thunderbolt) {
            ConsoleUI.printField("Thunderbolt mode", value: thunderboltRunTypeLabel(plan.thunderboltRunType))
            if plan.thunderboltRunType != .showdown {
                ConsoleUI.printField("Sweep mode", value: sweepModeLabel(plan.sweepMode))
            }
            if plan.thunderboltRunType == .showdown || plan.thunderboltRunType == .full {
                ConsoleUI.printField("CA model mode", value: thunderboltCAModelModeLabel(plan.thunderboltCAModelMode))
                ConsoleUI.printField("Model update", value: thunderboltPriorUpdatePolicyLabel(plan.thunderboltPriorUpdatePolicy))
                if plan.thunderboltRunType == .showdown {
                    ConsoleUI.printField(
                        "CA slot overrides",
                        value: plan.thunderboltSlotOverrides.map(thunderboltSlotOverrideLabel) ?? "default topology"
                    )
                }
                let profilesLabel: String
                if mode == .extend {
                    switch plan.thunderboltShowdownScope {
                    case .allProfiles:
                        profilesLabel = "all profiles (fixed)"
                    case .singleProfile:
                        profilesLabel = thunderboltShowdownScopeLabel(plan.thunderboltShowdownScope)
                    }
                } else {
                    profilesLabel = thunderboltShowdownScopeLabel(plan.thunderboltShowdownScope)
                }
                ConsoleUI.printField("CA profiles", value: profilesLabel)
            }
        }

        ConsoleUI.printField("Report", value: "\(plan.reportDirectory)/<timestamp>.md")
        if shouldOfferAdvancedVideoOptions(plan) {
            if plan.videoPreset != defaultVideoPreset {
                ConsoleUI.printField("Video preset", value: plan.videoPreset)
            }
            if plan.videoTimeoutSeconds != defaultVideoTimeoutSeconds {
                ConsoleUI.printField("Video timeout", value: "\(plan.videoTimeoutSeconds)s")
            }
        }
    }

    // MARK: - Helpers

    private func completedItems(mode: WizardMode, plan: BenchmarkPlan, media: MediaFolderChoice?) -> [(String, String)] {
        var items: [(String, String)] = []

        if plan.runLimitFinder {
            items.append(("Mode", "limit finder"))
            items.append(("Workload", plan.limitConfig.workload.rawValue))
        } else {
            items.append(("Mode", modeLabel(mode)))
            if mode == .singleStage, let only = plan.components.first {
                items.append(("Stage", only.rawValue))
            }
            if mode == .extend {
                items.append(("Archive", plan.components.contains(.archive) ? "included" : "not included"))
            }
        }

        if let media {
            items.append(("Media folder", media.path))
            items.append(("Media", media.summary))
        }

        if !plan.runLimitFinder {
            if plan.components.contains(.archive), let ssd = plan.ssdPath, !ssd.isEmpty {
                items.append(("External SSD", ssd))
            } else if isSingleStagePipeline(mode: mode, plan: plan) {
                let ssd = (plan.ssdPath?.isEmpty == false) ? (plan.ssdPath ?? "") : "(not set)"
                items.append(("External SSD", ssd))
            }
        }

        if !plan.runLimitFinder, let tbWorkers = plan.tbWorkers, !tbWorkers.isEmpty {
            items.append(("Workers", tbWorkers))
        }
        if shouldShowSweepMode(plan) {
            items.append(("Sweep mode", sweepModeLabel(plan.sweepMode)))
        }

        return items
    }

    private func shouldShowSweepMode(_ plan: BenchmarkPlan) -> Bool {
        guard !plan.runLimitFinder else { return false }
        guard plan.components.contains(.thunderbolt) else { return false }
        guard plan.hasChosenSweepMode else { return false }
        return plan.thunderboltRunType != .showdown
    }

    private func sweepModeLabel(_ mode: BurstSearchStrategy) -> String {
        switch mode {
        case .optimized:
            return "smart"
        case .bruteForce:
            return "exhaustive"
        }
    }

    private func thunderboltRunTypeLabel(_ mode: ThunderboltRunType) -> String {
        switch mode {
        case .full:
            return "full stage"
        case .burstSweep:
            return "burst sweep + leaderboard"
        case .showdown:
            return "showdown"
        }
    }

    private func thunderboltShowdownScopeLabel(_ scope: ThunderboltShowdownScope) -> String {
        switch scope {
        case .allProfiles:
            return "all profiles"
        case .singleProfile(let profile):
            return "single profile (\(profile.rawValue))"
        }
    }

    private func thunderboltCAModelModeLabel(_ mode: ThunderboltCAModelMode) -> String {
        switch mode {
        case .auto:
            return "auto (recommended)"
        case .strict:
            return "strict (prior parity)"
        }
    }

    private func thunderboltPriorUpdatePolicyLabel(_ policy: ThunderboltPriorUpdatePolicy) -> String {
        switch policy {
        case .off:
            return "don't update"
        case .candidateOnly:
            return "save for review"
        case .promoteForce:
            return "promote if safe"
        case .promoteGuarded:
            return "promote only if CA wins"
        }
    }

    private func thunderboltSlotOverrideLabel(_ overrides: ThunderboltCASlotOverrides) -> String {
        var parts: [String] = []
        if let local = overrides.localSlots {
            parts.append("local=\(local)")
        }
        let remote = overrides.remoteSlotsByHost
            .sorted { lhs, rhs in lhs.key < rhs.key }
            .map { "\($0.key)=\($0.value)" }
        parts.append(contentsOf: remote)
        return parts.joined(separator: ",")
    }

    private func hostsOnlyWorkersLabel(_ raw: String?) -> String {
        guard let raw = trimmedNonEmpty(raw) else { return "" }
        let workers = Config.parseThunderboltWorkers(raw)
        return workers.map(\.host).joined(separator: ",")
    }

    private func totalEditSteps(mode: WizardMode, plan: BenchmarkPlan) -> Int {
        switch mode {
        case .singleStage:
            if plan.components.first == .thunderbolt {
                var n = 1 // stage
                if plan.requiresMediaFolder { n += 1 }
                n += 1 // workers
                n += 1 // run type
                switch plan.thunderboltRunType {
                case .showdown:
                    n += 4 // slot overrides + model mode + prior update policy + scope
                case .full:
                    n += 4 // sweep mode + model mode + prior update policy + scope
                case .burstSweep:
                    n += 1 // sweep mode
                }
                if shouldOfferAdvancedVideoOptions(plan) { n += 1 }
                return n
            }
            var n = 1
            if plan.requiresMediaFolder { n += 1 }
            if shouldPromptForExternalSSD(mode: mode, plan: plan) { n += 1 }
            if shouldOfferAdvancedVideoOptions(plan) { n += 1 }
            return n
        case .limitFinder:
            return shouldOfferAdvancedVideoOptions(plan) ? 3 : 2
        case .profile, .extend:
            var n = 1
            if mode == .extend, plan.components.contains(.archive) { n += 1 }
            if mode == .extend, plan.components.contains(.thunderbolt) { n += 4 }
            if shouldOfferAdvancedVideoOptions(plan) { n += 1 }
            return n
        }
    }

    private func modeLabel(_ mode: WizardMode) -> String {
        switch mode {
        case .profile:
            return "profile"
        case .extend:
            return "extended"
        case .singleStage:
            return "single stage"
        case .limitFinder:
            return "stress test"
        }
    }

    private func mediaExpectation(mode: WizardMode, plan: BenchmarkPlan) -> MediaFolderExpectation {
        if mode == .limitFinder || plan.runLimitFinder {
            switch plan.limitConfig.workload {
            case .image: return .needsImages
            case .video: return .needsVideos
            case .mixed: return .needsImagesAndVideos
            }
        }

        if mode == .singleStage, let stage = plan.components.first {
            switch stage {
            case .image: return .needsImages
            case .video: return .needsVideos
            case .thunderbolt: return .needsVideos
            case .pipeline: return .needsImages
            default: return .any
            }
        }

        return .any
    }

    private func mediaExpectationFailureMessage(expectation: MediaFolderExpectation, summary: MediaFolderSummary) -> String? {
        switch expectation {
        case .any:
            return nil
        case .needsImages:
            return summary.imageCount > 0 ? nil : "Need at least one image file in this folder."
        case .needsVideos:
            return summary.videoCount > 0 ? nil : "Need at least one video file in this folder."
        case .needsImagesAndVideos:
            if summary.imageCount == 0 { return "Mixed workload needs at least one image file in this folder." }
            if summary.videoCount == 0 { return "Mixed workload needs at least one video file in this folder." }
            return nil
        }
    }

    // MARK: - Thunderbolt Workers

    private func promptForThunderboltWorkers(
        completed: [(String, String)],
        plan: inout BenchmarkPlan,
        stepNumber: Int,
        totalSteps: Int? = nil
    ) {
        let bridges = ThunderboltDispatcher.discoverBridgeSources()
        let port = defaultThunderboltPort

        // Scenario C: No bridges
        guard !bridges.isEmpty else {
            redraw(completed: completed)
            printPromptTitle("Thunderbolt Workers", stepNumber: stepNumber, totalSteps: totalSteps)
            ConsoleUI.printHint("Offloads video processing to worker Macs via Thunderbolt bridge.")
            Swift.print("")
            ConsoleUI.printWarning("No Thunderbolt bridges detected.")
            ConsoleUI.printHint("Connect Macs via Thunderbolt cable and assign bridge IPs to use this stage.")
            Swift.print("")
            Swift.print("  Skipping thunderbolt stage.")
            plan.components.removeAll { $0 == .thunderbolt }
            plan.tbWorkers = nil
            return
        }

        // Resolve existing workers from env or plist
        let existingRaw: String?
        let existingSource: String
        if let env = trimmedNonEmpty(ProcessInfo.processInfo.environment["TB_WORKERS"]) {
            existingRaw = env
            existingSource = "environment"
        } else {
            let plist = loadBenchmarkMediaLaunchAgentEnvironment()
            existingRaw = trimmedNonEmpty(plist["TB_WORKERS"])
            existingSource = existingRaw != nil ? "plist" : "not configured"
        }

        let workers = existingRaw.map { Config.parseThunderboltWorkers($0) } ?? []

        // Scenario A: Workers already configured
        var needsSetupHeader = true
        if !workers.isEmpty, let existingRaw {
            needsSetupHeader = false
            retry: while true {
                redraw(completed: completed)
                printPromptTitle("Thunderbolt Workers", stepNumber: stepNumber, totalSteps: totalSteps)
                ConsoleUI.printHint("Offloads video processing to worker Macs via Thunderbolt bridge.")
                Swift.print("")
                printDetectedBridges(bridges)
                ConsoleUI.printField(
                    "Workers configured",
                    value: "\(existingRaw) \(ConsoleUI.dim)(from \(existingSource))\(ConsoleUI.reset)"
                )
                Swift.print("")

                // Probe each worker
                var reachableCount = 0
                var totalSlots = 0
                for w in workers {
                    let bridge = matchingBridge(host: w.host, bridges: bridges)
                    let via = bridge.map { " via \($0.name)" } ?? ""
                    ConsoleUI.printHint("Probing \(w.host):\(port)\(via)...")
                    let ok = bridge.map { probeThunderboltWorker(host: w.host, port: port, source: $0) } ?? false
                    if ok {
                        reachableCount += 1
                        totalSlots += w.slots
                    } else {
                        ConsoleUI.printWarning("Could not reach \(w.host):\(port)")
                    }
                }

                if reachableCount > 0 {
                    let label = reachableCount == workers.count
                        ? "\(reachableCount) worker\(reachableCount == 1 ? "" : "s") reachable"
                        : "\(reachableCount) of \(workers.count) workers reachable"
                    ConsoleUI.printSuccess("\(label), \(totalSlots) slots total")
                    Swift.print("")

                    if !ConsoleUI.keepOrChange(label: "workers", current: existingRaw) {
                        plan.tbWorkers = existingRaw
                        return
                    }
                    break retry
                }

                // All unreachable
                Swift.print("")
                Swift.print("  Workers configured but not reachable. Check that worker.swift is")
                Swift.print("  running on each worker Mac.")
                Swift.print("")

                while true {
                    Swift.print("  \(ConsoleUI.bold)Action\(ConsoleUI.reset) \(ConsoleUI.dim)(Enter=retry, c=change, s=skip)\(ConsoleUI.reset): ", terminator: "")
                    fflush(stdout)
                    let input = (readLine() ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                    if input.isEmpty { continue retry }
                    if input == "c" { break retry }
                    if input == "s" {
                        plan.components.removeAll { $0 == .thunderbolt }
                        plan.tbWorkers = nil
                        return
                    }
                    ConsoleUI.printError("Press Enter to retry, c to change, or s to skip.")
                }
            }
        }

        // Scenario B: No workers configured (or change from Scenario A)
        if needsSetupHeader {
            redraw(completed: completed)
            printPromptTitle("Thunderbolt Workers", stepNumber: stepNumber, totalSteps: totalSteps)
            ConsoleUI.printHint("Offloads video processing to worker Macs via Thunderbolt bridge.")
            Swift.print("")
            printDetectedBridges(bridges)
            ConsoleUI.printWarning("No workers configured.")
            Swift.print("")
            ConsoleUI.printHint("Setting up Thunderbolt workers requires generating a worker script,")
            ConsoleUI.printHint("copying it to each worker Mac, and verifying connectivity.")
            Swift.print("")
        }

        while true {
            if !ConsoleUI.confirm("Run guided setup now", defaultYes: true) {
                plan.components.removeAll { $0 == .thunderbolt }
                plan.tbWorkers = nil
                return
            }

            Swift.print("")
            if runOrchestratorThunderbolt() {
                let plist = loadBenchmarkMediaLaunchAgentEnvironment()
                if let newWorkers = trimmedNonEmpty(plist["TB_WORKERS"]) {
                    Swift.print("")
                    ConsoleUI.printSuccess("Workers configured: \(newWorkers)")
                    Swift.print("")
                    ConsoleUI.printHint("Continuing benchmark setup...")
                    plan.tbWorkers = newWorkers
                    return
                }
            }

            Swift.print("")
            ConsoleUI.printWarning("Worker setup did not complete.")
            Swift.print("")
            if ConsoleUI.confirm("Skip thunderbolt and continue", defaultYes: true) {
                plan.components.removeAll { $0 == .thunderbolt }
                plan.tbWorkers = nil
                return
            }
        }
    }

    private func printDetectedBridges(_ bridges: [ThunderboltDispatcher.BridgeSource]) {
        ConsoleUI.printHint("Detected bridges:")
        for bridge in bridges {
            Swift.print("    \(bridge.name)  \(bridge.ip)")
        }
        Swift.print("")
    }

    private func matchingBridge(
        host: String,
        bridges: [ThunderboltDispatcher.BridgeSource]
    ) -> ThunderboltDispatcher.BridgeSource? {
        var parsed = in_addr()
        guard host.withCString({ inet_pton(AF_INET, $0, &parsed) }) == 1 else { return nil }
        let addr = UInt32(bigEndian: parsed.s_addr)
        return bridges.first(where: { (addr & $0.mask) == $0.network })
    }

    private func probeThunderboltWorker(
        host: String,
        port: Int,
        source: ThunderboltDispatcher.BridgeSource
    ) -> Bool {
        let timeoutMS = Config.intDefaults["TB_CONNECT_TIMEOUT"]?.fallback ?? 500
        if let fd = ThunderboltTransport.connect(host: host, port: port, timeoutMS: timeoutMS, sourceIP: source.ip) {
            ThunderboltTransport.closeConnection(fd: fd)
            return true
        }
        return false
    }

    private func runOrchestratorThunderbolt() -> Bool {
        let dir = (CommandLine.arguments[0] as NSString).deletingLastPathComponent
        let path = "\(dir)/Orchestrator"
        guard FileManager.default.fileExists(atPath: path) else {
            ConsoleUI.printError("Could not find orchestrator binary at \(path)")
            return false
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: path)
        process.arguments = ["--thunderbolt"]
        process.standardInput = FileHandle.standardInput
        process.standardOutput = FileHandle.standardOutput
        process.standardError = FileHandle.standardError

        do {
            try process.run()
            process.waitUntilExit()
            return process.terminationStatus == 0
        } catch {
            ConsoleUI.printError("Failed to run orchestrator: \(error)")
            return false
        }
    }

}
