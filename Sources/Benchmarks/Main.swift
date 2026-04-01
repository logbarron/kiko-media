import Foundation
import Darwin
import KikoMediaCore

private let benchmarkCommand = "swift run benchmark"
private let benchmarkAliasCommand = "swift run benchmarks"
private let benchmarkReleaseCommand = "swift run -c release benchmark"
private let benchmarkReleaseAliasCommand = "swift run -c release benchmarks"
let defaultVideoPreset = "AVAssetExportPreset1920x1080"
let defaultVideoTimeoutSeconds = 300
let defaultThunderboltPort = Config.intDefaults["TB_PORT"]!.fallback
let defaultThunderboltConnectTimeoutMS = Config.intDefaults["TB_CONNECT_TIMEOUT"]!.fallback

@main
struct BenchmarkRunner {
    static func main() async throws {
        let args = Array(CommandLine.arguments.dropFirst())

        if args.contains("--help") || args.contains("-h") {
            printHelp()
            return
        }
        if args.contains("--help-advanced") {
            printAdvancedUsage()
            return
        }
        if args.contains("--list") {
            printBenchmarkList()
            return
        }

        let plan: BenchmarkPlan
        if args.isEmpty || (args.contains("--wizard") && !args.contains("--json")) {
            plan = try BenchmarkWizard().run()
            ConsoleUI.clearScreen()
        } else {
            do {
                plan = try parseCLIArguments(args)
            } catch let error as CLIArgumentError {
                printCLIErrorAndExit(error)
            } catch {
                printCLIErrorAndExit(.invalidValue(option: "arguments", value: "\(error)", reason: "unknown parse failure"))
            }
        }

        DateUtils.configure(eventTimezone: Config.envString("EVENT_TIMEZONE"))

        if let tbWorkers = plan.tbWorkers {
            setenv("TB_WORKERS", tbWorkers, 1)
        }

        do {
            try await runWithSIGINTHandling(plan: plan)
        } catch let error as BenchmarkInterruptError {
            fputs("\n", stderr)
            fputs("Benchmark interrupted: \(error)\n", stderr)
            fputs("\n", stderr)
            Foundation.exit(benchmarkExitCode(for: error))
        } catch {
            fputs("\n", stderr)
            fputs("Benchmark failed: \(error)\n", stderr)
            fputs("\n", stderr)
            Foundation.exit(benchmarkExitCode(for: error))
        }
    }
}

// MARK: - Execution

struct BenchmarkSIGINTHandlingHooks: @unchecked Sendable {
    var installInterruptHandler: (_ onInterrupt: @escaping () -> Void) -> (() -> Void)
    var emitInterruptMessage: (_ isJSONMode: Bool) -> Void
    var runBenchmarksOperation: (_ plan: BenchmarkPlan, _ interruptState: BenchmarkInterruptState) async throws -> Void
    var classifyInterruptedError: (_ error: Error, _ interruptState: BenchmarkInterruptState?) -> Bool

    static func live() -> BenchmarkSIGINTHandlingHooks {
        BenchmarkSIGINTHandlingHooks(
            installInterruptHandler: installSIGINTHandler,
            emitInterruptMessage: { isJSONMode in
                emitInterruptCancellationMessage(isJSONMode: isJSONMode)
            },
            runBenchmarksOperation: { plan, interruptState in
                try await runBenchmarks(plan: plan, interruptState: interruptState)
            },
            classifyInterruptedError: isBenchmarkInterrupted
        )
    }
}

func benchmarkExitCode(for error: Error) -> Int32 {
    if error is BenchmarkInterruptError {
        return 130
    }
    return 1
}

func runWithSIGINTHandling(
    plan: BenchmarkPlan,
    hooks: BenchmarkSIGINTHandlingHooks = .live()
) async throws {
    let interruptState = BenchmarkInterruptState()
    let benchmarkTask = Task {
        try await hooks.runBenchmarksOperation(plan, interruptState)
    }

    let uninstallInterruptHandler = hooks.installInterruptHandler {
        let isFirstInterrupt = interruptState.requestInterrupt()
        if isFirstInterrupt {
            hooks.emitInterruptMessage(plan.jsonMode)
        }
        benchmarkTask.cancel()
    }
    defer {
        uninstallInterruptHandler()
    }

    do {
        try await benchmarkTask.value
    } catch {
        if hooks.classifyInterruptedError(error, interruptState) {
            throw BenchmarkInterruptError.interrupted
        }
        throw error
    }
}

private func installSIGINTHandler(_ onInterrupt: @escaping () -> Void) -> (() -> Void) {
    let signalQueue = DispatchQueue(label: "com.kiko.media.benchmark.sigint")
    signal(SIGINT, SIG_IGN)

    let sigSource = DispatchSource.makeSignalSource(signal: SIGINT, queue: signalQueue)
    sigSource.setEventHandler(handler: onInterrupt)
    sigSource.resume()

    return {
        sigSource.cancel()
        signal(SIGINT, SIG_DFL)
    }
}

private func emitInterruptCancellationMessage(isJSONMode: Bool) {
    if isJSONMode {
        fputs("\nBenchmark interrupted by Ctrl-C. Cancelling run and finalizing state...\n", stderr)
        return
    }
    BenchOutput.write("\r\u{1B}[2K")
    BenchOutput.line("  \(ConsoleUI.yellow)Ctrl-C received. Cancelling run and finalizing state...\(ConsoleUI.reset)")
}

private func runBenchmarks(
    plan: BenchmarkPlan,
    interruptState: BenchmarkInterruptState? = nil
) async throws {
    try interruptState?.throwIfInterrupted()

    if plan.jsonMode {
        try await runJSONMode(plan: plan, interruptState: interruptState)
        return
    }

    let reportURL = try BenchOutput.startReport(reportDirectory: plan.reportDirectory)
    var didPrintCompletionFooter = false
    defer {
        if !didPrintCompletionFooter, interruptState?.isInterrupted == true {
            printInterruptedFooter(reportURL: reportURL)
        }
        BenchOutput.finishReport()
    }
    let memoryGuardConfig = BenchmarkMemoryGuard.bootstrap()

    BenchOutput.line("  \(ConsoleUI.bold)kiko-media benchmark runner\(ConsoleUI.reset)")
    BenchOutput.line("  \(ConsoleUI.dim)Run summary and measurements.\(ConsoleUI.reset)")
    BenchOutput.line("")

    var runMetadata: [BenchmarkRuntimeField] = [
        BenchmarkRuntimeField(label: "Timestamp", value: "\(Date())"),
        BenchmarkRuntimeField(label: "Report", value: reportURL.path),
        BenchmarkRuntimeField(label: "Report directory", value: "\(plan.reportDirectory)/"),
    ]

    if plan.runLimitFinder {
        runMetadata.append(BenchmarkRuntimeField(label: "Mode", value: "limit finder"))
    } else {
        let stageList = orderedStages(plan.components).map { $0.rawValue }.joined(separator: ", ")
        runMetadata.append(BenchmarkRuntimeField(label: "Stages", value: stageList))
    }

    runMetadata.append(BenchmarkRuntimeField(label: "Video preset", value: plan.videoPreset))
    runMetadata.append(BenchmarkRuntimeField(label: "Video timeout", value: "\(plan.videoTimeoutSeconds)s"))
    if let memoryGuardConfig {
        runMetadata.append(BenchmarkRuntimeField(label: "Memory guard", value: "enabled"))
        runMetadata.append(BenchmarkRuntimeField(label: "Memory warning", value: "\(memoryGuardConfig.warningMB)MB"))
        runMetadata.append(BenchmarkRuntimeField(label: "Memory stop", value: "\(memoryGuardConfig.limitMB)MB"))
        runMetadata.append(BenchmarkRuntimeField(label: "Memory source", value: memoryGuardConfig.source))
    } else {
        runMetadata.append(BenchmarkRuntimeField(label: "Memory guard", value: BenchmarkMemoryGuard.statusSummary()))
    }
    if let ssdFolder = plan.ssdPath, !ssdFolder.isEmpty {
        runMetadata.append(BenchmarkRuntimeField(label: "External SSD folder", value: ssdFolder))
        runMetadata.append(
            BenchmarkRuntimeField(
                label: "Keep SSD artifacts",
                value: plan.keepSSDBenchArtifacts ? "yes" : "no"
            )
        )
    } else {
        runMetadata.append(BenchmarkRuntimeField(label: "External SSD folder", value: "(not set)"))
    }

    if let tbWorkers = plan.tbWorkers, !tbWorkers.isEmpty {
        runMetadata.append(BenchmarkRuntimeField(label: "Workers", value: tbWorkers))
    }
    if !plan.runLimitFinder, plan.components.contains(.thunderbolt) {
        runMetadata.append(BenchmarkRuntimeField(label: "Thunderbolt mode", value: thunderboltRunTypeLabel(plan.thunderboltRunType)))
        if plan.thunderboltRunType != .showdown {
            runMetadata.append(BenchmarkRuntimeField(label: "Sweep mode", value: sweepModeLabel(plan.sweepMode)))
        }
        let thunderboltUsesCAModel =
            plan.runCAAcceptance
            || plan.caArrivalProfile != nil
            || plan.thunderboltRunType == .showdown
            || plan.thunderboltRunType == .full
        let thunderboltUsesShowdownPath =
            !plan.runCAAcceptance
            && plan.caArrivalProfile == nil
            && (plan.thunderboltRunType == .showdown || plan.thunderboltRunType == .full)
        if thunderboltUsesCAModel {
            runMetadata.append(
                BenchmarkRuntimeField(
                    label: "CA model mode",
                    value: thunderboltCAModelModeLabel(plan.thunderboltCAModelMode)
                )
            )
        }
        if plan.thunderboltRunType == .showdown {
            runMetadata.append(
                BenchmarkRuntimeField(
                    label: "CA profiles",
                    value: thunderboltShowdownScopeLabel(plan.thunderboltShowdownScope)
                )
            )
            runMetadata.append(BenchmarkRuntimeField(label: "Policy order", value: "order-neutral (FIFO->CA and CA->FIFO)"))
            runMetadata.append(
                BenchmarkRuntimeField(
                    label: "CA slot overrides",
                    value: plan.thunderboltSlotOverrides.map(thunderboltSlotOverrideLabel) ?? "default topology"
                )
            )
        }
        if plan.thunderboltRunType == .full {
            runMetadata.append(
                BenchmarkRuntimeField(
                    label: "CA profiles",
                    value: thunderboltShowdownScopeLabel(plan.thunderboltShowdownScope)
                )
            )
        }
        if thunderboltUsesShowdownPath {
            runMetadata.append(
                BenchmarkRuntimeField(
                    label: "Model update",
                    value: thunderboltPriorUpdatePolicyLabel(plan.thunderboltPriorUpdatePolicy)
                )
            )
        }
    }
    if let policy = plan.caSchedulerPolicy {
        runMetadata.append(BenchmarkRuntimeField(label: "Scheduler policy", value: policy.rawValue))
    }
    if let profile = plan.caArrivalProfile {
        runMetadata.append(BenchmarkRuntimeField(label: "Arrival profile", value: profile.rawValue))
    }
    if let rawOut = plan.caRawOutputPath, !rawOut.isEmpty {
        runMetadata.append(BenchmarkRuntimeField(label: "CA raw output", value: rawOut))
    }
    if let summaryOut = plan.caSummaryOutputPath, !summaryOut.isEmpty {
        runMetadata.append(BenchmarkRuntimeField(label: "CA summary output", value: summaryOut))
    }
    if plan.runCAAcceptance {
        runMetadata.append(BenchmarkRuntimeField(label: "CA acceptance", value: "enabled"))
        if let output = plan.caAcceptanceOutputPath, !output.isEmpty {
            runMetadata.append(BenchmarkRuntimeField(label: "Acceptance output", value: output))
        }
    }
    if !plan.runLimitFinder, plan.components.contains(.pipeline) {
        let priorUpdate: String
        switch plan.updateProductionPriorFromRun {
        case true:
            priorUpdate = "enabled"
        case false:
            priorUpdate = "disabled"
        case nil:
            priorUpdate = "default"
        }
        runMetadata.append(BenchmarkRuntimeField(label: "Production prior update", value: priorUpdate))
    }

    if plan.runLimitFinder {
        runMetadata.append(BenchmarkRuntimeField(label: "Limit workload", value: plan.limitConfig.workload.rawValue))
        runMetadata.append(BenchmarkRuntimeField(label: "Limit start", value: "\(plan.limitConfig.startLoad)"))
        runMetadata.append(BenchmarkRuntimeField(label: "Limit step", value: "\(plan.limitConfig.stepLoad)"))
        if plan.limitConfig.maxLoad > 0 {
            runMetadata.append(BenchmarkRuntimeField(label: "Limit max", value: "\(plan.limitConfig.maxLoad)"))
        } else {
            runMetadata.append(BenchmarkRuntimeField(label: "Limit max", value: "auto"))
        }
        if let imageJobs = plan.limitConfig.imageJobs {
            runMetadata.append(BenchmarkRuntimeField(label: "Limit image jobs", value: "\(imageJobs)"))
        }
        if let videoJobs = plan.limitConfig.videoJobs {
            runMetadata.append(BenchmarkRuntimeField(label: "Limit video jobs", value: "\(videoJobs)"))
        }
        runMetadata.append(BenchmarkRuntimeField(label: "Limit error threshold", value: "\(plan.limitConfig.errorRateThreshold)"))
        runMetadata.append(
            BenchmarkRuntimeField(
                label: "Limit timeout threshold",
                value: "\(plan.limitConfig.timeoutThresholdSeconds)s (P95)"
            )
        )
        runMetadata.append(BenchmarkRuntimeField(label: "Limit memory cap", value: "\(plan.limitConfig.memoryCapMB)MB"))
        runMetadata.append(BenchmarkRuntimeField(label: "Limit thermal threshold", value: plan.limitConfig.thermalThreshold))
        runMetadata.append(BenchmarkRuntimeField(label: "Limit refine steps", value: "\(plan.limitConfig.refinementSteps)"))
        runMetadata.append(BenchmarkRuntimeField(label: "Limit soak", value: "\(plan.limitConfig.soakSeconds)s"))
        if let jsonOut = plan.limitConfig.jsonOutputPath, !jsonOut.isEmpty {
            runMetadata.append(BenchmarkRuntimeField(label: "Limit JSON output", value: jsonOut))
        }
    }

    BenchmarkRuntimeRenderer.printFieldSection("Run Metadata", fields: runMetadata)

    // Contract: Always print (and write) an explicit benchmark manifest up front.
    var manifestRows: [BenchmarkRuntimeMenuRow] = []
    if plan.runLimitFinder {
        manifestRows.append(
            BenchmarkRuntimeMenuRow(
                title: "limit finder (\(plan.limitConfig.workload.rawValue))",
                details: ["Finds a stable concurrency knee via ramp/refine/soak and writes a result summary."]
            )
        )
    } else {
        for stage in orderedStages(plan.components) {
            let spec = BenchmarkCatalog.spec(for: stage)
            let hasSSDPath = (plan.ssdPath ?? "").isEmpty == false
            var suffix = ""
            if stage == .archive, !hasSSDPath {
                suffix = " (skipped: no external SSD path set)"
            }
            var details = [spec.detail, "Expected runtime: \(spec.expectedRuntime)"]
            if !suffix.isEmpty {
                details.append("Status: skipped because external SSD path is not set.")
            }
            if stage == .pipeline {
                if hasSSDPath {
                    details.append("Archive in realistic pipeline: enabled via external SSD path.")
                } else {
                    details.append("Archive in realistic pipeline: skipped because external SSD path is not set.")
                }
            }
            manifestRows.append(BenchmarkRuntimeMenuRow(title: "\(spec.id.rawValue) · \(spec.title)", details: details))
        }
    }
    BenchmarkRuntimeRenderer.printMenuSection("Benchmark Manifest", rows: manifestRows)

    BenchmarkRuntimeRenderer.printSectionTitle("Runtime State")
    BenchmarkRuntimeRenderer.printBody("Media folder is never modified.")
    BenchmarkRuntimeRenderer.printField("Temp", "\(NSTemporaryDirectory())kiko-bench-*-<uuid> (auto-cleaned)")
    if !plan.runLimitFinder, let ssdPath = plan.ssdPath, !ssdPath.isEmpty {
        let stages = orderedStages(plan.components)
        let benchLeaf: String?
        if stages.contains(.archive) {
            benchLeaf = "archive"
        } else if stages.contains(.pipeline) {
            benchLeaf = "realistic-pipeline"
        } else {
            benchLeaf = nil
        }
        if let benchLeaf {
            BenchmarkRuntimeRenderer.printField(
                "Archive bench dir",
                "\(ssdPath)/bench-results/<run-id>/\(benchLeaf) (removed unless keep is enabled)"
            )
        }
    }

    let media: [MediaFile]
    let mediaPath: String?
    try interruptState?.throwIfInterrupted()
    if plan.runLimitFinder || plan.requiresMediaFolder {
        guard let folder = plan.mediaFolder, !folder.isEmpty else {
            throw CLIArgumentError.missingValue(option: "<media-folder>")
        }
        mediaPath = folder
        media = try loadMediaFolder(path: folder)

        let summary = try summarizeMediaFolder(path: folder)
        BenchmarkRuntimeRenderer.printSectionTitle("Media Summary")
        BenchmarkRuntimeRenderer.printField("Folder", folder)
        BenchmarkRuntimeRenderer.printBody(summary.summaryLine)
    } else {
        mediaPath = nil
        media = []
    }

    let hw = HardwareProfile.detect()
    BenchmarkRuntimeRenderer.printSectionTitle("System")
    BenchmarkRuntimeRenderer.printField("Chip", hw.summary)
    BenchmarkRuntimeRenderer.printField("OS", ProcessInfo.processInfo.operatingSystemVersionString)
    BenchmarkRuntimeRenderer.printField("Video encode engines", "\(hw.videoEncodeEngines) (via IOKit AppleAVE2Driver)")
    BenchmarkRuntimeRenderer.printField("HW codecs", "\(hw.hwEncoderNames.count) available")
    for codec in hw.hwEncoderNames {
        BenchmarkRuntimeRenderer.printDetail(codec)
    }
    BenchmarkRuntimeRenderer.printField("Memory baseline", "\(getMemoryMB())MB")
    if let memoryGuardConfig {
        BenchmarkRuntimeRenderer.printField("Memory guardrail", "\(memoryGuardConfig.warningMB)MB warning, \(memoryGuardConfig.limitMB)MB stop")
    } else {
        BenchmarkRuntimeRenderer.printField("Memory guardrail", BenchmarkMemoryGuard.statusSummary())
    }
    BenchmarkRuntimeRenderer.printField("Thermal", getThermalState())

    await PipelineWalkthrough.printIfEnabled(plan: plan, media: media)

    if plan.runLimitFinder {
        try interruptState?.throwIfInterrupted()
        guard let mediaPath else { throw CLIArgumentError.missingValue(option: "<media-folder>") }
        BenchmarkRuntimeRenderer.printSectionTitle("Benchmark Stage: limit finder")
        try BenchmarkMemoryGuard.checkpoint(stage: "limit", detail: "stage start")
        try await benchmarkLimitFinder(
            corpus: media,
            corpusPath: mediaPath,
            preset: plan.videoPreset,
            timeout: plan.videoTimeoutSeconds,
            hardware: hw,
            config: plan.limitConfig
        )
        try BenchmarkMemoryGuard.checkpoint(stage: "limit", detail: "stage end")
        try interruptState?.throwIfInterrupted()
        didPrintCompletionFooter = true
        printFooter(reportURL: reportURL)
        return
    }

    for stage in orderedStages(plan.components) {
        try interruptState?.throwIfInterrupted()
        let spec = BenchmarkCatalog.spec(for: stage)
        BenchmarkRuntimeRenderer.printSectionTitle("Benchmark Stage: \(spec.id.rawValue) · \(spec.title)")
        let stageStartMemory = getMemoryMB()
        try BenchmarkMemoryGuard.checkpoint(stage: spec.id.rawValue, detail: "stage start")

        switch stage {
        case .image:
            try benchmarkImageThumbnails(corpus: media)
            try benchmarkImagePreviews(corpus: media)
            benchmarkImageTimestamp(corpus: media)
            try benchmarkImageMemory(corpus: media)

        case .video:
            printVideoEncoders()
            BenchOutput.line("")
            try await benchmarkVideoAnalysis(corpus: media)
            BenchOutput.line("")
            try await benchmarkDecodeOnly(corpus: media)
            BenchOutput.line("")
            try await benchmarkVideoTranscode(corpus: media, preset: plan.videoPreset, timeout: plan.videoTimeoutSeconds)
            BenchOutput.line("")
            try await benchmarkVideoThumbnails(corpus: media)

        case .sha256:
            try benchmarkSHA256(corpus: media)

        case .db:
            try await benchmarkDatabase()

        case .archive:
            guard let ssdPath = plan.ssdPath, !ssdPath.isEmpty else {
                BenchmarkRuntimeRenderer.printBody("Skipping archive stage (no --ssd-path provided)")
                break
            }
            try await benchmarkArchiveToSSD(
                corpus: media,
                ssdPath: ssdPath,
                sha256BufferSize: 1_048_576,
                keepArtifacts: plan.keepSSDBenchArtifacts
            )

        case .thunderbolt:
            if plan.runCAAcceptance {
                try await benchmarkThunderboltCAAcceptance(
                    corpus: media,
                    preset: plan.videoPreset,
                    timeout: plan.videoTimeoutSeconds,
                    hardware: hw,
                    outputPath: plan.caAcceptanceOutputPath,
                    modelMode: plan.thunderboltCAModelMode
                )
            } else if let profile = plan.caArrivalProfile {
                let policy = plan.caSchedulerPolicy ?? .fifo
                _ = try await benchmarkThunderboltCA(
                    corpus: media,
                    preset: plan.videoPreset,
                    timeout: plan.videoTimeoutSeconds,
                    hardware: hw,
                    policy: policy,
                    profile: profile,
                    rawOutputPath: plan.caRawOutputPath,
                    summaryOutputPath: plan.caSummaryOutputPath,
                    modelMode: plan.thunderboltCAModelMode
                )
            } else {
                switch plan.thunderboltRunType {
                case .showdown:
                    let profiles: [CAArrivalProfile]
                    switch plan.thunderboltShowdownScope {
                    case .allProfiles:
                        profiles = CAArrivalProfile.allCases
                    case .singleProfile(let profile):
                        profiles = [profile]
                    }
                    try await benchmarkThunderboltShowdown(
                        corpus: media,
                        preset: plan.videoPreset,
                        timeout: plan.videoTimeoutSeconds,
                        hardware: hw,
                        profiles: profiles,
                        slotOverrides: plan.thunderboltSlotOverrides,
                        modelMode: plan.thunderboltCAModelMode,
                        priorUpdatePolicy: plan.thunderboltPriorUpdatePolicy
                    )
                case .burstSweep:
                    try await benchmarkThunderbolt(
                        corpus: media,
                        preset: plan.videoPreset,
                        timeout: plan.videoTimeoutSeconds,
                        hardware: hw,
                        sweepMode: plan.sweepMode,
                        includeShowdown: false,
                        showdownModelMode: plan.thunderboltCAModelMode,
                        showdownPriorUpdatePolicy: .candidateOnly
                    )
                case .full:
                    let profiles: [CAArrivalProfile]
                    switch plan.thunderboltShowdownScope {
                    case .allProfiles:
                        profiles = CAArrivalProfile.allCases
                    case .singleProfile(let profile):
                        profiles = [profile]
                    }
                    try await benchmarkThunderbolt(
                        corpus: media,
                        preset: plan.videoPreset,
                        timeout: plan.videoTimeoutSeconds,
                        hardware: hw,
                        sweepMode: plan.sweepMode,
                        showdownProfiles: profiles,
                        showdownModelMode: plan.thunderboltCAModelMode,
                        showdownPriorUpdatePolicy: plan.thunderboltPriorUpdatePolicy
                    )
                }
            }

        case .pipeline:
            let imageSweep = try await benchmarkImageConcurrency(corpus: media, hardware: hw)
            let videoSweepResult = try await benchmarkVideoConcurrency(
                corpus: media,
                hardware: hw,
                preset: plan.videoPreset,
                timeout: plan.videoTimeoutSeconds
            )
            let videoSweep = videoSweepResult.points
            let mixedSweep = try await benchmarkMixedRatioSweep(corpus: media, hardware: hw, preset: plan.videoPreset, timeout: plan.videoTimeoutSeconds)
            let recommendation = printConcurrencyRecommendationCard(
                hardware: hw,
                imageSweep: imageSweep,
                videoSweep: videoSweep,
                mixedSweep: mixedSweep
            )
            let shouldAttemptPriorUpdate = shouldAttemptPipelinePriorUpdate(plan.updateProductionPriorFromRun)
            do {
                let (candidateArtifact, outcome) = try updatePipelineBenchmarkPriorFromRun(
                    corpus: media,
                    videoSweep: videoSweep,
                    corpusFrameCounts: videoSweepResult.corpusFrameCounts,
                    localAffineSamples: videoSweepResult.localAffineSamples,
                    hardware: hw,
                    preset: plan.videoPreset,
                    shouldAttemptUpdate: shouldAttemptPriorUpdate
                )
                reportThunderboltPriorUpdate(
                    candidateArtifact: candidateArtifact,
                    outcome: outcome,
                    outputLine: BenchOutput.line
                )
            } catch {
                reportThunderboltPriorUpdate(
                    candidateArtifact: nil,
                    outcome: .failed(error),
                    outputLine: BenchOutput.line
                )
                throw error
            }
            let imgC = recommendation?.imageConcurrency ?? 4
            let vidC = recommendation?.videoConcurrency ?? 2
            try await benchmarkRealisticPipeline(
                corpus: media,
                imageConcurrency: imgC,
                videoConcurrency: vidC,
                ssdPath: plan.ssdPath,
                keepArtifacts: plan.keepSSDBenchArtifacts,
                preset: plan.videoPreset,
                timeout: plan.videoTimeoutSeconds
            )

        case .comparison:
            try benchmarkJPEGQualityCurve(corpus: media)
            try benchmarkThumbnailSizeCurve(corpus: media)
            try benchmarkSHA256BufferCurve(corpus: media)
            await benchmarkTranscodePresetComparison(
                corpus: media,
                timeoutSeconds: plan.videoTimeoutSeconds
            )
        }

        let stageEndMemory = getMemoryMB()
        let delta = stageEndMemory - stageStartMemory
        let deltaText = delta >= 0 ? "+\(delta)MB" : "\(delta)MB"
        BenchmarkRuntimeRenderer.printField(
            "Stage memory",
            "\(stageStartMemory)MB -> \(stageEndMemory)MB (\(deltaText))"
        )
        try BenchmarkMemoryGuard.checkpoint(stage: spec.id.rawValue, detail: "stage end")
        try interruptState?.throwIfInterrupted()
    }

    try interruptState?.throwIfInterrupted()
    didPrintCompletionFooter = true
    printFooter(reportURL: reportURL)
}

private func runJSONMode(
    plan: BenchmarkPlan,
    interruptState: BenchmarkInterruptState? = nil
) async throws {
    try interruptState?.throwIfInterrupted()
    guard !plan.runCAAcceptance, plan.caAcceptanceOutputPath == nil else {
        throw CLIArgumentError.invalidValue(
            option: "--json",
            value: "enabled",
            reason: "cannot combine with --ca-acceptance or --ca-acceptance-out"
        )
    }
    guard !plan.runLimitFinder else {
        throw CLIArgumentError.invalidValue(
            option: "--json",
            value: "enabled",
            reason: "requires --stage thunderbolt or --stage pipeline"
        )
    }
    guard plan.components.count == 1 else {
        throw CLIArgumentError.invalidValue(
            option: "--json",
            value: "enabled",
            reason: "requires exactly one --stage"
        )
    }
    guard let mediaFolder = plan.mediaFolder, !mediaFolder.isEmpty else {
        throw CLIArgumentError.missingValue(option: "<media-folder>")
    }

    _ = BenchmarkMemoryGuard.bootstrap()
    let media = try loadMediaFolder(path: mediaFolder)
    let hw = HardwareProfile.detect()
    BenchOutput.redirectToStderr(true)
    defer { BenchOutput.redirectToStderr(false) }
    try BenchmarkMemoryGuard.checkpoint(stage: "json", detail: "stage start")

    let json: String
    switch plan.components.first {
    case .thunderbolt:
        try interruptState?.throwIfInterrupted()
        if let profile = plan.caArrivalProfile {
            let policy = plan.caSchedulerPolicy ?? .fifo
            json = try await benchmarkThunderboltCAJSON(
                corpus: media,
                preset: plan.videoPreset,
                timeout: plan.videoTimeoutSeconds,
                hardware: hw,
                policy: policy,
                profile: profile,
                modelMode: plan.thunderboltCAModelMode
            )
        } else {
            json = try await benchmarkThunderboltJSON(
                corpus: media,
                preset: plan.videoPreset,
                timeout: plan.videoTimeoutSeconds,
                hardware: hw,
                sweepMode: plan.sweepMode
            )
        }
    case .pipeline:
        try interruptState?.throwIfInterrupted()
        json = try await benchmarkPipelineJSON(
            corpus: media,
            hardware: hw,
            preset: plan.videoPreset,
            timeout: plan.videoTimeoutSeconds
        )
    default:
        throw CLIArgumentError.invalidValue(
            option: "--json",
            value: plan.components.first?.rawValue ?? "unknown",
            reason: "JSON mode supports --stage thunderbolt or --stage pipeline"
        )
    }

    try interruptState?.throwIfInterrupted()
    try BenchmarkMemoryGuard.checkpoint(stage: "json", detail: "stage end")
    guard let data = json.data(using: .utf8) else {
        throw CLIArgumentError.invalidValue(
            option: "--json",
            value: "encoding",
            reason: "could not encode payload"
        )
    }
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write(Data([0x0A]))
}

private func orderedStages(_ stages: [BenchmarkComponent]) -> [BenchmarkComponent] {
    let wanted = Set(stages)
    return BenchmarkComponent.allCases.filter { wanted.contains($0) }
}

private func thunderboltRunTypeLabel(_ mode: ThunderboltRunType) -> String {
    switch mode {
    case .full:
        return "full stage"
    case .burstSweep:
        return "burst sweep + leaderboard"
    case .showdown:
        return "fifo vs ca showdown"
    }
}

private func sweepModeLabel(_ mode: BurstSearchStrategy) -> String {
    switch mode {
    case .optimized:
        return "smart"
    case .bruteForce:
        return "exhaustive"
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
    case .strict:
        return "strict (prior only)"
    case .auto:
        return "auto (prior -> caps -> local fallback)"
    }
}

private func thunderboltSlotOverrideLabel(_ overrides: ThunderboltCASlotOverrides) -> String {
    var parts: [String] = []
    if let localSlots = overrides.localSlots {
        parts.append("local=\(localSlots)")
    }
    let remote = overrides.remoteSlotsByHost
        .sorted { lhs, rhs in lhs.key < rhs.key }
        .map { "\($0.key)=\($0.value)" }
    parts.append(contentsOf: remote)
    return parts.isEmpty ? "(none)" : parts.joined(separator: ",")
}

private func printFooter(reportURL: URL) {
    let peakMemory = max(getMemoryMB(), BenchmarkMemoryGuard.peakMB())
    BenchmarkRuntimeRenderer.printFieldSection(
        "Run Complete",
        fields: [
            BenchmarkRuntimeField(label: "Thermal", value: getThermalState()),
            BenchmarkRuntimeField(label: "Memory", value: "\(getMemoryMB())MB"),
            BenchmarkRuntimeField(label: "Memory peak", value: "\(peakMemory)MB"),
            BenchmarkRuntimeField(label: "Memory guard", value: BenchmarkMemoryGuard.statusSummary()),
            BenchmarkRuntimeField(label: "Report", value: reportURL.path),
        ]
    )
    BenchOutput.line("")
}

private func printInterruptedFooter(reportURL: URL) {
    let peakMemory = max(getMemoryMB(), BenchmarkMemoryGuard.peakMB())
    BenchOutput.write("\r\u{1B}[2K")
    BenchmarkRuntimeRenderer.printFieldSection(
        "Run Interrupted",
        fields: [
            BenchmarkRuntimeField(label: "Reason", value: "SIGINT (Ctrl-C)"),
            BenchmarkRuntimeField(label: "Thermal", value: getThermalState()),
            BenchmarkRuntimeField(label: "Memory", value: "\(getMemoryMB())MB"),
            BenchmarkRuntimeField(label: "Memory peak", value: "\(peakMemory)MB"),
            BenchmarkRuntimeField(label: "Memory guard", value: BenchmarkMemoryGuard.statusSummary()),
            BenchmarkRuntimeField(label: "Report", value: reportURL.path),
        ]
    )
    BenchOutput.line("  \(ConsoleUI.yellow)Partial run finalized cleanly.\(ConsoleUI.reset)")
    BenchOutput.line("")
}

// MARK: - CLI Parsing

private enum CLIArgumentError: Error, CustomStringConvertible {
    case unknownOption(String)
    case missingValue(option: String)
    case invalidValue(option: String, value: String, reason: String)

    var description: String {
        switch self {
        case .unknownOption(let option):
            return "unknown option '\(option)'."
        case .missingValue(let option):
            return "missing value for '\(option)'."
        case .invalidValue(let option, let value, let reason):
            return "invalid value '\(value)' for '\(option)' (\(reason))."
        }
    }
}

private func parseCLIArguments(_ args: [String]) throws -> BenchmarkPlan {
    var plan = BenchmarkPlan()

    var stage: BenchmarkComponent? = nil
    var mediaFolder: String? = nil
    var caModelModeWasExplicit = false

    var index = 0
    while index < args.count {
        let arg = args[index]

        switch arg {
        case "--wizard":
            index += 1

        case "--component", "--stage":
            let value = try requireValue(for: arg, args: args, index: index).lowercased()
            guard let parsed = BenchmarkComponent(rawValue: value) else {
                throw CLIArgumentError.invalidValue(
                    option: arg,
                    value: value,
                    reason: "expected one of: \(BenchmarkComponent.allCases.map(\.rawValue).joined(separator: ", "))"
                )
            }
            stage = parsed
            index += 2

        case "--pipeline":
            stage = .pipeline
            index += 1

        case "--media-folder":
            mediaFolder = try requireValue(for: arg, args: args, index: index)
            index += 2

        case "--video-preset":
            plan.videoPreset = try requireValue(for: arg, args: args, index: index)
            index += 2

        case "--video-timeout":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            plan.videoTimeoutSeconds = parsed
            index += 2

        case "--tb-workers":
            let value = try requireValue(for: arg, args: args, index: index)
            let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else {
                throw CLIArgumentError.invalidValue(
                    option: arg,
                    value: value,
                    reason: "expected host:slots[,host:slots]"
                )
            }
            guard setenv("TB_WORKERS", trimmed, 1) == 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "could not set TB_WORKERS")
            }
            index += 2

        case "--tb-port":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), (1...65_535).contains(parsed) else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected an integer in 1...65535")
            }
            guard setenv("TB_PORT", String(parsed), 1) == 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "could not set TB_PORT")
            }
            index += 2

        case "--tb-connect-timeout":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value) else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected an integer in 100...30000")
            }
            let normalized = (1...30).contains(parsed) ? parsed * 1_000 : parsed
            guard (100...30_000).contains(normalized) else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected an integer in 100...30000 (legacy 1...30 seconds also accepted)")
            }
            guard setenv("TB_CONNECT_TIMEOUT", String(normalized), 1) == 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "could not set TB_CONNECT_TIMEOUT")
            }
            index += 2

        case "--sweep-mode":
            let value = try requireValue(for: arg, args: args, index: index).lowercased()
            switch value {
            case "smart":
                plan.sweepMode = .optimized()
            case "exhaustive":
                plan.sweepMode = .bruteForce
            default:
                throw CLIArgumentError.invalidValue(
                    option: arg,
                    value: value,
                    reason: "expected one of: smart, exhaustive"
                )
            }
            index += 2

        case "--scheduler-policy":
            let value = try requireValue(for: arg, args: args, index: index).lowercased()
            guard let policy = CASchedulerPolicy(rawValue: value) else {
                throw CLIArgumentError.invalidValue(
                    option: arg,
                    value: value,
                    reason: "expected one of: fifo, ca"
                )
            }
            plan.caSchedulerPolicy = policy
            index += 2

        case "--arrival-profile":
            let value = try requireValue(for: arg, args: args, index: index).lowercased()
            guard let profile = CAArrivalProfile(rawValue: value) else {
                throw CLIArgumentError.invalidValue(
                    option: arg,
                    value: value,
                    reason: "expected one of: \(CAArrivalProfile.allCases.map(\.rawValue).joined(separator: ", "))"
                )
            }
            plan.caArrivalProfile = profile
            index += 2

        case "--ca-model-mode":
            let value = try requireValue(for: arg, args: args, index: index).lowercased()
            guard let parsedMode = ThunderboltCAModelMode(rawValue: value) else {
                throw CLIArgumentError.invalidValue(
                    option: arg,
                    value: value,
                    reason: "expected one of: \(ThunderboltCAModelMode.allCases.map(\.rawValue).joined(separator: ", "))"
                )
            }
            plan.thunderboltCAModelMode = parsedMode
            caModelModeWasExplicit = true
            index += 2

        case "--refresh-prior-before-showdown":
            if plan.thunderboltPriorUpdatePolicy == .off {
                plan.thunderboltPriorUpdatePolicy = .candidateOnly
            }
            index += 1

        case "--promote-prior":
            plan.thunderboltPriorUpdatePolicy = .promoteGuarded
            index += 1

        case "--force-promote-prior", "--force-promote":
            plan.thunderboltPriorUpdatePolicy = .promoteForce
            index += 1

        case "--ca-raw-out":
            plan.caRawOutputPath = try requireValue(for: arg, args: args, index: index)
            index += 2

        case "--ca-summary-out":
            plan.caSummaryOutputPath = try requireValue(for: arg, args: args, index: index)
            index += 2

        case "--ca-acceptance":
            plan.runCAAcceptance = true
            index += 1

        case "--ca-acceptance-out":
            plan.caAcceptanceOutputPath = try requireValue(for: arg, args: args, index: index)
            index += 2

        case "--json":
            plan.jsonMode = true
            index += 1

        case "--ssd-path":
            plan.ssdPath = try requireValue(for: arg, args: args, index: index)
            index += 2

        case "--keep-ssd-bench":
            plan.keepSSDBenchArtifacts = true
            index += 1

        case "--report-dir":
            plan.reportDirectory = try requireValue(for: arg, args: args, index: index)
            index += 2

        case "--limit":
            plan.runLimitFinder = true
            index += 1

        case "--limit-workload":
            let value = try requireValue(for: arg, args: args, index: index).lowercased()
            guard let workload = LimitWorkload(rawValue: value) else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected one of: image, video, mixed")
            }
            plan.limitConfig.workload = workload
            index += 2

        case "--limit-start":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            plan.limitConfig.startLoad = parsed
            index += 2

        case "--limit-step":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            plan.limitConfig.stepLoad = parsed
            index += 2

        case "--limit-max":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            plan.limitConfig.maxLoad = parsed
            index += 2

        case "--limit-image-jobs":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            plan.limitConfig.imageJobs = parsed
            index += 2

        case "--limit-video-jobs":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            plan.limitConfig.videoJobs = parsed
            index += 2

        case "--limit-error-threshold":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Double(value), parsed >= 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a number >= 0")
            }
            plan.limitConfig.errorRateThreshold = parsed
            index += 2

        case "--limit-timeout-threshold":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Double(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a number > 0")
            }
            plan.limitConfig.timeoutThresholdSeconds = parsed
            index += 2

        case "--limit-memory-cap":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed > 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected a positive integer")
            }
            plan.limitConfig.memoryCapMB = parsed
            index += 2

        case "--limit-thermal-threshold":
            let value = try requireValue(for: arg, args: args, index: index).lowercased()
            let valid = ["nominal", "fair", "serious", "critical"]
            guard valid.contains(value) else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected one of: \(valid.joined(separator: ", "))")
            }
            plan.limitConfig.thermalThreshold = value
            index += 2

        case "--limit-refine-steps":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed >= 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected an integer >= 0")
            }
            plan.limitConfig.refinementSteps = parsed
            index += 2

        case "--limit-soak-seconds":
            let value = try requireValue(for: arg, args: args, index: index)
            guard let parsed = Int(value), parsed >= 0 else {
                throw CLIArgumentError.invalidValue(option: arg, value: value, reason: "expected an integer >= 0")
            }
            plan.limitConfig.soakSeconds = parsed
            index += 2

        case "--limit-json-out":
            plan.limitConfig.jsonOutputPath = try requireValue(for: arg, args: args, index: index)
            index += 2

        default:
            if arg.hasPrefix("-") {
                throw CLIArgumentError.unknownOption(arg)
            }
            if mediaFolder == nil {
                // Positional <media-folder>
                mediaFolder = arg
                index += 1
            } else {
                throw CLIArgumentError.invalidValue(option: "arguments", value: arg, reason: "unexpected extra positional argument")
            }
        }
    }

    if let stage {
        plan.components = [stage]
    }
    let directCACommand =
        plan.caArrivalProfile != nil
        || plan.caSchedulerPolicy != nil
        || plan.caRawOutputPath != nil
        || plan.caSummaryOutputPath != nil
        || plan.runCAAcceptance
        || plan.caAcceptanceOutputPath != nil
    if !caModelModeWasExplicit {
        plan.thunderboltCAModelMode = directCACommand ? .strict : .auto
    }
    if plan.caArrivalProfile != nil && plan.caSchedulerPolicy == nil {
        plan.caSchedulerPolicy = .fifo
    }
    plan.mediaFolder = mediaFolder

    // Validate minimal requirements for CLI runs.
    if plan.runLimitFinder {
        guard let folder = plan.mediaFolder, !folder.isEmpty else {
            throw CLIArgumentError.missingValue(option: "<media-folder>")
        }
        return plan
    }

    if plan.requiresMediaFolder {
        guard let folder = plan.mediaFolder, !folder.isEmpty else {
            throw CLIArgumentError.missingValue(option: "<media-folder>")
        }
    }
    if plan.components.count == 1, plan.components.first == .archive {
        guard let ssd = plan.ssdPath, !ssd.isEmpty else {
            throw CLIArgumentError.missingValue(option: "--ssd-path")
        }
    }
    let caModeEnabled =
        plan.caArrivalProfile != nil
        || plan.caSchedulerPolicy != nil
        || plan.caRawOutputPath != nil
        || plan.caSummaryOutputPath != nil
        || plan.runCAAcceptance
        || plan.caAcceptanceOutputPath != nil
    if caModeEnabled {
        guard plan.components.count == 1, plan.components.first == .thunderbolt else {
            throw CLIArgumentError.invalidValue(
                option: "--arrival-profile/--scheduler-policy",
                value: "set",
                reason: "CA mode is only valid with --stage thunderbolt"
            )
        }
        if plan.runCAAcceptance {
            if plan.jsonMode {
                throw CLIArgumentError.invalidValue(
                    option: "--ca-acceptance",
                    value: "set",
                    reason: "cannot combine with --json"
                )
            }
            if plan.caArrivalProfile != nil
                || plan.caRawOutputPath != nil
                || plan.caSummaryOutputPath != nil {
                throw CLIArgumentError.invalidValue(
                    option: "--ca-acceptance",
                    value: "set",
                    reason: "cannot combine with --arrival-profile, --ca-raw-out, or --ca-summary-out"
                )
            }
            if plan.caSchedulerPolicy != nil {
                throw CLIArgumentError.invalidValue(
                    option: "--ca-acceptance",
                    value: "set",
                    reason: "policy is fixed to fifo and CA pair runs"
                )
            }
        } else {
            guard plan.caArrivalProfile != nil else {
                throw CLIArgumentError.missingValue(option: "--arrival-profile")
            }
        }
    }

    if caModelModeWasExplicit,
       !plan.components.contains(.thunderbolt) {
        throw CLIArgumentError.invalidValue(
            option: "--ca-model-mode",
            value: plan.thunderboltCAModelMode.rawValue,
            reason: "requires thunderbolt stage in the run plan"
        )
    }
    if plan.thunderboltPriorUpdatePolicy != .off {
        guard plan.components.contains(.thunderbolt) else {
            throw CLIArgumentError.invalidValue(
                option: "--refresh-prior-before-showdown/--promote-prior/--force-promote-prior",
                value: "set",
                reason: "requires thunderbolt stage in the run plan"
            )
        }
        if plan.thunderboltRunType == .burstSweep {
            throw CLIArgumentError.invalidValue(
                option: "--refresh-prior-before-showdown/--promote-prior/--force-promote-prior",
                value: "set",
                reason: "showdown prior maintenance is only available for thunderbolt showdown/full runs"
            )
        }
        if caModeEnabled {
            throw CLIArgumentError.invalidValue(
                option: "--refresh-prior-before-showdown/--promote-prior/--force-promote-prior",
                value: "set",
                reason: "showdown prior maintenance is not used with direct CA commands"
            )
        }
    }

    return plan
}

private func requireValue(for option: String, args: [String], index: Int) throws -> String {
    let valueIndex = index + 1
    guard valueIndex < args.count else {
        throw CLIArgumentError.missingValue(option: option)
    }
    return args[valueIndex]
}

private func printCLIErrorAndExit(_ error: CLIArgumentError) -> Never {
    fputs("\n", stderr)
    fputs("  \(ConsoleUI.bold)kiko-media benchmark runner\(ConsoleUI.reset)\n", stderr)
    fputs("  \(ConsoleUI.red)✗\(ConsoleUI.reset) \(error.description)\n", stderr)
    fputs("\n", stderr)
    fputs("  \(ConsoleUI.dim)Help: \(benchmarkCommand) --help\(ConsoleUI.reset)\n", stderr)
    fputs("  \(ConsoleUI.dim)Advanced: \(benchmarkCommand) --help-advanced\(ConsoleUI.reset)\n", stderr)
    fputs("\n", stderr)
    Foundation.exit(2)
}

// MARK: - Help

private func printBenchmarkList() {
    print()
    ConsoleUI.printSubsectionTitle("kiko-media benchmark runner")
    ConsoleUI.printHint("Benchmark stage catalog.")
    print()
    ConsoleUI.printSubsectionTitle("Usage")
    ConsoleUI.printBody("  \(benchmarkReleaseCommand) --stage <name> <media-folder>")
    print()
    let stageItems = BenchmarkCatalog.components
        .filter { $0.id != .thunderbolt }
        .map { ($0.id.rawValue, $0.detail) }
    let workerItems = BenchmarkCatalog.components
        .filter { $0.id == .thunderbolt }
        .map { ($0.id.rawValue, "(requires external Macs) \($0.detail)") }

    printBenchmarkHelpGroup("Stages", stageItems)
    if !workerItems.isEmpty {
        printBenchmarkHelpGroup("Workers", workerItems)
    }
}

private func printBenchmarkHelpGroup(_ title: String, _ items: [(String, String)]) {
    ConsoleUI.printSubsectionTitle(title)
    let padWidth = 18
    for (flag, desc) in items {
        let paddedFlag: String
        if flag.count >= padWidth {
            paddedFlag = "\(flag)  "
        } else {
            paddedFlag = flag.padding(toLength: padWidth, withPad: " ", startingAt: 0)
        }
        print("    \(paddedFlag)\(ConsoleUI.dim)\(desc)\(ConsoleUI.reset)")
    }
    print()
}

private func printHelp() {
    print()
    ConsoleUI.printSubsectionTitle("kiko-media benchmark runner")
    ConsoleUI.printHint("Measure processing performance on your hardware.")
    print()
    ConsoleUI.printSubsectionTitle("Usage")
    ConsoleUI.printBody("  \(benchmarkReleaseCommand)")
    ConsoleUI.printHint("  Pass <media-folder> to skip the wizard and run the full suite.")
    print()
    printBenchmarkHelpGroup("Modes", [
        ("--wizard",                    "Start interactive mode"),
        ("--stage <name> <media-folder>", "Run a single stage"),
        ("--limit <media-folder>",        "Find optimal concurrency"),
    ])
    printBenchmarkHelpGroup("Reference", [
        ("--list",                        "Stage catalog"),
        ("--help-advanced",               "Full option reference"),
    ])
    ConsoleUI.printSubsectionTitle("Notes")
    ConsoleUI.printHint("  Use -c release for accurate performance numbers.")
    ConsoleUI.printHint("  Archive stage requires --ssd-path.")
    ConsoleUI.printHint("  Thunderbolt stage requires TB_WORKERS.")
    print()
}

private func printAdvancedUsage() {
    print()
    ConsoleUI.printSubsectionTitle("kiko-media benchmark runner")
    ConsoleUI.printHint("Full option reference. For usage patterns run: \(benchmarkCommand) --help")
    print()
    printBenchmarkHelpGroup("Options", [
        ("--wizard",                        "Start interactive mode"),
        ("--list",                          "List benchmark stages"),
        ("--stage <name>",                  "Run a single stage (see --list for names)"),
        ("--component <name>",              "Alias for --stage"),
        ("--pipeline",                      "Alias for --stage pipeline"),
        ("--media-folder <path>",           "Same as positional <media-folder>"),
        ("--ssd-path <folder>",             "External SSD path for archive stage"),
        ("--keep-ssd-bench",                "Keep archive bench artifacts (default: delete)"),
        ("--video-preset <name>",           "Transcode preset (default: 1920x1080)"),
        ("--video-timeout <sec>",           "Per-transcode timeout in seconds (default: \(defaultVideoTimeoutSeconds))"),
        ("--report-dir <dir>",              "Report output directory (default: bench-results/)"),
        ("--json",                          "JSON output to stdout (--stage thunderbolt or pipeline)"),
    ])
    printBenchmarkHelpGroup("Thunderbolt", [
        ("--tb-workers <spec>",             "Workers as host:slots[,host:slots]"),
        ("--tb-port <port>",                "Worker port (default: \(defaultThunderboltPort))"),
        ("--tb-connect-timeout <ms>",       "Connection timeout in milliseconds (default: \(defaultThunderboltConnectTimeoutMS))"),
        ("--sweep-mode <mode>",             "Burst sweep mode: exhaustive or smart (default: smart)"),
        ("--scheduler-policy <policy>",     "CA scheduler policy: fifo or ca"),
        ("--arrival-profile <profile>",     "CA arrivals: all-at-once, burst-1-20-5-5-1, trickle"),
        ("--ca-model-mode <mode>",          "CA model mode: strict or auto (showdown default: auto)"),
        ("--refresh-prior-before-showdown", "Build prior candidate before showdown when local gap exists"),
        ("--promote-prior",                 "Promote prior candidate to canonical when comparator passes"),
        ("--force-promote-prior",           "Allow promote when no-regression passes but improvement is absent"),
        ("--force-promote",                 "Alias for --force-promote-prior"),
        ("--ca-raw-out <path>",       "Write CA raw JSON artifact"),
        ("--ca-summary-out <path>",   "Write CA summary markdown artifact"),
        ("--ca-acceptance",           "Run required FIFO vs CA acceptance matrix"),
        ("--ca-acceptance-out <path>","Write acceptance JSON report"),
    ])
    printBenchmarkHelpGroup("Limit Finder", [
        ("--limit",                         "Run limit finder (skips other stages)"),
        ("--limit-workload <name>",         "image, video, or mixed (default: mixed)"),
        ("--limit-start <n>",               "Starting load (default: 1)"),
        ("--limit-step <n>",                "Ramp step (default: 1)"),
        ("--limit-max <n>",                 "Max load (default: auto)"),
        ("--limit-image-jobs <n>",          "Override image jobs per step"),
        ("--limit-video-jobs <n>",          "Override video jobs per step"),
        ("--limit-error-threshold <n>",     "Failure-rate stop threshold (default: 0.05)"),
        ("--limit-timeout-threshold <sec>", "P95 latency stop threshold (default: 30)"),
        ("--limit-memory-cap <mb>",         "Peak memory stop threshold (default: 4096)"),
        ("--limit-thermal-threshold <name>","nominal, fair, serious, critical (default: serious)"),
        ("--limit-refine-steps <n>",        "Refinement iterations (default: 4)"),
        ("--limit-soak-seconds <sec>",      "Soak at knee and knee+1 (default: 0)"),
        ("--limit-json-out <path>",         "JSON output file path"),
    ])
    ConsoleUI.printSubsectionTitle("Aliases")
    ConsoleUI.printHint("  \(benchmarkAliasCommand)")
    ConsoleUI.printHint("  \(benchmarkReleaseAliasCommand)")
    print()
}
