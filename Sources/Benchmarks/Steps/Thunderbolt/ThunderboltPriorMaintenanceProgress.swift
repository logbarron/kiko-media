import Foundation

enum ThunderboltPriorMaintenanceStage: CaseIterable {
    case localSweep
    case frameCounting
    case affineSampleCollection
    case remoteSamplePreparation
    case remoteTelemetry
    case setupRebuild

    var label: String {
        switch self {
        case .localSweep:
            return "local sweep"
        case .frameCounting:
            return "frame counting"
        case .affineSampleCollection:
            return "affine sample collection"
        case .remoteSamplePreparation:
            return "remote sample prep"
        case .remoteTelemetry:
            return "remote telemetry"
        case .setupRebuild:
            return "setup rebuild"
        }
    }
}

enum ThunderboltPriorMaintenanceStageStatus: String {
    case pending
    case running
    case done
    case skipped
    case failed
}

private struct ThunderboltPriorMaintenanceStageState {
    var status: ThunderboltPriorMaintenanceStageStatus = .pending
    var detail = ""
    var startedAt: ContinuousClock.Instant?
    var completedAt: ContinuousClock.Instant?
    var lastProgressAt: ContinuousClock.Instant?
}

actor ThunderboltPriorMaintenanceProgressReporter {
    private let clock = ContinuousClock()
    private let transientWriter: @Sendable (String) -> Void
    private let transientEnabled: Bool
    private var renderedLineCount = 0
    private var refreshTask: Task<Void, Never>?
    private var activated = false
    private var states: [ThunderboltPriorMaintenanceStage: ThunderboltPriorMaintenanceStageState] =
        Dictionary(uniqueKeysWithValues: ThunderboltPriorMaintenanceStage.allCases.map { ($0, ThunderboltPriorMaintenanceStageState()) })

    init(
        transientWriter: @escaping @Sendable (String) -> Void = BenchOutput.writeTerminalOnly,
        transientEnabled: Bool = BenchOutput.supportsTerminalOnlyWrites()
    ) {
        self.transientWriter = transientWriter
        self.transientEnabled = transientEnabled
    }

    func startStage(_ stage: ThunderboltPriorMaintenanceStage, detail: String) {
        activated = true
        var state = states[stage] ?? ThunderboltPriorMaintenanceStageState()
        let now = clock.now
        if state.startedAt == nil {
            state.startedAt = now
        }
        state.status = .running
        state.detail = detail
        state.lastProgressAt = now
        state.completedAt = nil
        states[stage] = state
        ensureRefreshTask()
        renderTransient()
    }

    func updateStage(_ stage: ThunderboltPriorMaintenanceStage, detail: String) {
        activated = true
        var state = states[stage] ?? ThunderboltPriorMaintenanceStageState()
        switch state.status {
        case .done, .skipped, .failed:
            return
        case .pending, .running:
            break
        }
        let now = clock.now
        if state.startedAt == nil {
            state.startedAt = now
        }
        state.status = .running
        state.detail = detail
        state.lastProgressAt = now
        state.completedAt = nil
        states[stage] = state
        ensureRefreshTask()
        renderTransient()
    }

    func completeStage(_ stage: ThunderboltPriorMaintenanceStage, detail: String) {
        activated = true
        var state = states[stage] ?? ThunderboltPriorMaintenanceStageState()
        let now = clock.now
        if state.startedAt == nil {
            state.startedAt = now
        }
        state.status = .done
        state.detail = detail
        state.lastProgressAt = now
        state.completedAt = now
        states[stage] = state
        renderTransient()
    }

    func skipStage(_ stage: ThunderboltPriorMaintenanceStage, detail: String) {
        activated = true
        var state = states[stage] ?? ThunderboltPriorMaintenanceStageState()
        let now = clock.now
        state.status = .skipped
        state.detail = detail
        state.lastProgressAt = now
        state.completedAt = now
        states[stage] = state
        renderTransient()
    }

    func failStage(_ stage: ThunderboltPriorMaintenanceStage, detail: String) {
        activated = true
        var state = states[stage] ?? ThunderboltPriorMaintenanceStageState()
        let now = clock.now
        if state.startedAt == nil {
            state.startedAt = now
        }
        state.status = .failed
        state.detail = detail
        state.lastProgressAt = now
        state.completedAt = now
        states[stage] = state
        renderTransient()
    }

    func finish(
        terminalFinalize: @escaping @Sendable (String) -> Void = BenchOutput.write
    ) -> [String] {
        guard activated else { return [] }
        refreshTask?.cancel()
        refreshTask = nil
        let now = clock.now
        let lines = renderLines(now: now, includeLastProgress: true)
        if transientEnabled {
            render(lines: lines, using: terminalFinalize)
        } else {
            renderFinal(lines: lines, using: terminalFinalize)
        }
        renderedLineCount = 0
        return lines
    }

    private func ensureRefreshTask() {
        guard transientEnabled else { return }
        guard refreshTask == nil else { return }
        refreshTask = Task { [weak self] in
            while let self, !Task.isCancelled {
                try? await Task.sleep(for: .seconds(1))
                await self.refreshIfRunning()
            }
        }
    }

    private func refreshIfRunning() {
        guard states.values.contains(where: { $0.status == .running }) else {
            refreshTask?.cancel()
            refreshTask = nil
            return
        }
        renderTransient()
    }

    private func renderTransient() {
        guard transientEnabled else { return }
        render(lines: renderLines(now: clock.now, includeLastProgress: true), using: transientWriter)
    }

    private func render(
        lines: [String],
        using writer: @escaping @Sendable (String) -> Void
    ) {
        let lineCount = max(renderedLineCount, lines.count)
        if renderedLineCount > 0 {
            writer("\u{1B}[\(renderedLineCount)F")
        }
        for index in 0..<lineCount {
            writer("\r\u{1B}[2K")
            if index < lines.count {
                writer(lines[index])
            }
            writer("\n")
        }
        renderedLineCount = lines.count
    }

    private func renderFinal(
        lines: [String],
        using writer: @escaping @Sendable (String) -> Void
    ) {
        for line in lines {
            writer(line)
            writer("\n")
        }
    }

    private func renderLines(now: ContinuousClock.Instant, includeLastProgress: Bool) -> [String] {
        let stageWidth = 24
        let statusWidth = 8
        let detailWidth = 34
        let elapsedWidth = 8
        let lastProgressWidth = 13

        let header = [
            padded("  stage", width: stageWidth + 2),
            padded("status", width: statusWidth),
            padded("detail", width: detailWidth),
            padded("elapsed", width: elapsedWidth, alignment: .right),
            padded("last progress", width: lastProgressWidth, alignment: .right),
        ].joined(separator: "  ")

        var lines: [String] = [
            header,
            "  " + String(repeating: "-", count: max(0, header.count - 2)),
        ]

        for stage in ThunderboltPriorMaintenanceStage.allCases {
            let state = states[stage] ?? ThunderboltPriorMaintenanceStageState()
            let elapsed = formatElapsed(state: state, now: now)
            let lastProgress = includeLastProgress ? formatLastProgress(state: state, now: now) : ""
            let detail = clipped(state.detail, width: detailWidth)
            lines.append(
                [
                    padded("  " + stage.label, width: stageWidth + 2),
                    padded(state.status.rawValue, width: statusWidth),
                    padded(detail, width: detailWidth),
                    padded(elapsed, width: elapsedWidth, alignment: .right),
                    padded(lastProgress, width: lastProgressWidth, alignment: .right),
                ].joined(separator: "  ")
            )
        }

        return lines
    }

    private func formatElapsed(
        state: ThunderboltPriorMaintenanceStageState,
        now: ContinuousClock.Instant
    ) -> String {
        guard let startedAt = state.startedAt else { return "" }
        let end = state.completedAt ?? now
        return formatDuration(max(0, (end - startedAt).seconds))
    }

    private func formatLastProgress(
        state: ThunderboltPriorMaintenanceStageState,
        now: ContinuousClock.Instant
    ) -> String {
        guard state.status == .running,
              let lastProgressAt = state.lastProgressAt else {
            return ""
        }
        return formatDuration(max(0, (now - lastProgressAt).seconds))
    }

    private func formatDuration(_ seconds: Double) -> String {
        let totalSeconds = max(0, Int(seconds.rounded(.down)))
        let hours = totalSeconds / 3_600
        let minutes = (totalSeconds % 3_600) / 60
        let remainingSeconds = totalSeconds % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, remainingSeconds)
        }
        return String(format: "%02d:%02d", minutes, remainingSeconds)
    }

    private func clipped(_ value: String, width: Int) -> String {
        guard value.count > width, width > 3 else { return value }
        let prefix = value.prefix(max(0, width - 3))
        return "\(prefix)..."
    }

    private func padded(
        _ value: String,
        width: Int,
        alignment: BenchmarkRuntimeTableAlignment = .left
    ) -> String {
        let padCount = max(0, width - value.count)
        let pad = String(repeating: " ", count: padCount)
        switch alignment {
        case .left:
            return value + pad
        case .right:
            return pad + value
        }
    }
}
