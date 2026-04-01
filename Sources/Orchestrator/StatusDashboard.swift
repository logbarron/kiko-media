import Foundation
import Darwin
import KikoMediaCore

struct StatusDashboardLayout {
    static let boxWidth = 44
    static let innerWidth = boxWidth - 6
    static let sectionDividerWidth = boxWidth - 1
    static let dotLeaderWidth = 18

    static let galleryLabelWidth = 11
    static let galleryQueuedWidth = 6
    static let galleryProcessingWidth = 10
    static let galleryTotalWidth = 5
}

private func statusDashboardPad(_ text: String, width: Int) -> String {
    let padding = max(0, width - text.count)
    return String(repeating: " ", count: padding) + text
}

private func statusDashboardMetricCell(_ value: Int?, width: Int) -> String {
    guard let value else { return statusDashboardPad("·", width: width) }
    if value == 0 {
        return statusDashboardPad("·", width: width)
    }
    return statusDashboardPad(String(value), width: width)
}

func statusDashboardGalleryHeaderRow() -> String {
    let label = String(repeating: " ", count: StatusDashboardLayout.galleryLabelWidth)
    let queued = statusDashboardPad("queued", width: StatusDashboardLayout.galleryQueuedWidth)
    let processing = statusDashboardPad("processing", width: StatusDashboardLayout.galleryProcessingWidth)
    let total = statusDashboardPad("total", width: StatusDashboardLayout.galleryTotalWidth)
    return "  \(label)   \(queued)   \(processing)   \(total)"
}

func statusDashboardGalleryRow(label: String, queued: Int?, processing: Int?, total: Int?) -> String {
    let lbl = label.padding(toLength: StatusDashboardLayout.galleryLabelWidth, withPad: " ", startingAt: 0)
    let q = statusDashboardMetricCell(queued, width: StatusDashboardLayout.galleryQueuedWidth)
    let p = statusDashboardMetricCell(processing, width: StatusDashboardLayout.galleryProcessingWidth)
    let t = statusDashboardMetricCell(total, width: StatusDashboardLayout.galleryTotalWidth)
    return "  \(lbl)   \(q)   \(p)   \(t)"
}

final class StatusDashboard: @unchecked Sendable {
    private let lock = DispatchQueue(label: "status-dashboard")
    private let home: String
    private let repoRoot: String

    private var refreshTimer: DispatchSourceTimer?
    private var collectionTimer: DispatchSourceTimer?

    private var configState: ConfigurationState = .missing
    private var serviceLoaded: [(name: String, loaded: Bool)] = []
    private var sleepEnabled: Bool = false
    private var sleepPID: Int? = nil
    private var ssdPath: String? = nil
    private var ssdMounted: Bool = false
    private var workers: [(name: String, reachable: Bool)] = []
    private var counts: Database.DashboardCounts? = nil
    private var publicPort: Int = 3001
    private var internalPort: Int = 3002
    private var anyServiceRunning: Bool { serviceLoaded.contains { $0.loaded } }

    private var database: Database?
    private var tbPort: UInt16 = 12400
    private var tbTimeout: Int = thunderboltConnectTimeoutMS
    private var workerHosts: [(host: String, name: String)] = []
    private var schedulerSummary = OperatorVideoRuntimeSummary(
        policy: .auto,
        runtime: .fifoLocalOnly,
        reason: .noWorkers
    )

    init(home: String, repoRoot: String) {
        self.home = home
        self.repoRoot = repoRoot
    }

    func start() {
        resolveConfig()
        collectData()

        if isColorEnabled {
            paintFrame(renderFrame())
        } else {
            print(renderFramePlain())
            return
        }

        let refresh = DispatchSource.makeTimerSource(queue: .global(qos: .utility))
        refresh.schedule(deadline: .now() + 0.25, repeating: 0.25, leeway: .milliseconds(50))
        refresh.setEventHandler { [weak self] in
            guard let self else { return }
            self.lock.sync {
                self.paintFrame(self.renderFrame())
            }
        }
        refresh.resume()
        refreshTimer = refresh

        let collection = DispatchSource.makeTimerSource(queue: .global(qos: .utility))
        collection.schedule(deadline: .now() + 2.0, repeating: 2.0, leeway: .milliseconds(200))
        collection.setEventHandler { [weak self] in
            guard let self else { return }
            self.collectData()
            self.lock.sync {
                self.paintFrame(self.renderFrame())
            }
        }
        collection.resume()
        collectionTimer = collection
    }

    func stop() {
        refreshTimer?.cancel()
        collectionTimer?.cancel()
        refreshTimer = nil
        collectionTimer = nil
    }

    // MARK: - Config Resolution

    private func resolveConfig() {
        let processEnv = ProcessInfo.processInfo.environment
        let persistedEnv = loadMediaLaunchAgentEnvironment(home: home)

        func resolve(_ key: String, fallback: String) -> String {
            if let v = processEnv[key]?.trimmingCharacters(in: .whitespacesAndNewlines), !v.isEmpty { return v }
            if let v = persistedEnv[key]?.trimmingCharacters(in: .whitespacesAndNewlines), !v.isEmpty { return v }
            return fallback
        }

        func resolveInt(_ key: String, fallback: Int) -> Int {
            Int(resolve(key, fallback: "\(fallback)")) ?? fallback
        }

        publicPort = resolveInt("PUBLIC_PORT", fallback: Config.intDefaults["PUBLIC_PORT"]?.fallback ?? 3001)
        internalPort = resolveInt("INTERNAL_PORT", fallback: Config.intDefaults["INTERNAL_PORT"]?.fallback ?? 3002)

        let ssdRaw = resolve("EXTERNAL_SSD_PATH", fallback: "")
        ssdPath = ssdRaw.isEmpty ? nil : ssdRaw

        tbPort = UInt16(resolveInt("TB_PORT", fallback: Config.intDefaults["TB_PORT"]?.fallback ?? 12400))
        tbTimeout = thunderboltConnectTimeoutMS
        if let envTimeoutRaw = processEnv["TB_CONNECT_TIMEOUT"]?.trimmingCharacters(in: .whitespacesAndNewlines),
           !envTimeoutRaw.isEmpty {
            if let envTimeout = parseThunderboltStatusConnectTimeout(envTimeoutRaw) {
                tbTimeout = envTimeout
            } else {
                printWarning("Ignoring invalid TB_CONNECT_TIMEOUT from environment: \(envTimeoutRaw)")
            }
        }
        if tbTimeout == thunderboltConnectTimeoutMS,
           let persistedTimeoutRaw = persistedEnv["TB_CONNECT_TIMEOUT"]?.trimmingCharacters(in: .whitespacesAndNewlines),
           !persistedTimeoutRaw.isEmpty {
            if let persistedTimeout = parseThunderboltStatusConnectTimeout(persistedTimeoutRaw) {
                tbTimeout = persistedTimeout
            } else {
                printWarning("Ignoring invalid TB_CONNECT_TIMEOUT from com.kiko.media.plist: \(persistedTimeoutRaw)")
            }
        }

        let workersResolution = resolveThunderboltWorkersRaw(processEnv: processEnv, persistedEnv: persistedEnv)
        if let raw = workersResolution.raw {
            let parsed = parseThunderboltStatusWorkers(raw)
            workerHosts = parsed.workers.enumerated().map { (i, w) in
                (host: w.host, name: "W\(i + 1)")
            }
            workers = workerHosts.map { (name: $0.name, reachable: false) }
            schedulerSummary = resolveOperatorVideoRuntimeSummary(
                processEnv: processEnv,
                persistedEnv: persistedEnv,
                workers: parsed.workers,
                port: Int(tbPort),
                connectTimeoutMS: tbTimeout
            ).summary
        } else {
            schedulerSummary = resolveOperatorVideoRuntimeSummary(
                processEnv: processEnv,
                persistedEnv: persistedEnv,
                workers: [],
                port: Int(tbPort),
                connectTimeoutMS: tbTimeout
            ).summary
        }

        let baseDirRaw = resolve("BASE_DIRECTORY", fallback: "~/Documents/kiko-media")
        let baseDir = NSString(string: baseDirRaw).expandingTildeInPath
        let dbPath = "\(baseDir)/metadata.db"

        guard FileManager.default.fileExists(atPath: dbPath) else { return }
        database = try? Database(
            path: dbPath,
            busyTimeout: 5000,
            cacheSize: -2000,
            defaultPageSize: 100,
            maxPageSize: 500,
            maxPageOffset: 10000,
            sqlBatchSize: 500
        )
    }

    // MARK: - Data Collection

    private func collectData() {
        let configResult = detectConfigurationState(home: home, repoRoot: repoRoot)

        let services: [(name: String, loaded: Bool)] = [
            ("caddy", isLaunchAgentLoaded("com.kiko.caddy")),
            ("tusd", isLaunchAgentLoaded("com.kiko.tusd")),
            ("kiko-media", isLaunchAgentLoaded("com.kiko.media")),
        ]

        let sleepOn = isSleepPreventionEnabled()
        let pid = sleepOn ? caffeinatePID() : nil

        let mounted: Bool
        if let path = ssdPath {
            mounted = VolumeUtils.isMounted(volumeContainingPath: path)
        } else {
            mounted = false
        }

        var workerResults = workers
        if !workerHosts.isEmpty {
            let bridges = detectBridgeInterfaces().filter { $0.ipv4 != nil }
            workerResults = workerHosts.map { wh in
                var reachable = false
                if let bridge = selectBridge(for: wh.host, from: bridges),
                   let sourceIP = bridge.ipv4 {
                    if let socket = connectSocket(
                        host: wh.host,
                        port: tbPort,
                        sourceIP: sourceIP,
                        timeoutMS: tbTimeout
                    ) {
                        Darwin.close(socket)
                        reachable = true
                    }
                }
                return (name: wh.name, reachable: reachable)
            }
        }

        let dashCounts = database.flatMap { try? $0.getDashboardCountsSync() }

        lock.sync {
            self.configState = configResult
            self.serviceLoaded = services
            self.sleepEnabled = sleepOn
            self.sleepPID = pid
            self.ssdMounted = mounted
            self.workers = workerResults
            self.counts = dashCounts
        }
    }

    private func caffeinatePID() -> Int? {
        let (exitCode, output) = runProcess(
            executable: "/bin/ps",
            arguments: ["-eo", "pid,comm"]
        )
        guard exitCode == 0 else { return nil }
        for line in output.split(separator: "\n") {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasSuffix("/caffeinate") || trimmed.hasSuffix(" caffeinate") {
                let parts = trimmed.split(separator: " ", maxSplits: 1)
                if let pidStr = parts.first, let pid = Int(pidStr) {
                    return pid
                }
            }
        }
        return nil
    }

    // MARK: - Rendering

    private func dotLeader(label: String, value: String, width: Int = StatusDashboardLayout.dotLeaderWidth) -> String {
        let dotsNeeded = max(2, width - label.count)
        let dots = "\(dim)\(String(repeating: "·", count: dotsNeeded))\(reset)"
        return "  \(label) \(dots)  \(value)"
    }

    private func sectionDivider(title: String? = nil, width: Int = StatusDashboardLayout.sectionDividerWidth) -> String {
        guard let title else {
            return "  \(dim)\(String(repeating: "─", count: width))\(reset)"
        }
        let prefix = "── "
        let suffix = " "
        let remaining = max(0, width - prefix.count - title.count - suffix.count)
        return "  \(dim)\(prefix)\(reset)\(bold)\(title)\(reset)\(dim)\(suffix)\(String(repeating: "─", count: remaining))\(reset)"
    }

    private func renderFrame() -> String {
        var lines: [String] = []
        lines.append("")

        // Title box
        lines.append("  \(dim)┌\(String(repeating: "─", count: StatusDashboardLayout.boxWidth - 2))┐\(reset)")

        let title = "kiko-media"
        let titlePad = String(repeating: " ", count: max(0, StatusDashboardLayout.innerWidth - title.count))
        lines.append("  \(dim)│\(reset)  \(bold)\(title)\(reset)\(titlePad)  \(dim)│\(reset)")

        let subtitle = "Public :\(publicPort) · Internal :\(internalPort)"
        let subtitlePad = String(repeating: " ", count: max(0, StatusDashboardLayout.innerWidth - subtitle.count))
        lines.append("  \(dim)│\(reset)  \(subtitle)\(subtitlePad)  \(dim)│\(reset)")

        lines.append("  \(dim)└\(String(repeating: "─", count: StatusDashboardLayout.boxWidth - 2))┘\(reset)")
        lines.append("")

        lines.append(sectionDivider(title: "System"))
        lines.append("")

        // Config (only when not running)
        if !anyServiceRunning {
            let configValue: String
            switch configState {
            case .complete:
                configValue = "ready"
            case .missing:
                configValue = "\(red)not configured\(reset)"
            case .partial(let found, let total, _):
                configValue = "\(red)partial (\(found)/\(total))\(reset)"
            }
            lines.append(dotLeader(label: "Config", value: configValue))
        }

        // Services
        let servicesValue: String
        if serviceLoaded.allSatisfy({ !$0.loaded }) {
            servicesValue = "·"
        } else {
            servicesValue = serviceLoaded.map { svc in
                svc.loaded ? svc.name : "\(red)\(svc.name)\(reset)"
            }.joined(separator: " \(dim)·\(reset) ")
        }
        lines.append(dotLeader(label: "Services", value: servicesValue))

        // Sleep
        let sleepValue: String
        if !sleepEnabled && !anyServiceRunning {
            if case .missing = configState {
                sleepValue = "·"
            } else {
                sleepValue = "off"
            }
        } else if !sleepEnabled && anyServiceRunning {
            sleepValue = "\(red)off\(reset)"
        } else if let pid = sleepPID {
            sleepValue = "active (pid \(pid))"
        } else {
            sleepValue = "active"
        }
        lines.append(dotLeader(label: "Sleep", value: sleepValue))

        // SSD
        if let path = ssdPath {
            let ssdValue: String
            if !ssdMounted && anyServiceRunning {
                ssdValue = "\(red)\(path)\(reset)"
            } else {
                ssdValue = path
            }
            lines.append(dotLeader(label: "SSD", value: ssdValue))
        } else {
            if case .missing = configState {
                lines.append(dotLeader(label: "SSD", value: "·"))
            }
        }

        lines.append("")

        lines.append(sectionDivider(title: "Thunderbolt"))
        lines.append("")
        lines.append(dotLeader(label: "Policy", value: schedulerSummary.policy.rawValue))
        lines.append(dotLeader(label: "Runtime", value: schedulerSummary.runtime.rawValue))
        let workerValue = if !workers.isEmpty {
            workers.map { w in
                if !w.reachable && anyServiceRunning {
                    return "\(red)\(w.name)\(reset)"
                }
                return w.name
            }.joined(separator: " \(dim)·\(reset) ")
        } else {
            "·"
        }
        lines.append(dotLeader(label: "Workers", value: workerValue))

        lines.append("")

        // Gallery section
        lines.append(sectionDivider(title: "Gallery"))
        lines.append("")

        lines.append("\(dim)\(statusDashboardGalleryHeaderRow())\(reset)")

        if let c = counts {
            lines.append(
                statusDashboardGalleryRow(
                    label: "Images",
                    queued: c.queuedImages,
                    processing: c.processingImages,
                    total: c.completeImages
                )
            )
            lines.append(
                statusDashboardGalleryRow(
                    label: "Videos",
                    queued: c.queuedVideos,
                    processing: c.processingVideos,
                    total: c.completeVideos
                )
            )
            lines.append(statusDashboardGalleryRow(label: "Moderated", queued: nil, processing: nil, total: c.moderated))
            if c.failed > 0 {
                let lbl = "Failed".padding(toLength: StatusDashboardLayout.galleryLabelWidth, withPad: " ", startingAt: 0)
                let q = statusDashboardMetricCell(nil, width: StatusDashboardLayout.galleryQueuedWidth)
                let p = statusDashboardMetricCell(nil, width: StatusDashboardLayout.galleryProcessingWidth)
                let t = statusDashboardPad(String(c.failed), width: StatusDashboardLayout.galleryTotalWidth)
                lines.append("  \(red)\(lbl)\(reset)   \(q)   \(p)   \(red)\(t)\(reset)")
            }
        } else {
            lines.append(statusDashboardGalleryRow(label: "Images", queued: nil, processing: nil, total: nil))
            lines.append(statusDashboardGalleryRow(label: "Videos", queued: nil, processing: nil, total: nil))
            lines.append(statusDashboardGalleryRow(label: "Moderated", queued: nil, processing: nil, total: nil))
        }

        lines.append("")
        lines.append("  \(dim)Ctrl-C to close dashboard.\(reset)")
        return lines.joined(separator: "\n") + "\n"
    }

    private func paintFrame(_ frame: String) {
        guard isColorEnabled else { return }
        print("\u{1b}[H\u{1b}[J", terminator: "")
        print(frame, terminator: "")
        fflush(stdout)
    }

    private func renderFramePlain() -> String {
        var lines: [String] = []
        lines.append("")
        lines.append("  kiko-media")
        lines.append("  Public :\(publicPort) · Internal :\(internalPort)")
        lines.append("")
        lines.append("  System:")

        if !anyServiceRunning {
            switch configState {
            case .complete: lines.append("    Config: ready")
            case .missing: lines.append("    Config: not configured")
            case .partial(let f, let t, _): lines.append("    Config: partial (\(f)/\(t))")
            }
        }

        let svcNames = serviceLoaded.filter(\.loaded).map(\.name)
        lines.append("    Services: \(svcNames.isEmpty ? "·" : svcNames.joined(separator: " · "))")

        if sleepEnabled {
            lines.append("    Sleep: active\(sleepPID.map { " (pid \($0))" } ?? "")")
        } else {
            lines.append("    Sleep: off")
        }

        if let path = ssdPath {
            lines.append("    SSD: \(path)\(ssdMounted ? "" : " (unmounted)")")
        }

        lines.append("")
        lines.append("  Thunderbolt:")
        lines.append("    Policy: \(schedulerSummary.policy.rawValue)")
        lines.append("    Runtime: \(schedulerSummary.runtime.rawValue)")
        if !workers.isEmpty {
            let wNames = workers.map { "\($0.name)\($0.reachable ? "" : " (unreachable)")" }
            lines.append("    Workers: \(wNames.joined(separator: " · "))")
        } else {
            lines.append("    Workers: ·")
        }

        lines.append("")
        if let c = counts {
            lines.append("  Gallery:")
            lines.append(
                "    Images:    queued=\(c.queuedImages) processing=\(c.processingImages) total=\(c.completeImages)"
            )
            lines.append(
                "    Videos:    queued=\(c.queuedVideos) processing=\(c.processingVideos) total=\(c.completeVideos)"
            )
            lines.append("    Moderated: \(c.moderated)")
            if c.failed > 0 {
                lines.append("    Failed:    \(c.failed)")
            }
        } else {
            lines.append("  Gallery: no data")
        }
        lines.append("")
        return lines.joined(separator: "\n")
    }
}
