import Foundation
import Dispatch
import Darwin
import Security
import KikoMediaCore

// MARK: - Thunderbolt Setup Command

struct ThunderboltBridgeInterface {
    let name: String
    let ipv4: String?
    let netmask: UInt32?
    let mtu: Int?
}

struct ThunderboltWorkerTemplateSettings {
    let port: UInt16
    let connectTimeout: Int
    let transcodePreset: String
    let thumbSize: Int
    let thumbTime: String
    let thumbQuality: String
    let transcodeTimeout: Int
    let workDirPrefix: String
}

func thunderboltWorkerTemplateReplacements(settings: ThunderboltWorkerTemplateSettings) -> [String: String] {
    [
        "__TB_PORT__": String(settings.port),
        "__WORK_DIR__": settings.workDirPrefix,
        "__VIDEO_TRANSCODE_PRESET__": settings.transcodePreset,
        "__VIDEO_THUMBNAIL_SIZE__": String(settings.thumbSize),
        "__VIDEO_THUMBNAIL_TIME__": settings.thumbTime,
        "__VIDEO_THUMBNAIL_QUALITY__": settings.thumbQuality,
        "__VIDEO_TRANSCODE_TIMEOUT__": String(settings.transcodeTimeout),
    ]
}

let thunderboltConnectTimeoutMS = 500

private let thunderboltConnectTimeoutLegacySecondsRange = 1 ... 30
private let thunderboltConnectTimeoutMSRange = Config.intDefaults["TB_CONNECT_TIMEOUT"]!.range!

func thunderboltWorkerLabel(index: Int) -> String {
    "W\(index + 1)"
}

private func parseThunderboltConnectTimeoutMSValue(_ raw: String) -> Int? {
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty, let value = Int(trimmed) else { return nil }
    if thunderboltConnectTimeoutLegacySecondsRange.contains(value) {
        return value * 1_000
    }
    guard thunderboltConnectTimeoutMSRange.contains(value) else { return nil }
    return value
}

func ipv4Raw(_ ip: String) -> UInt32? {
    var addr = in_addr()
    let rc = ip.withCString { cstr in
        inet_pton(AF_INET, cstr, &addr)
    }
    return rc == 1 ? addr.s_addr : nil
}

func hasThunderboltBridgeService() -> Bool {
    let result = runProcess(executable: "/usr/sbin/networksetup", arguments: ["-listallnetworkservices"])
    guard result.exitCode == 0 else { return false }
    for rawLine in result.output.split(separator: "\n") {
        let line = rawLine.trimmingCharacters(in: .whitespaces)
        if line.hasPrefix("*") {
            let disabled = line.dropFirst().trimmingCharacters(in: .whitespaces)
            if disabled == "Thunderbolt Bridge" { return true }
        } else if line == "Thunderbolt Bridge" {
            return true
        }
    }
    return false
}

func detectBridgeInterfaces() -> [ThunderboltBridgeInterface] {
    var addrs: UnsafeMutablePointer<ifaddrs>?
    guard getifaddrs(&addrs) == 0, let first = addrs else { return [] }
    defer { freeifaddrs(first) }

    var names = Set<String>()
    var ipv4ByName: [String: String] = [:]
    var netmaskByName: [String: UInt32] = [:]

    var cursor: UnsafeMutablePointer<ifaddrs>? = first
    while let current = cursor {
        defer { cursor = current.pointee.ifa_next }
        let name = String(cString: current.pointee.ifa_name)
        guard name.hasPrefix("bridge") else { continue }
        names.insert(name)

        guard ipv4ByName[name] == nil,
              let addr = current.pointee.ifa_addr,
              addr.pointee.sa_family == UInt8(AF_INET) else { continue }
        guard let rawMask = current.pointee.ifa_netmask,
              rawMask.pointee.sa_family == UInt8(AF_INET) else { continue }

        var host = [CChar](repeating: 0, count: Int(NI_MAXHOST))
        let rc = getnameinfo(
            addr,
            socklen_t(addr.pointee.sa_len),
            &host,
            socklen_t(host.count),
            nil,
            0,
            NI_NUMERICHOST
        )
        if rc == 0 {
            ipv4ByName[name] = String(decoding: host.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }, as: UTF8.self)
            let mask = rawMask.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { maskPtr in
                maskPtr.pointee.sin_addr.s_addr
            }
            netmaskByName[name] = mask
        }
    }

    return names.sorted().map { name in
        ThunderboltBridgeInterface(
            name: name,
            ipv4: ipv4ByName[name],
            netmask: netmaskByName[name],
            mtu: detectMTU(interfaceName: name)
        )
    }
}

func detectMTU(interfaceName: String) -> Int? {
    let result = runProcess(executable: "/sbin/ifconfig", arguments: [interfaceName])
    guard result.exitCode == 0,
          let range = result.output.range(of: "mtu ") else { return nil }
    let suffix = result.output[range.upperBound...]
    let digits = suffix.prefix { $0.isNumber }
    return Int(digits)
}

func countConnectedThunderboltDevices(from output: String) -> Int? {
    guard let data = output.data(using: .utf8),
          let root = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let buses = root["SPThunderboltDataType"] as? [[String: Any]] else {
        return nil
    }

    var count = 0
    for bus in buses {
        if let items = bus["_items"] as? [[String: Any]] {
            count += items.count
        }
    }
    return count
}

func resolveThunderboltSetting(_ key: String, defaults: [String: DefaultSpec]) -> String? {
    if let envValueRaw = ProcessInfo.processInfo.environment[key] {
        let envValue = envValueRaw.trimmingCharacters(in: .whitespacesAndNewlines)
        if !envValue.isEmpty {
            if key == "TB_CONNECT_TIMEOUT" {
                if let timeoutMS = parseThunderboltConnectTimeoutMSValue(envValue) {
                    if thunderboltConnectTimeoutLegacySecondsRange.contains(Int(envValue) ?? 0) {
                        printWarning(
                            "Interpreting legacy TB_CONNECT_TIMEOUT=\(envValue)s as \(timeoutMS)ms; update env to explicit milliseconds."
                        )
                    }
                    return String(timeoutMS)
                }
                printWarning("Ignoring invalid \(key) from environment: \(envValue). Using defaults.env value.")
                return defaults[key]?.defaultValue
            }
            if let spec = defaults[key], !validateSpec(envValue, spec: spec) {
                printWarning("Ignoring invalid \(key) from environment: \(envValue). Using defaults.env value.")
            } else {
                return envValue
            }
        }
    }
    return defaults[key]?.defaultValue
}

func loadThunderboltWorkerTemplateSettings(repoRoot: String) -> ThunderboltWorkerTemplateSettings? {
    let defaultsPath = "\(repoRoot)/deploy/defaults.env"
    let defaults = parseDefaults(defaultsPath)
    guard !defaults.isEmpty else {
        printError("Cannot read \(defaultsPath)")
        return nil
    }

    guard let portString = resolveThunderboltSetting("TB_PORT", defaults: defaults),
          let connectTimeoutString = resolveThunderboltSetting("TB_CONNECT_TIMEOUT", defaults: defaults),
          let thumbSizeString = resolveThunderboltSetting("VIDEO_THUMBNAIL_SIZE", defaults: defaults),
          let thumbTime = resolveThunderboltSetting("VIDEO_THUMBNAIL_TIME", defaults: defaults),
          let thumbQuality = resolveThunderboltSetting("VIDEO_THUMBNAIL_QUALITY", defaults: defaults),
          let transcodeTimeoutString = resolveThunderboltSetting("VIDEO_TRANSCODE_TIMEOUT", defaults: defaults),
          let transcodePreset = resolveThunderboltSetting("VIDEO_TRANSCODE_PRESET", defaults: defaults),
          let port = UInt16(portString),
          let connectTimeout = Int(connectTimeoutString),
          let thumbSize = Int(thumbSizeString),
          let transcodeTimeout = Int(transcodeTimeoutString) else {
        printError("Could not resolve Thunderbolt worker template settings from environment/defaults.")
        return nil
    }

    return ThunderboltWorkerTemplateSettings(
        port: port,
        connectTimeout: connectTimeout,
        transcodePreset: transcodePreset,
        thumbSize: thumbSize,
        thumbTime: thumbTime,
        thumbQuality: thumbQuality,
        transcodeTimeout: transcodeTimeout,
        workDirPrefix: "/tmp/kiko-worker"
    )
}

func promptWorkerCount(defaultCount: Int) -> Int {
    let normalizedDefault = min(max(defaultCount, 1), 16)
    while true {
        let raw = prompt("How many worker Macs?", default: String(normalizedDefault))
        guard let count = Int(raw), (1...16).contains(count) else {
            printError("Enter a number from 1 to 16.")
            continue
        }
        return count
    }
}

func promptWorkerBridgeIPs(count: Int, localBridgeIPs: Set<String>) -> [String] {
    var workerIPs: [String] = []
    workerIPs.reserveCapacity(count)

    for index in 1...count {
        while true {
            let input = promptRequired("Worker \(index) IP")
            if !validateIPv4(input) {
                printError("Enter a valid IPv4 address.")
                continue
            }
            if localBridgeIPs.contains(input) {
                printWarning("That IP matches this orchestrator's bridge interface.")
                if !confirm("Use it anyway") { continue }
            }
            if workerIPs.contains(input) {
                printWarning("IP already entered. Add anyway only if this is intentional.")
                if !confirm("Use this IP again") { continue }
            }
            workerIPs.append(input)
            break
        }
    }

    return workerIPs
}

func summarizeThunderboltBenchmarkMedia(at path: String) -> (imageCount: Int, videoCount: Int)? {
    let fm = FileManager.default
    let expandedPath = expandTildePath(path)
    var isDir: ObjCBool = false
    guard fm.fileExists(atPath: expandedPath, isDirectory: &isDir), isDir.boolValue else { return nil }

    let files = (try? fm.contentsOfDirectory(atPath: expandedPath)) ?? []
    var imageCount = 0
    var videoCount = 0

    for name in files {
        guard !name.hasPrefix(".") else { continue }
        let fullPath = "\(expandedPath)/\(name)"
        var entryIsDir: ObjCBool = false
        guard fm.fileExists(atPath: fullPath, isDirectory: &entryIsDir), !entryIsDir.boolValue else {
            continue
        }
        guard let attrs = try? fm.attributesOfItem(atPath: fullPath),
              let size = attrs[.size] as? Int,
              size > 0 else {
            continue
        }

        // Mirror benchmark classification: any non-image media is treated as video.
        if ImageProcessor.isImage(path: fullPath) {
            imageCount += 1
        } else {
            videoCount += 1
        }
    }

    return (imageCount, videoCount)
}

func connectSocket(host: String, port: UInt16, sourceIP: String, timeoutMS: Int = thunderboltConnectTimeoutMS) -> Int32? {
    ThunderboltTransport.connect(
        host: host,
        port: Int(port),
        timeoutMS: timeoutMS,
        sourceIP: sourceIP
    )
}

func selectBridge(for workerIP: String, from bridges: [ThunderboltBridgeInterface]) -> ThunderboltBridgeInterface? {
    let candidates = bridges.filter { $0.ipv4 != nil }
    guard !candidates.isEmpty else { return nil }

    guard let remote = ipv4Raw(workerIP) else {
        return candidates[0]
    }

    for bridge in candidates {
        guard let ip = bridge.ipv4,
              let local = ipv4Raw(ip),
              let netmask = bridge.netmask else { continue }
        if (remote & netmask) == (local & netmask) {
            return bridge
        }
    }
    return candidates[0]
}

func waitForWorkerOnline(workerIP: String, port: UInt16, sourceIP: String, connectTimeoutMS: Int) -> Bool {
    let spinner = ["|", "/", "-", "\\"]
    var spinnerIndex = 0
    var probes = 0

    while true {
        let glyph = spinner[spinnerIndex % spinner.count]
        print(
            "  \(dim)Waiting for \(workerIP)... \(glyph)\(reset)",
            terminator: "\r"
        )
        fflush(stdout)

        if let socket = connectSocket(
            host: workerIP,
            port: port,
            sourceIP: sourceIP,
            timeoutMS: connectTimeoutMS
        ) {
            Darwin.close(socket)
            print("  \(String(repeating: " ", count: 96))", terminator: "\r")
            printSuccess("\(workerIP) reachable")
            return true
        }

        probes += 1
        spinnerIndex += 1
        if probes % 15 == 0 {
            print("  \(String(repeating: " ", count: 96))", terminator: "\r")
            if !confirm("\(workerIP) not responding. Keep probing") {
                return false
            }
        }
        usleep(2_000_000)
    }
}

func probeWorkersParallel(
    workers: [(workerIP: String, sourceIP: String)],
    port: UInt16,
    timeoutMS: Int
) -> [Bool] {
    guard !workers.isEmpty else { return [] }

    struct Pending {
        let index: Int
        let fd: Int32
    }

    var reachable = [Bool](repeating: false, count: workers.count)
    var pending: [Pending] = []

    for (index, worker) in workers.enumerated() {
        var hints = addrinfo(
            ai_flags: AI_ADDRCONFIG,
            ai_family: AF_INET,
            ai_socktype: SOCK_STREAM,
            ai_protocol: IPPROTO_TCP,
            ai_addrlen: 0, ai_canonname: nil, ai_addr: nil, ai_next: nil
        )
        var addrResult: UnsafeMutablePointer<addrinfo>?
        guard getaddrinfo(worker.workerIP, String(port), &hints, &addrResult) == 0,
              let ai = addrResult else { continue }
        defer { freeaddrinfo(ai) }

        let fd = socket(ai.pointee.ai_family, ai.pointee.ai_socktype, ai.pointee.ai_protocol)
        guard fd >= 0 else { continue }

        var one: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))

        var sourceAddr = sockaddr_in()
        sourceAddr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        sourceAddr.sin_family = sa_family_t(AF_INET)
        sourceAddr.sin_port = 0
        let parseRC = worker.sourceIP.withCString { inet_pton(AF_INET, $0, &sourceAddr.sin_addr) }
        guard parseRC == 1 else { Darwin.close(fd); continue }

        let bindRC = withUnsafePointer(to: &sourceAddr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                Darwin.bind(fd, sa, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindRC == 0 else { Darwin.close(fd); continue }

        let flags = fcntl(fd, F_GETFL, 0)
        guard flags >= 0, fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0 else {
            Darwin.close(fd); continue
        }

        let rc = Darwin.connect(fd, ai.pointee.ai_addr, ai.pointee.ai_addrlen)
        if rc == 0 {
            reachable[index] = true
            Darwin.close(fd)
            continue
        }
        guard errno == EINPROGRESS else { Darwin.close(fd); continue }

        pending.append(Pending(index: index, fd: fd))
    }

    if !pending.isEmpty {
        var pollfds = pending.map { pollfd(fd: $0.fd, events: Int16(POLLOUT), revents: 0) }
        let timeoutMillis = Int32(max(timeoutMS, 1))

        var pollRC: Int32
        repeat {
            pollRC = poll(&pollfds, nfds_t(pollfds.count), timeoutMillis)
        } while pollRC < 0 && errno == EINTR

        for (i, probe) in pending.enumerated() {
            defer { Darwin.close(probe.fd) }
            guard pollfds[i].revents & Int16(POLLOUT) != 0 else { continue }
            var socketError: Int32 = 0
            var errorLen = socklen_t(MemoryLayout<Int32>.size)
            getsockopt(probe.fd, SOL_SOCKET, SO_ERROR, &socketError, &errorLen)
            if socketError == 0 {
                reachable[probe.index] = true
            }
        }
    }

    return reachable
}

func updateMediaLaunchAgentEnvironment(home: String, updates: [String: String]) -> Bool {
    let plistPath = launchAgentPath(home: home, plist: "com.kiko.media.plist")
    guard FileManager.default.fileExists(atPath: plistPath) else {
        printError("Missing LaunchAgent plist: \(plistPath)")
        printHint("Run the full setup wizard first so com.kiko.media.plist exists.")
        return false
    }

    do {
        let url = URL(fileURLWithPath: plistPath)
        let data = try Data(contentsOf: url)
        var format = PropertyListSerialization.PropertyListFormat.xml
        guard var root = try PropertyListSerialization.propertyList(
            from: data,
            options: [.mutableContainersAndLeaves],
            format: &format
        ) as? [String: Any] else {
            printError("Unexpected plist structure in \(plistPath)")
            return false
        }

        var env = root["EnvironmentVariables"] as? [String: Any] ?? [:]
        for (key, value) in updates {
            env[key] = value
        }
        root["EnvironmentVariables"] = env

        let output = try PropertyListSerialization.data(fromPropertyList: root, format: .xml, options: 0)
        try output.write(to: url, options: .atomic)
        chmod600(plistPath)
        printSuccess("Updated ~/Library/LaunchAgents/com.kiko.media.plist")
        return true
    } catch {
        printError("Failed updating LaunchAgent plist: \(error.localizedDescription)")
        return false
    }
}

func persistThunderboltWorkersConfiguration(
    home: String,
    tbWorkers: String,
    tbPort: UInt16,
    tbConnectTimeout: Int
) -> Bool {
    updateMediaLaunchAgentEnvironment(
        home: home,
        updates: [
            "TB_WORKERS": tbWorkers,
            "TB_PORT": String(tbPort),
            "TB_CONNECT_TIMEOUT": String(tbConnectTimeout),
        ]
    )
}

func persistConcurrencyConfiguration(
    home: String,
    imageConcurrency: Int,
    videoConcurrency: Int
) -> Bool {
    updateMediaLaunchAgentEnvironment(
        home: home,
        updates: [
            "MAX_CONCURRENT_IMAGES": String(imageConcurrency),
            "MAX_CONCURRENT_VIDEOS": String(videoConcurrency),
        ]
    )
}

func loadMediaLaunchAgentEnvironment(home: String) -> [String: String] {
    let plistPath = launchAgentPath(home: home, plist: "com.kiko.media.plist")
    guard FileManager.default.fileExists(atPath: plistPath) else { return [:] }

    do {
        let data = try Data(contentsOf: URL(fileURLWithPath: plistPath))
        var format = PropertyListSerialization.PropertyListFormat.xml
        guard let root = try PropertyListSerialization.propertyList(
            from: data,
            options: [],
            format: &format
        ) as? [String: Any],
        let env = root["EnvironmentVariables"] as? [String: Any] else {
            return [:]
        }

        var resolved: [String: String] = [:]
        resolved.reserveCapacity(env.count)
        for (key, value) in env {
            if let string = value as? String {
                resolved[key] = string
            }
        }
        return resolved
    } catch {
        printWarning("Could not read LaunchAgent environment from \(plistPath): \(error.localizedDescription)")
        return [:]
    }
}

func resolveThunderboltRuntimeBaseDirectory(persistedEnv: [String: String]) -> String {
    if let rawBaseDirectory = persistedEnv["BASE_DIRECTORY"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !rawBaseDirectory.isEmpty {
        return expandTildePath(rawBaseDirectory)
    }
    return expandTildePath("~/Documents/kiko-media")
}

struct ThunderboltStatusWorker {
    let host: String
    let slots: Int
}

func parseThunderboltStatusWorkers(_ raw: String) -> (workers: [ThunderboltStatusWorker], invalidEntries: [String]) {
    var workers: [ThunderboltStatusWorker] = []
    workers.reserveCapacity(8)
    var invalidEntries: [String] = []

    for entry in raw.split(separator: ",", omittingEmptySubsequences: false) {
        let trimmed = entry.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { continue }

        let parts = trimmed.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)
        guard parts.count == 2 else {
            invalidEntries.append(trimmed)
            continue
        }

        let host = String(parts[0]).trimmingCharacters(in: .whitespacesAndNewlines)
        let slotsRaw = String(parts[1]).trimmingCharacters(in: .whitespacesAndNewlines)
        guard !host.isEmpty, let slots = Int(slotsRaw), slots > 0 else {
            invalidEntries.append(trimmed)
            continue
        }

        workers.append(ThunderboltStatusWorker(host: host, slots: slots))
    }

    return (workers: workers, invalidEntries: invalidEntries)
}

func parseThunderboltStatusPort(_ raw: String?) -> UInt16? {
    guard let raw else { return nil }
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty,
          let value = Int(trimmed),
          (1...65535).contains(value) else { return nil }
    return UInt16(value)
}

func parseThunderboltStatusConnectTimeout(_ raw: String?) -> Int? {
    guard let raw else { return nil }
    return parseThunderboltConnectTimeoutMSValue(raw)
}

func resolveThunderboltWorkersRaw(
    processEnv: [String: String],
    persistedEnv: [String: String]
) -> (raw: String?, source: String) {
    if let value = processEnv["TB_WORKERS"]?.trimmingCharacters(in: .whitespacesAndNewlines), !value.isEmpty {
        return (raw: value, source: "environment")
    }
    if let value = persistedEnv["TB_WORKERS"]?.trimmingCharacters(in: .whitespacesAndNewlines), !value.isEmpty {
        return (raw: value, source: "com.kiko.media.plist")
    }
    return (raw: nil, source: "not configured")
}

func printThunderboltWorkerCountStatus(home: String) {
    let processEnv = ProcessInfo.processInfo.environment
    let persistedEnv = loadMediaLaunchAgentEnvironment(home: home)
    let workersResolution = resolveThunderboltWorkersRaw(processEnv: processEnv, persistedEnv: persistedEnv)

    guard let workersRaw = workersResolution.raw else {
        printField("Thunderbolt workers", "0 \(dim)(TB_WORKERS not set)\(reset).")
        return
    }

    let parsedWorkers = parseThunderboltStatusWorkers(workersRaw)
    let totalSlots = parsedWorkers.workers.reduce(0) { $0 + $1.slots }
    printField(
        "Thunderbolt workers",
        "\(parsedWorkers.workers.count) worker(s), \(totalSlots) slot(s) \(dim)(source: \(workersResolution.source))\(reset)."
    )
    if !parsedWorkers.invalidEntries.isEmpty {
        let noun = parsedWorkers.invalidEntries.count == 1 ? "entry" : "entries"
        printWarning("Ignoring \(parsedWorkers.invalidEntries.count) invalid TB_WORKERS \(noun) while counting.")
    }
}

func printThunderboltStopWorkerIndependenceNote() {
    printHint("Remote Thunderbolt workers are independent; this command only controls local orchestrator services.")
}

func runThunderboltStartAdvisoryProbe(repoRoot: String, home: String) {
    let processEnv = ProcessInfo.processInfo.environment
    let persistedEnv = loadMediaLaunchAgentEnvironment(home: home)
    let workersResolution = resolveThunderboltWorkersRaw(processEnv: processEnv, persistedEnv: persistedEnv)

    guard let workersRaw = workersResolution.raw else {
        printHint("TB_WORKERS is not configured; skipping advisory worker probe.")
        return
    }

    let parsedWorkers = parseThunderboltStatusWorkers(workersRaw)
    if parsedWorkers.workers.isEmpty {
        printWarning("TB_WORKERS has no valid entries; skipping advisory worker probe.")
        return
    }

    let probeExit = runThunderboltStatusCommand(repoRoot: repoRoot, home: home)
    if probeExit != 0 {
        printHint("Advisory probe only: services stay running even when workers are unreachable.")
    }
}

func runThunderboltStatusCommand(repoRoot _: String, home: String) -> Int32 {
    printSectionTitle("Thunderbolt Worker Status")

    let processEnv = ProcessInfo.processInfo.environment
    let persistedEnv = loadMediaLaunchAgentEnvironment(home: home)

    let workersResolution = resolveThunderboltWorkersRaw(processEnv: processEnv, persistedEnv: persistedEnv)
    let workersRaw = workersResolution.raw
    let workersSource = workersResolution.source
    let parsedWorkers = workersRaw.map(parseThunderboltStatusWorkers)
        ?? (workers: [], invalidEntries: [])

    let defaultPort = Config.intDefaults["TB_PORT"]!.fallback
    var portSource = "default \(defaultPort)"
    var port = UInt16(defaultPort)

    if let envPortRaw = processEnv["TB_PORT"]?.trimmingCharacters(in: .whitespacesAndNewlines), !envPortRaw.isEmpty {
        if let envPort = parseThunderboltStatusPort(envPortRaw) {
            port = envPort
            portSource = "environment"
        } else {
            printWarning("Ignoring invalid TB_PORT from environment: \(envPortRaw)")
        }
    }

    if portSource == "default \(defaultPort)",
       let persistedPortRaw = persistedEnv["TB_PORT"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !persistedPortRaw.isEmpty {
        if let persistedPort = parseThunderboltStatusPort(persistedPortRaw) {
            port = persistedPort
            portSource = "com.kiko.media.plist"
        } else {
            printWarning("Ignoring invalid TB_PORT from com.kiko.media.plist: \(persistedPortRaw)")
        }
    }

    var connectTimeoutSource = "default \(thunderboltConnectTimeoutMS)ms"
    var connectTimeoutMS = thunderboltConnectTimeoutMS

    if let envTimeoutRaw = processEnv["TB_CONNECT_TIMEOUT"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !envTimeoutRaw.isEmpty {
        if let envTimeout = parseThunderboltStatusConnectTimeout(envTimeoutRaw) {
            connectTimeoutMS = envTimeout
            connectTimeoutSource = "environment"
        } else {
            printWarning("Ignoring invalid TB_CONNECT_TIMEOUT from environment: \(envTimeoutRaw)")
        }
    }

    if connectTimeoutSource == "default \(thunderboltConnectTimeoutMS)ms",
       let persistedTimeoutRaw = persistedEnv["TB_CONNECT_TIMEOUT"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !persistedTimeoutRaw.isEmpty {
        if let persistedTimeout = parseThunderboltStatusConnectTimeout(persistedTimeoutRaw) {
            connectTimeoutMS = persistedTimeout
            connectTimeoutSource = "com.kiko.media.plist"
        } else {
            printWarning("Ignoring invalid TB_CONNECT_TIMEOUT from com.kiko.media.plist: \(persistedTimeoutRaw)")
        }
    }

    let runtimeStatus = resolveOperatorVideoRuntimeSummary(
        processEnv: processEnv,
        persistedEnv: persistedEnv,
        workers: parsedWorkers.workers,
        port: Int(port),
        connectTimeoutMS: connectTimeoutMS
    )

    for invalid in parsedWorkers.invalidEntries {
        printWarning("Ignoring invalid TB_WORKERS entry: \(invalid)")
    }
    printHint("TB_WORKERS source: \(workersSource)")
    printHint("TB_PORT source: \(portSource)")
    printHint("TB_CONNECT_TIMEOUT source: \(connectTimeoutSource)")
    printHint("VIDEO_SCHEDULER_POLICY source: \(runtimeStatus.policyResolution.source)")
    print()
    printField("Policy", runtimeStatus.summary.policy.rawValue)
    printField("Runtime", runtimeStatus.summary.runtime.rawValue)
    printField("Reason", runtimeStatus.summary.reason.rawValue)
    print()

    guard workersRaw != nil else {
        printError("TB_WORKERS is not configured. Set TB_WORKERS or run --thunderbolt first.")
        return 1
    }
    guard !parsedWorkers.workers.isEmpty else {
        printError("TB_WORKERS has no valid entries. Expected format: host:slots,host:slots")
        return 1
    }

    let bridges = detectBridgeInterfaces().filter { $0.ipv4 != nil }
    if bridges.isEmpty {
        printWarning("No local bridge interface with IPv4 address found; source-bound probes cannot run.")
    }
    printCompactSectionTitle("Workers")
    print()

    var reachable = 0
    var unreachable = 0
    for worker in parsedWorkers.workers {
        guard let bridge = selectBridge(for: worker.host, from: bridges),
              let sourceIP = bridge.ipv4 else {
            printBody("\(worker.host):\(port) (\(worker.slots) slot(s)) — \(red)unreachable\(reset)")
            printHint("No bridge source IP available for \(worker.host); cannot run source-bound probe.")
            unreachable += 1
            continue
        }

        if let socket = connectSocket(
            host: worker.host,
            port: port,
            sourceIP: sourceIP,
            timeoutMS: connectTimeoutMS
        ) {
            Darwin.close(socket)
            printBody("\(worker.host):\(port) (\(worker.slots) slot(s)) via \(sourceIP) (\(bridge.name)) — \(green)reachable\(reset)")
            reachable += 1
        } else {
            printBody("\(worker.host):\(port) (\(worker.slots) slot(s)) via \(sourceIP) (\(bridge.name)) — \(red)unreachable\(reset)")
            printHint("Could not connect to worker at \(worker.host):\(port) using source-bind \(sourceIP). Check worker/service/network path.")
            unreachable += 1
        }
    }

    print()
    if unreachable == 0 {
        printSuccess("All configured workers are reachable (\(reachable)).")
        return 0
    }

    printWarning("Unreachable workers: \(unreachable) of \(parsedWorkers.workers.count)")
    print()
    printHint("Reconfigure: orchestrator --thunderbolt")
    return 1
}

struct ThunderboltBenchmarkDelegationInvocation {
    let executable: String
    let arguments: [String]
    let displayCommand: String
}

struct ThunderboltDelegatedBenchmarkWorker: Decodable {
    let index: Int
    let host: String
    let configuredSlots: Int

    enum CodingKeys: String, CodingKey {
        case index
        case host
        case configuredSlots = "configured_slots"
    }
}

struct ThunderboltDelegatedBenchmarkRemoteWorker: Decodable {
    let index: Int
    let host: String
    let slots: Int
}

struct ThunderboltDelegatedBenchmarkBestConfig: Decodable {
    let localSlots: Int
    let remoteWorkers: [ThunderboltDelegatedBenchmarkRemoteWorker]
    let wallSeconds: Double
    let videosPerMin: Double
    let completedVideos: Int
    let failedVideos: Int

    enum CodingKeys: String, CodingKey {
        case localSlots = "local_slots"
        case remoteWorkers = "remote_workers"
        case wallSeconds = "wall_seconds"
        case videosPerMin = "videos_per_min"
        case completedVideos = "completed_videos"
        case failedVideos = "failed_videos"
    }
}

private let thunderboltDelegatedBenchmarkSchemaVersion = 1

struct ThunderboltDelegatedBenchmarkPayload: Decodable {
    let schemaVersion: Int
    let workers: [ThunderboltDelegatedBenchmarkWorker]
    let bestConfig: ThunderboltDelegatedBenchmarkBestConfig

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case workers
        case bestConfig = "best_config"
    }
}

struct ThunderboltDelegatedRecommendation {
    let localSlots: Int
    let recommendedSlots: [Int]
    let wallSeconds: Double
    let videosPerMin: Double
}

enum ThunderboltDelegatedRecommendationResult {
    case success(ThunderboltDelegatedRecommendation)
    case fallback(reason: String)
}

enum ThunderboltDelegatedRecommendationValidationResult {
    case success(ThunderboltDelegatedRecommendation)
    case failure(String)
}

func swiftRunBenchmarkDelegationInvocation() -> ThunderboltBenchmarkDelegationInvocation {
    ThunderboltBenchmarkDelegationInvocation(
        executable: "/usr/bin/swift",
        arguments: ["run", "-c", "release", "benchmark"],
        displayCommand: "swift run -c release benchmark"
    )
}

func resolveThunderboltBenchmarkDelegationInvocations(repoRoot: String) -> [ThunderboltBenchmarkDelegationInvocation] {
    let prebuiltBenchmarkPath = "\(repoRoot)/.build/release/benchmark"
    if FileManager.default.isExecutableFile(atPath: prebuiltBenchmarkPath) {
        return [
            ThunderboltBenchmarkDelegationInvocation(
                executable: prebuiltBenchmarkPath,
                arguments: [],
                displayCommand: "\(repoRoot)/.build/release/benchmark"
            ),
            swiftRunBenchmarkDelegationInvocation(),
        ]
    }

    return [swiftRunBenchmarkDelegationInvocation()]
}

func validateDelegatedThunderboltRecommendationPayload(
    _ payload: ThunderboltDelegatedBenchmarkPayload,
    configuredWorkers: [Config.ThunderboltWorker]
) -> ThunderboltDelegatedRecommendationValidationResult {
    guard payload.schemaVersion == thunderboltDelegatedBenchmarkSchemaVersion else {
        return .failure(
            "unsupported schema_version \(payload.schemaVersion); expected \(thunderboltDelegatedBenchmarkSchemaVersion)"
        )
    }

    guard payload.workers.count == configuredWorkers.count else {
        return .failure("\"workers\" count must match configured TB_WORKERS entries")
    }

    for (expectedIndex, configuredWorker) in configuredWorkers.enumerated() {
        let payloadWorker = payload.workers[expectedIndex]
        if payloadWorker.index != expectedIndex {
            return .failure("\"workers[\(expectedIndex)].index\" must equal \(expectedIndex)")
        }
        if payloadWorker.host != configuredWorker.host {
            return .failure("\"workers[\(expectedIndex)].host\" must preserve TB_WORKERS order and host token")
        }
        if payloadWorker.configuredSlots != configuredWorker.slots {
            return .failure("\"workers[\(expectedIndex)].configured_slots\" must preserve configured slots")
        }
    }

    let remoteWorkers = payload.bestConfig.remoteWorkers
    guard remoteWorkers.count == payload.workers.count else {
        return .failure("\"best_config.remote_workers\" must include one entry per worker")
    }

    var seenIndices = Set<Int>()
    var recommendedSlots: [Int] = []
    recommendedSlots.reserveCapacity(remoteWorkers.count)

    for (expectedPosition, remoteWorker) in remoteWorkers.enumerated() {
        if remoteWorker.index != expectedPosition {
            return .failure("\"best_config.remote_workers\" must be ordered by worker index")
        }
        guard payload.workers.indices.contains(remoteWorker.index) else {
            return .failure("\"best_config.remote_workers[\(expectedPosition)].index\" is out of range")
        }
        guard seenIndices.insert(remoteWorker.index).inserted else {
            return .failure("\"best_config.remote_workers\" contains duplicate index \(remoteWorker.index)")
        }

        let payloadWorker = payload.workers[remoteWorker.index]
        if remoteWorker.host != payloadWorker.host {
            return .failure("\"best_config.remote_workers[\(expectedPosition)].host\" must match workers[index].host")
        }
        guard (0...16).contains(remoteWorker.slots) else {
            return .failure("\"best_config.remote_workers[\(expectedPosition)].slots\" must be in 0...16")
        }
        guard remoteWorker.slots <= payloadWorker.configuredSlots else {
            return .failure(
                "\"best_config.remote_workers[\(expectedPosition)].slots\" must be <= workers[\(remoteWorker.index)].configured_slots"
            )
        }

        recommendedSlots.append(remoteWorker.slots)
    }

    guard (0...16).contains(payload.bestConfig.localSlots) else {
        return .failure("\"best_config.local_slots\" must be in 0...16")
    }
    guard payload.bestConfig.failedVideos == 0 else {
        return .failure("\"best_config.failed_videos\" must be 0")
    }
    guard payload.bestConfig.completedVideos > 0 else {
        return .failure("\"best_config.completed_videos\" must be > 0")
    }
    guard payload.bestConfig.wallSeconds.isFinite, payload.bestConfig.wallSeconds > 0 else {
        return .failure("\"best_config.wall_seconds\" must be finite and > 0")
    }
    guard payload.bestConfig.videosPerMin.isFinite, payload.bestConfig.videosPerMin >= 0 else {
        return .failure("\"best_config.videos_per_min\" must be finite and >= 0")
    }

    return .success(
        ThunderboltDelegatedRecommendation(
            localSlots: payload.bestConfig.localSlots,
            recommendedSlots: recommendedSlots,
            wallSeconds: payload.bestConfig.wallSeconds,
            videosPerMin: payload.bestConfig.videosPerMin
        )
    )
}

func parseDelegatedThunderboltRecommendation(
    jsonOutput: String,
    configuredWorkers: [Config.ThunderboltWorker]
) -> ThunderboltDelegatedRecommendationValidationResult {
    guard !jsonOutput.isEmpty else {
        return .failure("benchmark emitted empty JSON output")
    }
    guard let data = jsonOutput.data(using: .utf8) else {
        return .failure("benchmark emitted non-UTF8 output")
    }

    let decoder = JSONDecoder()
    do {
        let payload = try decoder.decode(ThunderboltDelegatedBenchmarkPayload.self, from: data)
        return validateDelegatedThunderboltRecommendationPayload(payload, configuredWorkers: configuredWorkers)
    } catch {
        return .failure("JSON decode failed: \(error.localizedDescription)")
    }
}

func runDelegatedThunderboltBenchmark(
    repoRoot: String,
    mediaPath: String,
    tbWorkers: String,
    tbPort: UInt16,
    tbConnectTimeout: Int,
    runtimeBaseDir: String? = nil,
    sweepMode: String = "exhaustive",
    showProgress: Bool = false
) -> ThunderboltDelegatedRecommendationResult {
    let configuredWorkers = Config.parseThunderboltWorkers(tbWorkers)
    guard !configuredWorkers.isEmpty else {
        return .fallback(reason: "TB_WORKERS input is empty or invalid.")
    }

    let delegatedArgs = [
        "--stage", "thunderbolt",
        "--json",
        mediaPath,
        "--tb-workers", tbWorkers,
        "--tb-port", String(tbPort),
        "--tb-connect-timeout", String(tbConnectTimeout),
        "--sweep-mode", sweepMode,
    ]
    let environmentOverrides: [String: String]?
    if let runtimeBaseDir {
        let trimmedRuntimeBaseDir = runtimeBaseDir.trimmingCharacters(in: .whitespacesAndNewlines)
        environmentOverrides = trimmedRuntimeBaseDir.isEmpty
            ? nil
            : ["BASE_DIRECTORY": trimmedRuntimeBaseDir]
    } else {
        environmentOverrides = nil
    }

    let invocations = resolveThunderboltBenchmarkDelegationInvocations(repoRoot: repoRoot)
    for (attempt, invocation) in invocations.enumerated() {
        let args = invocation.arguments + delegatedArgs
        let execution: (exitCode: Int32, output: String)
        if showProgress {
            execution = runProcessStdoutOnly(
                executable: invocation.executable,
                arguments: args,
                workingDirectory: repoRoot,
                environmentOverrides: environmentOverrides
            )
        } else {
            execution = runProcess(
                executable: invocation.executable,
                arguments: args,
                workingDirectory: repoRoot,
                environmentOverrides: environmentOverrides
            )
        }

        if execution.exitCode != 0 {
            let summary = firstLine(execution.output)
            let reason = summary.isEmpty
                ? "exit \(execution.exitCode)"
                : "exit \(execution.exitCode): \(summary)"
            if attempt < invocations.count - 1 {
                continue
            }
            return .fallback(reason: "delegated benchmark failed (\(reason))")
        }

        switch parseDelegatedThunderboltRecommendation(
            jsonOutput: execution.output,
            configuredWorkers: configuredWorkers
        ) {
        case .success(let recommendation):
            return .success(recommendation)
        case .failure(let reason):
            if attempt < invocations.count - 1 {
                continue
            }
            return .fallback(reason: "delegated recommendation payload invalid (\(reason))")
        }
    }

    return .fallback(reason: "delegated benchmark did not produce a usable recommendation.")
}

func computeConfigSpaceSize(
    localCeiling: Int,
    perWorkerCaps: [Int]
) -> Int {
    guard localCeiling >= 0, perWorkerCaps.allSatisfy({ $0 >= 0 }) else { return 0 }

    var configCount = localCeiling + 1
    for cap in perWorkerCaps {
        let (product, overflow) = configCount.multipliedReportingOverflow(by: cap + 1)
        if overflow {
            return Int.max
        }
        configCount = product
    }
    return max(0, configCount - 1)
}

func promptSweepMode(
    configSpaceSize: Int,
    ceilings: [Int]
) -> String {
    if configSpaceSize <= 25 {
        print("  \(configSpaceSize) configurations detected. Running exhaustive sweep.")
        print()
        return "exhaustive"
    }

    let machineCount = ceilings.count
    while true {
        print("  \(dim)── Sweep Mode ──────────────────────────────────────────────\(reset)")
        print()
        print("    \(configSpaceSize) configurations detected.")
        print()
        printActionMenuItem(
            1,
            title: "Algorithmic Search",
            detail: "Profiles each machine, then targets best combinations."
        )
        print("\(listDetailIndent)\(dim)Less runs across \(machineCount) machine(s).\(reset)")
        print()
        printActionMenuItem(
            2,
            title: "Exhaustive Search",
            detail: "Tests every configuration."
        )
        print()

        let input = prompt("Sweep mode", default: "1").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if input.isEmpty || input == "1" || input == "smart" {
            return "smart"
        }
        if input == "2" || input == "exhaustive" {
            return "exhaustive"
        }
        printError("Enter 1 or 2.")
        print()
    }
}

// MARK: - Pipeline Benchmark Delegation

struct PipelineDelegatedBenchmarkPayload: Decodable {
    let imageConcurrency: Int
    let videoConcurrency: Int

    enum CodingKeys: String, CodingKey {
        case imageConcurrency = "image_concurrency"
        case videoConcurrency = "video_concurrency"
    }
}

struct PipelineDelegatedRecommendation {
    let imageConcurrency: Int
    let videoConcurrency: Int
}

enum PipelineDelegatedRecommendationResult {
    case success(PipelineDelegatedRecommendation)
    case fallback(reason: String)
}

func runDelegatedPipelineBenchmark(
    repoRoot: String,
    mediaPath: String,
    showProgress: Bool = false
) -> PipelineDelegatedRecommendationResult {
    let delegatedArgs = [
        "--stage", "pipeline",
        "--json",
        mediaPath,
    ]

    let stderrPassthrough = showProgress
    let invocations = resolveThunderboltBenchmarkDelegationInvocations(repoRoot: repoRoot)
    for (attempt, invocation) in invocations.enumerated() {
        let execution: (exitCode: Int32, output: String)
        if stderrPassthrough {
            execution = runProcessStdoutOnly(
                executable: invocation.executable,
                arguments: invocation.arguments + delegatedArgs,
                workingDirectory: repoRoot
            )
        } else {
            execution = runProcess(
                executable: invocation.executable,
                arguments: invocation.arguments + delegatedArgs,
                workingDirectory: repoRoot
            )
        }

        if execution.exitCode != 0 {
            let summary = firstLine(execution.output)
            let reason = summary.isEmpty
                ? "exit \(execution.exitCode) — check folder has both images and videos"
                : "exit \(execution.exitCode): \(summary)"
            if attempt < invocations.count - 1 {
                continue
            }
            return .fallback(reason: reason)
        }

        guard !execution.output.isEmpty,
              let data = execution.output.data(using: .utf8) else {
            if attempt < invocations.count - 1 { continue }
            return .fallback(reason: "benchmark produced no output — check folder has both images and videos")
        }

        do {
            let payload = try JSONDecoder().decode(PipelineDelegatedBenchmarkPayload.self, from: data)
            guard (1...64).contains(payload.imageConcurrency) else {
                if attempt < invocations.count - 1 { continue }
                return .fallback(reason: "image_concurrency \(payload.imageConcurrency) out of range 1...64")
            }
            guard (1...16).contains(payload.videoConcurrency) else {
                if attempt < invocations.count - 1 { continue }
                return .fallback(reason: "video_concurrency \(payload.videoConcurrency) out of range 1...16")
            }
            return .success(PipelineDelegatedRecommendation(
                imageConcurrency: payload.imageConcurrency,
                videoConcurrency: payload.videoConcurrency
            ))
        } catch {
            if attempt < invocations.count - 1 { continue }
            return .fallback(reason: "JSON decode failed — check folder has both images and videos")
        }
    }

    return .fallback(reason: "delegated benchmark did not produce a usable recommendation.")
}

func runThunderboltCommand(repoRoot: String, home: String) -> Int32 {
    let tbSubtitle = "Thunderbolt offload setup."
    let totalSteps = 7
    var connectedThunderboltDevices: Int?
    var bridges: [ThunderboltBridgeInterface] = []

    // Step 1: Thunderbolt Bridge — detect + guide.
    bridgeCheck: while true {
        redraw([], subtitle: tbSubtitle)
        printStep(1, of: totalSteps, "Thunderbolt Bridge")
        print()

        let profiler = runProcess(
            executable: "/usr/sbin/system_profiler",
            arguments: ["SPThunderboltDataType", "-json"]
        )
        if profiler.exitCode == 0 {
            connectedThunderboltDevices = countConnectedThunderboltDevices(from: profiler.output)
        }

        bridges = detectBridgeInterfaces()
        let hasBridgeIP = bridges.contains(where: { $0.ipv4 != nil })

        let cablesText: String
        if let count = connectedThunderboltDevices, count > 0 {
            cablesText = "\(count) connected"
        } else {
            cablesText = "0 connected"
        }

        let bridgeIPText: String
        if let bridge = bridges.first(where: { $0.ipv4 != nil }), let ip = bridge.ipv4 {
            bridgeIPText = ip
        } else {
            bridgeIPText = "not assigned"
        }

        let mtuText: String
        if let bridge = bridges.first, let mtu = bridge.mtu {
            mtuText = String(mtu)
        } else {
            mtuText = "\(dim)—\(reset)"
        }

        printCompactSectionTitle("Bridge")
        print("    \(dim)Cables\(reset)      \(cablesText)")
        print("    \(dim)IP\(reset)          \(bridgeIPText)")
        print("    \(dim)MTU\(reset)         \(mtuText)")
        print()

        if hasBridgeIP {
            printSuccess("Thunderbolt bridge is ready.")

            let nonJumbo = bridges.filter { $0.mtu != nil && $0.mtu != 9000 }
            if !nonJumbo.isEmpty {
                printWarning("MTU is not 9000. Recommended: set to 9000 in Details > Hardware.")
            }

            print()
            waitForEnter()
            break bridgeCheck
        }

        printWarning("Thunderbolt bridge is not ready.")
        print()
        printHint("Set up the Thunderbolt bridge before continuing.")
        printHint("See docs/runbook.md § Thunderbolt Bridge Prerequisites")
        print()

        while true {
            print("  \(bold)Action\(reset) \(dim)(Enter=retry, q=quit)\(reset): ", terminator: "")
            fflush(stdout)
            let input = (readLine() ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            if input.isEmpty { continue bridgeCheck }
            if input == "q" { return 0 }
            printError("Press Enter to retry, or q to quit.")
        }
    }

    // Step 2: Worker Bridge IPs.
    redraw([], subtitle: tbSubtitle)
    printStep(2, of: totalSteps, "Worker Bridge IPs")
    print()

    let primaryBridge = bridges.first(where: { $0.ipv4 != nil })
    let bridgeIPDisplay: String
    if let bridge = primaryBridge, let ip = bridge.ipv4 {
        if let mask = bridge.netmask {
            let hostMask = UInt32(bigEndian: mask)
            let prefix = (~hostMask).leadingZeroBitCount
            bridgeIPDisplay = "\(ip)/\(prefix)"
        } else {
            bridgeIPDisplay = ip
        }
    } else {
        bridgeIPDisplay = "not assigned"
    }

    let cablesDisplay: String
    if let count = connectedThunderboltDevices, count > 0 {
        cablesDisplay = "\(count) connected"
    } else {
        cablesDisplay = "0 connected"
    }

    printCompactSectionTitle("This Mac")
    print("    \(dim)Bridge IP\(reset)     \(bridgeIPDisplay)")
    print("    \(dim)Cables\(reset)        \(cablesDisplay)")
    print()

    let suggestedWorkers = connectedThunderboltDevices.map { max($0, 1) } ?? 1
    let workerCount = promptWorkerCount(defaultCount: suggestedWorkers)
    print()

    let exampleWorkerIP: String
    if let ip = primaryBridge?.ipv4 {
        let parts = ip.split(separator: ".")
        if parts.count == 4, let last = Int(parts[3]) {
            exampleWorkerIP = "\(parts[0]).\(parts[1]).\(parts[2]).\(last == 10 ? 11 : 10)"
        } else {
            exampleWorkerIP = "192.168.100.10"
        }
    } else {
        exampleWorkerIP = "192.168.100.10"
    }

    printHint("Each worker needs Thunderbolt Bridge with a static IP")
    printHint("on the same subnet, e.g. \(exampleWorkerIP)")
    print()

    let localBridgeIPs = Set(bridges.compactMap(\.ipv4))
    let workerIPs = promptWorkerBridgeIPs(count: workerCount, localBridgeIPs: localBridgeIPs)
    print()
    printSuccess("\(workerIPs.count) worker IP\(workerIPs.count == 1 ? "" : "s") configured.")
    print()
    waitForEnter()

    // Step 3: Deploy Workers — generate artifact + show instructions.
    redraw([], subtitle: tbSubtitle)
    printStep(3, of: totalSteps, "Deploy Workers")
    print()

    guard let settings = loadThunderboltWorkerTemplateSettings(repoRoot: repoRoot) else {
        return 1
    }

    let templatePath = "\(repoRoot)/deploy/worker.swift.template"
    guard FileManager.default.fileExists(atPath: templatePath) else {
        printError("Missing template: \(templatePath)")
        return 1
    }

    let presetDisplay = settings.transcodePreset.hasPrefix("AVAssetExportPreset")
        ? String(settings.transcodePreset.dropFirst("AVAssetExportPreset".count))
        : settings.transcodePreset

    printCompactSectionTitle("Worker Settings")
    print("    \(dim)Port\(reset)          \(settings.port)")
    print("    \(dim)Transcode\(reset)     \(presetDisplay)")
    print()

    let outputPath = "\(repoRoot)/deploy/worker.swift"
    do {
        let content = try processTemplate(
            at: templatePath,
            replacements: thunderboltWorkerTemplateReplacements(settings: settings)
        )
        try assertNoUnreplacedPlaceholders(content, in: "deploy/worker.swift.template")
        try writeFile(content, to: outputPath)
        printSuccess("Generated deploy/worker.swift")
    } catch {
        printError("Failed generating deploy/worker.swift: \(error.localizedDescription)")
        return 1
    }

    print()
    printCompactSectionTitle("Worker File")
    print("    \(dim)Copy this file to each worker Mac\(reset)")
    print("    \(dim)Location\(reset)      \(outputPath)")
    print()
    printCompactSectionTitle("Start Worker")
    print("    \(dim)On the worker, open Terminal and run:\(reset)")
    print("    swift worker.swift")
    print("    \(dim)(run this in the folder where you copied worker.swift)\(reset)")
    print()
    waitForEnter("Press Enter when workers are running")

    // Step 4: Detect workers — parallel probe with auto-retry.
    var probeTargets: [(workerIP: String, sourceIP: String)] = []

    for workerIP in workerIPs {
        if let bridge = selectBridge(for: workerIP, from: bridges),
           let sourceIP = bridge.ipv4 {
            probeTargets.append((workerIP: workerIP, sourceIP: sourceIP))
        }
    }

    guard !probeTargets.isEmpty else {
        redraw([], subtitle: tbSubtitle)
        printStep(4, of: totalSteps, "Detect Workers")
        print()
        printError("No local bridge available for probing.")
        return 1
    }

    var reachable = [Bool](repeating: false, count: probeTargets.count)
    let maxSweepsBeforeAsk = 10
    var sweepsSinceLastProgress = 0

    detectLoop: while true {
        let unreachableIndices = reachable.indices.filter { !reachable[$0] }

        if !unreachableIndices.isEmpty {
            let toProbe = unreachableIndices.map { probeTargets[$0] }
            let results = probeWorkersParallel(
                workers: toProbe,
                port: settings.port,
                timeoutMS: settings.connectTimeout
            )
            var foundNew = false
            for (i, unreachableIndex) in unreachableIndices.enumerated() {
                if results[i] {
                    reachable[unreachableIndex] = true
                    foundNew = true
                }
            }
            sweepsSinceLastProgress = foundNew ? 0 : sweepsSinceLastProgress + 1
        }

        redraw([], subtitle: tbSubtitle)
        printStep(4, of: totalSteps, "Detect Workers")
        print()

        printCompactSectionTitle("Workers")
        for (i, target) in probeTargets.enumerated() {
            if reachable[i] {
                print("    \(green)✓\(reset) \(target.workerIP)")
            } else {
                print("    \(dim)· \(target.workerIP)\(reset)")
            }
        }

        if reachable.allSatisfy({ $0 }) {
            break detectLoop
        }

        if sweepsSinceLastProgress >= maxSweepsBeforeAsk {
            let notResponding = reachable.filter { !$0 }.count
            print()
            printWarning("\(notResponding) of \(probeTargets.count) workers not responding.")
            if !confirm("Keep trying") {
                break detectLoop
            }
            sweepsSinceLastProgress = 0
        }
    }

    let reachableWorkers: [(workerIP: String, sourceIP: String)] = probeTargets.enumerated()
        .filter { reachable[$0.offset] }
        .map { (workerIP: $0.element.workerIP, sourceIP: $0.element.sourceIP) }

    guard !reachableWorkers.isEmpty else {
        print()
        printError("No workers detected. Nothing to benchmark.")
        return 1
    }

    print()
    printSuccess("\(reachableWorkers.count) of \(probeTargets.count) workers detected.")
    print()
    waitForEnter()

    // Step 5: collect a media folder and run the benchmark passes needed for worker sizing.
    var step5Error: String? = nil
    var mediaPath = ""
    var imageCount = 0
    var videoCount = 0

    while true {
        redraw([], subtitle: tbSubtitle)
        printStep(5, of: totalSteps, "Run Benchmarks")
        print()

        if let err = step5Error {
            printError(err)
            print()
        }

        printHint("Drag a folder from Finder, or type the path.")
        printHint("Needs sample videos from a real event.")
        print()

        let input = promptRequired("Media folder")
        let expanded = normalizePathInput(input)

        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: expanded, isDirectory: &isDir), isDir.boolValue else {
            step5Error = "Not a valid directory: \(expanded)"
            continue
        }

        guard let mediaSummary = summarizeThunderboltBenchmarkMedia(at: expanded) else {
            step5Error = "Not a valid directory: \(expanded)"
            continue
        }
        if mediaSummary.imageCount == 0 && mediaSummary.videoCount == 0 {
            step5Error = "No supported media files found in: \(expanded)"
            continue
        }
        if mediaSummary.videoCount == 0 {
            step5Error = "No videos found in: \(expanded)"
            continue
        }
        if mediaSummary.videoCount < 4 {
            step5Error = "Found \(mediaSummary.videoCount) video\(mediaSummary.videoCount == 1 ? "" : "s"), need at least 4 for reliable benchmarks."
            continue
        }

        mediaPath = expanded
        imageCount = mediaSummary.imageCount
        videoCount = mediaSummary.videoCount
        break
    }

    // Phase 1: probe hardware, show Macs table, run benchmarks.
    redraw([], subtitle: tbSubtitle)
    printStep(5, of: totalSteps, "Run Benchmarks")
    print()
    print("  \(dim)Media folder:\(reset) \(mediaPath)")
    print("  \(dim)Images:\(reset) \(imageCount)")
    print("  \(dim)Videos:\(reset) \(videoCount)")
    print()

    let existingEnv = loadMediaLaunchAgentEnvironment(home: home)
    let runtimeBaseDir = resolveThunderboltRuntimeBaseDirectory(persistedEnv: existingEnv)
    let existingImg = existingEnv["MAX_CONCURRENT_IMAGES"].flatMap { Int($0) }
    let existingVid = existingEnv["MAX_CONCURRENT_VIDEOS"].flatMap { Int($0) }

    let localCaps = WorkerCaps.detectLocal()
    guard let localCores = localCaps.totalCores, let localEngines = localCaps.videoEncodeEngines else {
        printError("Local hardware detection failed")
        return 1
    }
    let localCeiling = ThunderboltCapabilities.sweepCeiling(
        totalCores: localCores,
        videoEncodeEngines: localEngines
    )

    var workerCaps: [WorkerCaps] = []
    var perWorkerCaps: [Int] = []
    workerCaps.reserveCapacity(reachableWorkers.count)
    perWorkerCaps.reserveCapacity(reachableWorkers.count)
    for worker in reachableWorkers {
        guard let caps = ThunderboltTransport.queryCapabilities(
            host: worker.workerIP,
            port: Int(settings.port),
            timeoutMS: settings.connectTimeout,
            sourceIP: worker.sourceIP
        ) else {
            printError("Could not query capabilities for \(worker.workerIP)")
            return 1
        }
        guard let cores = caps.totalCores, let engines = caps.videoEncodeEngines else {
            printError("Hardware detection failed on \(worker.workerIP)")
            return 1
        }
        workerCaps.append(caps)
        perWorkerCaps.append(min(ThunderboltCapabilities.sweepCeiling(totalCores: cores, videoEncodeEngines: engines), 16))
    }

    func chipLabel(_ name: String?) -> String {
        guard let name else { return "?" }
        return name.hasPrefix("Apple ") ? String(name.dropFirst(6)) : name
    }
    func coreLabel(_ p: Int?, _ e: Int?) -> String {
        guard let p, let e else { return "?" }
        return "\(p)/\(e)"
    }
    func hwLabel(_ enc: Int?, _ dec: Int?) -> String {
        let e = enc.map(String.init) ?? "?"
        let d = dec.map(String.init) ?? "?"
        return "\(e)/\(d)"
    }

    printCompactSectionTitle("Macs")
    print("    \(dim)Name     Chip        CPU (P/E)  HW (E/D)  Max Slots\(reset)")
    print("  \(dim)------------------------------------------------------\(reset)")
    print("    Local    \(chipLabel(localCaps.chipName).padding(toLength: 10, withPad: " ", startingAt: 0))  \(coreLabel(localCaps.performanceCores, localCaps.efficiencyCores).padding(toLength: 9, withPad: " ", startingAt: 0))  \(hwLabel(localCaps.videoEncodeEngines, localCaps.videoDecodeEngines).padding(toLength: 8, withPad: " ", startingAt: 0))  \(localCeiling)")
    for (index, _) in reachableWorkers.enumerated() {
        let caps = workerCaps[index]
        let ceiling = perWorkerCaps[index]
        print("    W\(index + 1)       \(chipLabel(caps.chipName).padding(toLength: 10, withPad: " ", startingAt: 0))  \(coreLabel(caps.performanceCores, caps.efficiencyCores).padding(toLength: 9, withPad: " ", startingAt: 0))  \(hwLabel(caps.videoEncodeEngines, caps.videoDecodeEngines).padding(toLength: 8, withPad: " ", startingAt: 0))  \(ceiling)")
    }
    print()

    let configSpaceSize = computeConfigSpaceSize(
        localCeiling: localCeiling,
        perWorkerCaps: perWorkerCaps
    )
    let sweepMode = promptSweepMode(
        configSpaceSize: configSpaceSize,
        ceilings: [localCeiling] + perWorkerCaps
    )

    printHint("This might take a while.")
    printHint("To cancel: pkill -f benchmark (from another terminal)")
    print()

    let delegatedWorkers = zip(reachableWorkers, perWorkerCaps)
        .map { "\($0.0.workerIP):\($0.1)" }.joined(separator: ",")
    let burstResult = runDelegatedThunderboltBenchmark(
        repoRoot: repoRoot,
        mediaPath: mediaPath,
        tbWorkers: delegatedWorkers,
        tbPort: settings.port,
        tbConnectTimeout: settings.connectTimeout,
        runtimeBaseDir: runtimeBaseDir,
        sweepMode: sweepMode,
        showProgress: true
    )

    // Phase 2: redraw with results card.
    redraw([], subtitle: tbSubtitle)
    printStep(5, of: totalSteps, "Run Benchmarks")
    print()

    var localSlots: Int? = nil
    var slotRecommendations: [Int] = []
    var configurationSectionTitle = "Recommended Configuration"

    printCompactSectionTitle("Media")
    print("    \(dim)Path\(reset)          \(mediaPath)")
    print("    \(dim)Images\(reset)        \(imageCount)")
    print("    \(dim)Videos\(reset)        \(videoCount)")
    print()

    switch burstResult {
    case .success(let recommendation):
        localSlots = recommendation.localSlots
        slotRecommendations = recommendation.recommendedSlots
        printCompactSectionTitle("Burst Sweep")
        print("    \(dim)Local slots\(reset)   \(recommendation.localSlots)")
        for (index, worker) in reachableWorkers.enumerated() {
            let slots = recommendation.recommendedSlots[index]
            let workerLabel = thunderboltWorkerLabel(index: index)
            print("    \(dim)\(workerLabel) \(worker.workerIP)\(reset)   \(slots) slots")
        }
        print("    \(dim)Speed\(reset)         \(String(format: "%.2f", recommendation.videosPerMin)) videos/min")
        print("    \(dim)Duration\(reset)      \(String(format: "%.1fs", recommendation.wallSeconds))")
        print()
        printSuccess("Benchmarks complete.")
    case .fallback(let reason):
        printWarning("Burst sweep unavailable: \(reason)")
        printHint("Worker slots will be entered manually in the next step.")
        slotRecommendations = perWorkerCaps
        configurationSectionTitle = "Default Configuration (benchmark unavailable)"
    }

    print()
    waitForEnter()

    // Step 6: Confirm/override settings.
    redraw([], subtitle: tbSubtitle)
    printStep(6, of: totalSteps, "Confirm Settings")
    print()

    let finalImageConcurrency = existingImg ?? 6
    let finalLocalSlots = max(1, localSlots ?? existingVid ?? 2)

    printCompactSectionTitle(configurationSectionTitle)
    print("    \(dim)Images (local)\(reset)     \(finalImageConcurrency) concurrent")
    print("    \(dim)Videos (local)\(reset)     \(finalLocalSlots) concurrent")
    print()
    printCompactSectionTitle("Worker Slots")
    for (index, worker) in reachableWorkers.enumerated() {
        let slots = slotRecommendations[index]
        let workerLabel = thunderboltWorkerLabel(index: index)
        print("    \(dim)\(workerLabel) \(worker.workerIP)\(reset)     \(slots)")
    }
    print()

    var acceptedImageConcurrency = finalImageConcurrency
    var acceptedVideoConcurrency = finalLocalSlots
    var workerSlots: [(workerIP: String, slots: Int)] = reachableWorkers.enumerated().map { index, worker in
        (workerIP: worker.workerIP, slots: slotRecommendations[index])
    }

    if !confirm("Accept these settings") {
        while true {
            let raw = prompt("Images (local)", default: String(finalImageConcurrency))
            if let val = Int(raw), (1...64).contains(val) {
                acceptedImageConcurrency = val
                break
            }
            printError("Enter a value from 1 to 64.")
        }
        while true {
            let raw = prompt("Videos (local)", default: String(finalLocalSlots))
            if let val = Int(raw), (1...16).contains(val) {
                acceptedVideoConcurrency = val
                break
            }
            printError("Enter a value from 1 to 16.")
        }
        workerSlots = []
        for (index, worker) in reachableWorkers.enumerated() {
            let recommended = slotRecommendations[index]
            let workerLabel = thunderboltWorkerLabel(index: index)
            while true {
                let raw = prompt("\(workerLabel) slots for \(worker.workerIP)", default: String(recommended))
                if let val = Int(raw), (0...16).contains(val) {
                    workerSlots.append((workerIP: worker.workerIP, slots: val))
                    break
                }
                printError("Enter a value from 0 to 16.")
            }
        }
    }
    print()
    waitForEnter()

    // Step 7: Apply configuration — persist all three settings to plist.
    redraw([], subtitle: tbSubtitle)
    printStep(7, of: totalSteps, "Apply Configuration")
    print()

    let concurrencyOK = persistConcurrencyConfiguration(
        home: home,
        imageConcurrency: acceptedImageConcurrency,
        videoConcurrency: acceptedVideoConcurrency
    )
    if concurrencyOK {
        printSuccess("MAX_CONCURRENT_IMAGES = \(acceptedImageConcurrency)")
        printSuccess("MAX_CONCURRENT_VIDEOS = \(acceptedVideoConcurrency)")
    } else {
        printWarning("Could not write concurrency settings to plist.")
    }

    let activeWorkerSlots = workerSlots.filter { $0.slots > 0 }
    if activeWorkerSlots.isEmpty {
        printWarning("All workers have 0 slots. TB_WORKERS unchanged in plist.")
    } else {
        let tbWorkersValue = activeWorkerSlots.map { "\($0.workerIP):\($0.slots)" }.joined(separator: ",")
        let tbOK = persistThunderboltWorkersConfiguration(
            home: home,
            tbWorkers: tbWorkersValue,
            tbPort: settings.port,
            tbConnectTimeout: settings.connectTimeout
        )
        if tbOK {
            printSuccess("TB_WORKERS = \(tbWorkersValue)")
            printSuccess("TB_PORT = \(settings.port)")
            printSuccess("TB_CONNECT_TIMEOUT = \(settings.connectTimeout)ms")
        } else {
            printWarning("Could not write Thunderbolt settings to plist.")
        }
    }

    print()
    printHint("Restart kiko-media to apply: \(setupInvocationBase(repoRoot: repoRoot)) --restart")
    print()

    return 0
}
