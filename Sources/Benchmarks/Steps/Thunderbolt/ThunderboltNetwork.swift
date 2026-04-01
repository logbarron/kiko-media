import Foundation
import KikoMediaCore

func resolveThunderboltBenchmarkSettings() -> ThunderboltSettingsResolution {
    let processEnv = ProcessInfo.processInfo.environment
    let persistedEnv = loadBenchmarkMediaLaunchAgentEnvironment()

    let workersRaw: String?
    let workersSource: String
    if let envWorkers = trimmedNonEmpty(processEnv["TB_WORKERS"]) {
        workersRaw = envWorkers
        workersSource = "environment"
    } else if let persistedWorkers = trimmedNonEmpty(persistedEnv["TB_WORKERS"]) {
        workersRaw = persistedWorkers
        workersSource = "com.kiko.media.plist"
    } else {
        workersRaw = nil
        workersSource = "not configured"
    }

    let portDefault = benchmarkThunderboltDefaultPort
    let timeoutDefault = benchmarkThunderboltDefaultConnectTimeout

    var warnings: [String] = []

    let port: Int
    let portSource: String
    if let envPortRaw = trimmedNonEmpty(processEnv["TB_PORT"]) {
        if let envPort = parseIntSetting(envPortRaw, range: 1...65_535) {
            port = envPort
            portSource = "environment"
        } else {
            port = portDefault
            portSource = "default"
            warnings.append("Ignoring invalid TB_PORT from environment: \(envPortRaw)")
        }
    } else if let persistedPortRaw = trimmedNonEmpty(persistedEnv["TB_PORT"]) {
        if let persistedPort = parseIntSetting(persistedPortRaw, range: 1...65_535) {
            port = persistedPort
            portSource = "com.kiko.media.plist"
        } else {
            port = portDefault
            portSource = "default"
            warnings.append("Ignoring invalid TB_PORT from com.kiko.media.plist: \(persistedPortRaw)")
        }
    } else {
        port = portDefault
        portSource = "default"
    }

    let connectTimeout: Int
    let connectTimeoutSource: String
    if let envTimeoutRaw = trimmedNonEmpty(processEnv["TB_CONNECT_TIMEOUT"]) {
        if let envTimeoutRawValue = Int(envTimeoutRaw) {
            let envTimeout = (1...30).contains(envTimeoutRawValue) ? envTimeoutRawValue * 1_000 : envTimeoutRawValue
            if (100...30_000).contains(envTimeout) {
                connectTimeout = envTimeout
                connectTimeoutSource = "environment"
            } else {
                connectTimeout = timeoutDefault
                connectTimeoutSource = "default"
                warnings.append("Ignoring invalid TB_CONNECT_TIMEOUT from environment: \(envTimeoutRaw)")
            }
        } else {
            connectTimeout = timeoutDefault
            connectTimeoutSource = "default"
            warnings.append("Ignoring invalid TB_CONNECT_TIMEOUT from environment: \(envTimeoutRaw)")
        }
    } else if let persistedTimeoutRaw = trimmedNonEmpty(persistedEnv["TB_CONNECT_TIMEOUT"]) {
        if let persistedTimeoutRawValue = Int(persistedTimeoutRaw) {
            let persistedTimeout = (1...30).contains(persistedTimeoutRawValue) ? persistedTimeoutRawValue * 1_000 : persistedTimeoutRawValue
            if (100...30_000).contains(persistedTimeout) {
                connectTimeout = persistedTimeout
                connectTimeoutSource = "com.kiko.media.plist"
            } else {
                connectTimeout = timeoutDefault
                connectTimeoutSource = "default"
                warnings.append("Ignoring invalid TB_CONNECT_TIMEOUT from com.kiko.media.plist: \(persistedTimeoutRaw)")
            }
        } else {
            connectTimeout = timeoutDefault
            connectTimeoutSource = "default"
            warnings.append("Ignoring invalid TB_CONNECT_TIMEOUT from com.kiko.media.plist: \(persistedTimeoutRaw)")
        }
    } else {
        connectTimeout = timeoutDefault
        connectTimeoutSource = "default"
    }

    return ThunderboltSettingsResolution(
        workersRaw: workersRaw,
        workersSource: workersSource,
        port: port,
        portSource: portSource,
        connectTimeout: connectTimeout,
        connectTimeoutSource: connectTimeoutSource,
        warnings: warnings
    )
}

func loadBenchmarkMediaLaunchAgentEnvironment() -> [String: String] {
    let home = FileManager.default.homeDirectoryForCurrentUser.path
    let plistPath = "\(home)/Library/LaunchAgents/com.kiko.media.plist"
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
        return [:]
    }
}

func trimmedNonEmpty(_ raw: String?) -> String? {
    guard let raw else { return nil }
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    return trimmed.isEmpty ? nil : trimmed
}

func resolveLocalCASlotsDefault() -> Int {
    let processEnv = ProcessInfo.processInfo.environment
    if let envRaw = trimmedNonEmpty(processEnv["MAX_CONCURRENT_VIDEOS"]),
       let envValue = Int(envRaw), envValue > 0 {
        return envValue
    }

    let persistedEnv = loadBenchmarkMediaLaunchAgentEnvironment()
    if let persistedRaw = trimmedNonEmpty(persistedEnv["MAX_CONCURRENT_VIDEOS"]),
       let persistedValue = Int(persistedRaw), persistedValue > 0 {
        return persistedValue
    }

    return max(1, Config.intDefaults["MAX_CONCURRENT_VIDEOS"]?.fallback ?? 2)
}

func parseIntSetting(_ raw: String, range: ClosedRange<Int>) -> Int? {
    guard let value = Int(raw), range.contains(value) else { return nil }
    return value
}

func resolveBridgeBoundWorkers(
    workers: [ThunderboltWorkerSpec]
) -> ([ThunderboltBoundWorkerSpec], [ThunderboltWorkerBindingIssue]) {
    let bridgeSources = ThunderboltDispatcher.discoverBridgeSources()
    guard !bridgeSources.isEmpty else {
        let issues = workers.map {
            ThunderboltWorkerBindingIssue(
                worker: $0,
                reason: "no local bridge interface with IPv4 address detected"
            )
        }
        return ([], issues)
    }

    var boundWorkers: [ThunderboltBoundWorkerSpec] = []
    boundWorkers.reserveCapacity(workers.count)
    var issues: [ThunderboltWorkerBindingIssue] = []
    issues.reserveCapacity(workers.count)

    for worker in workers {
        guard let resolved = resolveIPv4Address(host: worker.host) else {
            issues.append(
                ThunderboltWorkerBindingIssue(
                    worker: worker,
                    reason: "could not resolve worker IPv4 address"
                )
            )
            continue
        }

        guard let source = bridgeSources.first(where: { (resolved.address & $0.mask) == $0.network }) else {
            issues.append(
                ThunderboltWorkerBindingIssue(
                    worker: worker,
                    reason: "no local bridge route to \(resolved.ip)"
                )
            )
            continue
        }

        boundWorkers.append(
            ThunderboltBoundWorkerSpec(
                host: worker.host,
                connectHost: resolved.ip,
                slots: worker.slots,
                sourceIP: source.ip,
                bridgeName: source.name
            )
        )
    }

    return (boundWorkers, issues)
}

func resolveIPv4Address(host: String) -> (ip: String, address: UInt32)? {
    var parsed = in_addr()
    if host.withCString({ inet_pton(AF_INET, $0, &parsed) }) == 1 {
        let ipText = ThunderboltDispatcher.ipv4String(parsed) ?? host
        return (ipText, UInt32(bigEndian: parsed.s_addr))
    }

    var hints = addrinfo(
        ai_flags: AI_ADDRCONFIG,
        ai_family: AF_INET,
        ai_socktype: SOCK_STREAM,
        ai_protocol: IPPROTO_TCP,
        ai_addrlen: 0,
        ai_canonname: nil,
        ai_addr: nil,
        ai_next: nil
    )
    var result: UnsafeMutablePointer<addrinfo>?
    let rc = getaddrinfo(host, nil, &hints, &result)
    guard rc == 0, let first = result else { return nil }
    defer { freeaddrinfo(first) }

    var cursor: UnsafeMutablePointer<addrinfo>? = first
    while let current = cursor {
        defer { cursor = current.pointee.ai_next }

        guard current.pointee.ai_family == AF_INET,
              let sockaddrPointer = current.pointee.ai_addr else {
            continue
        }

        let socketAddress = sockaddrPointer.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee }
        let rawAddress = UInt32(bigEndian: socketAddress.sin_addr.s_addr)
        guard let ipText = ThunderboltDispatcher.ipv4String(socketAddress.sin_addr) else { continue }
        return (ipText, rawAddress)
    }

    return nil
}

func precomputeSourceHashes(_ videos: [MediaFile]) throws -> [String: String] {
    var hashes: [String: String] = [:]
    hashes.reserveCapacity(videos.count)

    for video in videos {
        let sha = try SHA256Utility.calculateSHA256(path: video.path, bufferSize: BenchDefaults.sha256BufferSize)
        hashes[video.path] = sha
    }

    return hashes
}

func benchmarkThunderboltConnectivity(
    workers: [ThunderboltBoundWorkerSpec],
    port: Int,
    connectTimeout: Int
) -> [ThunderboltConnectivityResult] {
    let probeResults = ThunderboltWorkerProbe.measureReachability(
        endpoints: workers.map {
            ThunderboltWorkerProbeTarget(
                host: $0.connectHost,
                port: port,
                sourceIP: $0.sourceIP
            )
        },
        timeoutMS: connectTimeout
    )

    return zip(workers, probeResults).map { worker, result in
        ThunderboltConnectivityResult(
            worker: worker,
            reachable: result.reachable,
            connectMillis: result.connectMillis
        )
    }
}

func parseThunderboltWorkers(_ raw: String) -> [ThunderboltWorkerSpec] {
    Config.parseThunderboltWorkers(raw)
}

func probeWorkerCapabilities(
    workers: [ThunderboltBoundWorkerSpec],
    port: Int,
    connectTimeout: Int
) -> [WorkerCaps?] {
    ThunderboltWorkerProbe.queryCapabilities(
        endpoints: workers.map {
            ThunderboltWorkerProbeTarget(
                host: $0.connectHost,
                port: port,
                sourceIP: $0.sourceIP
            )
        },
        timeoutMS: connectTimeout
    )
}

func effectiveRemoteMaxSlots(
    workers: [ThunderboltBoundWorkerSpec]
) -> [Int] {
    workers.map(\.slots)
}
