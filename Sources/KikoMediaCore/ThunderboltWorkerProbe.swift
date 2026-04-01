import Foundation

package struct ThunderboltWorkerProbeTarget: Sendable {
    package let host: String
    package let port: Int
    package let sourceIP: String?

    package init(host: String, port: Int, sourceIP: String?) {
        self.host = host
        self.port = port
        self.sourceIP = sourceIP
    }
}

package struct ThunderboltWorkerReachabilityProbeResult: Sendable {
    package let reachable: Bool
    package let connectMillis: Double

    package init(reachable: Bool, connectMillis: Double) {
        self.reachable = reachable
        self.connectMillis = connectMillis
    }
}

package enum ThunderboltWorkerProbe {
    private static func elapsedMillis(
        from start: ContinuousClock.Instant,
        to end: ContinuousClock.Instant
    ) -> Double {
        let (seconds, attoseconds) = start.duration(to: end).components
        return (Double(seconds) + Double(attoseconds) / 1_000_000_000_000_000_000) * 1_000
    }

    package static func measureReachability(
        endpoints: [ThunderboltWorkerProbeTarget],
        timeoutMS: Int
    ) -> [ThunderboltWorkerReachabilityProbeResult] {
        let clock = ContinuousClock()

        return endpoints.map { endpoint in
            let started = clock.now
            let fd = ThunderboltTransport.connect(
                host: endpoint.host,
                port: endpoint.port,
                timeoutMS: timeoutMS,
                sourceIP: endpoint.sourceIP
            )
            let elapsedMS = elapsedMillis(from: started, to: clock.now)

            if let fd {
                ThunderboltTransport.closeConnection(fd: fd)
                return ThunderboltWorkerReachabilityProbeResult(
                    reachable: true,
                    connectMillis: elapsedMS
                )
            }

            return ThunderboltWorkerReachabilityProbeResult(
                reachable: false,
                connectMillis: elapsedMS
            )
        }
    }

    package static func queryCapabilities(
        endpoints: [ThunderboltWorkerProbeTarget],
        timeoutMS: Int,
        maxConcurrency: Int = 16
    ) -> [WorkerCaps?] {
        guard !endpoints.isEmpty else { return [] }

        let count = endpoints.count
        let buffer = UnsafeMutableBufferPointer<WorkerCaps?>.allocate(capacity: count)
        buffer.initialize(repeating: nil)
        nonisolated(unsafe) let base = buffer.baseAddress!

        let semaphore = DispatchSemaphore(value: min(count, max(1, maxConcurrency)))
        let group = DispatchGroup()

        for (index, endpoint) in endpoints.enumerated() {
            semaphore.wait()
            group.enter()
            DispatchQueue.global().async {
                defer { semaphore.signal(); group.leave() }
                base[index] = ThunderboltTransport.queryCapabilities(
                    host: endpoint.host,
                    port: endpoint.port,
                    timeoutMS: timeoutMS,
                    sourceIP: endpoint.sourceIP
                )
            }
        }
        group.wait()

        let results: [WorkerCaps?] = (0..<count).map { base[$0] }
        buffer.deallocate()
        return results
    }
}
