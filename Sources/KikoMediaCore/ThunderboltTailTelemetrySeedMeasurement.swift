import Foundation

package struct ThunderboltTailTelemetrySeedProbeEndpoint: Sendable {
    package let id: String
    package let host: String
    package let port: Int
    package let sourceIP: String?
    package let connectTimeoutMS: Int

    package init(
        id: String,
        host: String,
        port: Int,
        sourceIP: String?,
        connectTimeoutMS: Int
    ) {
        self.id = id
        self.host = host
        self.port = port
        self.sourceIP = sourceIP
        self.connectTimeoutMS = connectTimeoutMS
    }
}

package struct ThunderboltTailTelemetrySeedSampleCandidate: Sendable {
    package let path: String
    package let fileSize: Int
    package let originalName: String
    package let mimeType: String

    package init(
        path: String,
        fileSize: Int,
        originalName: String,
        mimeType: String = ""
    ) {
        self.path = path
        self.fileSize = fileSize
        self.originalName = originalName
        self.mimeType = mimeType
    }
}

package struct ThunderboltTailTelemetrySeedEstimates: Sendable {
    package let txOutMSByID: [String: Double]
    package let publishOverheadMSByID: [String: Double]

    package init(
        txOutMSByID: [String: Double],
        publishOverheadMSByID: [String: Double]
    ) {
        self.txOutMSByID = txOutMSByID
        self.publishOverheadMSByID = publishOverheadMSByID
    }
}

package enum ThunderboltTailTelemetrySeedMeasurement {
    package typealias RoundTripRunner = @Sendable (
        _ endpoint: ThunderboltTailTelemetrySeedProbeEndpoint,
        _ sample: ThunderboltTailTelemetrySeedSampleCandidate,
        _ sourceSHA256: String,
        _ tempDir: String
    ) -> ThunderboltRawRoundTripResult

    package static func measure(
        endpoints: [ThunderboltTailTelemetrySeedProbeEndpoint],
        sampleCandidates: [ThunderboltTailTelemetrySeedSampleCandidate],
        providedTxOutEstimateMSByID: [String: Double] = [:],
        providedPublishOverheadEstimateMSByID: [String: Double] = [:],
        sha256BufferSize: Int,
        roundTripRunner: RoundTripRunner? = nil
    ) -> ThunderboltTailTelemetrySeedEstimates {
        guard !endpoints.isEmpty,
              let sample = deterministicSample(from: sampleCandidates),
              let sourceSHA256 = try? SHA256Utility.calculateSHA256(
                  path: sample.path,
                  bufferSize: sha256BufferSize
              ) else {
            return ThunderboltTailTelemetrySeedEstimates(
                txOutMSByID: [:],
                publishOverheadMSByID: [:]
            )
        }

        let tempDir = makeTempDir(prefix: "tb-ca-tail")
        defer { cleanup(tempDir) }

        var txOutByID: [String: Double] = [:]
        txOutByID.reserveCapacity(endpoints.count)
        var publishByID: [String: Double] = [:]
        publishByID.reserveCapacity(endpoints.count)

        for endpoint in endpoints {
            let txOutProvided = providedTxOutEstimateMSByID[endpoint.id] != nil
            let publishProvided = providedPublishOverheadEstimateMSByID[endpoint.id] != nil
            if txOutProvided, publishProvided {
                continue
            }

            let result: ThunderboltRawRoundTripResult
            if let roundTripRunner {
                result = roundTripRunner(endpoint, sample, sourceSHA256, tempDir)
            } else {
                result = ThunderboltRawExecution.runRemoteRoundTrip(
                    host: endpoint.host,
                    port: endpoint.port,
                    sourceIP: endpoint.sourceIP,
                    connectTimeoutMS: endpoint.connectTimeoutMS,
                    filePath: sample.path,
                    fileSize: sample.fileSize,
                    originalName: sample.originalName,
                    mimeType: sample.mimeType,
                    sourceSHA256: sourceSHA256,
                    tempDir: tempDir,
                    sha256BufferSize: sha256BufferSize
                )
            }
            guard result.success else { continue }

            if !txOutProvided,
               let estimateMS = sanitizedPositiveMS(result.txOutMS) {
                txOutByID[endpoint.id] = estimateMS
            }
            if !publishProvided,
               let estimateMS = sanitizedPositiveMS(result.publishOverheadMS) {
                publishByID[endpoint.id] = estimateMS
            }
        }

        return ThunderboltTailTelemetrySeedEstimates(
            txOutMSByID: txOutByID,
            publishOverheadMSByID: publishByID
        )
    }

    private static func deterministicSample(
        from candidates: [ThunderboltTailTelemetrySeedSampleCandidate]
    ) -> ThunderboltTailTelemetrySeedSampleCandidate? {
        guard var sample = candidates.first else { return nil }
        for candidate in candidates.dropFirst() {
            if candidate.fileSize > sample.fileSize
                || (candidate.fileSize == sample.fileSize && candidate.path < sample.path) {
                sample = candidate
            }
        }
        return sample
    }

    private static func sanitizedPositiveMS(_ value: Double?) -> Double? {
        guard let value,
              value.isFinite,
              value > 0 else {
            return nil
        }
        return max(0, value)
    }

    private static func makeTempDir(prefix: String) -> String {
        let path = NSTemporaryDirectory() + "kiko-core-\(prefix)-\(UUID().uuidString)"
        try? FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true)
        return path
    }

    private static func cleanup(_ path: String) {
        try? FileManager.default.removeItem(atPath: path)
    }
}
