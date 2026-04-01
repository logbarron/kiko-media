import Foundation

package struct ThunderboltRawRoundTripResult: Sendable {
    package let success: Bool
    package let sendSeconds: Double
    package let processNanos: UInt64
    package let receiveSeconds: Double
    package let totalSeconds: Double
    package let firstRunningLatencySecondsEstimate: Double?
    package let txOutMS: Double?
    package let publishOverheadMS: Double?
    package let slotHealthDownOnFailure: Bool?

    package init(
        success: Bool,
        sendSeconds: Double,
        processNanos: UInt64,
        receiveSeconds: Double,
        totalSeconds: Double,
        firstRunningLatencySecondsEstimate: Double? = nil,
        txOutMS: Double? = nil,
        publishOverheadMS: Double? = nil,
        slotHealthDownOnFailure: Bool? = nil
    ) {
        self.success = success
        self.sendSeconds = sendSeconds
        self.processNanos = processNanos
        self.receiveSeconds = receiveSeconds
        self.totalSeconds = totalSeconds
        self.firstRunningLatencySecondsEstimate = firstRunningLatencySecondsEstimate
        self.txOutMS = txOutMS
        self.publishOverheadMS = publishOverheadMS
        self.slotHealthDownOnFailure = slotHealthDownOnFailure
    }
}

package enum ThunderboltRawExecution {
    private static func elapsedSeconds(
        from start: ContinuousClock.Instant,
        to end: ContinuousClock.Instant
    ) -> Double {
        let (seconds, attoseconds) = start.duration(to: end).components
        return Double(seconds) + Double(attoseconds) / 1_000_000_000_000_000_000
    }

    package static func runRemoteRoundTrip(
        host: String,
        port: Int,
        sourceIP: String?,
        connectTimeoutMS: Int,
        filePath: String,
        fileSize: Int,
        originalName: String,
        mimeType: String,
        sourceSHA256: String,
        tempDir: String,
        sha256BufferSize: Int
    ) -> ThunderboltRawRoundTripResult {
        let clock = ContinuousClock()
        let started = clock.now

        guard let fd = ThunderboltTransport.connect(
            host: host,
            port: port,
            timeoutMS: connectTimeoutMS,
            sourceIP: sourceIP
        ) else {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: 0,
                processNanos: 0,
                receiveSeconds: 0,
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                slotHealthDownOnFailure: true
            )
        }
        defer { ThunderboltTransport.closeConnection(fd: fd) }

        let sendStarted = clock.now
        guard ThunderboltTransport.sendHeader(
            fd: fd,
            fileSize: fileSize,
            sha256Hex: sourceSHA256,
            name: originalName,
            mime: mimeType
        ) else {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: elapsedSeconds(from: sendStarted, to: clock.now),
                processNanos: 0,
                receiveSeconds: 0,
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                slotHealthDownOnFailure: true
            )
        }

        guard ThunderboltTransport.sendFileData(fd: fd, filePath: filePath, fileSize: fileSize) else {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: elapsedSeconds(from: sendStarted, to: clock.now),
                processNanos: 0,
                receiveSeconds: 0,
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                slotHealthDownOnFailure: true
            )
        }
        let sendSeconds = elapsedSeconds(from: sendStarted, to: clock.now)

        let receiveStarted = clock.now
        guard let response = ThunderboltTransport.readResponseHeader(fd: fd) else {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: sendSeconds,
                processNanos: 0,
                receiveSeconds: elapsedSeconds(from: receiveStarted, to: clock.now),
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                slotHealthDownOnFailure: true
            )
        }
        let responseHeaderReceived = clock.now
        let dispatchToCompletionSeconds = elapsedSeconds(from: sendStarted, to: responseHeaderReceived)
        let estimatedFirstRunningLatencySeconds = max(
            0,
            dispatchToCompletionSeconds - (Double(response.processNanos) / 1_000_000_000.0)
        )

        guard response.status == 0x01 else {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: sendSeconds,
                processNanos: response.processNanos,
                receiveSeconds: elapsedSeconds(from: receiveStarted, to: clock.now),
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                firstRunningLatencySecondsEstimate: estimatedFirstRunningLatencySeconds,
                slotHealthDownOnFailure: false
            )
        }

        let previewPath = "\(tempDir)/\(UUID().uuidString)-preview.mp4"
        let thumbPath = "\(tempDir)/\(UUID().uuidString)-thumb.jpg"
        defer {
            try? FileManager.default.removeItem(atPath: previewPath)
            try? FileManager.default.removeItem(atPath: thumbPath)
        }

        guard ThunderboltTransport.readToFile(fd: fd, count: response.prevSize, path: previewPath) else {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: sendSeconds,
                processNanos: response.processNanos,
                receiveSeconds: elapsedSeconds(from: receiveStarted, to: clock.now),
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                firstRunningLatencySecondsEstimate: estimatedFirstRunningLatencySeconds,
                slotHealthDownOnFailure: true
            )
        }

        guard ThunderboltTransport.readToFile(fd: fd, count: response.thumbSize, path: thumbPath) else {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: sendSeconds,
                processNanos: response.processNanos,
                receiveSeconds: elapsedSeconds(from: receiveStarted, to: clock.now),
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                firstRunningLatencySecondsEstimate: estimatedFirstRunningLatencySeconds,
                slotHealthDownOnFailure: true
            )
        }
        let payloadReadCompleted = clock.now

        let receiveSeconds = elapsedSeconds(from: receiveStarted, to: clock.now)
        let txOutMS = max(0, elapsedSeconds(from: responseHeaderReceived, to: payloadReadCompleted) * 1_000.0)

        do {
            let previewSHA256 = try SHA256Utility.calculateSHA256(
                path: previewPath,
                bufferSize: sha256BufferSize
            )
            guard previewSHA256.lowercased() == response.prevSHA256.lowercased() else {
                return ThunderboltRawRoundTripResult(
                    success: false,
                    sendSeconds: sendSeconds,
                    processNanos: response.processNanos,
                    receiveSeconds: receiveSeconds,
                    totalSeconds: elapsedSeconds(from: started, to: clock.now),
                    firstRunningLatencySecondsEstimate: estimatedFirstRunningLatencySeconds,
                    txOutMS: txOutMS,
                    publishOverheadMS: max(0, elapsedSeconds(from: payloadReadCompleted, to: clock.now) * 1_000.0),
                    slotHealthDownOnFailure: true
                )
            }

            let thumbSHA256 = try SHA256Utility.calculateSHA256(
                path: thumbPath,
                bufferSize: sha256BufferSize
            )
            guard thumbSHA256.lowercased() == response.thumbSHA256.lowercased() else {
                return ThunderboltRawRoundTripResult(
                    success: false,
                    sendSeconds: sendSeconds,
                    processNanos: response.processNanos,
                    receiveSeconds: receiveSeconds,
                    totalSeconds: elapsedSeconds(from: started, to: clock.now),
                    firstRunningLatencySecondsEstimate: estimatedFirstRunningLatencySeconds,
                    txOutMS: txOutMS,
                    publishOverheadMS: max(0, elapsedSeconds(from: payloadReadCompleted, to: clock.now) * 1_000.0),
                    slotHealthDownOnFailure: true
                )
            }
            let finalizationCompleted = clock.now
            return ThunderboltRawRoundTripResult(
                success: true,
                sendSeconds: sendSeconds,
                processNanos: response.processNanos,
                receiveSeconds: receiveSeconds,
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                firstRunningLatencySecondsEstimate: estimatedFirstRunningLatencySeconds,
                txOutMS: txOutMS,
                publishOverheadMS: max(
                    0,
                    elapsedSeconds(from: payloadReadCompleted, to: finalizationCompleted) * 1_000.0
                )
            )
        } catch {
            return ThunderboltRawRoundTripResult(
                success: false,
                sendSeconds: sendSeconds,
                processNanos: response.processNanos,
                receiveSeconds: receiveSeconds,
                totalSeconds: elapsedSeconds(from: started, to: clock.now),
                firstRunningLatencySecondsEstimate: estimatedFirstRunningLatencySeconds,
                txOutMS: txOutMS,
                publishOverheadMS: max(0, elapsedSeconds(from: payloadReadCompleted, to: clock.now) * 1_000.0),
                slotHealthDownOnFailure: true
            )
        }
    }
}
