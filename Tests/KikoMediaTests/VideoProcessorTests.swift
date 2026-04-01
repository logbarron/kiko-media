import Testing
import Foundation
@testable import KikoMediaCore

@Suite("Video Processing")
struct VideoProcessorTests {
    private enum ExportFailure: Error {
        case failedAfterTimeoutCancel
        case failedLongAfterTimeoutCancel
    }

    private final class CancelProbe: @unchecked Sendable {
        private let lock = NSLock()
        private var cancelled = false

        func markCancelled() {
            lock.lock()
            cancelled = true
            lock.unlock()
        }

        func isCancelled() -> Bool {
            lock.lock()
            defer { lock.unlock() }
            return cancelled
        }
    }

    @Test("Timeout explicitly cancels non-cooperative export work")
    func timeoutCancelsNonCooperativeExport() async {
        let probe = CancelProbe()

        do {
            try await VideoProcessor.runExportWithTimeout(
                timeoutSeconds: 0,
                export: {
                    let deadline = Date().addingTimeInterval(1.2)
                    while Date() < deadline {
                        if probe.isCancelled() {
                            return
                        }
                        // Ignore cooperative task cancellation to simulate AV export APIs.
                        try? await Task.sleep(for: .milliseconds(10))
                    }
                },
                cancelExport: {
                    probe.markCancelled()
                }
            )
            Issue.record("Expected timeout")
        } catch VideoProcessorError.timeout {
            // Expected.
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(probe.isCancelled())
    }

    @Test("Timeout preserves export failure that arrives immediately after cancel")
    func timeoutPreservesImmediateExportFailure() async {
        let probe = CancelProbe()

        do {
            try await VideoProcessor.runExportWithTimeout(
                timeoutSeconds: 0,
                export: {
                    while !probe.isCancelled() {
                        // Ignore cooperative task cancellation to simulate AV export APIs.
                        try? await Task.sleep(for: .milliseconds(5))
                    }
                    throw ExportFailure.failedAfterTimeoutCancel
                },
                cancelExport: {
                    probe.markCancelled()
                }
            )
            Issue.record("Expected export failure")
        } catch ExportFailure.failedAfterTimeoutCancel {
            // Expected.
        } catch VideoProcessorError.timeout {
            Issue.record("Expected underlying export error, got timeout")
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Timeout preserves export failure that arrives well after cancel")
    func timeoutPreservesDelayedExportFailure() async {
        let probe = CancelProbe()

        do {
            try await VideoProcessor.runExportWithTimeout(
                timeoutSeconds: 0,
                export: {
                    while !probe.isCancelled() {
                        // Ignore cooperative task cancellation to simulate AV export APIs.
                        try? await Task.sleep(for: .milliseconds(5))
                    }

                    // Simulate export session surfacing a concrete failure after cancellation delay.
                    try? await Task.sleep(for: .milliseconds(500))
                    throw ExportFailure.failedLongAfterTimeoutCancel
                },
                cancelExport: {
                    probe.markCancelled()
                }
            )
            Issue.record("Expected delayed export failure")
        } catch ExportFailure.failedLongAfterTimeoutCancel {
            // Expected.
        } catch VideoProcessorError.timeout {
            Issue.record("Expected delayed underlying export error, got timeout")
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("runtime estimate scale follows hardware-encoder FPS baseline")
    func runtimeEstimateScaleUsesFPSBaseline() {
        #expect(VideoProcessor.runtimeEstimateScaleForHardwareEncoder(fps: 30) == 1.0)
        #expect(VideoProcessor.runtimeEstimateScaleForHardwareEncoder(fps: 60) == 2.0)
        #expect(VideoProcessor.runtimeEstimateScaleForHardwareEncoder(fps: 120) == 2.0)
        #expect(VideoProcessor.runtimeEstimateScaleForHardwareEncoder(fps: 15) == 0.5)
        #expect(VideoProcessor.runtimeEstimateScaleForHardwareEncoder(fps: 0) == 0.8)
    }
}
