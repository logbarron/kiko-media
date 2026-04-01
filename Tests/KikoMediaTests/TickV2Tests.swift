import Foundation
import Testing
@testable import KikoMediaCore

@Suite("Tick v2")
struct TickV2Tests {
    @Test("encodes exactly 24 bytes in big-endian layout")
    func encodesExactLayout() throws {
        let tick = ProgressTickV2(
            status: .running,
            jobHandle: 0x11223344,
            sessionID: 0x55667788,
            errorClass: .none,
            progress: 0.5,
            elapsedMS: 0x01020304,
            estRemainingMS: 0xAABBCCDD
        )

        let frame = tick.encode()
        #expect(frame == Data([
            ProgressTickV2.version,
            ProgressTickV2.Status.running.rawValue,
            0x11, 0x22, 0x33, 0x44,
            0x55, 0x66, 0x77, 0x88,
            ProgressTickV2.ErrorClass.none.rawValue,
            0x00,
            0x3F, 0x00, 0x00, 0x00,
            0x01, 0x02, 0x03, 0x04,
            0xAA, 0xBB, 0xCC, 0xDD,
        ]))
    }

    @Test("decodes encoded v2 frame")
    func decodeRoundTrip() throws {
        let tick = ProgressTickV2(
            status: .complete,
            jobHandle: 17,
            sessionID: 99,
            errorClass: .none,
            progress: 1.0,
            elapsedMS: 4_000,
            estRemainingMS: 0
        )
        let decoded = try ProgressTickV2.decode(tick.encode())
        #expect(decoded == tick)
    }

    @Test("decode rejects non-zero reserved byte")
    func decodeRejectsNonZeroReservedByte() {
        let tick = ProgressTickV2(
            status: .running,
            jobHandle: 3,
            sessionID: 4,
            errorClass: .none,
            progress: 0.25,
            elapsedMS: 50,
            estRemainingMS: 950
        )
        var frame = tick.encode()
        frame[11] = 0x7F

        do {
            _ = try ProgressTickV2.decode(frame)
            Issue.record("Expected decode failure for non-zero reserved byte")
        } catch let error as ProgressTickV2DecodeError {
            #expect(error == .invalidReservedByte(0x7F))
        } catch {
            Issue.record("Unexpected decode error type: \(error)")
        }
    }

    @Test("validator rejects non-failed status with non-none error class")
    func rejectsInvalidErrorClassForStatus() {
        let tick = ProgressTickV2(
            status: .running,
            jobHandle: 1,
            sessionID: 2,
            errorClass: .transient,
            progress: 0.2,
            elapsedMS: 100,
            estRemainingMS: 900
        )
        let outcome = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: nil,
            knownJobHandle: 1,
            expectedSessionID: 2
        )
        #expect(outcome == .invalid(.errorClassStatusMismatch))
    }

    @Test("validator rejects failed status with none error class")
    func rejectsFailedWithNoneErrorClass() {
        let tick = ProgressTickV2(
            status: .failed,
            jobHandle: 1,
            sessionID: 2,
            errorClass: .none,
            progress: 0.2,
            elapsedMS: 100,
            estRemainingMS: 0
        )
        let outcome = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: nil,
            knownJobHandle: 1,
            expectedSessionID: 2
        )
        #expect(outcome == .invalid(.errorClassStatusMismatch))
    }

    @Test("validator rejects progress outside 0...1")
    func rejectsProgressOutOfRange() {
        let tick = ProgressTickV2(
            status: .running,
            jobHandle: 1,
            sessionID: 2,
            errorClass: .none,
            progress: 1.2,
            elapsedMS: 100,
            estRemainingMS: 900
        )
        let outcome = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: nil,
            knownJobHandle: 1,
            expectedSessionID: 2
        )
        #expect(outcome == .invalid(.progressOutOfRange))
    }

    @Test("validator rejects progress decrease for same stream")
    func rejectsProgressDecrease() {
        let tick = ProgressTickV2(
            status: .running,
            jobHandle: 1,
            sessionID: 2,
            errorClass: .none,
            progress: 0.4,
            elapsedMS: 200,
            estRemainingMS: 800
        )
        let outcome = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: 0.5,
            knownJobHandle: 1,
            expectedSessionID: 2
        )
        #expect(outcome == .invalid(.progressDecreased))
    }

    @Test("validator rejects progress decrease for failed tick on same stream")
    func rejectsFailedProgressBelowPreviousRunningProgress() {
        let tick = ProgressTickV2(
            status: .failed,
            jobHandle: 1,
            sessionID: 2,
            errorClass: .transient,
            progress: 0.4,
            elapsedMS: 200,
            estRemainingMS: 0
        )
        let outcome = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: 0.5,
            knownJobHandle: 1,
            expectedSessionID: 2
        )
        #expect(outcome == .invalid(.progressDecreased))
    }

    @Test("validator rejects progress decrease for complete tick on same stream")
    func rejectsCompleteProgressBelowPreviousRunningProgress() {
        let tick = ProgressTickV2(
            status: .complete,
            jobHandle: 1,
            sessionID: 2,
            errorClass: .none,
            progress: 0.4,
            elapsedMS: 200,
            estRemainingMS: 0
        )
        let outcome = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: 0.5,
            knownJobHandle: 1,
            expectedSessionID: 2
        )
        #expect(outcome == .invalid(.progressDecreased))
    }

    @Test("validator rejects unknown job handle and session mismatch")
    func rejectsUnknownHandleAndSessionMismatch() {
        let tick = ProgressTickV2(
            status: .running,
            jobHandle: 77,
            sessionID: 6,
            errorClass: .none,
            progress: 0.2,
            elapsedMS: 100,
            estRemainingMS: 900
        )
        let unknownHandle = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: nil,
            knownJobHandle: 78,
            expectedSessionID: 6
        )
        #expect(unknownHandle == .invalid(.unknownJobHandle))

        let badSession = ProgressTickV2Validator.validate(
            tick: tick,
            previousProgress: nil,
            knownJobHandle: 77,
            expectedSessionID: 9
        )
        #expect(badSession == .invalid(.sessionMismatch))
    }

    @Test("validator rejects non-zero remaining estimate for terminal statuses")
    func rejectsTerminalRemainingEstimateWhenNonZero() {
        let completeTick = ProgressTickV2(
            status: .complete,
            jobHandle: 7,
            sessionID: 8,
            errorClass: .none,
            progress: 1.0,
            elapsedMS: 500,
            estRemainingMS: 1
        )
        let completeOutcome = ProgressTickV2Validator.validate(
            tick: completeTick,
            previousProgress: 0.9,
            knownJobHandle: 7,
            expectedSessionID: 8
        )
        #expect(completeOutcome == .invalid(.terminalRemainingNonZero))

        let failedTick = ProgressTickV2(
            status: .failed,
            jobHandle: 7,
            sessionID: 8,
            errorClass: .transient,
            progress: 0.9,
            elapsedMS: 500,
            estRemainingMS: 12
        )
        let failedOutcome = ProgressTickV2Validator.validate(
            tick: failedTick,
            previousProgress: 0.9,
            knownJobHandle: 7,
            expectedSessionID: 8
        )
        #expect(failedOutcome == .invalid(.terminalRemainingNonZero))
    }

    @Test("job handle allocator does not reuse handles after exhaustion")
    func jobHandleAllocatorDoesNotReuseAfterExhaustion() async {
        let dispatcher = ThunderboltDispatcher(
            workers: [.init(host: "127.0.0.1", slots: 1)],
            port: 6550,
            connectTimeout: 500,
            thumbsDir: "/tmp",
            previewsDir: "/tmp",
            sha256BufferSize: 4096,
            complexityAwareSchedulingEnabled: true,
            bridgeSourcesOverride: []
        )

        await dispatcher.testHookSetNextJobHandle(UInt32.max)
        #expect(await dispatcher.testHookAllocateJobHandle() == UInt32.max)
        #expect(await dispatcher.testHookAllocateJobHandle() == nil)
        #expect(await dispatcher.testHookAllocateJobHandle() == nil)
        await dispatcher.shutdown()
    }

    @Test("strict rollout gate accepts only v2 when complexity-aware scheduling is enabled")
    func strictGateBehavior() {
        #expect(TickProtocolGate.isAccepted(version: 1, complexityAwareSchedulingEnabled: false))
        #expect(TickProtocolGate.isAccepted(version: 2, complexityAwareSchedulingEnabled: false))
        #expect(!TickProtocolGate.isAccepted(version: 1, complexityAwareSchedulingEnabled: true))
        #expect(TickProtocolGate.isAccepted(version: 2, complexityAwareSchedulingEnabled: true))
    }

    @Test("tick stream staleness boundary is strict greater-than 1000ms")
    func stalenessBoundaryIsStrictGreaterThanOneSecond() {
        #expect(!ThunderboltDispatcher.isTickStreamStale(elapsedMS: 1_000))
        #expect(ThunderboltDispatcher.isTickStreamStale(elapsedMS: 1_001))
    }

    @Test("tick read timeout is configured above 1000ms boundary")
    func tickReadTimeoutUsesStrictBoundaryBuffer() {
        #expect(ThunderboltDispatcher.slotTickStalenessTimeoutMS > 1_000)
    }
}
