import Foundation

package struct ProgressTickV2: Sendable, Equatable {
    package enum Status: UInt8, Sendable {
        case running = 0x01
        case complete = 0x02
        case failed = 0x03
    }

    package enum ErrorClass: UInt8, Sendable {
        case none = 0x00
        case transient = 0x01
        case permanent = 0x02
    }

    package static let version: UInt8 = 0x02
    package static let encodedByteCount = 24

    package let status: Status
    package let jobHandle: UInt32
    package let sessionID: UInt32
    package let errorClass: ErrorClass
    package let progress: Float
    package let elapsedMS: UInt32
    package let estRemainingMS: UInt32

    package init(
        status: Status,
        jobHandle: UInt32,
        sessionID: UInt32,
        errorClass: ErrorClass,
        progress: Float,
        elapsedMS: UInt32,
        estRemainingMS: UInt32
    ) {
        self.status = status
        self.jobHandle = jobHandle
        self.sessionID = sessionID
        self.errorClass = errorClass
        self.progress = progress
        self.elapsedMS = elapsedMS
        self.estRemainingMS = estRemainingMS
    }

    package func encode() -> Data {
        var frame = Data(capacity: Self.encodedByteCount)
        frame.append(Self.version)
        frame.append(status.rawValue)

        var jobHandleBE = jobHandle.bigEndian
        frame.append(Data(bytes: &jobHandleBE, count: 4))

        var sessionIDBE = sessionID.bigEndian
        frame.append(Data(bytes: &sessionIDBE, count: 4))

        frame.append(errorClass.rawValue)
        frame.append(0x00) // reserved

        var progressBitsBE = progress.bitPattern.bigEndian
        frame.append(Data(bytes: &progressBitsBE, count: 4))

        var elapsedMSBE = elapsedMS.bigEndian
        frame.append(Data(bytes: &elapsedMSBE, count: 4))

        var estRemainingMSBE = estRemainingMS.bigEndian
        frame.append(Data(bytes: &estRemainingMSBE, count: 4))

        return frame
    }

    package static func decode(_ data: Data) throws -> ProgressTickV2 {
        guard data.count == Self.encodedByteCount else {
            throw ProgressTickV2DecodeError.invalidLength(data.count)
        }

        let version = data[0]
        guard version == Self.version else {
            throw ProgressTickV2DecodeError.unsupportedVersion(version)
        }

        guard let status = Status(rawValue: data[1]) else {
            throw ProgressTickV2DecodeError.invalidStatus(data[1])
        }

        let jobHandle = data.withUnsafeBytes {
            $0.loadUnaligned(fromByteOffset: 2, as: UInt32.self).bigEndian
        }
        let sessionID = data.withUnsafeBytes {
            $0.loadUnaligned(fromByteOffset: 6, as: UInt32.self).bigEndian
        }
        guard let errorClass = ErrorClass(rawValue: data[10]) else {
            throw ProgressTickV2DecodeError.invalidErrorClass(data[10])
        }
        guard data[11] == 0x00 else {
            throw ProgressTickV2DecodeError.invalidReservedByte(data[11])
        }
        let progressBits = data.withUnsafeBytes {
            $0.loadUnaligned(fromByteOffset: 12, as: UInt32.self).bigEndian
        }
        let progress = Float(bitPattern: progressBits)
        let elapsedMS = data.withUnsafeBytes {
            $0.loadUnaligned(fromByteOffset: 16, as: UInt32.self).bigEndian
        }
        let estRemainingMS = data.withUnsafeBytes {
            $0.loadUnaligned(fromByteOffset: 20, as: UInt32.self).bigEndian
        }

        return ProgressTickV2(
            status: status,
            jobHandle: jobHandle,
            sessionID: sessionID,
            errorClass: errorClass,
            progress: progress,
            elapsedMS: elapsedMS,
            estRemainingMS: estRemainingMS
        )
    }
}

package enum ProgressTickV2DecodeError: Error, Equatable {
    case invalidLength(Int)
    case unsupportedVersion(UInt8)
    case invalidStatus(UInt8)
    case invalidErrorClass(UInt8)
    case invalidReservedByte(UInt8)
}

package enum ProgressTickV2ValidationIssue: Equatable {
    case errorClassStatusMismatch
    case progressOutOfRange
    case progressDecreased
    case unknownJobHandle
    case sessionMismatch
    case terminalRemainingNonZero
}

package enum ProgressTickV2ValidationOutcome: Equatable {
    case valid
    case invalid(ProgressTickV2ValidationIssue)
}

package enum ProgressTickV2Validator {
    package static func validate(
        tick: ProgressTickV2,
        previousProgress: Float?,
        knownJobHandle: UInt32,
        expectedSessionID: UInt32
    ) -> ProgressTickV2ValidationOutcome {
        let failedHasErrorClass = tick.status == .failed && tick.errorClass != .none
        let nonFailedHasNoErrorClass = tick.status != .failed && tick.errorClass == .none
        if !(failedHasErrorClass || nonFailedHasNoErrorClass) {
            return .invalid(.errorClassStatusMismatch)
        }
        if tick.status != .running && tick.estRemainingMS != 0 {
            return .invalid(.terminalRemainingNonZero)
        }
        if !tick.progress.isFinite || tick.progress < 0 || tick.progress > 1 {
            return .invalid(.progressOutOfRange)
        }
        if let previousProgress,
           tick.progress < previousProgress {
            return .invalid(.progressDecreased)
        }
        if tick.jobHandle != knownJobHandle {
            return .invalid(.unknownJobHandle)
        }
        if tick.sessionID != expectedSessionID {
            return .invalid(.sessionMismatch)
        }
        return .valid
    }
}

package enum TickProtocolGate {
    package static func isAccepted(version: UInt8, complexityAwareSchedulingEnabled: Bool) -> Bool {
        if complexityAwareSchedulingEnabled {
            return version == ProgressTickV2.version
        }
        return version == 1 || version == ProgressTickV2.version
    }

    package static func isAccepted(version: Int?, complexityAwareSchedulingEnabled: Bool) -> Bool {
        guard let version,
              let version = UInt8(exactly: version) else {
            return false
        }
        return isAccepted(version: version, complexityAwareSchedulingEnabled: complexityAwareSchedulingEnabled)
    }
}
