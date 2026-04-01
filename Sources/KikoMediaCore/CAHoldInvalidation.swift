import Foundation

package enum CAHoldInvalidation {
    package static let targetReadyAtDriftThresholdMS: Double = 250

    package enum Reason: Sendable, Equatable {
        case targetSlotDown
        case targetReadyAtDrifted
        case targetSlotImpossible
    }

    package static func invalidationReason(
        baselineReadyAtMS: Double,
        currentReadyAtMS: Double?,
        slotIsDown: Bool,
        targetStillPossible: Bool
    ) -> Reason? {
        guard targetStillPossible else {
            return .targetSlotImpossible
        }
        guard let currentReadyAtMS else {
            return .targetSlotImpossible
        }
        guard !slotIsDown else {
            return .targetSlotDown
        }
        if abs(currentReadyAtMS - baselineReadyAtMS) >= targetReadyAtDriftThresholdMS {
            return .targetReadyAtDrifted
        }
        return nil
    }
}
