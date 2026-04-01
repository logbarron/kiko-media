package enum TransferOverheadEstimator {
    package struct Update: Sendable {
        package let baseline: Double
        package let estimate: Double
    }

    private static let degradeTriggerMultiplier: Double = 1.25
    private static let baselinePriorWeight: Double = 0.90
    private static let degradedEstimatePriorWeight: Double = 0.70
    private static let recoveryEstimatePriorWeight: Double = 0.85

    package static func next(
        previousBaseline: Double?,
        previousEstimate: Double?,
        sample: Double
    ) -> Update {
        guard let previousBaseline else {
            return Update(baseline: sample, estimate: sample)
        }

        let nextBaseline = previousBaseline * baselinePriorWeight + sample * (1 - baselinePriorWeight)
        if sample >= previousBaseline * degradeTriggerMultiplier {
            let estimate = previousEstimate ?? nextBaseline
            return Update(
                baseline: nextBaseline,
                estimate: estimate * degradedEstimatePriorWeight + sample * (1 - degradedEstimatePriorWeight)
            )
        }

        if let previousEstimate {
            return Update(
                baseline: nextBaseline,
                estimate: previousEstimate * recoveryEstimatePriorWeight
                    + nextBaseline * (1 - recoveryEstimatePriorWeight)
            )
        }

        return Update(baseline: nextBaseline, estimate: nextBaseline)
    }

    package static func reduce(
        previousBaseline: Double?,
        previousEstimate: Double?,
        sampleMS: Double?
    ) -> Update? {
        guard let sampleMS,
              sampleMS.isFinite,
              sampleMS >= 0 else {
            return nil
        }
        return next(
            previousBaseline: previousBaseline,
            previousEstimate: previousEstimate,
            sample: sampleMS
        )
    }

    package static func reducePositive(
        previousBaseline: Double?,
        previousEstimate: Double?,
        sampleMS: Double?
    ) -> Update? {
        guard let sampleMS,
              sampleMS.isFinite,
              sampleMS > 0 else {
            return nil
        }
        return next(
            previousBaseline: previousBaseline,
            previousEstimate: previousEstimate,
            sample: sampleMS
        )
    }

    package static func reduce(
        previousBaseline: Double?,
        previousEstimate: Double?,
        sampleMS: UInt32?
    ) -> Update? {
        reduce(
            previousBaseline: previousBaseline,
            previousEstimate: previousEstimate,
            sampleMS: sampleMS.map(Double.init)
        )
    }
}
