package enum LiveAdaptiveMSPerFrameC1Estimator {
    package struct Update: Sendable {
        package let estimate: Double
        package let smoothedError: Double
        package let smoothedAbsoluteError: Double
    }

    private static let gamma: Double = 0.15
    private static let epsilon: Double = 0.001
    private static let alphaMin: Double = 0.05
    private static let alphaMax: Double = 1.0

    package static func next(
        previousEstimate: Double?,
        previousSmoothedError: Double?,
        previousSmoothedAbsoluteError: Double?,
        initialEstimate: Double?,
        observed: Double
    ) -> Update? {
        guard observed.isFinite, observed > 0 else { return nil }

        let estimate0 = if let previousEstimate,
                           previousEstimate.isFinite,
                           previousEstimate > 0 {
            previousEstimate
        } else if let initialEstimate,
                  initialEstimate.isFinite,
                  initialEstimate > 0 {
            initialEstimate
        } else {
            observed
        }

        if let previousEstimate,
           previousEstimate.isFinite,
           previousEstimate > 0,
           let previousSmoothedError,
           let previousSmoothedAbsoluteError,
           previousSmoothedError.isFinite,
           previousSmoothedAbsoluteError.isFinite,
           previousSmoothedAbsoluteError >= 0 {
            let error = observed - previousEstimate
            let smoothedError = (gamma * error) + ((1 - gamma) * previousSmoothedError)
            let smoothedAbsoluteError =
                (gamma * abs(error)) + ((1 - gamma) * previousSmoothedAbsoluteError)
            let alpha = adaptiveAlpha(
                smoothedError: smoothedError,
                smoothedAbsoluteError: smoothedAbsoluteError
            )
            return Update(
                estimate: previousEstimate + (alpha * error),
                smoothedError: smoothedError,
                smoothedAbsoluteError: smoothedAbsoluteError
            )
        }

        let error = observed - estimate0
        let smoothedError = error
        let smoothedAbsoluteError = max(abs(error), epsilon)
        let alpha = adaptiveAlpha(
            smoothedError: smoothedError,
            smoothedAbsoluteError: smoothedAbsoluteError
        )
        return Update(
            estimate: estimate0 + (alpha * error),
            smoothedError: smoothedError,
            smoothedAbsoluteError: smoothedAbsoluteError
        )
    }

    private static func adaptiveAlpha(
        smoothedError: Double,
        smoothedAbsoluteError: Double
    ) -> Double {
        let denominator = max(smoothedAbsoluteError, epsilon)
        let ratio = abs(smoothedError / denominator)
        return min(max(ratio, alphaMin), alphaMax)
    }
}
