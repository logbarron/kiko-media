import Foundation

package struct CASuccessfulExecutionSampleModel: Sendable, Equatable {
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let degradationCurve: [CADegradationPoint]

    package init(
        msPerFrameC1: Double,
        fixedOverheadMS: Double = 0,
        degradationCurve: [CADegradationPoint]
    ) {
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.degradationCurve = degradationCurve
    }
}

package enum ThunderboltAdaptiveTelemetryReducer {
    package struct TailUpdate: Sendable {
        package let txOutEstimateMS: Double?
        package let publishOverheadEstimateMS: Double?
    }

    private static let tailTelemetryEMAPriorWeight: Double = 0.80
    private static let tailTelemetrySampleCeilingMS: Double = 120_000

    package static func variableRuntimeMS(
        processNanos: UInt64,
        fixedOverheadMS: Double
    ) -> Double? {
        let observedRuntimeMS = Double(processNanos) / 1_000_000.0
        guard observedRuntimeMS.isFinite, observedRuntimeMS > 0 else { return nil }

        let fixedOverheadMS = max(0, fixedOverheadMS)
        let variableRuntimeMS = observedRuntimeMS - fixedOverheadMS
        guard variableRuntimeMS.isFinite, variableRuntimeMS > 0 else { return nil }
        return variableRuntimeMS
    }

    package static func normalizedMSPerFrameC1(
        processNanos: UInt64,
        frameCount: Double,
        model: CASuccessfulExecutionSampleModel,
        concurrency: Int
    ) -> Double? {
        guard frameCount.isFinite, frameCount > 0 else { return nil }
        guard let variableRuntimeMS = variableRuntimeMS(
            processNanos: processNanos,
            fixedOverheadMS: model.fixedOverheadMS
        ) else {
            return nil
        }

        let observedMSPerFrame = variableRuntimeMS / frameCount
        guard observedMSPerFrame.isFinite, observedMSPerFrame > 0 else {
            return nil
        }

        let degradation = CAProfileAndFallbackMath.resolvedDegradation(
            from: model.degradationCurve,
            concurrency: concurrency
        )
        guard degradation.factor.isFinite, degradation.factor > 0 else {
            return nil
        }

        let normalizedMSPerFrame = observedMSPerFrame / degradation.factor
        guard normalizedMSPerFrame.isFinite, normalizedMSPerFrame > 0 else {
            return nil
        }
        return normalizedMSPerFrame
    }

    package static func nextTailUpdate(
        previousTxOutEstimateMS: Double?,
        previousPublishOverheadEstimateMS: Double?,
        txOutSampleMS: Double?,
        publishOverheadSampleMS: Double?
    ) -> TailUpdate {
        TailUpdate(
            txOutEstimateMS: nextTailTelemetryEMA(
                previous: previousTxOutEstimateMS,
                sampleMS: txOutSampleMS
            ) ?? previousTxOutEstimateMS,
            publishOverheadEstimateMS: nextTailTelemetryEMA(
                previous: previousPublishOverheadEstimateMS,
                sampleMS: publishOverheadSampleMS
            ) ?? previousPublishOverheadEstimateMS
        )
    }

    private static func clampTailTelemetrySampleMS(_ sampleMS: Double?) -> Double? {
        guard let sampleMS, sampleMS.isFinite else { return nil }
        return min(max(0, sampleMS), tailTelemetrySampleCeilingMS)
    }

    private static func nextTailTelemetryEMA(previous: Double?, sampleMS: Double?) -> Double? {
        guard let sample = clampTailTelemetrySampleMS(sampleMS) else { return nil }

        let next: Double
        if let previous, previous.isFinite {
            next = previous * tailTelemetryEMAPriorWeight
                + sample * (1 - tailTelemetryEMAPriorWeight)
        } else {
            next = sample
        }
        return clampTailTelemetrySampleMS(next)
    }
}
