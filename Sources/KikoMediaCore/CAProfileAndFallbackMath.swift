import Foundation

package struct CAResolvedVideoCostDerivation: Sendable, Equatable {
    package enum FrameCountSource: Sendable, Equatable {
        case measuredFrameCount
        case contentDurationFallback
        case defaultFallback
    }

    package enum DurationSource: Sendable, Equatable {
        case measuredDuration
        case missing
    }

    package enum RuntimeSource: Sendable, Equatable {
        case probeEstimate
        case estimatedProcessingRuntime
        case modeledFromFrameCount
        case contentDurationFallback
        case defaultFallback
    }

    package let frameCountSource: FrameCountSource
    package let durationSource: DurationSource
    package let runtimeSource: RuntimeSource

    package init(
        frameCountSource: FrameCountSource,
        durationSource: DurationSource,
        runtimeSource: RuntimeSource
    ) {
        self.frameCountSource = frameCountSource
        self.durationSource = durationSource
        self.runtimeSource = runtimeSource
    }
}

package struct CAResolvedVideoCost: Sendable, Equatable {
    package let frameCount: Double
    package let durationSeconds: Double?
    package let runtimeSeconds: Double
    package let confidence: EstimateConfidence
    package let derivation: CAResolvedVideoCostDerivation

    package init(
        frameCount: Double,
        durationSeconds: Double?,
        runtimeSeconds: Double,
        confidence: EstimateConfidence,
        derivation: CAResolvedVideoCostDerivation
    ) {
        self.frameCount = frameCount
        self.durationSeconds = durationSeconds
        self.runtimeSeconds = runtimeSeconds
        self.confidence = confidence
        self.derivation = derivation
    }
}

package struct CALocalPriorProfileShaping: Sendable, Equatable {
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let degradationCurve: [CADegradationPoint]
    package let usedFallbackMSPerFrameC1: Bool
    package let usedFallbackDegradationCurve: Bool
}

package struct CAValidatedPriorProfile: Sendable, Equatable {
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let degradationCurve: [CADegradationPoint]
}

package enum CAProfileAndFallbackMath {
    package static let denseDegradationRepairRuleName = "monotone-interpolation-with-upward-clamp"
    private static let defaultDurationSeconds: Double = 60.0
    private static let minimumPositiveSeconds: Double = 0.001
    private static let flatC1DegradationCurve = [CADegradationPoint(concurrency: 1, ratioToC1: 1.0)]

    package static func validMSPerFrameC1(_ value: Double?) -> Double? {
        guard let value,
              value.isFinite,
              value > 0 else {
            return nil
        }
        return value
    }

    package static func resolvedFixedOverheadMS(_ value: Double?) -> Double {
        guard let value, value.isFinite, value > 0 else {
            return 0
        }
        return value
    }

    package static func resolvedAvgCorpusFrameCount(_ value: Double?) -> Double {
        guard let value, value.isFinite, value > 0 else {
            return 0
        }
        return value
    }

    package static func degradationCurve(from curveByConcurrency: [Int: Double]?) -> [CADegradationPoint] {
        guard let curveByConcurrency else { return [] }
        let rawCurve = curveByConcurrency.map { concurrency, ratioToC1 in
            CADegradationPoint(
                concurrency: concurrency,
                ratioToC1: ratioToC1
            )
        }
        return repairedDenseDegradationCurve(from: rawCurve)
    }

    package static func degradationCurve(from machine: BenchmarkPriorMachine?) -> [CADegradationPoint] {
        guard let machine else { return [] }
        return degradationCurve(from: machine.cells)
    }

    package static func degradationCurve(from cells: [BenchmarkPriorCell]?) -> [CADegradationPoint] {
        guard let cells else { return [] }
        let rawCurve = cells.map { cell in
            CADegradationPoint(
                concurrency: cell.concurrency,
                ratioToC1: cell.degradationRatio
            )
        }
        return repairedDenseDegradationCurve(from: rawCurve)
    }

    package static func localPriorProfileShaping(
        from machine: BenchmarkPriorMachine?,
        fallbackMSPerFrameC1: Double = 1.0
    ) -> CALocalPriorProfileShaping {
        let resolvedCurve = degradationCurve(from: machine)
        let resolvedMSPerFrameC1 = validMSPerFrameC1(machine?.msPerFrameC1) ?? fallbackMSPerFrameC1
        return CALocalPriorProfileShaping(
            msPerFrameC1: resolvedMSPerFrameC1,
            fixedOverheadMS: resolvedFixedOverheadMS(machine?.fixedOverheadMS),
            degradationCurve: resolvedCurve.isEmpty ? flatC1DegradationCurve : resolvedCurve,
            usedFallbackMSPerFrameC1: validMSPerFrameC1(machine?.msPerFrameC1) == nil,
            usedFallbackDegradationCurve: resolvedCurve.isEmpty
        )
    }

    package static func validatedPriorProfile(from machine: BenchmarkPriorMachine?) -> CAValidatedPriorProfile? {
        guard let affineModel = resolvedRemoteAffineModel(from: machine) else {
            return nil
        }
        let curve = degradationCurve(from: machine)
        guard !curve.isEmpty else { return nil }
        return CAValidatedPriorProfile(
            msPerFrameC1: affineModel.msPerFrameC1,
            fixedOverheadMS: affineModel.fixedOverheadMS,
            degradationCurve: curve
        )
    }

    package static func explicitRemoteAffineModel(
        from machine: BenchmarkPriorMachine?
    ) -> BenchmarkPriorAffineModel? {
        guard let machine,
              let msPerFrameC1 = validMSPerFrameC1(machine.msPerFrameC1) else {
            return nil
        }
        return BenchmarkPriorAffineModel(
            msPerFrameC1: msPerFrameC1,
            fixedOverheadMS: resolvedFixedOverheadMS(machine.fixedOverheadMS),
            avgCorpusFrameCount: resolvedAvgCorpusFrameCount(machine.avgCorpusFrameCount),
            source: machine.affineModelSource
        )
    }

    package static func resolvedRemoteAffineModel(
        from machine: BenchmarkPriorMachine?
    ) -> BenchmarkPriorAffineModel? {
        guard let machine,
              let explicitModel = explicitRemoteAffineModel(from: machine) else {
            return nil
        }
        guard explicitModel.source == .legacyHeuristic else {
            return explicitModel
        }
        let c1P50MS = machine.cells.first(where: { $0.concurrency == 1 }).map { Double($0.msPerVideoP50) }
        guard let adjusted = adjustedRemotePriorEstimates(
            msPerFrameC1: machine.msPerFrameC1,
            fixedOverheadMS: machine.fixedOverheadMS,
            avgCorpusFrameCount: machine.avgCorpusFrameCount,
            c1P50MS: c1P50MS
        ) else {
            return nil
        }
        return BenchmarkPriorAffineModel(
            msPerFrameC1: adjusted.msPerFrameC1,
            fixedOverheadMS: adjusted.fixedOverheadMS,
            avgCorpusFrameCount: explicitModel.avgCorpusFrameCount,
            source: .legacyHeuristic
        )
    }

    package static func requiresRemoteAffineMigration(
        _ machine: BenchmarkPriorMachine?
    ) -> Bool {
        machine?.usesLegacyAffineHeuristic == true
    }

    package static func adjustedRemotePriorEstimates(
        msPerFrameC1 rawMS: Double?,
        fixedOverheadMS rawFixed: Double?,
        avgCorpusFrameCount: Double?,
        c1P50MS: Double?
    ) -> (msPerFrameC1: Double, fixedOverheadMS: Double)? {
        guard let ms = validMSPerFrameC1(rawMS) else { return nil }
        let fixed = resolvedFixedOverheadMS(rawFixed)
        guard fixed <= 0,
              let avgFrames = avgCorpusFrameCount,
              avgFrames > 100,
              let c1P50 = c1P50MS,
              c1P50 > 0 else {
            return (ms, fixed)
        }
        let impliedTotal = ms * avgFrames
        let ratio = impliedTotal / c1P50
        guard ratio > 0.95, ratio < 1.05 else {
            return (ms, fixed)
        }
        let c1P50Seconds = c1P50 / 1000.0
        let extraReduction = min(0.15, max(0, c1P50Seconds - 0.5) * 0.05)
        let adjustedMS = max(0.001, ms * max(0.55, 0.75 - extraReduction))
        return (adjustedMS, fixed)
    }

    package static func priorProfile(
        forSignature signature: String,
        in priorTable: BenchmarkPriorTable
    ) -> CAValidatedPriorProfile? {
        let machine = priorTable.machines.first { machine in
            machine.signature == signature
        }
        return validatedPriorProfile(from: machine)
    }

    package static func conservativeComparableDegradationCurve(
        from curve: [CADegradationPoint]
    ) -> [CADegradationPoint] {
        let repairedCurve = repairedDenseDegradationCurve(from: curve)
        guard !repairedCurve.isEmpty else { return flatC1DegradationCurve }
        let maxConcurrency = max(repairedCurve.map(\.concurrency).max() ?? 1, 1)
        let c1Ratio = repairedCurve.first(where: { $0.concurrency == 1 })?.ratioToC1 ?? 1.0
        let mostConservativeRatio = max(
            repairedCurve.map(\.ratioToC1).max() ?? c1Ratio,
            c1Ratio
        )
        return (1...maxConcurrency).map { concurrency in
            CADegradationPoint(
                concurrency: concurrency,
                ratioToC1: concurrency == 1 ? c1Ratio : mostConservativeRatio
            )
        }
    }

    package static func repairedDenseDegradationCurve(
        from curve: [CADegradationPoint]
    ) -> [CADegradationPoint] {
        let sanitizedCurve = sanitizedDegradationCurve(from: curve)
        guard !sanitizedCurve.isEmpty else { return [] }

        var knownRatiosByConcurrency: [Int: Double] = [:]
        knownRatiosByConcurrency.reserveCapacity(sanitizedCurve.count + 1)
        for point in sanitizedCurve {
            let conservativeRatio = max(1.0, point.ratioToC1)
            if let existing = knownRatiosByConcurrency[point.concurrency] {
                knownRatiosByConcurrency[point.concurrency] = max(existing, conservativeRatio)
            } else {
                knownRatiosByConcurrency[point.concurrency] = conservativeRatio
            }
        }
        if knownRatiosByConcurrency[1] == nil {
            knownRatiosByConcurrency[1] = 1.0
        }

        let sortedKnownConcurrencies = knownRatiosByConcurrency.keys.sorted()
        var monotoneKnownRatiosByConcurrency: [Int: Double] = [:]
        monotoneKnownRatiosByConcurrency.reserveCapacity(sortedKnownConcurrencies.count)
        var previousKnownRatio = 1.0
        for concurrency in sortedKnownConcurrencies {
            let monotoneRatio = max(previousKnownRatio, knownRatiosByConcurrency[concurrency] ?? previousKnownRatio)
            monotoneKnownRatiosByConcurrency[concurrency] = monotoneRatio
            previousKnownRatio = monotoneRatio
        }

        let knownPoints = sortedKnownConcurrencies.compactMap { concurrency in
            monotoneKnownRatiosByConcurrency[concurrency].map {
                CADegradationPoint(concurrency: concurrency, ratioToC1: $0)
            }
        }
        let maxConcurrency = max(knownPoints.last?.concurrency ?? 1, 1)

        var repairedCurve: [CADegradationPoint] = []
        repairedCurve.reserveCapacity(maxConcurrency)
        for concurrency in 1...maxConcurrency {
            let repairedRatio: Double
            if let exactRatio = monotoneKnownRatiosByConcurrency[concurrency] {
                repairedRatio = exactRatio
            } else if let lower = knownPoints.last(where: { $0.concurrency < concurrency }),
                      let upper = knownPoints.first(where: { $0.concurrency > concurrency }) {
                let span = Double(upper.concurrency - lower.concurrency)
                let offset = Double(concurrency - lower.concurrency)
                let interpolatedRatio = lower.ratioToC1
                    + ((upper.ratioToC1 - lower.ratioToC1) * (offset / span))
                repairedRatio = max(lower.ratioToC1, interpolatedRatio)
            } else if let lower = knownPoints.last(where: { $0.concurrency < concurrency }) {
                repairedRatio = lower.ratioToC1
            } else if let upper = knownPoints.first(where: { $0.concurrency > concurrency }) {
                repairedRatio = upper.ratioToC1
            } else {
                repairedRatio = 1.0
            }

            let monotoneRatio = max(repairedCurve.last?.ratioToC1 ?? 1.0, repairedRatio)
            repairedCurve.append(
                CADegradationPoint(
                    concurrency: concurrency,
                    ratioToC1: monotoneRatio
                )
            )
        }
        return repairedCurve
    }

    package static func resolvedDegradation(
        from curve: [CADegradationPoint],
        concurrency: Int
    ) -> (clampedConcurrency: Int, factor: Double) {
        let repairedCurve = repairedDenseDegradationCurve(from: curve)
        guard !repairedCurve.isEmpty else { return (1, 1.0) }

        let clampedConcurrency = min(max(concurrency, 1), repairedCurve.last?.concurrency ?? 1)
        guard repairedCurve.indices.contains(clampedConcurrency - 1) else {
            return (clampedConcurrency, repairedCurve.last?.ratioToC1 ?? 1.0)
        }
        return (
            clampedConcurrency,
            repairedCurve[clampedConcurrency - 1].ratioToC1
        )
    }

    package static func seedRuntimeSeconds(
        priorTable: BenchmarkPriorTable,
        localSignature: String,
        localConcurrency: Int
    ) -> Double? {
        if let exact = priorTable.lookup(signature: localSignature, concurrency: localConcurrency) {
            return max(minimumPositiveSeconds, Double(exact.msPerVideoP50) / 1_000.0)
        }
        if let single = priorTable.lookup(signature: localSignature, concurrency: 1) {
            return max(minimumPositiveSeconds, Double(single.msPerVideoP50) / 1_000.0)
        }
        return nil
    }

    package static func fallbackFrameCount(
        durationSeconds: Double?,
        frameCount: Double?
    ) -> Double {
        let duration = if let durationSeconds,
            durationSeconds.isFinite,
            durationSeconds > 0 {
            durationSeconds
        } else {
            defaultDurationSeconds
        }
        let nominalFrameRate: Double? = if let frameCount,
            frameCount.isFinite,
            frameCount > 0,
            duration > 0 {
            frameCount / duration
        } else {
            nil
        }
        return ComplexityAwareScheduler.frameCount(
            durationSeconds: duration,
            nominalFrameRate: nominalFrameRate
        )
    }

    package static func resolveVideoCost(
        frameCount: Double?,
        durationSeconds: Double?,
        runtimeSeconds explicitRuntimeSeconds: Double?,
        confidence: EstimateConfidence,
        runtimeSourceWhenPresent: CAResolvedVideoCostDerivation.RuntimeSource,
        localMSPerFrameC1: Double?,
        localFixedOverheadMS: Double = 0
    ) -> CAResolvedVideoCost {
        let resolvedDurationSeconds: Double? = if let durationSeconds,
            durationSeconds.isFinite,
            durationSeconds > 0 {
            durationSeconds
        } else {
            nil
        }
        let durationSource: CAResolvedVideoCostDerivation.DurationSource = if resolvedDurationSeconds != nil {
            .measuredDuration
        } else {
            .missing
        }

        let resolvedFrameCount: Double
        let frameCountSource: CAResolvedVideoCostDerivation.FrameCountSource
        if let frameCount,
           frameCount.isFinite,
           frameCount > 0 {
            resolvedFrameCount = max(1, frameCount)
            frameCountSource = .measuredFrameCount
        } else if let resolvedDurationSeconds {
            resolvedFrameCount = fallbackFrameCount(durationSeconds: resolvedDurationSeconds, frameCount: nil)
            frameCountSource = .contentDurationFallback
        } else {
            resolvedFrameCount = fallbackFrameCount(durationSeconds: nil, frameCount: nil)
            frameCountSource = .defaultFallback
        }

        let resolvedRuntimeSeconds: Double
        let runtimeSource: CAResolvedVideoCostDerivation.RuntimeSource
        if let explicitRuntimeSeconds,
           explicitRuntimeSeconds.isFinite,
           explicitRuntimeSeconds > 0 {
            resolvedRuntimeSeconds = max(minimumPositiveSeconds, explicitRuntimeSeconds)
            runtimeSource = runtimeSourceWhenPresent
        } else if let modeledRuntimeSeconds = runtimeSeconds(
            frameCount: resolvedFrameCount,
            localMSPerFrameC1: localMSPerFrameC1,
            localFixedOverheadMS: localFixedOverheadMS
        ) {
            resolvedRuntimeSeconds = modeledRuntimeSeconds
            runtimeSource = .modeledFromFrameCount
        } else if let resolvedDurationSeconds {
            resolvedRuntimeSeconds = max(minimumPositiveSeconds, resolvedDurationSeconds * 2.0)
            runtimeSource = .contentDurationFallback
        } else {
            resolvedRuntimeSeconds = max(minimumPositiveSeconds, defaultDurationSeconds * 2.0)
            runtimeSource = .defaultFallback
        }

        return CAResolvedVideoCost(
            frameCount: resolvedFrameCount,
            durationSeconds: resolvedDurationSeconds,
            runtimeSeconds: resolvedRuntimeSeconds,
            confidence: confidence,
            derivation: CAResolvedVideoCostDerivation(
                frameCountSource: frameCountSource,
                durationSource: durationSource,
                runtimeSource: runtimeSource
            )
        )
    }

    package static func runtimeSeconds(
        frameCount: Double,
        localMSPerFrameC1: Double?,
        localFixedOverheadMS: Double = 0,
        degradationFactor: Double = 1.0
    ) -> Double? {
        guard frameCount.isFinite,
              frameCount > 0,
              let localMSPerFrameC1 = validMSPerFrameC1(localMSPerFrameC1),
              degradationFactor.isFinite,
              degradationFactor > 0 else {
            return nil
        }

        let runtimeSeconds = (
            resolvedFixedOverheadMS(localFixedOverheadMS) +
            (frameCount * localMSPerFrameC1 * degradationFactor)
        ) / 1_000.0
        guard runtimeSeconds.isFinite, runtimeSeconds > 0 else {
            return nil
        }
        return max(minimumPositiveSeconds, runtimeSeconds)
    }

    package static func runtimeSecondsFallback(
        durationSeconds: Double?,
        frameCount: Double?,
        localMSPerFrameC1: Double?,
        localFixedOverheadMS: Double = 0
    ) -> Double {
        resolveVideoCost(
            frameCount: frameCount,
            durationSeconds: durationSeconds,
            runtimeSeconds: nil,
            confidence: .low,
            runtimeSourceWhenPresent: .estimatedProcessingRuntime,
            localMSPerFrameC1: localMSPerFrameC1,
            localFixedOverheadMS: localFixedOverheadMS
        ).runtimeSeconds
    }

    package static func seededFallbackFrameCount(
        seedRuntimeSeconds: Double?,
        localMSPerFrameC1: Double,
        localFixedOverheadMS: Double = 0
    ) -> Double {
        guard let seedRuntimeSeconds,
              seedRuntimeSeconds.isFinite,
              seedRuntimeSeconds > 0,
              let localMSPerFrameC1 = validMSPerFrameC1(localMSPerFrameC1) else {
            return fallbackFrameCount(durationSeconds: nil, frameCount: nil)
        }

        let seedRuntimeMS = seedRuntimeSeconds * 1_000.0
        let variableRuntimeMS = max(1, seedRuntimeMS - resolvedFixedOverheadMS(localFixedOverheadMS))
        return max(1, variableRuntimeMS / localMSPerFrameC1)
    }

    private static func sanitizedDegradationCurve(
        from curve: [CADegradationPoint]
    ) -> [CADegradationPoint] {
        curve.compactMap { point in
            guard point.concurrency > 0,
                  point.ratioToC1.isFinite,
                  point.ratioToC1 > 0 else {
                return nil
            }
            return point
        }
        .sorted { lhs, rhs in
            lhs.concurrency < rhs.concurrency
        }
    }
}
