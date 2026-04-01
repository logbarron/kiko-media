import Foundation

// MARK: - Public Types

enum BurstOptimizerError: Error {
    case noValidConfig
}

struct BurstSearchResult: Sendable {
    let bestConfig: [Int]
    let bestTime: Double
    let evaluations: Int
    let spaceSize: Int
    let history: [BurstEvalRecord]
}

struct BurstEvalRecord: Sendable {
    let config: [Int]
    let elapsed: Double
    let phase: String
    let predicted: Double?
    let isNewBest: Bool
}

// MARK: - Optimizer

private let burstOptimizerPenaltyElapsed = 1_000_000.0
private let burstOptimizerCalibrationMinSamples = 4
private let burstOptimizerCalibrationScaleMin = 0.25
private let burstOptimizerCalibrationScaleMax = 4.0
private let burstOptimizerCalibrationPenaltyLimitMultiplier = 3.0

func optimizeBurstConcurrency(
    ceilings: [Int],
    evaluate: @Sendable ([Int]) async throws -> Double,
    numVideos: Int,
    topK: Int = 10,
    onEval: ((_ record: BurstEvalRecord) -> Void)? = nil
) async throws -> BurstSearchResult {
    guard !ceilings.isEmpty, ceilings.allSatisfy({ $0 >= 0 }), ceilings.contains(where: { $0 > 0 }) else {
        throw BurstOptimizerError.noValidConfig
    }

    let ndim = ceilings.count
    let safeNumVideos = max(1, numVideos)
    let spaceSize = burstConfigSpaceSize(ceilings: ceilings)

    var cache: [[Int]: Double] = [:]
    var evalCount = 0
    var bestConfig: [Int]? = nil
    var bestTime = Double.infinity
    var history: [BurstEvalRecord] = []
    var throughputs = Array(repeating: [0.0], count: ndim)

    struct BurstCalibrationSample {
        let rawPrediction: Double
        let totalSlots: Int
        let elapsed: Double
    }

    struct BurstCalibrationModel {
        let scale: Double
        let perExtraSlotPenalty: Double
    }

    struct Phase2Candidate {
        let config: [Int]
        let rawPrediction: Double
    }

    var calibrationSamples: [BurstCalibrationSample] = []
    var phase1CalibratedConfigs: [[Int]] = []

    func unitConfig(dim: Int, slots: Int) -> [Int] {
        var config = [Int](repeating: 0, count: ndim)
        config[dim] = slots
        return config
    }

    func allValidConfigs() -> [[Int]] {
        var results: [[Int]] = []
        var current = [Int](repeating: 0, count: ndim)

        func generate(dim: Int) {
            if dim == ndim {
                if current.reduce(0, +) > 0 {
                    results.append(current)
                }
                return
            }

            for value in 0...ceilings[dim] {
                current[dim] = value
                generate(dim: dim + 1)
            }
        }

        generate(dim: 0)
        return results
    }

    func l1Neighbors(of config: [Int]) -> [[Int]] {
        var neighbors: [[Int]] = []
        for dim in 0..<ndim {
            for delta in [-1, 1] {
                let updated = config[dim] + delta
                guard updated >= 0, updated <= ceilings[dim] else { continue }
                var candidate = config
                candidate[dim] = updated
                guard candidate.reduce(0, +) > 0 else { continue }
                guard cache[candidate] == nil else { continue }
                neighbors.append(candidate)
            }
        }
        return neighbors
    }

    func modelPredict(_ config: [Int]) -> Double {
        var totalThroughput = 0.0
        for index in 0..<ndim {
            let slots = config[index]
            guard slots > 0 else { continue }
            let throughputIndex = min(slots, throughputs[index].count - 1)
            totalThroughput += throughputs[index][throughputIndex]
        }
        guard totalThroughput > 0 else { return .infinity }
        return Double(safeNumVideos) / totalThroughput
    }

    func totalSlots(_ config: [Int]) -> Int {
        config.reduce(0, +)
    }

    func median(_ values: [Double]) -> Double? {
        guard !values.isEmpty else { return nil }
        let sorted = values.sorted()
        let middle = sorted.count / 2
        if sorted.count.isMultiple(of: 2) {
            return (sorted[middle - 1] + sorted[middle]) / 2
        }
        return sorted[middle]
    }

    func recordCalibrationSample(config: [Int], rawPrediction: Double, elapsed: Double) {
        guard rawPrediction.isFinite, rawPrediction > 0 else { return }
        guard elapsed.isFinite, elapsed > 0, elapsed < burstOptimizerPenaltyElapsed else { return }
        calibrationSamples.append(
            BurstCalibrationSample(
                rawPrediction: rawPrediction,
                totalSlots: totalSlots(config),
                elapsed: elapsed
            )
        )
    }

    func fitCalibrationModel() -> BurstCalibrationModel? {
        let validSamples = calibrationSamples.filter { sample in
            sample.rawPrediction.isFinite &&
            sample.rawPrediction > 0 &&
            sample.elapsed.isFinite &&
            sample.elapsed > 0 &&
            sample.elapsed < burstOptimizerPenaltyElapsed
        }
        guard validSamples.count >= burstOptimizerCalibrationMinSamples else { return nil }

        let ratios = validSamples.compactMap { sample -> Double? in
            let ratio = sample.elapsed / sample.rawPrediction
            guard ratio.isFinite, ratio > 0 else { return nil }
            return ratio
        }
        guard let rawScale = median(ratios), rawScale.isFinite, rawScale > 0 else { return nil }
        let scale = min(max(rawScale, burstOptimizerCalibrationScaleMin), burstOptimizerCalibrationScaleMax)

        let penalties = validSamples.compactMap { sample -> Double? in
            guard sample.totalSlots > 1 else { return nil }
            let extraSlots = Double(sample.totalSlots - 1)
            let penalty = (sample.elapsed - sample.rawPrediction * scale) / extraSlots
            guard penalty.isFinite else { return nil }
            return penalty
        }

        let perExtraSlotPenalty: Double
        if let medianPenalty = median(penalties) {
            let typicalElapsed = median(validSamples.map(\.elapsed)) ?? 1.0
            let penaltyLimit = max(1.0, typicalElapsed * burstOptimizerCalibrationPenaltyLimitMultiplier)
            perExtraSlotPenalty = min(max(medianPenalty, -penaltyLimit), penaltyLimit)
        } else {
            perExtraSlotPenalty = 0
        }

        guard perExtraSlotPenalty.isFinite else { return nil }
        return BurstCalibrationModel(scale: scale, perExtraSlotPenalty: perExtraSlotPenalty)
    }

    func calibratedPredict(
        rawPrediction: Double,
        config: [Int],
        model: BurstCalibrationModel?
    ) -> Double {
        guard rawPrediction.isFinite, rawPrediction > 0 else { return .infinity }
        guard let model else { return rawPrediction }

        let extraSlots = Double(max(0, totalSlots(config) - 1))
        let calibrated = rawPrediction * model.scale + extraSlots * model.perExtraSlotPenalty
        guard calibrated.isFinite, calibrated > 0 else { return rawPrediction }
        return min(calibrated, burstOptimizerPenaltyElapsed)
    }

    @discardableResult
    func eval(config: [Int], phase: String, predicted: Double? = nil) async throws -> Double {
        if let cached = cache[config] {
            return cached
        }

        evalCount += 1

        let measured: Double
        do {
            measured = try await evaluate(config)
        } catch {
            if isBenchmarkInterrupted(error, interruptState: nil) {
                throw error
            }
            measured = burstOptimizerPenaltyElapsed
        }
        let elapsed = (measured.isFinite && measured > 0) ? measured : burstOptimizerPenaltyElapsed
        cache[config] = elapsed

        let isNewBest = elapsed.isFinite && elapsed < bestTime
        if isNewBest {
            bestTime = elapsed
            bestConfig = config
        }

        let record = BurstEvalRecord(
            config: config,
            elapsed: elapsed,
            phase: phase,
            predicted: predicted,
            isNewBest: isNewBest
        )
        history.append(record)
        onEval?(record)
        return elapsed
    }

    func phase1() async throws {
        throughputs = Array(repeating: [0.0], count: ndim)
        phase1CalibratedConfigs.removeAll(keepingCapacity: true)

        let order = (0..<ndim).sorted { ceilings[$0] > ceilings[$1] }
        for dim in order {
            guard ceilings[dim] > 0 else { continue }
            for slots in 1...ceilings[dim] {
                let config = unitConfig(dim: dim, slots: slots)
                let elapsed = try await eval(config: config, phase: "profile")
                phase1CalibratedConfigs.append(config)
                let throughput = Double(safeNumVideos) / elapsed
                throughputs[dim].append((throughput.isFinite && throughput > 0) ? throughput : 0.0)
            }
        }

        // Seed calibration only after full phase-1 throughput table is populated.
        for config in phase1CalibratedConfigs {
            guard let elapsed = cache[config] else { continue }
            let rawPrediction = modelPredict(config)
            recordCalibrationSample(config: config, rawPrediction: rawPrediction, elapsed: elapsed)
        }
    }

    func phase2() async throws {
        var candidates: [Phase2Candidate] = []
        for config in allValidConfigs() {
            guard cache[config] == nil else { continue }
            candidates.append(Phase2Candidate(config: config, rawPrediction: modelPredict(config)))
        }

        let evalLimit = min(max(topK, 0), candidates.count)
        for _ in 0..<evalLimit {
            let model = fitCalibrationModel()

            var bestIndex: Int?
            var bestScore = Double.infinity
            for index in candidates.indices {
                let score = calibratedPredict(
                    rawPrediction: candidates[index].rawPrediction,
                    config: candidates[index].config,
                    model: model
                )
                guard let currentBestIndex = bestIndex else {
                    bestIndex = index
                    bestScore = score
                    continue
                }

                let bestConfig = candidates[currentBestIndex].config
                if score < bestScore || (score == bestScore && candidates[index].config.lexicographicallyPrecedes(bestConfig)) {
                    bestIndex = index
                    bestScore = score
                }
            }

            guard let selectedIndex = bestIndex else { break }
            let selected = candidates.remove(at: selectedIndex)
            let predicted = calibratedPredict(
                rawPrediction: selected.rawPrediction,
                config: selected.config,
                model: model
            )
            let elapsed = try await eval(config: selected.config, phase: "model", predicted: predicted)
            recordCalibrationSample(config: selected.config, rawPrediction: selected.rawPrediction, elapsed: elapsed)
        }
    }

    func phase3() async throws {
        guard let initialBest = bestConfig else { return }

        var currentBest = initialBest
        for _ in 0..<3 {
            let oldBestTime = bestTime
            for neighbor in l1Neighbors(of: currentBest) {
                _ = try await eval(config: neighbor, phase: "refine")
            }
            guard bestTime < oldBestTime, let updatedBest = bestConfig else { break }
            currentBest = updatedBest
        }

        let sorted = cache.sorted { $0.value < $1.value }
        if sorted.count > 1 {
            for index in 1..<min(4, sorted.count) {
                for neighbor in l1Neighbors(of: sorted[index].key) {
                    _ = try await eval(config: neighbor, phase: "refine")
                }
            }
        }
    }

    func bruteForce() async throws {
        let configs = allValidConfigs().sorted {
            $0.reduce(0, +) > $1.reduce(0, +)
        }
        for config in configs {
            _ = try await eval(config: config, phase: "brute")
        }
    }

    if spaceSize <= 25 {
        try await bruteForce()
    } else {
        try await phase1()
        try await phase2()
        try await phase3()
    }

    guard let resolvedBest = bestConfig, bestTime.isFinite else {
        throw BurstOptimizerError.noValidConfig
    }

    return BurstSearchResult(
        bestConfig: resolvedBest,
        bestTime: bestTime,
        evaluations: evalCount,
        spaceSize: spaceSize,
        history: history
    )
}

private func burstConfigSpaceSize(ceilings: [Int]) -> Int {
    var size = 1
    for ceiling in ceilings {
        let (product, overflow) = size.multipliedReportingOverflow(by: ceiling + 1)
        if overflow {
            return Int.max
        }
        size = product
    }
    return max(0, size - 1)
}
