import Foundation
import Testing
@testable import benchmarks

@Suite("Burst optimizer")
struct BurstOptimizerTests {
    @Test("invalid ceilings throw noValidConfig")
    func invalidCeilingsThrowNoValidConfig() async {
        await expectNoValidConfig([])
        await expectNoValidConfig([0, 0])
        await expectNoValidConfig([-1, 2])
    }

    @Test("small search space runs brute force only")
    func smallSpaceUsesBruteForceOnly() async throws {
        let result = try await optimizeBurstConcurrency(
            ceilings: [2, 2, 1],
            evaluate: { config in
                Double(config[0] * 100 + config[1] * 10 + config[2] + 1)
            },
            numVideos: 24,
            topK: 3
        )

        #expect(result.spaceSize == 17)
        #expect(result.evaluations == 17)
        #expect(result.history.count == 17)
        #expect(result.history.allSatisfy { $0.phase == "brute" })
        #expect(result.history.allSatisfy { $0.predicted == nil })
        #expect(result.bestConfig == [0, 0, 1])
        #expect(result.bestTime == 2.0)
    }

    @Test("phase2 respects topK and does not duplicate evaluations")
    func phase2RespectsTopKAndNoDuplicates() async throws {
        let result = try await optimizeBurstConcurrency(
            ceilings: [4, 4, 1],
            evaluate: additiveEvaluator,
            numVideos: 60,
            topK: 3
        )

        let modelRows = result.history.filter { $0.phase == "model" }
        #expect(modelRows.count == 3)
        #expect(modelRows.allSatisfy { row in
            guard let predicted = row.predicted else { return false }
            return predicted.isFinite && predicted > 0
        })

        let uniqueConfigs = Set(result.history.map(\.config))
        #expect(uniqueConfigs.count == result.evaluations)
    }

    @Test("topK bounds clamp in optimized search")
    func topKBoundsClamp() async throws {
        let zeroTopK = try await optimizeBurstConcurrency(
            ceilings: [4, 4, 1],
            evaluate: additiveEvaluator,
            numVideos: 60,
            topK: 0
        )
        #expect(zeroTopK.history.filter { $0.phase == "model" }.isEmpty)

        let hugeTopK = try await optimizeBurstConcurrency(
            ceilings: [4, 4, 1],
            evaluate: additiveEvaluator,
            numVideos: 60,
            topK: 999
        )
        let expectedModelCount = hugeTopK.spaceSize - [4, 4, 1].reduce(0, +)
        #expect(hugeTopK.history.filter { $0.phase == "model" }.count == expectedModelCount)
        #expect(hugeTopK.evaluations == hugeTopK.spaceSize)
    }

    @Test("failed eval records penalty and cannot win")
    func failedEvalUsesPenaltyAndCannotWin() async throws {
        enum ForcedError: Error {
            case fail
        }

        let result = try await optimizeBurstConcurrency(
            ceilings: [2, 1],
            evaluate: { config in
                if config == [2, 1] {
                    throw ForcedError.fail
                }
                return Double(config[0] * 10 + config[1] + 1)
            },
            numVideos: 12,
            topK: 2
        )

        let penaltyRecord = result.history.first { $0.config == [2, 1] }
        #expect(penaltyRecord != nil)
        #expect(penaltyRecord?.elapsed == 1_000_000.0)
        #expect(result.bestConfig != [2, 1])
        #expect(result.bestTime < 1_000_000.0)
    }

    @Test("zero-ceiling dimensions do not trap in optimized path")
    func zeroCeilingDimensionDoesNotTrap() async throws {
        let result = try await optimizeBurstConcurrency(
            ceilings: [0, 5, 5],
            evaluate: additiveEvaluator,
            numVideos: 50,
            topK: 4
        )

        #expect(result.spaceSize == 35)
        let profileRows = result.history.filter { $0.phase == "profile" }
        #expect(profileRows.count == 10)
        #expect(profileRows.allSatisfy { $0.config[0] == 0 })
    }

    private func expectNoValidConfig(_ ceilings: [Int]) async {
        do {
            _ = try await optimizeBurstConcurrency(
                ceilings: ceilings,
                evaluate: { _ in 1.0 },
                numVideos: 8
            )
            Issue.record("Expected noValidConfig for ceilings \(ceilings)")
        } catch BurstOptimizerError.noValidConfig {
            // Expected path.
        } catch {
            Issue.record("Unexpected error for ceilings \(ceilings): \(error)")
        }
    }

    private var additiveEvaluator: @Sendable ([Int]) async throws -> Double {
        { config in
            let throughput =
                Double(config[0]) * 1.0 +
                Double(config[1]) * 1.2 +
                Double(config[2]) * 1.5
            if throughput <= 0 {
                return 1_000_000.0
            }
            return 60.0 / throughput
        }
    }
}
