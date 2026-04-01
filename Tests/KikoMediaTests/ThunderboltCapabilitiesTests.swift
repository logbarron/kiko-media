import Testing
@testable import KikoMediaCore

@Suite("ThunderboltCapabilities")
struct ThunderboltCapabilitiesTests {

    @Test("10 cores, 1 engine -> 3")
    func sweepCeiling10Cores1Engine() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: 10, videoEncodeEngines: 1) == 3)
    }

    @Test("14 cores, 2 engines -> 5")
    func sweepCeiling14Cores2Engines() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: 14, videoEncodeEngines: 2) == 5)
    }

    @Test("28 cores, 4 engines -> 9")
    func sweepCeiling28Cores4Engines() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: 28, videoEncodeEngines: 4) == 9)
    }

    @Test("cores < engine formula -> capped by cores")
    func sweepCeilingCoresCapped() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: 2, videoEncodeEngines: 4) == 2)
    }

    @Test("zero cores clamps to 1")
    func sweepCeilingZeroCores() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: 0, videoEncodeEngines: 2) == 1)
    }

    @Test("zero engines clamps to 1")
    func sweepCeilingZeroEngines() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: 10, videoEncodeEngines: 0) == 3)
    }

    @Test("negative values clamp to 1")
    func sweepCeilingNegativeValues() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: -1, videoEncodeEngines: -1) == 1)
    }

    @Test("1 core, 1 engine -> 1")
    func sweepCeilingMinimal() {
        #expect(ThunderboltCapabilities.sweepCeiling(totalCores: 1, videoEncodeEngines: 1) == 1)
    }
}
