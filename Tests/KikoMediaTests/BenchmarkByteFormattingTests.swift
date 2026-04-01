import Testing
@testable import benchmarks

@Suite("Benchmark byte formatting")
struct BenchmarkByteFormattingTests {
    @Test("shared formatter canonicalizes spacing, base, and precision")
    func canonicalFormattingPolicy() {
        #expect(BenchmarkByteFormatter.format(-1) == "0 KB")
        #expect(BenchmarkByteFormatter.format(0) == "0 KB")
        #expect(BenchmarkByteFormatter.format(1_000) == "1 KB")
        #expect(BenchmarkByteFormatter.format(1_500_000) == "1.5 MB")
        #expect(BenchmarkByteFormatter.format(100_000_000) == "100 MB")
        #expect(BenchmarkByteFormatter.format(1_000_000_000) == "1 GB")
        #expect(BenchmarkByteFormatter.format(1_000_000_000_000) == "1.0 TB")
    }

    @Test("pipeline and thunderbolt byte labels stay identical for same inputs")
    func pipelineAndThunderboltConsistency() {
        let samples = [0, 950, 12_345, 1_500_000, 234_000_000, 1_000_000_000, 2_200_000_000]
        for bytes in samples {
            #expect(PipelineWalkthrough.formatByteSize(bytes) == formatThunderboltCorpusSize(bytes))
        }
    }

    @Test("benchmark surfaces delegate to canonical formatter")
    func benchmarkSurfacesDelegateToSharedFormatter() {
        let samples: [Int64] = [0, 1_234, 9_876_543, 123_456_789, 1_234_567_890]
        for bytes in samples {
            let expected = BenchmarkByteFormatter.format(bytes)
            #expect(ConsoleUI.formatBytes(bytes) == expected)
            #expect(formatThunderboltCorpusSize(Int(bytes)) == expected)
            #expect(PipelineWalkthrough.formatByteSize(Int(bytes)) == expected)
        }
    }
}
