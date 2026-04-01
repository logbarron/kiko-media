import Foundation
import OSLog

package struct BenchmarkPriorCell: Sendable, Codable, Equatable {
    package let concurrency: Int
    package let videosPerMin: Double
    package let msPerVideoP50: Int
    package let msPerVideoP95: Int
    package let degradationRatio: Double

    enum CodingKeys: String, CodingKey {
        case concurrency
        case videosPerMin = "videos_per_min"
        case msPerVideoP50 = "ms_per_video_p50"
        case msPerVideoP95 = "ms_per_video_p95"
        case degradationRatio = "degradation_ratio"
    }

    package init(
        concurrency: Int,
        videosPerMin: Double,
        msPerVideoP50: Int,
        msPerVideoP95: Int,
        degradationRatio: Double = 1.0
    ) {
        self.concurrency = concurrency
        self.videosPerMin = videosPerMin
        self.msPerVideoP50 = msPerVideoP50
        self.msPerVideoP95 = msPerVideoP95
        self.degradationRatio = degradationRatio
    }
}

package enum BenchmarkPriorAffineModelSource: String, Sendable, Codable, Equatable {
    case explicit
    case legacyHeuristic = "legacy_heuristic"
}

package struct BenchmarkPriorAffineModel: Sendable, Equatable {
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let avgCorpusFrameCount: Double
    package let source: BenchmarkPriorAffineModelSource

    package init(
        msPerFrameC1: Double,
        fixedOverheadMS: Double,
        avgCorpusFrameCount: Double,
        source: BenchmarkPriorAffineModelSource
    ) {
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.avgCorpusFrameCount = avgCorpusFrameCount
        self.source = source
    }
}

package struct BenchmarkPriorMachine: Sendable, Codable, Equatable {
    package let signature: String
    package let chipName: String
    package let performanceCores: Int
    package let efficiencyCores: Int
    package let videoEncodeEngines: Int
    package let osVersion: String
    package let transcodePreset: String
    package let msPerFrameC1: Double
    package let fixedOverheadMS: Double
    package let avgCorpusFrameCount: Double
    package let affineModelSource: BenchmarkPriorAffineModelSource
    package let cells: [BenchmarkPriorCell]

    enum CodingKeys: String, CodingKey {
        case signature
        case chipName = "chip_name"
        case performanceCores = "performance_cores"
        case efficiencyCores = "efficiency_cores"
        case videoEncodeEngines = "video_encode_engines"
        case osVersion = "os_version"
        case transcodePreset = "transcode_preset"
        case msPerFrameC1 = "ms_per_frame_c1"
        case fixedOverheadMS = "fixed_overhead_ms"
        case avgCorpusFrameCount = "avg_corpus_frame_count"
        case affineModelSource = "affine_model_source"
        case cells
    }

    package init(
        signature: String,
        chipName: String,
        performanceCores: Int,
        efficiencyCores: Int,
        videoEncodeEngines: Int,
        osVersion: String,
        transcodePreset: String,
        msPerFrameC1: Double = 0,
        fixedOverheadMS: Double = 0,
        avgCorpusFrameCount: Double = 0,
        affineModelSource: BenchmarkPriorAffineModelSource = .explicit,
        cells: [BenchmarkPriorCell]
    ) {
        self.signature = signature
        self.chipName = chipName
        self.performanceCores = performanceCores
        self.efficiencyCores = efficiencyCores
        self.videoEncodeEngines = videoEncodeEngines
        self.osVersion = osVersion
        self.transcodePreset = transcodePreset
        self.msPerFrameC1 = msPerFrameC1
        self.fixedOverheadMS = fixedOverheadMS
        self.avgCorpusFrameCount = avgCorpusFrameCount
        self.affineModelSource = affineModelSource
        self.cells = cells.sorted { $0.concurrency < $1.concurrency }
    }

    package init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Missing source means the artifact predates WS8. Keep it on the legacy path
        // regardless of which affine fields happen to be present so maintenance can promote it.
        let resolvedAffineModelSource =
            try container.decodeIfPresent(BenchmarkPriorAffineModelSource.self, forKey: .affineModelSource)
            ?? .legacyHeuristic
        try self.init(
            signature: container.decode(String.self, forKey: .signature),
            chipName: container.decode(String.self, forKey: .chipName),
            performanceCores: container.decode(Int.self, forKey: .performanceCores),
            efficiencyCores: container.decode(Int.self, forKey: .efficiencyCores),
            videoEncodeEngines: container.decode(Int.self, forKey: .videoEncodeEngines),
            osVersion: container.decode(String.self, forKey: .osVersion),
            transcodePreset: container.decode(String.self, forKey: .transcodePreset),
            msPerFrameC1: container.decode(Double.self, forKey: .msPerFrameC1),
            fixedOverheadMS: container.decodeIfPresent(Double.self, forKey: .fixedOverheadMS) ?? 0,
            avgCorpusFrameCount: container.decode(Double.self, forKey: .avgCorpusFrameCount),
            affineModelSource: resolvedAffineModelSource,
            cells: container.decode([BenchmarkPriorCell].self, forKey: .cells)
        )
    }

    package var usesLegacyAffineHeuristic: Bool {
        self.affineModelSource == .legacyHeuristic
    }
}

package struct BenchmarkPriorCorpusSummary: Sendable, Codable, Equatable {
    package let videoCount: Int
    package let totalBytes: Int64

    enum CodingKeys: String, CodingKey {
        case videoCount = "video_count"
        case totalBytes = "total_bytes"
    }

    package init(videoCount: Int, totalBytes: Int64) {
        self.videoCount = videoCount
        self.totalBytes = totalBytes
    }
}

package struct BenchmarkPriorArtifact: Sendable, Codable, Equatable {
    package static let supportedVersion = 2

    package enum LoadResult: Sendable, Equatable {
        case missing
        case invalid
        case unsupportedVersion(Int)
        case loaded(BenchmarkPriorArtifact)
    }

    package let version: Int
    package let generatedAt: Date
    package let corpusHash: String
    package let corpusSummary: BenchmarkPriorCorpusSummary
    package let machines: [BenchmarkPriorMachine]

    enum CodingKeys: String, CodingKey {
        case version
        case generatedAt = "generated_at"
        case corpusHash = "corpus_hash"
        case corpusSummary = "corpus_summary"
        case machines
    }

    package init(
        version: Int = BenchmarkPriorArtifact.supportedVersion,
        generatedAt: Date,
        corpusHash: String,
        corpusSummary: BenchmarkPriorCorpusSummary,
        machines: [BenchmarkPriorMachine]
    ) {
        self.version = version
        self.generatedAt = generatedAt
        self.corpusHash = corpusHash
        self.corpusSummary = corpusSummary
        self.machines = machines
    }

    package static func loadResult(fromPath path: String) -> LoadResult {
        guard FileManager.default.fileExists(atPath: path) else {
            return .missing
        }

        let data: Data
        do {
            data = try Data(contentsOf: URL(fileURLWithPath: path))
        } catch {
            Logger.kiko.error(
                "Failed to read benchmark prior artifact at \(path, privacy: .public): \(error.localizedDescription)"
            )
            return .invalid
        }

        struct PriorVersionEnvelope: Decodable {
            let version: Int
        }

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        guard let versionEnvelope = try? decoder.decode(PriorVersionEnvelope.self, from: data) else {
            Logger.kiko.error("Failed to decode benchmark prior version envelope at \(path, privacy: .public)")
            return .invalid
        }
        guard versionEnvelope.version == BenchmarkPriorArtifact.supportedVersion else {
            if versionEnvelope.version == 1 {
                Logger.kiko.error(
                    "Rejected benchmark prior v1 at \(path, privacy: .public); regenerate benchmark-prior.json with v2"
                )
            } else {
                Logger.kiko.error(
                    "Rejected benchmark prior version \(versionEnvelope.version, privacy: .public) at \(path, privacy: .public); expected v\(BenchmarkPriorArtifact.supportedVersion, privacy: .public)"
                )
            }
            return .unsupportedVersion(versionEnvelope.version)
        }
        guard let artifact = try? decoder.decode(BenchmarkPriorArtifact.self, from: data) else {
            Logger.kiko.error("Failed to decode benchmark prior v2 payload at \(path, privacy: .public)")
            return .invalid
        }
        return .loaded(artifact)
    }

    package static func load(fromPath path: String) -> BenchmarkPriorArtifact? {
        guard case let .loaded(artifact) = loadResult(fromPath: path) else {
            return nil
        }
        return artifact
    }

    package func write(toPath path: String) throws {
        let fileURL = URL(fileURLWithPath: path)
        try FileManager.default.createDirectory(
            at: fileURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(self)
        try data.write(to: fileURL, options: .atomic)
    }
}

package struct BenchmarkPriorTable: Sendable, Equatable {
    package private(set) var machines: [BenchmarkPriorMachine]

    package init(machines: [BenchmarkPriorMachine] = []) {
        self.machines = machines
    }

    package init(artifact: BenchmarkPriorArtifact) {
        self.machines = artifact.machines
    }

    package mutating func merge(remoteMachine: BenchmarkPriorMachine) {
        if let index = machines.firstIndex(where: { $0.signature == remoteMachine.signature }) {
            machines[index] = remoteMachine
            return
        }
        machines.append(remoteMachine)
    }

    package func exactMachine(signature: String?) -> BenchmarkPriorMachine? {
        guard let signature else { return nil }
        return machines.first(where: { $0.signature == signature })
    }

    package func hardwareCompatibleMachine(
        signature: String?
    ) -> BenchmarkPriorMachine? {
        guard let components = WorkerSignatureComponents.parse(signature: signature) else {
            return nil
        }
        return hardwareCompatibleMachine(components: components)
    }

    package func hardwareCompatibleMachine(
        components: WorkerSignatureComponents?
    ) -> BenchmarkPriorMachine? {
        guard let components else { return nil }
        let compatibleMachines = machines.filter { machine in
            WorkerSignatureComponents(machine: machine).hardwarePresetKey == components.hardwarePresetKey
        }
        guard !compatibleMachines.isEmpty else { return nil }
        if let exactOSMatch = compatibleMachines.first(where: { $0.osVersion == components.osVersion }) {
            return exactOSMatch
        }
        let sortedByClosestOS = compatibleMachines.sorted { lhs, rhs in
            let lhsDistance = WorkerSignatureComponents.osDistance(lhs.osVersion, components.osVersion)
            let rhsDistance = WorkerSignatureComponents.osDistance(rhs.osVersion, components.osVersion)
            if lhsDistance != rhsDistance {
                return lhsDistance < rhsDistance
            }
            if lhs.avgCorpusFrameCount != rhs.avgCorpusFrameCount {
                return lhs.avgCorpusFrameCount > rhs.avgCorpusFrameCount
            }
            return lhs.signature < rhs.signature
        }
        return sortedByClosestOS.first
    }

    package func lookup(signature: String, concurrency: Int) -> BenchmarkPriorCell? {
        machines
            .first(where: { $0.signature == signature })?
            .cells
            .first(where: { $0.concurrency == concurrency })
    }
}

package struct WorkerSignatureComponents: Sendable, Equatable {
    package let chipName: String
    package let performanceCores: Int
    package let efficiencyCores: Int
    package let videoEncodeEngines: Int
    package let osVersion: String
    package let preset: String

    package init(
        chipName: String,
        performanceCores: Int,
        efficiencyCores: Int,
        videoEncodeEngines: Int,
        osVersion: String,
        preset: String
    ) {
        self.chipName = chipName.trimmingCharacters(in: .whitespacesAndNewlines)
        self.performanceCores = max(0, performanceCores)
        self.efficiencyCores = max(0, efficiencyCores)
        self.videoEncodeEngines = max(0, videoEncodeEngines)
        self.osVersion = osVersion
        self.preset = preset
    }

    package init(machine: BenchmarkPriorMachine) {
        self.init(
            chipName: machine.chipName,
            performanceCores: machine.performanceCores,
            efficiencyCores: machine.efficiencyCores,
            videoEncodeEngines: machine.videoEncodeEngines,
            osVersion: machine.osVersion,
            preset: machine.transcodePreset
        )
    }

    package var exactSoftwareKey: String {
        WorkerSignatureBuilder.make(
            chipName: chipName,
            performanceCores: performanceCores,
            efficiencyCores: efficiencyCores,
            videoEncodeEngines: videoEncodeEngines,
            preset: preset,
            osVersion: osVersion
        ) ?? ""
    }

    package var hardwarePresetKey: String {
        "chip=\(chipName);ecores=\(efficiencyCores);encoders=\(videoEncodeEngines);pcores=\(performanceCores);preset=\(preset)"
    }

    package static func parse(signature: String?) -> WorkerSignatureComponents? {
        guard let signature else { return nil }
        let parts = signature
            .split(separator: ";", omittingEmptySubsequences: true)
            .reduce(into: [String: String]()) { partial, component in
                let pieces = component.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard pieces.count == 2 else { return }
                partial[String(pieces[0])] = String(pieces[1])
            }
        guard let chipName = parts["chip"],
              let efficiencyCores = parts["ecores"].flatMap(Int.init),
              let videoEncodeEngines = parts["encoders"].flatMap(Int.init),
              let osVersion = parts["os"],
              let performanceCores = parts["pcores"].flatMap(Int.init),
              let preset = parts["preset"] else {
            return nil
        }
        return WorkerSignatureComponents(
            chipName: chipName,
            performanceCores: performanceCores,
            efficiencyCores: efficiencyCores,
            videoEncodeEngines: videoEncodeEngines,
            osVersion: osVersion,
            preset: preset
        )
    }

    package static func osDistance(_ lhs: String, _ rhs: String) -> Int {
        let lhsComponents = parseOSVersion(lhs)
        let rhsComponents = parseOSVersion(rhs)
        let majorDistance = abs(lhsComponents.major - rhsComponents.major) * 10_000
        let minorDistance = abs(lhsComponents.minor - rhsComponents.minor) * 100
        let patchDistance = abs(lhsComponents.patch - rhsComponents.patch)
        return majorDistance + minorDistance + patchDistance
    }

    private static func parseOSVersion(_ raw: String) -> (major: Int, minor: Int, patch: Int) {
        let parts = raw.split(separator: ".", omittingEmptySubsequences: false).map(String.init)
        let major = parts.indices.contains(0) ? Int(parts[0]) ?? 0 : 0
        let minor = parts.indices.contains(1) ? Int(parts[1]) ?? 0 : 0
        let patch = parts.indices.contains(2) ? Int(parts[2]) ?? 0 : 0
        return (major, minor, patch)
    }
}

package enum WorkerSignatureBuilder {
    package static func normalizedOS(_ version: OperatingSystemVersion) -> String {
        "\(version.majorVersion).\(version.minorVersion)"
    }

    package static func make(
        chipName: String,
        performanceCores: Int,
        efficiencyCores: Int,
        videoEncodeEngines: Int,
        preset: String,
        osVersion: OperatingSystemVersion
    ) -> String {
        let chip = chipName.trimmingCharacters(in: .whitespacesAndNewlines)
        let os = normalizedOS(osVersion)
        return "chip=\(chip);ecores=\(max(0, efficiencyCores));encoders=\(max(0, videoEncodeEngines));os=\(os);pcores=\(max(0, performanceCores));preset=\(preset)"
    }

    package static func make(
        chipName: String?,
        performanceCores: Int?,
        efficiencyCores: Int?,
        videoEncodeEngines: Int?,
        preset: String,
        osVersion: String?
    ) -> String? {
        guard let chipName,
              let performanceCores,
              let efficiencyCores,
              let videoEncodeEngines,
              let osVersion
        else {
            return nil
        }
        let chip = chipName.trimmingCharacters(in: .whitespacesAndNewlines)
        return "chip=\(chip);ecores=\(max(0, efficiencyCores));encoders=\(max(0, videoEncodeEngines));os=\(osVersion);pcores=\(max(0, performanceCores));preset=\(preset)"
    }
}
