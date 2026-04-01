import Foundation
import KikoMediaCore

struct ThunderboltCAPriorPaths: Sendable {
    let canonicalPath: String
    let candidatePath: String
}

func validMSPerFrameC1(_ value: Double?) -> Double? {
    CAProfileAndFallbackMath.validMSPerFrameC1(value)
}

func caDegradationCurve(from curveByConcurrency: [Int: Double]?) -> [CADegradationPoint] {
    CAProfileAndFallbackMath.degradationCurve(from: curveByConcurrency)
}

func caSlots(
    localSlots: Int,
    reachableWorkers: [ThunderboltBoundWorkerSpec]
) -> [ThunderboltCASlot] {
    var slots: [ThunderboltCASlot] = []
    slots.reserveCapacity(localSlots + reachableWorkers.reduce(0) { $0 + $1.slots })

    for index in 1...localSlots {
        slots.append(.local(index: index))
    }
    for worker in reachableWorkers {
        let workerSlots = worker.slots
        guard workerSlots > 0 else { continue }
        for index in 1...workerSlots {
            slots.append(.remote(worker: worker, index: index))
        }
    }
    return slots
}

func loadCAPriorTable() -> BenchmarkPriorTable {
    let canonicalPath = resolveCanonicalBenchmarkPriorPath()
    if let artifact = BenchmarkPriorArtifact.load(fromPath: canonicalPath) {
        return BenchmarkPriorTable(artifact: artifact)
    }
    return BenchmarkPriorTable()
}

func caDegradationCurve(from machine: BenchmarkPriorMachine?) -> [CADegradationPoint] {
    CAProfileAndFallbackMath.degradationCurve(from: machine)
}

func caEstimates(
    videos: [MediaFile],
    localMSPerFrameC1: Double,
    localFixedOverheadMS: Double = 0,
    onProgress: (@Sendable (Int, Int) -> Void)? = nil
) async -> [Double] {
    let videoCosts = await caResolvedVideoCosts(
        videos: videos,
        localMSPerFrameC1: localMSPerFrameC1,
        localFixedOverheadMS: localFixedOverheadMS,
        onProgress: onProgress
    )
    return videoCosts.map(\.frameCount)
}

func caResolvedVideoCosts(
    videos: [MediaFile],
    localMSPerFrameC1: Double,
    localFixedOverheadMS: Double = 0,
    onProgress: (@Sendable (Int, Int) -> Void)? = nil
) async -> [CAResolvedVideoCost] {
    var videoCosts: [CAResolvedVideoCost] = []
    videoCosts.reserveCapacity(videos.count)
    onProgress?(0, videos.count)
    for video in videos {
        let mimeType = MediaProcessor.enqueueMIMEType(
            filePath: video.path,
            originalName: video.name
        )
        videoCosts.append(
            await MediaProcessor.resolveVideoCostForQueue(
                filePath: video.path,
                mimeType: mimeType,
                localMSPerFrameC1: localMSPerFrameC1,
                localFixedOverheadMS: localFixedOverheadMS
            )
        )
        onProgress?(videoCosts.count, videos.count)
    }
    return videoCosts
}

func caArrivalOffsets(profile: CAArrivalProfile, count: Int) -> [Double] {
    guard count > 0 else { return [] }

    switch profile {
    case .allAtOnce:
        return Array(repeating: 0, count: count)

    case .trickle:
        return (0..<count).map { Double($0) }

    case .burst_1_20_5_5_1:
        var offsets: [Double] = []
        offsets.reserveCapacity(count)

        let phases: [(jobs: Int, at: Double)] = [
            (1, 0),
            (20, 5),
            (5, 10),
            (5, 15),
        ]
        var remaining = count
        for phase in phases where remaining > 0 {
            let take = min(phase.jobs, remaining)
            offsets.append(contentsOf: Array(repeating: phase.at, count: take))
            remaining -= take
        }
        if remaining > 0 {
            offsets.append(contentsOf: Array(repeating: 20, count: remaining))
        }
        return offsets
    }
}

func caAcceptanceCorpusSignature(videos: [MediaFile]) -> String {
    var hash: UInt64 = 0xcbf29ce484222325
    let sortedVideos = videos.sorted { lhs, rhs in
        if lhs.name != rhs.name {
            return lhs.name < rhs.name
        }
        if lhs.sizeBytes != rhs.sizeBytes {
            return lhs.sizeBytes < rhs.sizeBytes
        }
        return lhs.path < rhs.path
    }

    for video in sortedVideos {
        let line = "\(video.name)\t\(video.sizeBytes)\n"
        for byte in line.utf8 {
            hash ^= UInt64(byte)
            hash &*= 0x100000001b3
        }
    }
    return String(format: "%016llx", hash)
}

func resolveThunderboltCAPriorPaths() -> ThunderboltCAPriorPaths {
    let canonicalPath = resolveCanonicalBenchmarkPriorPath()
    let canonicalURL = URL(fileURLWithPath: canonicalPath)
    let candidatePath = canonicalURL
        .deletingLastPathComponent()
        .appendingPathComponent("benchmark-prior.candidate.json")
        .path
    return ThunderboltCAPriorPaths(
        canonicalPath: canonicalPath,
        candidatePath: candidatePath
    )
}
