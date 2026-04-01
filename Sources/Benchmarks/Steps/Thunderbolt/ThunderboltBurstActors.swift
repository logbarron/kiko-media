import Foundation

struct ThunderboltBenchmarkJSONWorker: Encodable, Sendable {
    let index: Int
    let host: String
    let configuredSlots: Int

    enum CodingKeys: String, CodingKey {
        case index
        case host
        case configuredSlots = "configured_slots"
    }
}

struct ThunderboltBenchmarkJSONRemoteWorker: Encodable, Sendable {
    let index: Int
    let host: String
    let slots: Int
}

struct ThunderboltBenchmarkJSONBestConfig: Encodable, Sendable {
    let localSlots: Int
    let remoteWorkers: [ThunderboltBenchmarkJSONRemoteWorker]
    let wallSeconds: Double
    let completedVideos: Int
    let failedVideos: Int
    let videosPerMin: Double

    enum CodingKeys: String, CodingKey {
        case localSlots = "local_slots"
        case remoteWorkers = "remote_workers"
        case wallSeconds = "wall_seconds"
        case completedVideos = "completed_videos"
        case failedVideos = "failed_videos"
        case videosPerMin = "videos_per_min"
    }
}

let thunderboltDelegatedBenchmarkSchemaVersion = 1

struct ThunderboltBenchmarkJSONPayload: Encodable, Sendable {
    let schemaVersion: Int
    let workers: [ThunderboltBenchmarkJSONWorker]
    let bestConfig: ThunderboltBenchmarkJSONBestConfig

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case workers
        case bestConfig = "best_config"
    }
}

actor ThunderboltJobCursor {
    private let total: Int
    private var nextIndex = 0

    init(total: Int) {
        self.total = total
    }

    func next() -> Int? {
        guard nextIndex < total else { return nil }
        let value = nextIndex
        nextIndex += 1
        return value
    }
}

actor ThunderboltBurstStore {
    private var jobs: [ThunderboltBurstJob] = []

    @discardableResult
    func append(_ job: ThunderboltBurstJob) -> Int {
        jobs.append(job)
        return jobs.count
    }

    func snapshot() -> [ThunderboltBurstJob] {
        jobs
    }
}
