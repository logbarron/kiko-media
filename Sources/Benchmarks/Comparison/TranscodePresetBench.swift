import Foundation
import AVFoundation
import KikoMediaCore

func benchmarkTranscodePresetComparison(
    corpus: [MediaFile],
    timeoutSeconds: Int
) async {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Transcode Preset Comparison (<= 1080p, .mp4)")
    BenchmarkRuntimeRenderer.printField("Timeout", "\(timeoutSeconds)s")

    let fm = FileManager.default
    let inputVideos = corpus.filter { $0.type == .video }
    guard !inputVideos.isEmpty else {
        BenchOutput.line("  No video files in media folder, skipping")
        return
    }

    let allPresets = Array(Set(AVAssetExportSession.allExportPresets())).sorted()

    struct VideoCandidate {
        let file: MediaFile
        let pixels: Int
        let compatiblePresets: Set<String>
    }

    var videos: [VideoCandidate] = []
    videos.reserveCapacity(inputVideos.count)

    for file in inputVideos {
        do {
            let info = try await getVideoInfo(path: file.path)
            let asset = AVURLAsset(url: URL(fileURLWithPath: file.path))
            let compatible = compatibleMP4Presets(asset: asset, presets: allPresets)
            videos.append(VideoCandidate(
                file: file,
                pixels: info.width * info.height,
                compatiblePresets: compatible
            ))
        } catch {
            BenchOutput.line("  Skipping \(file.name) (unreadable video: \(error))")
        }
    }

    guard !videos.isEmpty else {
        BenchOutput.line("  No readable video files in media folder, skipping")
        return
    }

    BenchmarkRuntimeRenderer.printField("Files", "\(videos.count) videos")
    BenchmarkRuntimeRenderer.printField("Presets", "\(allPresets.count) total (testing compatible presets only)")
    BenchmarkRuntimeRenderer.printField("Filter", "output long side <= 1920 and short side <= 1080")
    BenchOutput.line("")

    let tmpDir = makeTempDir("transcode-presets")
    defer { cleanup(tmpDir) }

    struct ResultRow {
        let preset: String
        let videoCount: Int
        let avgSeconds: Double
        let avgOutBytes: Double
        let maxOutputWidth: Int
        let maxOutputHeight: Int
    }

    struct ExcludedPreset {
        let preset: String
        let reason: String
    }

    var rows: [ResultRow] = []
    var excluded: [ExcludedPreset] = []
    rows.reserveCapacity(allPresets.count)
    excluded.reserveCapacity(allPresets.count)

    for preset in allPresets {
        let compatible = videos.filter { $0.compatiblePresets.contains(preset) }
        guard !compatible.isEmpty else { continue }

        // Gate: pick the highest-resolution compatible input to decide if this preset stays <= 1080p.
        guard let gate = compatible.max(by: { $0.pixels < $1.pixels }) else { continue }

        var durations: [Double] = []
        var outBytes: [Double] = []
        var maxOutput: (width: Int, height: Int, pixels: Int)?

        func runOnce(file: MediaFile, index: Int) async -> (seconds: Double, outBytes: Int, width: Int, height: Int)? {
            let out = "\(tmpDir)/\(sanitizePathComponent(preset))-\(index).mp4"
            do {
                let d = try await measureAsync {
                    try await VideoProcessor.transcode(
                        sourcePath: file.path,
                        outputPath: out,
                        timeoutSeconds: timeoutSeconds,
                        preset: preset
                    )
                }

                let attrs = (try? fm.attributesOfItem(atPath: out)) ?? [:]
                let size = (attrs[.size] as? NSNumber)?.intValue ?? 0

                let info = try await getVideoInfo(path: out)
                try? fm.removeItem(atPath: out)

                return (d.seconds, size, info.width, info.height)
            } catch {
                try? fm.removeItem(atPath: out)
                return nil
            }
        }

        func recordOutputDimensions(width: Int, height: Int) {
            let pixels = width * height
            if let current = maxOutput {
                if pixels > current.pixels {
                    maxOutput = (width: width, height: height, pixels: pixels)
                }
            } else {
                maxOutput = (width: width, height: height, pixels: pixels)
            }
        }

        // Run gate transcode.
        guard let gateResult = await runOnce(file: gate.file, index: 0) else {
            excluded.append(ExcludedPreset(preset: preset, reason: "failed to export"))
            continue
        }

        if !is1080pOrLess(width: gateResult.width, height: gateResult.height) {
            excluded.append(ExcludedPreset(
                preset: preset,
                reason: "output \(gateResult.width)x\(gateResult.height) exceeds 1920x1080"
            ))
            continue
        }

        durations.append(gateResult.seconds)
        outBytes.append(Double(gateResult.outBytes))
        recordOutputDimensions(width: gateResult.width, height: gateResult.height)

        // Run remaining compatible videos.
        var idx = 1
        for candidate in compatible {
            guard candidate.file.path != gate.file.path else { continue }
            guard let r = await runOnce(file: candidate.file, index: idx) else {
                idx += 1
                continue
            }
            if !is1080pOrLess(width: r.width, height: r.height) {
                excluded.append(ExcludedPreset(
                    preset: preset,
                    reason: "output exceeded 1920x1080 for \(candidate.file.name) (\(r.width)x\(r.height))"
                ))
                durations.removeAll(keepingCapacity: true)
                outBytes.removeAll(keepingCapacity: true)
                break
            }

            durations.append(r.seconds)
            outBytes.append(Double(r.outBytes))
            recordOutputDimensions(width: r.width, height: r.height)
            idx += 1
        }

        guard !durations.isEmpty, let maxOutput else { continue }

        let denom = Double(durations.count)
        rows.append(ResultRow(
            preset: preset,
            videoCount: durations.count,
            avgSeconds: durations.reduce(0, +) / denom,
            avgOutBytes: outBytes.reduce(0, +) / denom,
            maxOutputWidth: maxOutput.width,
            maxOutputHeight: maxOutput.height
        ))
    }

    rows.sort { $0.avgSeconds < $1.avgSeconds }

    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Preset", width: 24),
        BenchmarkRuntimeTableColumn(header: "Videos", width: 6, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Avg time", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Avg size", width: 8, alignment: .right),
        BenchmarkRuntimeTableColumn(header: "Max output", width: 10),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for row in rows {
        let presetLabel = displayPresetName(row.preset)
        let timeLabel = fmt(row.avgSeconds)
        let sizeLabel = String(format: "%.1fMB", row.avgOutBytes / (1024.0 * 1024.0))
        let outLabel = "\(row.maxOutputWidth)x\(row.maxOutputHeight)"

        BenchmarkRuntimeRenderer.printTableRow(
            [
                presetLabel,
                "\(row.videoCount)",
                timeLabel,
                sizeLabel,
                outLabel,
            ],
            columns: columns
        )
    }

    if !excluded.isEmpty {
        BenchOutput.line("")
        BenchmarkRuntimeRenderer.printSubsectionTitle("Excluded Presets")
        let excludedColumns: [BenchmarkRuntimeTableColumn] = [
            BenchmarkRuntimeTableColumn(header: "Preset", width: 24),
            BenchmarkRuntimeTableColumn(header: "Reason", width: 56),
        ]
        BenchmarkRuntimeRenderer.printTableHeader(excludedColumns)
        for item in excluded {
            BenchmarkRuntimeRenderer.printTableRow(
                [
                    displayPresetName(item.preset),
                    item.reason,
                ],
                columns: excludedColumns
            )
        }
    }
}

private func is1080pOrLess(width: Int, height: Int) -> Bool {
    let longSide = max(width, height)
    let shortSide = min(width, height)
    return longSide <= 1920 && shortSide <= 1080
}

private func compatibleMP4Presets(asset: AVAsset, presets: [String]) -> Set<String> {
    var out = Set<String>()
    out.reserveCapacity(presets.count)

    for preset in presets {
        guard let session = AVAssetExportSession(asset: asset, presetName: preset) else { continue }
        if session.supportedFileTypes.contains(.mp4) {
            out.insert(preset)
        }
    }

    return out
}

private func sanitizePathComponent(_ s: String) -> String {
    let underscore = UnicodeScalar(95)!
    let hyphen = UnicodeScalar(45)!
    let mapped = s.unicodeScalars.map { scalar -> Character in
        if CharacterSet.alphanumerics.contains(scalar) || scalar == underscore || scalar == hyphen {
            return Character(scalar)
        }
        return "_"
    }
    return String(mapped)
}

private func displayPresetName(_ preset: String) -> String {
    if preset.hasPrefix("AVAssetExportPreset") {
        return String(preset.dropFirst("AVAssetExportPreset".count))
    }
    if let last = preset.split(separator: ".").last {
        return String(last)
    }
    return preset
}
