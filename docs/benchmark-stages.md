# Benchmark Stages Reference

Complete reference for all 8 benchmark stages plus the limit finder mode.

Run command: `swift run -c release benchmark [options] [<media-folder>]`

- No args starts the interactive wizard.
- `<media-folder>` is required for media stages and `--limit`.
- `--stage db` can run without a media folder.

Default suite runs all stages except `comparison` (opt-in via `--stage comparison`).

---

## 1. image

**What it measures:** Single-threaded image processing performance using CoreGraphics/ImageIO.

**Requires:** Media folder with image files.

### Sub-benchmarks

| Function | Iterations | What it does |
|---|---|---|
| `benchmarkImageThumbnails(corpus:)` | 10 per image | Calls `ImageProcessor.generateThumbnail()` at 512px, quality 0.85. Reports p50/p95/min/max per file. |
| `benchmarkImagePreviews(corpus:)` | 10 per image | Calls `ImageProcessor.generatePreview()` at 1440px, quality 0.90. Reports p50/p95/min/max per file. |
| `benchmarkImageTimestamp(corpus:)` | 50 per image | Calls `ImageProcessor.extractTimestamp()` (EXIF read, no I/O output). Reports p50/p95/min/max per file. |
| `benchmarkImageMemory(corpus:)` | 1 per image | Generates one 512px thumbnail per image while sampling `getMemoryMB()` before and after. Reports delta/before/after per file. |

**Outputs:** Per-file timing table for each sub-benchmark. Memory delta table for the memory sub-benchmark.

**Runtime characteristics:** Scales linearly with image count. Thumbnail and preview benchmarks write to a temp directory (auto-cleaned). Timestamp extraction is metadata-only. Runtime depends on corpus size; benchmark output also records hardware profile for context.

---

## 2. video

**What it measures:** Video analysis, decode, transcode, and thumbnail generation using AVFoundation.

**Requires:** Media folder with video files.

### Sub-benchmarks

| Function | Iterations | What it does |
|---|---|---|
| `printVideoEncoders()` | n/a | Queries `VTCopyVideoEncoderList` and prints all available hardware and software encoders with HW/SW type and encoder ID. Informational only. |
| `benchmarkVideoAnalysis(corpus:)` | 1 per video | Calls `getVideoInfo()` via `AVURLAsset.loadTracks()` to extract codec, resolution, FPS, frame count, and duration. No processing, just metadata. |
| `benchmarkDecodeOnly(corpus:)` | 1 per video | Creates `AVAssetReader` with NV12 pixel format output, reads all frames via `copyNextSampleBuffer()` with `alwaysCopiesSampleData = false`. Reports decode FPS and realtime multiplier (decode FPS / source FPS). |
| `benchmarkVideoTranscode(corpus:, preset:, timeout:)` | 1 per video | Calls `VideoProcessor.transcode()` using `AVAssetExportSession`. Concurrently samples CPU usage every 200ms during each transcode. Reports wall time, CPU mean%, CPU p95%, pipeline FPS, output size in MB, compression ratio, and output codec/resolution. |
| `benchmarkVideoThumbnails(corpus:)` | 1 per video | Calls `VideoProcessor.generateThumbnail()` at 512px, time=1.0s, quality 0.85. Reports timing per file. |

**Outputs:** Encoder list table, video metadata table, per-file timing tables for decode/transcode/thumbnails with throughput metrics.

**Runtime characteristics:** Decode-only reports decode FPS and realtime multiplier per video. Transcode timing is reported separately and can vary with source characteristics and configured preset. Videos are transcoded to the configurable preset (default: `AVAssetExportPreset1920x1080`) with a configurable timeout (default: 300s). All outputs written to temp directories (auto-cleaned).

---

## 3. sha256

**What it measures:** SHA-256 hashing throughput over the full media corpus.

**Requires:** Media folder (all files, both images and videos).

### Sub-benchmarks

| Function | Iterations | What it does |
|---|---|---|
| `benchmarkSHA256(corpus:)` | 10 per file | Uses `SHA256Utility.calculateSHA256()` with a fixed reusable read buffer and `CryptoKit` streaming updates (no per-chunk `Data` accumulation). Reports p50/p95/min/max duration and MB/s per file. |

**Outputs:** Per-file timing table with throughput in MB/s.

**Runtime characteristics:** Files are hashed via repeated chunked reads in `SHA256Utility.calculateSHA256()` using CryptoKit streaming updates. Buffer size defaults to 1MB (`SHA256_BUFFER_SIZE`).

---

## 4. db

**What it measures:** SQLite (GRDB) write and read throughput using synthetic workloads. No media folder required.

**Requires:** Nothing (creates temporary databases).

### Sub-benchmarks

All three sub-benchmarks run within `benchmarkDatabase()`:

| Sub-benchmark | What it does |
|---|---|
| Sequential inserts | Creates a fresh DB, inserts 1000 rows sequentially via `db.insertQueued()`. Reports total time and rows/s. |
| Concurrent inserts (8 tasks) | Creates a fresh DB, inserts 1000 rows across 8 concurrent Swift tasks (125 each) via `db.insertQueued()`. Reports total time and rows/s. Measures DatabaseQueue serialization overhead. |
| Reads under write load | Seeds 500 complete rows, then runs 200 writes + 500 reads (4 reader tasks x 125 reads via `db.getAllAssets(limit:offset:)`) concurrently. Reports combined time. |

**Outputs:** Three metric lines showing row counts and durations; the two insert benchmarks also report throughput rates.

**Runtime characteristics:** Database files are created in temp directories with production-matching GRDB configuration (busy timeout, cache size, page size from `BenchDefaults`). Auto-cleaned.

---

## 5. archive

**What it measures:** Copy-to-SSD throughput with SHA-256 verification using `StorageManager.archiveOriginal()`.

**Requires:** Media folder AND external SSD path (`--ssd-path`).

### Sub-benchmarks

| Function | What it does |
|---|---|
| `benchmarkArchiveToSSD(corpus:, ssdPath:, sha256BufferSize:, keepArtifacts:)` | Iterates over every file in the corpus. For each file, calls `StorageManager.archiveOriginal()` which copies the file to `<ssd-path>/bench-results/<run-id>/archive/` and verifies the copy via SHA-256. Times each file individually. |

**Outputs:** Summary table with OK/failed counts, total data in MB, aggregate MB/s throughput, and per-file timing statistics (p50/p95/min/max).

**Runtime characteristics:** The benchmark copies each file to the SSD and verifies it with SHA-256. The benchmark creates a timestamped subdirectory on the SSD. By default, artifacts are removed after the benchmark completes; use `--keep-ssd-bench` to retain them. Checks `VolumeUtils.isMounted()` before proceeding. Skipped entirely if `--ssd-path` is not provided.

---

## 6. thunderbolt

**What it measures:** Thunderbolt bridge worker connectivity, single-video round-trip profiling (local vs remote), and multi-worker burst processing throughput.

**Requires:** Media folder with video files, configured Thunderbolt workers (`TB_WORKERS` env var or `--tb-workers`, format: `host:slots[,host:slots]`), and running worker processes on remote Macs connected via Thunderbolt bridge.

### Sub-benchmarks

All run within `benchmarkThunderbolt(corpus:, preset:, timeout:, hardware:, sweepMode:)`:

| Sub-benchmark | What it does |
|---|---|
| Source binding | Resolves each worker's IPv4 address and matches it to a local Thunderbolt bridge interface route. Reports source IP and bridge interface per worker. |
| Connectivity | For each bound worker, opens a TCP connection with source-IP binding, measures connect latency in milliseconds. Reports reachable/unreachable status per worker. |
| SHA-256 precompute | Hashes all video files via `SHA256Utility.calculateSHA256()` to prepare for integrity verification during transfers. |
| Profiling round-trip | Picks the largest video. Runs a local baseline (thumbnail + transcode). Then sends the same video to each remote worker and measures send, processing, receive, and total round-trip time. Reports per-target breakdown (local vs each remote worker). |
| Capability probe | For each reachable worker, sends a `__kiko_caps__` sentinel with bounded parallelism (<=16 in-flight) and reports detected-worker count (`X/Y detected`). Capability queries are informational; burst remote-slot ranges use configured worker slots directly. |
| Burst sweep | Evaluates local slots `0...localCeiling` (where `localCeiling = min(cores, engines*2+1)`) and remote slots `0...configuredSlots` per worker (excluding all-zero config). For each configuration, runs all videos through concurrent local/remote dispatch. Reports wall time, videos/min, failed count, and p95 completion time; leaderboard is ranked by wall time with speedup vs local-only baseline. |
| Measured FIFO vs CA showdown | Runs FIFO and CA across all three arrival profiles (`all-at-once`, `burst-1-20-5-5-1`, `trickle`). Reports `total_wait(sumW)`, `p95_wait`, `wall_makespan`, and `failed_count` for each policy/profile pair. |
| Sweep strategy | `exhaustive` evaluates all configurations. `smart` runs a 3-phase optimizer: (1) per-machine profiling (`O(sum ceilings)`), (2) additive-throughput prediction + top-K evaluation (`topK` default `10`), (3) local L1 refinement around best candidates. For spaces `<= 25`, smart mode auto-falls back to exhaustive internally. Both modes apply in non-JSON and JSON runs via `--sweep-mode smart|exhaustive`. |

**CA scheduler mode (`--stage thunderbolt`):**
- `--arrival-profile <all-at-once|burst-1-20-5-5-1|trickle>` enables CA scheduler benchmark mode.
- `--scheduler-policy <fifo|ca>` selects policy (default: `fifo` when arrival profile is set).
- `--ca-raw-out <path>` writes full CA JSON artifact (non-`--json` runs).
- `--ca-summary-out <path>` writes CA markdown summary artifact (non-`--json` runs).
- `--ca-acceptance` runs the required acceptance matrix (`fifo` + `ca`) across all three arrival profiles and fails if pass criteria are not met.
- `--ca-acceptance-out <path>` writes acceptance JSON report (`generated_at`, `corpus_video_count`, `corpus_signature`, per-profile metrics/comparisons, `all_pass`).
- Acceptance pass criteria:
  - `ca.sumW_seconds < fifo.sumW_seconds`
  - `ca.failed_count <= fifo.failed_count`
  - `p95`/`makespan` regressions are reported for review when pass criteria still hold.

**JSON mode (`--json --stage thunderbolt`):**
- Without `--arrival-profile`: outputs burst benchmark JSON with workers and best configuration (`--sweep-mode smart|exhaustive` applies).
- With `--arrival-profile`: outputs CA scheduler JSON with `scheduler_policy`, `arrival_profile`, `total_jobs`, `successful_jobs`, `failed_count`, `metrics`, and `jobs`.
- `--ca-acceptance` is non-JSON mode only and should be run without `--json`.

**Outputs:** Binding info, connectivity results, capability-detection summary (`X/Y detected`), per-worker profiling round-trip breakdown, burst sweep results, and a leaderboard table.

**Runtime characteristics:** Requires external Macs with running worker processes. Exhaustive mode runtime is `O(videos * combinations)` where combinations is the cartesian product of slot counts. Smart mode runs optimizer phases, and both modes emit the same best-configuration JSON schema. Settings are resolved from (in priority order): CLI flags, environment variables, `com.kiko.media.plist` EnvironmentVariables. Skipped gracefully if no workers are configured or reachable.

---

## 7. pipeline

**What it measures:** Optimal concurrency levels for image and video processing, mixed-workload behavior, and end-to-end realistic pipeline throughput.

**Requires:** Media folder with image files (videos optional but needed for video/mixed sweeps and realistic pipeline video jobs).

### Sub-benchmarks

| Function | What it does |
|---|---|
| `benchmarkImageConcurrency(corpus:, hardware:)` | **Image concurrency sweep.** Runs `runImagePipeline()` at increasing concurrency levels (derived from CPU core count, e.g. 1, 3, 6, 9, 12 on a 10-core machine). Each level processes `max(imageCount * 3, 20)` jobs. Each job runs thumbnail (512px) + preview (1440px) + timestamp extraction in parallel (matching production). Reports images/min, p50/p95 latency, peak memory, and thermal state per level. |
| `benchmarkVideoConcurrency(corpus:, hardware:, preset:, timeout:)` | **Video concurrency sweep.** Runs `runVideoPipeline()` at levels from 1 to `min(cores, engines*2+1)`. Each level processes `videoCount` jobs. Each job runs thumbnail + transcode in parallel (matching production). Reports videos/min, p50/p95, peak memory, thermal per level. |
| `benchmarkMixedRatioSweep(corpus:, hardware:, preset:, timeout:)` | **Mixed workload sweep.** Runs image and video pipelines simultaneously at various (image, video) concurrency combinations. Image levels are derived from `base = max(1, (imageSweepMax + 3) / 4)`, stepping through `base`, `base+1`, and `base*2` (capped at `imageSweepMax`). Video levels are 1 and `max(2, encodeEngines)`. Reports combined assets/min, individual img/min and vid/min, peak memory, thermal per combination. |
| `printConcurrencyRecommendationCard(...)` | Analyzes sweep results using a "knee point" algorithm (lowest concurrency that achieves >=98% of peak throughput). Prints recommended concurrency settings and suggested `.env` values for `MAX_CONCURRENT_IMAGES` and `MAX_CONCURRENT_VIDEOS`. |
| `benchmarkRealisticPipeline(corpus:, imageConcurrency:, videoConcurrency:, ssdPath:, preset:, timeout:)` | **Full production-matching pipeline.** Uses sweep-derived recommendation when available; otherwise falls back to image concurrency `4` and video concurrency `2`. Image jobs perform: DB insert (queued) -> DB update (processing) -> parallel thumbnail+preview+timestamp -> SHA-256 hash -> SSD archive (if `--ssd-path` provided) -> DB mark complete. Video jobs follow the same flow but run transcode-based preview generation and skip timestamp extraction. Processes `max(imageCount * 3, 24)` image jobs and `videoCount` video jobs simultaneously. Reports total time, completed/failed counts, assets/min throughput, peak memory, thermal state. |

**Outputs:** Three sweep tables with throughput/memory/thermal data, a recommendation card, realistic pipeline summary metrics, and (when video sweep data exists) benchmark-prior maintenance at `{BASE_DIRECTORY}/benchmark-prior.json` (default `~/Documents/kiko-media/benchmark-prior.json`). If no canonical prior exists, the run writes one. Later runs may skip the write, write a candidate artifact, or promote a stronger candidate instead of blindly overwriting the canonical prior. The resulting prior affects production CA only when `VIDEO_SCHEDULER_POLICY=auto`.

**Runtime characteristics:** Sweep levels multiply the total work. The realistic pipeline exercises the full production code path including database operations and optional SSD archival. All temp files are auto-cleaned. The pipeline runners use a bounded task group (`runPipelineLoop`) that maintains up to `maxConcurrent` in-flight tasks and replenishes as tasks finish until all jobs are consumed.

---

## 8. comparison

**What it measures:** Parameter tradeoff curves for JPEG quality, thumbnail dimensions, SHA-256 buffer sizes, and AVFoundation transcode presets.

**Requires:** Media folder (images for JPEG/thumbnail curves, all files for SHA-256, videos for preset comparison).

**Note:** This stage is opt-in. It is excluded from the default full-suite run and must be explicitly requested via `--stage comparison`.

### Sub-benchmarks

| Function | What it does |
|---|---|
| `benchmarkJPEGQualityCurve(corpus:)` | Generates 512px thumbnails at 7 quality levels (0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95) using up to 5 sampled images. Reports average time and average output file size per quality level, with size delta relative to the 0.85 baseline. |
| `benchmarkThumbnailSizeCurve(corpus:)` | Generates thumbnails at 6 dimension levels (256, 384, 512, 640, 768, 1024) at fixed quality 0.85 using up to 5 sampled images. Reports average time and average output file size per size level, with size delta relative to the 512px baseline. |
| `benchmarkSHA256BufferCurve(corpus:)` | Hashes the entire corpus at 6 buffer sizes (64KB, 256KB, 512KB, 1MB, 2MB, 4MB). Reports MB/s throughput per buffer size with delta relative to the 1MB baseline. Single pass per buffer size (no iterations). |
| `benchmarkTranscodePresetComparison(corpus:, timeoutSeconds:)` | Enumerates all `AVAssetExportSession.allExportPresets()`, filters to those compatible with the corpus videos and producing output <= 1920x1080. For each qualifying preset, transcodes all compatible videos and reports average time, average output size, and max output dimensions. Presets producing output larger than 1080p are listed in an "Excluded Presets" table with reasons. Results sorted by average time (fastest first). |

**Outputs:** Four comparison tables showing parameter-vs-performance tradeoff curves.

**Runtime characteristics:** JPEG quality and thumbnail size curves are fast (small sample, no iterations beyond the parameter sweep). SHA-256 buffer curve is I/O-bound and fast. Transcode preset comparison is the slowest sub-benchmark here, as it transcodes every compatible video for every qualifying preset. All temp files auto-cleaned.

---

## Limit Finder (special mode)

**What it measures:** Finds the optimal stable concurrency "knee point" by ramping load until failure thresholds are hit, then binary-searching the boundary, and optionally soak-testing.

**Invocation:** `--limit [--limit-workload image|video|mixed] <media-folder>`

**Requires:** Media folder. Images required for `image`/`mixed` workloads, videos required for `video`/`mixed` workloads.

### Algorithm

1. **Ramp phase:** Starting at `--limit-start` (default 1), increments by `--limit-step` (default 1) up to `--limit-max` (default: cores + 2). At each load level, runs the selected workload pipeline and checks stop conditions.

2. **Refine phase:** If a stable-to-unstable boundary is found with gap > 1, performs up to `--limit-refine-steps` (default 4) binary search iterations to narrow the boundary.

3. **Soak phase:** If `--limit-soak-seconds` > 0, runs the workload continuously at the knee point and at knee+1 for the specified duration, accumulating results to verify stability under sustained load.

### Stop conditions (any triggers "bad" for a step)

- Failure rate > `--limit-error-threshold` (default 0.05)
- P95 latency > `--limit-timeout-threshold` (default 30s)
- Peak memory >= `--limit-memory-cap` (default 4096MB)
- Thermal state >= `--limit-thermal-threshold` (default serious)

### Workload types

| Workload | Pipeline | Concurrency mapping |
|---|---|---|
| `image` | `runImagePipeline()` (thumbnail + preview + timestamp) | load = image concurrency |
| `video` | `runVideoPipeline()` (thumbnail + transcode) | load = video concurrency |
| `mixed` | Both pipelines in parallel | image concurrency = load, video concurrency = min(load, encode_engines + 1) |

### Outputs

- Per-step table: phase, load, assets/min, failure %, P95 latency, peak memory, thermal, stop reason
- Recommendation: knee (last good), max stable, first unstable, confidence notes
- Optional JSON output (`--limit-json-out <path>`): full run metadata, config, all step results, and recommendation

**Runtime characteristics:** By default, each step runs `max(imageCount * 3, 24)` image jobs and/or `videoCount` video jobs; you can override these with `--limit-image-jobs` and `--limit-video-jobs`. Total runtime depends on how many ramp/refine/soak steps execute before hitting the boundary.

---

## Common infrastructure

- **Temp directories:** All benchmarks write to `$TMPDIR/kiko-bench-*-<uuid>`, auto-cleaned on completion.
- **Media files:** Never modified. Corpus files are reused across jobs (round-robin `idx % count`).
- **Reports:** Written to `bench-results/` (configurable via `--report-dir`). Each run gets a report file.
- **Benchmark prior artifact:** Written to `{BASE_DIRECTORY}/benchmark-prior.json` (default `~/Documents/kiko-media/benchmark-prior.json`) and independent of `--report-dir`.
- **Canonical resolver behavior:** Both pipeline and thunderbolt benchmark flows emit/update benchmark prior via the same canonical path resolver.
- **Hardware detection:** `HardwareProfile.detect()` reads chip name (`machdep.cpu.brand_string`), P/E core counts (`hw.perflevel0/1.physicalcpu`), memory (`hw.memsize`), video encode engine count (IOKit `IOServiceNameMatching("ave2")` instance count), and HW encoder list (`VTCopyVideoEncoderList`).
- **Thermal/memory monitoring:** `getThermalState()` and `getMemoryMB()` sampled at key points throughout all stages.
- **Memory guardrail:** Full benchmark mode enforces a process memory guardrail (`BenchmarkMemoryGuard`) with warning + stop thresholds. Default stop limit is the lesser of 80% of physical RAM and `(physical RAM - 512MB)`, clamped to at least 1GB. Override with `BENCH_MEMORY_LIMIT_MB=<mb>` or disable with `BENCH_MEMORY_LIMIT_MB=0`.
- **BenchDefaults:** All processing parameters (max image pixels, max dimension, compression ratio, SHA-256 buffer size, SQLite settings) are derived from `Config` defaults to match production behavior exactly.
