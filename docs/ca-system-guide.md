# CA Scheduling System Guide

Implementation-truthful reference for the complexity-aware (CA) video scheduling subsystem as it exists today.

This document is descriptive, not aspirational. It intentionally avoids exact line-number citations because they drift quickly and are not durable ground truth. Symbols and files are the stable anchors.

---

## 1. System Overview

### Scope

CA scheduling only changes **video** selection, routing, holding, and benchmark/showdown evaluation. Image processing stays FIFO.

### Primary blast radius

| Concern | Primary files |
|---|---|
| Solver and batch objective | `Sources/KikoMediaCore/ComplexityAwareScheduler.swift` |
| Production queueing, holds, recompute | `Sources/KikoMediaCore/MediaProcessor.swift` |
| Production dispatch execution and local fallback/EMA updates | `Sources/KikoMediaCore/MediaProcessor+Processing.swift` |
| Production and benchmark scheduling metric math | `Sources/KikoMediaCore/SchedulingMetrics.swift` |
| Production startup recovery, artifact verification, SSD rebuild | `Sources/KikoMediaCore/MediaProcessor+Recovery.swift` |
| Production held-job state | `Sources/KikoMediaCore/CAPendingHeldVideoState.swift` |
| Production projected topology and hold/dispatch split | `Sources/KikoMediaCore/CAProjectedSlotSelection.swift` |
| Production active-video bookkeeping and transient exclusions | `Sources/KikoMediaCore/CADispatchState.swift` |
| Hold invalidation rules | `Sources/KikoMediaCore/CAHoldInvalidation.swift` |
| Recompute coalescing and completion refill priority | `Sources/KikoMediaCore/CARecomputeCoordinator.swift` |
| Production remote dispatch, slot health, drift, live remote telemetry | `Sources/KikoMediaCore/ThunderboltDispatcher.swift` |
| Transport protocol, capability query framing, raw I/O helpers | `Sources/KikoMediaCore/ThunderboltTransport.swift` |
| Worker capability payload and sweep ceiling | `Sources/KikoMediaCore/ThunderboltCapabilities.swift` |
| Reachability and capability probes | `Sources/KikoMediaCore/ThunderboltWorkerProbe.swift` |
| Raw benchmark round-trip telemetry measurement | `Sources/KikoMediaCore/ThunderboltRawExecution.swift` |
| Tail-telemetry seed measurement | `Sources/KikoMediaCore/ThunderboltTailTelemetrySeedMeasurement.swift` |
| Prior/profile shaping helpers | `Sources/KikoMediaCore/CAProfileAndFallbackMath.swift` |
| Benchmark runtime scheduler and hold/rebase behavior | `Sources/KikoMediaCore/CABenchmarkRuntimeEngine.swift` |
| Benchmark run coordinator | `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltCARunCore.swift` |
| Shared remote model decisions and confidence tiers | `Sources/KikoMediaCore/CARemoteModelDecision.swift` |
| Shared topology/model builder | `Sources/KikoMediaCore/CATopologyModelBuilder.swift`, `Sources/KikoMediaCore/CATopologyModelAssembly.swift`, `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltCAModelBuilder.swift` |
| Benchmark settings, bridge binding, connectivity, worker capability probes | `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltNetwork.swift` |
| Benchmark helper utilities (prior paths, arrival offsets, frame-count fallback) | `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltUtilities.swift` |
| Prior artifact format and lookup | `Sources/KikoMediaCore/BenchmarkPrior.swift` |
| Prior promotion policy | `Sources/KikoMediaCore/BenchmarkPriorPolicy.swift` |
| Showdown winner/verdict policy | `Sources/KikoMediaCore/BenchmarkShowdownPolicy.swift` |
| Showdown orchestration and summary rendering | `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltShowdownOrchestration.swift` |
| Showdown scoring and guidance helpers | `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltShowdownScoring.swift` |
| Prior maintenance and promotion application | `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltShowdownPriorMaintenance.swift` |
| Benchmark entrypoint and burst sweep | `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltEntryPoints.swift`, `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltBurstSweepOrchestration.swift`, `Sources/Benchmarks/Steps/Thunderbolt/ThunderboltBurstSweepExecution.swift` |
| Tick protocol and rollout gate | `Sources/KikoMediaCore/ProgressTickV2.swift`, `deploy/worker.swift.template` |
| Adaptive estimators | `Sources/KikoMediaCore/LiveAdaptiveMSPerFrameC1Estimator.swift`, `Sources/KikoMediaCore/TransferOverheadEstimator.swift`, `Sources/KikoMediaCore/ThunderboltAdaptiveTelemetryReducer.swift` |
| Production CA activation gate | `Sources/KikoMediaCore/CAActivationGate.swift` |

### What CA scheduling is

CA scheduling replaces FIFO video dispatch with a solver-driven batch chooser.

The solver:

1. Enumerates every currently legal `(job, machine, slot)` choice.
2. Scores each choice using projected completion time, transfer overheads, fixed overhead, degradation, and a small concurrency penalty.
3. Chooses a batch using `batchPlanLessThan` semantics:
   - maximize picked-job count first
   - then minimize projected makespan
   - then minimize projected completion sum
   - then fall back to deterministic tie-breaks

The same pick can become either:

- a **dispatch**, if the chosen slot is ready now
- a **hold**, if the chosen slot is predicted to finish sooner than dispatching immediately somewhere else

Dispatch uses a two-stage solver pipeline (`pickTwoStageBatch`):

1. **Stage 1 (ready-now):** fills all currently free slots with ready-now jobs using `readyPolicy: .readyNowOnly`
2. **Stage 2 (reservations):** creates reservation picks for remaining jobs using `readyPolicy: .includeFutureReady` on the projected machines from stage 1

A reservation pick is only created when:

- the target slot is future-ready (`tReadySlotMS > nowMS`)
- its projected `tDoneMS` improvement over the best ready-now alternative on the current projected state exceeds `reservationMinimumBenefitMS` (currently `1`)

A currently free slot is never left unused when a ready-now job exists.

### Execution contexts

The subsystem exists in three materially different contexts:

1. **Production**
   - Entry point: `MediaProcessor.processQueues`
   - Real webhook arrivals
   - Real local processing
   - Real Thunderbolt dispatch
   - Holds become timer-driven wakeups

2. **Benchmark single run**
   - Entry point: `runThunderboltCA`
   - Uses `CABenchmarkRuntimeEngine`
   - Local slots use the benchmark local runner
   - Remote slots use real Thunderbolt workers
   - Holds are polled and requeued inside the benchmark runtime

3. **Benchmark showdown**
   - Entry point: `benchmarkThunderboltMeasuredShowdown`
   - Runs FIFO and CA in both orders for each arrival profile
   - Computes order-neutral averages
   - Produces verdict and optional prior-promotion decision support

### Production and benchmark model path

Production and benchmark now share the same model-construction path through `CATopologyModelBuilder.build(...)`.

Both contexts call this shared builder, which uses `CARemoteModelDecisionKernel.resolve(...)` internally to produce `CARemoteModelDecision` values with confidence-tiered remote modeling:

- **`exactPrior`** — exact worker signature match in the prior table (multiplier `1.00`, no concurrency cap)
- **`hardwareCompatiblePrior`** — same hardware/preset key, closest OS version (multiplier `1.15`, concurrency capped to `1`)
- **`capabilityBacked`** — remote capabilities provide runtime slope/curve (multiplier `1.25`, concurrency capped to `1`)
- **`localFallback`** — local profile values used as conservative remote estimate (multiplier `1.35`, concurrency capped to `1`)

Lower-confidence tiers also receive conservative comparable degradation curves.

Production passes `mode: .auto` to the shared builder. Benchmark passes the mode selected by the user (`.strict` or `.auto`).

Live remote EMA and transfer/tail telemetry can override runtime/overhead terms on top of the modeled worker, regardless of confidence tier.

### How FIFO and CA differ

| Decision point | FIFO | CA |
|---|---|---|
| Job selection | Queue order | Solver-selected batch |
| Slot selection | First available route, remote-first then local | Solver-scored across all modeled machines/slots |
| Future-ready slots | Ignored | Considered in stage 2 (reservations) via `readyPolicy: .includeFutureReady`, after stage 1 fills all ready-now slots |
| Holds | None | Yes |
| Recompute | Not used for routing | Arrival, completion, remote fail, slot-down batch, slot-up, ETA drift |
| Local live runtime EMA | Updated but not consumed by FIFO routing | Updated and consumed |
| Remote live runtime EMA | Updated but not consumed by FIFO routing | Updated and consumed when the worker is modeled (any confidence tier) in production or benchmark |

---

## 2. Data Flow: Production Path

### 2.1 Webhook arrival to queued job

The production path starts in `Sources/KikoMedia/WebhookHandler.swift`.

`WebhookHandler.handleUploadComplete`:

1. Validates the tusd payload.
2. Resolves and sanitizes the upload path.
3. Rejects queue-full/shutdown cases before DB insert when possible.
4. Detects image vs video.
5. Inserts the queued asset row.
6. Calls `processor.enqueueWebhookAsset(...)`.

Inside `MediaProcessor.enqueue(...)`:

1. Videos go through `makeQueuedJob(...)`, which calls `resolveVideoCostForQueue(...)`.
2. `resolveVideoCostForQueue(...)` races `VideoProcessor.probeRuntimeEstimate(...)` against `metadataProbeTimeoutNanos == 25ms`.
3. The probe outcome is passed to `CAProfileAndFallbackMath.resolveVideoCost(...)` which produces a canonical `CAResolvedVideoCost` with:
   - `frameCount` — probed or fallback-derived
   - `durationSeconds` — probed or nil
   - `runtimeSeconds` — probed or fallback-derived
   - `confidence` — `.high` on successful probe, `.low` otherwise
   - `derivation` — tracing how each field was resolved
4. Shared runtime-fallback constants from `CAProfileAndFallbackMath` are:
   - `defaultDurationSeconds == 60`
   - `minimumPositiveSeconds == 0.001`
   - if modeled runtime cannot be derived, `runtimeSecondsFallback(...)` uses `max(0.001, duration * 2.0)`
5. The `ProcessingJob` is constructed with the resolved cost record.
6. `enqueueJob(...)` appends video jobs into `pendingHeldVideoState`.
7. `schedulingVideoIDs` is updated for video jobs.
8. `signalRecompute(.arrive)` is emitted when CA is active.
9. `processQueues(reconsiderHeldJobs: true)` starts immediately.

`ProcessingJob` is the core queue-carrier type for production scheduling. It carries:

- `uploadId`
- `originalName`
- `filePath`
- `assetType`
- `arrivalAtSeconds`
- `resolvedVideoCost: CAResolvedVideoCost?`
- `isRepair`
- `restoreStatus`

The init still accepts legacy fields (`estimatedVideoRuntimeSeconds`, `frameCount`, `probedDurationSeconds`, `videoEstimateConfidence`) for backward compatibility, but resolves them into a single `CAResolvedVideoCost` at construction time via `CAProfileAndFallbackMath.resolveVideoCost(...)`.

### 2.2 `processQueues`: production scheduling loop

`MediaProcessor.processQueues(...)` is the production scheduler.

There are two internal entry points:

- `processQueues(reconsiderHeldJobs:)` — actor-internal, generic recompute (arrivals, slot changes)
- `processCompletionRefillQueues(reconsiderHeldJobs:)` — private, completion-driven refill (video completions)

Both delegate to the private `processQueues(reconsiderHeldJobs:requestedPassKind:)`.

The loop behaves as follows:

1. The function is shutdown-guarded up front: if `isShuttingDown == true`, it returns immediately.
2. If `reconsiderHeldJobs == true`, it calls `reconsiderHeldVideoJobsForRecompute()`, which evaluates each held entry against its target slot's current state and only invalidates impacted holds (slot down, ready-at drift beyond `CAHoldInvalidation.targetReadyAtDriftThresholdMS == 250`, or target slot impossible). Stable holds are preserved.
3. Otherwise, it only releases held jobs whose `wakeAt <= now`.
4. `recomputeCoordinator.beginRecomputeRun(requestedPassKind:reconsiderHeldJobs:)` prevents overlapping runs.
   - if a run is already in flight, it marks the appropriate pending pass (`completionRefillPending` or `genericRecomputePending`) and preserves whether that pending pass should run with held-job reconsideration enabled, then the current call returns
   - `finishRecomputeRun()` is paired with the successful begin through `defer`
5. Each pass starts by:
   - `beginRecomputePass()` which returns a `CARecomputeCoordinator.Pass`, prioritizing `.completionRefill` over `.genericRecompute`
   - the returned `Pass` also carries `reconsiderHeldJobs`, so a coalesced completion-refill pass can still execute with held-job reconsideration even if the outer run started as a generic pass
   - `await Task.yield()`
6. Each pass then:
   - dispatches images FIFO up to `maxConcurrentImages`
   - dispatches videos until no more progress is possible
   - re-queries `currentVideoSlotCapacity()` on every inner video-loop iteration
   - if the executing pass has `reconsiderHeldJobs == true`, no queued video jobs remain, and some CA slot is ready now, production performs one extra held-only idle-capacity reconsideration by releasing held jobs back into queued order and re-running selection
7. After each pass:
   - image queue compaction may run
   - queued video compaction may run
   - if CA is active and a dispatcher exists, `ThunderboltDispatcher.noteBaselineSnapshot()` seeds drift baselines from current remote ETA estimates
8. The outer loop repeats while `recomputeCoordinator.requiresAnotherRecomputePass == true`.

### 2.3 Video dequeue: FIFO path vs CA path

`dequeueNextVideoJobs(maxCount:)` is the switch point.

#### FIFO path

FIFO:

1. Calls `fifoRoutingDirectives(limit:)`.
2. Consumes queued jobs in queue order with `pendingHeldVideoState.takeNextQueued()`.
3. Routes remote-first, then local.

Implementation detail:

- if a real dispatcher exists, FIFO only uses remote slots that are idle and not down in the dispatcher's snapshot
- if a test override exists without a dispatcher, FIFO can synthesize remote routes from config

#### CA path

CA:

1. Reads the queued slice from `pendingHeldVideoState.queuedStartIndex ..< queuedEndIndex`.
2. Builds `CAJob` values with `makeComplexityAwareJob(...)`.
3. Uses the queued absolute index as both:
   - the pending token
   - the `enqueueOrder`
4. Frame count comes directly from `job.resolvedVideoCost?.frameCount`. If the resolved cost is missing or the frame count is not finite and positive, a precondition failure is triggered. There is no separate solver-side fallback chain — the canonical `CAResolvedVideoCost` resolved at enqueue time is the single source of truth for both solver and dispatch.
5. Calls `complexityAwareProjectedSlotSelection(nowMS:)`.

### 2.4 Production projected topology assembly

`complexityAwareProjectedSlotSelection(nowMS:)` builds the machine/slot view used by the solver.

It delegates to `productionTopologyModelBuild(localProfile:)`, which calls `CATopologyModelBuilder.build(mode: .auto, ...)` — the same shared builder used by benchmark. The build result is a `CATopologyModelBuildResult` carrying machine profiles, slot bindings, and diagnostics.

#### Local machine

Local assembly uses:

- `localComplexityAwareProfile` as the required local prior/profile
- `localRemainingRuntimeSnapshotMS(...)` for active local slots
- `localLiveMSPerFrameC1EMA ?? localComplexityAwareProfile.msPerFrameC1` for local runtime slope
- local machine ID `local`
- local slot IDs `local#s1`, `local#s2`, ...

Local remaining runtime is computed by `CADispatchState.localRemainingRuntimeSnapshotMS(...)`, which:

1. only includes active jobs considered local
2. estimates each active job's runtime through `runtimeEstimateForActiveVideoJob(...)`
3. subtracts elapsed local runtime if `markLocalVideoRuntimeStart(...)` has happened
4. sorts the remaining runtimes

`runtimeEstimateForActiveVideoJob(...)` prefers:

1. frame-count-based estimation via `CAProfileAndFallbackMath.runtimeSeconds(...)` using current local live EMA or local prior, plus current projected local degradation — only when `frameCountSource != .defaultFallback`
2. otherwise `max(0.001, videoCost.runtimeSeconds)` when finite
3. otherwise `0.001` as a minimal stub value

#### Remote machines

Remote workers are included via the shared `CATopologyModelBuilder`, which uses `CARemoteModelDecisionKernel.resolve(...)` to evaluate each worker through a confidence-tiered cascade:

1. **`exactPrior`** — exact worker signature match in prior table (multiplier `1.00`)
2. **`hardwareCompatiblePrior`** — same hardware/preset key, closest OS version (multiplier `1.15`, concurrency capped to `1`)
3. **`capabilityBacked`** — capability-reported runtime slope/curve (multiplier `1.25`, concurrency capped to `1`)
4. **`localFallback`** — local profile values as conservative remote estimate (multiplier `1.35`, concurrency capped to `1`)

A worker is excluded only when none of these tiers can produce a valid model.

For each included worker:

- `machine.id` is `"<host>:<port>"`
- `msPerFrameC1` uses `worker.liveMSPerFrameC1 ?? decision.msPerFrameC1` (with confidence multiplier applied)
- `fixedOverheadMS` comes from the decision (with confidence multiplier applied)
- `degradationCurve` comes from the decision (lower-confidence tiers use conservative comparable curves)
- `txInMS` uses `max(0, worker.transferOverheadEstimateMS ?? 0)`
- `txOutMS` uses `max(0, worker.txOutEstimateMS ?? 0)`
- `publishOverheadMS` uses `max(0, worker.publishOverheadEstimateMS ?? 0)`

Busy remote slot `readyAtMS` is:

- `nowMS + max(1, slot.estimatedRemainingMS ?? 1_000)` when busy
- `nowMS` when idle

`slot.estimatedRemainingMS` in dispatcher snapshots prefers:

1. the latest tick-stream ETA
2. otherwise observed remote process EMA minus elapsed time
3. otherwise prior P50 estimate for the current concurrency minus elapsed time

### 2.5 `CAProjectedSlotSelection.plan`

`CAProjectedSlotSelection.Assembly.plan(...)` is the production bridge into the solver.

The important production wrapper types in this layer are:

- `CAProjectedSlotSelection.PendingJob`
- `CAProjectedSlotSelection.DispatchPick`
- `CAProjectedSlotSelection.HoldPick`
- `CAProjectedSlotSelection.Plan`
- `CAProjectedSlotSelection.Route`
- `CAProjectedSlotSelection.MachineContext`
- `CAProjectedSlotSelection.Assembly`
- `CARemoteSlotKey`

It:

1. Converts queued jobs into `CAPendingPickJob`s.
2. Resolves per-job excluded remote slots into concrete `CASlotRef`s.
3. Calls `ComplexityAwareScheduler.pickTwoStageBatch(... maxReadyNowCount:)`.
4. Walks the resulting `CATwoStagePickResult`:
   - `readyNowPicks` become dispatches
   - `reservationPicks` become holds (with `wakeAtMS = max(pick.score.tReadySlotMS, nowMS)`)
5. Produces:
   - `dispatches`
   - `holds`
   - `consumedTokens`
   - `clearedExcludedTokens`

`clearedExcludedTokens` can mean either:

- the solver had to clear an excluded slot because that job had no other legal choice
- the job had an excluded remote slot but the chosen remote slot was different, so the exclusion is no longer needed

For holds, `wakeAtMS` is `max(pick.score.tReadySlotMS, nowMS)`. Note that `tReadySlotMS` is the solver's capacity-constrained slot ready time, which may differ from the raw `slot.readyAtMS`.

### 2.6 Back in `processQueues`: consumed queue items, holds, and launches

After `plan(...)` returns:

1. transient remote exclusions are cleared for `clearedExcludedTokens`
2. every `consumedToken` is removed from the queued array
3. dispatch picks become `VideoDequeueSelection`s
4. hold picks become `CAPendingHeldVideoState.HeldJob`s
5. `storeHeldJobs(...)` updates held state and returns the next wake plan
6. `applyHeldVideoWakePlan(...)` arms a `Task.sleep(...)` wake for the earliest hold

`VideoDequeueBatch.madeHoldDecisions` becomes `true` when at least one hold was stored, even if there were no immediate dispatches. That detail matters because the inner scheduling loop will keep iterating once more after a pure-hold batch.

### 2.7 Dispatch execution: remote first when selected, then local fallback if needed

`MediaProcessor.process(job:)` calls `processVideo(...)` for video jobs.

Before routing, `process(job:)` computes a dispatch-time `resolvedFrameCount` via `dispatchFrameCount(for:)`, which simply returns `job.resolvedVideoCost?.frameCount`. This is the same resolved cost record used by the solver-side `makeComplexityAwareJob(...)`, so solver and dispatch always use identical frame counts.

#### Remote path

When the stored routing directive is `.remote(...)` and either a dispatcher or test override exists:

1. `beginRemoteVideoDispatch(uploadId:)` marks the upload as in-flight remote.
2. Timestamp extraction starts concurrently.
3. The remote dispatch happens through:
   - test override, or
   - `ThunderboltDispatcher.dispatch(...)`
4. Result handling:
   - `.success`
     - clears transient exclusion state
     - returns remote success
   - `.transientRetry(slotHealthDown:)`
     - may preserve a transient remote exclusion for that slot when CA is active and the slot itself did not go down
     - returns `transientRemoteRetry = true`
     - `process(job:)` then calls `requeueForTransientRemoteFailure(...)`
   - `.permanentFailure`
     - clears exclusion state
     - continues into the shared local-fallback tail
   - `.fallbackLocal`
     - clears exclusion state
     - continues into the shared local-fallback tail
5. Any remote path that does not return success or transient-retry then runs the shared fallback-local tail:
   - timestamp extraction is cancelled
   - `processVideo(...)` applies `.unavailable`
   - this is a second outcome application after the earlier `.permanentFailure` or `.fallbackLocal` marker
   - local processing starts

#### Local path

Local video execution:

1. Calls `markLocalVideoRuntimeStart(uploadId:)`.
2. Runs thumbnail, preview transcode, and timestamp extraction concurrently.
3. If thumbnail and preview both succeed and frame count is finite and positive:
   - subtracts local fixed overhead
   - divides by frame count
   - normalizes back to concurrency-1 using the local degradation curve at current local concurrency
   - feeds the normalized observation into `LiveAdaptiveMSPerFrameC1Estimator.next(...)`

Unlike benchmark mode, local fallback after a failed remote attempt still contributes to the **production** local live EMA if the local fallback work succeeds.

#### Archive and terminalization path

After successful processing and cancellation checks:

- repair jobs skip archive entirely
- non-repair jobs launch `archiveOriginal(...)` in a detached task
- archive outcomes behave like this:
  - `.success`
    - continue to terminal status persistence
  - `.ssdUnavailable` or `.ssdWriteFailed`
    - log "retry on restart"
    - return early without terminalizing the asset
  - `.failed`
    - clean up derived files
    - mark the asset `.failed`
    - return
  - `.checksumMismatch`
    - mark the asset `.failed`
    - keep the upload for review
    - return
  - `.verificationFailed`
    - mark the asset `.failed`
    - keep the upload for review
    - return
- after successful terminalization, non-repair jobs remove the upload payload and `.info` sidecar

### 2.8 Completion and recompute

`jobCompleted(uploadId:)`:

1. removes the task from `activeTasks`
2. removes the job type from `activeJobs`
3. decrements image count or completes active-video state in `CADispatchState`
4. drains any deferred transient requeue and re-enqueues it unless shutting down
5. checks `freedVideoCapacity`:
   - if only an image slot was freed, calls `processQueues()` with generic recompute and returns early
   - if a video slot was freed, continues to step 6
6. emits `signalRecompute(.finish)` when CA is active (video completions only)
7. calls `processCompletionRefillQueues(reconsiderHeldJobs: true)` which uses `.completionRefill` pass kind for priority refill

Dispatcher-originated recomputes arrive through `requestRecomputeFromDispatcher(trigger:)`.

Current trigger flow:

- `.arrive` originates in `MediaProcessor` as `.genericRecompute`
- `.finish` originates in `ThunderboltDispatcher` and maps to `.completionRefill`
- `.fail`, `.slotDownBatch`, `.slotUp`, and `.etaDrift` originate in `ThunderboltDispatcher` and map to `.genericRecompute`

Two failure-handling details from `MediaProcessor+Processing.swift` are easy to miss:

- `requeueForTransientRemoteFailure(...)`
  - deletes derived files
  - persists status back to `.queued`
  - defers the actual re-enqueue until `jobCompleted(...)` clears the still-in-flight active slot
  - clears transient exclusion state and aborts if queued-status persistence fails
  - on that persistence-failure path, the job is not re-enqueued; it stays `.processing` in the DB until startup recovery
- `handleCompletionPersistenceFailure(...)`
  - only applies to non-repair jobs
  - calls `database.reEnqueueForRetry(id:)`
  - uses `completionPersistenceStartupRetryLimit == 1`
  - first failure keeps the upload queued for startup retry
  - second failure marks the asset `.failed` and leaves the upload in place for manual review

Terminal-status persistence also has a shared retry policy:

- `updateTerminalStatusLogged(...)` retries at `0ms`, `50ms`, `200ms`
- `markCompleteTerminalLogged(...)` retries at `0ms`, `50ms`, `200ms`

Shutdown behavior is coupled to the same scheduling path:

- `shutdown()` calls `recomputeCoordinator.cancelPendingSlotDownBatchFlush()`
- it clears held and queued video state through `pendingHeldVideoState.clearAll()`
- it clears deferred transient requeues, selected routing, and transient exclusions through `dispatchState.clearShutdownPendingState()`
- mutating hold/recompute helpers thread `allowScheduling: !isShuttingDown` so new wake tasks and deferred slot-down flushes stop being armed during shutdown

### 2.9 Scheduling metrics in production

Production scheduling metrics are CA-only.

`recordSchedulingSuccessIfNeeded(...)` records successful **video** completions as `SchedulingSuccessfulJob(arriveAtSeconds, liveAtSeconds)`.

`recordStatusTransition(...)` maintains the failed-ID set for video jobs already known to the scheduler.

`schedulingMetricsSnapshot()` returns:

- `sumWSeconds`
- `p95Seconds`
- `makespanSeconds`
- `failedCount`

Exact math from `Sources/KikoMediaCore/SchedulingMetrics.swift` `SchedulingMetricMath.compute(...)`:

- per-job wall time is `max(0, liveAtSeconds - arriveAtSeconds)`
- `sumWSeconds` is the sum of those wall times
- `p95Seconds` is linear interpolation on the sorted wall-time array at index `0.95 * (count - 1)`
- `makespanSeconds` is `max(0, maxLiveAtSeconds - minArriveAtSeconds)` across successful jobs
- empty successful-job sets return `0` for both `p95Seconds` and `makespanSeconds`
- `failedCount` is clamped with `max(0, failedCount)`

Images do not participate in these metrics.

### 2.10 Recompute coordination

`CARecomputeCoordinator` provides three separate behaviors:

1. prevent overlapping recompute runs
2. coalesce bursts of `.slotDownBatch`
3. prioritize completion-driven refill over generic recompute

The coordinator tracks two separate pending pass classes, and each pending class also preserves whether that pass should run with held-job reconsideration enabled:

- `completionRefillPending` — set by video completions and dispatcher `.finish` triggers
- `genericRecomputePending` — set by arrivals, slot-down batches, slot-up, ETA drift, and failures

Key rules:

- `beginRecomputeRun(requestedPassKind:reconsiderHeldJobs:)` returns `false` and marks the appropriate pending pass if a run is already active
- `beginRecomputePass()` returns a `Pass`, prioritizing `.completionRefill` over `.genericRecompute`
- if multiple pending signals of the same pass class arrive while a run is active, their `reconsiderHeldJobs` requirement is OR-combined
- `requiresAnotherRecomputePass` is `completionRefillPending || genericRecomputePending`
- `.slotDownBatch` is converted into at most one deferred flush task
- the flush task waits one `Task.yield()` and then calls back into `flushSlotDownBatchRecompute()`
- slot-down coalescing is allowed when CA is active **or** when held jobs exist, even if the current scheduling policy is FIFO
- `allowScheduling == false` suppresses both deferred slot-down flush scheduling and later flush execution

### 2.11 Dynamic video slot capacity

`currentVideoSlotCapacity()` behaves differently by policy.

FIFO:

- returns `configuredVideoSlots`
- that is local configured video slots plus all configured remote slots

CA:

- returns local-only capacity when `localComplexityAwareProfile == nil`
- otherwise calls `productionTopologyModelBuild(...)` and returns `buildResult.diagnostics.totalExecutableSlotCount`
- when no dispatcher exists, `productionTopologyModelBuild` internally builds a local-only topology with empty remote snapshot, so the result is effectively local-only capacity
- this count includes all modeled remote slots across all confidence tiers (`exactPrior`, `hardwareCompatiblePrior`, `capabilityBacked`, `localFallback`), not just exact-prior-backed slots
- production and benchmark both use `CATopologyModelBuilder.build(...)`, so the capacity counting is now shared

### 2.12 Startup recovery and CA backlog replay

Startup order in `KikoMediaEntry.swift` is:

1. optional `rebuildFromSSD()` when `needsRebuild == true` and the SSD is mounted
2. `recoverIncomplete()`
3. `verifyDerivedArtifacts()`

`recoverIncomplete()` is the direct startup/crash-recovery path for the production CA backlog.

Current rules:

- it scans `uploadDir` for non-hidden files whose filename is a valid asset ID and not a `.info` sidecar
- known DB IDs behave by current DB status:
  - `.complete` / `.moderated`
    - delete upload payload and `.info`
  - `.queued` / `.processing`
    - redetect media type from the upload payload
    - on success, `enqueueRecoveryJob(...)` with `arrivalAtSeconds = asset.createdAt`
    - on type-detection failure, mark `.failed`
  - `.failed`
    - skip
- known-ID per-upload reads use a bounded retry (`0ms`, `50ms`, `200ms`)
- if a known upload still cannot be loaded after those retries, recovery explicitly marks it `.failed` and leaves the upload on disk for manual review
- unknown DB IDs are only admitted when:
  - `.info` decodes as `TusdUpload`
  - `.info.id`, if present, matches the filename
  - deferred-size uploads are rejected
  - `.info.size` exists and matches the actual file size exactly
  - media type can be detected from the payload
- unknown uploads use sanitized `metaData["filename"]` when present, otherwise `recovered_<id>`
- after the upload-dir scan, only `.queued` DB assets whose upload payload is missing are marked `.failed`
- `restartQueuesAfterRecovery()` is currently just `processQueues()`

### 2.13 Derived-artifact verification and SSD rebuild

`verifyDerivedArtifacts()` is a second startup healing pass, not part of steady-state dispatch.

Current behavior:

- moderation markers are treated as the durable moderation source of truth
- terminal DB moderation state is reconciled to markers before repair decisions
- `.processing` assets with an upload file present are skipped here because `recoverIncomplete()` owns them
- a thumb is valid only when the file exists and `ImageProcessor.isImage(...)` returns true
- an image preview is valid only when the file exists and `ImageProcessor.isImage(...)` returns true
- a video preview is valid only when the file exists and `VideoProcessor.isVideo(...)` returns true
- stranded `.processing` assets whose derived files are already valid are restored to marker-backed terminal status
- missing/invalid derived files trigger repair only when the SSD original exists
- if the SSD is mounted and the original is missing, the asset is marked `.failed`
- if the SSD is not mounted, repair is skipped rather than failing the asset
- repair jobs are re-enqueued with:
  - `isRepair = true`
  - `restoreStatus = markerStatus`
  - `arrivalAtSeconds = asset.createdAt`

`rebuildFromSSD()` is a separate fail-closed database reconstruction path.

Exact current rules:

- rebuild aborts immediately if the SSD is not mounted
- rebuild aborts immediately if moderation markers cannot be read
- only top-level SSD files are enumerated (`.skipsSubdirectoryDescendants`)
- candidate files must:
  - have a valid asset ID in the basename
  - have either no extension or an extension that maps to image/movie UTI
  - actually pass image detection or async video detection
- timestamps are resolved in this order:
  - embedded image/video timestamp
  - file modification time
  - current time
- inserts are bounded by `config.maxConcurrentRebuildProbes`
- inserted terminal status comes from moderation markers: `.moderated` when marked, otherwise `.complete`
- after inserts, terminal moderation state is reconciled again
- stale moderation markers are pruned only when there were zero rebuild insert failures; otherwise pruning is skipped fail-closed

---

## 3. Data Flow: Benchmark Path

### 3.0 Benchmark setup, network resolution, and helper utilities

`prepareThunderboltCARunSetup(...)` has a real network/setup phase before the runtime exists.

Current settings resolution from `ThunderboltNetwork.swift`:

- `TB_WORKERS`, `TB_PORT`, and `TB_CONNECT_TIMEOUT` prefer process environment first
- if missing there, they fall back to `~/Library/LaunchAgents/com.kiko.media.plist` `EnvironmentVariables`
- otherwise they fall back to benchmark defaults
- `TB_PORT` must parse into `1 ... 65535`
- `TB_CONNECT_TIMEOUT` accepts:
  - raw milliseconds in `100 ... 30_000`, or
  - shorthand seconds in `1 ... 30`, which are multiplied by `1000`
- `resolveLocalCASlotsDefault()` prefers `MAX_CONCURRENT_VIDEOS` from process env, then persisted launch-agent env, then generated config defaults

Current worker-binding / reachability path:

- configured workers are parsed from `TB_WORKERS`
- `resolveBridgeBoundWorkers(...)`:
  - discovers local bridge-source IPv4s through `ThunderboltDispatcher.discoverBridgeSources()`
  - resolves each worker host to IPv4
  - keeps a worker only when some bridge source shares its subnet
  - emits binding issues for unresolved hosts or unroutable workers
- `ThunderboltWorkerProbe.measureReachability(...)`:
  - probes sequentially, not in parallel
  - measures elapsed connect time around `ThunderboltTransport.connect(...)`
  - closes the fd immediately on success
- only reachable workers survive into benchmark setup unless the caller supplied overrides

Transfer and helper defaults:

- fallback remote `txInMS` is successful connect latency from reachability probes
- provided `txInMS` / `txOutMS` / `publishOverheadMS` overrides are ignored unless they are positive finite numbers
- provided values override measured fallback values after that sanitization step
- `loadCAPriorTable()` loads only the canonical `benchmark-prior.json`
- `resolveThunderboltCAPriorPaths()` derives the candidate path as sibling `benchmark-prior.candidate.json`

Before model resolution, setup also performs an in-memory remote-prior merge from worker capabilities:

- `ThunderboltWorkerProbe.queryCapabilities(...)` is run for reachable workers unless overrides were supplied
- for each capability payload with:
  - `workerSignature`
  - `osVersion`
  - non-empty `priorCells`
- setup merges a `BenchmarkPriorMachine` into the working `priorTable`
- existing valid persisted values for:
  - `msPerFrameC1`
  - `fixedOverheadMS`
  - `avgCorpusFrameCount`
  are preserved
- otherwise:
  - `msPerFrameC1` can fall back to capability `msPerFrameC1`
  - fixed overhead falls back to `0`
  - average corpus frame count falls back to `0`

That setup-time merge can add remote machines to the effective prior table before `buildThunderboltCAModelProfiles(...)` runs.

`ThunderboltUtilities.swift` also owns several setup-adjacent helpers:

- `caResolvedVideoCosts(...)`
  - the primary function: probes each video through `MediaProcessor.resolveVideoCostForQueue(...)` and returns `[CAResolvedVideoCost]`
  - uses the same shared resolver as production enqueue, ensuring benchmark and production use identical frame-count resolution
- `caEstimates(...)`
  - thin wrapper around `caResolvedVideoCosts(...)` that returns `videoCosts.map(\.frameCount)`
- `caArrivalOffsets(...)`
  - `.allAtOnce`: every job at `0s`
  - `.trickle`: jobs at `0s, 1s, 2s, ...`
  - `.burst_1_20_5_5_1`: `1@0s`, `20@5s`, `5@10s`, `5@15s`, remainder `@20s`
- `caAcceptanceCorpusSignature(...)`
  - sorts by `(name, sizeBytes, path)`
  - hashes `"<name>\\t<size>\\n"` lines with 64-bit FNV-1a

### 3.0.1 Tail-telemetry seed measurement and raw round trips

Benchmark setup seeds fixed remote `txOutMS` / `publishOverheadMS` through `measureThunderboltCATailTelemetryEstimates(...)`.

Current measurement flow:

- it maps workers by `host` and videos by `path`
- it delegates to `ThunderboltTailTelemetrySeedMeasurement.measure(...)`
- sample selection is deterministic:
  - largest `fileSize` wins
  - ties are broken by lexicographically smaller `path`
- source SHA-256 is computed once for that chosen sample
- one temporary directory is shared across all endpoint measurements
- an endpoint is skipped only when **both** provided estimates already exist for that endpoint ID
- a successful sample only contributes positive finite `txOutMS` / `publishOverheadMS`

`ThunderboltRawExecution.runRemoteRoundTrip(...)` is the default measurement primitive.

It currently:

1. connects to the worker
2. sends the normal Thunderbolt upload header and file bytes
3. reads the normal worker response header
4. treats worker status `0x01` as the only success status
5. streams preview and thumb payloads to temporary files
6. SHA-verifies both payloads

Its reported timing fields mean:

- `sendSeconds`
  - header + upload send time
- `processNanos`
  - worker-reported process time from the response header
- `receiveSeconds`
  - response-header read through payload read completion
- `totalSeconds`
  - full wall-clock round trip
- `firstRunningLatencySecondsEstimate`
  - `max(0, dispatchToCompletionSeconds - processSeconds)`
  - here `dispatchToCompletionSeconds` is measured from `sendStarted` to `responseHeaderReceived`
  - that excludes connect time and payload-read time
- `txOutMS`
  - response-header receipt to payload-read completion
- `publishOverheadMS`
  - payload-read completion to post-read SHA verification completion

Failure classification is also implementation-specific:

- connect/send/read/integrity failures generally report `slotHealthDownOnFailure = true`
- non-success worker status reports `slotHealthDownOnFailure = false`

### 3.1 Benchmark runtime initialization

`makeThunderboltCABenchmarkRuntimeEngine(...)` builds `CABenchmarkRuntimeEngine` from:

- `policy`
- `videoCosts: [CAResolvedVideoCost]`
- `machineProfiles`
- `slotBindings`

The runtime owns:

- mutable per-machine `msPerFrameC1`
- mutable per-machine `txInMS`
- fixed per-machine `txOutMS`
- fixed per-machine `publishOverheadMS`
- fixed per-machine `modeledConcurrencyCap` (through `machineProfiles`)
- per-slot `readyAtMS`
- per-slot `isDown`
- held dispatches
- preserved decision-reference times
- collected solver telemetry

### 3.2 Benchmark run coordinator

`runThunderboltCA(...)` creates:

- one scheduler task
- one worker task per slot
- a `ThunderboltCASlotCompletionQueue`
- a `CABenchmarkRuntimeEngine`

The scheduler task loops like this:

1. drain completed slot ordinals
2. mark completed slots ready in the runtime
3. requeue all held dispatches on completion
4. release any held dispatches whose wake time has arrived
5. snapshot `hasPending`, `hasHeldDispatches`, and `isComplete`
6. if the run is complete, no held dispatches remain, and all slots are free, break
7. if there are both free slots and pending jobs, call `scheduleRuntimeBatch(...)`, yield resulting dispatch items, and immediately continue when `madeProgress == true`
8. if the run is complete but some slots are still busy, sleep `5ms`
9. if there are no pending jobs but held dispatches remain, sleep until `min(5ms, max(1ms, nextHeldWake - now))`
10. if there are no pending jobs and arrivals are not complete, wait on `runtimeEngine.waitForWork()`
11. if there are no free slots, sleep `5ms`
12. otherwise fall through to a final `5ms` sleep

### 3.3 Benchmark scheduling and hold behavior

The benchmark runtime has two layers:

1. `scheduleBatch(...)`
   - FIFO uses queue order
   - CA uses the shared solver
2. `scheduleRuntimeBatch(...)`
   - adds the benchmark-only hold and decision-reference logic

In CA mode, `scheduleCABatch(...)`:

1. calls `pickTwoStageBatchWithTelemetry(... maxReadyNowCount:)` — the same two-stage solver as production
2. stage 1 fills ready-now slots; stage 2 creates reservation picks for remaining jobs
3. combines `readyNowPicks + reservationPicks` into the dispatch item list
4. removes each selected pending job immediately from `pending`
5. stores the chosen projected `readyAtMS` back into the benchmark slot state

Benchmark CA uses the same two-stage property as production:

- stage 1 fills all free slots first with ready-now jobs
- stage 2 creates reservations only when the benefit exceeds `reservationMinimumBenefitMS` against the current projected state

### 3.4 `scheduleRuntimeBatch(...)`: decision-reference preservation and hold-before-dispatch

For every scheduled item:

1. `decisionReferenceAtSeconds` is chosen as:
   - an already-preserved value for this job, if one exists
   - otherwise:
     - if total job count is `1`, use the actual decision time
     - else if the item already waited, or scheduler lag exceeds `0.1s`, use the arrival time
     - else use the actual decision time
2. `preparedItem = item.rebasedDecisionReference(to: decisionReferenceAtSeconds)`
3. Hold rules:
   - if policy is CA and the **original** item had `predictedSlotReadyMS > 0`, the item is held
   - if the chosen slot is not actually free in `freeSlotOrdinals`, the item is also held
4. Otherwise the item is dispatched immediately

The hold decision for future-ready CA picks is made from the **pre-rebase** prediction, not the shifted values.

### 3.5 Benchmark holds vs production holds

| Aspect | Production | Benchmark |
|---|---|---|
| Hold storage | `CAPendingHeldVideoState` keyed by upload ID | `heldDispatches` array |
| Hold trigger | chosen slot `readyAtMS > nowMS` | chosen item `predictedSlotReadyMS > 0`, or chosen slot not currently free |
| Wake mechanism | `Task.sleep(...)` on the earliest wake plan | scheduler loop polls `nextHeldWakeSeconds` |
| Recompute release | only impacted holds are invalidated via `reconsiderHeldVideoJobsForRecompute()` using `CAHoldInvalidation` | only impacted holds are invalidated via `invalidateHeldDispatchesForTargetStateChanges()` using `CAHoldInvalidation`; ready ones are also released by wake-time polling |
| Decision reference | not preserved | preserved in `preservedDecisionAtSecondsByIndex` |

### 3.6 `requeueHeldDispatches(...)`

Benchmark held dispatch release works like this:

1. remove held items by descending index
2. sort released items by arrival time, then by job index
3. preserve each released item's `decisionAtSeconds` back into `preservedDecisionAtSecondsByIndex`
4. reinsert the jobs into `pending`, preserving arrival-time ordering
5. wake any arrival waiters

### 3.7 Benchmark completion accounting and adaptation

A benchmark completion can update different subsystems:

- `recordSlotHealth(...)` can mark slot up/down state
- `recordTransferOverhead(...)` can update `txInMS`
- `recordCompletion(...)` can update per-machine `msPerFrameC1`

Adaptation now counts all successful execution paths:

- successful local-native jobs count
- successful remote jobs count
- local fallback after remote failure also counts toward adaptation

Prediction samples carry fields for:

- predicted machine
- decision time
- predicted slot-ready/start/done
- actual start/done
- waited flag
- success flag
- executor mismatch flag

---

## 4. The Solver

### 4.1 Core types

All shared solver types live in `Sources/KikoMediaCore/ComplexityAwareScheduler.swift`.

Primary public types:

- `CAMilliseconds`
- `CAJob`
- `CASlot`
- `CAMachine`
- `CACandidate`
- `CADegradationPoint`
- `CAScore`
- `CAScoredCandidate`
- `CASlotRef`
- `CAPendingPickJob`
- `CAReadyPolicy`
- `CAPick`
- `CAPickResult`
- `CATwoStagePickResult`
- `CADecisionOption`
- `CADecisionAction`
- `CAFutureAwareDecision`
- `CASolverTelemetry`

### 4.2 Score function

`ComplexityAwareScheduler.score(candidate:nowMS:)` computes:

```text
tReadySlotMS   = capacityConstrainedSlotReadyMS(machine, slot.readyAtMS)
tReadyInputMS  = nowMS + machine.txInMS
tStartMS       = max(tReadySlotMS, tReadyInputMS)
activeSlotsAtStart = count(non-down slots with readyAtMS > tStartMS on the same machine)
requestedConcurrency = activeSlotsAtStart + 1
degradation = resolvedDegradation(machine, requestedConcurrency)
variableRunMS = frameCount * msPerFrameC1 * degradation.factor
concurrencyPenaltyMS = activeSlotsAtStart > 0 ? activeSlotsAtStart * activeSlotsAtStart * variableRunMS * 0.10 : 0
runMS = fixedOverheadMS + variableRunMS + concurrencyPenaltyMS
tDoneMS = tStartMS + runMS + txOutMS + publishOverheadMS
```

Important exact behaviors:

- down slots return `nil`
- when `machine.modeledConcurrencyCap == nil`, `tReadySlotMS` is just the raw `slot.readyAtMS`
- when `machine.modeledConcurrencyCap` exists, `tReadySlotMS` is advanced forward until active concurrency drops below that cap; this is the same capacity-constrained ready time later used for reservation-hold wakeups
- `activeSlotsAtStart` uses `readyAtMS > tStartMS`, not `>=`
- degradation curves are repaired to be dense and monotone at `CAMachine` init via `CAProfileAndFallbackMath.repairedDenseDegradationCurve(from:)`, so missing points never occur at score time
- repair fills gaps via monotone interpolation with upward clamping; concurrency 1 is always `1.0`

### 4.3 Tie-break chain

`tieBreakLessThan(...)` compares two scored candidates in this exact order:

1. lower `tDoneMS`
2. lower `runMS`
3. earlier `arrivalAtMS`
4. lower `enqueueOrder`
5. lexically lower `machine.id`
6. lexically lower `slot.id`
7. lexically lower `job.id`

### 4.4 Batch comparison

Two different comparators matter:

1. `batchObjectiveLessThan(...)`
   - lower effective makespan first, where `effectiveMakespan = max(baselineTailMS, projectedMakespan)`
   - if both sides tie exactly at `baselineTailMS` and `baselineTailMS > 0`, prefer the plan with higher `scheduledFrameSum`
   - then lower projected completion sum
   - then element-wise candidate tie-break
2. `batchPlanLessThan(...)`
   - higher pick count first
   - then `batchObjectiveLessThan(...)`
   - then element-wise `batchChoiceLessThan(...)`

`batchChoiceLessThan(...)` adds one more exact deterministic rule after scored-candidate ties:

- prefer `excludedSlotWasCleared == false`
- then lower `machineIndex`
- then lower `slotIndex`

### 4.5 `pickBatch(...)` and `pickTwoStageBatch(...)`

`pickTwoStageBatch(...)` is the production and benchmark solver entrypoint. It calls `pickBatch(...)` internally for each stage:

- Stage 1: `pickBatchWithTelemetry(... readyPolicy: .readyNowOnly)` fills free slots
- Stage 2: `pickBatchWithTelemetry(... readyPolicy: .includeFutureReady)` on remaining jobs using projected machines from stage 1, with a minimum benefit gate (`reservationMinimumBenefitMS == 1`) evaluated against the current projected state

`pickBatch(...)` remains the core single-stage solver.

Externally visible stages:

1. `enumerateBatchChoices(...)`
2. `greedySeedPlan(...)`
3. `resolveBestBatchPlanBnB(...)`
4. `buildPickResult(...)`

But that is not a one-shot linear pipeline:

- `pickBatch(...)` calls `pickBatchWithTelemetry(...)`
- that wraps `resolveBestBatchPlanWithTelemetry(...)`
- `greedySeedPlan(...)` re-runs `enumerateBatchChoices(...)` on each pick
- `bnbSearch(...)` re-runs `enumerateBatchChoices(...)` at each recursive level
- `solverWallMS` is measured around the whole telemetry wrapper, not just the BnB core

### 4.6 Choice enumeration and exclusion clearing

`enumerateBatchChoices(...)` works per pending job.

For each job:

1. try the preferred choice set with the job's excluded slot still excluded
2. if that produced no choices and the job had an exclusion, retry with the exclusion cleared

The retry is per-job, not global.

### 4.7 Greedy seed and branch-and-bound

`greedySeedPlan(...)`:

1. repeatedly picks the single best currently available choice
2. reserves the chosen slot forward to the pick's `tDoneMS`
3. removes the chosen job
4. repeats up to `maxCount`

Both `resolveBestBatchPlanBnB(...)` and the exhaustive oracle path compute `baselineTailMS = committedTailMS(for: machines)` at solve start, then thread that value through all plan comparisons.

`resolveBestBatchPlanBnB(...)` uses the greedy seed as the first incumbent.

Pruning rules in `bnbSearch(...)`:

1. prune if maximum possible pick count cannot beat the incumbent
2. when max possible pick count only ties the incumbent:
   - compute optimistic per-job best `tDoneMS`
   - compute lower-bound effective makespan as `max(baselineTailMS, currentMakespan, rthOptimistic)`
   - prune if that lower-bound makespan loses to the incumbent
   - if lower-bound makespan still ties:
     - when the tie is exactly at `baselineTailMS > 0`, first compute an optimistic `upperBoundFrameSum` and prune if it cannot match the incumbent's `scheduledFrameSum`
     - if frame-sum still ties, prune by lower-bound completion sum
     - otherwise, outside the baseline-tail case, prune directly by lower-bound completion sum
3. apply symmetry deduplication before recursing

### 4.8 Symmetry deduplication

`deduplicateSymmetricSlots(...)` groups choices by:

- pending index
- machine index
- slot ready time
- slot down flag
- whether exclusion was cleared
- which other pending jobs exclude that slot

Only the comparator-minimal representative survives in each group.

### 4.9 Oracle solver

`pickBatchOracle(...)` is the exhaustive reference solver used in tests.

It:

- enumerates every branch
- does no pruning
- still returns the same `CAPickResult` shape as the live solver
- also populates solver telemetry

### 4.10 `futureAwareDecision(...)`

This helper is single-job only.

It computes:

1. best future-ready choice under `.includeFutureReady`
2. best ready-now choice under `.readyNowOnly`

Selection rule:

- choose the future-ready choice only when:
  - that future choice is actually future-ready (`tReadySlotMS > nowMS`), and
  - its `tDoneMS` beats the best ready-now choice
- otherwise choose ready-now when available
- otherwise fall back to best future-ready

### 4.11 Slot reservation and ready policies

`reserveProjectedSlot(...)` updates a chosen slot's `readyAtMS` to:

- `max(currentSlot.readyAtMS, doneAtMS)`

Ready policies:

- `.readyNowOnly`
  - only slots with `readyAtMS <= nowMS`
- `.includeFutureReady`
  - all non-down slots

Down-slot filtering is separate from ready-policy filtering.

---

## 5. The Model

### 5.1 Prior artifact

The persisted prior artifact lives in `benchmark-prior.json` and is modeled by `BenchmarkPriorArtifact`.

Current supported format:

- `version == 2`
- `generatedAt`
- `corpusHash`
- `corpusSummary`
- `machines`

`BenchmarkPriorCorpusSummary` carries:

- `videoCount`
- `totalBytes`

Each `BenchmarkPriorMachine` carries:

- canonical worker signature
- hardware identity fields
- preset identity
- `msPerFrameC1`
- `fixedOverheadMS`
- `avgCorpusFrameCount`
- `affineModelSource: BenchmarkPriorAffineModelSource` (`.explicit` or `.legacyHeuristic`)
- per-concurrency `cells`

Each `BenchmarkPriorCell` carries:

- `concurrency`
- `videosPerMin`
- `msPerVideoP50`
- `msPerVideoP95`
- `degradationRatio`

Important exact behaviors:

- load result distinguishes `missing`, `invalid`, `unsupportedVersion`, and `loaded`
- lookup supports both exact-by-signature (`exactMachine(signature:)`) and hardware-compatible (`hardwareCompatibleMachine(signature:)` / `hardwareCompatibleMachine(components:)`) matching
- hardware-compatible match finds machines with the same `hardwarePresetKey` (chip/cores/encoders/preset, excluding OS), preferring the closest OS version
- merge is upsert-by-signature
- `WorkerSignatureBuilder.make(...)` emits:
  - `chip=...;ecores=...;encoders=...;os=...;pcores=...;preset=...`
  - the keys are alphabetically ordered
  - OS version is part of the signature, so OS updates change the key
- `WorkerSignatureComponents.parse(signature:)` splits a signature into component fields, exposing `exactSoftwareKey` (full signature) and `hardwarePresetKey` (chip/cores/encoders/preset without OS)
- `affineModelSource` defaults to `.legacyHeuristic` when decoding artifacts that predate WS8 (missing `affine_model_source` JSON key)

### 5.2 Shared prior/profile helpers

`CAProfileAndFallbackMath` provides the shared shaping rules.

#### `localPriorProfileShaping(...)`

This helper returns `CALocalPriorProfileShaping`.

Returns:

- prior `msPerFrameC1` if valid, otherwise fallback `1.0`
- prior `fixedOverheadMS` if finite and positive, otherwise `0`
- prior degradation curve if non-empty, otherwise flat `[(1, 1.0)]`
- flags telling the caller which local values were fallback-derived

#### `validatedPriorProfile(...)`

This is the strict validated prior profile used by:

- production local CA activation
- production remote worker inclusion
- production slot-capacity counting

It now uses `resolvedRemoteAffineModel(from:)` to derive `msPerFrameC1` and `fixedOverheadMS`.

It requires:

- valid resolved remote `msPerFrameC1` (via `resolvedRemoteAffineModel`)
- non-empty degradation curve (repaired to dense monotone form)

#### `resolvedRemoteAffineModel(...)`

This is the primary entry point for remote affine model resolution. It dispatches based on `affineModelSource`:

- **`.explicit`** — returns `msPerFrameC1` and `fixedOverheadMS` directly from the prior machine, no heuristic adjustment
- **`.legacyHeuristic`** — falls through to `adjustedRemotePriorEstimates(...)` for backward-compatible slope correction

#### `adjustedRemotePriorEstimates(...)`

This correction is the legacy path, used only when `affineModelSource == .legacyHeuristic`. It runs when all of these are true:

1. raw `msPerFrameC1` is valid
2. resolved `fixedOverheadMS <= 0`
3. `avgCorpusFrameCount > 100`
4. a valid concurrency-1 `c1P50MS` exists
5. `msPerFrameC1 * avgCorpusFrameCount` is within `(0.95, 1.05)` of `c1P50MS`

When that happens, the stored slope is reduced to compensate for fixed overhead that was effectively absorbed into the slope-only fit.

The exact correction is:

```text
c1P50Seconds = c1P50MS / 1000
extraReduction = min(0.15, max(0, c1P50Seconds - 0.5) * 0.05)
adjustedMS = max(0.001, msPerFrameC1 * max(0.55, 0.75 - extraReduction))
```

Prior maintenance rewrites legacy heuristic priors into explicit affine form (workers with `affineModelSource == .legacyHeuristic` are flagged for maintenance even when otherwise healthy).

### 5.3 Production model path

Production CA model construction calls `CATopologyModelBuilder.build(mode: .auto, ...)` through `buildProductionTopologyModel(...)`. This is the same shared builder used by benchmark.

It always builds:

- one local machine
- zero or more remote machines resolved through `CARemoteModelDecisionKernel.resolve(...)` with confidence-tiered modeling

Production passes `mode: .auto`, which enables all four confidence tiers (`exactPrior`, `hardwareCompatiblePrior`, `capabilityBacked`, `localFallback`). Lower-confidence tiers receive safety multipliers and concurrency caps (see Section 2.4).

Production remote machines can still change over time because live telemetry overlays:

- `liveMSPerFrameC1`
- transfer-in estimate
- transfer-out estimate
- publish-overhead estimate

Those overlays apply on top of whatever confidence tier the worker resolved to.

### 5.4 Benchmark model path

Benchmark setup uses:

- `buildThunderboltCAModelProfiles(...)` which delegates to `CATopologyModelBuilder.build(...)`
- the same `CARemoteModelDecisionKernel.resolve(...)` and `CATopologyModelAssembly.build(...)` used by production, through the shared builder

`CARemoteModelDecisionMode` is the strict/auto switch. Benchmark passes the user-selected mode (`.strict` or `.auto`). Each `CARemoteModelDecision` carries:

- `host`
- `machineID`
- `msPerFrameC1`
- `fixedOverheadMS`
- `degradationCurve`
- `txInMS`
- `txOutMS`
- `publishOverheadMS`
- `msSource`
- `curveSource`
- `exclusionReason`
- `fallbackActive`
- `confidenceTier: CAMachineConfidenceTier`
- `confidenceMultiplier: Double`
- `concurrencyCap: Int?`
- `usesLegacyAffineHeuristic: Bool`

#### Strict mode

A remote worker is excluded unless all of these exist:

- worker signature
- exact prior table entry for that signature
- valid prior `msPerFrameC1`
- non-empty prior degradation curve

If strict mode excludes reachable remotes, benchmark setup can throw `ThunderboltBenchmarkJSONError.caStrictRemoteExclusion`.

#### Auto mode

Remote model resolution uses a confidence-tiered cascade via `resolveBaseModel(...)`:

1. **`exactPrior`** — exact signature match in prior table (multiplier `1.00`)
2. **`hardwareCompatiblePrior`** — same hardware/preset key, closest OS version (multiplier `1.15`, concurrency capped to `1`)
3. **`capabilityBacked`** — capability-reported runtime slope/curve (multiplier `1.25`, concurrency capped to `1`)
4. **`localFallback`** — local profile values as conservative estimate (multiplier `1.35`, concurrency capped to `1`)

Fixed overhead uses `resolvedRemoteAffineModel(...)` for exact/hardware-compatible priors, otherwise `0`.

`fallbackActive` is true whenever either the slope source or curve source is not `prior(remote)`.

### 5.5 Topology assembly (shared)

`CATopologyModelAssembly.build(...)` is called internally by `CATopologyModelBuilder.build(...)`, which is used by both production and benchmark:

It works with these topology types:

- `CATopologyModelWorker`
- `CATopologyModelLocalProfile`
- `CATopologyModelMachineProfile`
- `CATopologyModelSlotBinding`
- `CATopologyModelInputRow`
- `CATopologyModelCoverageRow`
- `CATopologyModelDiagnostics`
- `CATopologyModelBuildResult`

1. creates the local machine at machine index `0`
2. builds coverage rows and strict-exclusion diagnostics for all reachable workers
3. lazily creates each remote machine when the first eligible slot for that host is encountered
4. builds `slotBindings`
5. emits `modelInputs`
6. emits diagnostics:
   - reachable vs modeled workers
   - reachable vs modeled slots
   - strict exclusions
   - fallback activity
   - local prior gap
   - remote prior gap

When slots are generated through the standard benchmark utility `caSlots(...)`, local and remote slot IDs are 1-based.

### 5.6 Live adaptation

The same adaptive estimator is used in multiple places. All four environments now use a single shared normalization function.

#### Shared estimator

`LiveAdaptiveMSPerFrameC1Estimator.next(...)`:

- returns `LiveAdaptiveMSPerFrameC1Estimator.Update(estimate, smoothedError, smoothedAbsoluteError)`
- takes both historical state and `initialEstimate`
- if there is no valid previous estimate/error state, it cold-starts from `previousEstimate`, else `initialEstimate`, else `observed`
- smooths signed error with `gamma = 0.15`
- smooths absolute error with the same `gamma`
- derives `alpha = clamp(abs(smoothedError / max(smoothedAbsoluteError, 0.001)), 0.05, 1.0)`
- updates `estimate += alpha * error`

#### Shared normalization (all environments)

`ThunderboltAdaptiveTelemetryReducer.normalizedMSPerFrameC1(processNanos:frameCount:model:concurrency:)` is the single shared normalizer used by all four environments:

1. subtracts fixed overhead from observed runtime via `variableRuntimeMS(...)`
2. divides by frame count
3. normalizes back to concurrency-1 using the degradation curve via `CAProfileAndFallbackMath.resolvedDegradation(from:concurrency:)`

The `model` parameter is a `CASuccessfulExecutionSampleModel` carrying `msPerFrameC1`, `fixedOverheadMS`, and `degradationCurve`.

#### Per-environment usage

- **Production local** (`MediaProcessor.updateLocalLiveMSPerFrame`): builds a `CASuccessfulExecutionSampleModel` from the local profile, calls the shared normalizer
- **Production remote** (`ThunderboltDispatcher.recordSuccessfulExecutionSample`): receives a `successfulExecutionSampleModel` from the dispatch call, calls the shared normalizer with worker-reported `processNanos`
- **Benchmark local** (`CABenchmarkRuntimeEngine.recordCompletion`): calls the shared normalizer
- **Benchmark remote** (`CABenchmarkRuntimeEngine.recordCompletion`): calls the shared normalizer

### 5.7 Transfer and tail telemetry

#### Transfer-in

`TransferOverheadEstimator` maintains:

- a slow-moving `baseline`
- a reactive `estimate`
- update payload type `TransferOverheadEstimator.Update(baseline, estimate)`

Rules:

- cold start initializes both baseline and estimate to the raw sample
- degraded sample if `sample >= baseline * 1.25`
- degraded update:
  - baseline: `0.90 * previous + 0.10 * sample`
  - estimate: `0.70 * previousEstimate + 0.30 * sample`
- recovery update:
  - baseline: same slow baseline EMA
  - estimate: `0.85 * previousEstimate + 0.15 * nextBaseline`

Both dispatcher and benchmark runtime use `reduce(...)`, which accepts zero-valued finite samples.

`reducePositive(...)` (which requires strictly positive finite samples) still exists but is no longer used in the main transfer-overhead path.

#### Transfer-out and publish overhead

`ThunderboltAdaptiveTelemetryReducer.nextTailUpdate(...)` separately maintains:

- `txOutEstimateMS`
- `publishOverheadEstimateMS`

Each sample:

- is clamped into `0 ... 120_000ms`
- uses EMA prior weight `0.80`
- if the sample is `nil` or non-finite, the previous estimate is preserved
- the clamp is applied both before the EMA step and to the EMA result

These values feed:

- production CA remote scoring through dispatcher snapshots
- benchmark machine profiles when present in setup

---

## 6. Hold Mechanics

### 6.1 `CAPendingHeldVideoState`

Production held state is split into:

- queued jobs in an array with `queueHead`
- held jobs in `heldJobsByUploadID`
- a monotonically increasing `heldWakeToken`

Key exact behaviors:

- `takeNextQueued()` advances `queueHead`
- `compactQueuedIfNeeded(threshold:)` only compacts when:
  - `queueHead > threshold`, and
  - `queueHead > queuedJobs.count / 2`
- `nextWakePlan(...)` always increments the wake token
- stale wake tasks are ignored if their token no longer matches
- mutating APIs thread an `allowScheduling` flag
- when `allowScheduling == false`, wake-token advancement still happens, but the returned wake plan carries `wakeAt == nil`
- `clearAll()` uses `allowScheduling: false`

### 6.2 Production hold creation and release

Production CA creates holds only from stage 2 reservation picks in the two-stage dispatch pipeline.

Release paths:

1. wake timer path
   - `handleHeldVideoWake(token:)`
   - releases only ready held jobs
   - only if the token is still current
2. recompute path
   - `reconsiderHeldVideoJobsForRecompute()` evaluates each held entry against its target slot's current state using `CAHoldInvalidation`
   - only invalidated holds are released (target slot down, ready-at drift beyond `targetReadyAtDriftThresholdMS == 250`, or target slot impossible)
   - stable holds are preserved across targeted recompute invalidation
   - additionally, a production pass executing with `reconsiderHeldJobs == true` gets one held-only idle-capacity reconsideration when all queued video work is exhausted and some CA slot is ready now; in that case all held jobs are released back into queued order for re-planning
3. explicit API path
   - `setVideoHold(...)`

Held-job requeue order is not arbitrary:

- held upload IDs are sorted by `arrivalAtSeconds`
- ties are broken by `uploadId`
- each released job is inserted ahead of the first queued job with a strictly later arrival time

### 6.3 `setVideoHold(...)`

`MediaProcessor.setVideoHold(uploadId:hold:)` is more nuanced than a simple set/unset API.

It rejects the request when:

- the processor is shutting down
- the job is currently active as a video

Behavior by case:

- `hold == nil`
  - release the hold if the job is currently held
- `hold != nil` and `hold.wakeAt <= now`
  - if the job is already held, release it immediately
  - if the job is only queued, do **not** store a hold; just kick `processQueues()`
- `hold != nil` and the job is already held
  - update the held metadata
- `hold != nil` and the job is queued
  - move it from queued to held

### 6.4 Benchmark hold logic

Benchmark hold behavior lives in `CABenchmarkRuntimeEngine.scheduleRuntimeBatch(...)`.

An item is held when:

- CA predicted `predictedSlotReadyMS > 0`, or
- the chosen slot ordinal is not currently free

Each `HeldDispatch` now tracks `slotOrdinal` and `targetReadyAtSeconds` for invalidation.

Wake times:

- future-ready hold:
  - `(item.decisionAtSeconds ?? nowSeconds) + predictedSlotReadyMS / 1000`
- not-free hold:
  - `(item.decisionAtSeconds ?? nowSeconds) + max(0, (predictedSlotReadyMS ?? 0) / 1000.0)`

On completion, `invalidateHeldDispatchesForTargetStateChanges()` evaluates each held dispatch using `CAHoldInvalidation` and only releases holds whose target slot state materially changed (slot down, ready-at drift beyond threshold, or slot impossible). Stable holds are preserved.

### 6.5 Benchmark decision-reference rebasing

`CABenchmarkRuntimeDispatchItem.rebasedDecisionReference(to:)`:

1. computes `decisionShiftMS = max(0, (currentDecisionAtSeconds - targetDecisionAtSeconds) * 1000)`
2. adds that shift to:
   - `predictedSlotReadyMS`
   - `predictedStartMS`
   - `predictedDoneMS`
3. sets `waited = waited || decisionShiftMS > 0`

This preserves benchmark observability relative to the chosen logical decision reference instead of the later re-solve time.

---

## 7. Thunderbolt Dispatch

### 7.0 Transport, capabilities, and probes

`ThunderboltTransport.swift` is the wire-level truth underneath both production dispatch and benchmark probing.

Current exact behavior:

- all network resolution is IPv4-only (`AF_INET`)
- `connect(...)`
  - optionally binds to a specific source IPv4 before connecting
  - sets `SO_NOSIGPIPE`
  - switches the socket to nonblocking mode
  - treats `EINPROGRESS` as a poll-for-writable connect
- request header format is:
  - 8-byte big-endian file size
  - 64 ASCII SHA-256 hex bytes
  - 2-byte big-endian name length + UTF-8 name
  - 2-byte big-endian MIME length + UTF-8 MIME
- normal response header format is exactly 145 bytes:
  - byte `0`: `status: UInt8`
  - bytes `1 ... 8`: `processNanos: UInt64`
  - bytes `9 ... 12`: `prevSize: UInt32`
  - bytes `13 ... 76`: `prevSHA256: 64-byte UTF-8 hex`
  - bytes `77 ... 80`: `thumbSize: UInt32`
  - bytes `81 ... 144`: `thumbSHA256: 64-byte UTF-8 hex`
- `sendFileData(...)` uses `sendfile(...)`
  - if `EAGAIN` occurs with zero progress, it polls for writability with a `25ms` timeout
- `readExactly(...)` optionally polls until a deadline derived from `readTimeoutMS`
- `readToFile(...)` streams to disk in `1_048_576`-byte chunks

Capabilities are a special transport query, not a separate protocol:

- request uses:
  - `fileSize = 0`
  - zero SHA-256
  - `name = "__kiko_caps__"`
  - `mime = "application/x-kiko-caps+json"`
- read timeout is hardcoded to `250ms`
- valid capability response requires:
  - status `0x03`
  - `prevSize` in `1 ... 32_768`
  - `thumbSize == 0`
  - zero thumb SHA-256
  - payload SHA-256 match before JSON decode

`WorkerCaps` is the decoded capability payload. It can carry:

- hardware counts and chip/OS identity
- `workerSignature`
- remote `priorCells`
- remote `msPerFrameC1`
- remote degradation curve
- supported tick version

`WorkerCaps.detectLocal()` only fills local-detectable fields and always advertises `tickVersion = ProgressTickV2.version`.

`ThunderboltCapabilities.sweepCeiling(totalCores:videoEncodeEngines:)` is exactly:

- `min(max(1, totalCores), max(1, videoEncodeEngines) * 2 + 1)`

`ThunderboltWorkerProbe.queryCapabilities(...)` is parallel but bounded:

- result order matches endpoint order
- default `maxConcurrency == 16`
- each worker query is just `ThunderboltTransport.queryCapabilities(...)`

### 7.1 Connection lifecycle

`ThunderboltDispatcher` is a long-lived actor with persistent per-slot connection state.

Current flow:

1. init:
   - builds slot state per worker
   - production slot IDs are `"<host>#s1"`, `"<host>#s2"`, ...
   - discovers bridge sources
   - resolves IPv4 addresses when possible
2. dispatch preflight:
   - rejects shutdown
   - rejects no-bridge case with local fallback
   - rejects invalid indices as permanent failure
   - rejects busy/down target slots through transient failure handling
3. prior warmup:
   - `warmupPrior()` opportunistically kicks remote prior merge probes
4. connect:
   - if `slot.fd == nil`, connect using the selected bridge source IP
   - successful fresh connection triggers `markSlotUp(... includeFreshStream: true)`
5. dispatch:
   - allocate job handle
   - build worker MIME with optional tick metadata
   - send header
   - send file data
   - optionally consume tick stream
   - read response header
   - read preview/thumb payloads
   - verify SHA-256

Job-handle allocation is monotonic within the dispatcher session:

- once `nextJobHandle` reaches `UInt32.max`, `jobHandleExhausted` is latched
- all later dispatches then fail permanently for that dispatcher instance

Connections are kept open across successful dispatches and closed on failure or shutdown.

### 7.2 Slot state machine

Each slot tracks:

- `fd`
- `isBusy`
- `isDown`
- dispatched frame count / concurrency
- current job handle
- last ETA
- drift baseline state

Important transitions:

- idle to busy:
  - dispatch start
- busy to idle:
  - dispatch outcome finalized
- any to down:
  - `markSlotDown(...)`
  - only emits `.slotDownBatch` on a real transition to down
- down to up:
  - `markSlotUp(...)`
- fresh-connected stream to up:
  - `markSlotUp(... includeFreshStream: true)`
  - this emits `.slotUp` even if the slot was not previously marked down

### 7.3 Transient failure and grace recovery

`handleTransientFailure(...)`:

1. optionally starts a long-down probe
2. optionally waits for grace recovery
3. if grace recovery succeeds:
   - emits `.fail`
   - returns `.transientRetry(slotHealthDown: false)`
   - does **not** increment durable retry count
4. otherwise retry counting has two modes:
   - if `onRetryIncrement` exists, use `max(0, durableRetryCount)`
   - otherwise seed from `max(0, onRetrySeed?(uploadId) ?? 0) + 1`
5. if retry count exceeds `2`, escalates to permanent failure
6. otherwise emits `.fail` and returns `.transientRetry(...)`

### 7.4 Long-down probe, ETA drift, and snapshot behavior

#### Long-down probe

When a down slot needs recovery:

- grace recovery uses `slotRecoveryGraceNanos == 1.5s`
- probe task sleeps with exponential backoff plus jitter
  - initial delay `250ms`
  - max delay `4s`
  - jitter `100ms` (first probe skips jitter)
- each probe tries a short connect with `500ms` timeout
- successful probe restores the fd, clears busy/dispatch metadata, and marks the slot up

#### ETA drift

Running tick ETA drift uses:

- baseline ETA captured by `noteBaselineSnapshot()`
- elapsed time since that baseline
- current live ETA from tick stream

Trigger rule:

- threshold is `max(250ms, min(2000ms, 0.20 * baselineETA))`
- there are three drift zones, not two:
  - `drift >= threshold`
    - increment `driftHighCount`
    - reset `driftLowCount`
    - two consecutive highs trigger when armed
    - after a trigger fires, `driftHighCount` is reset to `0` and the slot is disarmed
  - `drift <= threshold * 0.5`
    - increment `driftLowCount`
    - reset `driftHighCount`
    - two consecutive lows re-arm
    - after re-arm, `driftLowCount` is reset to `0`
  - `threshold * 0.5 < drift < threshold`
    - reset both counters
- dispatcher also coalesces drift-trigger recomputes with a `200ms` minimum spacing

#### Snapshot fallback for remote remaining runtime

Production CA remote ready times depend on dispatcher snapshots.

If a busy remote slot has:

- live ETA from tick stream:
  - use it directly
- otherwise:
  - use observed process EMA, otherwise prior P50 for current concurrency
  - subtract elapsed time since dispatch start

This fallback matters because production CA can score remote busy slots even before the first running tick arrives.

### 7.5 Bridge routing, MIME construction, and tick protocol

Bridge routing:

- dispatcher resolves each worker host to IPv4
- picks a bridge source IP whose subnet contains the worker address
- binds outbound connects to that source IP
- unresolved host resolution is backoff-cached for `30s` before another resolve attempt is made

Worker MIME:

- when CA scheduling is enabled, MIME is suffixed with `#kiko-v2:h=<jobHandle>,s=<sessionID>`
- workers echo that metadata in tick frames

Tick protocol:

- CA mode accepts only tick protocol version `2`
- non-CA mode accepts legacy `1` and `2`
- `ProgressTickV2` frames are 24 bytes
- byte layout is:
  - byte `0`: version
  - byte `1`: status
  - bytes `2 ... 5`: job handle
  - bytes `6 ... 9`: session ID
  - byte `10`: error class
  - byte `11`: reserved, must be `0`
  - bytes `12 ... 15`: `progress: Float`
  - bytes `16 ... 19`: `elapsedMS`
  - bytes `20 ... 23`: `estRemainingMS`
- `ProgressTickV2.ErrorClass` is:
  - `none`
  - `transient`
  - `permanent`
- decode surface is `ProgressTickV2DecodeError`
- validation surface is `ProgressTickV2ValidationIssue` via `ProgressTickV2Validator`
- terminal ticks must carry `estRemainingMS == 0`
- validator rejection cases are:
  - status/error-class mismatch
  - progress out of range
  - progress decreased
  - unknown job handle
  - session mismatch
  - terminal remaining nonzero
- decode errors are:
  - invalid length
  - unsupported version
  - invalid status
  - invalid error class
  - invalid reserved byte
- invalid frames are tolerated up to 3 consecutive failures; the 4th becomes a transient stream failure
- a tick stream is considered stale only when silence exceeds `1000ms`; dispatcher timeout is set to `1001ms`

Response-header handoff after tick streaming is its own code path:

- after tick-stream completion, dispatcher sets a `5000ms` read timeout
- `readResponseHeaderAfterTickStream(...)` then accepts up to `8` trailing valid running ticks before the response header
- if the next 24 bytes are not a valid running tick for the expected job/session, those bytes are treated as the prefix of the normal 145-byte response header and the remaining bytes are read normally

### 7.6 Remote prior merge and live remote telemetry

`mergeRemotePriorIfNeeded(...)` is not tied only to the chosen slot. `warmupPrior()` can launch the probe for any worker with a routable bridge source.

On successful capabilities query:

1. dispatcher stores the worker signature
2. if capabilities include `osVersion` and non-empty `priorCells`, dispatcher merges a machine into the prior table
   - existing valid persisted values for `msPerFrameC1`, `fixedOverheadMS`, and `avgCorpusFrameCount` are preserved
   - otherwise `msPerFrameC1` can fall back to capability `msPerFrameC1`
   - otherwise fixed overhead falls back to `0`
   - otherwise average corpus frame count falls back to `0`
3. otherwise, if caps have a signature but no usable `priorCells`, dispatcher falls back to persisted cells already present in the local prior table for that signature
4. the worker's `priorP50MSByConcurrency` map is refreshed from those effective cells

This merge affects:

- production CA remote eligibility
- production remote adaptation normalization
- remote ready-time fallback when no live tick ETA exists

---

## 8. Showdown, Prior Maintenance, and Benchmark Entry Flow

### 8.1 FIFO vs CA showdown

`benchmarkThunderboltMeasuredShowdown(...)` runs each arrival profile in both policy orders:

1. FIFO then CA
2. CA then FIFO

Per profile, that yields:

- 2 FIFO runs
- 2 CA runs

Order-neutral averages are computed from those runs.

Orchestration details from `ThunderboltShowdownOrchestration.swift`:

- total measured runs are `profiles.count * 4`
- fixed policy sequences are:
  - `[.fifo, .complexityAware]`
  - `[.complexityAware, .fifo]`
- `prepareThunderboltShowdownSession(...)`:
  - prepares or reuses `ThunderboltCARunSetup`
  - auto-labels workers as `W1`, `W2`, ... when labels are not supplied
  - classifies preflight from `setup.diagnostics.localPriorGap` and `remotePriorGap`
  - that shared classification surface is `BenchmarkPriorPreflightClassification`
  - optionally runs showdown prior maintenance before any measured run
- `runThunderboltShowdownProfile(...)` requires exactly 2 FIFO and 2 CA runs per profile and throws if that invariant is violated
- final promotion handling is deferred until after verdict computation through `finalizeThunderboltShowdownPriorPromotionIfNeeded(...)`

### 8.2 Winner policy

`BenchmarkShowdownPolicyKernel.winner(...)` returns `BenchmarkShowdownSelection` (`.fifo` or `.complexityAware`) and compares FIFO vs CA in this exact order:

1. fewer failed jobs
2. lower makespan
3. lower sumW
4. lower p95
5. if fully tied, FIFO wins

Floating-point ties use `abs(lhs - rhs) < 0.000001`.

### 8.3 Verdict policy

`BenchmarkShowdownPolicyKernel.comparatorDecision(...)` is the shared FIFO-vs-CA gate used by standalone acceptance, showdown verdicts, and guarded prior promotion. It computes:

- `failedCountNonRegression = ca.failedCount <= fifo.failedCount` (with `0.000001` tie tolerance)
- `makespanNonRegression = ca.makespanSeconds <= fifo.makespanSeconds` (with the same `0.000001` tie tolerance)
- `sumWImproved = ca.sumWSeconds < fifo.sumWSeconds`, but near-equal ties within `0.000001` do not count as improvement
- `p95NonRegression = ca.p95Seconds <= fifo.p95Seconds` (with the same `0.000001` tie tolerance)
- `pass = failedCountNonRegression && makespanNonRegression && sumWImproved && p95NonRegression`

`BenchmarkShowdownPolicyKernel.verdict(...)` works over `BenchmarkShowdownComparatorDecision`, `BenchmarkShowdownScore`, `BenchmarkShowdownGuidanceDecision`, and `BenchmarkShowdownVerdict`.

The measured showdown path computes one aggregate `comparatorDecision` from the overall FIFO and CA averages across all runs, then computes:

- `comparatorPass = totalFailedAcrossRuns == 0 && aggregateDecision.pass && profileWins.ca > profileWins.fifo`

Decision order:

1. any failed jobs across runs
   - `stabilizeReliability`
2. shared comparator passes
   - `enableCA`
3. FIFO wins wall-clock scorecard
   - `keepFIFO(requiresPriorRefresh: preflight != .healthy)`
4. count scorecards among:
   - sumW
   - p95
   - wall
   - profile wins
5. if FIFO wins at least 3
   - `keepFIFO(...)`
6. otherwise
   - `inconclusive(requiresPriorRefresh: ...)`

Scoring helpers in `ThunderboltShowdownScoring.swift` also add these exact rules:

- `showdownMetricScore(...)`
  - compares FIFO vs CA one profile at a time
  - uses `showdownNearlyEqual(...)` with `abs(lhs - rhs) < 0.000001`
  - counts `{ fifo, ca, ties }`
- `showdownProfileWinnerScore(...)`
  - counts per-profile winners after `BenchmarkShowdownPolicyKernel.winner(...)`
- `showdownGuidanceLines(verdict:)`
  - returns exactly two next-step lines for every verdict case
- `showdownWinnerLabel(...)`
  - prints either `FIFO`, `CA`, or `tie (...)` from those aggregate score counters

The standalone CA acceptance gate now reuses that same shared comparator decision:

- `evaluateThunderboltCAAcceptanceGate(...)` reports:
  - `sumWImproved`
  - `failedCountNonRegression`
  - `p95Regressed = !p95NonRegression`
  - `makespanRegressed = !makespanNonRegression`
- gate pass is exactly the shared comparator pass:
  - `failedCountNonRegression && makespanNonRegression && sumWImproved && p95NonRegression`

### 8.4 Prior promotion policy

`BenchmarkPriorPolicyKernel.evaluatePromotion(...)` rejects in this order:

1. remote worker coverage regression
2. dropped currently modeled signatures
3. remote slot coverage regression
4. local-prior validity regression
5. weaker corpus than canonical
6. comparator rejected candidate when comparator is required and force is not set

Acceptance rules:

- comparator-required mode:
  - accept if comparator passed, or force was requested
- otherwise:
  - accept if coverage improved
  - or accept if force was requested

`improvedCoverage` is exactly:

- remote slot coverage increased, or
- remote worker coverage increased, or
- local prior validity improved

Force does **not** bypass the earlier no-regression guards.

Force **does** bypass two later checks:

- in comparator-required mode, it bypasses the showdown-comparator rejection gate
- in non-comparator mode, it bypasses the final "must improve coverage" rule

### 8.5 Prior maintenance

Showdown prior maintenance is implemented in `ThunderboltShowdownPriorMaintenance.swift`.

It is skipped immediately when:

- policy is `.off`
- preflight is `.healthy` and `allowHealthyPreflight == false`

The progress board has 6 stages:

1. local sweep
2. frame counting
3. affine sample collection
4. remote sample preparation
5. remote telemetry
6. setup rebuild

Current maintenance flow:

1. run local-only burst configs from `1 ... maxLocal`
2. derive a synthetic local concurrency sweep from the successful local-only runs
   - for each successful local-only run:
     - `serviceSeconds = (wallSeconds * concurrency) / completed`
     - both synthetic `p50Seconds` and `p95Seconds` are set to that service time
   - keep the best (lowest `p50Seconds`) point per concurrency
3. probe frame counts for the corpus
4. collect local concurrency-1 affine samples
5. if remote gaps require it:
   - choose representative videos by frame-count quantiles
   - probe isolated remote telemetry
   - fit remote affine process models
   - build remote prior cells
6. build the candidate through `buildThunderboltShowdownPriorCandidateArtifact(...)`, which ultimately materializes the local/merged artifact through `buildThunderboltLocalPriorCandidateArtifact(...)`
7. apply the requested update policy
8. rebuild `ThunderboltCARunSetup` using the candidate as an overlay

Remote maintenance has two important validity rules:

- sample invalidation is first-match in this order:
  - non-isolated probe
  - unsuccessful remote probe
  - invalid concurrency
  - missing worker signature
  - local fallback
  - executor mismatch
  - missing process time
- a worker is eligible for remote prior generation only if the valid sample set includes concurrency `1`

### 8.6 Prior update policies and bootstrap behavior

Current policy meanings:

- `.off`
  - do not write candidate or canonical output
- `.candidateOnly`
  - write candidate only
- `.promoteForce`
  - candidate may promote without showdown comparator
- `.promoteGuarded`
  - candidate promotion is deferred until showdown comparator passes

Bootstrap rule:

- if there is no canonical prior yet and policy is not `.off`, `applyThunderboltShowdownPriorUpdatePolicy(...)` writes the candidate directly to the canonical path

That means first-run bootstrap bypasses normal candidate-only / guarded promotion semantics, but policy `.off` still skips all writes.

There is also an existing-canonical short-circuit:

- if `allowExistingCanonicalSkip == true`
- and the canonical artifact already contains an exactly matching local machine profile for `localSignature`
- and canonical corpus coverage is equal-or-stronger
- then the update is skipped as "existing canonical already good enough"

### 8.7 Full benchmark pipeline

`benchmarkThunderbolt(...)` currently runs:

1. corpus summary
2. network health
3. profiling
4. burst sweep and leaderboard
5. full prior maintenance, only when:
   - showdown is included, and
   - prior update policy is not `.off`
6. measured FIFO vs CA showdown

`shouldRunThunderboltFullPriorMaintenance(includeShowdown:priorUpdatePolicy:)` is exactly:

- `includeShowdown && priorUpdatePolicy != .off`

When that full-maintenance path runs, it is intentionally broader than normal showdown maintenance:

- it forces `allowHealthyPreflight = true`
- it probes all reachable remote workers, not only currently detected gap hosts

The "network health" phase in that pipeline is concrete:

- resolve benchmark settings
- parse and bridge-bind configured workers
- run reachability probes
- report binding / connectivity failures before later stages

### 8.8 Burst sweep

`executeThunderboltBurstSweep(...)` supports:

- brute-force search
- optimized search

Brute-force:

- validates search-space size
- enumerates baseline first
- then traverses the remaining configs
- runs every config through `runThunderboltBurstConfig(...)`

Optimized:

- delegates to `optimizeBurstConcurrency(...)`
- evaluates fewer configs
- stores realized results in a mutex-protected map
- can display predicted and realized rows separately

`runThunderboltBurstConfig(...)`:

1. creates temporary thumbs/previews directories
2. creates a dispatcher only for remote lanes with non-zero slots
3. creates one lane per local slot plus one lane per active remote slot
   - if that would produce zero lanes, it synthesizes one local fallback lane
4. dispatches each video on the next available lane
5. locally falls back when remote dispatch returns fallback/transient/permanent failure

`ThunderboltBurstSweepOrchestration.swift` adds two benchmark-export behaviors that were previously undocumented:

- `benchmarkThunderboltJSON(...)`
  - requires at least one video
  - requires at least one reachable worker after binding + connectivity filtering
  - runs the burst sweep
  - emits optional prior maintenance side effects through `emitThunderboltJSONPriorAfterBurstSweepIfNeeded(...)`
  - returns a JSON payload for the best successful config
- reachable-worker selection preserves configured worker order
  - unreachable or unbindable workers stay in the exported `workers` list
  - but receive `0` slots in `best_config.remote_workers`

`validateThunderboltBenchmarkJSONPayload(...)` enforces:

- `schema_version` must match the delegated benchmark schema constant
- `workers.count` must exactly match configured `TB_WORKERS` entries
- `workers[index]` must preserve configured order, host token, and configured slots
- `best_config.remote_workers` must contain exactly one ordered entry per configured worker
- remote-worker slot counts must be in `0 ... 16`
- `completed_videos > 0`
- `failed_videos == 0`

The burst-search ceiling for local slots also comes from `ThunderboltCapabilities.sweepCeiling(...)`, not a benchmark-local heuristic.

### 8.9 `scripts/codex_ca_pair.swift`

The script is a thin wrapper around the benchmark executable.

It supports:

- `--media-folder`
- positional media-folder path
- `--profile`
- `--model-mode`
- `--video-preset`
- `--video-timeout`
- `--order`
- `--out-dir`
- `--debug`
- `--release`
- `--help`

Build command:

- release: `swift run -c release benchmark`
- debug: `swift run benchmark`

Each run always passes:

- `--stage thunderbolt`
- `--media-folder`
- `--arrival-profile`
- `--ca-model-mode`
- `--report-dir`
- `--json`
- `--scheduler-policy fifo|ca`
- `--ca-raw-out`
- `--ca-summary-out`

If configured, it also forwards:

- `--video-preset`
- `--video-timeout`

Any arguments after `--` are appended to both benchmark runs unless they are in the script's reserved-flag list.

Then it decodes the run artifacts and prints the pairwise comparison.

### 8.10 Benchmark model and observability types

`Sources/Benchmarks/Steps/Thunderbolt/ThunderboltModels.swift` contains the benchmark-facing wrapper types for:

- bound workers
- slots and slot bindings
- machine profiles
- dispatch items
- model inputs
- adaptation rows
- prediction samples
- solver telemetry rows
- observability payloads
- run setup and diagnostics

Concrete type anchors that matter in practice include:

- `ThunderboltCAMachineProfile`
- `ThunderboltCASlot`
- `ThunderboltCADispatchItem`
- `ThunderboltCAModelInputRow`
- `ThunderboltCAAdaptationRow`
- `ThunderboltCAPredictionSample`
- `ThunderboltCASolverTelemetryRow`
- `ThunderboltCAObservability`
- `ThunderboltCARunSetup`
- `ThunderboltPriorWriteOutcome`
- `ThunderboltRemoteMaintenanceSampleInvalidationReason`

`Sources/Benchmarks/Steps/Thunderbolt/ThunderboltErrors.swift` defines the benchmark/showdown-specific error surfaces:

- `ThunderboltBenchmarkJSONError`
- `ThunderboltCAAcceptanceError`
- `ThunderboltShowdownPriorMaintenanceError`

### 8.11 Activation gate

When `VIDEO_SCHEDULER_POLICY=auto`, production CA enablement is controlled by `CAActivationGate.evaluate(...)`.

When `VIDEO_SCHEDULER_POLICY=fifo`, production forces FIFO dequeue and skips CA enablement even if the gate would otherwise pass.

When `VIDEO_SCHEDULER_POLICY=none`, production forces local-only FIFO and also skips dispatcher creation.

Current decision order:

1. workers must be present
2. prior artifact must not be missing
3. prior artifact must not be invalid
4. local prior profile must resolve
5. strict tick-v2 rollout must not have been rejected
6. otherwise CA is enabled

`strictTickV2Accepted` is optional:

- `nil` means "no explicit rejection" and passes the gate
- only explicit `false` disables CA on the tick-v2 rollout check

The local prior profile for activation comes from `resolveLocalPriorProfile(...)`:

- local hardware detection through `WorkerCaps.detectLocal()`
- local signature construction via `WorkerSignatureBuilder`
- exact lookup in the prior table via `priorProfile(forSignature:in:)`
- if exact lookup fails, falls through to hardware-compatible lookup via `priorTable.hardwareCompatibleMachine(signature:)` and `validatedPriorProfile(from:)`

This means a routine OS update no longer blocks CA activation when a hardware-compatible prior exists.

Production CA still requires a resolved local prior profile (exact or hardware-compatible). Benchmark setup can run with fallback local modeling even when no prior resolves.

---

## 9. Test Inventory

This section is intentionally file-level rather than line-level. The test files are durable blast-radius anchors; per-test line numbers are not.

### Solver and selection

- `Tests/KikoMediaTests/CABranchAndBoundOracleTests.swift`
  - live solver vs exhaustive oracle
  - exclusion clearing
  - projected slot reuse
  - projected machines exactness
- `Tests/KikoMediaTests/CASchedulerDeterminismTests.swift`
  - score-field invariants
  - future-aware single-job decisions
  - batch objective ordering
  - deterministic tie-break behavior
  - degradation clamping
- `Tests/KikoMediaTests/CASolverCoverageTests.swift`
  - exclusion retry semantics
  - projected occupancy carryover
  - deterministic repeated picks
- `Tests/KikoMediaTests/CARemoteDispatchSelectionTests.swift`
  - remote slot/machine scoring
  - busy vs idle scoring
  - frame-count scaling
- `Tests/KikoMediaTests/CAResolvedVideoCostTests.swift`
  - canonical resolved video cost parity between solver and dispatch
  - frame-count fallback derivation
  - benchmark/production resolver sharing
- `Tests/KikoMediaTests/CALiveAdaptationNormalizationTests.swift`
  - shared normalization parity across all four environments (production local/remote, benchmark local/remote)
  - fixed-overhead subtraction
  - degradation-curve normalization

### Production integration and queue behavior

- `Tests/KikoMediaTests/MediaProcessorBehaviorTests.swift`
  - production holds, wakes, recomputes, stale wakes
  - held-only idle-capacity reconsideration, including coalesced completion-refill inside an in-flight generic run
  - transient remote requeue path
  - scheduling metrics
  - projected-state carryover
  - same-slot reuse across multi-pick batches
- `Tests/KikoMediaTests/CAFallbackPolicyTests.swift`
  - fallback-to-local behavior
  - frame-count/runtime fallback rules
  - prior-missing behavior
- `Tests/KikoMediaTests/CALocalFIFOInvariantTests.swift`
  - FIFO invariants when CA is not active
  - held-job reinsert ordering
- `Tests/KikoMediaTests/CAConfigCompatibilityTests.swift`
  - activation-gate compatibility
  - tick-v2 gating impact
- `Tests/KikoMediaTests/CrashRecoveryTests.swift`
  - CA backlog recovery path
  - known-upload per-row DB read failure is retried, then marked failed fail-closed
  - invalid video preview files are rejected during startup artifact verification
  - retained-upload behavior after completion-persistence failure

### Benchmark runtime and model construction

- `Tests/KikoMediaTests/ThunderboltCAModelModeTests.swift`
  - strict vs auto remote modeling
  - tail-telemetry modeling
  - affine fixed-overhead handling
  - benchmark hold/dispatch parity with production decisions
  - solver telemetry collection
- `Tests/KikoMediaTests/ThunderboltCACompletionAccountingTests.swift`
  - adaptation accounting
  - held decision timing preservation
  - arrival waiting and free-slot dispatch behavior
- `Tests/KikoMediaTests/ThunderboltCAUtilityTests.swift`
  - helper math
  - slot enumeration
  - seed runtime / frame-count fallback helpers
  - prior shaping helpers

### Dispatcher, telemetry, tick protocol, and recompute

- `Tests/KikoMediaTests/ThunderboltDispatcherRoutingTests.swift`
  - bridge routing and host resolution
- `Tests/KikoMediaTests/ThunderboltDispatcherRecoveryTests.swift`
  - long-down probe recovery
- `Tests/KikoMediaTests/ThunderboltDispatcherGraceModelTests.swift`
  - grace recovery and retry accounting
- `Tests/KikoMediaTests/ThunderboltDispatcherTickStreamTests.swift`
  - stale tick handling
  - late tick behavior
- `Tests/KikoMediaTests/ThunderboltDispatcherTailTelemetryTests.swift`
  - transfer overhead learning
  - tail telemetry EMA
  - remote live `msPerFrameC1`
  - snapshot fallback remaining-runtime behavior
- `Tests/KikoMediaTests/ThunderboltTransportTests.swift`
  - transport framing
  - capabilities query limits
- `Tests/KikoMediaTests/TickV2Tests.swift`
  - v2 tick encoding/decoding/validation
  - strict rollout gate
  - staleness boundary
- `Tests/KikoMediaTests/RecomputeCoalescingTests.swift`
  - slot-down burst coalescing
  - completion-refill priority and pass-level `reconsiderHeldJobs` propagation
- `Tests/KikoMediaTests/DriftBaselineTests.swift`
  - drift baseline seeding
  - drift trigger arming requirements
- `Tests/KikoMediaTests/ThunderboltCapabilitiesTests.swift`
  - sweep ceiling math

### Showdown, prior artifacts, and prior maintenance

- `Tests/KikoMediaTests/BenchmarkPriorTests.swift`
  - prior artifact load/lookup/signature rules
- `Tests/KikoMediaTests/BenchmarkPriorCanonicalPathTests.swift`
  - canonical-path behavior
- `Tests/KikoMediaTests/PipelinePriorUpdateTests.swift`
  - pipeline update semantics
- `Tests/KikoMediaTests/ThunderboltShowdownGuidanceTests.swift`
  - verdict guidance
- `Tests/KikoMediaTests/ThunderboltShowdownPriorPromotionTests.swift`
  - promotion safety guards
  - guarded vs force semantics
- `Tests/KikoMediaTests/ThunderboltShowdownMetadataOutputTests.swift`
  - showdown metadata output
- `Tests/KikoMediaTests/ThunderboltRemotePriorMaintenanceTests.swift`
  - remote maintenance gap handling
  - overlay usage
  - representative sample selection
- `Tests/KikoMediaTests/ThunderboltFullPriorMaintenanceTests.swift`
  - full-maintenance path before showdown
- `Tests/KikoMediaTests/ThunderboltRemoteMaintenanceTelemetryTests.swift`
  - remote telemetry validity rules
- `Tests/KikoMediaTests/ThunderboltNonJSONPriorEmissionTests.swift`
  - non-JSON/full-path prior emission behavior
- `Tests/KikoMediaTests/BenchmarkWizardThunderboltStepAccountingTests.swift`
  - wizard step accounting for CA/thunderbolt flows

### Metrics and benchmark orchestration

- `Tests/KikoMediaTests/SchedulingMetricsTests.swift`
  - `sumW`, `p95`, `makespan`, `failedCount`
- `Tests/KikoMediaTests/BurstOptimizerTests.swift`
  - optimized burst-search behavior
- `Tests/KikoMediaTests/BenchmarkCLISmokeTests.swift`
  - benchmark CLI smoke coverage
- `Tests/KikoMediaTests/BenchmarkInterruptHandlingTests.swift`
  - interrupt behavior during benchmark flows
- `Tests/KikoMediaTests/BenchmarkLeaderboardPolishTests.swift`
  - burst leaderboard output behavior

---

## 10. Glossary

| Term | Definition |
|---|---|
| `sumW` | Sum of successful-job wall times, where each wall time is `liveAtSeconds - arriveAtSeconds`. Failed jobs are counted separately, not folded into wall times. |
| `p95` | 95th percentile of successful-job wall times using linear interpolation over the sorted wall-time array. |
| `makespan` | `max(liveAtSeconds) - min(arriveAtSeconds)` across successful jobs. |
| `msPerFrameC1` | Milliseconds per frame normalized to concurrency 1. |
| `degradation curve` | Array of `(concurrency, ratioToC1)` values. Runtime slope is multiplied by the ratio for the clamped concurrency. |
| `fixedOverheadMS` | Frame-count-independent per-video cost that is added before `txOutMS` and `publishOverheadMS`. |
| `prior` | Persisted `benchmark-prior.json` artifact. |
| `validated prior profile` | A prior profile with valid adjusted `msPerFrameC1` and a non-empty degradation curve. Production CA activation and production remote eligibility both depend on this shape. |
| `hold` | A selected scheduler pick that is not dispatched yet because its chosen slot is future-ready, or in benchmark runtime because the chosen slot is not currently free. |
| `recompute` | A re-run of CA scheduling after arrival, completion, remote failure, slot-health change, or ETA drift. |
| `slot` | One concurrent execution lane on one machine. |
| `machine` | One modeled executor with slots, runtime slope, degradation curve, and overhead terms. |
| `worker` | A remote Thunderbolt-connected Mac. |
| `bridge source` | The local bridge interface/IP selected for source-bound worker connects. |
| `sessionID` | Random dispatcher session identifier embedded in v2 tick MIME metadata and required on v2 tick frames. |
| `job handle` | Per-dispatch monotonic identifier used to match tick frames to the active remote dispatch. |
| `tick v2` | The current 24-byte binary progress frame used when CA scheduling is enabled. |
| `transfer-in` | Time to get the source video to the worker; modeled as `txInMS`. |
| `transfer-out` | Time to receive processed artifacts after the worker finishes; modeled as `txOutMS`. |
| `publish overhead` | Post-receive verification/finalization time after payload transfer; modeled as `publishOverheadMS`. |
| `drift` | Difference between the solver-seeded remote ETA baseline and live tick ETA after accounting for elapsed time. |
| `grace recovery` | Short wait window after a down-slot event during which a recovered slot can avoid incrementing durable retry state. |
| `long-down probe` | Background reconnection loop for down slots using exponential backoff plus jitter. |
| `showdown` | FIFO vs CA head-to-head benchmark with order-neutral averaging. |
| `comparator pass` | `true` only when there were no failures and CA won more arrival profiles than FIFO. |
| `promotion` | Replacing canonical `benchmark-prior.json` with a candidate, subject to no-regression and optional comparator rules. |
| `bootstrap write` | First-run behavior where a candidate is written directly to the canonical prior path because no canonical prior exists yet and the update policy is not `.off`. |
| `full prior maintenance` | The pre-showdown six-stage candidate-building flow used when showdown is enabled and prior update policy is not `.off`. |
