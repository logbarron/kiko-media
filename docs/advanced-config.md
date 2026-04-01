# Advanced Configuration

Most tabled environment variables in this document are defined in `deploy/defaults.env` with defaults. `TURNSTILE_SECRET`, `SESSION_HMAC_SECRET`, and `INTERNAL_AUTH_SECRET` are required runtime secrets, and `GATE_SECRET` is an optional runtime secret; these are provided via setup/runtime environment and are not read from `deploy/defaults.env`. Most deployments need no changes beyond what the setup wizard collects (domain, WireGuard IP, Cloudflare token, Turnstile keys, optional gate secret, SSD path, timezone).

Override any value via environment variable or the setup wizard's "Advanced Configuration" step. Invalid numeric/ranged values are logged as warnings and the default is used instead. `VIDEO_TRANSCODE_PRESET` and `VIDEO_SCHEDULER_POLICY` are fail-fast: startup refuses to proceed if either value is unsupported.

Runtime default maps in `KikoMediaCore` are generated from `deploy/defaults.env`:
```bash
swift scripts/generate_config_defaults.swift
```
CI/local verification command:
```bash
swift scripts/generate_config_defaults.swift --check
```

---

## How to Override

`__REPO_DIR__` below means wherever you cloned the kiko-media repo (see `docs/runbook.md` "Your Values" table for all placeholders).

**Environment variable (manual run):**
```bash
MAX_CONCURRENT_IMAGES=6 VIDEO_TRANSCODE_TIMEOUT=600 __REPO_DIR__/.build/release/KikoMedia
```

**launchd plist (production):**
Edit `~/Library/LaunchAgents/com.kiko.media.plist` and change the value in the `EnvironmentVariables` dict:
```xml
<key>MAX_CONCURRENT_IMAGES</key>
<string>6</string>
```
Then reload:
```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
```

**Setup wizard:**
```bash
swift run -c release orchestrator
```
At Step 10, choose "y" for advanced options. The wizard walks through each section, writes runtime values into launchd plists, and applies frontend values when generating `deploy/index.html`/`deploy/Caddyfile`.

Service-control shortcuts:
```bash
swift run -c release orchestrator --status
swift run -c release orchestrator --tb-status
swift run -c release orchestrator --start
swift run -c release orchestrator --shutdown
swift run -c release orchestrator --restart
swift run -c release orchestrator --thunderbolt
```

Frontend artifact refresh:
```bash
swift scripts/regen-frontend.swift
```

---

## Operational

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `PUBLIC_PORT` | int | `3001` | 1-65535 | Port for the public-facing HTTP listener |
| `INTERNAL_PORT` | int | `3002` | 1-65535 | Port for the internal/moderation HTTP listener |
| `BIND_ADDRESS` | string | `127.0.0.1` | -- | Host/address value for the public HTTP listener (typically an IP). Only affects the public listener. The internal listener always binds to 127.0.0.1 for security |
| `BASE_DIRECTORY` | string | `~/Documents/kiko-media` | -- | Root directory for all runtime data (uploads, thumbs, previews, logs, moderated, metadata.db) |
| `EXTERNAL_SSD_PATH` | string | `""` *(empty; setup wizard normally sets this)* | -- | Path where originals are archived with SHA256 verification. Must be a `/Volumes/<VolumeName>/...` path (mount detection is strict), e.g. `/Volumes/YourSSD/originals` |
| `HEALTH_CHECK_INTERVAL` | int | `60` | 1-3600 | Seconds between DB watchdog health checks. On failure, triggers graceful shutdown; launchd then restarts the process |
| `JSON_MAX_BODY_BYTES` | int | `1048576` | 65536-4194304 | Maximum JSON request body bytes accepted by bounded API body collection |
| `WEBHOOK_RETRY_AFTER_SECONDS` | int | `5` | 1-120 | `Retry-After` seconds returned on webhook queue-full/shutdown `503` responses |
| `TUSD_MAX_SIZE` | int | `2147483648` | 1-10737418240 | Maximum upload size in bytes (default 2GB, max 10GB). Enforced by tusd before files reach kiko-media |
| `EVENT_TIMEZONE` | string | `""` | -- | IANA timezone ID (e.g. `America/New_York`). Empty or unset = system timezone. Frozen at startup for deterministic EXIF timestamps |

When to change:
- `PUBLIC_PORT` / `INTERNAL_PORT` -- if the default ports conflict with other services. After changing them, regenerate `deploy/Caddyfile` from `deploy/Caddyfile.template` (or manually edit the generated `deploy/Caddyfile`) so reverse proxies target `127.0.0.1:__PUBLIC_PORT__` and `127.0.0.1:__INTERNAL_PORT__` correctly. Also update tusd hook target wiring (`-hooks-http http://127.0.0.1:__INTERNAL_PORT__/hooks/upload-complete`) and any HAProxy/WireGuard frontends if those assumptions are hard-coded in your deployment. The internal listener is still bound to `127.0.0.1`, so moderation APIs remain network-restricted even when you expose the public listener elsewhere.
- `BIND_ADDRESS` -- only affects the public listener bind address in `kiko-media`; Caddy public API upstream hops stay pinned to `127.0.0.1:__PUBLIC_PORT__`. The internal listener always binds to `127.0.0.1` regardless of this setting, so moderation endpoints are never network-accessible. Only change if you need the public listener accessible on a non-loopback interface. The default `127.0.0.1` means only Caddy and other local processes on the same machine can reach it.
- `BASE_DIRECTORY` -- if the default `~/Documents/kiko-media` is on a volume with insufficient space.
- `HEALTH_CHECK_INTERVAL` -- lower for faster crash detection, higher to reduce log noise.
- `JSON_MAX_BODY_BYTES` -- raise only if you intentionally accept larger JSON payloads. Keep tight to bound request-memory pressure. This limit also applies to tusd webhook JSON bodies (`pre-create`/`post-finish`).
- `WEBHOOK_RETRY_AFTER_SECONDS` -- increase if upstream retry behavior should back off more aggressively under queue pressure.
- `TUSD_MAX_SIZE` -- increase if guests need to upload files larger than 2GB (e.g. long 4K videos). Max 10GB. The setup wizard propagates this value to both tusd and the upload page.
- If `PUBLIC_PORT` and `INTERNAL_PORT` are set to conflicting bind tuples, startup fails with an address-in-use bind error.

Operational note:
- `__BASE_DIRECTORY__/moderated/` must remain readable. If marker files are unreadable, startup moderation reconciliation is skipped and SSD rebuild aborts (fail closed) to avoid unintentionally exposing moderated content.
- Keep the Caddy/tusd wiring in sync with these ports. Caddy serves the public site on `https://__DOMAIN__:8443` (bound to the WireGuard IP) and reverse-proxies public API routes to `127.0.0.1:__PUBLIC_PORT__`, proxies `/files/*` to tusd at `127.0.0.1:1080`, and proxies internal-site API routes to `127.0.0.1:__INTERNAL_PORT__`; tusd's `-hooks-http` target uses `INTERNAL_PORT`. After editing any of these env vars, reload the launchd plists so the new values take effect (`launchctl bootout`/`bootstrap` or `swift run -c release orchestrator --restart`).

---

## Processing

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `MAX_CONCURRENT_IMAGES` | int | `6` | 1-64 | Parallel image processing jobs |
| `MAX_CONCURRENT_VIDEOS` | int | `2` | 1-16 | Parallel local video processing jobs (Media Engine) |
| `VIDEO_SCHEDULER_POLICY` | string | `auto` | -- | Production video scheduling policy: `auto`, `fifo`, or `none` |
| `TB_WORKERS` | string | `""` | -- | Thunderbolt worker list in `host:slots,host:slots` format (example: `192.168.100.10:2,192.168.100.20:1`) |
| `TB_PORT` | int | `12400` | 1-65535 | TCP port used for all Thunderbolt worker connections |
| `TB_CONNECT_TIMEOUT` | int | `500` | 100-30000 | Milliseconds to wait for worker TCP connect attempt; timeout/unreachable connect falls back to local processing for that job |
| `MAX_CONCURRENT_REBUILD_PROBES` | int | `8` | 1-128 | Parallel SSD rebuild candidate/timestamp probe workers |
| `MAX_PENDING_WEBHOOK_JOBS` | int | `1000` | 0-100000 | Maximum pending webhook-enqueued jobs before new webhook inserts are rejected with `503` |
| `THUMBNAIL_SIZE` | int | `512` | 1-10000 | Max thumbnail dimension in pixels |
| `THUMBNAIL_QUALITY` | double | `0.85` | 0.0-1.0 | JPEG compression quality for thumbnails |
| `PREVIEW_SIZE` | int | `1440` | 1-10000 | Max preview image dimension in pixels |
| `PREVIEW_QUALITY` | double | `0.90` | 0.0-1.0 | JPEG compression quality for preview images |
| `VIDEO_THUMBNAIL_SIZE` | int | `512` | 1-10000 | Max video thumbnail dimension in pixels |
| `VIDEO_THUMBNAIL_TIME` | double | `1.0` | 0.0-300.0 | Seconds into video to capture thumbnail frame |
| `VIDEO_THUMBNAIL_QUALITY` | double | `0.85` | 0.0-1.0 | JPEG compression quality for video thumbnails |
| `VIDEO_TRANSCODE_TIMEOUT` | int | `300` | 1-3600 | Max seconds for a single video transcode operation |
| `VIDEO_TRANSCODE_PRESET` | string | `AVAssetExportPreset1920x1080` | Must match one of `AVAssetExportSession.allExportPresets()` on this host | AVFoundation export preset name for video transcoding |

When to change:
- `MAX_CONCURRENT_IMAGES` -- increase on machines with more cores, decrease if memory is constrained. Each image job uses CoreGraphics which is CPU-bound.
- `MAX_CONCURRENT_VIDEOS` -- this controls only local video slots. Keep the default `2` local slots, then tune with benchmark sweeps for your workload.
- `VIDEO_SCHEDULER_POLICY` -- choose the production behavior explicitly:
  - `auto` = current behavior; if offload is configured and CA readiness passes, use CA, otherwise fall back to FIFO with offload
  - `fifo` = force FIFO dequeue while keeping Thunderbolt offload available when workers and bridge are present
  - `none` = disable Thunderbolt runtime offload and run local-only FIFO
  Invalid values fail startup. `benchmark-prior.json` matters for production only when this policy is `auto`.
- `TB_WORKERS` -- set worker endpoints as `host:slots,host:slots`. Example: `192.168.100.10:2,192.168.100.20:1`. Use IPv4 worker addresses on the Thunderbolt Bridge subnet; dispatch source-route selection is bridge-subnet based. `TB_WORKERS` describes worker inventory and remote slot capacity; it does not by itself decide production scheduler policy. If no bridge source exists at startup, remote offload is disabled for that process until restart. Run `swift run -c release orchestrator --thunderbolt` to detect bridges, benchmark throughput, and write validated `TB_WORKERS`/`TB_PORT`/`TB_CONNECT_TIMEOUT` into the LaunchAgent. For the operator flow that finishes CA readiness under `VIDEO_SCHEDULER_POLICY=auto`, follow `docs/runbook.md` after Thunderbolt setup and complete the benchmark step there before restarting.
- `TB_PORT` -- change only if workers are configured to listen on a non-default port.
- `TB_CONNECT_TIMEOUT` -- controls TCP connect attempt duration in milliseconds. Increase on unstable links; lower for faster local fallback when workers are down. Other remote-dispatch failures also fall back to local processing. Legacy values `1...30` are interpreted as seconds by runtime config loading for backward compatibility.
- `TB_WORKERS` and CA readiness -- when `VIDEO_SCHEDULER_POLICY=auto`, parsed-valid `TB_WORKERS` is the first prerequisite for complexity-aware (CA) scheduling. Full CA scheduling additionally requires: a valid benchmark-prior.json v2 artifact at `{BASE_DIRECTORY}/benchmark-prior.json`, a valid local machine prior profile matching the current hardware + `VIDEO_TRANSCODE_PRESET`, and all configured workers reporting `tick_version=2` via source-routed capability probes at startup. If any gate fails, the system logs the specific reason and falls back to FIFO dequeue. Remote offload still remains best-effort whenever the dispatcher exists. If parsing yields zero valid workers (empty or invalid-only string), behavior is local FIFO.
- When remote offload is active, strict tick v2 validation is enforced for worker progress frames and benchmark-prior cells are merged once per worker on first successful capability probe.
- Thunderbolt benchmark sweep mode is runtime-only (not an env var): use `--sweep-mode smart|exhaustive` with `swift run benchmark --stage thunderbolt --json ...`, or choose sweep mode interactively in `swift run -c release orchestrator --thunderbolt` when config space is >25.
- `MAX_CONCURRENT_REBUILD_PROBES` -- controls startup SSD rebuild probe/timestamp worker fan-out. Increase if rebuild of very large corpuses is slow; decrease if startup competes too heavily for CPU/IO.
- `MAX_PENDING_WEBHOOK_JOBS` -- lower this if you want stricter backpressure under ingest spikes; raise it if bursts are expected and you have RAM headroom. `0` rejects all new webhook inserts (maintenance mode behavior). When the processor is shutting down or rejects an insert, the webhook handler returns `503` with `Retry-After: WEBHOOK_RETRY_AFTER_SECONDS`, so adjust that retry delay in concert with this limit.
- `THUMBNAIL_SIZE` / `PREVIEW_SIZE` -- adjust output resolution. Larger previews use more disk and bandwidth.
- `*_QUALITY` -- lower values reduce file size at the cost of visual quality. 0.85 is a good balance for thumbnails; 0.90 preserves detail in previews.

Behavior note:
- JPEG thumbnail/preview writes always enable ImageIO sharing color optimization (`kCGImageDestinationOptimizeColorForSharing`) to normalize wide-gamut sources for broader client compatibility. This has no env-var toggle.
- If `database.updateType` fails during processing, the job aborts early and remains recoverable on startup; this behavior has no env-var toggle.
- Startup SSD rebuild candidate probing/timestamp extraction uses bounded task-group parallelism capped by `MAX_CONCURRENT_REBUILD_PROBES`.
- `VIDEO_TRANSCODE_TIMEOUT` -- increase for very long videos (>10 min). If a transcode exceeds this, the job fails.
- `VIDEO_TRANSCODE_PRESET` -- choose whichever entry from `AVAssetExportSession.allExportPresets()` matches your output target (default `AVAssetExportPreset1920x1080`). Startup fails fast if the preset is unsupported on the current machine.
- Offload and queue policy are related but not identical:
  - `VIDEO_SCHEDULER_POLICY=auto` means CA when ready, otherwise FIFO with offload when a dispatcher exists
  - `VIDEO_SCHEDULER_POLICY=fifo` means FIFO with offload when a dispatcher exists
  - `VIDEO_SCHEDULER_POLICY=none` means local-only FIFO, even if `TB_WORKERS` is configured
  - empty/invalid `TB_WORKERS` or no bridge interface means no dispatcher, regardless of policy
- Remote dispatch is best-effort per job. If dispatch cannot start or fails after connect (protocol/validation/worker/payload integrity failure), the same job immediately runs locally.
- If webhook queue admission is closed (pending depth at `MAX_PENDING_WEBHOOK_JOBS` or processor shutdown) or an enqueue rollback occurs, new upload IDs receive `503 Service Unavailable` with `Retry-After: WEBHOOK_RETRY_AFTER_SECONDS`; existing IDs still return `200` (idempotent duplicate behavior). This gives tusd/client retries time to back off before re-posting.

---

## Frontend (Generated `deploy/index.html`)

These variables are consumed by the setup wizard when generating `deploy/index.html` and are not read directly by `kiko-media` at runtime.
After editing `deploy/index.html.template`, run `swift scripts/regen-frontend.swift` to re-render the generated HTML and matching CSP hashes from the existing persisted config.

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `UPLOAD_CHUNK_SIZE_BYTES` | int | `5242880` | 262144-33554432 | tus-js upload chunk size in bytes |
| `PARALLEL_UPLOADS` | int | `1` | 1-6 | Max concurrent uploads per browser tab |
| `UPLOAD_RETRY_BASE_MS` | int | `3000` | 100-60000 | Base delay for upload retry backoff |
| `UPLOAD_RETRY_MAX_MS` | int | `20000` | 500-300000 | Max delay for upload retry backoff |
| `UPLOAD_RETRY_STEPS` | int | `5` | 2-8 | Number of upload retry delay entries |
| `POLL_MAX_INFLIGHT` | int | `3` | 1-20 | Max simultaneous pending-tile poll requests |
| `GALLERY_POLL_BASE_MS` | int | `5000` | 1000-60000 | Base gallery poll interval for new uploads |
| `GALLERY_POLL_MAX_MS` | int | `30000` | 2000-300000 | Max gallery poll backoff interval |
| `PHOTO_THUMB_POLL_BASE_MS` | int | `500` | 100-10000 | Base pending-photo thumbnail poll delay |
| `PHOTO_THUMB_POLL_MAX_MS` | int | `5000` | 500-60000 | Max pending-photo thumbnail poll delay |
| `PHOTO_PREVIEW_POLL_BASE_MS` | int | `1000` | 100-10000 | Base pending-photo preview poll delay |
| `PHOTO_PREVIEW_POLL_MAX_MS` | int | `10000` | 500-120000 | Max pending-photo preview poll delay |
| `VIDEO_PREVIEW_EARLY_BASE_MS` | int | `1000` | 100-10000 | Base pending-video early-phase poll delay |
| `VIDEO_PREVIEW_EARLY_MAX_MS` | int | `4000` | 500-60000 | Max pending-video early-phase poll delay |
| `VIDEO_PREVIEW_LATE_MS` | int | `10000` | 1000-120000 | Pending-video late-phase fixed poll delay |
| `VIDEO_PREVIEW_EARLY_WINDOW_MS` | int | `60000` | 1000-600000 | Time before pending-video polling switches to late phase |

When to change:
- `UPLOAD_CHUNK_SIZE_BYTES` -- raise for faster uploads on stable networks; lower on unstable/mobile links.
- `PARALLEL_UPLOADS` -- increase for faster ingest from a single client; this also increases backend pressure.
- `UPLOAD_RETRY_*` -- tune retry aggressiveness for unreliable guest networks.
- `POLL_MAX_INFLIGHT` -- lower to reduce concurrent poll traffic, higher to reduce pending-tile readiness latency.
- `GALLERY_POLL_*` -- tune how quickly clients discover uploads from other guests.
- `PHOTO_*` and `VIDEO_*` poll knobs -- tune pending tile readiness cadence without editing template code.

---

## Security

> **WARNING:** These values are safety limits that protect against decompression bomb attacks and malicious uploads. Changing them may expose the system to denial-of-service or memory exhaustion. Only modify if you understand the implications.

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `MAX_IMAGE_PIXELS` | int | `250000000` | 1-1000000000 | Maximum total pixels (width x height) allowed in an uploaded image |
| `MAX_IMAGE_DIMENSION` | int | `20000` | 1-100000 | Maximum single dimension (width or height) in pixels |
| `MAX_COMPRESSION_RATIO` | double | `200.0` | 1.0-10000.0 | Maximum ratio of decompressed size to file size. Images exceeding this are rejected as potential decompression bombs |

When to change:
- `MAX_IMAGE_PIXELS` -- the default (250 million pixels) allows images up to ~15,800 x 15,800. Increase only if you expect legitimate uploads above this.
- `MAX_IMAGE_DIMENSION` -- the default (20,000px) handles panoramas and very high-resolution photos. Increase for specialized imaging equipment.
- `MAX_COMPRESSION_RATIO` -- a crafted image can be tiny on disk but enormous when decoded. The default 200x threshold blocks most decompression bombs while allowing normal photos. Raising this reduces protection.

Fixed (non-configurable) validation limits:
- Asset IDs are validated at request boundaries and must be non-empty, `<=128` UTF-8 bytes, and must not contain path separators, `..`, leading `.`, NUL, whitespace, or control characters.

---

## Database

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `SQLITE_BUSY_TIMEOUT` | int | `5000` | 0-60000 | Milliseconds to wait when the database is locked before returning SQLITE_BUSY |
| `SQLITE_CACHE_SIZE` | int | `-20000` | -- | SQLite page cache size. Negative = KiB, positive = pages. `-20000` = 20,000 KiB |
| `DEFAULT_PAGE_SIZE` | int | `100` | 1-10000 | Default number of assets per gallery page when `?limit` is not specified |
| `MAX_PAGE_SIZE` | int | `500` | 1-100000 | Effective maximum for `?limit` in gallery queries (higher values are clamped) |
| `MAX_PAGE_OFFSET` | int | `10000` | 0-1000000 | Effective maximum for `?offset` (higher values are clamped), capping deep pagination |
| `SQL_BATCH_SIZE` | int | `500` | 1-999 | Rows per batch for bulk SQL operations (e.g., `getExistingIds` during crash recovery). Must be under SQLite's 999 variable limit |

When to change:
- `SQLITE_BUSY_TIMEOUT` -- increase if you see SQLITE_BUSY errors under heavy load.
- `SQLITE_CACHE_SIZE` -- increase for better read performance on machines with spare RAM. `-40000` = 40 MB.
- `DEFAULT_PAGE_SIZE` / `MAX_PAGE_SIZE` -- adjust for frontend infinite scroll behavior. Larger pages = fewer requests but more data per response.
- `SQL_BATCH_SIZE` -- keep within `1-999` (SQLite variable limit). Lower values split `getExistingIds` into more queries.
- Pagination inputs are clamped server-side (`limit` to `1...MAX_PAGE_SIZE`, `offset` to `0...MAX_PAGE_OFFSET`) rather than rejected.

---

## Session

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `SESSION_COOKIE_TTL` | int | `14400` | 1-86400 | Session cookie lifetime in seconds (14400 = 4 hours) |
| `SESSION_COOKIE_NAME` | string | `kiko_session` | -- | Name of the HMAC-signed session cookie |
| `TURNSTILE_SECRET` | string | *(required; set by setup wizard)* | -- | Cloudflare Turnstile secret used for siteverify. Must be non-empty or startup fails |
| `SESSION_HMAC_SECRET` | string | *(required; generated by setup wizard)* | >=32 bytes | HMAC key used to sign session cookies. Must be at least 32 bytes or startup fails |
| `INTERNAL_AUTH_SECRET` | string | *(required; generated by setup wizard)* | >=32 bytes (recommended) | Shared secret for internal moderation API authorization (`Authorization` header injected by local Caddy internal site). Empty/missing values fail closed on protected internal routes (`401`) |
| `GATE_SECRET` | string | *(optional; unset by default)* | -- | Optional shared event gate secret checked at `/api/turnstile/verify`. When unset/empty, Turnstile-only behavior remains |
| `TURNSTILE_VERIFY_TIMEOUT` | int | `10` | 1-120 | Seconds to wait for Cloudflare Turnstile siteverify API response |
| `TURNSTILE_MAX_RESPONSE` | int | `65536` | 1024-1048576 | Maximum bytes to read from Turnstile siteverify response |
| `TURNSTILE_MAX_INFLIGHT_VERIFICATIONS` | int | `64` | 1-1024 | Maximum concurrent Turnstile siteverify calls |
| `TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS` | int | `1` | 1-10 | `Retry-After` seconds returned when `/api/turnstile/verify` is overloaded/unavailable |
| `TURNSTILE_EXPECTED_HOSTNAME` | string | `""` | -- | Expected hostname in Turnstile siteverify response (must match your public gallery host, e.g. `photos.example.com`) |
| `TURNSTILE_EXPECTED_ACTION` | string | `kiko_verify` | -- | Expected action in Turnstile siteverify response (must match frontend widget `action`) |
| `TURNSTILE_EXPECTED_CDATA` | string | `kiko_public` | -- | Expected cdata in Turnstile siteverify response (must match frontend widget `cData`) |

When to change:
- `SESSION_COOKIE_TTL` -- increase if guests complain about frequent re-verification. Decrease for tighter security. Foreground API calls re-run Turnstile automatically on expiry; background pollers pause and resume after user-triggered verification.
- `TURNSTILE_SECRET` / `SESSION_HMAC_SECRET` / `INTERNAL_AUTH_SECRET` -- wizard-generated values are read at startup. Rotate only when necessary (for example, suspected compromise), then restart services so updated values are loaded.
- `GATE_SECRET` -- set to require an additional gate secret at `/api/turnstile/verify`; unset (or set to empty) to disable the extra gate and return to Turnstile-only behavior.
- `TURNSTILE_VERIFY_TIMEOUT` -- increase on slow networks. Decrease to fail faster if Cloudflare is unreachable.
- `TURNSTILE_EXPECTED_HOSTNAME` -- normally leave as generated by the setup wizard (copied from Step 1 Domain). Override only if your expected public hostname changes.
- `TURNSTILE_EXPECTED_ACTION` / `TURNSTILE_EXPECTED_CDATA` -- override only if you intentionally change frontend Turnstile widget `action` / `cData` values. Keep backend and frontend values in sync.

Behavior note:
- Turnstile siteverify retries once on transient failures (`408`, `429`, `5xx`, or transient network errors) with a short backoff.
- Hostname comparison is normalized to lowercase without trailing dots; `action` and `cdata` are trimmed before comparison.
- If `GATE_SECRET` is set and non-empty, `/api/turnstile/verify` requires `turnstileOK` plus a matching `gateSecret` in the request body before issuing a session cookie. Legacy `password` and `inviteToken` request fields remain accepted for compatibility.
- Successful `/api/turnstile/verify` sets `SESSION_COOKIE_NAME` with `HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=SESSION_COOKIE_TTL`.
- Public `/api/gallery`, `/api/thumbs/{id}`, and `/api/preview/{id}` require a valid `SESSION_COOKIE_NAME` cookie and return `401` when missing/invalid.
- The tusd `pre-create` hook validates the browser `SESSION_COOKIE_NAME` cookie (default `kiko_session`) using the HMAC secret. If missing or malformed, `pre-create` returns `401` and the upload is rejected before creation. `post-finish` is still forwarded by tusd, but cookie-based admission gating happens at `pre-create`.
- When Turnstile verification is overloaded/unavailable (exceeds `TURNSTILE_MAX_INFLIGHT_VERIFICATIONS` or Cloudflare is transiently failing), `/api/turnstile/verify` returns `503 Service Unavailable` with `Retry-After: TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS` so the client can retry the same token.

---

## Caching

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `CACHE_CONTROL` | string | `public, max-age=31536000, immutable` | -- | Cache-Control header value for served thumbnails and previews |

When to change:
- During development, set to `no-cache` (or a low TTL) to surface changes quickly. In production, the default (1 year, immutable) is generally safe because new uploads use unique asset IDs, so caches stay tied to each asset ID.

---

## Internal

| Env Var | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `QUEUE_COMPACTION_THRESHOLD` | int | `100` | 1-100000 | Queue-head threshold for compaction; compaction runs when head > threshold and head > half the queue |
| `SHA256_BUFFER_SIZE` | int | `1048576` | 4096-16777216 | Buffer size in bytes for reading files during SHA256 hashing (default 1 MB) |

When to change:
- These rarely need adjustment. `SHA256_BUFFER_SIZE` can be increased on machines with large RAM to reduce read-call overhead when hashing very large files. `QUEUE_COMPACTION_THRESHOLD` controls when the processing queues drop already-processed entries (the head is reset when it exceeds the threshold and more than half the queue has been consumed); lower values keep the slice compacted more often, higher values delay compaction to amortize work during large single sessions.

Behavior note:
- Media processing graceful shutdown (track/cancel/join in-flight jobs during app teardown) is always enabled and has no environment toggle.

---

## Quick Reference

| Env Var | Default |
|---------|---------|
| `PUBLIC_PORT` | `3001` |
| `INTERNAL_PORT` | `3002` |
| `BIND_ADDRESS` | `127.0.0.1` |
| `BASE_DIRECTORY` | `~/Documents/kiko-media` |
| `EXTERNAL_SSD_PATH` | *(set by setup wizard)* |
| `HEALTH_CHECK_INTERVAL` | `60` |
| `JSON_MAX_BODY_BYTES` | `1048576` |
| `WEBHOOK_RETRY_AFTER_SECONDS` | `5` |
| `TUSD_MAX_SIZE` | `2147483648` |
| `EVENT_TIMEZONE` | `""` |
| `MAX_CONCURRENT_IMAGES` | `6` |
| `MAX_CONCURRENT_VIDEOS` | `2` |
| `VIDEO_SCHEDULER_POLICY` | `auto` |
| `TB_WORKERS` | `""` |
| `TB_PORT` | `12400` |
| `TB_CONNECT_TIMEOUT` | `500` |
| `MAX_CONCURRENT_REBUILD_PROBES` | `8` |
| `MAX_PENDING_WEBHOOK_JOBS` | `1000` |
| `THUMBNAIL_SIZE` | `512` |
| `THUMBNAIL_QUALITY` | `0.85` |
| `PREVIEW_SIZE` | `1440` |
| `PREVIEW_QUALITY` | `0.90` |
| `VIDEO_THUMBNAIL_SIZE` | `512` |
| `VIDEO_THUMBNAIL_TIME` | `1.0` |
| `VIDEO_THUMBNAIL_QUALITY` | `0.85` |
| `VIDEO_TRANSCODE_TIMEOUT` | `300` |
| `VIDEO_TRANSCODE_PRESET` | `AVAssetExportPreset1920x1080` |
| `UPLOAD_CHUNK_SIZE_BYTES` | `5242880` |
| `PARALLEL_UPLOADS` | `1` |
| `UPLOAD_RETRY_BASE_MS` | `3000` |
| `UPLOAD_RETRY_MAX_MS` | `20000` |
| `UPLOAD_RETRY_STEPS` | `5` |
| `POLL_MAX_INFLIGHT` | `3` |
| `GALLERY_POLL_BASE_MS` | `5000` |
| `GALLERY_POLL_MAX_MS` | `30000` |
| `PHOTO_THUMB_POLL_BASE_MS` | `500` |
| `PHOTO_THUMB_POLL_MAX_MS` | `5000` |
| `PHOTO_PREVIEW_POLL_BASE_MS` | `1000` |
| `PHOTO_PREVIEW_POLL_MAX_MS` | `10000` |
| `VIDEO_PREVIEW_EARLY_BASE_MS` | `1000` |
| `VIDEO_PREVIEW_EARLY_MAX_MS` | `4000` |
| `VIDEO_PREVIEW_LATE_MS` | `10000` |
| `VIDEO_PREVIEW_EARLY_WINDOW_MS` | `60000` |
| `MAX_IMAGE_PIXELS` | `250000000` |
| `MAX_IMAGE_DIMENSION` | `20000` |
| `MAX_COMPRESSION_RATIO` | `200.0` |
| `SQLITE_BUSY_TIMEOUT` | `5000` |
| `SQLITE_CACHE_SIZE` | `-20000` |
| `DEFAULT_PAGE_SIZE` | `100` |
| `MAX_PAGE_SIZE` | `500` |
| `MAX_PAGE_OFFSET` | `10000` |
| `SQL_BATCH_SIZE` | `500` |
| `SESSION_COOKIE_TTL` | `14400` |
| `SESSION_COOKIE_NAME` | `kiko_session` |
| `TURNSTILE_SECRET` | *(required; set by setup wizard)* |
| `SESSION_HMAC_SECRET` | *(required; generated by setup wizard)* |
| `INTERNAL_AUTH_SECRET` | *(required; generated by setup wizard)* |
| `GATE_SECRET` | *(optional; unset by default)* |
| `TURNSTILE_VERIFY_TIMEOUT` | `10` |
| `TURNSTILE_MAX_RESPONSE` | `65536` |
| `TURNSTILE_MAX_INFLIGHT_VERIFICATIONS` | `64` |
| `TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS` | `1` |
| `TURNSTILE_EXPECTED_HOSTNAME` | `""` |
| `TURNSTILE_EXPECTED_ACTION` | `kiko_verify` |
| `TURNSTILE_EXPECTED_CDATA` | `kiko_public` |
| `CACHE_CONTROL` | `public, max-age=31536000, immutable` |
| `QUEUE_COMPACTION_THRESHOLD` | `100` |
| `SHA256_BUFFER_SIZE` | `1048576` |

Additionally, `TURNSTILE_SECRET`, `SESSION_HMAC_SECRET`, `INTERNAL_AUTH_SECRET`, `EXTERNAL_SSD_PATH`, and optional `GATE_SECRET` are collected by the basic setup wizard steps. Leave `GATE_SECRET` empty to keep Turnstile-only behavior, or set it to require gate-secret proof at verify time. `TURNSTILE_EXPECTED_HOSTNAME` is set automatically from Step 1 Domain, and `TURNSTILE_EXPECTED_ACTION` / `TURNSTILE_EXPECTED_CDATA` default to `kiko_verify` / `kiko_public` (all overridable in advanced options). Thunderbolt worker discovery/benchmarking lives in `swift run -c release orchestrator --thunderbolt`, which writes `TB_WORKERS`/`TB_PORT`/`TB_CONNECT_TIMEOUT` into the media LaunchAgent.

> **Note:** Startup is fail-closed for Turnstile gating. `TURNSTILE_SECRET`, `SESSION_HMAC_SECRET`, `TURNSTILE_EXPECTED_HOSTNAME`, `TURNSTILE_EXPECTED_ACTION`, and `TURNSTILE_EXPECTED_CDATA` must all be non-empty, and `SESSION_HMAC_SECRET` must be at least 32 bytes. If any check fails, `kiko-media` refuses to start. The setup wizard auto-generates a 32-byte HMAC secret using the system CSPRNG (`SecRandomCopyBytes`) and writes it to the launchd plist. (Manual: `openssl rand -base64 32`)
>
> **Verification fields:** In addition to hostname, server-side Turnstile verification enforces `action` and `cdata` using `TURNSTILE_EXPECTED_ACTION` and `TURNSTILE_EXPECTED_CDATA`. The setup wizard writes matching values into `deploy/index.html` widget config.
