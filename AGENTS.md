# Repository Guidelines

## What This Is

Photo and video upload system for single-day events. Guests upload from phones, media is processed, and the results are shown in a gallery. There are no user accounts. Public gallery/media/heart routes use Cloudflare Turnstile-backed session cookies, and `GATE_SECRET` is optional extra gating checked during `/api/turnstile/verify`.

## Design Philosophy: Rule 1

All changes must satisfy these constraints, in priority order:

1. **Performance is non-negotiable:** every change must preserve or improve performance.
2. **Security and privacy** must also be preserved or improved, but never by causing a performance regression.
3. **Least code:** remove code when possible; if code must be added, add only the smallest amount needed to satisfy constraints 1 and 2.

## Tech Stack

- **Backend:** Swift 6.2, Hummingbird 2.x, GRDB (SQLite), AsyncHTTPClient
- **Platform:** `Package.swift` enforces macOS 26+; deployment/tooling assume Apple Silicon
- **Services:** Caddy (TLS/routing), tusd (TUS uploads), `KikoMedia` (processing/API)
- **Network:** WireGuard tunnel to VPS, HAProxy TCP passthrough, Cloudflare DNS-only

## Repo Map

- `Package.swift` — targets, dependencies, platform constraints
- `Sources/KikoMedia/` — app runtime, routers, session gating, webhook glue
- `Sources/KikoMediaCore/` — database, media processing, storage, CA scheduling, Thunderbolt offload, shared utilities
- `Sources/KikoMediaExecutable/` — executable entrypoint
- `Sources/Orchestrator/` — setup wizard, dependency install, lifecycle, status dashboard, Thunderbolt setup
- `Sources/Benchmarks/` — benchmark CLI, comparison stages, pipeline sweeps, benchmark-prior maintenance
- `scripts/` — `generate_config_defaults.swift`, `regen-frontend.swift`, `wipe-test-media.swift`, `codex_ca_pair.swift`
- `deploy/` — `defaults.env`, `index.html.template`, `Caddyfile.template`, `worker.swift.template`, generated `index.html`/`Caddyfile`/`worker.swift`, launchd plists, static assets
- `docs/` — `runbook.md`, `architecture.md`, `advanced-config.md`, `security.md`, `benchmark-stages.md`, `ca-system-guide.md`, `architecture-overview.svg`, demo captures under `docs/html/` and `docs/tui/`
- `Tests/KikoMediaTests/` — app/core/orchestrator/benchmark tests

## Runtime Data

All runtime data lives under `BASE_DIRECTORY` (default `~/Documents/kiko-media`):

- `uploads/` — tusd temporary files
- `thumbs/` — default 512px JPEG thumbnails; configurable via env
- `previews/` — default 1440px image previews and H.264 MP4 video previews using `AVAssetExportPreset1920x1080`; configurable via env
- `logs/` — `caddy-access.log`, `caddy-run.log`, `tusd.log`, `kiko-media.log`
- `moderated/` — marker files persisted outside the DB
- `metadata.db` — SQLite metadata database
- `metadata.db-wal` / `metadata.db-shm` — transient SQLite sidecars
- `benchmark-prior.json` — benchmark-generated CA prior artifact used by production scheduling gates

Originals are archived to `EXTERNAL_SSD_PATH`.

## Key Behavioral Truths

- macOS-native media stack only: CoreGraphics, ImageIO, and AVFoundation are part of the runtime design.
- No Docker or containers.
- `kiko-media` is a single process with two HTTP listeners:
  - public: `BIND_ADDRESS:PUBLIC_PORT` (defaults `127.0.0.1:3001`)
  - internal: hardcoded `127.0.0.1:INTERNAL_PORT` (default `3002`)
- Public `/health` and `/api/turnstile/verify` are unauthenticated bootstrap routes. Public gallery/hearts/thumbs/previews require a valid session cookie.
- Internal moderation/gallery/file routes exist only on the internal listener and require `Authorization: <INTERNAL_AUTH_SECRET>`.
- Moderation removes assets from the public gallery and prevents further public heart increments, but direct public `/api/thumbs/{id}` and `/api/preview/{id}` access still works if the ID is known and the session is valid.
- Session gating fails closed at startup: `TURNSTILE_SECRET` and `SESSION_HMAC_SECRET` must be present and non-empty, the HMAC secret must be at least 32 bytes, `TURNSTILE_EXPECTED_HOSTNAME` must resolve non-empty, and action/cdata must resolve non-empty. `TURNSTILE_EXPECTED_ACTION` and `TURNSTILE_EXPECTED_CDATA` have non-empty defaults.
- Originals are archived to the external SSD with SHA256 verification. Recovery/rebuild behavior depends on that SSD being mounted and readable.
- `VIDEO_SCHEDULER_POLICY` is `auto`, `fifo`, or `none`. In `auto`, complexity-aware scheduling only activates when `TB_WORKERS`, a valid base-dir `benchmark-prior.json` v2/local prior, and strict tick-v2 worker checks all pass. Otherwise runtime falls back to FIFO.

## Where To Look

- Deploy/run/troubleshoot: `docs/runbook.md`
- Architecture, routes, data flow: `docs/architecture.md`
- Security/privacy/access control: `docs/security.md`
- Env vars, defaults, ranges: `docs/advanced-config.md`
- Benchmark flow and prior artifact behavior: `docs/benchmark-stages.md`
- CA/Thunderbolt system details: `docs/ca-system-guide.md`
- Internal moderation UI: `http://localhost:8080` on the Mac

## Conventions

- `Config` lives in `Sources/KikoMediaCore/Config.swift`; `Config.load()` lives in `Sources/KikoMedia/AppConfig.swift`.
- `ConfigDefaults.generated.swift` is generated from `deploy/defaults.env`; do not edit it manually.
- App/runtime logging uses `Logger.kiko` with subsystem `com.kiko.media` and category `processing`. Orchestrator and Benchmarks mostly use terminal output helpers instead.
- Deploy templates and launchd plists use `__PLACEHOLDER__` tokens and are rendered by `Sources/Orchestrator/FileGeneration.swift`.
- Local processing defaults are `MAX_CONCURRENT_IMAGES=6` and `MAX_CONCURRENT_VIDEOS=2`, but effective video capacity can expand with Thunderbolt workers.
- `EVENT_TIMEZONE` is optional. Startup freezes the timezone choice for deterministic timestamp formatting.
