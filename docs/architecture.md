# Technical Reference

---

## Your Values

This document uses `__PLACEHOLDER__` tokens for deployment-specific values. Replace them with your own before use.

| Placeholder | Description | Source |
|-------------|-------------|--------|
| `__DOMAIN__` | Public hostname for the event site (e.g. `photos.example.com`) | Your DNS / Cloudflare config |
| `__WG_MAC_IP__` | Host Mac's WireGuard tunnel IP (e.g. `10.0.0.2`, mapped to template placeholder `__WIREGUARD_BIND_IP__`) | WireGuard config on Mac |
| `__WG_VPS_IP__` | VPS's WireGuard tunnel IP (e.g. `10.0.0.1`) | WireGuard config on VPS |
| `__WG_PORT__` | WireGuard listen port on VPS (e.g. `51820`) | WireGuard config on VPS |
| `__REPO_DIR__` | Where you cloned the `kiko-media` repo (e.g. `~/kiko-media`) | Your choice when cloning |
| `__BASE_DIRECTORY__` | Runtime data directory (e.g. `~/Documents/kiko-media`) | Default from `deploy/defaults.env`; configurable in wizard Step 10 |

---

## Design Philosophy

This system follows **Rule 1** — Design for:

- **Maximum Performance** - Parallel processing, long-cache assets, minimal network hops
- **Maximum Security & Privacy** - TLS terminates on-premises, no cloud processing of content
- **Least Code** - Three binaries, no containers, no auth system, single HTML file

---

## 1. Architecture

### Network Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                           INTERNET                              │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  CLOUDFLARE (DNS-only, grey cloud)                              │
│  └─ __DOMAIN__ → VPS public IP                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  VPS (Ubuntu, public IP)                                        │
│  ├─ HAProxy :443 (TCP passthrough, no TLS termination)          │
│  │   └─ Backend: __WG_MAC_IP__:8443                             │
│  └─ WireGuard :__WG_PORT__                                      │
│      └─ Tunnel: __WG_VPS_IP__ ←→ __WG_MAC_IP__                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                      WireGuard Tunnel
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  HOST MAC (macOS 26+, behind NAT)                               │
│  ├─ WireGuard __WG_MAC_IP__                                     │
│  │                                                              │
│  ├─ Caddy (public site)                                         │
│  │   ├─ Binds: __WG_MAC_IP__:8443                               │
│  │   ├─ TLS: Let's Encrypt via Cloudflare DNS-01                │
│  │   ├─ /files/* ──────────────────► tusd :1080                 │
│  │   ├─ /api/turnstile/verify ─────► kiko-media :PUBLIC_PORT    │
│  │   ├─ /api/gallery ──────────────► kiko-media :PUBLIC_PORT    │
│  │   ├─ /api/heart-counts ─────────► kiko-media :PUBLIC_PORT    │
│  │   ├─ /api/assets/*/heart ───────► kiko-media :PUBLIC_PORT    │
│  │   ├─ /api/thumbs/* ─────────────► kiko-media :PUBLIC_PORT    │
│  │   ├─ /api/preview/* ────────────► kiko-media :PUBLIC_PORT    │
│  │   └─ /, /index.html, static ────► deploy/ files              │
│  │                                                              │
│  ├─ Caddy (internal site)                                       │
│  │   ├─ Binds: 127.0.0.1:8080                                   │
│  │   ├─ /api/gallery ──────────────► kiko-media :INTERNAL_PORT  │
│  │   ├─ /api/assets/* ─────────────► kiko-media :INTERNAL_PORT  │
│  │   ├─ /api/thumbs/* ─────────────► kiko-media :INTERNAL_PORT  │
│  │   ├─ /api/preview/* ────────────► kiko-media :INTERNAL_PORT  │
│  │   └─ /, /index.html, static ────► deploy/ files              │
│  │                                                              │
│  ├─ tusd :1080 (binds 127.0.0.1)                                │
│  │   ├─ TUS protocol resumable uploads                          │
│  │   ├─ Max file: TUSD_MAX_SIZE (default 2GB)                   │
│  │   ├─ Storage: __BASE_DIRECTORY__/uploads/                    │
│  │   └─ Webhook: POST http://127.0.0.1:INTERNAL_PORT/hooks/upload-complete│
│  │                                                              │
│  └─ kiko-media (Swift/Hummingbird, single process)              │
│      ├─ Public listener :PUBLIC_PORT (default 3001)             │
│      │   ├─ GET /health                                         │
│      │   ├─ POST /api/turnstile/verify                          │
│      │   ├─ GET /api/gallery                                    │
│      │   ├─ POST /api/assets/{id}/heart                         │
│      │   ├─ POST /api/heart-counts                              │
│      │   ├─ GET /api/thumbs/{id}                                │
│      │   └─ GET /api/preview/{id}                               │
│      ├─ Internal listener :INTERNAL_PORT (default 3002)         │
│      │   ├─ POST /hooks/upload-complete                         │
│      │   ├─ GET /api/gallery (includes status)                  │
│      │   ├─ PATCH /api/assets/{id}                              │
│      │   ├─ GET /api/thumbs/{id}                                │
│      │   └─ GET /api/preview/{id}                               │
│      └─ Shared: Database, MediaProcessor (one instance each)    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  EXTERNAL SSD (EXTERNAL_SSD_PATH, e.g. /Volumes/MySSD/originals)│
│  └─ {id}[.{ext}] (SHA256-verified archive; extension only when original name has one) │
└─────────────────────────────────────────────────────────────────┘
```

### Why This Architecture

| Decision | Rationale (Rule 1) |
|----------|-------------------|
| TLS on the host Mac | VPS never sees plaintext content (security) |
| HAProxy TCP mode | Zero processing on VPS, just passthrough (performance) |
| WireGuard tunnel | NAT traversal without exposing Mac to internet (security) |
| Single process, two listeners | Hard route separation; no moderation routes on public listener (security) |
| No user accounts | Least code; Turnstile session gating replaces login, single-day event (acceptable tradeoff) |
| Single HTML file | Same UI for public/internal; behavior differs by hostname (least code) |

---

## 2. Components

### 2.1 kiko-media (Swift)

**Source:** `Sources/KikoMediaExecutable/` + `Sources/KikoMedia/` + `Sources/KikoMediaCore/`
**Binary:** `.build/release/KikoMedia`
**Platform:** macOS 26+
**Dependencies:** Hummingbird 2.x, GRDB 6.x, AsyncHTTPClient 1.x (see Package.swift for minimum versions)

#### Source Files

| File | Purpose |
|------|---------|
| `Sources/KikoMediaExecutable/main.swift` | Executable entry (`@main`) forwarding to `KikoMediaAppRuntime.run()` |
| `KikoMediaEntry.swift` | Runtime startup wiring, two HTTP listeners, watchdog, graceful shutdown |
| `Database.swift` | SQLite with GRDB, asset queries, status updates |
| `MediaProcessor.swift` | Async job queue, processing pipeline, crash recovery |
| `FileServer.swift` | Serve thumbs/previews with range requests |
| `WebhookHandler.swift` | Handle tusd upload-complete webhook, pre-create validation |
| `ImageProcessor.swift` | Thumbnail/preview generation via CoreGraphics |
| `VideoProcessor.swift` | Thumbnail/transcode via AVFoundation |
| `StorageManager.swift` | Archive to SSD with SHA256 verification |
| `ThunderboltDispatcher.swift` | Optional remote video dispatcher actor (worker slots, fallback orchestration) |
| `ThunderboltCapabilities.swift` | Shared sweep ceiling formula for burst sweep search space capping |
| `ThunderboltTransport.swift` | BSD transport primitives for offload protocol (connect/send/receive/source-bind/capability query) |
| `ImageUtils.swift` | JPEG encoding utility |
| `Logger.swift` | OSLog configuration |
| `DateUtils.swift` | EXIF timestamp formatting with frozen event timezone |
| `VolumeUtils.swift` | External volume mount detection (SSD availability) |
| `SessionCookie.swift` | Stateless HMAC-signed session cookies for Turnstile gating |
| `TurnstileVerifier.swift` | Cloudflare Turnstile siteverify integration (actor) |
| `ModerationMarkers.swift` | Persist moderation state outside DB for recovery |
| `HeartRevisionTracker.swift` | Monotonic heart-revision counter for gallery polling |
| `RouterBuilders.swift` | Shared HTTP route wiring (used by production + tests) |

#### Configuration

All operational values are configurable via environment variables with sensible defaults. `Config.load()` in `Sources/KikoMedia/AppConfig.swift` reads env vars at startup: numeric values are range-validated (invalid/out-of-range values fall back to defaults with warnings), string values fall back when unset/empty, `VIDEO_TRANSCODE_PRESET` is validated against `AVAssetExportSession.allExportPresets()`, and `VIDEO_SCHEDULER_POLICY` is validated against `auto|fifo|none`. Unsupported values fail startup.

The setup wizard (`swift run orchestrator`) writes all 30+ env vars into the launchd plist. For the full list of env vars, types, defaults, and valid ranges, see [`docs/advanced-config.md`](advanced-config.md). The orchestrator command also provides operational shortcuts: `swift run orchestrator --status|--tb-status|--start|--shutdown|--restart|--thunderbolt`. Frontend artifact refresh is exposed separately via `swift scripts/regen-frontend.swift`. Test-media cleanup is exposed separately via `swift scripts/wipe-test-media.swift` (interactive confirmation, no restart).

Key env vars for basic setup:
- `TURNSTILE_SECRET` / `SESSION_HMAC_SECRET` / `TURNSTILE_EXPECTED_HOSTNAME` / `TURNSTILE_EXPECTED_ACTION` / `TURNSTILE_EXPECTED_CDATA` -- all required for Turnstile session gating (production, fail-closed)
- `TURNSTILE_EXPECTED_ACTION` / `TURNSTILE_EXPECTED_CDATA` defaults: `kiko_verify` / `kiko_public` (configurable)
- `EXTERNAL_SSD_PATH` -- archive path (no default; set by setup wizard, e.g. `/Volumes/YourSSD/originals`)
- `EVENT_TIMEZONE` -- IANA timezone for EXIF timestamps (default: system timezone)
- `BASE_DIRECTORY` -- root for all runtime data (default `~/Documents/kiko-media`)
- `VIDEO_SCHEDULER_POLICY` -- production video scheduling policy: `auto`, `fifo`, or `none`
- `TB_WORKERS` / `TB_PORT` / `TB_CONNECT_TIMEOUT` -- optional Thunderbolt worker list (`host:slots,...`), worker TCP port, and connect timeout (milliseconds)
- Parsed-valid `TB_WORKERS` provides worker inventory and enables Thunderbolt offload when `VIDEO_SCHEDULER_POLICY` allows it. Under `VIDEO_SCHEDULER_POLICY=auto`, CA scheduling becomes active only when the benchmark prior artifact, the local prior profile, and strict tick v2 acceptance also pass; otherwise runtime falls back to FIFO dequeue.

**Turnstile:** Startup is fail-closed. `TURNSTILE_SECRET`, `SESSION_HMAC_SECRET`, `TURNSTILE_EXPECTED_HOSTNAME`, `TURNSTILE_EXPECTED_ACTION`, and `TURNSTILE_EXPECTED_CDATA` must all be non-empty, and `SESSION_HMAC_SECRET` must be at least 32 bytes (UTF-8). If any check fails, the app refuses to start. During verification, `TurnstileVerifier` requires `success == true`, `hostname == TURNSTILE_EXPECTED_HOSTNAME`, `action == TURNSTILE_EXPECTED_ACTION`, and `cdata == TURNSTILE_EXPECTED_CDATA`.
`TurnstileVerifier` uses an internal AsyncHTTPClient and performs explicit async shutdown during app teardown (after `ServiceGroup.run()` returns), rather than relying on actor `deinit`.
`MediaProcessor` also performs explicit app-lifecycle shutdown: in-flight processing tasks are tracked, cancelled on shutdown, and awaited before process exit.

**Event timezone:** `EVENT_TIMEZONE` is an optional IANA timezone identifier (e.g. `America/New_York`). When set, all EXIF timestamp formatting uses this timezone instead of the system timezone. This matters when the server runs in a different timezone to the event. The timezone is frozen at startup when `DateUtils.configure(eventTimezone:)` is called from `KikoMediaAppRuntime.run()` in `Sources/KikoMedia/KikoMediaEntry.swift`, preventing mid-run drift from system settings changes. If empty or unset, the system timezone is used. If set to an invalid value, a warning is logged and the system timezone is used as fallback.

### 2.2 Caddy

**Binary:** `~/bin/caddy`
**Config:** `deploy/Caddyfile` (generated from `deploy/Caddyfile.template` by `swift run orchestrator`)

Two site blocks:

1. **Public site** (`__DOMAIN__`)
   - Binds to `__WG_MAC_IP__:8443` (WireGuard interface only)
   - TLS via Cloudflare DNS-01 challenge
   - Routes uploads to tusd (HTTP/1.1 forced), API to `kiko-media` `PUBLIC_PORT` (default `3001`)
   - Does NOT route `/health` or `/hooks/*` (loopback-only on the app)

2. **Internal site** (`localhost:8080`, `127.0.0.1:8080`)
   - Binds to `127.0.0.1` only (loopback)
   - HTTP only (no TLS needed for local access)
   - Routes API to `kiko-media` `INTERNAL_PORT` (default `3002`)
   - No `/files/*` route (uploads disabled)

### 2.3 tusd

**Binary:** `~/bin/tusd`
**Protocol:** TUS 1.0 (resumable uploads)

| Flag | Value | Purpose |
|------|-------|---------|
| `-upload-dir` | `__BASE_DIRECTORY__/uploads` | Chunk and completed file storage |
| `-max-size` | `TUSD_MAX_SIZE` (default 2GB) | Max file size in bytes (configurable via `TUSD_MAX_SIZE` env var) |
| `-base-path` | `/files/` | URL path (must match Caddy routing) |
| `-behind-proxy` | (flag) | Trust X-Forwarded-* headers from Caddy |
| `-hooks-http` | `http://127.0.0.1:INTERNAL_PORT/hooks/upload-complete` (default 3002) | Webhook endpoint (internal listener) |
| `-hooks-enabled-events` | `pre-create,post-finish` | Fire pre-create + upload-complete events |
| `-hooks-http-forward-headers` | `Cookie` | Forward cookie header to hook endpoint |
| `-disable-download` | (flag) | Disable download endpoints (uploads only) |
| `-host` | `127.0.0.1` | Listen only on localhost |
| `-port` | `1080` | Listen port |
| `-verbose=false` | — | Reduce log noise (tusd defaults to verbose) |

**Storage:** tusd creates two files per upload:
- `{id}` - The actual file data (no extension)
- `{id}.info` - JSON metadata (filename, size, offset, etc.)

After a successful archive + completion, kiko-media deletes the upload file and `.info`.
If processing fails or the SSD is unavailable, files are preserved for retry/manual review.

### 2.4 Frontend

**File:** `deploy/index.html` (generated from `deploy/index.html.template` by `swift run orchestrator` or refreshed from existing persisted config by `swift scripts/regen-frontend.swift`)
**Libraries:** tus-js-client (self-hosted), Cloudflare Turnstile (external)

Single HTML file served on both hosts. Behavior differs by hostname:

```javascript
const isInternal = ['localhost', '127.0.0.1'].includes(window.location.hostname);
```

| Feature | Public Host | Internal Host |
|---------|-------------|---------------|
| Turnstile verification | Session cookie required; frontend runs Turnstile after 401/403 if needed | Skipped |
| Upload button | Visible (after verification) | Hidden |
| File input | Enabled (after verification) | Disabled |
| Moderation controls | Hidden | Visible |
| Gallery status field | Not used | Used (shows hidden badge) |

#### Turnstile Verification Flow (Public Host)

1. Page loads with modal hidden; Turnstile widget pre-renders in background
2. `fetchAssets()` runs immediately via `fetchWithReauth`
3. If session cookie is valid: gallery loads, modal never shown
4. If 401: `fetchWithReauth` triggers `rerunVerification()` → modal appears with Turnstile widget
5. Turnstile challenge runs (usually invisible, `appearance: "interaction-only"`, `execution: "render"`)
6. On success, client enters `ready` (when gate input is required) or POSTs token to `/api/turnstile/verify`
7. Server validates via Cloudflare siteverify and issues `<SESSION_COOKIE_NAME>` (default `kiko_session`)
8. Modal auto-hides via `_pendingReauth` callback; original fetch retries and gallery loads
9. If session expires later:
   - Foreground API calls (via `fetchWithReauth`) re-run Turnstile automatically
   - Background pollers pause and show the "Session expired — tap to continue" prompt

**Modal states:** `verifying` (spinner) → `interaction` (Turnstile checkbox, rare) → `ready` (gate input when required) → `verified` (buttons) or `failed` (retry). On refresh with valid cookie, the modal is never shown.

---

## 3. Data Model

### SQLite Schema

**File:** `__BASE_DIRECTORY__/metadata.db`
**Mode:** WAL, synchronous=NORMAL

```sql
CREATE TABLE assets (
    id          TEXT PRIMARY KEY,    -- tusd upload ID
    type        TEXT NOT NULL,       -- 'image' | 'video'
    timestamp   TEXT NOT NULL,       -- 'YYYY:MM:DD HH:MM:SS' (EXIF format)
    originalName TEXT NOT NULL,      -- Original filename from upload
    status      TEXT NOT NULL DEFAULT 'queued',
    createdAt   DATETIME NOT NULL,
    completedAt DATETIME,            -- NULL until processing completes
    retryCount  INTEGER NOT NULL DEFAULT 0,
    heartCount  INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_assets_status_timestamp ON assets (status, timestamp);
```

### Asset Status Values

| Status | Meaning |
|--------|---------|
| `queued` | Inserted, waiting for processing |
| `processing` | Currently being processed |
| `complete` | Processing done, visible in public gallery |
| `moderated` | Hidden from public gallery (soft delete) |
| `failed` | Processing failed, needs manual review |

### File System Layout

```
~/bin/                              # Binaries
├── caddy                           # Caddy with Cloudflare DNS plugin
└── tusd                            # TUS resumable upload server

__REPO_DIR__/                       # Source repository
├── Package.swift                   # Swift package definition
├── Sources/KikoMediaExecutable/
│   └── main.swift                  # @main entry; calls KikoMediaAppRuntime.run()
├── Sources/KikoMedia/
│   ├── AppConfig.swift             # Runtime config loading + environment parsing
│   ├── AppModels.swift             # API request/response payload models
│   ├── FileServer.swift            # Serve thumbs/previews with range requests
│   ├── KikoMediaEntry.swift        # Runtime wiring, two listeners, watchdog, graceful shutdown
│   ├── KikoMediaUtilities.swift    # Shared app-level helper utilities
│   ├── RequestDecoding.swift       # Bounded JSON body decode helpers
│   ├── RouterBuilders.swift        # Shared HTTP route wiring (prod + tests)
│   ├── SessionCookie.swift         # Stateless HMAC-signed session cookies
│   ├── HeartRevisionTracker.swift  # Monotonic heart-revision counter for gallery polling
│   ├── SessionGating.swift         # Fail-closed Turnstile/session setup checks
│   ├── TurnstileVerifier.swift     # Cloudflare Turnstile siteverify integration
│   └── WebhookHandler.swift        # Handle tusd upload-complete webhook, pre-create validation
├── Sources/KikoMediaCore/
│   ├── Config.swift                # Environment defaults + range validation
│   ├── Database.swift              # SQLite with GRDB, asset queries
│   ├── MediaProcessor.swift        # Async job queue, processing pipeline
│   ├── ThunderboltCapabilities.swift # Shared sweep ceiling formula
│   ├── ThunderboltDispatcher.swift # Remote video worker dispatcher actor
│   ├── ThunderboltTransport.swift  # Offload transport primitives + capability query
│   ├── ImageProcessor.swift        # Thumbnail/preview via CoreGraphics
│   ├── VideoProcessor.swift        # Thumbnail/transcode via AVFoundation
│   ├── StorageManager.swift        # Archive to SSD with SHA256 verification
│   ├── SHA256Utility.swift         # Shared file hashing utility
│   ├── ModerationMarkers.swift     # Moderation marker files (persisted outside DB)
│   ├── DateUtils.swift             # EXIF timestamp formatting, frozen timezone
│   ├── VolumeUtils.swift           # External volume mount detection
│   ├── Logger.swift                # OSLog configuration
│   ├── ImageUtils.swift            # JPEG encoding utility
│   ├── RequestBodyLimits.swift     # API request body bounds
│   └── TusdUpload.swift            # tusd webhook payload decode
├── Tests/
│   └── KikoMediaTests/             # Swift Testing + HummingbirdTesting suites
├── Sources/Orchestrator/
│   ├── main.swift                  # Orchestrator entry point
│   ├── CLI.swift                   # CLI parse/help and command dispatch
│   ├── Wizard.swift                # Interactive setup wizard flow
│   └── Thunderbolt.swift           # Thunderbolt setup + delegation
├── deploy/
│   ├── defaults.env                # Single source of truth for config defaults
│   ├── Caddyfile.template          # Caddy configuration template
│   ├── index.html.template         # Single-page gallery UI template
│   ├── tus.min.js                  # Self-hosted tus-js-client library
│   └── launchd/                    # launchd plist templates
│       ├── com.kiko.caddy.plist
│       ├── com.kiko.tusd.plist
│       └── com.kiko.media.plist
├── docs/
│   ├── architecture.md             # This file
│   ├── runbook.md                  # Operational procedures
│   ├── advanced-config.md          # All env vars with defaults and ranges
│   ├── security.md                 # Security and privacy model
│   └── benchmark-stages.md         # Benchmark methodology and stages
└── .build/release/KikoMedia        # Compiled binary

__BASE_DIRECTORY__/             # Runtime data
├── uploads/                        # Temporary: tusd uploads
│   ├── {upload-id}                 # Raw file (no extension)
│   └── {upload-id}.info            # tusd metadata JSON
├── thumbs/                         # Permanent: thumbnails (`THUMBNAIL_SIZE`, default 512px)
│   └── {id}.jpg
├── previews/                       # Permanent: web-optimized
│   ├── {id}.jpg                    # Images: max dimension from `PREVIEW_SIZE` (default 1440px)
│   └── {id}.mp4                    # Videos: MP4; dimensions depend on `VIDEO_TRANSCODE_PRESET` (default `AVAssetExportPreset1920x1080`)
├── moderated/                      # Moderation state (survives DB rebuild)
│   └── {id}                        # Empty marker file per moderated asset
├── logs/
│   ├── caddy-access.log
│   ├── caddy-run.log
│   ├── tusd.log
│   └── kiko-media.log
└── metadata.db                     # SQLite + WAL files

EXTERNAL_SSD_PATH/                  # External SSD archive (e.g. /Volumes/MySSD/originals)
└── {id}[.{ext}]                    # SHA256-verified archive copies
```

### Logging

- **kiko-media:** Uses OSLog with subsystem `com.kiko.media` (see `Logger.swift`). Also captured to `__BASE_DIRECTORY__/logs/kiko-media.log` via launchd StandardOutPath/StandardErrorPath.
- **tusd:** Writes to `__BASE_DIRECTORY__/logs/tusd.log` via launchd StandardOutPath/StandardErrorPath.
- **Caddy:** HTTP access logs write to `__BASE_DIRECTORY__/logs/caddy-access.log`. Caddy process stdout/stderr is captured separately by launchd to `__BASE_DIRECTORY__/logs/caddy-run.log`.

---

## 4. API Reference

### Public Listener (:PUBLIC_PORT, default 3001)

**Binding:** Configurable via `BIND_ADDRESS` (default `127.0.0.1`). External exposure happens only through Caddy routes.
**Important:** Caddy does **not** route `/health` or `/hooks/*`; those are loopback-only.
**Route classes:** `GET /health` and `POST /api/turnstile/verify` are open on the public listener. `GET /api/gallery`, `POST /api/assets/{id}/heart`, `POST /api/heart-counts`, `GET /api/thumbs/{id}`, and `GET /api/preview/{id}` are session-gated.

**Session Cookie Gating:** In production, Turnstile/session gating is fail-closed at startup, so public gallery/heart/thumb/preview routes require a valid session cookie (default name: `kiko_session`, configurable via `SESSION_COOKIE_NAME`). Requests without a valid cookie receive `401 Unauthorized`.

#### GET /health
Health check endpoint. Not gated. Queries the database (`getTotalAssetCount()`) to verify DB connectivity before returning OK; throws 500 if the DB is unreachable.

**Response:** `200 OK`
```json
{"status":"ok"}
```

#### POST /api/turnstile/verify
Exchange Turnstile token for a session cookie.

**Request body:**
```json
{"token": "0.ABC123..."}
```

When `GATE_SECRET` is enabled, the frontend adds `"gateSecret": "..."` to the same request body. Older clients may still send `"password"` or `"inviteToken"`; both remain accepted for compatibility.

**Responses:**
- `204 No Content` + `Set-Cookie: <SESSION_COOKIE_NAME>=...` (default `kiko_session`) — Success
- `400 Bad Request` — Missing or invalid request body
- `413 Content Too Large` — Request body exceeds `JSON_MAX_BODY_BYTES`
- `403 Forbidden` — Turnstile verification failed
- `503 Service Unavailable` + `Retry-After` — Turnstile verification temporarily unavailable (overload/transient failures); client should retry the same token

**Behavior:**
1. Decode token from request body
2. POST to Cloudflare siteverify with secret + token
3. Require `success == true`, `hostname == TURNSTILE_EXPECTED_HOSTNAME`, `action == TURNSTILE_EXPECTED_ACTION`, and `cdata == TURNSTILE_EXPECTED_CDATA`
4. If `GATE_SECRET` is configured, require a matching gate proof (`gateSecret`, or legacy `password` / `inviteToken` aliases)
5. On success, create HMAC-signed session cookie (TTL from `SESSION_COOKIE_TTL`, default 4 hours)
6. Return cookie in `Set-Cookie` header

#### GET /api/gallery
List visible assets (status='complete' only). **Requires a valid session cookie in production (fail-closed Turnstile/session gating).**

**Query params:**
- `limit` (optional, default 100, max 500)
- `offset` (optional, default 0)
- `sort=hearts` (optional; sorts by `heartCount` descending, then timestamp descending)

**Response:** `200 OK` or `401 Unauthorized`
```json
{
  "assets": [
    {"id": "abc123", "type": "image", "heartCount": 12},
    {"id": "def456", "type": "video", "heartCount": 3}
  ],
  "total": 42,
  "heartRevision": 7
}
```

`heartRevision` is a monotonically increasing integer (via `HeartRevisionTracker`) that bumps on each heart. Clients use it to detect when heart counts have changed since their last poll.

#### POST /api/assets/{id}/heart
Increment the public heart count for a visible asset. Only increments assets with `status=complete`. **Requires a valid session cookie in production (fail-closed Turnstile/session gating).**

**Response:** `200 OK`, `400 Bad Request`, `401 Unauthorized`, or `404 Not Found`
```json
{"heartCount": 13}
```

#### POST /api/heart-counts
Batch-fetch heart counts for multiple assets. **Requires a valid session cookie in production (fail-closed Turnstile/session gating).**

**Request body:**
```json
{"ids": ["abc123", "def456"]}
```

**Validation:**
- Max 500 IDs per request (returns `400 Bad Request` if exceeded)
- Each ID validated (`1...128` UTF-8 bytes + traversal/control checks)
- Duplicates are deduplicated before query
- Only returns counts for assets with `status=complete` or `status=moderated`

**Response:** `200 OK`, `400 Bad Request`, or `401 Unauthorized`
```json
{"heartCounts": {"abc123": 12, "def456": 3}}
```

#### GET /api/thumbs/{id}
Serve thumbnail image. **Requires a valid session cookie in production (fail-closed Turnstile/session gating).**

**Response:** `200 OK`, `206 Partial Content`, `400 Bad Request`, `401 Unauthorized`, `404 Not Found`, `410 Gone`, or `416 Range Not Satisfiable`
- Content-Type: image/jpeg
- Cache-Control: from `CACHE_CONTROL` (default `public, max-age=31536000, immutable`)
- Supports Range requests
- Returns `410 Gone` if the asset has `status=failed` (terminal processing failure)

#### GET /api/preview/{id}
Serve preview image or video. **Requires a valid session cookie in production (fail-closed Turnstile/session gating).**

**Response:** `200 OK`, `206 Partial Content`, `400 Bad Request`, `401 Unauthorized`, `404 Not Found`, `410 Gone`, or `416 Range Not Satisfiable`
- Content-Type: image/jpeg or video/mp4 (based on asset type in DB)
- Cache-Control: from `CACHE_CONTROL` (default `public, max-age=31536000, immutable`)
- Supports Range requests
- Returns `410 Gone` if the asset has `status=failed` (terminal processing failure)

---

### Internal Listener (:INTERNAL_PORT, default 3002)

**Route classes:** `POST /hooks/upload-complete` is an internal-only tusd webhook. `GET /api/gallery`, `PATCH /api/assets/{id}`, `GET /api/thumbs/{id}`, and `GET /api/preview/{id}` require `Authorization: <INTERNAL_AUTH_SECRET>` on the internal listener.

#### POST /hooks/upload-complete
Webhook from tusd (pre-create + post-finish). Internal-only endpoint; called directly by tusd (not routed by Caddy).

**Request body:** TusdHookRequest
```json
{
  "Type": "post-finish",
  "Event": {
    "Upload": {
      "ID": "abc123...",
      "Size": 12345678,
      "Offset": 12345678,
      "MetaData": {
        "filename": "IMG_1234.HEIC",
        "filetype": "image/heic"
      },
      "Storage": {
        "Type": "filestore",
        "Path": "/Users/<username>/Documents/kiko-media/uploads/abc123..."
      }
    }
  }
}
```

Pre-create uses `Type: "pre-create"` and does not include an upload ID or storage path yet.

**Response:** `200 OK`, `400 Bad Request`, `401 Unauthorized`, `413 Content Too Large`, or `503 Service Unavailable` (with `Retry-After`)

**Behavior (pre-create):**
1. Validate session cookie from forwarded headers (production fail-closed)
2. Reject with 401 if cookie is missing or invalid
3. Return 200 to allow upload to proceed

**Behavior (post-finish):**
1. Enforce bounded JSON body read using `JSON_MAX_BODY_BYTES` (oversized request returns 413)
2. Validate completion invariants (`SizeIsDeferred != true`, `Size > 0`, `Offset >= 0`, `Offset == Size`)
3. Validate path within upload directory (path traversal protection)
4. Verify file exists
5. Check queue admission (`MAX_PENDING_WEBHOOK_JOBS`, shutdown state)
6. If admission is closed:
   - existing asset ID -> return 200 (idempotent duplicate)
   - new asset ID -> return `503 Service Unavailable` with `Retry-After: WEBHOOK_RETRY_AFTER_SECONDS` (no DB insert)
7. Detect asset type once (`ImageProcessor.isImage`) and INSERT INTO assets with status='queued' (`ON CONFLICT IGNORE` for idempotency)
8. If inserted, enqueue for processing using the same detected type (no second type probe)
9. If enqueue later rejects (race), roll back queued insert and return `503 Service Unavailable` with `Retry-After: WEBHOOK_RETRY_AFTER_SECONDS`

#### GET /api/gallery
List moderatable assets (status='complete' OR 'moderated').

**Query params:** Same as public except `sort=hearts` is not supported (internal gallery sorts by timestamp descending only)

**Response:** `200 OK`
```json
{
  "assets": [
    {"id": "abc123", "type": "image", "status": "complete", "heartCount": 12},
    {"id": "def456", "type": "video", "status": "moderated", "heartCount": 3}
  ],
  "total": 42,
  "heartRevision": null
}
```

Note: `status` and `heartCount` fields are included. `heartRevision` is always `null` for the internal gallery. `total` counts only complete+moderated.

#### PATCH /api/assets/{id}
Toggle asset visibility.

**Request body:**
```json
{"status": "complete"}
```
or
```json
{"status": "moderated"}
```

**Responses:**
- `204 No Content` - Success
- `400 Bad Request` - Invalid status or asset not in complete/moderated state
- `404 Not Found` - Asset doesn't exist
- `413 Content Too Large` - Request body exceeds `JSON_MAX_BODY_BYTES`
- `500 Internal Server Error` - Marker/DB update failure (state rolled back)

**Behavior:**
- Idempotent (setting same status is OK)
- Only allows transitions between `complete` and `moderated`
- Assets in queued/processing/failed cannot be moderated
- Uses bounded JSON body parsing (`JSON_MAX_BODY_BYTES`, oversized body rejected with 413)
- Marker-file and DB updates are rollback-safe; failures return 500 without partial state

#### GET /api/thumbs/{id}, GET /api/preview/{id}
Same as public listener (same headers/range support), but protected by internal shared-secret authorization instead of session cookie gating.

---

## 5. Data Flows

### Flow A: Turnstile Verification (Public Host)

```
Browser                    Caddy                kiko-media              Cloudflare
   │                         │                      │                      │
   │ (page load, modal hidden, fetchAssets() runs)  │                      │
   │                         │                      │                      │
   ├─[Turnstile widget loads and runs challenge in background]────────────►│
   │                         │                      │                      │
   │◄─────────────────────[Token]──────────────────────────────────────────┤
   │                         │                      │                      │
   ├─POST /api/turnstile/verify──────────────────►│                        │
   │  Body: {"token":"..."}  │                      │                      │
   │                         │                      ├─POST siteverify─────►│
   │                         │                      │  secret + token      │
   │                         │                      │◄──{"success":true}───┤
   │                         │                      │                      │
   │◄─204 No Content─────────┤◄─────────────────────┤                      │
   │  Set-Cookie: <SESSION_COOKIE_NAME>=...         │                      │
   │                         │                      │                      │
   │ (modal auto-hides, pending retry loads gallery)│                      │
```

**Session cookie:** HMAC-signed, stateless, HttpOnly, Secure, SameSite=Lax cookie (`SESSION_COOKIE_NAME`, default `kiko_session`) with TTL from `SESSION_COOKIE_TTL` (default 4 hours). On 401/403 from gated foreground API requests, frontend re-runs Turnstile automatically; background pollers instead pause and wait for a user-initiated verify action.

### Flow B: Upload (with Pre-create Validation)

```
Browser                    Caddy                tusd                kiko-media
   │                         │                    │                      │
   ├─POST /files/───────────►├───────────────────►│                      │
   │  Headers:               │                    │                      │
   │  Tus-Resumable: 1.0.0   │                    ├─POST pre-create─────►│
   │  Upload-Length: N       │                    │  (Cookie forwarded)  │
   │  Upload-Metadata: ...   │                    │                      │
   │  Cookie: <SESSION_COOKIE_NAME> (default `kiko_session`) │◄─200 OK (or 401)─────┤
   │                         │                    │                      │
   │                         │                    ├─Creates upload───────┤
   │                         │                    │  {id} in uploads/    │
   │◄─Location: /files/{id}──┤◄───────────────────┤                      │
   │                         │                    │                      │
   ├─PATCH /files/{id}──────►├───────────────────►│                      │
   │  Upload-Offset: 0       │                    ├─Writes chunk─────────┤
   │  Body: [chunk data]     │                    │                      │
   │                         │                    │                      │
   │ (repeat for each chunk) │                    │                      │
   │                         │                    │                      │
   │                         │                    ├─POST post-finish─────►
   │                         │                    │                      │
   │                         │                    │                      ├──┐
   │                         │                    │                      │  │ Insert DB
   │                         │                    │                      │  │ Enqueue job
   │                         │                    │◄─────────200 OK──────┤◄─┘
```

### Flow C: Processing

```
MediaProcessor (actor, image queue default 6 / local video queue default 2 + optional remote slots)
   │
   ├─Dequeue job
   ├─Track in-flight task handle (for shutdown cancel/join)
   ├─UPDATE status = 'processing'
   ├─Persist queued media type to DB (`UPDATE type = ?`)
   │   └─If DB type update fails: log + abort job (avoid stale type drift)
   │
   ├─structured async let child tasks (parallel):
   │   ├─Generate thumbnail (`THUMBNAIL_SIZE`, default 512px)
   │   ├─Generate preview (images: `PREVIEW_SIZE`, default 1440px; videos: preset-driven, default `AVAssetExportPreset1920x1080`)
   │   └─Extract timestamp (EXIF or video metadata)
   │   (No `Task.detached` wrapper for these branches)
   │
   ├─Archive to SSD:
   │   ├─Calculate SHA256 of upload
   │   ├─Copy to $EXTERNAL_SSD_PATH/{id}[.{ext}]
   │   ├─Calculate SHA256 of copy
   │   └─Verify match
   │
   ├─UPDATE status = 'complete', timestamp = ?
   └─Delete upload file + .info
```

During app shutdown (`SIGTERM`/`SIGINT`), `MediaProcessor.shutdown()`:
1. Stops scheduling new work
2. Cancels in-flight processing tasks
3. Awaits task completion before process exit
4. Leaves unfinished uploads for startup recovery (`recoverIncomplete()`)

### Flow D: Gallery View (Session Gated)

```
Browser                    Caddy                kiko-media
   │                         │                      │
   ├─GET /api/gallery───────►├─────────────────────►│
   │  Cookie: <SESSION_COOKIE_NAME> (default `kiko_session`) │                      ├─Validate cookie
   │                         │                      │  (returns 401 if invalid)
   │                         │                      ├─SELECT * FROM assets
   │                         │                      │  WHERE status='complete'
   │                         │                      │  ORDER BY timestamp DESC
   │◄─────────JSON──────────┤◄─────────────────────┤
   │                         │                      │
   │ (render grid)           │                      │
   │                         │                      │
   ├─GET /api/thumbs/{id}───►├─────────────────────►│
   │  Cookie: <SESSION_COOKIE_NAME> (default `kiko_session`) │                      ├─Validate cookie
   │◄─────────JPEG──────────┤◄─────────────────────┤
   │                         │   Cache-Control:     │
   │ (click thumbnail)       │   from CACHE_CONTROL │
   │                         │                      │
   ├─GET /api/preview/{id}──►├─────────────────────►│
   │  Cookie: <SESSION_COOKIE_NAME> (default `kiko_session`) │                      ├─Validate cookie
   │◄───────JPEG/MP4────────┤◄─────────────────────┤
```

Note: Production startup is fail-closed on Turnstile/session configuration. Missing or invalid Turnstile/session secrets prevent the service from starting.

### Flow E: Crash Recovery

On startup, `MediaProcessor.recoverIncomplete()`:

```
1. List uploads/ directory (exclude hidden, exclude .info)
2. Query DB for matching IDs
3. For each upload file:
   ├─ IN DB + status=complete|moderated → Delete upload (cleanup)
   ├─ IN DB + status=queued|processing  → Re-queue (interrupted)
   ├─ IN DB + status=failed             → Skip (manual review)
   └─ NOT IN DB                         → Insert + queue only when valid `.info` metadata is present
                                          (non-deferred size, size matches file, metadata decode succeeds)
4. Start processing queue
```

### Flow F: Database Recovery

On startup, before any other recovery:

```
1. Check if metadata.db exists
   └─ NO → needsRebuild = true

2. Try Database(path:)
   └─ THROWS → Move aside corrupt files (timestamped), needsRebuild = true

3. Run PRAGMA quick_check
   └─ FAILS → Move aside corrupt files, needsRebuild = true

4. Compare SSD file count vs DB total count
   └─ SSD > DB → needsRebuild = true

5. If needsRebuild AND SSD mounted:
   ├─ Load moderated IDs from __BASE_DIRECTORY__/moderated/
   │   └─ If marker directory unreadable → abort rebuild (fail closed)
   ├─ Set rebuild worker cap = MAX_CONCURRENT_REBUILD_PROBES
   ├─ For each file on SSD (bounded task-group parallelism):
   │   ├─ Extract ID, type, timestamp
   │   ├─ Check if ID in moderated set
   │   └─ Insert with status = moderated or complete
   └─ Log: "Rebuild complete: X inserted, Y skipped, Z failed"

6. If needsRebuild AND SSD NOT mounted:
   └─ exit(1) with error log (hard exit; launchd restarts process)
```

**Runtime watchdog** (every `HEALTH_CHECK_INTERVAL` seconds, default 60):
```
1. Call getTotalAssetCount()
2. If throws → log error and trigger graceful shutdown
3. Process exits with error; launchd restarts process → recovery runs
```

**Moderation persistence:**
- On PATCH /api/assets/{id} with status=moderated: create `moderated/{id}` marker file
- On PATCH /api/assets/{id} with status=complete: delete `moderated/{id}` marker file
- On rebuild: read marker files, apply moderation to matching IDs; if markers are unreadable, rebuild aborts (fail closed)

### Flow G: Derived Artifact Verification

On startup, after `recoverIncomplete()`, `MediaProcessor.verifyDerivedArtifacts()` verifies every `.complete` and `.moderated` asset (plus `.processing` assets) has both derived files present (`thumbs/{id}.jpg` and `previews/{id}.{jpg|mp4}`).

```
1. Read moderated marker IDs
   ├─ Success: use markers as source of truth for complete<->moderated reconciliation
   └─ Failure: skip moderation reconciliation (fail closed; preserve DB moderation state)
2. Query DB for verifiable assets: complete, moderated, processing
3. For each asset:
   ├─ If both thumb + preview exist:
   │    └─ If status=processing and upload file is missing → restore status from markers
   │         (if markers unreadable: conservative restore to `moderated`)
   ├─ Else (missing thumb and/or preview):
   │    ├─ If SSD not mounted → log and skip (retry on next restart)
   │    ├─ If original missing on SSD → mark failed (can't repair)
   │    └─ Else:
   │         ├─ Set status=processing
   │         ├─ Enqueue repair job using SSD original as input (no re-archive)
   │         └─ After repair completes, restore status from marker-derived target
   │              (if markers unreadable: preserve complete/moderated DB state;
   │               stranded processing restores conservatively to `moderated`)
4. Process any queued repairs
```

---

## 6. Processing Details

### Image Processing

**Library:** CoreGraphics/ImageIO (no external dependencies)

| Output | Max Dimension | Quality | Format |
|--------|---------------|---------|--------|
| Thumbnail | `THUMBNAIL_SIZE` (default 512px) | `THUMBNAIL_QUALITY` (default 0.85) | JPEG |
| Preview | `PREVIEW_SIZE` (default 1440px) | `PREVIEW_QUALITY` (default 0.90) | JPEG |

Timestamp extraction priority:
1. EXIF DateTimeOriginal
2. TIFF DateTime
3. Current time (fallback)

Implementation hardening: `ImageProcessor.extractTimestamp(...)` and `ImageProcessor.isImage(...)` run their ImageIO probing work inside explicit `autoreleasepool {}` blocks to bound transient allocations during async processing and startup verification.
Performance hardening: `ImageProcessor.generateThumbnail(...)` and `ImageProcessor.generatePreview(...)` each create one `CGImageSource` and reuse it for both dimension validation and thumbnail generation, avoiding duplicate source open/header parse work.

**Timezone handling:** When the fallback (current time) is used, `DateUtils.exifTimestamp(from:)` formats it using the frozen event timezone (see `EVENT_TIMEZONE` in configuration above). EXIF timestamps from cameras are naive local time strings with no timezone — they are stored as-is.

EXIF is **stripped** from thumbnails and previews (ImageIO doesn't copy metadata).

Color profile handling: derived JPEGs are encoded with ImageIO sharing color optimization (`kCGImageDestinationOptimizeColorForSharing`), which normalizes wide-gamut inputs (for example Display P3) to a sharing-compatible profile for broader client consistency.

### Video Processing

**Library:** AVFoundation (no external dependencies)

| Output | Specification | Quality | Format |
|--------|--------------|---------|--------|
| Thumbnail | `VIDEO_THUMBNAIL_SIZE` (default 512px) @ `VIDEO_THUMBNAIL_TIME` (default 1.0s) | `VIDEO_THUMBNAIL_QUALITY` (default 0.85) | JPEG |
| Preview | H.264 MP4 via `VIDEO_TRANSCODE_PRESET` (default `AVAssetExportPreset1920x1080`); effective dimensions depend on source + preset | — | MP4 |

Timestamp extraction priority:
1. AVAsset.creationDate
2. Metadata scan for creation date
3. Current time (fallback)

### Storage Archive

**Library:** CryptoKit for SHA256

Process:
1. Check SSD mounted (URL.resourceValues isVolume)
2. SHA256 of upload file (1MB chunks)
3. Copy to a temporary SSD path (`.partial-<UUID>`)
4. SHA256 of temporary destination
5. Verify checksums match, then atomically move/replace into final SSD path

If SSD unavailable (or early DB type metadata write fails): job is preserved and the asset stays in processing state for retry on restart.

---

## Thunderbolt Video Offload

Thunderbolt offload is an optional video-only path. Images always process locally. A dispatcher is created only when `VIDEO_SCHEDULER_POLICY != none`, `TB_WORKERS` has at least one valid entry, and a local `bridge*` IPv4 interface is available; otherwise offload is disabled.

### Dispatcher Design

- `ThunderboltDispatcher` is an actor that owns worker slot state and dispatch decisions.
- `WorkerConnection` tracks `host`, shared `port`, `activeSlots`, and `maxSlots`.
- Dispatch is load-aware when complexity-aware scheduling is active under `VIDEO_SCHEDULER_POLICY=auto`: select argmin(t_done) across all candidates (local + remote) using frame-count-based runtime prediction with degradation-aware scoring. If CA is inactive or policy is `fifo`, selection falls back to first available worker.
- `MediaProcessor` computes video dequeue capacity as local slots (`MAX_CONCURRENT_VIDEOS`) plus remote slots only when a dispatcher exists; if dispatcher creation is skipped, remote slots contribute `0`.
- The dispatcher source-binds outbound sockets to local `bridge*` IPv4 addresses (subnet match per worker where possible).

### Data Flow (Remote Video Path)

```
tusd post-finish webhook
  -> MediaProcessor.enqueue(video)
  -> MediaProcessor.process(job)
     -> set status = processing
     -> ThunderboltDispatcher.dispatch(...)
        -> connect (source-bound) to worker
        -> send request header + file bytes
        -> worker transcodes preview + thumbnail
        -> receive response header + preview + thumbnail
        -> verify preview/thumb SHA-256
        -> write previews/{id}.mp4 + thumbs/{id}.jpg
     -> archive original on orchestrator SSD
     -> mark complete in DB + timestamp
```

### Protocol Format (Locked)

```
Orchestrator -> Worker:
[8B fileSize BE][64B sha256 hex][2B nameLen BE][name UTF-8][2B mimeLen BE][mime UTF-8 or empty][file bytes]

Worker -> Orchestrator:
[1B status][8B processNanos BE][4B prevSize BE][64B prevSHA256 hex][4B thumbSize BE][64B thumbSHA256 hex][preview bytes][thumb bytes]
```

Status codes:
- `0x01` — success (preview + thumbnail in response)
- `0x02` — worker processing failure
- `0x03` — capability response (hardware info in preview segment, empty thumb segment)

Implementation notes:
- Request filename is sanitized with `lastPathComponent` before send.
- Orchestrator send path uses BSD `sendfile()` and response receive streams directly to disk.
- Success status byte is `0x01`; any other status triggers local fallback.

### Capability Query Protocol

The orchestrator and benchmark can probe a worker's hardware capabilities before running burst sweeps. This trims the sweep search space using per-worker hardware ceilings.

**Request:** Standard protocol header with sentinel values:
- `fileSize = 0`, SHA = 64 zeroes
- `name = "__kiko_caps__"`, `mime = "application/x-kiko-caps+json"`

**Response:** Status `0x03` with:
- `thumbSize = 0`, `thumbSHA` = 64 zeroes (discriminator)
- Preview segment contains JSON: `{"total_cores": N, "video_encode_engines": N}`
- `previewSHA` verified against actual payload

**Sweep ceiling formula:** `min(max(1, totalCores), max(1, videoEncodeEngines) * 2 + 1)` — shared via `ThunderboltCapabilities.sweepCeiling()` in `KikoMediaCore`.

**Fallback:** Probe failure is treated as an error in orchestrator setup (no silent default); benchmark uses `min(configuredSlots, 16)`. Old workers that don't recognize the sentinel return non-`0x03` status or timeout, both handled as probe failure.

### Burst Sweep Search Modes (Benchmark Delegation)

Thunderbolt recommendation benchmarking (delegated benchmark JSON mode) supports two burst search strategies:

- `exhaustive` (default): evaluates every valid configuration.
- `smart`: runs a 3-phase optimizer (`profile` -> `model` -> `refine`) to reduce evaluations while preserving the same JSON payload contract.

Implementation details:

- Smart mode auto-falls back to exhaustive when config space is `<= 25`.
- Benchmark-evaluation throws and failed runs are assigned a finite penalty wall time, so search continues and payload values remain finite.
- Orchestrator `--thunderbolt` prompts for mode when config space is >25, then passes `--sweep-mode` into delegated benchmark execution.

### Integrity and Fallback

- SHA-256 is verified both directions:
  - source file hash sent by orchestrator and checked by worker
  - preview/thumb hashes returned by worker and checked by orchestrator
- Dispatcher returns a `DispatchResult` enum (`success`, `fallbackLocal`, `transientRetry`, `permanentFailure`). On `fallbackLocal` or `permanentFailure`, `MediaProcessor` falls through to local `processVideo(...)`. On `transientRetry`, the job is re-queued for retry after slot health recovery.
- Partial offload outputs are deleted before fallback.
- Remote offload introduces no new DB states and reuses the existing queue/status lifecycle.

### Acceptance Contract (6 Rules)

Acceptance rules enforced by the current runtime:

| Rule | How current implementation satisfies it |
|------|-----------------------------------------|
| No new DB states | Remote path stays inside `processing -> complete/failed/moderated` lifecycle. |
| Remote fail -> local fallback | Dispatcher returns `fallbackLocal`/`permanentFailure` on non-transient failures; local processing runs in the same job. Transient failures re-queue. |
| SHA-256 both directions | Upload hash checked on worker receive; preview/thumb hashes checked on orchestrator receive. |
| Orchestrator crash -> recovery re-queues | Upload-file lifecycle is unchanged, so existing startup recovery handles interrupted jobs. |
| No dual writes | Worker returns bytes only; orchestrator writes `thumbs/`, `previews/`, and DB records. |
| Unplug doesn't break local | Connect/read failures degrade to local processing; queue continues. |

---

## 7. Security Model

### Network Isolation

| Component | Binding | Accessible From |
|-----------|---------|-----------------|
| Caddy public | __WG_MAC_IP__:8443 | WireGuard tunnel only |
| Caddy internal | 127.0.0.1:8080 | Localhost only |
| tusd | 127.0.0.1:1080 | Localhost only |
| kiko-media public | `BIND_ADDRESS:PUBLIC_PORT` (default `127.0.0.1:3001`) | Localhost only by default; follows configured bind address |
| kiko-media internal | `127.0.0.1:INTERNAL_PORT` (default `127.0.0.1:3002`) | Localhost only |

### Route Separation

The internal listener (`INTERNAL_PORT`, default `3002`) has `PATCH /api/assets/{id}`.
The public listener (`PUBLIC_PORT`, default `3001`) does **not** have this route.

Even if Caddy were misconfigured, there is no moderation endpoint to hit on the public listener.

Additionally, `/health` and `/hooks/upload-complete` are not routed by Caddy at all; they are
loopback-only endpoints on the app.

### Input Validation

| Endpoint | Validation |
|----------|------------|
| POST /api/turnstile/verify | Token required, bounded JSON body (`JSON_MAX_BODY_BYTES`), bounded in-flight validations (overload returns `503` + `Retry-After`), Cloudflare siteverify success + hostname/action/cdata match (against configured expected values) |
| POST /hooks/upload-complete (pre-create) | Session cookie validation (production fail-closed), bounded JSON body |
| POST /hooks/upload-complete (post-finish) | Bounded JSON body, asset ID validation (`1...128` UTF-8 bytes + traversal/control checks), `Size > 0`, `Offset >= 0`, `Offset == Size`, path traversal check, file existence, bounded queue admission (new IDs: `503` + `Retry-After` on overflow/shutdown; existing IDs: idempotent `200`) |
| GET /api/gallery | Session cookie validation, limit capped at 500, offset >= 0 |
| POST /api/assets/{id}/heart | Session cookie validation, asset ID validation (`1...128` UTF-8 bytes + traversal/control checks), only increments `status=complete` assets |
| POST /api/heart-counts | Session cookie validation, bounded JSON body, max 500 IDs per batch, each ID validated, deduplication |
| GET /api/thumbs/{id} | Session cookie validation, asset ID validation (`1...128` UTF-8 bytes + traversal/control checks), path traversal check, DB lookup |
| GET /api/preview/{id} | Session cookie validation, asset ID validation (`1...128` UTF-8 bytes + traversal/control checks), path traversal check, DB lookup |
| PATCH /api/assets/{id} | Bounded JSON body, asset ID validation (`1...128` UTF-8 bytes + traversal/control checks), status enum validation, current state check, marker/DB rollback on failure |

### TLS

- Caddy terminates TLS on the host Mac
- Let's Encrypt certificate via Cloudflare DNS-01
- VPS HAProxy: TCP passthrough (no TLS termination, no MITM capability)
- HSTS enabled: `max-age=63072000; includeSubDomains`

### Content Security Policy

Strict CSP using SHA-256 hashes for inline script/style (no `unsafe-inline`).

**Public block** (includes Turnstile):
- `default-src 'none'` - deny by default
- `script-src 'self' https://challenges.cloudflare.com 'sha256-...'` - external JS, Turnstile script, hashed inline
- `style-src 'sha256-...'` - hashed inline style only
- `img-src 'self' blob:` - images from same origin + local blob URLs (pending upload previews)
- `media-src 'self'` - video from same origin
- `connect-src 'self'` - XHR/fetch same origin only
- `frame-src https://challenges.cloudflare.com` - Turnstile widget iframe
- `frame-ancestors 'none'` - prevent clickjacking

**Internal block** (no Turnstile): Same policy but without `https://challenges.cloudflare.com` in `script-src` and no `frame-src` directive (moderation UI doesn't use Turnstile).

If `deploy/index.html.template` is edited, run `swift scripts/regen-frontend.swift` to regenerate hashes and update `deploy/Caddyfile` (see runbook).

### Known Gaps

| Gap | Risk | Mitigation |
|-----|------|------------|
| No user authentication | Anyone who passes Turnstile can upload/view (no accounts, no identity) | Turnstile blocks bots; URL is secret, event-scoped |
| Session cookie TTL default 4 hours | Users may need to re-verify after idle periods | Foreground API calls auto re-verify on 401/403; background pollers pause and require user-triggered verify |
| Moderation hides from gallery only | Direct /api/thumbs/{id} and /api/preview/{id} remain accessible if ID is known (and session valid) | Accept link-secrecy model during event window |
| Original end-user IP not available at app layer | Can't block abusive users precisely in app logic | App receives proxied local-origin traffic; enforce network controls at edge/VPS if needed |
| Cloudflare API token in plist | Token exposure if host compromised | Scope token to DNS zone, rotate after event |
| Turnstile/HMAC secrets in plist | Secret exposure if host compromised | Rotate after event |
| Silent DB corruption | Corruption that returns empty results (not errors) won't be detected until restart | Startup recovery handles it; runtime detection would add complexity |

---

## 8. Privacy

### Data Collected

| Data | Location | Purpose |
|------|----------|---------|
| Photos/videos (original) | `__BASE_DIRECTORY__/uploads/` (temporary staging) + External SSD (canonical archive) | Low-drag ingest/recovery staging, then canonical archive |
| Photos/videos (processed) | Mac SSD | Viewing |
| Original filenames | SQLite | Internal metadata for archive extension handling, recovery/repair paths, and diagnostics (not in public gallery API) |
| EXIF timestamps | SQLite | Sort order |

### Data NOT Collected

- Original end-user IP addresses at app layer (app sees proxied local-origin traffic; edge/VPS logs may still contain peer addresses)
- User identities (no auth)
- Device fingerprints (no tracking)
- Viewing history (no analytics)

### EXIF Handling

| File Type | EXIF Status |
|-----------|-------------|
| Original (SSD) | PRESERVED (including GPS) |
| Thumbnail | STRIPPED |
| Preview (image) | STRIPPED |
| Preview (video) | VARIES (AVFoundation may preserve some) |

---

## 9. Operational Limits

All server-side values below are configurable via environment variables. See [`docs/advanced-config.md`](advanced-config.md) for the full reference.

| Resource | Default | Env Var | Configured In |
|----------|---------|---------|---------------|
| Max file size | 2GB | `TUSD_MAX_SIZE` | Generated tusd launch args (`-max-size`) |
| Max JSON request body | 1MB | `JSON_MAX_BODY_BYTES` | Config.load() |
| Webhook 503 Retry-After | 5s | `WEBHOOK_RETRY_AFTER_SECONDS` | Config.load() |
| Max concurrent image processing | 6 | `MAX_CONCURRENT_IMAGES` | Config.load() |
| Max concurrent local video processing | 2 | `MAX_CONCURRENT_VIDEOS` | Config.load() |
| Video scheduler policy | `auto` | `VIDEO_SCHEDULER_POLICY` | Config.load() |
| Thunderbolt workers (optional) | empty (disabled) | `TB_WORKERS` | Config.load() |
| Thunderbolt worker port | 12400 | `TB_PORT` | Config.load() |
| Thunderbolt connect timeout | 500ms | `TB_CONNECT_TIMEOUT` | Config.load() |
| Max concurrent SSD rebuild probes | 8 | `MAX_CONCURRENT_REBUILD_PROBES` | Config.load() |
| Max concurrent uploads (client) | 1 | `PARALLEL_UPLOADS` | Generated `deploy/index.html` |
| Gallery page size | 100 default, 500 max | `DEFAULT_PAGE_SIZE`, `MAX_PAGE_SIZE` | Config.load() |
| Upload chunk size | 5MB | `UPLOAD_CHUNK_SIZE_BYTES` | Generated `deploy/index.html` |
| Pending poll max inflight | 3 | `POLL_MAX_INFLIGHT` | Generated `deploy/index.html` |
| Gallery poll base/max backoff | 5000ms / 30000ms | `GALLERY_POLL_BASE_MS`, `GALLERY_POLL_MAX_MS` | Generated `deploy/index.html` |
| Caddy upload timeout | 5 minutes (generated default) | -- | Caddyfile transport (deployment-configurable) |
| Session cookie TTL | 4 hours (14400s) | `SESSION_COOKIE_TTL` | Config.load() |
| Turnstile token validity | Cloudflare-enforced (not app-configurable) | -- | N/A (external enforcement) |
| Video transcode timeout | 300s | `VIDEO_TRANSCODE_TIMEOUT` | Config.load() |
| Health check interval | 60s | `HEALTH_CHECK_INTERVAL` | Config.load() |

*Last reviewed against source: March 18, 2026.*
