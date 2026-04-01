# Security & Privacy

Assets are self-hosted and encrypted in transit end-to-end (nothing between the guest's device and the Mac sees plaintext). Media is processed locally by default; when `TB_WORKERS` contains at least one valid `host:slots` entry and `VIDEO_SCHEDULER_POLICY` is not `none`, video jobs may be offloaded to operator-managed Thunderbolt workers on the private bridge network.

## Network Path

The Mac is not on the public internet. Guest connections traverse: Cloudflare DNS (grey cloud — resolves domain to VPS, never proxies or sees plaintext) → VPS HAProxy (TCP passthrough, layer 4 only, per-IP rate limiting) → WireGuard tunnel (encrypted, Mac-initiated from behind residential NAT) → Caddy on the Mac (TLS terminates here, bound exclusively to the WireGuard interface). The VPS handles only encrypted bytes. A compromised VPS can deny service but cannot read or modify traffic.

## Privacy

No user accounts. No IP addresses stored or logged. No device fingerprinting. No upload-to-user mapping. Session cookies contain only an HMAC-signed expiry timestamp — nothing that identifies a person. tusd sends `RemoteAddr` in webhook payloads; the app's Decodable struct accepts it (optional field) but never reads, stores, or logs the value.

Thumbnails and previews are pure pixel recompression — GPS, camera make/model, lens info, serial numbers are all stripped. Only the capture timestamp (second precision, event-local timezone, no UTC offset) survives into the database. Originals are archived to an external SSD and never served. Only Cloudflare Turnstile JavaScript is third-party; all other scripts/assets are self-hosted. No analytics, no tracking pixels.

## Access Control

Access control is split by route class:

- Public open routes: `GET /health` and `POST /api/turnstile/verify`
- Public session-gated routes: `GET /api/gallery`, `POST /api/assets/{id}/heart`, `POST /api/heart-counts`, `GET /api/thumbs/{id}`, and `GET /api/preview/{id}`
- Internal shared-secret routes: `GET /api/gallery`, `PATCH /api/assets/{id}`, `GET /api/thumbs/{id}`, and `GET /api/preview/{id}` on the internal listener require `Authorization: <INTERNAL_AUTH_SECRET>`
- Internal webhook route: `POST /hooks/upload-complete` on the internal listener (loopback-only); tusd sends both `pre-create` and `post-finish` hook types to this single endpoint. Pre-create validation checks the forwarded guest session cookie before accepting an upload

To get a public session, a guest must pass Turnstile (hostname, action, and cdata all validated — not just the boolean) and optionally provide a gate secret. Session-gated endpoints return 401 for missing/invalid sessions; Turnstile and gate-secret failures are rejected before session issuance (403/503). Secret comparison hashes both sides to 32 bytes before constant-time XOR to eliminate timing and length-leak attacks. Cookies are `HttpOnly; Secure; SameSite=Lax` with limited default TTL. Invite tokens use URL fragments (never sent to the server) and are cleaned from browser history after extraction.

## What Protects the Media

Asset IDs come from tusd and are validated for allowed characters/length; unpredictability is inherited from tusd. The gallery returns only `complete` assets; moderated and failed assets are filtered at the query level. The gallery supports `?sort=hearts` for heart-sorted ordering and includes a `heartRevision` counter for efficient client-side poll invalidation. `POST /api/heart-counts` accepts a batch of known asset IDs and returns their heart counts (including moderated assets, consistent with the "accessible if ID is known" policy). There is no endpoint that lists all IDs. Moderation hides assets from the gallery but does not block direct access by UUID + valid session — the system has no user accounts, so there is no per-asset authorization. Security enforcement is session gating on public gallery/heart/thumb/preview routes, internal shared-secret auth on moderation/internal asset routes, plus strict asset-ID validation.

Originals (full EXIF, full resolution) exist only on the external SSD with no network exposure. Only processed derivatives (512px thumbs, 1440px/1080p previews) are served. Nothing is encrypted at rest by the application — at-rest encryption depends on FileVault and the external drive. The Mac itself is the trust boundary.

## Thunderbolt Offload (Optional)

Thunderbolt offload changes the trust boundary for video payloads. By default, media is processed on the local Mac. If operators configure Thunderbolt workers, some video jobs may be processed on operator-managed worker hosts on the private bridge network. Operators who want the narrowest trust boundary can keep video processing local with `VIDEO_SCHEDULER_POLICY=none`.

Configuration and scheduler behavior for `VIDEO_SCHEDULER_POLICY`, `TB_WORKERS`, CA activation prerequisites, and tick v2 rollout are documented in `docs/advanced-config.md` and `docs/ca-system-guide.md`.

## Fail-Closed

The app refuses to start with missing or weak secrets (HMAC ≥32 bytes required). Missing Turnstile fields → rejected. Invalid session → 401. If moderation markers are unreadable at startup, artifact verification preserves existing DB moderation state for complete/moderated assets and restores stranded processing assets to moderated (fail-closed to avoid exposing potentially moderated content). A periodic watchdog health-checks the database; failure triggers graceful shutdown. Unreplaced template placeholders → wizard aborts.

Thunderbolt remote processing is integrity fail-safe: connect/protocol/tick/SHA/status failures delete partial remote outputs and return control to local processing for the same job.

## Attack Surface

Core runtime dependencies are Caddy, tusd, and kiko-media. Media processing uses only macOS-native frameworks (CoreGraphics, ImageIO, AVFoundation) — the image/video attack surface is Apple's hardened, OS-updated code. If `TB_WORKERS` is configured and `VIDEO_SCHEDULER_POLICY` is not `none`, Thunderbolt worker hosts and worker scripts become additional trusted processing surfaces for video payloads. Internal listener is hardcoded to `127.0.0.1` and not configurable. tusd binds to localhost with downloads disabled. CSP starts at `default-src 'none'`. Bot user agents blocked. `Server` header stripped. All indexing blocked.

Asset IDs reject path traversal characters (128-byte cap). File paths resolved through symlinks and prefix-checked. Filenames sanitized. Decompression bombs caught before allocation. Video transcodes enforce timeouts. Archives verified with bidirectional SHA256. Moderation state lives as filesystem marker files outside the database — DB corruption cannot un-moderate content.
