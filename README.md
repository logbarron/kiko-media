# Kiko Media

<p align="left">
  <img alt="Kiko Media" src="docs/kikomedia.webp" width="960">
</p>

Kiko Media is a self-hosted photo and video system for single-day events. Guests upload from their phones, media is processed on a local Mac, and the gallery updates as assets become ready.

Read the full story on my website: [Kiko Media](https://loganbarron.com/posts/2026-03-20-kiko-media)

## Core Capabilities

- Wizard-driven setup and operations (`orchestrator`)
- Benchmark-driven configuration (`benchmark`)
- Optional Thunderbolt remote workers for video transcode offload
- External SSD archive with integrity verification
- Single-page, event-focused gallery UI

## Event Flow

1. Guest opens event URL and passes Turnstile (plus optional gate secret flow).
2. Server issues a signed session cookie.
3. Uploads run through `tusd` (resumable TUS protocol).
4. `kiko-media` processes thumbnails/previews and updates gallery APIs.
5. Gallery refreshes as assets become ready.
6. Originals are archived to SSD when configured.

## Architecture Snapshot

| Area | Implementation |
|---|---|
| Backend | Swift 6.2+, Hummingbird 2.x, GRDB/SQLite |
| Media | CoreGraphics, ImageIO, AVFoundation |
| Upload Service | `tusd` |
| Edge | Caddy |
| Network | WireGuard tunnel to VPS, HAProxy passthrough, Cloudflare DNS-only |
| Runtime | Native macOS binaries managed by launchd |
| Frontend | Generated single-page `index.html`, vanilla JS, self-hosted `tus-js-client` |

<p align="center">
  <img src="docs/architecture-overview.svg" alt="Kiko Media architecture overview" width="860">
</p>

## Demo Grid

The four clips below (along with the others in the drop down) attempt to show how the system looks and works. More information here - [Kiko Media](https://loganbarron.com/posts/2026-03-20-kiko-media). 

### Happy Path

Guest entry, upload, and gallery.

### Live Updates

Gallery refresh behavior as new assets become available.

### Orchestrator Setup

Initial setup path through the terminal UI.

### Thunderbolt Worker Status

Worker visibility and remote processing status.

<details>
<summary>Additional demos</summary>

### Auth Flows

Turnstile bootstrap plus optional gate-secret flow.


### Upload

Resumable upload UX from the guest-facing gallery.


### Hearts

Light social feedback without turning the event into an account system.


### Pull to Refresh

Mobile refresh behavior for the event gallery.


### Theme Toggle

Theme switching in the gallery UI.


### Orchestrator Lifecycle

Service lifecycle management from the terminal UI.


### Orchestrator Help

Built-in operator guidance inside the TUI.


### Orchestrator Status

System health and current service state.


### Thunderbolt Setup

Provisioning remote Macs for distributed video work.


### Benchmark Profile

Profile-driven tuning for machine-specific media throughput.


</details>

## Docs

Use these documents for depth:

- [Runbook](docs/runbook.md): setup, deployment, operations, troubleshooting
- [Architecture](docs/architecture.md): API routes, processing flow, security boundaries
- [Advanced Config](docs/advanced-config.md): env vars, defaults, ranges, operational notes
- [Security](docs/security.md): security model, access control, attack surface, privacy
- [Benchmark Stages](docs/benchmark-stages.md): benchmark methodology and stage details
- [Complexity Aware Scheduling](docs/ca-system-guide.md): full reference guide of the CA system

## Commands

```bash
swift run -c release orchestrator
swift run -c release orchestrator --status
swift run -c release orchestrator --start
swift run -c release orchestrator --shutdown
swift run -c release orchestrator --restart
swift run -c release orchestrator --thunderbolt
swift run -c release orchestrator --tb-status

swift run -c release benchmark
swift run -c release benchmark --list
swift run -c release benchmark --stage pipeline --media-folder ~/corpus

swift test
swift test -c release

swift scripts/regen-frontend.swift
swift scripts/wipe-test-media.swift
swift scripts/generate_config_defaults.swift
```

## License

[MIT](LICENSE)