# Operations Guide

## Quick Architecture Overview

```
Phone → Browser (tus-js-client)
           ↓
Cloudflare Turnstile token → POST /api/turnstile/verify → session cookie
           ↓
Cloudflare DNS → VPS HAProxy → WireGuard → Host Mac (__WG_MAC_IP__)
                                              ↓
                                    Caddy :8443 (TLS termination; requires WireGuard link up)
                                              │
             ┌────────────────────────────────┴────────────────────────────────┐
             │                                                                 │
/files/* → tusd :1080 (127.0.0.1 only)         public /api/* → kiko-media :3001
                                                (/api/turnstile/verify, gallery, thumbs, preview)
                                                     :3001 (public routes, session-gated except verify)
                                          localhost:8080 → :3002 (moderation, 127.0.0.1 only)
             │                                                                 │
             └─────────────────────────────────────────────────────────────────┘


```
---

## Your Values

This document uses `__PLACEHOLDER__` tokens for deployment-specific values. For most commands, collect each placeholder value from the section listed in the **Source** column before you run it.

| Placeholder | Description | Example | Source |
|-------------|-------------|---------|--------|
| `__DOMAIN__` | Full public hostname for the photo site | `photos.example.com` | Cloudflare DNS records plan (Section 3.2) |
| `__BASE_DOMAIN__` | Root domain registered with Cloudflare | `example.com` | Cloudflare zone/account context (Section 3.1) |
| `__WG_VPS_IP__` | WireGuard tunnel IP of the VPS | `10.77.0.1` | Chosen during WireGuard setup (Section 2.8) |
| `__WG_MAC_IP__` | WireGuard tunnel IP of the Mac (mapped to template placeholder `__WIREGUARD_BIND_IP__`) | `10.77.0.2` | Set before Section 2.9 |
| `__WG_VPS_PUBLIC_KEY__` | WireGuard public key of the VPS | `base64...` | Generated during VPS WireGuard setup (Section 2.7) |
| `__WG_MAC_PUBLIC_KEY__` | WireGuard public key of the Mac tunnel | `base64...` | Shown in the WireGuard app after creating the tunnel (Section 4.6) |
| `__WG_VPS_PRIVATE_KEY__` | WireGuard private key of the VPS | `base64...` | Generated during VPS WireGuard setup (Section 2.7) |
| `__VPS_USER__` | Non-root SSH user on the VPS | `deploy` | Created during VPS setup (Section 2.5) |
| `__REPO_DIR__` | Where you cloned the `kiko-media` repo | `~/kiko-media` | Repo location step (Section 4.1) |
| `__BASE_DIRECTORY__` | Runtime data directory (uploads, thumbs, previews, logs, DB) | `~/Documents/kiko-media` | Default from `deploy/defaults.env`; configurable in wizard Step 10 |
| `__PUBLIC_PORT__` | Public HTTP listener port for `kiko-media` | `3001` | Default from `deploy/defaults.env`; configurable in wizard Step 10 |
| `__INTERNAL_PORT__` | Internal/moderation HTTP listener port for `kiko-media` | `3002` | Default from `deploy/defaults.env`; configurable in wizard Step 10 |
| `__BIND_ADDRESS__` | Bind address for the public `kiko-media` listener | `127.0.0.1` | Default from `deploy/defaults.env`; configurable in wizard Step 10 |
| `__TUSD_MAX_SIZE__` | Maximum tusd upload size in bytes | `2147483648` | Default from `deploy/defaults.env`; configurable in wizard Step 10 |
| `__EXTERNAL_SSD_PATH__` | Archive path for originals on the external SSD | `/Volumes/KikoSSD/originals` | Set in wizard Step 7 (External SSD) |

`<VPS_IPv4>`, `<VPS_IPv6>`, and `<VPS_IP>` appear in angle brackets where the value comes from your VPS provider's dashboard after provisioning.

---

## What's Running

| Service | Binary | Port(s) | Binding | Purpose |
|---------|--------|---------|---------|---------|
| Caddy | `~/bin/caddy` | 8443, 8080 | __WG_MAC_IP__:8443, 127.0.0.1:8080 | TLS termination, routing, static files |
| tusd | `~/bin/tusd` | 1080 | 127.0.0.1 | TUS resumable uploads |
| kiko-media | `__REPO_DIR__/.build/release/KikoMedia` | `PUBLIC_PORT`, `INTERNAL_PORT` (defaults: 3001, 3002) | `BIND_ADDRESS`:`PUBLIC_PORT` (default `127.0.0.1:3001`), `127.0.0.1`:`INTERNAL_PORT` | Processing, gallery API |

**launchd identifiers:** `com.kiko.caddy`, `com.kiko.tusd`, `com.kiko.media`

`PUBLIC_PORT`, `INTERNAL_PORT`, `BIND_ADDRESS`, and `TUSD_MAX_SIZE` are configurable during the Setup Wizard (Section 4.7) in the Advanced Configuration prompts. Commands below use `__PLACEHOLDER__` tokens (for example `__PUBLIC_PORT__` and `__INTERNAL_PORT__`) so they remain correct for non-default configs.

---

## First-Time Setup Order

For a first-time install, follow this order:

1. Complete **VPS Setup** (Section 2.1–2.12), except the Mac `[Peer]` block in Section 2.8. Keep that block omitted/commented (no placeholder key), then after Section 4.6 add the Mac peer once by following Section 4.6 step 5.
2. Complete **Cloudflare DNS + Turnstile** (Section 3)
3. Complete **Mac Setup** through **WireGuard** (Section 4.6)
4. Gather the inputs listed in Section 4.5, then run the **Setup Wizard** (Section 4.7). Build `kiko-media` manually in Section 4.8 only if the wizard build fails or you choose manual builds.
5. Start the system (Section 5)

---

## 1. Prerequisites

### Hardware
- Apple Silicon Mac (e.g. Mac Mini M4)
- macOS 26+ (check: `sw_vers -productVersion` — the major version must be `26` or higher)
- External SSD (mount path configured via setup wizard)
- *Optional* HDMI dummy plug (prevents GPU throttling when headless on Mac Mini)

`macOS 26` here is the SwiftPM deployment target from `Package.swift` (`platforms: [.macOS(.v26)]`), not the Darwin kernel version.

### Software
- Xcode Command Line Tools (`xcode-select --install`)
- Swift 6.2+ toolchain (check: `swift --version` — `Package.swift` sets `swift-tools-version: 6.2`)
- WireGuard (Mac App Store)

### System Settings
- Automatic Login enabled (required for launchd after reboot)
- If FileVault is enabled, the Mac must be manually unlocked after reboot (automatic login will not work)

### Accounts
- VPS provider for public IP relay
- Cloudflare account with domain (for DNS and TLS certificates)

---

## 2. VPS Setup

For setting up a new VPS relay from scratch.

### 2.1 Provision VPS

1. **Log into your VPS provider's dashboard** and create a new instance
2. Configure:
	   - **Image:** Ubuntu 24.04 LTS
	   - **Region:** Closest to venue
	   - **Plan:** 1 vCPU / 1GB RAM (2GB recommended). Dedicated is not required for HAProxy + WireGuard relay.
	   - **Label:** `kiko-relay`
	   - **Root Password:** Generate strong password
	   - **SSH Keys:** Add your public key
3. **Create instance**
4. Wait for provisioning (1-2 minutes)
5. Record from dashboard:
   - **IPv4 address** → `VPS_IPv4`
   - **IPv6 address** (Network tab) → `VPS_IPv6`

### 2.2 Configure Cloud Firewall

1. In your VPS provider's dashboard, create a **Cloud Firewall** (or equivalent)
2. **Label:** `kiko-relay-fw`
3. **Inbound Rules:**

| Label | Protocol | Ports | Sources | Action |
|-------|----------|-------|---------|--------|
| SSH | TCP | 22 | All IPv4, All IPv6 | Accept |
| WireGuard | UDP | 51820 | All IPv4, All IPv6 | Accept |
| HTTPS | TCP | 443 | All IPv4, All IPv6 | Accept |

4. **Default Inbound Policy:** Drop
5. **Outbound:** Accept all (default)
6. **Attach** the firewall to your VPS instance
7. **Create Firewall**

### 2.3 Initial VPS Setup

SSH into the VPS:

```bash
ssh root@<VPS_IPv4>
```

Update the system:

```bash
apt update
```
```bash
apt -y upgrade
```

Reboot if required (Ubuntu creates this file after some upgrades):

```bash
if [ -f /var/run/reboot-required ]; then
  cat /var/run/reboot-required
  reboot
fi
```

If the VPS rebooted, wait for it to come back up and then re-run `ssh root@<VPS_IPv4>` before continuing.

### 2.4 Install Packages
```bash
apt install -y wireguard wireguard-tools haproxy ufw
```

### 2.5 SSH Hardening (recommended)

Run these commands **one at a time** (pasting as a block can fail due to interactive prompts):

```bash
adduser __VPS_USER__
# ↑ Sets a password — record it (needed for sudo)
```
```bash
usermod -aG sudo __VPS_USER__
```
```bash
mkdir -p /home/__VPS_USER__/.ssh
```
```bash
cp /root/.ssh/authorized_keys /home/__VPS_USER__/.ssh/
```
```bash
chown -R __VPS_USER__:__VPS_USER__ /home/__VPS_USER__/.ssh
```
```bash
chmod 700 /home/__VPS_USER__/.ssh
```
```bash
chmod 600 /home/__VPS_USER__/.ssh/authorized_keys
```

Disable password auth and root login. Some images include provider/cloud-init drop-ins under `/etc/ssh/sshd_config.d/`. Do not delete them by default; verify effective settings after reload.

Write the hardening config:

```bash
cat > /etc/ssh/sshd_config.d/49-hardening.conf << 'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
PermitRootLogin no
PubkeyAuthentication yes
PermitEmptyPasswords no
MaxAuthTries 3
X11Forwarding no
ClientAliveInterval 300
ClientAliveCountMax 2
LoginGraceTime 60
LogLevel VERBOSE
EOF
```

Ensure SSH is installed and running before reload:

```bash
dpkg -s openssh-server >/dev/null 2>&1 || apt install -y openssh-server
systemctl enable --now ssh
systemctl is-active ssh
# Expected: active
```

Test and reload:

```bash
sshd -t && systemctl reload ssh
```

Verify:

```bash
sshd -T | grep -iE '^(passwordauthentication|permitrootlogin) '
# Expected: passwordauthentication no
#           permitrootlogin no
```

Verify `ssh __VPS_USER__@<VPS_IP>` works before closing the root session.

Recovery if verification fails (keep the root session open until this works):
```bash
# Check permissions (sshd is strict)
ls -ld /home/__VPS_USER__ /home/__VPS_USER__/.ssh /home/__VPS_USER__/.ssh/authorized_keys

# Fix ownership/permissions if needed
chown -R __VPS_USER__:__VPS_USER__ /home/__VPS_USER__/.ssh
chmod 700 /home/__VPS_USER__/.ssh
chmod 600 /home/__VPS_USER__/.ssh/authorized_keys

# Check the exact failure reason
tail -n 50 /var/log/auth.log
```

From here on, continue the VPS setup as `__VPS_USER__` (use `sudo` when needed):
```bash
ssh __VPS_USER__@<VPS_IP>
```

### 2.6 Firewall (UFW)

Run these **one at a time** (the first `sudo` command will prompt for the deploy password):

```bash
sudo ufw default deny incoming
```
```bash
sudo ufw default allow outgoing
```
```bash
sudo ufw limit 22/tcp
```
```bash
sudo ufw allow 51820/udp
```
```bash
sudo ufw allow 443/tcp
```
```bash
sudo ufw logging medium
```
```bash
sudo ufw enable
```

### 2.7 WireGuard Keys

Generate keys:

```bash
sudo bash -c 'umask 077; wg genkey | tee /etc/wireguard/private.key | wg pubkey > /etc/wireguard/public.key'
```

Record public key:

```bash
sudo cat /etc/wireguard/public.key
# Record this as __WG_VPS_PUBLIC_KEY__ (used in the Mac tunnel config)
```

Record private key:

```bash
sudo cat /etc/wireguard/private.key
# Record this as __WG_VPS_PRIVATE_KEY__ (used in wg0.conf below)
```

### 2.8 WireGuard Config

Open the config file:

```bash
sudo nano /etc/wireguard/wg0.conf
```

Paste this (replacing `__WG_VPS_PRIVATE_KEY__` with the private key from Section 2.7):

```ini
[Interface]
Address = __WG_VPS_IP__/24
ListenPort = 51820
PrivateKey = __WG_VPS_PRIVATE_KEY__
```

Use the same `/24` as the planned Mac tunnel IP (for example `10.77.0.1` on VPS and `10.77.0.2` on Mac).

Do not add an empty or commented `[Peer]` block yet. The real `[Peer]` block is added later in Section 4.6, step 5.

### 2.9 HAProxy Config
```bash
sudo tee /etc/haproxy/haproxy.cfg << 'EOF'
global
    log /dev/log local0
    maxconn 4096

defaults
    mode tcp
    timeout connect 10s
    timeout client 30m
    timeout server 30m
    timeout client-fin 1s
    timeout server-fin 1s

frontend https_in
    bind [::]:443 v4v6
    stick-table type ip size 1m expire 10s store conn_rate(10s),conn_cur
    tcp-request connection track-sc0 src
    tcp-request connection reject if { src_conn_rate gt 100 } or { src_conn_cur gt 200 }
    default_backend home_server

backend home_server
    server home __WG_MAC_IP__:8443 check
EOF
```

### 2.10 VPS Performance Tuning

**Sysctl tuning** (idempotent; safe to re-run):

Create the config file:

```bash
cat << 'EOF' | sudo tee /etc/sysctl.d/99-kiko-media.conf
# BBR congestion control (improves throughput on high-latency cellular connections)
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr

# TCP buffer tuning (optimize for large file uploads)
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864
net.ipv4.tcp_rmem = 4096 87380 33554432
net.ipv4.tcp_wmem = 4096 65536 33554432
net.ipv4.tcp_mtu_probing = 1
net.core.netdev_max_backlog = 50000
EOF
```

Apply it:

```bash
sudo sysctl -p /etc/sysctl.d/99-kiko-media.conf
```

Verify:

```bash
sysctl net.ipv4.tcp_congestion_control
# Expected: bbr
```

### 2.11 Start VPS Services

Start WireGuard (it is OK if you have no peers yet). The first `sudo` may prompt for the deploy password:

```bash
sudo systemctl enable --now wg-quick@wg0
```

Enable HAProxy for auto-start, then restart to pick up the config from Section 2.9 (apt install auto-starts HAProxy with the default config; enable --now would be a no-op):

```bash
sudo systemctl enable haproxy
```
```bash
sudo systemctl restart haproxy
```

### 2.12 Verify VPS Setup

Check WireGuard running (on first-time setup, you won't see a peer until you add the Mac key in Section 4.6):

```bash
sudo wg show
```

Check HAProxy listening:

```bash
sudo ss -tulnp | grep 443
```

Check services enabled for auto-start:

```bash
systemctl is-enabled wg-quick@wg0
```
```bash
systemctl is-enabled haproxy
```

---

## 3. Cloudflare DNS Setup

### 3.1 Create API Token

1. Log into [Cloudflare Dashboard](https://dash.cloudflare.com)
2. **Profile** (top right) → **API Tokens** → **Create Token**
3. Select **Create Custom Token**
4. Configure:
   - **Token name:** `Caddy DNS __BASE_DOMAIN__`
   - **Permissions:**
     - Zone → Zone → Read
     - Zone → DNS → Edit
   - **Zone Resources:** Include → Specific zone → `__BASE_DOMAIN__`
5. **Create Token**
6. **Copy token immediately** (shown only once)

You will paste this token into the setup wizard (Section 4.7). The wizard writes it into `~/Library/LaunchAgents/com.kiko.caddy.plist` for Caddy's DNS-01 certificate flow.

### 3.2 Create DNS Records

1. **Cloudflare Dashboard** → `__BASE_DOMAIN__` → **DNS** → **Records**

2. **Add A record:**
   - Type: `A`
   - Name: `photos`
   - IPv4: `<VPS_IPv4>`
   - Proxy status: **DNS only (grey cloud)** ← Critical

3. **Add AAAA record (optional):**
   - Type: `AAAA`
   - Name: `photos`
   - IPv6: `<VPS_IPv6>`
   - Proxy status: **DNS only (grey cloud)** ← Critical

**Both clouds MUST be grey.** Orange cloud = Cloudflare proxy = upload size limits.

### 3.3 Verify DNS

```bash
dig __DOMAIN__ A +short
```
```bash
dig __DOMAIN__ AAAA +short
```

Should return VPS IPs.

### 3.4 Test IPv6 Connectivity

Only publish AAAA record if IPv6 works end-to-end.

From VPS — verify IPv6 works:

```bash
curl -6 ifconfig.me
```

After full setup — test IPv6 path from Mac (public Caddy blocks curl/wget user agents; use a browser-like UA for CLI checks):

```bash
curl -6 -I https://__DOMAIN__/ -A "Mozilla/5.0"
```
```bash
curl -4 -I https://__DOMAIN__/ -A "Mozilla/5.0"
```

**If IPv6 fails:** Remove the AAAA record in Cloudflare. IPv4-only is fine. A broken AAAA can add noticeable delay for some clients due to Happy Eyeballs fallback.

---

### 3.5 Cloudflare Turnstile Setup

Turnstile provides bot protection for the public site without traditional CAPTCHAs.

#### 3.5.1 Create Turnstile Widget

1. Log into [Cloudflare Dashboard](https://dash.cloudflare.com)
2. **Turnstile** (left sidebar) → **Add widget**
3. Configure:
   - **Widget name:** `kiko-photos` (or descriptive name)
   - **Hostname:** `__DOMAIN__`
   - **Widget mode:** Managed
   - **Pre-clearance:** Off
4. **Create**
5. **Copy immediately:**
   - **Site key** → paste into the setup wizard (Section 4.7). The wizard writes it into `deploy/index.html`.
   - **Secret key** → paste into the setup wizard (Section 4.7). The wizard writes it into `~/Library/LaunchAgents/com.kiko.media.plist`.

#### 3.5.2 Update Turnstile Keys

The setup wizard (`swift run -c release orchestrator`) writes:
- the **site key** into `deploy/index.html`
- the **secret key** into `~/Library/LaunchAgents/com.kiko.media.plist`

To change either key later, re-run the wizard and restart services. Do not replace `__TURNSTILE_SITEKEY__` inside `deploy/index.html.template` (keep wizard placeholders intact).

#### 3.5.3 Verify CSP

The public block's CSP already includes `https://challenges.cloudflare.com` in `script-src` and `frame-src`. The internal block omits these (no Turnstile needed). If you've customized CSP, ensure the Turnstile rules are present in the public block.

---

## 4. Mac Setup

This section has two paths. Pick one before continuing.

A. Wizard path (recommended)
- Clone the repo and `cd __REPO_DIR__`
- Complete Section 4.5 (inputs) and Section 4.6 (Mac WireGuard)
- Run `swift run -c release orchestrator` (Section 4.7)
- Skip Sections 4.2, 4.3, 4.4, and 4.8 unless the wizard fails

B. Manual path (fallback)
- Use this only if you intentionally want manual setup or the wizard cannot complete
- Follow Sections 4.2, 4.3, 4.4, and 4.8 for manual install/build steps

## 4.1 Repo Location

Pick a location for the repo (`__REPO_DIR__`). It does **not** have to be `~/kiko-media` or any specific path.

At minimum, verify:

```bash
cd __REPO_DIR__
ls -la Package.swift
```

### 4.2 Create Directories (Manual Fallback Only)

> **Note:** The setup wizard (Section 4.7) handles this automatically. These manual steps are only needed if the wizard flow fails or you prefer manual setup.

```bash
# Binaries
mkdir -p ~/bin

# Data directories
mkdir -p __BASE_DIRECTORY__/{uploads,thumbs,previews,logs,moderated}

# External SSD
mkdir -p __EXTERNAL_SSD_PATH__
```

### 4.3 Install Caddy (Manual Fallback Only)

> **Note:** The setup wizard (Section 4.7) handles this automatically. These manual steps are only needed if the wizard flow fails or you prefer manual setup.

```bash
# Download with Cloudflare DNS plugin
curl -L "https://caddyserver.com/api/download?os=darwin&arch=arm64&p=github.com%2Fcaddy-dns%2Fcloudflare" \
  -o ~/bin/caddy
chmod +x ~/bin/caddy

# Verify
~/bin/caddy version
```

### 4.4 Install tusd (Manual Fallback Only)

> **Note:** The setup wizard (Section 4.7) handles this automatically. These manual steps are only needed if the wizard flow fails or you prefer manual setup.

```bash
# Get latest tag from GitHub API (robust JSON parse via plutil)
TAG=$(curl -fsSL https://api.github.com/repos/tus/tusd/releases/latest | plutil -extract tag_name raw -o - -)

curl -fL "https://github.com/tus/tusd/releases/download/${TAG}/tusd_darwin_arm64.zip" -o tusd.zip
unzip tusd.zip
mv tusd_darwin_arm64/tusd ~/bin/tusd
chmod +x ~/bin/tusd
rm -rf tusd.zip tusd_darwin_arm64

# Verify
~/bin/tusd -version
```

**Manual run (for testing):**
```bash
~/bin/tusd \
    -upload-dir __BASE_DIRECTORY__/uploads \
    -max-size __TUSD_MAX_SIZE__ \
    -base-path /files/ \
    -behind-proxy \
    -hooks-http http://127.0.0.1:__INTERNAL_PORT__/hooks/upload-complete \
    -hooks-enabled-events pre-create,post-finish \
    -hooks-http-forward-headers Cookie \
    -disable-download \
    -host 127.0.0.1 \
    -port 1080 \
    -verbose=false
```

### 4.5 Wizard Inputs Checklist

Do not run the setup wizard until you have these values ready (you will paste them into prompts):

| Wizard prompt | What you need | Where you get it |
|---------------|---------------|------------------|
| Domain | `__DOMAIN__` | Your DNS plan (Section 3) |
| WireGuard IP | `__WG_MAC_IP__` | You choose it when configuring the Mac tunnel (Section 4.6) |
| Cloudflare API token | A token with Zone:Read + DNS:Edit | Section 3.1 |
| Turnstile site key + secret key | Two strings | Section 3.5 |
| Event gate secret (optional) | Shared event code/token written to `GATE_SECRET` (leave empty to disable). | You optionally set it |
| SSD originals path | e.g. `__EXTERNAL_SSD_PATH__` | Your mounted SSD |
| Event timezone (optional) | e.g. `America/New_York` | Your event location |

If you haven't done VPS/WireGuard yet, complete Section 2 and Section 4.6 first. The wizard needs `__WG_MAC_IP__`, and Caddy will not start unless WireGuard is up.

### 4.6 Configure WireGuard (Mac)

1. Download WireGuard from Mac App Store
2. Click `+` → **Add Empty Tunnel**
3. In the popup:
   - **Name:** `vps-tunnel`
   - **Public key** is displayed below the name — copy and record this as `__WG_MAC_PUBLIC_KEY__` (needed for VPS peer config)
   - **On Demand:** check **Ethernet**
   - The config box is pre-filled with `[Interface]` and `PrivateKey = <auto-generated>`. Paste the remaining lines below them so the full config looks like:

```ini
[Interface]
PrivateKey = <already filled in — do not change>
Address = __WG_MAC_IP__/24
MTU = 1380

[Peer]
PublicKey = __WG_VPS_PUBLIC_KEY__
Endpoint = <VPS_IP>:51820
AllowedIPs = __WG_VPS_IP__/32
PersistentKeepalive = 25
```

4. Save

5. Add the Mac public key to the VPS:

```bash
sudo tee -a /etc/wireguard/wg0.conf << 'EOF'

[Peer]
PublicKey = __WG_MAC_PUBLIC_KEY__
AllowedIPs = __WG_MAC_IP__/32
EOF
```

Restart WireGuard:

```bash
sudo systemctl restart wg-quick@wg0
```

6. Activate the tunnel in the WireGuard app on the Mac, then verify:

On Mac:

```bash
ping -c 1 __WG_VPS_IP__
```

On VPS (expect a recent "latest handshake" for the Mac peer):

```bash
sudo wg show
```

Optional, from VPS:

```bash
ping -c 1 __WG_MAC_IP__
```

### 4.7 Run the Setup Wizard

The setup wizard configures all templates and installs launchd plists. It collects your domain, WireGuard IP, Cloudflare token, Turnstile keys, optional event gate secret, SSD path, and timezone, then generates all config files. **The wizard starts all services automatically at the end.** Caddy will immediately attempt DNS-01 certificate issuance.

**Before running the wizard:** If your network uses AdGuard/Pi-hole (or any local DNS override), flush the DNS cache for `__DOMAIN__` now. Required on first deploy and whenever DNS records change. The wizard starts Caddy at the end — if DNS is stale, certificate issuance will fail.

```bash
cd __REPO_DIR__
swift run -c release orchestrator
```

Non-interactive shortcuts (no config changes):
```bash
# Show configuration + service status
swift run -c release orchestrator --status

# Show Thunderbolt worker reachability plus effective runtime context
swift run -c release orchestrator --tb-status

# Start all services (enable + bootstrap + caffeinate)
swift run -c release orchestrator --start

# Stop all services for current session (bootout + remove caffeinate)
swift run -c release orchestrator --stop

# Persistently disable all services across reboots (disable + bootout)
swift run -c release orchestrator --shutdown

# Full reload: stop then start
swift run -c release orchestrator --restart

# Run Thunderbolt worker generation + benchmark flow
swift run -c release orchestrator --thunderbolt

# Help / usage
swift run -c release orchestrator --help
```

Frontend artifact refresh (template changes only):
```bash
swift scripts/regen-frontend.swift
```

Test media wipe (stops services, removes uploaded/processed media, leaves system stopped):
```bash
swift scripts/wipe-test-media.swift
```

The wizard:
1. Downloads Caddy and tusd to `~/bin/` (or detects existing installs and offers to keep them)
2. Generates `deploy/Caddyfile` from `deploy/Caddyfile.template` (domain + WireGuard IP)
3. Generates `deploy/index.html` from `deploy/index.html.template` (Turnstile sitekey + upload limits)
4. Installs launchd plists to `~/Library/LaunchAgents/` (with all placeholders replaced, including all advanced config env vars, and permissions set to `600`)
5. Creates data directories under `__BASE_DIRECTORY__/` (uploads/thumbs/previews/logs/moderated)
6. Auto-generates a session HMAC secret (or prompts you to paste one)
7. Optionally configures advanced settings (processing limits, security thresholds, ports, caching, etc.)
8. Optionally runs Thunderbolt offload setup (worker script generation, worker detection, benchmark, and runtime knob writes to `com.kiko.media.plist`)
9. Optionally builds kiko-media (or detects existing binary)

The Advanced Configuration step asks whether to configure advanced options. Defaults work well for most deployments. For the full list of configurable values, see [`docs/advanced-config.md`](advanced-config.md).

After the wizard completes, it attempts to secure the plists (mode `600`). If you ever edit them manually or permission changes, re-apply:
```bash
chmod 600 ~/Library/LaunchAgents/com.kiko.*.plist
```

**Re-running the wizard:** You can re-run `swift run -c release orchestrator` at any time to change settings. It remembers previous values within a session and lets you keep or change each one.
After generating files, the wizard proceeds through build + optional flush and then, by default, reloads services by stopping them first and then re-enabling + bootstrapping all three LaunchAgents (you can stop before restart if needed).
If you enable flush mode, it deletes `metadata.db` (+ `-wal`/`-shm`) and clears `thumbs/` + `previews/`, while preserving `uploads/` and `moderated/`.
If you want a true empty test reset instead, run `swift scripts/wipe-test-media.swift`. It has no flags; it reads `BASE_DIRECTORY` and `EXTERNAL_SSD_PATH` from `~/Library/LaunchAgents/com.kiko.media.plist`, prints the wipe targets, and only proceeds after you type `WIPE`. It stops services, clears `uploads/`, `thumbs/`, `previews/`, `moderated/`, removes `metadata.db` (+ `-wal`/`-shm`), and clears the configured SSD archive directory. It does not restart services.

**Turnstile protection:** Startup is fail-closed. `TURNSTILE_SECRET`, `SESSION_HMAC_SECRET`, `TURNSTILE_EXPECTED_HOSTNAME`, `TURNSTILE_EXPECTED_ACTION`, and `TURNSTILE_EXPECTED_CDATA` must all be set and non-empty, and `SESSION_HMAC_SECRET` must be at least 32 bytes. If any check fails, `kiko-media` refuses to start. The wizard sets hostname/action/cdata defaults and writes matching widget values into `deploy/index.html`. If you manually change Turnstile action/cdata values in `deploy/index.html`, update `TURNSTILE_EXPECTED_ACTION` and `TURNSTILE_EXPECTED_CDATA` to match before restarting.

**Event timezone:** The wizard prompts for a timezone. When empty (or unset), kiko-media uses the Mac's system timezone. If the Mac is in a different timezone to the event, set this to the event's IANA timezone identifier (e.g. `America/New_York`).

If an invalid identifier is set, the app logs a warning and falls back to the system timezone. Check logs on startup to confirm:
```bash
log show --predicate 'subsystem == "com.kiko.media"' --last 1m | grep -i timezone
# Expected: "Event timezone: America/New_York (from EVENT_TIMEZONE)"
#       or: "Event timezone: America/Los_Angeles (system default)"
```

**External SSD path:** The wizard prompts for the SSD originals path and validates that the selected path (or its parent) is writable before writing it as `EXTERNAL_SSD_PATH` in the launchd plist. Runtime mount detection is strict and expects `EXTERNAL_SSD_PATH` to be under `/Volumes/<VolumeName>/...`. There is no default -- the wizard requires you to select or enter your SSD path.

### 4.8 Build kiko-media (Manual Fallback Only)

> **Note:** The setup wizard (Section 4.7) offers to build automatically after generating config files. These manual steps are only needed if the wizard build fails or you prefer manual builds.

```bash
cd __REPO_DIR__
swift build -c release

# Verify
ls -la .build/release/KikoMedia
```

---

## Thunderbolt Worker Setup

Use this when you have Thunderbolt-connected worker Macs for video offload.

This flow configures `Thunderbolt offload`. It does not by itself guarantee `CA scheduling`, which still depends on the runtime activation gate and a usable local `benchmark-prior.json` baseline.

1. Run the standalone Thunderbolt flow:
```bash
cd __REPO_DIR__
swift run -c release orchestrator --thunderbolt
```
2. The wizard prompts for worker bridge IPs, then generates one `deploy/worker.swift` artifact and prints copy/run instructions.
3. Copy `deploy/worker.swift` to each worker Mac and run it in Terminal:
```bash
swift worker.swift
```
4. Continue the wizard to online detection + benchmark. It probes each worker over the Thunderbolt bridge (including hardware capability detection for sweep ceiling computation), delegates Thunderbolt benchmarking to the benchmark target, and writes runtime knobs into `~/Library/LaunchAgents/com.kiko.media.plist`.
   - Writes local concurrency knobs: `MAX_CONCURRENT_IMAGES` and `MAX_CONCURRENT_VIDEOS`.
   - Writes remote knobs: `TB_WORKERS`, `TB_PORT`, and `TB_CONNECT_TIMEOUT` (when worker slots are selected).
   - If all worker slots are set to `0`, `TB_WORKERS` is left unchanged (not auto-cleared).
   - Thunderbolt benchmark input requires videos only (at least 4 recommended for reliable sweep results). Images are optional in this flow.
   - If configuration space is greater than 25, the wizard prompts for sweep mode:
     - `Algorithmic Search` (`smart`): 3-phase optimizer, fewer benchmark runs
     - `Exhaustive Search` (`exhaustive`): evaluates every configuration
   - If configuration space is 25 or less, sweep mode selection is skipped and exhaustive mode runs automatically.
5. Verify worker reachability:
```bash
swift run -c release orchestrator --tb-status
```

If this flow completed successfully, `--tb-status` shows both worker reachability and the effective runtime summary. Reachable workers alone do not guarantee CA runtime under `VIDEO_SCHEDULER_POLICY=auto`.

Notes:
- Workers should listen on bridge IP only; the generated script auto-detects `bridge*` IPv4 and warns if MTU is not 9000.
- `TB_WORKERS` describes Thunderbolt worker inventory and remote slot capacity.
- `VIDEO_SCHEDULER_POLICY` chooses production behavior:
  - `auto` = CA when ready, otherwise FIFO with offload
  - `fifo` = force FIFO with offload
  - `none` = local-only FIFO
- `TB_WORKERS` plus `VIDEO_SCHEDULER_POLICY != none` enables Thunderbolt offload configuration at startup.
- Parser rule: entries must be `host:slots` with non-empty host and `slots > 0`; invalid entries are ignored.
- Under `VIDEO_SCHEDULER_POLICY=auto`, if at least one valid worker entry parses, runtime evaluates whether CA scheduling can be enabled.
- Under `VIDEO_SCHEDULER_POLICY=auto`, CA scheduling turns on only when the local prior artifact/profile and strict tick v2 checks also pass after restart.
- If `TB_WORKERS` is empty or all entries are invalid, video dequeue is local FIFO regardless of policy.
- `TB_WORKERS=""` disables remote offload and keeps strict FIFO local behavior.
- Workers respond to capability probes (sentinel `__kiko_caps__`) with `totalCores` and `videoEncodeEngines`, used to compute per-worker sweep ceilings.
- Sweep-mode choice (`smart` vs `exhaustive`) affects recommendation benchmarking only; runtime offload behavior after settings are written is unchanged.
- When CA scheduling is active, strict tick v2 validation is enforced for worker progress.
- Re-generate and re-copy `deploy/worker.swift` after upgrades; legacy worker scripts may connect but fail tick validation and fall back to local processing.
- To force FIFO while keeping offload configured:
```bash
plutil -replace EnvironmentVariables.VIDEO_SCHEDULER_POLICY -string fifo ~/Library/LaunchAgents/com.kiko.media.plist
swift run -c release orchestrator --restart
```
- To disable Thunderbolt runtime offload without clearing worker inventory:
```bash
plutil -replace EnvironmentVariables.VIDEO_SCHEDULER_POLICY -string none ~/Library/LaunchAgents/com.kiko.media.plist
swift run -c release orchestrator --restart
```
- To explicitly disable TB workers:
```bash
plutil -replace EnvironmentVariables.TB_WORKERS -string "" ~/Library/LaunchAgents/com.kiko.media.plist
swift run -c release orchestrator --restart
```

### Complete CA Scheduling Setup

After Thunderbolt worker setup is complete, use this when `CA scheduling` still needs the local prior baseline or local prior refresh.

1. Run the benchmark flow:
```bash
cd __REPO_DIR__
swift run -c release benchmark
```
2. Choose a mode that includes `pipeline`:
```text
Profile
```
`Profile` is enough for the local `benchmark-prior.json` baseline because it includes `pipeline`.

`Extended` also works if you want to rerun the archive or Thunderbolt stages.

3. Use a media folder that includes images and videos from a real event.
4. Let the benchmark flow create or update the production baseline at `{BASE_DIRECTORY}/benchmark-prior.json`.
   - This baseline affects production CA only when `VIDEO_SCHEDULER_POLICY=auto`.
5. Restart services so the final Thunderbolt and CA settings are loaded:
```bash
swift run -c release orchestrator --restart
```

This is the canonical operator path after Thunderbolt worker setup, both during initial setup and when adding Thunderbolt later.

## Thunderbolt Troubleshooting

### Worker not reachable (`--tb-status` shows unreachable)

Checks:
- Confirm worker is running in foreground Terminal on the worker Mac.
- Confirm worker IP in `TB_WORKERS` is the worker bridge IP (not Wi-Fi/LAN IP):
```bash
grep -A2 TB_WORKERS ~/Library/LaunchAgents/com.kiko.media.plist
```
- Confirm local bridge source IP exists on orchestrator:
```bash
ifconfig | grep -A3 '^bridge'
```
- Re-run guided setup to regenerate and re-probe:
```bash
swift run -c release orchestrator --thunderbolt
```

If macOS Application Firewall is enabled on worker Macs, allow incoming connections for `/usr/bin/swift` (or temporarily disable firewall for event operation) so the worker listener can accept bridge traffic.

### SHA mismatch from worker

Symptoms:
- Logs show offload failure with preview/thumb SHA mismatch and local fallback.

Fix:
- Re-copy the generated worker script from orchestrator to worker.
- Stop/restart worker process (`Ctrl-C`, then `swift worker.swift` again).
- Ensure both Macs are on a stable Thunderbolt bridge path (avoid mixed Wi-Fi routing).

### Tick v2 mismatch (legacy worker script)

Symptoms:
- Logs contain `read tick v2 frame failed`, `tick v2 decode failed`, or repeated local-processing fallback after dispatch attempts.

Fix:
- Re-generate worker script:
```bash
swift run -c release orchestrator --thunderbolt
```
- Copy the new `deploy/worker.swift` to each worker and restart it (`Ctrl-C`, then `swift worker.swift`).

### Slow transfer or poor benchmark results

Checks:
- Verify Thunderbolt bridge MTU is 9000 on both Macs (wizard warns when non-jumbo MTU is detected).
- Ensure source-bind is actually using bridge subnets (re-run `--thunderbolt` and keep detected bridge IPs).
- Re-run benchmark sweep after fixing MTU/cabling:
```bash
swift run -c release orchestrator --thunderbolt
```

Expected behavior:
- Fallback is per-job: on remote failure, the same job immediately continues local processing (no manual requeue needed).
- Triggers include missing bridge route, connect timeout/unreachable worker, tick-v2 validation failure, worker error status, and SHA mismatch.
- If `TB_WORKERS` is configured but no `bridge*` IPv4 exists at startup, offload is disabled until restart.

---

## 5. Starting the System

### Prerequisites
1. External SSD mounted (at the path configured during setup)
2. WireGuard tunnel connected
   - Caddy binds to `__WG_MAC_IP__`; if WireGuard is down, Caddy will fail to start
3. If your network uses AdGuard/Pi-hole (or any local DNS override), flush the DNS cache for `__DOMAIN__` before starting services. Required on first deploy and whenever DNS records change (new VPS IP, recreated records, etc.). Keep AdGuard/Pi-hole enabled after flushing. **Note:** If you used the setup wizard (Section 4.7), this flush must happen before running the wizard — see Section 4.7.

### Prevent Sleep

Prevent the Mac from sleeping while services are running (required on event day).

If you use the Section 5 shortcut (`swift run -c release orchestrator --start`), launchd starts `caffeinate -s` for you.

Manual (dedicated Terminal window):
```bash
caffeinate -s
```
If using manual mode, leave this window open for the duration of the event. This prevents the Mac from sleeping while on AC power.

### Start Services

Verify WireGuard is connected:

```bash
ping -c 1 __WG_VPS_IP__
```

If this times out:
- Confirm the tunnel is active in the WireGuard app
- On the VPS: `sudo wg show` (expect a recent "latest handshake" for the Mac peer)
- Confirm UDP/51820 is allowed (provider firewall + UFW) and AllowedIPs are correct

> **DNS-01 timing note:** Caddy is configured with `propagation_delay 60s` and `propagation_timeout -1` (skip local TXT self-check). Local DNS cache usually does not block issuance, but first certificate attempts can still take about a minute and may retry if authoritative propagation is slow.
> **Resolver cache gate (AdGuard/Pi-hole with DNS hijack):** If DNS for `__DOMAIN__` changed since the last successful certificate issuance (new VPS IP, recreated records, etc.), clear DNS cache in AdGuard/Pi-hole before starting services. Keep AdGuard/Pi-hole enabled.

Load all three services:

```bash
launchctl enable gui/$(id -u)/com.kiko.caddy
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.caddy.plist
```
```bash
launchctl enable gui/$(id -u)/com.kiko.tusd
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.tusd.plist
```
```bash
launchctl enable gui/$(id -u)/com.kiko.media
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
```

Shortcut (loads all three in the correct order):
```bash
cd __REPO_DIR__
swift run -c release orchestrator --start
```

Verify all three are running:

```bash
launchctl list | grep kiko
```

Output columns: PID, LastExitStatus, Label. PID is `"-"` when not running. LastExitStatus is the most recent exit code (0 is clean, non-zero usually means it crashed).

### Verify Each Service

kiko-media health:

```bash
curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/health
```

Expected: `{"status":"ok"}`. This endpoint is not routed by Caddy. With default `BIND_ADDRESS=127.0.0.1`, it is loopback-only.

Gallery API (use the internal endpoint for testing):

```bash
curl http://localhost:8080/api/gallery
```

Expected: `{"assets":[...],"total":N}`. The public endpoint (`http://__BIND_ADDRESS__:__PUBLIC_PORT__/api/gallery`) returns 401 without a valid session cookie. Use the internal endpoint for verification.

tusd:

```bash
curl -X OPTIONS http://127.0.0.1:1080/files/ -H "Tus-Resumable: 1.0.0" -I 2>&1 | grep Tus
```

Expected: `Tus-Version` header in the output.

Caddy — on first start, Caddy obtains a TLS certificate via DNS-01 challenge. Watch for certificate status:

```bash
tail -f __BASE_DIRECTORY__/logs/caddy-run.log
```

Success: `"certificate obtained successfully","identifier":"__DOMAIN__"`
Failure: `"will retry"` with error details.

Once the certificate is obtained, verify (`-A` is required because the public Caddy config blocks curl/wget user agents):

```bash
curl -I https://__DOMAIN__:8443/ --resolve __DOMAIN__:8443:__WG_MAC_IP__ -A "Mozilla/5.0" 2>&1 | head -5
```

---

## 6. Moderation During Event

Open `http://localhost:8080` on the Mac to moderate. This is the internal-only site (Caddy binds to 127.0.0.1). It serves the same `index.html` but in internal mode: uploads disabled, moderation controls visible, Turnstile skipped.

Click an asset to open it, then toggle visibility. Hidden items are grayed with a **HIDDEN** badge and do not appear on the public gallery.

**Note:** Moderation only hides from the gallery. Direct `/api/thumbs/{id}` and `/api/preview/{id}` URLs remain accessible if the ID is known (acceptable under the link-secrecy model for the event window).

---

## 7. Testing

### 7.0 Automated Test Suite (Code)

Run the SwiftPM test suite before deploying any code changes. The commands below are the canonical workflow.

```bash
cd __REPO_DIR__
swift test --disable-xctest
```

**Note:** Tests use Swift Testing (`@Test`/`@Suite`), not XCTest. Without `--disable-xctest`, SwiftPM runs an empty XCTest pass first and prints a misleading "0 tests passed" line before the real results.

### 7.1 Test tusd Directly

> **Note:** tusd `pre-create` is session-gated. Cookie-less CLI creates should return `401`. For authenticated CLI tests, copy a fresh browser cookie using your configured `SESSION_COOKIE_NAME` (default: `kiko_session`).
>
> **Operator note (queue pressure):** If `post-finish` webhooks start returning `503 Service Unavailable` with `Retry-After`, webhook admission is backpressured (pending queue at `MAX_PENDING_WEBHOOK_JOBS` or processor shutdown). Tune `MAX_PENDING_WEBHOOK_JOBS` and `WEBHOOK_RETRY_AFTER_SECONDS` in advanced config. Canonical tuning guidance lives in [`docs/advanced-config.md`](advanced-config.md) (`Operational` + `Processing` sections).

```bash
# Cookie-less pre-create check (expected 401)
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://127.0.0.1:1080/files/ \
    -H "Tus-Resumable: 1.0.0" \
    -H "Upload-Length: 11" \
    -H "Upload-Metadata: filename dGVzdC50eHQ="
# Expected: 401

# Authenticated pre-create check (expected 201 + Location)
# If SESSION_COOKIE_NAME is custom, replace "kiko_session" below.
COOKIE='kiko_session=PASTE_VALUE_FROM_BROWSER'
curl -X POST http://127.0.0.1:1080/files/ \
    -H "Tus-Resumable: 1.0.0" \
    -H "Upload-Length: 11" \
    -H "Upload-Metadata: filename dGVzdC50eHQ=" \
    -H "Cookie: $COOKIE" \
    -D -
# Returns: Location: http://127.0.0.1:1080/files/{upload-id}
```

### 7.2 Local Upload Test

> **Note:** Turnstile session gating is required in production. Set `COOKIE='kiko_session=...'` first (reuse from Section 7.1; replace name if `SESSION_COOKIE_NAME` is custom), or the create step will return `401`.

```bash
# Create test image (1x1 red PNG, 70 bytes)
base64 -d <<< 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg==' > /tmp/test.png

# Create TUS upload
LOCATION=$(curl -s -X POST http://127.0.0.1:1080/files/ \
    -H "Tus-Resumable: 1.0.0" \
    -H "Upload-Length: 70" \
    -H "Upload-Metadata: filename dGVzdC5wbmc=,filetype aW1hZ2UvcG5n" \
    -H "Cookie: $COOKIE" \
    -D - | grep -i "^Location:" | tr -d '\r' | cut -d' ' -f2)

echo "Upload location: $LOCATION"
if [ -z "$LOCATION" ]; then
  echo "No Location header (likely missing/expired COOKIE; expected HTTP 201)"
  exit 1
fi

# Upload file content
curl -X PATCH "$LOCATION" \
    -H "Tus-Resumable: 1.0.0" \
    -H "Upload-Offset: 0" \
    -H "Content-Type: application/offset+octet-stream" \
    --data-binary @/tmp/test.png

# Check processing
log show --predicate 'subsystem == "com.kiko.media"' --last 1m
```

### 7.3 Verify Gallery

```bash
# Internal gallery (includes status) — use this for testing
curl http://localhost:8080/api/gallery
# Expected: {"assets":[...],"total":N}

# Public gallery — requires valid session cookie
curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/api/gallery
# Returns 401 Unauthorized without cookie (expected behavior)
# For unauthenticated checks, use the internal endpoint above.
```

### 7.4 Test Moderation

```bash
# Get an asset ID from the gallery
ID=$(curl -s http://localhost:8080/api/gallery | python3 -c "import sys,json; print(json.load(sys.stdin)['assets'][0]['id'])" 2>/dev/null)

# Hide it
curl -X PATCH "http://localhost:8080/api/assets/$ID" \
    -H "Content-Type: application/json" \
    -d '{"status":"moderated"}'
# Expected: 204 No Content

# Verify hidden from public gallery
# Public endpoint is session-gated.
# Verify via browser at https://__DOMAIN__/ after Turnstile verification.
# The internal endpoint (localhost:8080) still shows moderated assets with status="moderated"

# Unhide it
curl -X PATCH "http://localhost:8080/api/assets/$ID" \
    -H "Content-Type: application/json" \
    -d '{"status":"complete"}'
```

### 7.5 Test Public Access (requires VPS)

1. Ensure VPS running with HAProxy + WireGuard
2. Verify DNS: `dig __DOMAIN__`
3. Test from phone on cellular: `https://__DOMAIN__/`
   - If testing from the same home network, some routers block hairpin NAT. In that case,
     use local checks (`http://__BIND_ADDRESS__:__PUBLIC_PORT__/health`) or test from cellular.

### 7.6 Test Turnstile Verification (Session Gating)

Verify that session gating works correctly in production.

```bash
# 1. Confirm public endpoint rejects without cookie
curl -s -o /dev/null -w "%{http_code}" http://__BIND_ADDRESS__:__PUBLIC_PORT__/api/gallery
# Expected: 401

# 2. Confirm internal endpoint works (no gating)
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/gallery
# Expected: 200

# 3. Check logs confirm Turnstile is enabled
log show --predicate 'subsystem == "com.kiko.media"' --last 5m | grep -i turnstile
# Expected: "Turnstile protection enabled"
```

To test the full browser flow, open `https://__DOMAIN__/` on a phone (cellular) and verify:
1. If cookie is valid (refresh within your `SESSION_COOKIE_TTL` window, default 4 hours): gallery loads immediately, no modal shown
2. If no cookie or expired: modal appears, Turnstile runs (usually invisible), modal auto-dismisses after verification, gallery loads
3. Gallery loads successfully in both cases

---

## 8. Stopping the System

### Graceful Stop

```bash
# Stop in reverse order
launchctl stop com.kiko.media
launchctl stop com.kiko.tusd
launchctl stop com.kiko.caddy
```

Note: Upload files are preserved in `uploads/`. On restart, uploads with valid persisted metadata are re-queued; partial in-flight uploads remain for tusd to resume/complete.

### Transient Stop

Stop services for the current session. They will reload on next login via `RunAtLoad`.

Shortcut:
```bash
cd __REPO_DIR__
swift run -c release orchestrator --stop
```

Manual equivalent:
```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.tusd.plist
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.caddy.plist
```

### Persistent Disable

Stop services and prevent auto-start across reboots. Use this for OS updates or extended maintenance.

```bash
cd __REPO_DIR__
swift run -c release orchestrator --shutdown
```

Manual equivalent:
```bash
launchctl disable gui/$(id -u)/com.kiko.media
launchctl disable gui/$(id -u)/com.kiko.tusd
launchctl disable gui/$(id -u)/com.kiko.caddy
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.tusd.plist
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.caddy.plist
launchctl remove com.kiko.media.caffeinate
```

To re-enable after maintenance:
```bash
swift run -c release orchestrator --start
```

`--start` calls `launchctl enable` before `launchctl bootstrap` for each service it loads. If a service is still running but marked disabled after a partial/manual recovery flow, run `launchctl enable` for that service explicitly.

> **Semantic change (2026-04):** `--shutdown` previously performed a transient stop (equivalent to current `--stop`). It now persistently disables services across reboots. If you only need a temporary stop, use `--stop`.

### 8.1 Launchd Management (Single Service)

```bash
# Check status
launchctl list | grep kiko

# Stop a service
launchctl stop com.kiko.media

# Start a service
launchctl start com.kiko.media

# Restart a service (stop + start)
launchctl stop com.kiko.caddy && launchctl start com.kiko.caddy

# Force restart (kill + auto-restart via KeepAlive)
launchctl kickstart -k gui/$(id -u)/com.kiko.media

# Unload (stop for current session; reloads on next login)
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist

# Reload after editing a plist
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
launchctl enable gui/$(id -u)/com.kiko.media
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
```

---

## 9. Restart Scenarios

### 9.0 Quick Restart (Mac Reboot)

After a Mac reboot, services should auto-start if launchd plists are loaded. However, **Caddy depends on WireGuard** (it binds to `__WG_MAC_IP__`). If WireGuard is slow to connect, Caddy will fail.

**Verification checklist:**
```bash
# 1. Verify WireGuard connected first
ping -c 1 __WG_VPS_IP__
# If this fails, activate WireGuard manually in the app

# 2. Check all services running
launchctl list | grep kiko
# All three should show with PID (not "-")

# 3. If Caddy failed (shows "-" for PID), restart it after WireGuard is up
launchctl kickstart -k gui/$(id -u)/com.kiko.caddy

# 4. Health check
curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/health

# 5. Start sleep prevention (doesn't survive reboot)
# Wizard-managed: cd __REPO_DIR__ && swift run -c release orchestrator --start
# Manual: caffeinate -s
```

**If services didn't auto-start:** They may be disabled or not loaded. Run:
```bash
launchctl enable gui/$(id -u)/com.kiko.caddy
launchctl enable gui/$(id -u)/com.kiko.tusd
launchctl enable gui/$(id -u)/com.kiko.media
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.caddy.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.tusd.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
```

### 9.0.1 Quick Restart (VPS Reboot)

If the VPS rebooted (but wasn't destroyed), WireGuard and HAProxy should auto-start via systemd.

**Verification (from Mac):**
```bash
# 1. Check tunnel works
ping -c 3 __WG_VPS_IP__

# 2. If tunnel is down, SSH to VPS and verify services
ssh __VPS_USER__@<VPS_IP>
sudo systemctl status wg-quick@wg0
sudo systemctl status haproxy
sudo ss -tlnp | grep 443
```

If services didn't start, run:
```bash
sudo systemctl start wg-quick@wg0
sudo systemctl start haproxy
```

---

### 9.1 Restarting After Long Downtime

If the VPS was destroyed or the system was off for months:

### 9.1.1 Verify Mac State

```bash
# Check services not running
launchctl list | grep kiko

# Check data exists
ls __BASE_DIRECTORY__/
ls __EXTERNAL_SSD_PATH__/ | head

# Check database
sqlite3 __BASE_DIRECTORY__/metadata.db "SELECT COUNT(*) FROM assets;"
```

### 9.1.2 Provision New VPS (if needed)

Follow **Section 2 (VPS Setup)** steps 2.1 through 2.12 to:
1. Create new Ubuntu VPS (2.1)
2. Configure cloud firewall (2.2)
3. Initial setup and packages (2.3-2.4)
4. SSH hardening (2.5)
5. UFW, WireGuard, HAProxy (2.6-2.9)
6. Performance tuning (2.10)
7. Start services (2.11)
8. Verify VPS setup (2.12)

**Important:** Record the new VPS public IP and WireGuard public key for the next steps.
If you revoked your Cloudflare API token during teardown, recreate it in Section 3.1 and re-run Section 4.7 so the new token is written into the Caddy LaunchAgent plist.

### 9.1.3 Update Mac WireGuard Config

In WireGuard app:
1. Select `vps-tunnel`
2. Edit → Update Peer section:
   - `PublicKey` → new VPS public key
   - `Endpoint` → new VPS IP:51820
3. Save and Activate

### 9.1.4 Update VPS WireGuard Config

On VPS:
```bash
sudo nano /etc/wireguard/wg0.conf
# Ensure the [Peer] block for the Mac uses the current Mac public key and __WG_MAC_IP__/32
# Remove/replace stale duplicate Mac peer blocks if present
sudo systemctl restart wg-quick@wg0
```

### 9.1.5 Verify Tunnel

```bash
# From Mac
ping -c 3 __WG_VPS_IP__

# From VPS
ping -c 3 __WG_MAC_IP__
```

### 9.1.6 Update DNS (if VPS IP changed)

In Cloudflare:
1. Update A record for `__DOMAIN__` to new VPS IP
2. If you publish AAAA, update AAAA for `__DOMAIN__` to the new VPS IPv6
3. Keep records as DNS-only (grey cloud)
4. If your network enforces DNS through AdGuard/Pi-hole, clear DNS cache before starting services.

### 9.1.7 Start Services

Preferred:
```bash
cd __REPO_DIR__
swift run -c release orchestrator --start
```

Manual fallback:
```bash
launchctl enable gui/$(id -u)/com.kiko.caddy
launchctl enable gui/$(id -u)/com.kiko.tusd
launchctl enable gui/$(id -u)/com.kiko.media
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.caddy.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.tusd.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
```

Caddy will automatically obtain a new TLS certificate via DNS-01 challenge.

---

## 10. Break-Glass Procedures

Quick fixes for common failures during the event.

| Failure | Symptom | Quick Fix |
|---------|---------|-----------|
| VPS down | Site unreachable, `ping <VPS_IP>` fails | Spin up new VPS (Section 2), update DNS |
| WireGuard down | Site unreachable, VPS fine | Reactivate tunnel in WireGuard app |
| Caddy down | Connection refused on :8443 | `launchctl kickstart -k gui/$(id -u)/com.kiko.caddy` |
| tusd down | Uploads fail at /files/ | `launchctl kickstart -k gui/$(id -u)/com.kiko.tusd` |
| kiko-media down | Gallery empty, health check fails | `launchctl kickstart -k gui/$(id -u)/com.kiko.media` |
| SSD disconnected | Originals not archiving, jobs stuck | Reconnect SSD, restart kiko-media |
| Certificate expired | TLS errors in browser | `rm -rf ~/Library/Application\ Support/Caddy/certificates/` then restart Caddy |
| Certificate issuance failed | Caddy logs "timed out waiting for record to propagate" | Flush local DNS cache (AdGuard/Pi-hole), then follow Section 11 "Caddy Can't Get Certificate" |
| Processing stuck | Assets stay queued indefinitely | Check logs (`log stream --predicate 'subsystem == "com.kiko.media"'`), restart kiko-media |
| Disk full | Uploads fail, write errors | `df -h __BASE_DIRECTORY__`, clean uploads/ or old logs |

---

## 11. Detailed Troubleshooting

### Viewing Logs

```bash
# kiko-media uses OSLog (system log)
log stream --predicate 'subsystem == "com.kiko.media"' --level info

# Historical kiko-media logs
log show --predicate 'subsystem == "com.kiko.media"' --last 1h

# kiko-media stdout/stderr (captured by launchd)
tail -f __BASE_DIRECTORY__/logs/kiko-media.log

# tusd logs to a file
tail -f __BASE_DIRECTORY__/logs/tusd.log

# Caddy process logs (stdout/stderr captured by launchd) and HTTP access logs
tail -f __BASE_DIRECTORY__/logs/caddy-run.log
tail -f __BASE_DIRECTORY__/logs/caddy-access.log

# Recent errors (all services)
grep -i error __BASE_DIRECTORY__/logs/*.log | tail -20
```

### Service Won't Start

When running `KikoMedia` manually for debugging, export the same env values from `~/Library/LaunchAgents/com.kiko.media.plist` (especially `TURNSTILE_*` and `SESSION_HMAC_SECRET`) or startup will fail by design.

```bash
# Check launchd error code
launchctl error <code>

# Run manually to see errors
~/bin/caddy run --config __REPO_DIR__/deploy/Caddyfile
~/bin/tusd -upload-dir __BASE_DIRECTORY__/uploads -port 1080
__REPO_DIR__/.build/release/KikoMedia
```

### Permission Denied

```bash
# Ensure binaries are executable
chmod +x ~/bin/caddy
chmod +x ~/bin/tusd
chmod +x __REPO_DIR__/.build/release/KikoMedia
```

### Caddy Can't Get Certificate

1. If your network uses AdGuard/Pi-hole (or any local DNS override), flush the DNS cache for `__DOMAIN__` **first**. Without this, clearing certificates will not help — Caddy will fail again for the same reason.

2. Then clear certificate state and restart:

```bash
launchctl stop com.kiko.caddy
rm -rf ~/Library/Application\ Support/Caddy/certificates/
launchctl start com.kiko.caddy
```

3. Watch for success:

```bash
tail -f __BASE_DIRECTORY__/logs/caddy-run.log
# Success: "certificate obtained successfully","identifier":"__DOMAIN__"
# Failure: "will retry" with error details
```

4. If it still fails, verify the Cloudflare API token:

```bash
# Verify token in plist
grep -A1 CLOUDFLARE ~/Library/LaunchAgents/com.kiko.caddy.plist

# Test manually
CLOUDFLARE_API_TOKEN=xxx ~/bin/caddy run --config __REPO_DIR__/deploy/Caddyfile
```

### Uploads Fail

```bash
# Check tusd logs
tail -f __BASE_DIRECTORY__/logs/tusd.log

# Check webhook received
log stream --predicate 'subsystem == "com.kiko.media" AND eventMessage CONTAINS "Webhook"' --level info
```

### Gallery Empty After Uploads

```bash
# Check processing status
log stream --predicate 'subsystem == "com.kiko.media"' --level info

# Check database directly
sqlite3 __BASE_DIRECTORY__/metadata.db "SELECT id, status FROM assets ORDER BY createdAt DESC LIMIT 10;"
```

### Failed Jobs (status=failed)

1. Identify failures:
```bash
sqlite3 __BASE_DIRECTORY__/metadata.db "SELECT id, originalName, status FROM assets WHERE status='failed' ORDER BY createdAt DESC LIMIT 20;"
```
2. Check logs to determine cause (thumbnail/preview failure, video transcode, SSD write, checksum mismatch).
3. If the original upload file still exists in `uploads/` and you want to retry:
   - Fix the root cause (e.g., remount SSD, free disk space).
   - Manually set status back to `queued`, then restart the service to re-queue:
```bash
sqlite3 __BASE_DIRECTORY__/metadata.db "UPDATE assets SET status='queued' WHERE id='<upload-id>';"
launchctl stop com.kiko.media && launchctl start com.kiko.media
```
4. If the upload file is gone, re-upload the original file (new ID will be created).

### Site Unreachable

```bash
# 1. Check services
launchctl list | grep kiko

# 2. Check kiko-media health
curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/health

# 3. Check WireGuard tunnel
ping -c 1 __WG_VPS_IP__

# 4. Check VPS HAProxy
ssh __VPS_USER__@<VPS_IP> "sudo ss -tlnp | grep 443"

# 5. Check DNS
dig __DOMAIN__ A +short
```

### External SSD Not Detected

```bash
# Check mount
df -h __EXTERNAL_SSD_PATH__

# Check it's a real volume (not empty directory)
ls -la /Volumes/

# Note: EXTERNAL_SSD_PATH must be a /Volumes/<VolumeName>/... path (mount detection is strict).

# Processing will pause if SSD unavailable
# Jobs stay in "processing" and will retry when SSD is mounted and the service restarts
```

### Uploads Stall Partway Through

Reduce WireGuard MTU on both sides:

**On Mac** (in WireGuard app):
- Edit `vps-tunnel`
- Change `MTU = 1380` to `MTU = 1280`
- Save and reactivate

**On VPS:**
```bash
sudo nano /etc/wireguard/wg0.conf
# Add under [Interface]: MTU = 1280
sudo systemctl restart wg-quick@wg0
```

### Slow Upload Speeds

1. **Check VPS BBR is enabled:**
   ```bash
   sysctl net.ipv4.tcp_congestion_control
   # Should show: bbr
   ```

2. **Check WireGuard tunnel health:**
   ```bash
   # On Mac
   ping -c 10 __WG_VPS_IP__
   # Look for packet loss or high latency
   ```

### Certificate Renewal Failed

```bash
# Check Caddy logs on Mac
tail -100 __BASE_DIRECTORY__/logs/caddy-run.log | grep -i cert

# Force certificate renewal
launchctl stop com.kiko.caddy
rm -rf ~/Library/Application\ Support/Caddy/certificates/
launchctl start com.kiko.caddy

# Watch for renewal
tail -f __BASE_DIRECTORY__/logs/caddy-run.log
```

### Turnstile Verification Failures

**Symptoms:** Users see "Unable to verify" in the modal, or uploads are rejected.

**Check configuration:**
```bash
# Verify Turnstile env vars are set in launchd plist
grep -A4 TURNSTILE ~/Library/LaunchAgents/com.kiko.media.plist
grep -A2 SESSION_HMAC ~/Library/LaunchAgents/com.kiko.media.plist

# Check kiko-media logs for Turnstile status
log show --predicate 'subsystem == "com.kiko.media"' --last 5m | grep -i turnstile
```

**Common causes:**

| Issue | Symptom | Fix |
|-------|---------|-----|
| Wrong sitekey | Widget shows error | Re-run the setup wizard with the correct site key (updates `deploy/index.html` + CSP hashes), restart Caddy |
| Wrong secret | Server logs "verification failed" | Update `TURNSTILE_SECRET` in launchd plist, restart |
| Missing/invalid Turnstile env vars | Service fails startup with "Refusing startup..." | Set `TURNSTILE_SECRET`, `SESSION_HMAC_SECRET` (>=32 bytes), `TURNSTILE_EXPECTED_HOSTNAME`, `TURNSTILE_EXPECTED_ACTION`, and `TURNSTILE_EXPECTED_CDATA`; restart |
| Hostname/action/cdata mismatch | Server logs mismatch and verify returns 403 | Ensure `TURNSTILE_EXPECTED_HOSTNAME` matches your public host (hostname only, no `https://`, no path) and `TURNSTILE_EXPECTED_ACTION`/`TURNSTILE_EXPECTED_CDATA` match the public widget values in generated `deploy/index.html`; rerun setup wizard and restart if needed |
| CSP mismatch | Console error "Refused to load script" | Run `swift scripts/regen-frontend.swift` to regenerate `deploy/index.html` and matching CSP hashes. Use the full wizard instead if the underlying config also changed. |
| Script blocked | Widget never loads | Ad blocker or network blocking `challenges.cloudflare.com` |
| Siteverify timeout | Server logs "verification error" | Check outbound HTTPS from Mac, retry |

**Debugging without public-session auth:**
```bash
# Internal endpoints do not require the public session cookie:
curl http://localhost:8080/api/gallery
```

---

## 12. Monitoring

### Disk Usage

```bash
du -sh __BASE_DIRECTORY__/*
df -h __EXTERNAL_SSD_PATH__
```

### Processing Queue

```bash
# Check for stuck jobs
sqlite3 __BASE_DIRECTORY__/metadata.db "SELECT id, status FROM assets WHERE status IN ('queued', 'processing');"

# Check failed jobs
sqlite3 __BASE_DIRECTORY__/metadata.db "SELECT id, originalName FROM assets WHERE status='failed';"
```

### Asset Counts

```bash
sqlite3 __BASE_DIRECTORY__/metadata.db "SELECT status, COUNT(*) FROM assets GROUP BY status;"
```

---

## 13. Pre-Event Checklist

- [ ] External SSD connected and mounted at configured path
- [ ] `df -h __EXTERNAL_SSD_PATH__` shows adequate free space
- [ ] All three services running: `launchctl list | grep kiko`
- [ ] Sleep prevention active: `caffeinate -s` running (wizard-managed or manual)
- [ ] Health check passes: `curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/health`
- [ ] Internal moderation UI loads: `http://localhost:8080`
- [ ] WireGuard connected: `ping __WG_VPS_IP__`
- [ ] VPS running and DNS resolves
- [ ] Turnstile site key is present in generated `deploy/index.html` (wizard output)
- [ ] Test Turnstile verification from phone on cellular (first visit: modal → auto-dismiss → gallery; refresh within `SESSION_COOKIE_TTL`, default 4 hours: gallery loads immediately)
- [ ] Test Turnstile interaction path (temporarily swap in test sitekey `3x00000000000000000000FF`, verify checkbox appears, then **restore your real sitekey**; changing the sitekey changes the inline script, so re-run the wizard and restart Caddy to keep CSP hashes valid)
- [ ] Test upload from phone on cellular
- [ ] Update `deploy/tus.min.js` if needed (note: cached 24h, update day before event):
      `curl -sL "https://cdn.jsdelivr.net/npm/tus-js-client@latest/dist/tus.min.js" | sed '/sourceMappingURL/d' > deploy/tus.min.js`

---

## 14. Emergency Commands

### Force Restart All Services
```bash
launchctl kickstart -k gui/$(id -u)/com.kiko.caddy
launchctl kickstart -k gui/$(id -u)/com.kiko.tusd
launchctl kickstart -k gui/$(id -u)/com.kiko.media
```

### Kill Stuck Process
```bash
pkill -f benchmark
pkill -f KikoMedia
pkill -f tusd
pkill -f caddy
```

### Rebuild Binary
```bash
cd __REPO_DIR__
swift build -c release
launchctl stop com.kiko.media
launchctl start com.kiko.media
```

### Database Backup
```bash
# Safe SQLite backup (WAL-aware)
sqlite3 __BASE_DIRECTORY__/metadata.db ".backup '__BASE_DIRECTORY__/metadata-backup-$(date +%Y%m%d).db'"

# If you must copy files directly, include WAL + SHM:
# cp __BASE_DIRECTORY__/metadata.db* __BASE_DIRECTORY__/metadata-backup-$(date +%Y%m%d)/
```

### Database Recovery (Automatic)

The system automatically detects and recovers from database corruption or loss:

1. **On startup**, kiko-media checks:
   - If `metadata.db` exists
   - If it can be opened without errors
   - If it passes `PRAGMA quick_check`
   - If SSD file count exceeds DB record count (indicating missing records)

2. **If any check fails**:
   - Corrupt DB files are moved aside with timestamp (e.g., `metadata.db.corrupt-2026-02-05T14-30-00Z`)
   - Fresh DB is created
   - All files on SSD are scanned and re-indexed

3. **Moderation is preserved**:
   - Moderation state is stored in `__BASE_DIRECTORY__/moderated/` as marker files
   - On rebuild, previously moderated assets remain moderated
   - Non-moderated assets are restored as `complete` (visible)

4. **Runtime watchdog**:
   - Every `HEALTH_CHECK_INTERVAL` seconds (default: 60), the system verifies DB connectivity
   - If the check fails, the system triggers graceful shutdown (drains in-flight requests, then exits)
   - launchd restarts the process, triggering recovery

**Manual recovery** (if needed):
```bash
# Force restart to trigger recovery
launchctl kickstart -k gui/$(id -u)/com.kiko.media

# Check recovery logs
log show --predicate 'subsystem == "com.kiko.media"' --last 5m | grep -i rebuild
```

**Edge case**: If SSD is not mounted when recovery is needed, the system exits with an error. Remount SSD and restart.

---

## 15. Security Checklist

Verify before event day:

**Cloudflare:**
- [ ] A record points to VPS IPv4, **grey cloud** (DNS only)
- [ ] AAAA record points to VPS IPv6, **grey cloud** (or removed if IPv6 broken)

**VPS:**
- [ ] Cloud firewall attached (only ports 22, 443, 51820)
- [ ] UFW enabled (`sudo ufw status`)
- [ ] SSH: password auth disabled, root login disabled
- [ ] HAProxy enabled for auto-start (`systemctl is-enabled haproxy`)
- [ ] WireGuard enabled for auto-start (`systemctl is-enabled wg-quick@wg0`)
- [ ] BBR congestion control enabled

**Host Mac:**
- [ ] FileVault enabled (`fdesetup status`)
- [ ] All three services running (`launchctl list | grep kiko`)
- [ ] WireGuard tunnel active (in app)
- [ ] Health check passes (`curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/health`)
- [ ] `TURNSTILE_SECRET` configured in `com.kiko.media.plist`
- [ ] `SESSION_HMAC_SECRET` configured in `com.kiko.media.plist`
- [ ] `TURNSTILE_EXPECTED_HOSTNAME` / `TURNSTILE_EXPECTED_ACTION` / `TURNSTILE_EXPECTED_CDATA` configured in `com.kiko.media.plist`
- [ ] Logs confirm "Turnstile protection enabled" on startup

**Testing:**
- [ ] Upload from phone on cellular works
- [ ] Large upload (100MB+ video) completes
- [ ] Gallery shows uploaded assets
- [ ] Pause/resume works (start upload, force-close browser, reopen)

**If you edit `deploy/index.html.template`:** Keep exactly one inline `<style>` tag and one inline `<script>` tag in the template. The frontend regen path computes CSP hashes by expecting exactly one of each; 0 or >1 will fail regeneration. After any edits, run `swift scripts/regen-frontend.swift`. It regenerates `deploy/index.html`, updates matching CSP hashes in `deploy/Caddyfile`, and reloads Caddy only if the Caddyfile changed. Use the full setup wizard instead when you need to change sitekey, secrets, domain, bind IP, or Turnstile action/cdata values.

Optional verification (debugging):
```bash
cd __REPO_DIR__/deploy
python3 -c "
import re, hashlib, base64
html = open('index.html').read()
for tag in ['style', 'script']:
    match = re.search(f'<{tag}>(.*?)</{tag}>', html, re.DOTALL)
    if match:
        h = base64.b64encode(hashlib.sha256(match.group(1).encode()).digest()).decode()
        print(f'{tag}: sha256-{h}')
"
grep -n "Content-Security-Policy" Caddyfile
```

---

## 16. Post-Event Cleanup

### Backup Data

```bash
# On Mac - backup database and metadata
sqlite3 __BASE_DIRECTORY__/metadata.db ".backup '__BASE_DIRECTORY__/metadata-backup-$(date +%Y%m%d).db'"

# Verify originals are on SSD
ls -la __EXTERNAL_SSD_PATH__/ | head -20
df -h __EXTERNAL_SSD_PATH__
```

### Delete VPS

1. In your VPS provider's dashboard, select your instance and delete it
2. Confirm deletion

This stops all billing immediately.

### Revoke Cloudflare API Token

1. **Cloudflare Dashboard** → **Profile** → **API Tokens**
2. Find your Caddy DNS token
3. Click **...** → **Delete**

### Remove DNS Records (Optional)

1. **Cloudflare Dashboard** → `__BASE_DOMAIN__` → **DNS**
2. Delete the `photos` A record
3. Delete the `photos` AAAA record (if exists)

---

## 17. Complete Removal

To completely remove kiko-media from a Mac:

### 17.1 Stop Services

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.media.plist
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.tusd.plist
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.kiko.caddy.plist

# Remove wizard-managed sleep prevention job (if present)
launchctl remove com.kiko.media.caffeinate 2>/dev/null || true
```

### 17.2 Remove launchd Plists

```bash
rm ~/Library/LaunchAgents/com.kiko.caddy.plist
rm ~/Library/LaunchAgents/com.kiko.tusd.plist
rm ~/Library/LaunchAgents/com.kiko.media.plist
```

### 17.3 Remove Binaries

```bash
rm ~/bin/caddy
rm ~/bin/tusd
rm -rf __REPO_DIR__  # Entire repo including built binary
```

### 17.4 Remove Data (DESTRUCTIVE)

```bash
# WARNING: This deletes all photos, videos, and database
rm -rf __BASE_DIRECTORY__

# WARNING: This deletes all archived originals.
# Ensure this points to your event archive directory, not the SSD root.
rm -rf __EXTERNAL_SSD_PATH__
```

### 17.5 Remove Caddy State

```bash
rm -rf ~/Library/Application\ Support/Caddy
```

### 17.6 Deactivate WireGuard

In WireGuard app: Deactivate and delete the `vps-tunnel` configuration.

### 17.7 Revoke Cloudflare Token

In Cloudflare Dashboard: Profile → API Tokens → Delete the Caddy DNS token.

---

## Known Limitations

**Client IP Masking:** Because ingress passes through HAProxy TCP passthrough + WireGuard, Caddy sees tunnel-side source addresses (not guest WAN IPs). `kiko-media` receives traffic proxied from Caddy, so app-layer client IP is not the original guest IP. For a single-day event with share links, this is acceptable.

**Moderation is Gallery-Only:** Hiding an asset removes it from the public gallery but does not revoke direct access to `/api/thumbs/{id}` or `/api/preview/{id}` if the ID is already known. This is acceptable under the link-secrecy model for the event window.

**NAT Hairpin:** If you test `https://__DOMAIN__` from the same home network where the Mac runs, traffic goes: Laptop → Internet → VPS → WireGuard → Mac. Most routers support this "hairpin NAT," but some don't. For testing at home, use direct local access (`curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/health`) or test from cellular.

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         KIKO-MEDIA QUICK REFERENCE                       │
├─────────────────────────────────────────────────────────────────────────┤
│  HEALTH CHECK                                                            │
│    curl http://__BIND_ADDRESS__:__PUBLIC_PORT__/health                    │
│                                                                          │
│  SERVICE STATUS                                                          │
│    launchctl list | egrep 'com\\.kiko\\.(caddy|tusd|media)$'             │
│                                                                          │
│  VIEW LOGS                                                               │
│    log stream --predicate 'subsystem == "com.kiko.media"' --level info   │
│    log show --predicate 'subsystem == "com.kiko.media"' --last 1h        │
│    tail -f __BASE_DIRECTORY__/logs/kiko-media.log                    │
│    tail -f __BASE_DIRECTORY__/logs/*.log                             │
│    grep -i error __BASE_DIRECTORY__/logs/*.log | tail -20            │
│                                                                          │
│  RESTART SERVICES                                                        │
│    launchctl kickstart -k gui/$(id -u)/com.kiko.caddy                    │
│    launchctl kickstart -k gui/$(id -u)/com.kiko.tusd                     │
│    launchctl kickstart -k gui/$(id -u)/com.kiko.media                    │
│                                                                          │
│  CHECK DISK SPACE                                                        │
│    df -h __BASE_DIRECTORY__ __EXTERNAL_SSD_PATH__                         │
│    du -sh __BASE_DIRECTORY__/*                                       │
│                                                                          │
│  ASSET STATUS COUNTS                                                     │
│    sqlite3 __BASE_DIRECTORY__/metadata.db \                          │
│      "SELECT status, COUNT(*) FROM assets GROUP BY status;"              │
│                                                                          │
│  TEST WIREGUARD TUNNEL                                                   │
│    ping -c 1 __WG_VPS_IP__                                                   │
│                                                                          │
│  FORCE CERTIFICATE RENEWAL                                               │
│    rm -rf ~/Library/Application\ Support/Caddy/certificates/             │
│    launchctl kickstart -k gui/$(id -u)/com.kiko.caddy                    │
│                                                                          │
│  CHECK TURNSTILE STATUS                                                  │
│    log show --predicate 'subsystem == "com.kiko.media"' --last 5m \      │
│      | grep -i turnstile                                                 │
│                                                                          │
│  TEST GALLERY (use internal endpoint)                                    │
│    curl http://localhost:8080/api/gallery                                │
│                                                                          │
│  MODERATION UI                                                           │
│    http://localhost:8080                                                 │
└─────────────────────────────────────────────────────────────────────────┘
```

---

*Runbook maintained manually. Re-verify against code after major refactors.*
