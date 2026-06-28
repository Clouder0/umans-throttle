# Server Deployment Guide

## Prerequisites

- A Linux server with public or private IP accessible from your coding agents
- Rust toolchain (for building) or a release binary transferred to the server
- Root/sudo access for systemd installation

## 1. Build

```bash
# On the server (or cross-compile and copy the binary)
git clone <repo-url> umans-throttle
cd umans-throttle
cargo build --release
# Binary: target/release/umans-throttle (5.8MB, statically linked TLS via rustls)
```

## 2. Install binary + config

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin umans-throttle

sudo mkdir -p /usr/local/bin /etc/umans-throttle /var/lib/umans-throttle
sudo cp target/release/umans-throttle /usr/local/bin/
sudo cp config.example.toml /etc/umans-throttle/config.toml
sudo chown -R umans-throttle:umans-throttle /etc/umans-throttle /var/lib/umans-throttle
```

## 3. Configure

Edit `/etc/umans-throttle/config.toml`:

```toml
[upstream]
base_url = "https://api.code.umans.ai"

[server]
# Bind to all interfaces so remote agents can connect.
# Use 0.0.0.0 for public, or a private IP for restricted access.
listen = "0.0.0.0:8080"
shutdown_timeout = "30s"

[throttle]
# Set to your Umans plan's concurrency limit.
# Code Max = 4, Code Pro = 3.
max_in_flight = 4
max_wait = "5m"
idle_timeout = "2m"
release_grace = "250ms"

[observability]
usage_sampling = "on_429"
```

**Important:** `max_in_flight` must match your Umans plan. Setting it higher
than your plan's concurrency limit defeats the purpose (you'll get 429s).
Setting it to 0 is rejected at startup.

## 4. Install systemd service

```bash
sudo cp deploy/umans-throttle.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable umans-throttle
sudo systemctl start umans-throttle

# Check status
sudo systemctl status umans-throttle
sudo journalctl -u umans-throttle -f   # follow logs
```

## 5. Firewall

```bash
# Allow inbound to the proxy port (adjust for your firewall)
sudo ufw allow 8080/tcp

# Or if using iptables/nftables directly:
# sudo iptables -A INPUT -p tcp --dport 8080 -j ACCEPT
```

**Security recommendation:** If this is on a public IP, put it behind a
reverse proxy (nginx/Caddy) with TLS, or restrict to SSH tunnel access:

```bash
# From your dev machine, tunnel to the remote proxy:
ssh -L 8080:127.0.0.1:8080 user@your-server
# Then use http://127.0.0.1:8080 locally — traffic is encrypted via SSH
```

## 6. Point your agents at it

```bash
# Claude Code
export ANTHROPIC_BASE_URL=http://your-server-ip:8080
export ANTHROPIC_AUTH_TOKEN=sk-your-umans-api-key
claude --model umans-coder

# Or any tool that supports a custom base URL + API key
```

The proxy forwards `x-api-key` / `Authorization` headers as-is — it stores no keys.

## Operations

```bash
# Restart (drains in-flight up to shutdown_timeout, then force-exits)
sudo systemctl restart umans-throttle

# Reload config (no hot-reload yet — restart required)
sudo systemctl restart umans-throttle

# View logs
sudo journalctl -u umans-throttle --since "10 min ago"

# Check live metrics (if RUST_LOG=debug)
sudo journalctl -u umans-throttle -f | grep "permit"
```

## Troubleshooting

| Symptom | Check |
|---|---|
| All requests 503 | `max_in_flight` not 0? Upstream reachable? `journalctl` for errors |
| Requests hang | `idle_timeout` too short? Upstream slow? Check `journalctl` for idle timeout warnings |
| 502 Bad Gateway | Upstream down or unreachable. Test `curl https://api.code.umans.ai/v1/messages` from server |
| Connection refused | Service running? `systemctl status`, firewall open? |
