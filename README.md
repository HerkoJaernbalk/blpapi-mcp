# blpapi-mcp-chatgpt

Bloomberg MCP tooling split into two roles:

- Worker: runs close to Bloomberg Terminal and executes Bloomberg API calls.
- Gateway: exposes a ChatGPT-compatible `/mcp` endpoint over HTTP(S), adds auth, logging, and policy enforcement, then forwards to the worker.

## Architecture

```text
ChatGPT Desktop / Remote MCP Client
        |
        | HTTPS (Bearer auth, policy, logs)
        v
Gateway (this repo: blpapi-gateway-chatgpt)
        |
        | Private network / tunnel
        v
Worker (this repo: blpapi-worker-chatgpt)
        |
        v
Bloomberg Terminal + BBComm (localhost:8194)
```

## What Changed

- Existing Bloomberg tool logic remains in the worker (`src/blpapi_mcp/blp_mcp_server.py`).
- New gateway service at `src/blpapi_mcp/gateway.py`:
  - exposes `POST /mcp`
  - validates bearer auth before forwarding
  - logs requests and forwarding errors
  - enforces tool allowlist and argument limits
  - forwards responses in streaming HTTP mode
- Worker HTTP default host is now `127.0.0.1` (safer default than `0.0.0.0`).

## Tool Policy (Gateway Default)

Initially allowed:

- `bdp`
- `bds`
- `bdh`
- `instruments`
- `curve_list`
- `govt_list`
- `earning`
- `dividend`

Initially blocked unless feature-flagged:

- `bql`
- `bdtick`
- `beqs`

Enable high-risk tools by setting:

```bash
MCP_GATEWAY_ENABLE_HIGH_RISK_TOOLS=true
```

## Requirements

- Bloomberg Terminal running and logged in (worker machine)
- Python 3.12 recommended (project supports `>=3.10,<3.13`)
- `uv` or another Python package manager

## Install

```bash
uv sync
```

or install as a tool:

```bash
uv tool install "git+https://github.com/HerkoJaernbalk/blpapi-mcp@chatgpt" \
  --extra-index-url https://blpapi.bloomberg.com/repository/releases/python/simple/
```

## Run Worker (Office Machine)

Stdio mode (local MCP process):

```bash
blpapi-worker-chatgpt
```

HTTP mode (private listener):

```bash
blpapi-worker-chatgpt --http --host 127.0.0.1 --port 8080
```

If needed for LAN-only exposure, bind to a private IP explicitly:

```bash
blpapi-worker-chatgpt --http --host 192.168.1.50 --port 8080
```

## Run Gateway

Copy `.env.gateway.example` to `.env` (or export vars) and set at least:

- `WORKER_MCP_URL`
- `MCP_GATEWAY_TOKENS`

Then run:

```bash
blpapi-gateway-chatgpt --host 0.0.0.0 --port 8443
```

Health endpoint:

```text
GET /healthz
```

MCP endpoint:

```text
POST /mcp
Authorization: Bearer <token>
```

## Client Connection (ChatGPT-Compatible MCP)

Point the client to the gateway URL, for example:

```text
https://mcp.example.com/mcp
```

Send bearer auth using one of `MCP_GATEWAY_TOKENS`.

## Deployment Patterns

### Pattern A: Reverse Proxy TLS Termination

Run gateway behind Nginx/Caddy/Traefik:

- Internet-facing proxy terminates HTTPS.
- Proxy forwards `/mcp` to `http://127.0.0.1:8443/mcp`.
- Gateway forwards to worker over private route/tunnel.

### Pattern B: Private Tunnel Back To Office

Keep worker private, reachable only through a secure tunnel:

- Gateway in cloud/VPS.
- Tunnel from gateway to office worker (for example WireGuard, Tailscale, or SSH reverse tunnel).
- Set `WORKER_MCP_URL` to tunnel/private endpoint.

Full copy-paste examples are in `DEPLOYMENT.md`.

## Config Examples

See:

- `.env.worker.example`
- `.env.gateway.example`

## Remaining Auth Integration Steps

Current gateway auth is static bearer token validation. For production hardening, next steps:

1. Integrate an identity provider (OIDC/JWT verification with key rotation).
2. Add token scopes/claims mapped to per-tool policy.
3. Add rate limits and abuse detection (per token/IP).
4. Add structured audit logs to SIEM (request id, principal, tool, outcome).
5. Add secret management (Vault/KMS) instead of plain env tokens.

## Notes

- Bloomberg APIs still execute only on the worker side.
- Gateway policy enforcement is request-level (`tools/call`) and tool discovery filtering (`tools/list`).
- This package is intentionally renamed so it can coexist with your existing `blpapi-mcp` tool install.
