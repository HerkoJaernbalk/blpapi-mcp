# Tailscale Funnel Plan

## Goal

Expose the Bloomberg MCP server (running on the Bloomberg Terminal machine at port 8080)
to the public internet via Tailscale Funnel, so Claude Desktop's remote MCP connector
can reach it from Anthropic's cloud.

## Why Tailscale Funnel

- Tailscale Funnel makes a local port publicly accessible via a stable `https://<machine>.ts.net` URL
- No router config, no firewall changes, no dynamic DNS
- HTTPS is handled automatically (required by Claude Desktop connectors)
- Free tier is sufficient

## Steps

### 1. Install Tailscale on the Bloomberg machine

Download and install from https://tailscale.com/download
Log in with a Tailscale account (free tier works).

### 2. Enable Funnel

Tailscale Funnel must be enabled in the Tailscale admin console:
- Go to https://login.tailscale.com/admin/dns
- Enable "HTTPS Certificates" 
- Go to https://login.tailscale.com/admin/acls
- Add Funnel to the ACL policy (Tailscale will prompt you if not enabled)

### 3. Start the Bloomberg MCP server

```bash
blpapi-mcp --http
```

Confirm it says: `Bloomberg MCP server listening on http://0.0.0.0:8080/mcp`

### 4. Start Tailscale Funnel

```bash
tailscale funnel 8080
```

Tailscale will output a public URL like:
```
https://<machine-name>.tail1234.ts.net
```

The MCP endpoint will be:
```
https://<machine-name>.tail1234.ts.net/mcp
```

### 5. Add to Claude Desktop

In Claude Desktop → Customize → Connectors → "+" → Add custom connector:

```
https://<machine-name>.tail1234.ts.net/mcp
```

No OAuth needed.

### 6. Verify

In Claude Desktop chat, click "+" → Connectors — bloomberg should appear with its tools listed.

## Keeping it running

Tailscale Funnel runs as long as the `tailscale funnel` command is active.
To make it persistent across reboots on Windows, run as a background service or add to Task Scheduler.

The Bloomberg MCP server (`blpapi-mcp --http`) also needs to be running — same story,
add to Task Scheduler if you want it to start automatically.

## Security note

The Funnel URL is publicly accessible. There is no authentication on the MCP endpoint.
Anyone who knows the URL can query Bloomberg data. The URL is not guessable (random subdomain)
but it is not secret either. If this is a concern, Tailscale's ACL-based access (without Funnel,
using only the private Tailscale network) is the more secure option — but then only machines
on the same Tailscale network can connect, not Claude Desktop's cloud connectors.
