# SSE Transport Implementation Plan

## A Note

The original `djsamseng/blpapi-mcp` repo already had SSE support designed in — `types.py` has the `Transport` enum, `__init__.py` already parses `--sse`, `--host`, `--port`. It even had the Claude Code CLI command documented:

```
claude mcp add --transport sse blpapi-mcp http://127.0.0.1:8000/sse
```

This fork temporarily... *simplified* the transport layer to stdio for Claude Desktop compatibility, and made several meaningful improvements along the way (BQL support, periodicity, bloomberg_news, etc.). We are now restoring SSE with full confidence and zero embarrassment.

## Current State

Most of the groundwork is already in place:

- `types.py` — `Transport.SSE` / `Transport.STDIO` enum + `StartupArgs(transport, host, port)` already defined
- `__init__.py` — `--sse`, `--host`, `--port` CLI flags already parsed and wired into `StartupArgs`
- `blp_mcp_server.py` — last line already calls `mcp.run(transport=args.transport.value)`

The only things missing are:
1. Passing `host` and `port` into `mcp.run()` when in SSE mode
2. A startup log message showing the URL

## Blocked On

**Need to check the MCP SDK's `run()` signature on the Bloomberg Terminal machine.**

```
pip show mcp
```

Then check whether `FastMCP.run()` accepts `host` and `port` kwargs.

- **If yes** → one-line change, just add `host=args.host, port=args.port` to the existing `mcp.run()` call
- **If no** → need to call a different method, e.g. `mcp.run_sse_async()`, which may require wrapping in `asyncio.run()`

## Changes Required (once SDK is confirmed)

### `blp_mcp_server.py` — last line only

**Current:**
```python
mcp.run(transport=args.transport.value)
```

**Target (if SDK supports host/port):**
```python
if args.transport == types.Transport.SSE:
    print(f"Bloomberg MCP server listening on http://{args.host}:{args.port}/sse")
mcp.run(transport=args.transport.value, host=args.host, port=args.port)
```

### `__init__.py` — default host change

Current default host is `127.0.0.1` (localhost only). For remote access change to `0.0.0.0`:

```python
host = args.host if args.host is not None else "0.0.0.0"
port = args.port if args.port is not None else 8080
```

> Note: user requested default port 8080, current default is 8000.

### `pyproject.toml` — no changes needed

## ENV variable support (`SSE_MODE`, `PORT`)

Currently not implemented. `__init__.py` only reads CLI args.
If needed, add to `parse_args()`:

```python
import os
is_sse = args.sse or args.host is not None or args.port is not None or os.environ.get("SSE_MODE", "").lower() == "true"
port = args.port or int(os.environ.get("PORT", 8080))
host = args.host or os.environ.get("HOST", "0.0.0.0")
```

## Usage (once implemented)

```bash
# SSE mode, default port 8080
blpapi-mcp --sse

# Custom port
blpapi-mcp --sse --port 9090

# Custom host + port
blpapi-mcp --sse --host 0.0.0.0 --port 8080

# Via env vars
SSE_MODE=true PORT=8080 blpapi-mcp
```

Client connects to: `http://<bloomberg-machine-ip>:8080/sse`

## Network Access

By default this only works on the **same local network** as the Bloomberg machine.

Options for remote access:

- **Tailscale** (recommended) — install on both machines, they get stable private IPs that work across any network. No router config or firewall changes needed.
- **VPN** — connect the client machine to the same network as the Bloomberg machine, then use the local IP as normal.
- **Port forwarding** — expose port 8080 on the router and use the public IP. Works but is not ideal for Bloomberg data over the open internet.

The Claude Desktop config changes from launching a local subprocess (stdio) to pointing at the SSE URL:

```json
{
  "mcpServers": {
    "bloomberg": {
      "url": "http://<bloomberg-machine-ip>:8080/sse"
    }
  }
}
```

With Tailscale the IP would be the Tailscale address of the Bloomberg machine, reachable from anywhere.
