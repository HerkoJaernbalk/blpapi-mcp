# blpapi-mcp

Bloomberg data via MCP. Two components:

- **Worker** — runs on the Bloomberg Terminal machine, executes Bloomberg API calls
- **Gateway** — internet-facing, adds auth and policy, forwards to the worker

## Architecture

```text
ChatGPT / Claude / MCP Client
        |
        | HTTPS
        v
   Gateway :8443
        |
        | localhost or tunnel
        v
    Worker :8080
        |
        v
Bloomberg Terminal (localhost:8194)
```

## Tools

`bdp` `bds` `bdh` `instruments` `curve_list` `govt_list` `earning` `dividend`

High-risk (off by default): `bql` `beqs`  
Enable with: `MCP_GATEWAY_ENABLE_HIGH_RISK_TOOLS=true`

## Run

**Worker** (Bloomberg machine):
```bash
blpapi-worker-chatgpt --http --host 127.0.0.1 --port 8080
```

**Gateway**:
```bash
WORKER_MCP_URL=http://127.0.0.1:8080/mcp \
blpapi-gateway-chatgpt --host 0.0.0.0 --port 8443
```

## Install

```bash
uv sync
```
