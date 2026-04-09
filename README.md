# BLPAPI-MCP

A MCP server that gives Claude Desktop access to Bloomberg financial data.

> **Note:** A Bloomberg Terminal must be running on the same machine for this to work.

## Requirements

- Bloomberg Terminal (running and logged in)
- [Python 3.12](https://www.python.org/downloads/release/python-3120/) — newest version supported by blpapi
- [UV](https://docs.astral.sh/uv/getting-started/installation/) — Python package manager
- [Claude Desktop](https://claude.ai/download)

## Installation

### 1. Install Python 3.12

Download and install from [python.org](https://www.python.org/downloads/release/python-3120/). During installation, check **"Add Python to PATH"**.

### 2. Install UV

Open Command Prompt and run:

```bash
winget install astral-sh.uv
```

### 3. Install blpapi-mcp

```bash
uv tool install "git+https://github.com/HerkoJaernbalk/blpapi-mcp" --extra-index-url https://blpapi.bloomberg.com/repository/releases/python/simple/
```

The `--extra-index-url` is required because `blpapi` is distributed via Bloomberg's own package index, not the public Python one. UV will handle the isolated environment automatically.

### 4. Configure Claude Desktop

Open (or create) Claude Desktop's config file at:
```
%APPDATA%\Claude\claude_desktop_config.json
```

Add the following, replacing `<your-username>` with your Windows username:

```json
{
  "mcpServers": {
    "bloomberg": {
      "command": "C:\\Users\\<your-username>\\.local\\bin\\blpapi-mcp.exe"
    }
  }
}
```

> **Tip:** Not sure of your username? Open Command Prompt and type `echo %USERNAME%`

### 5. Restart Claude Desktop

Restart Claude Desktop. If everything is set up correctly, you should see Bloomberg listed as a connected tool.

## Running as a Network Server (HTTP mode)

Instead of running as a local subprocess, you can run blpapi-mcp as an HTTP server on the Bloomberg machine and connect to it from other computers on the same network.

### On the Bloomberg machine

```bash
blpapi-mcp --http
```

The server will print:
```
Bloomberg MCP server listening on http://0.0.0.0:8080/mcp
Connect clients to: http://192.168.x.x:8080/mcp
```

Use the IP from the second line in the client config below.


## Access Bloomberg on the Go

Once Claude Desktop is running on your Bloomberg Terminal machine, you can access Bloomberg data from anywhere using [Claude for iPhone](https://apps.apple.com/app/claude-ai/id6473753684) with the **Dispatch** feature.

Dispatch lets your phone connect to Claude Desktop running on your local machine, meaning you can pull Bloomberg data and do financial analysis on the go — as long as your Bloomberg Terminal machine is on and Claude Desktop is running.

The real power here is what Claude can do with the data: pull and compare metrics across dozens of securities in one prompt, run cross-sectional analysis, generate charts, and produce written summaries — all through natural conversation. No manual lookups, no repetitive navigation.

Any analysis Claude produces can be saved directly to your computer in any format — Excel, PowerPoint, Word, PDF, or others. Claude will ask where to save the file and prompt you to grant folder access the first time. Once permitted, files land on your computer ready to use.

## Updating

To update to the latest version:

```bash
uv tool install --force "git+https://github.com/HerkoJaernbalk/blpapi-mcp" --extra-index-url https://blpapi.bloomberg.com/repository/releases/python/simple/
```

## Troubleshooting

- **Bloomberg not showing in Claude** — make sure Bloomberg Terminal is open and logged in before starting Claude Desktop
- **Install fails** — make sure Python 3.12 is installed and UV is installed, then try again
- **Wrong path in config** — run `where blpapi-mcp` in Command Prompt to find the exact path

## Trademark Note

This project is not affiliated with Bloomberg Finance L.P. The use of the name Bloomberg is only descriptive of what this package is used with.
