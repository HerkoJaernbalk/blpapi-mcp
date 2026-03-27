# BLPAPI-MCP

A MCP server that gives Claude Desktop access to Bloomberg financial data.

> **Note:** A Bloomberg Terminal must be running on the same machine for this to work.

## Requirements

- Windows
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
