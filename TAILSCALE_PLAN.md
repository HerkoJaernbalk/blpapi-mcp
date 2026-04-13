# Remote Access Plan for Bloomberg MCP Server

The Bloomberg MCP server (`blpapi-mcp --http`) runs on the Bloomberg Terminal machine
and listens on port 8080. The goal is to make it reachable from other machines so that
Claude Desktop can connect to it remotely.

---

## Quick Personal Test (start here)

Before committing to any multi-user setup, validate the full remote flow with just
your own laptop.

**Requirements:** Tailscale installed on both the Bloomberg machine and your laptop.

### Steps

1. Install Tailscale on the Bloomberg machine: https://tailscale.com/download
2. Log in with a Tailscale account (free tier works for 1 user)
3. Install Tailscale on your laptop and log in with the same account
4. On the Bloomberg machine, start the MCP server:
   ```
   blpapi-mcp --http
   ```
5. On the Bloomberg machine, expose it to the tailnet:
   ```
   tailscale serve 8080
   ```
   Tailscale will output a URL like `https://<machine-name>.<tailnet>.ts.net`
6. On your laptop, add the MCP endpoint to Claude Desktop:
   ```
   https://<machine-name>.<tailnet>.ts.net/mcp
   ```
7. In Claude Desktop → Connectors — bloomberg tools should appear

This is private to your Tailscale network. Nothing is publicly accessible.

---

## Multi-User Option A: Corporate VPN (existing infrastructure)

**Use when:** Users are at a company that already has a VPN (Cisco AnyConnect,
Palo Alto GlobalProtect, Zscaler, etc.)

**How it works:** The Bloomberg machine sits on the office network. Users connect
via their existing corporate VPN and access the server by IP or hostname.

### Steps

1. Start the MCP server on the Bloomberg machine:
   ```
   blpapi-mcp --http
   ```
2. Ask IT to open port 8080 inbound on the Bloomberg machine from the office network
3. Ask IT to assign a stable hostname, e.g. `bloomberg-mcp.office.local`
4. Users add this to Claude Desktop:
   ```
   http://bloomberg-mcp.office.local:8080/mcp
   ```
   or with the machine's IP:
   ```
   http://192.168.x.x:8080/mcp
   ```

**User setup:** none — they're already on the VPN  
**Cost:** none  
**Code changes:** none  
**Auth:** VPN membership (IT controls who gets access)

---

## Multi-User Option B: Tailscale as the VPN (no existing infrastructure)

**Use when:** There is no corporate VPN, or users are spread across different
networks and companies.

**How it works:** You manage a Tailscale tailnet. You invite each user. The Bloomberg
machine is exposed only within the tailnet — nothing is public.

### Steps

1. Create a Tailscale account and set up an organization at https://tailscale.com
2. On the Bloomberg machine, install Tailscale, log in, then run:
   ```
   blpapi-mcp --http
   tailscale serve 8080
   ```
3. Invite each user to your tailnet via the Tailscale admin console
4. Each user installs Tailscale on their machine and accepts the invite (one-time)
5. Users add this to Claude Desktop:
   ```
   https://<bloomberg-machine>.<tailnet>.ts.net/mcp
   ```

**User setup:** install Tailscale once, accept invite  
**Cost:** free up to 3 users; $6/user/month (Starter) beyond that (~$120/month for 20 users)  
**Code changes:** none  
**Auth:** tailnet membership (you control invites; Tailscale ACLs for fine-grained access)

---

## Multi-User Option C: Tailscale Funnel + OAuth (no client install required)

**Use when:** Users cannot or will not install any software, or access is needed
from the cloud (e.g. claude.ai browser-based connectors, not just Claude Desktop).

**How it works:** The Bloomberg machine is exposed publicly via Tailscale Funnel
(stable HTTPS URL). The MCP server requires OAuth authentication — users log in
once via browser, Claude Desktop handles token refresh automatically.

### Steps

1. Enable Tailscale Funnel in the admin console:
   - https://login.tailscale.com/admin/dns → enable HTTPS Certificates
   - https://login.tailscale.com/admin/acls → add Funnel to ACL policy
2. On the Bloomberg machine:
   ```
   blpapi-mcp --http
   tailscale funnel 8080
   ```
   Note the public URL: `https://<machine-name>.<tailnet>.ts.net`
3. Set up Auth0 (free tier, up to 7,500 users): https://auth0.com
4. Add OAuth support to the MCP server using FastMCP's `token_verifier` and
   `AuthSettings` — see the MCP authorization guide:
   https://modelcontextprotocol.io/docs/tutorials/security/authorization
5. Users add the public URL to Claude Desktop and authenticate once via browser:
   ```
   https://<machine-name>.<tailnet>.ts.net/mcp
   ```

**User setup:** browser login once, no software install  
**Cost:** Tailscale free tier + Auth0 free tier = $0  
**Code changes:** required — OAuth middleware on the MCP server  
**Auth:** OAuth 2.1 Bearer tokens (Auth0 manages identity)

### Security note

The Funnel URL is publicly accessible by anyone who knows it. OAuth ensures
only authenticated users can use it, but the URL itself is not secret.
For a fully private setup, use Option A or B instead.
