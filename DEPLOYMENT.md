# Deployment Examples

## 1. HTTPS Gateway Behind Nginx

Gateway process:

```bash
blpapi-gateway-chatgpt --host 127.0.0.1 --port 8443
```

Nginx example:

```nginx
server {
    listen 443 ssl;
    server_name mcp.example.com;

    ssl_certificate     /etc/letsencrypt/live/mcp.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/mcp.example.com/privkey.pem;

    location /mcp {
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
        proxy_pass http://127.0.0.1:8443/mcp;
    }

    location /healthz {
        proxy_pass http://127.0.0.1:8443/healthz;
    }
}
```

## 2. Private Tunnel To Office Worker (SSH Reverse Tunnel)

On office worker machine (where Bloomberg Terminal runs):

```bash
blpapi-worker-chatgpt --http --host 127.0.0.1 --port 8080
ssh -N -R 18080:127.0.0.1:8080 user@gateway-host
```

On gateway machine:

```bash
export WORKER_MCP_URL=http://127.0.0.1:18080/mcp
blpapi-gateway-chatgpt --host 127.0.0.1 --port 8443
```

## 3. Private Mesh Network (Tailscale/WireGuard)

Office worker:

```bash
blpapi-worker-chatgpt --http --host 100.x.y.z --port 8080
```

Gateway:

```bash
export WORKER_MCP_URL=http://100.x.y.z:8080/mcp
blpapi-gateway-chatgpt --host 127.0.0.1 --port 8443
```

Keep firewall rules restricted to private mesh peers only.
