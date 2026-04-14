import argparse
import datetime as dt
import json
import logging
import os
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


DEFAULT_ALLOWED_TOOLS = {
    "bdp",
    "bds",
    "bdh",
    "instruments",
    "curve_list",
    "govt_list",
    "earning",
    "dividend",
}
HIGH_RISK_TOOLS = {"bql", "bdtick", "beqs"}


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _split_csv_env(name: str) -> set[str]:
    raw = os.getenv(name, "")
    return {item.strip() for item in raw.split(",") if item.strip()}


class GatewayConfig:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.worker_mcp_url = os.getenv("WORKER_MCP_URL", "http://127.0.0.1:8080/mcp")
        self.require_auth = not _env_bool("MCP_GATEWAY_ALLOW_UNAUTH", default=False)
        self.auth_tokens = _split_csv_env("MCP_GATEWAY_TOKENS")
        self.forward_timeout_sec = float(os.getenv("MCP_GATEWAY_TIMEOUT_SEC", "120"))
        self.max_request_bytes = int(os.getenv("MCP_GATEWAY_MAX_REQUEST_BYTES", "200000"))
        self.max_string_length = int(os.getenv("MCP_GATEWAY_MAX_STRING_LEN", "5000"))
        self.max_array_items = int(os.getenv("MCP_GATEWAY_MAX_ARRAY_ITEMS", "200"))
        self.max_object_keys = int(os.getenv("MCP_GATEWAY_MAX_OBJECT_KEYS", "200"))
        self.max_nesting_depth = int(os.getenv("MCP_GATEWAY_MAX_NESTING_DEPTH", "12"))

        allow = set(DEFAULT_ALLOWED_TOOLS)
        if _env_bool("MCP_GATEWAY_ENABLE_HIGH_RISK_TOOLS", default=False):
            allow |= HIGH_RISK_TOOLS
        allow |= _split_csv_env("MCP_GATEWAY_EXTRA_ALLOWED_TOOLS")
        self.allowed_tools = allow

    def redact(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "worker_mcp_url": self.worker_mcp_url,
            "require_auth": self.require_auth,
            "token_count": len(self.auth_tokens),
            "allowed_tools": sorted(self.allowed_tools),
            "forward_timeout_sec": self.forward_timeout_sec,
            "max_request_bytes": self.max_request_bytes,
        }


def _jsonrpc_error(req_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}
    if data is not None:
        payload["error"]["data"] = data
    return payload


def _extract_bearer_token(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return None
    return auth[7:].strip()


def _validate_basic_limits(value: Any, config: GatewayConfig, depth: int = 0) -> list[str]:
    if depth > config.max_nesting_depth:
        return [f"Maximum nesting depth exceeded ({config.max_nesting_depth})"]
    if isinstance(value, str):
        if len(value) > config.max_string_length:
            return [f"String length exceeds limit ({config.max_string_length})"]
        return []
    if isinstance(value, list):
        if len(value) > config.max_array_items:
            return [f"Array size exceeds limit ({config.max_array_items})"]
        errors: list[str] = []
        for item in value:
            errors.extend(_validate_basic_limits(item, config, depth + 1))
            if errors:
                return errors
        return []
    if isinstance(value, dict):
        if len(value) > config.max_object_keys:
            return [f"Object key count exceeds limit ({config.max_object_keys})"]
        errors = []
        for key, child in value.items():
            if isinstance(key, str) and len(key) > config.max_string_length:
                return [f"Key length exceeds limit ({config.max_string_length})"]
            errors.extend(_validate_basic_limits(child, config, depth + 1))
            if errors:
                return errors
        return []
    return []


def _parse_iso_date(value: Any) -> dt.date | None:
    if not isinstance(value, str):
        return None
    if value == "today":
        return dt.date.today()
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _validate_tool_args(tool: str, args: dict[str, Any], config: GatewayConfig) -> list[str]:
    errors = _validate_basic_limits(args, config)
    if errors:
        return errors

    tickers = args.get("tickers")
    if isinstance(tickers, list):
        max_tickers = 50
        if tool in {"bds", "bdh"}:
            max_tickers = 25
        if len(tickers) > max_tickers:
            return [f"`tickers` exceeds limit for {tool} ({max_tickers})"]

    flds = args.get("flds")
    if isinstance(flds, list):
        max_fields = 40 if tool == "bdp" else 20
        if len(flds) > max_fields:
            return [f"`flds` exceeds limit for {tool} ({max_fields})"]

    kwargs = args.get("kwargs")
    if isinstance(kwargs, dict) and len(kwargs) > 20:
        return ["`kwargs` has too many entries (20 max)"]

    if tool in {"instruments", "curve_list", "govt_list"}:
        max_results = args.get("max_results")
        if isinstance(max_results, int) and max_results > 100:
            return ["`max_results` cannot exceed 100"]

    if tool == "bdh":
        start_date = _parse_iso_date(args.get("start_date"))
        end_date = _parse_iso_date(args.get("end_date") or "today")
        if start_date and end_date and start_date > end_date:
            return ["`start_date` cannot be after `end_date`"]
        if start_date and end_date and (end_date - start_date).days > 3650:
            return ["`bdh` date window cannot exceed 3650 days"]

    return []


def _filter_tools_list_response(data: Any, allowed_tools: set[str]) -> Any:
    if not isinstance(data, dict):
        return data
    result = data.get("result")
    if not isinstance(result, dict):
        return data
    tools = result.get("tools")
    if not isinstance(tools, list):
        return data
    result["tools"] = [t for t in tools if isinstance(t, dict) and t.get("name") in allowed_tools]
    return data


def create_app(config: GatewayConfig) -> FastAPI:
    app = FastAPI(title="blpapi-mcp-gateway", version="0.1.0")
    logger = logging.getLogger("blpapi_mcp.gateway")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger.info("gateway config: %s", json.dumps(config.redact(), sort_keys=True))

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True}

    @app.post("/mcp")
    async def mcp_proxy(request: Request) -> Response:
        req_id: Any = None
        try:
            body = await request.body()
            if len(body) > config.max_request_bytes:
                return JSONResponse(
                    status_code=413,
                    content=_jsonrpc_error(req_id, -32010, "Request too large"),
                )

            payload = json.loads(body.decode("utf-8"))
            req_id = payload.get("id")
            method = payload.get("method")
            params = payload.get("params") or {}
        except Exception:
            logger.exception("Invalid JSON request")
            return JSONResponse(status_code=400, content=_jsonrpc_error(req_id, -32700, "Invalid JSON"))

        logger.info("mcp request method=%s id=%s", method, req_id)

        if config.require_auth:
            token = _extract_bearer_token(request)
            if not token or token not in config.auth_tokens:
                logger.warning("auth failed for method=%s id=%s", method, req_id)
                return JSONResponse(status_code=401, content=_jsonrpc_error(req_id, -32001, "Unauthorized"))

        if method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments") or {}
            if tool_name not in config.allowed_tools:
                logger.warning("tool blocked name=%s id=%s", tool_name, req_id)
                return JSONResponse(
                    status_code=403,
                    content=_jsonrpc_error(req_id, -32003, f"Tool `{tool_name}` is not allowed"),
                )
            if not isinstance(tool_args, dict):
                return JSONResponse(
                    status_code=400,
                    content=_jsonrpc_error(req_id, -32602, "`arguments` must be an object"),
                )
            arg_errors = _validate_tool_args(str(tool_name), tool_args, config)
            if arg_errors:
                logger.warning("argument policy blocked tool=%s id=%s reason=%s", tool_name, req_id, arg_errors[0])
                return JSONResponse(
                    status_code=400,
                    content=_jsonrpc_error(req_id, -32004, "Argument policy violation", {"reason": arg_errors[0]}),
                )

        upstream_headers = {
            "content-type": request.headers.get("content-type", "application/json"),
            "accept": request.headers.get("accept", "application/json, text/event-stream"),
        }
        # Preserve MCP session/protocol headers so upstream streamable-http state works.
        for header_name in ("mcp-session-id", "mcp-protocol-version", "last-event-id"):
            if request.headers.get(header_name):
                upstream_headers[header_name] = request.headers[header_name]

        if method == "tools/list":
            # tools/list responses are tiny; fetch and filter in-memory before returning.
            try:
                async with httpx.AsyncClient(timeout=config.forward_timeout_sec) as client:
                    upstream = await client.post(
                        config.worker_mcp_url,
                        content=body,
                        headers=upstream_headers,
                    )
                if upstream.status_code >= 400:
                    logger.error("upstream tools/list failed status=%s", upstream.status_code)
                    return JSONResponse(
                        status_code=502,
                        content=_jsonrpc_error(req_id, -32020, "Upstream error during tools/list"),
                    )
                upstream_data = upstream.json()
                filtered = _filter_tools_list_response(upstream_data, config.allowed_tools)
                response_headers = {"x-gateway": "blpapi-mcp-gateway"}
                if upstream.headers.get("mcp-session-id"):
                    response_headers["mcp-session-id"] = upstream.headers["mcp-session-id"]
                if upstream.headers.get("mcp-protocol-version"):
                    response_headers["mcp-protocol-version"] = upstream.headers["mcp-protocol-version"]
                return JSONResponse(
                    content=filtered,
                    status_code=upstream.status_code,
                    headers=response_headers,
                )
            except Exception:
                logger.exception("tools/list forwarding error")
                return JSONResponse(
                    status_code=502,
                    content=_jsonrpc_error(req_id, -32020, "Upstream connection error"),
                )

        try:
            client = httpx.AsyncClient(timeout=config.forward_timeout_sec)
            upstream = await client.send(
                client.build_request("POST", config.worker_mcp_url, content=body, headers=upstream_headers),
                stream=True,
            )
            if upstream.status_code >= 400:
                logger.error("upstream call failed status=%s method=%s id=%s", upstream.status_code, method, req_id)

            async def _stream_upstream() -> Any:
                try:
                    async for chunk in upstream.aiter_bytes():
                        yield chunk
                finally:
                    await upstream.aclose()
                    await client.aclose()

            passthrough_headers = {"x-gateway": "blpapi-mcp-gateway"}
            if upstream.headers.get("mcp-session-id"):
                passthrough_headers["mcp-session-id"] = upstream.headers["mcp-session-id"]
            if upstream.headers.get("mcp-protocol-version"):
                passthrough_headers["mcp-protocol-version"] = upstream.headers["mcp-protocol-version"]
            return StreamingResponse(
                _stream_upstream(),
                media_type=request.headers.get("accept", "application/json"),
                headers=passthrough_headers,
                status_code=upstream.status_code,
            )
        except Exception:
            logger.exception("stream forwarding error")
            return JSONResponse(
                status_code=502,
                content=_jsonrpc_error(req_id, -32020, "Upstream connection error"),
            )

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=os.getenv("MCP_GATEWAY_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MCP_GATEWAY_PORT", "8443")))
    parser.add_argument("--log-level", type=str, default=os.getenv("MCP_GATEWAY_LOG_LEVEL", "info"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = GatewayConfig(host=args.host, port=args.port)
    app = create_app(config)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
