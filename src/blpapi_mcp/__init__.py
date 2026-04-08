
import argparse
import os
import typing

from . import types
from . import blp_mcp_server

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("--sse", action="store_true", help="Run an sse server instead of stdio")
  parser.add_argument("--host", type=str, default=None)
  parser.add_argument("--port", type=int, default=None)

  args = parser.parse_args()
  is_sse = args.sse or args.host is not None or args.port is not None or os.environ.get("SSE_MODE", "").lower() == "true"

  transport = types.Transport.SSE if is_sse else types.Transport.STDIO
  host = args.host or os.environ.get("HOST", "0.0.0.0")
  port = args.port or int(os.environ.get("PORT", 8080))
  return types.StartupArgs(transport=transport, host=host, port=port)

def main() -> None:
  args = parse_args()
  blp_mcp_server.serve(args)
