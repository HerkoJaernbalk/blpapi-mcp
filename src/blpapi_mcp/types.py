
import json
import enum
import typing


class Transport(enum.Enum):
  HTTP="streamable-http"
  STDIO="stdio"

class StartupArgs:
  transport: Transport
  host: str
  port: int

  def __init__(self, transport: Transport, host: str, port: int) -> None:
    self.transport = transport
    self.host = host
    self.port = port

  def __str__(self) -> str:
    return json.dumps({
      "transport": self.transport.value,
      "host": self.host,
      "port": self.port,
    })

BloombergKWArgs = dict[str, typing.Any] | None
