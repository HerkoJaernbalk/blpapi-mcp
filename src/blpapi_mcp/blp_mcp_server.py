

import blpapi
import blpapi.version
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.utilities.logging import get_logger
from xbbg import blp

from . import types


def _df_to_json(df) -> str:
  return df.to_json(orient="split", date_format="iso")


def serve(args: types.StartupArgs):
  mcp = FastMCP("blpapi-mcp", host=args.host, port=args.port)

  logger = get_logger(__name__)
  logger.info("startup args:" + str(args))
  logger.info("blpapi version:" + blpapi.version()) # type: ignore

  @mcp.tool(
    name="bdp",
    description="Get Bloomberg reference data"
  )
  async def bdp(tickers: list[str], flds: list[str], kwargs: dict[str, object]) -> str:
    return _df_to_json(blp.bdp(tickers=tickers, flds=flds, kwargs=kwargs))

  @mcp.tool(
    name="bds",
    description="Get Bloomberg block data"
  )
  async def bds(tickers: list[str], flds: list[str], use_port: bool = False, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bds(tickers=tickers, flds=flds, use_port=use_port, kwargs=kwargs))

  @mcp.tool(
    name="bdh",
    description="Get Bloomberg historical data"
  )
  async def bdh(tickers: list[str], flds: list[str], start_date: str | None = None, end_date: str = "today", adjust: str | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bdh(tickers=tickers, flds=flds, start_date=start_date, end_date=end_date, adjust=adjust, kwargs=kwargs))

  @mcp.tool(
    name="bdib",
    description="Get Bloomberg intraday bar data"
  )
  async def bdib(ticker: str, dt: str, session: str = "allday", typ: str = "TRADE", kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bdib(ticker=ticker, dt=dt, session=session, typ=typ, kwargs=kwargs))

  @mcp.tool(
    name="bdtick",
    description="Get Bloomberg tick data"
  )
  async def bdtick(ticker: str, dt: str, session: str = "allday", time_range: tuple[str, ...] | None = None, types: list[str] | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bdtick(ticker=ticker, dt=dt, session=session, time_range=time_range, types=types, kwargs=kwargs))

  @mcp.tool(
    name="earning",
    description="Get Bloomberg earning exposure by Geo or Products"
  )
  async def earning(ticker: str, by: str = "Geo", typ: str = "Revenue", ccy: str | None = None, level: str | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.earning(ticker=ticker, by=by, typ=typ, ccy=ccy, level=level, kwargs=kwargs))

  @mcp.tool(
    name="dividend",
    description="Get Bloomberg dividend / split history"
  )
  async def dividend(tickers: list[str], typ: str = "all", start_date: str | None = None, end_date: str | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.dividend(tickers=tickers, typ=typ, start_date=start_date, end_date=end_date, kwargs=kwargs))

  @mcp.tool(
    name="beqs",
    description="Get Bloomberg equity screening"
  )
  async def beqs(screen: str, asof: str | None = None, typ: str = "PRIVATE", group: str = "General", kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.beqs(screen=screen, asof=asof, typ=typ, group=group, kwargs=kwargs))

  @mcp.tool(
    name="turnover",
    description="Calculate the adjusted turnover (in millions)"
  )
  async def turnover(tickers: list[str], flds: str = "Turnover", start_date: str | None = None, end_date: str | None = None, ccy: str = "USD", factor: float = 1e6) -> str:
    return _df_to_json(blp.turnover(tickers=tickers, flds=flds, start_date=start_date, end_date=end_date, ccy=ccy, factor=factor))


  mcp.run(transport=args.transport.value)
