

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
    description="""Get Bloomberg reference/snapshot data for one or more securities.
    Returns the latest value for each requested field.

    Ticker format examples:
      Equities:    'AAPL US Equity', 'MSFT US Equity', 'BP/ LN Equity', 'SAP GY Equity'
      Indices:     'SPX Index', 'UKX Index', 'NKY Index', 'INDU Index'
      FX:          'EURUSD Curncy', 'GBPUSD Curncy', 'USDJPY Curncy'
      Govt bonds:  'GT10 Govt' (US 10yr), 'GT2 Govt' (US 2yr)
      Futures:     'ESZ4 Index' (S&P future), 'CLZ4 Comdty' (crude oil)

    Common fields:
      Pricing:     PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, PX_BID, PX_ASK
      Valuation:   PE_RATIO, PX_TO_BOOK_RATIO, EV_TO_T12M_EBITDA, EQY_DVD_YLD_IND
      Financials:  MARKET_CAP, SALES_REV_TURN, EBITDA, CF_FREE_CASH_FLOW, BOOK_VAL_PER_SH
      Quality:     RETURN_ON_EQUITY, RETURN_ON_ASSET, GROSS_MARGIN, TOT_DEBT_TO_TOT_EQY
      Info:        NAME, TICKER, GICS_SECTOR_NAME, GICS_INDUSTRY_NAME, COUNTRY_ISO, CRNCY, EXCH_CODE
      Risk:        VOLATILITY_30D, VOLATILITY_90D, BETA_ADJUSTED_OVERRIDABLE, SHORT_INT_RATIO
      Estimates:   BEST_TARGET_PRICE, BEST_EPS, BEST_EPS_NXT_YR, ANALYST_RATING
      Ownership:   EQY_INST_PCT_SH_OUT, SHARES_OUTSTANDING, FLOAT_SHARES_OUTSTANDING
    """
  )
  async def bdp(tickers: list[str], flds: list[str], kwargs: dict[str, object] | None = None) -> str:
    return _df_to_json(blp.bdp(tickers=tickers, flds=flds, kwargs=kwargs))

  @mcp.tool(
    name="bds",
    description="""Get Bloomberg bulk/block data — returns multi-row datasets for a security.
    Use this when a field returns a table of data rather than a single value.

    Ticker format: same as bdp (e.g. 'AAPL US Equity')

    Common fields:
      Holders:      TOP_20_HOLDERS (top 20 institutional holders with % owned)
                    FUND_MNGR_AND_PFOLIO (fund managers and portfolios)
      Analysts:     ANALYST_RECOMMENDATIONS (buy/hold/sell breakdown and changes)
                    BEST_ANALYST_RATING (individual analyst ratings and targets)
      Earnings:     EARN_ANN_DT_AND_PER (upcoming earnings dates)
                    IS_EPS_SURP_HIST (historical EPS surprises vs estimates)
      Dividends:    EQY_DVD_HIST (full dividend history with ex-date, amount, type)
                    DVD_HIST_ALL (all dividend and split events)
      News/Events:  NEWS_STORY (recent news headlines)
                    BLOOMBERG_PEERS (peer/comparable companies)
      Fixed income: COUPONS (bond coupon schedule)
                    DEBT_STRUCTURE (full debt breakdown)
    """
  )
  async def bds(tickers: list[str], flds: list[str], use_port: bool = False, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bds(tickers=tickers, flds=flds, use_port=use_port, kwargs=kwargs))

  @mcp.tool(
    name="bdh",
    description="""Get Bloomberg historical time series data for one or more securities.
    Returns daily data between start_date and end_date.

    Ticker format: same as bdp (e.g. 'AAPL US Equity')
    Date format: 'YYYY-MM-DD' or 'today'
    adjust: 'all' (splits+dividends), 'dvd' (dividends only), 'split' (splits only), None

    Common fields:
      OHLCV:       PX_OPEN, PX_HIGH, PX_LOW, PX_LAST, PX_VOLUME
      Returns:     DAY_TO_DAY_TOT_RETURN_GROSS_DVDS (total return including dividends)
      Valuation:   PE_RATIO, PX_TO_BOOK_RATIO, EQY_DVD_YLD_IND, EV_TO_T12M_EBITDA
      Financials:  MARKET_CAP, SALES_REV_TURN, EBITDA
      Risk:        VOLATILITY_30D, VOLATILITY_90D, BETA_ADJUSTED_OVERRIDABLE
      FX:          PX_LAST works for currency pairs (e.g. 'EURUSD Curncy')

    Example: Get AAPL closing prices for 2024:
      tickers=['AAPL US Equity'], flds=['PX_LAST'], start_date='2024-01-01', end_date='2024-12-31'
    """
  )
  async def bdh(tickers: list[str], flds: list[str], start_date: str | None = None, end_date: str = "today", adjust: str | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bdh(tickers=tickers, flds=flds, start_date=start_date, end_date=end_date, adjust=adjust, kwargs=kwargs))

  @mcp.tool(
    name="bdib",
    description="""Get Bloomberg intraday bar data for a single security on a specific date.
    Returns OHLCV bars aggregated at a given interval (default 1 minute).

    Ticker format: same as bdp (e.g. 'AAPL US Equity')
    dt format: 'YYYY-MM-DD'
    session: 'allday' (all hours), 'am' (pre-market), 'pm' (post-market),
             'pre' (pre-market only), 'post' (after-hours only)
    typ: 'TRADE' (default), 'BID', 'ASK', 'BEST_BID', 'BEST_ASK'

    Use this for intraday price analysis, VWAP calculations, or studying intraday patterns.
    Example: Get AAPL 1-min bars for today's regular session:
      ticker='AAPL US Equity', dt='2024-01-15', session='allday', typ='TRADE'
    """
  )
  async def bdib(ticker: str, dt: str, session: str = "allday", typ: str = "TRADE", kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bdib(ticker=ticker, dt=dt, session=session, typ=typ, kwargs=kwargs))

  @mcp.tool(
    name="bdtick",
    description="""Get Bloomberg tick-by-tick trade and quote data for a single security on a specific date.
    Returns every individual trade or quote event — much more granular than intraday bars.

    Ticker format: same as bdp (e.g. 'AAPL US Equity')
    dt format: 'YYYY-MM-DD'
    session: 'allday', 'am', 'pm', 'pre', 'post'
    time_range: optional tuple of ('HH:MM:SS', 'HH:MM:SS') to limit time window
    types: list of event types to include — ['TRADE', 'BID', 'ASK', 'BID_BEST', 'ASK_BEST', 'AT_TRADE']

    Use for microstructure analysis, precise execution analysis, or spread analysis.
    Warning: can return very large datasets for liquid securities.
    """
  )
  async def bdtick(ticker: str, dt: str, session: str = "allday", time_range: tuple[str, ...] | None = None, types: list[str] | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.bdtick(ticker=ticker, dt=dt, session=session, time_range=time_range, types=types, kwargs=kwargs))

  @mcp.tool(
    name="earning",
    description="""Get Bloomberg earnings exposure breakdown by geography or business segment.
    Shows what percentage of a company's revenue/earnings comes from each region or product line.

    Ticker format: same as bdp (e.g. 'AAPL US Equity')
    by: 'Geo' (geographic breakdown) or 'Products' (business segment breakdown)
    typ: 'Revenue' (default), 'Operating_Income', 'Assets', 'Employees'
    ccy: currency to report in (e.g. 'USD', 'EUR') — defaults to reporting currency
    level: level of detail for segment hierarchy

    Example uses:
      - 'How much of AAPL revenue comes from China?' → by='Geo', typ='Revenue'
      - 'What are MSFT business segments by operating income?' → by='Products', typ='Operating_Income'
    """
  )
  async def earning(ticker: str, by: str = "Geo", typ: str = "Revenue", ccy: str | None = None, level: str | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.earning(ticker=ticker, by=by, typ=typ, ccy=ccy, level=level, kwargs=kwargs))

  @mcp.tool(
    name="dividend",
    description="""Get Bloomberg dividend and stock split history for one or more securities.
    Returns historical dividend payments and stock split events.

    Ticker format: same as bdp (e.g. 'AAPL US Equity')
    typ: 'all' (dividends + splits), 'dividend' (dividends only), 'split' (splits only)
    start_date / end_date: 'YYYY-MM-DD' to filter history range

    Returns columns including: ex-date, declared date, record date, pay date,
    dividend amount, split ratio, frequency, and currency.

    Example uses:
      - Dividend yield history and payment consistency analysis
      - Adjusting historical prices for splits
      - Comparing dividend growth across a basket of stocks
    """
  )
  async def dividend(tickers: list[str], typ: str = "all", start_date: str | None = None, end_date: str | None = None, kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.dividend(tickers=tickers, typ=typ, start_date=start_date, end_date=end_date, kwargs=kwargs))

  @mcp.tool(
    name="beqs",
    description="""Run a saved Bloomberg equity screen (EQS) and return matching securities.
    Screens must be pre-built and saved in Bloomberg Terminal under EQS <GO>.

    screen: exact name of the saved screen in Bloomberg (case-sensitive)
    typ: 'PRIVATE' (your own screens, default) or 'GLOBAL' (Bloomberg public screens)
    group: screen group/folder name, default 'General'
    asof: run the screen as of a historical date 'YYYY-MM-DD' (if supported)

    Returns a list of securities matching the screen criteria with basic data fields.

    Example uses:
      - Run a value screen you built to find cheap stocks
      - Pull constituents of a custom universe
      - Screen for dividend payers in a specific sector
    """
  )
  async def beqs(screen: str, asof: str | None = None, typ: str = "PRIVATE", group: str = "General", kwargs: types.BloombergKWArgs = None) -> str:
    return _df_to_json(blp.beqs(screen=screen, asof=asof, typ=typ, group=group, kwargs=kwargs))

  @mcp.tool(
    name="turnover",
    description="""Calculate adjusted daily trading turnover (value traded) for a basket of securities.
    Turnover = price × volume, adjusted for currency and scaled by factor.

    Ticker format: same as bdp (e.g. 'AAPL US Equity')
    flds: Bloomberg field for turnover, default 'Turnover'
    start_date / end_date: 'YYYY-MM-DD' date range
    ccy: target currency to convert turnover into, default 'USD'
    factor: divisor to scale the result, default 1e6 (returns values in millions)

    Example uses:
      - Compare daily liquidity across a stock universe
      - Filter out illiquid names from a portfolio
      - Analyse volume patterns over time for execution planning
    """
  )
  async def turnover(tickers: list[str], flds: str = "Turnover", start_date: str | None = None, end_date: str | None = None, ccy: str = "USD", factor: float = 1e6) -> str:
    return _df_to_json(blp.turnover(tickers=tickers, flds=flds, start_date=start_date, end_date=end_date, ccy=ccy, factor=factor))


  mcp.run(transport=args.transport.value)
