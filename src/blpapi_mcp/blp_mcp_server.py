
import json
import datetime as dt

import blpapi
import blpapi.version
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.utilities.logging import get_logger

from . import types


_REFDATA = "//blp/refdata"
_EXRSVC  = "//blp/exrsvc"
_TIMEOUT = 10_000  # ms

# Intraday session time windows (HH:MM:SS)
_SESSION_TIMES = {
    "allday": ("00:00:00", "23:59:59"),
    "am":     ("04:00:00", "12:00:00"),
    "pm":     ("12:00:00", "20:00:00"),
    "pre":    ("04:00:00", "09:30:00"),
    "post":   ("16:00:00", "20:00:00"),
}


def _make_session() -> blpapi.Session:
    opts = blpapi.SessionOptions()
    opts.setServerHost("localhost")
    opts.setServerPort(8194)
    session = blpapi.Session(opts)
    if not session.start():
        raise RuntimeError("Could not connect to Bloomberg Terminal (is BBComm running?)")
    return session


def _to_value(elem):
    """Recursively convert a blpapi Element to a native Python value."""
    if elem.isArray():
        return [_to_value(elem.getValueAsElement(i)) for i in range(elem.numValues())]
    dtype = elem.datatype()
    if dtype in (blpapi.DataType.SEQUENCE, blpapi.DataType.CHOICE):
        return {
            str(elem.getElement(i).name()): _to_value(elem.getElement(i))
            for i in range(elem.numElements())
        }
    if dtype == blpapi.DataType.BOOL:
        return elem.getValueAsBool()
    if dtype in (blpapi.DataType.INT32, blpapi.DataType.INT64):
        return elem.getValueAsInteger()
    if dtype in (blpapi.DataType.FLOAT32, blpapi.DataType.FLOAT64):
        v = elem.getValueAsFloat()
        return None if v != v else v  # NaN -> None
    if dtype == blpapi.DataType.DATE:
        d = elem.getValueAsDatetime()
        return f"{d.year:04d}-{d.month:02d}-{d.day:02d}"
    if dtype in (blpapi.DataType.TIME, blpapi.DataType.DATETIME):
        return str(elem.getValueAsDatetime())
    try:
        return elem.getValueAsString()
    except Exception:
        return None


def _drain(session) -> list:
    """Collect all response messages from the session queue."""
    msgs = []
    while True:
        ev = session.nextEvent(_TIMEOUT)
        etype = ev.eventType()
        if etype in (blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE):
            msgs.extend(list(ev))
        if etype == blpapi.Event.RESPONSE:
            break
        if etype == blpapi.Event.TIMEOUT:
            raise RuntimeError("Bloomberg request timed out after 10s")
    return msgs


def _fmt_date(date_str: str) -> str:
    """Convert YYYY-MM-DD to YYYYMMDD for Bloomberg historical requests."""
    if date_str == "today":
        return dt.date.today().strftime("%Y%m%d")
    return date_str.replace("-", "")


def _parse_datetime(date_str: str, time_str: str) -> blpapi.datetime:
    d = dt.datetime.strptime(f"{date_str}T{time_str}", "%Y-%m-%dT%H:%M:%S")
    return blpapi.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)


def serve(args: types.StartupArgs):
    mcp = FastMCP("blpapi-mcp", host=args.host, port=args.port)
    logger = get_logger(__name__)
    logger.info("startup args:" + str(args))
    logger.info("blpapi version:" + blpapi.version())  # type: ignore

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
        session = _make_session()
        try:
            if not session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = session.getService(_REFDATA)
            req = svc.createRequest("ReferenceDataRequest")
            for t in tickers:
                req.append("securities", t)
            for f in flds:
                req.append("fields", f)
            if kwargs:
                ovr = req.getElement("overrides")
                for k, v in kwargs.items():
                    o = ovr.appendElement()
                    o.setElement("fieldId", k)
                    o.setElement("value", str(v))
            session.sendRequest(req)
            result = {}
            for msg in _drain(session):
                sec_data = msg.getElement("securityData")
                for i in range(sec_data.numValues()):
                    sec = sec_data.getValueAsElement(i)
                    ticker = sec.getElementAsString("security")
                    fd = sec.getElement("fieldData")
                    result[ticker] = {
                        f: (_to_value(fd.getElement(f)) if fd.hasElement(f) else None)
                        for f in flds
                    }
            return json.dumps(result)
        finally:
            session.stop()

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
    async def bds(tickers: list[str], flds: list[str], kwargs: dict[str, object] | None = None) -> str:
        session = _make_session()
        try:
            if not session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = session.getService(_REFDATA)
            req = svc.createRequest("ReferenceDataRequest")
            for t in tickers:
                req.append("securities", t)
            for f in flds:
                req.append("fields", f)
            if kwargs:
                ovr = req.getElement("overrides")
                for k, v in kwargs.items():
                    o = ovr.appendElement()
                    o.setElement("fieldId", k)
                    o.setElement("value", str(v))
            session.sendRequest(req)
            result = {}
            for msg in _drain(session):
                sec_data = msg.getElement("securityData")
                for i in range(sec_data.numValues()):
                    sec = sec_data.getValueAsElement(i)
                    ticker = sec.getElementAsString("security")
                    fd = sec.getElement("fieldData")
                    result[ticker] = {
                        f: (_to_value(fd.getElement(f)) if fd.hasElement(f) else None)
                        for f in flds
                    }
            return json.dumps(result)
        finally:
            session.stop()

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
    async def bdh(tickers: list[str], flds: list[str], start_date: str | None = None, end_date: str = "today", adjust: str | None = None, kwargs: dict[str, object] | None = None) -> str:
        session = _make_session()
        try:
            if not session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = session.getService(_REFDATA)
            req = svc.createRequest("HistoricalDataRequest")
            for t in tickers:
                req.append("securities", t)
            for f in flds:
                req.append("fields", f)
            if start_date:
                req.set("startDate", _fmt_date(start_date))
            req.set("endDate", _fmt_date(end_date))
            req.set("periodicitySelection", "DAILY")
            if adjust in ("all", "dvd"):
                req.set("adjustmentNormal", True)
                req.set("adjustmentAbnormal", True)
            if adjust in ("all", "split"):
                req.set("adjustmentSplit", True)
            if kwargs:
                for k, v in kwargs.items():
                    req.set(k, v)
            session.sendRequest(req)
            result = {}
            for msg in _drain(session):
                sec_data = msg.getElement("securityData")
                ticker = sec_data.getElementAsString("security")
                fd_array = sec_data.getElement("fieldData")
                rows = []
                for i in range(fd_array.numValues()):
                    row_elem = fd_array.getValueAsElement(i)
                    row = {"date": _to_value(row_elem.getElement("date"))}
                    for f in flds:
                        row[f] = _to_value(row_elem.getElement(f)) if row_elem.hasElement(f) else None
                    rows.append(row)
                result[ticker] = rows
            return json.dumps(result)
        finally:
            session.stop()

    @mcp.tool(
        name="bdib",
        description="""Get Bloomberg intraday bar data for a single security on a specific date.
        Returns OHLCV bars aggregated at a given interval (default 1 minute).

        Ticker format: same as bdp (e.g. 'AAPL US Equity')
        dt format: 'YYYY-MM-DD'
        session: 'allday' (all hours), 'am' (04:00-12:00), 'pm' (12:00-20:00),
                 'pre' (04:00-09:30 pre-market), 'post' (16:00-20:00 after-hours)
        typ: 'TRADE' (default), 'BID', 'ASK', 'BEST_BID', 'BEST_ASK'
        interval: bar size in minutes (default 1)

        Use this for intraday price analysis, VWAP calculations, or studying intraday patterns.
        """
    )
    async def bdib(ticker: str, date: str, session: str = "allday", typ: str = "TRADE", interval: int = 1, kwargs: dict[str, object] | None = None) -> str:
        blp_session = _make_session()
        try:
            if not blp_session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = blp_session.getService(_REFDATA)
            req = svc.createRequest("IntradayBarRequest")
            req.set("security", ticker)
            req.set("eventType", typ)
            req.set("interval", interval)
            start_t, end_t = _SESSION_TIMES.get(session, _SESSION_TIMES["allday"])
            req.set("startDateTime", _parse_datetime(date, start_t))
            req.set("endDateTime", _parse_datetime(date, end_t))
            blp_session.sendRequest(req)
            bars = []
            for msg in _drain(blp_session):
                bar_data = msg.getElement("barData").getElement("barTickData")
                for i in range(bar_data.numValues()):
                    bar = bar_data.getValueAsElement(i)
                    bars.append({
                        "time":      _to_value(bar.getElement("time")),
                        "open":      bar.getElementAsFloat("open"),
                        "high":      bar.getElementAsFloat("high"),
                        "low":       bar.getElementAsFloat("low"),
                        "close":     bar.getElementAsFloat("close"),
                        "volume":    bar.getElementAsInteger("volume"),
                        "numEvents": bar.getElementAsInteger("numEvents"),
                    })
            return json.dumps(bars)
        finally:
            blp_session.stop()

    @mcp.tool(
        name="bdtick",
        description="""Get Bloomberg tick-by-tick trade and quote data for a single security on a specific date.
        Returns every individual trade or quote event — much more granular than intraday bars.

        Ticker format: same as bdp (e.g. 'AAPL US Equity')
        date format: 'YYYY-MM-DD'
        session: 'allday', 'am', 'pm', 'pre', 'post'
        time_range: optional tuple of ('HH:MM:SS', 'HH:MM:SS') to limit time window
        event_types: list of event types — ['TRADE', 'BID', 'ASK', 'BID_BEST', 'ASK_BEST', 'AT_TRADE']

        Use for microstructure analysis, precise execution analysis, or spread analysis.
        Warning: can return very large datasets for liquid securities.
        """
    )
    async def bdtick(ticker: str, date: str, session: str = "allday", time_range: tuple[str, ...] | None = None, event_types: list[str] | None = None, kwargs: dict[str, object] | None = None) -> str:
        blp_session = _make_session()
        try:
            if not blp_session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = blp_session.getService(_REFDATA)
            req = svc.createRequest("IntradayTickRequest")
            req.set("security", ticker)
            for etype in (event_types or ["TRADE"]):
                req.append("eventTypes", etype)
            if time_range and len(time_range) == 2:
                start_t, end_t = time_range[0], time_range[1]
            else:
                start_t, end_t = _SESSION_TIMES.get(session, _SESSION_TIMES["allday"])
            req.set("startDateTime", _parse_datetime(date, start_t))
            req.set("endDateTime", _parse_datetime(date, end_t))
            blp_session.sendRequest(req)
            ticks = []
            for msg in _drain(blp_session):
                tick_data = msg.getElement("tickData").getElement("tickData")
                for i in range(tick_data.numValues()):
                    tick = tick_data.getValueAsElement(i)
                    ticks.append({
                        "time":  _to_value(tick.getElement("time")),
                        "type":  tick.getElementAsString("type"),
                        "value": tick.getElementAsFloat("value"),
                        "size":  tick.getElementAsInteger("size") if tick.hasElement("size") else None,
                    })
            return json.dumps(ticks)
        finally:
            blp_session.stop()

    @mcp.tool(
        name="earning",
        description="""Get Bloomberg earnings exposure breakdown by geography or business segment.
        Shows what percentage of a company's revenue/earnings comes from each region or product line.

        Ticker format: same as bdp (e.g. 'AAPL US Equity')
        by: 'Geo' (geographic breakdown) or 'Products' (business segment breakdown)
        typ: 'Revenue' (default), 'Operating_Income', 'Assets', 'Employees'
        ccy: currency to report in (e.g. 'USD', 'EUR') — defaults to reporting currency

        Example uses:
          - 'How much of AAPL revenue comes from China?' → by='Geo', typ='Revenue'
          - 'What are MSFT business segments by operating income?' → by='Products', typ='Operating_Income'
        """
    )
    async def earning(ticker: str, by: str = "Geo", typ: str = "Revenue", ccy: str | None = None, kwargs: dict[str, object] | None = None) -> str:
        # Map parameters to Bloomberg bulk fields
        field_map = {
            ("Geo",      "Revenue"):          "GEO_SEGMENT_SALES_PCTS",
            ("Geo",      "Operating_Income"): "GEO_SEGMENT_OP_INC_PCTS",
            ("Products", "Revenue"):          "PRODUCT_SEGMENT_SALES_PCTS",
            ("Products", "Operating_Income"): "PRODUCT_SEGMENT_OP_INC_PCTS",
        }
        fld = field_map.get((by, typ), "GEO_SEGMENT_SALES_PCTS")
        overrides = dict(kwargs) if kwargs else {}
        if ccy:
            overrides["EQY_FUND_CRNCY"] = ccy
        session = _make_session()
        try:
            if not session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = session.getService(_REFDATA)
            req = svc.createRequest("ReferenceDataRequest")
            req.append("securities", ticker)
            req.append("fields", fld)
            if overrides:
                ovr = req.getElement("overrides")
                for k, v in overrides.items():
                    o = ovr.appendElement()
                    o.setElement("fieldId", k)
                    o.setElement("value", str(v))
            session.sendRequest(req)
            result = {}
            for msg in _drain(session):
                sec_data = msg.getElement("securityData")
                for i in range(sec_data.numValues()):
                    sec = sec_data.getValueAsElement(i)
                    t = sec.getElementAsString("security")
                    fd = sec.getElement("fieldData")
                    result[t] = _to_value(fd.getElement(fld)) if fd.hasElement(fld) else None
            return json.dumps(result)
        finally:
            session.stop()

    @mcp.tool(
        name="dividend",
        description="""Get Bloomberg dividend and stock split history for one or more securities.
        Returns historical dividend payments and stock split events.

        Ticker format: same as bdp (e.g. 'AAPL US Equity')
        typ: 'all' (dividends + splits), 'dividend' (dividends only), 'split' (splits only)
        start_date / end_date: 'YYYY-MM-DD' to filter history range

        Returns: ex-date, declared date, record date, pay date, amount, split ratio, frequency, currency.
        """
    )
    async def dividend(tickers: list[str], typ: str = "all", start_date: str | None = None, end_date: str | None = None, kwargs: dict[str, object] | None = None) -> str:
        fld = "DVD_HIST_ALL" if typ == "all" else "DVD_HIST" if typ == "dividend" else "SPLIT_HIST"
        overrides = dict(kwargs) if kwargs else {}
        if start_date:
            overrides["DVD_START_DT"] = _fmt_date(start_date)
        if end_date:
            overrides["DVD_END_DT"] = _fmt_date(end_date)
        session = _make_session()
        try:
            if not session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = session.getService(_REFDATA)
            req = svc.createRequest("ReferenceDataRequest")
            for t in tickers:
                req.append("securities", t)
            req.append("fields", fld)
            if overrides:
                ovr = req.getElement("overrides")
                for k, v in overrides.items():
                    o = ovr.appendElement()
                    o.setElement("fieldId", k)
                    o.setElement("value", str(v))
            session.sendRequest(req)
            result = {}
            for msg in _drain(session):
                sec_data = msg.getElement("securityData")
                for i in range(sec_data.numValues()):
                    sec = sec_data.getValueAsElement(i)
                    ticker = sec.getElementAsString("security")
                    fd = sec.getElement("fieldData")
                    result[ticker] = _to_value(fd.getElement(fld)) if fd.hasElement(fld) else []
            return json.dumps(result)
        finally:
            session.stop()

    @mcp.tool(
        name="beqs",
        description="""Run a saved Bloomberg equity screen (EQS) and return matching securities.
        Screens must be pre-built and saved in Bloomberg Terminal under EQS <GO>.

        screen: exact name of the saved screen in Bloomberg (case-sensitive)
        typ: 'PRIVATE' (your own screens, default) or 'GLOBAL' (Bloomberg public screens)
        group: screen group/folder name, default 'General'
        asof: run the screen as of a historical date 'YYYY-MM-DD' (if supported)

        Returns a list of securities matching the screen criteria.
        """
    )
    async def beqs(screen: str, asof: str | None = None, typ: str = "PRIVATE", group: str = "General", kwargs: dict[str, object] | None = None) -> str:
        session = _make_session()
        try:
            if not session.openService(_EXRSVC):
                raise RuntimeError(f"Failed to open {_EXRSVC}")
            svc = session.getService(_EXRSVC)
            req = svc.createRequest("RunScreenRequest")
            req.set("screenType", typ)
            req.set("screenName", screen)
            req.set("Group", group)
            if asof:
                req.set("asofDate", _fmt_date(asof))
            session.sendRequest(req)
            results = []
            for msg in _drain(session):
                if msg.hasElement("results"):
                    data = msg.getElement("results")
                    for i in range(data.numValues()):
                        results.append(_to_value(data.getValueAsElement(i)))
            return json.dumps(results)
        finally:
            session.stop()

    @mcp.tool(
        name="turnover",
        description="""Calculate daily trading turnover (value traded) for a basket of securities.
        Fetches historical PX_LAST and PX_VOLUME then computes turnover = price × volume / factor.

        Ticker format: same as bdp (e.g. 'AAPL US Equity')
        start_date / end_date: 'YYYY-MM-DD' date range
        ccy: informational label only — Bloomberg returns local currency values
        factor: divisor to scale the result, default 1e6 (returns values in millions)

        Example uses:
          - Compare daily liquidity across a stock universe
          - Filter out illiquid names from a portfolio
          - Analyse volume patterns over time for execution planning
        """
    )
    async def turnover(tickers: list[str], start_date: str | None = None, end_date: str | None = None, ccy: str = "USD", factor: float = 1e6) -> str:
        session = _make_session()
        try:
            if not session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = session.getService(_REFDATA)
            req = svc.createRequest("HistoricalDataRequest")
            for t in tickers:
                req.append("securities", t)
            req.append("fields", "PX_LAST")
            req.append("fields", "PX_VOLUME")
            if start_date:
                req.set("startDate", _fmt_date(start_date))
            req.set("endDate", _fmt_date(end_date or "today"))
            req.set("periodicitySelection", "DAILY")
            session.sendRequest(req)
            result = {}
            for msg in _drain(session):
                sec_data = msg.getElement("securityData")
                ticker = sec_data.getElementAsString("security")
                fd_array = sec_data.getElement("fieldData")
                rows = []
                for i in range(fd_array.numValues()):
                    row_elem = fd_array.getValueAsElement(i)
                    date_val = _to_value(row_elem.getElement("date"))
                    px = row_elem.getElementAsFloat("PX_LAST") if row_elem.hasElement("PX_LAST") else None
                    vol = row_elem.getElementAsFloat("PX_VOLUME") if row_elem.hasElement("PX_VOLUME") else None
                    tv = round((px * vol) / factor, 4) if px and vol else None
                    rows.append({"date": date_val, "PX_LAST": px, "PX_VOLUME": vol, "turnover": tv})
                result[ticker] = rows
            return json.dumps(result)
        finally:
            session.stop()

    mcp.run(transport=args.transport.value)
