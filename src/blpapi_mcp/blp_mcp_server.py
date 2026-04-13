
import csv
import io
import json
import datetime as dt
import socket

import blpapi
import blpapi.version
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.utilities.logging import get_logger

from . import types


_REFDATA      = "//blp/refdata"
_EXRSVC       = "//blp/exrsvc"
_BQLSVC       = "//blp/bqlsvc"
_INSTRUMENTS  = "//blp/instruments"
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


def _csv(rows: list[dict]) -> str:
    """Serialize a list of dicts to CSV. Token-efficient format for LLMs."""
    if not rows:
        return ""
    keys = list(dict.fromkeys(k for row in rows for k in row))
    # Drop columns where every value is None or empty — Bloomberg often returns null placeholders
    keys = [k for k in keys if any(row.get(k) not in (None, "") for row in rows)]
    if not keys:
        return ""
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(keys)
    for row in rows:
        vals = []
        for k in keys:
            v = row.get(k)
            if v is None:
                vals.append("")
            elif isinstance(v, float):
                vals.append(f"{v:.6g}")  # 6 significant figures, no trailing zeros
            else:
                vals.append(v)
        w.writerow(vals)
    return out.getvalue()


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
          Financials:  CUR_MKT_CAP, SALES_REV_TURN, EBITDA, CF_FREE_CASH_FLOW, BOOK_VAL_PER_SH
          Quality:     RETURN_ON_EQUITY, RETURN_ON_ASSET, GROSS_MARGIN, TOT_DEBT_TO_TOT_EQY
          Info:        NAME, TICKER, GICS_SECTOR_NAME, GICS_INDUSTRY_NAME, COUNTRY_ISO, CRNCY, EXCH_CODE
          Risk:        VOLATILITY_30D, VOLATILITY_90D, BETA_ADJUSTED_OVERRIDABLE, SHORT_INT_RATIO
          Estimates:   BEST_TARGET_PRICE, BEST_EPS, ANALYST_RATING
          Ownership:   EQY_INST_PCT_SH_OUT, SHARES_OUTSTANDING, FLOAT_SHARES_OUTSTANDING

        Consensus estimates (forward-looking, use with BEST_FPERIOD_OVERRIDE):
          BEST_EPS             — consensus EPS
          BEST_SALES           — consensus revenue
          BEST_EBIT            — consensus operating profit/EBIT (matches Terminal display)
          BEST_EBITDA          — consensus EBITDA
          BEST_NET_INCOME      — consensus net income
          BEST_EV_TO_BEST_EBITDA — consensus EV/EBITDA multiple

        Historical actuals (comparable/adjusted):
          IS_COMP_EPS_ADJUSTED — comparable EPS (excludes one-time items)
          IS_COMPARABLE_EBIT   — comparable EBIT (clean operating profit)
          These represent analyst-agreed "real earnings" for performance analysis.

        IMPORTANT field rules:
          - For consensus operating profit use BEST_EBIT, not BEST_OPER_INC
          - BEST_EBIT matches what the Bloomberg Terminal displays
          - NEVER use BEST_EPS_NXT_YR — use BEST_EPS with BEST_FPERIOD_OVERRIDE instead
          - IS_COMP_* fields are for historical actuals only

        kwargs: Bloomberg field overrides as key/value pairs.

        BEST_FPERIOD_OVERRIDE format:
          "2026Y"  — fiscal year 2026
          "2027Y"  — fiscal year 2027
          "2026Q1" — Q1 2026
          "2026Q2" — Q2 2026
          "2027Q4" — Q4 2027

        Example — FY2026 and FY2027 consensus estimates:
          bdp(tickers=['VOLVB SS Equity'],
              flds=['BEST_EPS', 'BEST_SALES', 'BEST_EBIT'],
              kwargs={'BEST_FPERIOD_OVERRIDE': '2026Y'})

        Example — Q1 2026 consensus estimates:
          bdp(tickers=['VOLVB SS Equity'],
              flds=['BEST_EPS', 'BEST_SALES', 'BEST_EBIT'],
              kwargs={'BEST_FPERIOD_OVERRIDE': '2026Q1'})
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
            if len(result) == 1:
                rows = [fields for fields in result.values()]
            else:
                rows = [{"ticker": t, **fields} for t, fields in result.items()]
            return _csv(rows)
        finally:
            session.stop()

    @mcp.tool(
        name="bds",
        description="""Get Bloomberg bulk/block data — returns multi-row datasets for a security.
        Use this when a field returns a table of data rather than a single value.

        Ticker format: same as bdp (e.g. 'AAPL US Equity')

        Common fields:
          Holders:      TOP_20_HOLDERS_PUBLIC_FILINGS (top 20 institutional holders with % owned)
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
            rows = []
            multi_ticker = len(result) > 1
            multi_field = len(flds) > 1
            for ticker, fields in result.items():
                for field, data in fields.items():
                    prefix: dict = {}
                    if multi_ticker:
                        prefix["ticker"] = ticker
                    if multi_field:
                        prefix["field"] = field
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                rows.append({**prefix, **item})
                            else:
                                rows.append({**prefix, "value": item})
                    else:
                        rows.append({**prefix, "value": data})
            return _csv(rows)
        finally:
            session.stop()

    @mcp.tool(
        name="bdh",
        description="""Get Bloomberg historical time series data for one or more securities.
        Returns historical data between start_date and end_date.

        Ticker format: same as bdp (e.g. 'AAPL US Equity')
        Date format: 'YYYY-MM-DD' or 'today'
        periodicity: 'DAILY' (default), 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'SEMI_ANNUALLY', 'YEARLY'
        adjust: 'all' (splits+dividends), 'dvd' (dividends only), 'split' (splits only), None
        kwargs: Bloomberg field overrides as key/value pairs (e.g. BEST_FPERIOD_OVERRIDE, EQY_FUND_CRNCY)

        Fields: same set as bdp. Useful additions for time series:
          DAY_TO_DAY_TOT_RETURN_GROSS_DVDS — total return including dividends
          For estimates use periodicity='QUARTERLY' or 'YEARLY' with BEST_EPS, BEST_SALES etc.
        """
    )
    async def bdh(tickers: list[str], flds: list[str], start_date: str | None = None, end_date: str = "today", periodicity: str = "DAILY", adjust: str | None = None, kwargs: dict[str, object] | None = None) -> str:
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
            req.set("periodicitySelection", periodicity)
            if adjust in ("all", "dvd"):
                req.set("adjustmentNormal", True)
                req.set("adjustmentAbnormal", True)
            if adjust in ("all", "split"):
                req.set("adjustmentSplit", True)
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
            if len(result) == 1:
                all_rows = [row for rows in result.values() for row in rows]
            else:
                all_rows = [{"ticker": ticker, **row} for ticker, rows in result.items() for row in rows]
            return _csv(all_rows)
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
            if kwargs:
                for k, v in kwargs.items():
                    req.set(k, v)
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
            return _csv(bars)
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
        Warning: can return very large datasets for liquid securities — use time_range or max_rows to limit.
        max_rows: cap output at this many ticks (default 5000); response includes truncation warning if hit.
        """
    )
    async def bdtick(ticker: str, date: str, session: str = "allday", time_range: tuple[str, ...] | None = None, event_types: list[str] | None = None, max_rows: int = 5000, kwargs: dict[str, object] | None = None) -> str:
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
            if kwargs:
                for k, v in kwargs.items():
                    req.set(k, v)
            blp_session.sendRequest(req)
            ticks = []
            truncated = False
            for msg in _drain(blp_session):
                if truncated:
                    break
                tick_data = msg.getElement("tickData").getElement("tickData")
                for i in range(tick_data.numValues()):
                    if len(ticks) >= max_rows:
                        truncated = True
                        break
                    tick = tick_data.getValueAsElement(i)
                    ticks.append({
                        "time":  _to_value(tick.getElement("time")),
                        "type":  tick.getElementAsString("type"),
                        "value": tick.getElementAsFloat("value"),
                        "size":  tick.getElementAsInteger("size") if tick.hasElement("size") else None,
                    })
            result = _csv(ticks)
            if truncated:
                result = f"# WARNING: truncated at {max_rows} rows — use time_range or max_rows to narrow\n" + result
            return result
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
            rows = []
            multi_ticker = len(result) > 1
            for t, data in result.items():
                prefix = {"ticker": t} if multi_ticker else {}
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            rows.append({**prefix, **item})
                        else:
                            rows.append({**prefix, "value": item})
                else:
                    rows.append({**prefix, "value": data})
            return _csv(rows)
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
            rows = []
            multi_ticker = len(result) > 1
            for t, data in result.items():
                prefix = {"ticker": t} if multi_ticker else {}
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            rows.append({**prefix, **item})
                        else:
                            rows.append({**prefix, "value": item})
                else:
                    rows.append({**prefix, "value": data})
            return _csv(rows)
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
            if not session.openService(_REFDATA):
                raise RuntimeError(f"Failed to open {_REFDATA}")
            svc = session.getService(_REFDATA)
            req = svc.createRequest("BeqsRequest")
            req.set("screenName", screen)
            req.set("screenType", typ)
            req.set("Group", group)
            if asof or kwargs:
                ovr = req.getElement("overrides")
                if asof:
                    o = ovr.appendElement()
                    o.setElement("fieldId", "REFERENCE_DATE")
                    o.setElement("value", _fmt_date(asof))
                if kwargs:
                    for k, v in kwargs.items():
                        o = ovr.appendElement()
                        o.setElement("fieldId", k)
                        o.setElement("value", str(v))
            session.sendRequest(req)
            results = []
            for msg in _drain(session):
                if msg.hasElement("responseError"):
                    err = msg.getElement("responseError")
                    code = err.getElementAsInteger("code") if err.hasElement("code") else "?"
                    message = err.getElementAsString("message") if err.hasElement("message") else str(err)
                    raise RuntimeError(f"BeqsRequest error {code}: {message}")
                if msg.hasElement("data"):
                    sec_data = msg.getElement("data").getElement("securityData")
                    for i in range(sec_data.numValues()):
                        sec = sec_data.getValueAsElement(i)
                        results.append({"security": sec.getElementAsString("security")})
                else:
                    # Unexpected message shape — surface it so failures aren't silent
                    elements = [str(msg.getElement(i).name()) for i in range(msg.numElements())]
                    raise RuntimeError(
                        f"Unexpected BeqsRequest response (type={msg.messageType()}, "
                        f"elements={elements}): {msg.toString()[:400]}"
                    )
            return _csv(results)
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
                    rows.append({"date": date_val, "turnover": tv})
                result[ticker] = rows
            if len(result) == 1:
                all_rows = [row for rows in result.values() for row in rows]
            else:
                all_rows = [{"ticker": ticker, **row} for ticker, rows in result.items() for row in rows]
            return _csv(all_rows)
        finally:
            session.stop()

    @mcp.tool(
        name="bql",
        description="""Run a Bloomberg Query Language (BQL) query. More powerful than bdp/bdh for
        estimates, financials, cross-sectional screens, and derived calculations.

        BQL syntax:
          get(<fields>) for(<universe>)

        Universe examples:
          ['AAPL US Equity', 'MSFT US Equity']   — explicit list
          members('SPX Index')                    — index members
          filter(members('SPX Index'), PE_RATIO < 15)  — filtered universe

        Field examples (cross-sectional, one value per security):
          PX_LAST, PE_RATIO, CUR_MKT_CAP, NAME
          BEST_EPS(fperiod='1Q2025')              — consensus estimate for a specific quarter
          BEST_EPS(fperiod='1FY')                 — next fiscal year estimate
          IS_EPS(fperiod='1Q2024')                — reported EPS for a past quarter

        Field examples (time series, returns rows per date):
          PX_LAST(dates=range(-1Y, 0D))                          — 1 year of daily prices
          IS_EPS(periodicity=QUARTERLY, dates=range(-8Q, 0Q))    — 8 quarters of reported EPS
          BEST_EPS(periodicity=QUARTERLY, dates=range(-8Q, 0Q))  — 8 quarters of consensus EPS

        Multiple fields in one query:
          get(PX_LAST, PE_RATIO, BEST_EPS(fperiod='1FY')) for(members('SPX Index'))

        Returns rows with 'field' (expression name), 'id' (security), 'value',
        and any secondary columns (e.g. 'DATE', 'PERIOD').
        Note: BQL requires a separate API entitlement beyond Terminal access.
        """
    )
    async def bql(query: str) -> str:
        session = _make_session()
        try:
            if not session.openService(_BQLSVC):
                raise RuntimeError("Failed to open //blp/bqlsvc — BQL may require a separate entitlement")
            svc = session.getService(_BQLSVC)
            req = svc.createRequest("sendQuery")
            req.set("expression", query)
            session.sendRequest(req)

            tables: dict = {}
            for msg in _drain(session):
                if msg.hasElement("responseError"):
                    err = msg.getElement("responseError")
                    raise RuntimeError(str(err))
                if not msg.hasElement("results"):
                    continue
                results = msg.getElement("results")
                for i in range(results.numValues()):
                    res = results.getValueAsElement(i)
                    name = res.getElementAsString("name") if res.hasElement("name") else str(i)
                    id_col = res.getElement("idColumn")
                    id_vals = _to_value(id_col.getElement("values"))
                    val_col = res.getElement("valuesColumn")
                    val_vals = _to_value(val_col.getElement("values"))
                    sec_cols: dict = {}
                    if res.hasElement("secondaryColumns"):
                        sc = res.getElement("secondaryColumns")
                        for j in range(sc.numValues()):
                            col = sc.getValueAsElement(j)
                            col_name = col.getElementAsString("name")
                            sec_cols[col_name] = _to_value(col.getElement("values"))
                    rows = []
                    for k, (id_v, val_v) in enumerate(zip(id_vals, val_vals)):
                        row: dict = {"id": id_v, "value": val_v}
                        for col_name, col_vals in sec_cols.items():
                            row[col_name] = col_vals[k] if k < len(col_vals) else None
                        rows.append(row)
                    tables[name] = rows

            rows = [{"field": fname, **row} for fname, field_rows in tables.items() for row in field_rows]
            return _csv(rows)
        finally:
            session.stop()


    @mcp.tool(
        name="instruments",
        description="""Search for Bloomberg securities by name or keyword using //blp/instruments.

        query: search string e.g. company name "Volvo" or "Apple"
        typ: asset class filter - 'Corp', 'Equity', 'Govt', 'Mtge', 'Muni', 'Pfd', 'Curncy', 'Index', 'Comdty'
        max_results: number of results to return, default 20

        Returns matching Bloomberg tickers and security names.
        Useful for discovering bond tickers, options, and other securities where the ticker is not known.
        """
    )
    async def instruments(query: str, typ: str = "Corp", max_results: int = 20) -> str:
        session = _make_session()
        try:
            if not session.openService(_INSTRUMENTS):
                raise RuntimeError(f"Failed to open {_INSTRUMENTS}")
            svc = session.getService(_INSTRUMENTS)
            ops = [svc.getOperation(i).name() for i in range(svc.numOperations())]
            if "SecurityLookupRequest" not in ops:
                raise RuntimeError(
                    f"SecurityLookupRequest not available on {_INSTRUMENTS}; available: {ops}"
                )
            req = svc.createRequest("SecurityLookupRequest")
            req.set("query", query)
            req.set("yellowKeyFilter", f"YK_FILTER_{typ.upper()}")
            req.set("maxResults", max_results)
            session.sendRequest(req)
            results = []
            for msg in _drain(session):
                if msg.hasElement("responseError"):
                    err = msg.getElement("responseError")
                    code = err.getElementAsInteger("code") if err.hasElement("code") else "?"
                    message = err.getElementAsString("message") if err.hasElement("message") else str(err)
                    raise RuntimeError(f"SecurityLookupRequest error {code}: {message}")
                elif msg.hasElement("results"):
                    res = msg.getElement("results")
                    for i in range(res.numValues()):
                        item = res.getValueAsElement(i)
                        row = {}
                        if item.hasElement("security"):
                            row["security"] = item.getElementAsString("security")
                        if item.hasElement("description"):
                            row["description"] = item.getElementAsString("description")
                        results.append(row)
                else:
                    elements = [str(msg.getElement(i).name()) for i in range(msg.numElements())]
                    raise RuntimeError(
                        f"Unexpected SecurityLookupRequest response (type={msg.messageType()}, "
                        f"elements={elements}): {msg.toString()[:400]}"
                    )
            return _csv(results)
        finally:
            session.stop()

    if args.transport == types.Transport.HTTP:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as _s:
            _s.connect(("8.8.8.8", 80))
            local_ip = _s.getsockname()[0]
        print(f"Bloomberg MCP server listening on http://{args.host}:{args.port}/mcp")
        print(f"Connect clients to: http://{local_ip}:{args.port}/mcp")
    mcp.run(transport=args.transport.value)
