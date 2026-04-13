# options_screen — parked until //blp/fo-discovery entitlement is available.
#
# To activate: copy the tool function into serve() in blp_mcp_server.py and
# add _FO = "//blp/fo-discovery" to the module-level constants.

_FO = "//blp/fo-discovery"

OPTIONS_SCREEN_TOOL = dict(
    name="options_screen",
    description="""Discover listed options for a given underlying security using //blp/fo-discovery.

        underlying: Bloomberg ticker e.g. 'VOLVB SS Equity', 'SPX Index', 'SX5E Index'
        expiry_from: filter options expiring from this date 'YYYYMMDD'
        expiry_to: filter options expiring to this date 'YYYYMMDD'
        periodicity: 'D' daily, 'W' weekly, 'M' monthly, 'Q' quarterly
        put_call: 'C' calls only, 'P' puts only, omit for both
        strike_min: minimum strike price
        strike_max: maximum strike price

        Returns all fields available for each option (SECURITY, OPT_EXPIRE_DT, OPT_PUT_CALL, STRIKE,
        plus greeks/vol if included in your entitlement).
    """,
)


async def options_screen(
    _make_session,
    _drain,
    _to_value,
    _csv,
    underlying: str,
    expiry_from: str | None = None,
    expiry_to: str | None = None,
    periodicity: str | None = None,
    put_call: str | None = None,
    strike_min: float | None = None,
    strike_max: float | None = None,
) -> str:
    session = _make_session()
    try:
        if not session.openService(_FO):
            raise RuntimeError(f"Failed to open {_FO}")
        svc = session.getService(_FO)
        ops = [svc.getOperation(i).name() for i in range(svc.numOperations())]
        if "OptionsScreenRequest" not in ops:
            raise RuntimeError(
                f"OptionsScreenRequest not available on {_FO}; available: {ops}"
            )
        req = svc.createRequest("OptionsScreenRequest")

        und = req.getElement("SEARCH_CRITERIA").getElement("UNDERLYING")
        und.setElement("UNDERLYING_SECURITY", underlying)
        und.setElement("UNDERLYING_TYPE", "PARSEKYABLE_DES_SOURCE")

        filters = req.getElement("FILTER_FIELDS")
        if expiry_from:
            filters.setElement("OPT_EXPIRE_DT_GTEQ", expiry_from)
        if expiry_to:
            filters.setElement("OPT_EXPIRE_DT_LTEQ", expiry_to)
        if periodicity:
            filters.setElement("EXPIRATION_PERIODICITY", periodicity)
        if put_call:
            filters.setElement("OPT_PUT_CALL", put_call)
        if strike_min is not None:
            filters.setElement("OPT_STRIKE_PX_GTEQ", strike_min)
        if strike_max is not None:
            filters.setElement("OPT_STRIKE_PX_LTEQ", strike_max)

        session.sendRequest(req)
        results = []
        for msg in _drain(session):
            if msg.hasElement("responseError"):
                err = msg.getElement("responseError")
                code = err.getElementAsInteger("code") if err.hasElement("code") else "?"
                message = err.getElementAsString("message") if err.hasElement("message") else str(err)
                raise RuntimeError(f"OptionsScreenRequest error {code}: {message}")
            elif msg.hasElement("data"):
                data = msg.getElement("data")
                for i in range(data.numValues()):
                    row_elem = data.getValueAsElement(i)
                    row = {
                        str(row_elem.getElement(j).name()): _to_value(row_elem.getElement(j))
                        for j in range(row_elem.numElements())
                    }
                    results.append(row)
            else:
                elements = [str(msg.getElement(i).name()) for i in range(msg.numElements())]
                raise RuntimeError(
                    f"Unexpected OptionsScreenRequest response (type={msg.messageType()}, "
                    f"elements={elements}): {msg.toString()[:400]}"
                )
        return _csv(results)
    finally:
        session.stop()
