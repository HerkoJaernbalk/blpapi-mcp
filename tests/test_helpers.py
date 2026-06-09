"""Unit tests for the pure (no Bloomberg session) helpers in blp_mcp_server."""

import datetime as dt

import pytest

from blpapi_mcp import types
from blpapi_mcp.blp_mcp_server import (
    _csv,
    _flatten_by_ticker,
    _fmt_date,
    _parse_datetime,
    _session_window,
    _single_field_rows,
)


class TestCsv:
    def test_empty(self):
        assert _csv([]) == ""

    def test_basic_rows(self):
        out = _csv([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
        assert out.splitlines() == ["a,b", "1,x", "2,y"]

    def test_drops_all_empty_columns(self):
        out = _csv([{"a": 1, "b": None}, {"a": 2, "b": ""}])
        assert out.splitlines() == ["a", "1", "2"]

    def test_all_columns_empty(self):
        assert _csv([{"a": None}, {"a": ""}]) == ""

    def test_none_becomes_empty_cell(self):
        out = _csv([{"a": 1, "b": None}, {"a": 2, "b": 3}])
        assert out.splitlines() == ["a,b", "1,", "2,3"]

    def test_float_formatting_six_sig_figs(self):
        out = _csv([{"v": 123.4567891}])
        assert out.splitlines() == ["v", "123.457"]

    def test_preserves_first_seen_column_order(self):
        out = _csv([{"a": 1}, {"b": 2, "a": 3}])
        assert out.splitlines()[0] == "a,b"


class TestFmtDate:
    def test_iso_date(self):
        assert _fmt_date("2024-01-02") == "20240102"

    def test_today(self):
        assert _fmt_date("today") == dt.date.today().strftime("%Y%m%d")


class TestParseDatetime:
    def test_returns_native_datetime(self):
        # Regression: blpapi.datetime is a module, not a constructor — passing a
        # native datetime to Request.set is the supported path.
        result = _parse_datetime("2024-01-02", "09:30:00")
        assert result == dt.datetime(2024, 1, 2, 9, 30, 0)

    def test_rejects_bad_input(self):
        with pytest.raises(ValueError):
            _parse_datetime("2024-13-02", "09:30:00")


class TestSessionWindow:
    def test_known_session(self):
        assert _session_window("pre") == ("04:00:00", "09:30:00")

    def test_unknown_session_raises(self):
        with pytest.raises(ValueError, match="Unknown session"):
            _session_window("lunch")


class TestSingleFieldRows:
    def test_single_ticker_scalar(self):
        assert _single_field_rows({"AAPL US Equity": 1.5}) == [{"value": 1.5}]

    def test_single_ticker_list_of_dicts(self):
        rows = _single_field_rows({"AAPL US Equity": [{"d": 1}, {"d": 2}]})
        assert rows == [{"d": 1}, {"d": 2}]

    def test_multi_ticker_prefixes_ticker(self):
        rows = _single_field_rows({"A": 1, "B": [{"d": 2}]})
        assert rows == [{"ticker": "A", "value": 1}, {"ticker": "B", "d": 2}]

    def test_error_dict(self):
        rows = _single_field_rows({"A": {"error": "bad security"}})
        assert rows == [{"error": "bad security"}]


class TestFlattenByTicker:
    def test_single_ticker_no_prefix(self):
        result = _flatten_by_ticker({"A": [{"x": 1}, {"x": 2}]})
        assert result == [{"x": 1}, {"x": 2}]

    def test_multi_ticker_prefixed(self):
        result = _flatten_by_ticker({"A": [{"x": 1}], "B": [{"x": 2}]})
        assert result == [{"ticker": "A", "x": 1}, {"ticker": "B", "x": 2}]


class TestStartupArgs:
    def test_str_is_json(self):
        args = types.StartupArgs(transport=types.Transport.HTTP, host="0.0.0.0", port=8080)
        assert str(args) == '{"transport": "streamable-http", "host": "0.0.0.0", "port": 8080}'
