"""Unit tests for _BloombergSession reset behaviour (no real Bloomberg connection)."""

import blpapi
import pytest

from blpapi_mcp import blp_mcp_server as m


class _FakeSession:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


@pytest.fixture
def bb(monkeypatch):
    """A _BloombergSession whose connect/open steps are stubbed out."""
    made = []

    def fake_make():
        s = _FakeSession()
        made.append(s)
        return s

    monkeypatch.setattr(m, "_make_session", fake_make)
    monkeypatch.setattr(m, "_open_service", lambda session, name: f"svc:{name}")
    monkeypatch.setattr(m.blpapi, "EventQueue", lambda: object())
    session = m._BloombergSession()
    session.made = made
    return session


def _use(bb, exc):
    with pytest.raises(type(exc)):
        with bb.request(m._REFDATA):
            raise exc


def test_reuses_session_and_service(bb):
    with bb.request(m._REFDATA) as (_, svc1):
        pass
    with bb.request(m._REFDATA) as (_, svc2):
        pass
    assert svc1 == svc2 == f"svc:{m._REFDATA}"
    assert len(bb.made) == 1


@pytest.mark.parametrize(
    "exc",
    [
        m._BloombergTimeout("timed out"),
        blpapi.exception.InvalidStateException("Session not started", 0),
        blpapi.exception.UnknownErrorException("boom", 0),
    ],
)
def test_session_errors_reset_session(bb, exc):
    _use(bb, exc)
    assert bb.made[0].stopped
    assert bb._session is None
    # Next call reconnects.
    with bb.request(m._REFDATA):
        pass
    assert len(bb.made) == 2


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("bad param"),
        blpapi.exception.NotFoundException("no such element", 0),
        blpapi.exception.InvalidArgumentException("bad arg", 0),
    ],
)
def test_parameter_errors_keep_session(bb, exc):
    _use(bb, exc)
    assert not bb.made[0].stopped
    assert bb._session is bb.made[0]
    with bb.request(m._REFDATA):
        pass
    assert len(bb.made) == 1


def test_reset_ignores_stale_session(bb):
    with bb.request(m._REFDATA) as (_, _svc):
        first = bb._session
    # Another thread already replaced the session; resetting the old one is a no-op.
    bb._reset(first)
    assert bb._session is None
    with bb.request(m._REFDATA):
        pass
    bb._reset(first)  # stale handle
    assert bb._session is bb.made[1]
    assert not bb.made[1].stopped
