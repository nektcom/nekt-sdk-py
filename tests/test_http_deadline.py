"""Every SDK HTTP call has a wall-clock ceiling, even when DNS never answers (NEKT-5259).

`requests` timeouts are applied after name resolution, and `socket.getaddrinfo`
takes no timeout — a dropped DNS query blocked a webhook POST for 18 hours. These
tests stall the resolver for real and assert the call comes back anyway.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

import nekt.http as nekt_http
from nekt.api import DEFAULT_TIMEOUT, UPLOAD_PART_TIMEOUT, NektAPI
from nekt.http import DeadlineExceeded, DeadlineSession, call_with_deadline, deadline_for


@pytest.fixture
def stalled_dns(monkeypatch):
    """Make `socket.getaddrinfo` hang the first `stalls` times for a host, like a dropped query."""
    release = threading.Event()
    real_getaddrinfo = socket.getaddrinfo
    state = {"stalls": 1, "calls": 0}

    def getaddrinfo(host, *args, **kwargs):
        if host == "stalled.test":
            state["calls"] += 1
            if state["calls"] <= state["stalls"]:
                release.wait(600)  # a dropped query: no answer, no error
            host = "127.0.0.1"
        return real_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    yield state
    release.set()  # let abandoned worker threads finish


@pytest.fixture
def short_deadlines(monkeypatch):
    monkeypatch.setattr(nekt_http, "DEADLINE_HEADROOM_SECONDS", 0.3)


@pytest.fixture
def api_server():
    """A local HTTP/1.1 server answering 200 {"value": "s3cret"} to every GET."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            body = b'{"value": "s3cret"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # silence
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1]
    server.shutdown()


def test_deadline_is_sized_from_the_timeout():
    assert deadline_for(DEFAULT_TIMEOUT) == 180
    # A 100 MB part upload keeps its long read window.
    assert deadline_for(UPLOAD_PART_TIMEOUT) == 660
    assert deadline_for(None) == 180
    assert deadline_for(5) == 60


def test_stalled_dns_raises_within_the_deadline(stalled_dns, short_deadlines, caplog):
    session = DeadlineSession()
    started = time.monotonic()

    with caplog.at_level(logging.WARNING, logger="nekt.http"):
        with pytest.raises(DeadlineExceeded):
            session.get("http://stalled.test:1/", timeout=(0.1, 0.1))

    assert time.monotonic() - started < 5
    assert any("exceeded" in r.getMessage() and "stalled.test" in r.getMessage() for r in caplog.records)


def test_deadline_breach_is_retryable_by_existing_policies():
    # tenacity (`TRANSIENT_EXCEPTIONS`) and backoff (`RequestException`) both catch it.
    assert issubclass(DeadlineExceeded, requests.exceptions.Timeout)
    assert issubclass(DeadlineExceeded, requests.exceptions.RequestException)


def test_api_call_recovers_after_a_dns_stall(stalled_dns, short_deadlines, api_server, monkeypatch):
    """The first attempt wedges in DNS, trips the deadline, and the retry succeeds."""
    api = NektAPI(data_access_token="t", api_url=f"http://stalled.test:{api_server}")
    # Shrink this call's timeout so the test deadline is ~0.5s rather than 180s.
    real_request = DeadlineSession.request
    monkeypatch.setattr(DeadlineSession, "request", lambda self, m, u, **kw: real_request(self, m, u, **{**kw, "timeout": (0.1, 0.1)}))

    assert api.load_secret("k") == "s3cret"
    assert stalled_dns["calls"] == 2


def test_errors_from_the_call_are_reraised_unchanged():
    def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        call_with_deadline(boom, deadline=5)


def test_session_defaults_a_timeout_when_the_caller_passes_none(monkeypatch):
    seen = {}

    def fake_request(self, method, url, *args, **kwargs):
        seen["timeout"] = kwargs["timeout"]
        return "ok"

    monkeypatch.setattr(requests.Session, "request", fake_request)
    assert DeadlineSession().get("http://example.test/") == "ok"
    assert seen["timeout"] == DEFAULT_TIMEOUT


def test_api_sessions_are_deadline_sessions():
    api = NektAPI(data_access_token="t", api_url="https://api.test")
    assert isinstance(api._session, DeadlineSession)
    assert isinstance(api._storage_session, DeadlineSession)
