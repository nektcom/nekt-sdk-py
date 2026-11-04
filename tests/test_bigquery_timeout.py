"""BigQuery clients must never issue an HTTP call without a timeout (NEKT-6284).

google-cloud-bigquery passes ``timeout=None`` for every call made without an
explicit one; a socket severed under such a call (a Cloud Run maintenance
migration) then blocks forever. ``build_bigquery_client`` hands the client a
session that substitutes a default and enforces a wall-clock deadline.
"""

from __future__ import annotations

import time

import pytest
from google.auth.transport.requests import AuthorizedSession
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.oauth2.credentials import Credentials

import nekt.http as nekt_http
from nekt.http import DEFAULT_TIMEOUT, DeadlineExceeded
from nekt.provider import bigquery as provider


@pytest.fixture
def captured(monkeypatch):
    calls: list[tuple[str, str, object]] = []

    def fake_request(self, method, url, *args, **kwargs):
        calls.append((method, url, kwargs.get("timeout")))
        return "response"

    monkeypatch.setattr(AuthorizedSession, "request", fake_request)
    return calls


def _session(timeout=provider.BIGQUERY_TIMEOUT):
    return provider._timeout_session_class()(Credentials(token="t"), timeout=timeout)


def test_none_timeout_is_replaced_with_the_default(captured):
    # google-cloud-core calls the session with every argument by keyword.
    out = _session().request(url="https://bigquery.googleapis.com/x", method="GET", headers={}, data=None, timeout=None)
    assert out == "response"
    assert captured == [("GET", "https://bigquery.googleapis.com/x", DEFAULT_TIMEOUT)]


def test_explicit_timeout_is_honoured(captured):
    _session().request("POST", "https://bigquery.googleapis.com/y", timeout=7)
    assert captured[0][2] == 7


def test_session_default_is_configurable(captured):
    _session(timeout=(1, 2)).request("GET", "https://bigquery.googleapis.com/z")
    assert captured[0][2] == (1, 2)


def test_stalled_request_raises_a_retryable_timeout(monkeypatch):
    def stalled(self, method, url, *args, **kwargs):
        time.sleep(5)
        return "late"

    monkeypatch.setattr(AuthorizedSession, "request", stalled)
    monkeypatch.setattr(nekt_http, "DEADLINE_HEADROOM_SECONDS", 0)
    with pytest.raises(DeadlineExceeded) as info:
        _session(timeout=(0.05, 0.05)).request("GET", "https://bigquery.googleapis.com/q", timeout=None)
    # The library's own retry policy must consider it transient, so the call is
    # retried on a fresh connection instead of failing the run.
    assert DEFAULT_RETRY._predicate(info.value)


def test_build_bigquery_client_installs_the_session():
    client = provider.build_bigquery_client("proj", Credentials(token="t"))
    assert client.project == "proj"
    assert isinstance(client._http, provider._timeout_session_class())
    assert client._http._default_timeout == provider.BIGQUERY_TIMEOUT


def test_build_bigquery_client_uses_adc_when_no_credentials(monkeypatch):
    import google.auth

    monkeypatch.setattr(google.auth, "default", lambda scopes=None: (Credentials(token="adc"), "adc-project"))
    client = provider.build_bigquery_client()
    assert client.project == "adc-project"
    assert isinstance(client._http, provider._timeout_session_class())
    # An explicit project still wins over the ADC project.
    assert provider.build_bigquery_client("mine").project == "mine"
