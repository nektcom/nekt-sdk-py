"""Unit tests for NektAPI.get_file_download_url* (presigned download URLs)."""

from __future__ import annotations

import pytest

from nekt.api import NektAPI
from nekt.exceptions import FileDownloadError


def _api() -> NektAPI:
    # Construction does no network I/O; it only sets session headers.
    return NektAPI(data_access_token="test-token", api_url="https://api.test")


class _Resp:
    ok = True

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def _capture_get(api: NektAPI, monkeypatch, payload: dict) -> dict:
    captured: dict = {}

    def fake_get(url, *args, **kwargs):
        captured["url"] = url
        return _Resp(payload)

    monkeypatch.setattr(api._session, "get", fake_get)
    return captured


def test_get_file_download_url_layer_scoped(monkeypatch):
    api = _api()
    captured = _capture_get(api, monkeypatch, {"download_url": "https://dl/abc"})

    url = api.get_file_download_url("raw", "vol", "doc.pdf")

    assert url == "https://dl/abc"
    assert captured["url"] == "https://api.test/api/v1/i/layers/raw/volumes/vol/files/doc.pdf/download/"


def test_get_file_download_url_by_volume_id_is_layerless(monkeypatch):
    api = _api()
    captured = _capture_get(api, monkeypatch, {"download_url": "https://dl/abc"})

    url = api.get_file_download_url_by_volume_id("vol-123", "doc.pdf")

    assert url == "https://dl/abc"
    assert captured["url"] == "https://api.test/api/v1/i/volumes/vol-123/files/doc.pdf/download/"
    assert "/layers/" not in captured["url"]


def test_get_file_download_url_by_file_id_uses_files_endpoint(monkeypatch):
    api = _api()
    captured = _capture_get(api, monkeypatch, {"download_url": "https://dl/abc"})

    url = api.get_file_download_url_by_file_id("file-123")

    assert url == "https://dl/abc"
    assert captured["url"] == "https://api.test/api/v1/i/files/file-123/download/"


def test_missing_download_url_raises(monkeypatch):
    api = _api()
    _capture_get(api, monkeypatch, {})
    with pytest.raises(FileDownloadError, match="No download URL"):
        api.get_file_download_url("raw", "vol", "doc.pdf")


@pytest.mark.parametrize(
    "call",
    [
        lambda api: api.get_file_download_url("", "vol", "f"),
        lambda api: api.get_file_download_url("raw", "", "f"),
        lambda api: api.get_file_download_url("raw", "vol", ""),
        lambda api: api.get_file_download_url_by_volume_id("", "f"),
        lambda api: api.get_file_download_url_by_volume_id("vol", ""),
        lambda api: api.get_file_download_url_by_file_id(""),
    ],
)
def test_empty_identifiers_rejected(call):
    api = _api()
    with pytest.raises(ValueError):
        call(api)


# --- get_download_url: one entry point, several identifier shapes ----------


def test_get_download_url_by_file_id(monkeypatch):
    api = _api()
    captured = _capture_get(api, monkeypatch, {"download_url": "https://dl/abc"})

    assert api.get_download_url(file_id="file-123") == "https://dl/abc"
    assert captured["url"].endswith("/api/v1/i/files/file-123/download/")


def test_get_download_url_by_volume_and_name(monkeypatch):
    api = _api()
    captured = _capture_get(api, monkeypatch, {"download_url": "https://dl/abc"})

    api.get_download_url(volume="vol-123", file_name="doc.pdf")

    assert captured["url"].endswith("/api/v1/i/volumes/vol-123/files/doc.pdf/download/")
    assert "/layers/" not in captured["url"]


def test_get_download_url_with_layer_is_layer_scoped(monkeypatch):
    api = _api()
    captured = _capture_get(api, monkeypatch, {"download_url": "https://dl/abc"})

    api.get_download_url(layer="Raw", volume="documents", file_name="doc.pdf")

    assert captured["url"].endswith(
        "/api/v1/i/layers/Raw/volumes/documents/files/doc.pdf/download/",
    )


def test_file_id_takes_precedence_over_the_rest(monkeypatch):
    """Records commonly carry every identifier; precedence beats rejecting them."""
    api = _api()
    captured = _capture_get(api, monkeypatch, {"download_url": "https://dl/abc"})

    api.get_download_url(
        file_id="file-123",
        volume="vol-123",
        layer="Raw",
        file_name="doc.pdf",
    )

    assert captured["url"].endswith("/api/v1/i/files/file-123/download/")


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"volume": "vol-123"},
        {"file_name": "doc.pdf"},
        {"layer": "Raw", "file_name": "doc.pdf"},
        {"layer": "Raw", "volume": "vol-123"},
    ],
)
def test_insufficient_identifiers_rejected(kwargs):
    api = _api()
    with pytest.raises(ValueError, match="Not enough identifiers"):
        api.get_download_url(**kwargs)


# --- download_file ---------------------------------------------------------


class _StreamResp:
    def __init__(self, body: bytes, status_code: int = 200) -> None:
        self.body = body
        self.status_code = status_code

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def iter_content(self, chunk_size):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start : start + chunk_size]


def _capture_storage_get(monkeypatch, response, api=None):
    """Intercept the credential-free session used for the presigned fetch."""
    captured = {}

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return response

    import requests as _requests

    session = _requests.Session()
    session.get = fake_get
    monkeypatch.setattr(
        "nekt.api.NektAPI._storage_session",
        property(lambda _self: session),
    )
    captured["session"] = session
    return captured


def test_download_file_writes_the_bytes(monkeypatch, tmp_path):
    api = _api()
    _capture_get(api, monkeypatch, {"download_url": "https://storage.test/signed?sig=1"})
    _capture_storage_get(monkeypatch, _StreamResp(b"\xff\xd8\xffpayload"))

    dest = tmp_path / "out.jpg"
    returned = api.download_file(str(dest), file_id="file-123")

    assert returned == str(dest)
    assert dest.read_bytes() == b"\xff\xd8\xffpayload"


def test_download_file_does_not_send_nekt_credentials_to_storage(monkeypatch, tmp_path):
    """The presigned URL is object storage; our token must never go there."""
    api = _api()
    _capture_get(api, monkeypatch, {"download_url": "https://storage.test/signed?sig=1"})
    captured = _capture_storage_get(monkeypatch, _StreamResp(b"data"))

    api.download_file(str(tmp_path / "out.bin"), file_id="file-123")

    # Nothing auth-shaped is passed explicitly...
    assert "headers" not in captured["kwargs"]
    assert "auth" not in captured["kwargs"]
    assert captured["kwargs"]["stream"] is True


def test_download_file_accepts_volume_and_name(monkeypatch, tmp_path):
    api = _api()
    captured_api = _capture_get(api, monkeypatch, {"download_url": "https://storage.test/s"})
    _capture_storage_get(monkeypatch, _StreamResp(b"data"))

    api.download_file(str(tmp_path / "out.bin"), volume="vol-123", file_name="doc.pdf")

    assert captured_api["url"].endswith("/api/v1/i/volumes/vol-123/files/doc.pdf/download/")


def test_download_file_leaves_no_partial_file_on_failure(monkeypatch, tmp_path):
    api = _api()
    _capture_get(api, monkeypatch, {"download_url": "https://storage.test/signed"})
    _capture_storage_get(monkeypatch, _StreamResp(b"", status_code=403))

    dest = tmp_path / "out.bin"
    with pytest.raises(FileDownloadError, match="403"):
        api.download_file(str(dest), file_id="file-123")

    assert not dest.exists()


def test_download_file_rejects_insufficient_identifiers(tmp_path):
    api = _api()
    with pytest.raises(ValueError, match="Not enough identifiers"):
        api.download_file(str(tmp_path / "out.bin"), volume="vol-123")


def test_storage_session_is_pooled_and_credential_free():
    """The download session must reuse connections yet carry no Nekt auth.

    A bare `requests.get` per file paid a fresh TLS handshake each time, which
    dominated the wall time of a bulk download. Reusing one session fixes that,
    but only if it is a *separate* session from the API client's — otherwise the
    data-access token would travel to a storage host.
    """
    api = _api()

    first = api._storage_session
    assert first is api._storage_session, "session must be reused across files"
    assert first is not api._session, "must not be the API client's session"

    auth_headers = {k for k in api._session.headers if "Token" in k or k.lower() == "authorization"}
    assert auth_headers, "sanity: the API session does carry auth"
    assert not any(k in first.headers for k in auth_headers)
