"""Unit tests for NektAPI.upload_file (multipart presigned upload orchestration)."""

from __future__ import annotations

import pytest

from nekt.api import NektAPI
from nekt.exceptions import FileUploadError


def _api() -> NektAPI:
    # Construction does no network I/O; it only sets session headers.
    return NektAPI(data_access_token="test-token", api_url="https://api.test")


class _PutResp:
    status_code = 200
    headers = {"ETag": '"etag-abc"'}

    def raise_for_status(self) -> None:  # noqa: D102
        pass


def test_upload_file_orchestrates(tmp_path, monkeypatch):
    f = tmp_path / "doc.pdf"
    f.write_bytes(b"hello world")

    api = _api()
    created: dict = {}
    completed: dict = {}
    monkeypatch.setattr(
        api,
        "create_volume_file",
        lambda **kw: (created.update(kw) or {"id": "fid", "presigned_url_list": [{"part_number": 1, "presigned_url": "https://put"}]}),
    )
    monkeypatch.setattr(api, "complete_volume_file_upload", lambda **kw: completed.update(kw))
    monkeypatch.setattr("nekt.api.requests.put", lambda url, data, timeout=None: _PutResp())

    result = api.upload_file("raw", "vol", str(f), file_name="custom.pdf")

    assert result == {
        "id": "fid",
        "name": "custom.pdf",
        "file_size": 11,
        "file_type": "application/pdf",
        "description": None,
    }
    # create_volume_file got the computed metadata
    assert created["name"] == "custom.pdf"
    assert created["file_size"] == 11
    assert created["file_type"] == "application/pdf"
    # complete got the assembled part with its stripped etag
    assert completed["file_id"] == "fid"
    assert completed["parts"] == [{"etag": "etag-abc", "part_number": 1}]


def test_upload_file_accepts_url_string_list(tmp_path, monkeypatch):
    """Some endpoints return presigned_url_list as a flat list of URL strings."""
    f = tmp_path / "doc.bin"
    f.write_bytes(b"x" * 10)

    api = _api()
    completed: dict = {}
    monkeypatch.setattr(
        api,
        "create_volume_file",
        lambda **kw: {"id": "fid", "presigned_url_list": ["https://put/1", "https://put/2"]},
    )
    monkeypatch.setattr(api, "complete_volume_file_upload", lambda **kw: completed.update(kw))

    seen: list = []

    def fake_put(url, data, timeout=None):
        seen.append((url, len(data)))
        return _PutResp()

    monkeypatch.setattr("nekt.api.requests.put", fake_put)

    result = api.upload_file("raw", "vol", str(f))

    assert result["id"] == "fid"
    # 10 bytes split into 2 parts of 5
    assert seen == [("https://put/1", 5), ("https://put/2", 5)]
    assert completed["parts"] == [
        {"etag": "etag-abc", "part_number": 1},
        {"etag": "etag-abc", "part_number": 2},
    ]


def test_upload_file_by_volume_id_uses_layerless_endpoints(tmp_path, monkeypatch):
    """upload_file_by_volume_id should hit the by-volume-id create/complete methods."""
    f = tmp_path / "doc.pdf"
    f.write_bytes(b"hello world")

    api = _api()
    created: dict = {}
    completed: dict = {}
    monkeypatch.setattr(
        api,
        "create_volume_file_by_volume_id",
        lambda **kw: (created.update(kw) or {"id": "fid", "presigned_url_list": ["https://put/1"]}),
    )
    monkeypatch.setattr(api, "complete_volume_file_upload_by_volume_id", lambda **kw: completed.update(kw))
    monkeypatch.setattr("nekt.api.requests.put", lambda url, data, timeout=None: _PutResp())

    result = api.upload_file_by_volume_id("vol-123", str(f), file_name="custom.pdf")

    assert result["id"] == "fid"
    assert created["volume_identifier"] == "vol-123"
    assert created["name"] == "custom.pdf"
    assert completed["volume_identifier"] == "vol-123"
    assert completed["parts"] == [{"etag": "etag-abc", "part_number": 1}]


def test_create_volume_file_by_volume_id_url(monkeypatch):
    """The layerless create endpoint omits the layer segment."""
    api = _api()
    captured: dict = {}

    class _Resp:
        ok = True

        def json(self):
            return {"id": "fid", "presigned_url_list": []}

    def fake_post(url, json):
        captured["url"] = url
        return _Resp()

    monkeypatch.setattr(api._session, "post", fake_post)
    api.create_volume_file_by_volume_id("vol-123", "n.txt", 3, "text/plain")
    assert captured["url"].endswith("/api/v1/i/volumes/vol-123/files/")
    assert "/layers/" not in captured["url"]


def test_upload_part_passes_timeout(tmp_path, monkeypatch):
    """The presigned-URL PUT must carry a timeout, or a stalled upload hangs forever."""
    f = tmp_path / "doc.bin"
    f.write_bytes(b"x" * 4)

    api = _api()
    monkeypatch.setattr(
        api,
        "create_volume_file",
        lambda **kw: {"id": "fid", "presigned_url_list": ["https://put/1"]},
    )
    monkeypatch.setattr(api, "complete_volume_file_upload", lambda **kw: None)

    captured: dict = {}

    def fake_put(url, data, timeout=None):
        captured["timeout"] = timeout
        return _PutResp()

    monkeypatch.setattr("nekt.api.requests.put", fake_put)

    api.upload_file("raw", "vol", str(f))

    assert captured["timeout"] is not None, "requests.put was called without a timeout"


def test_session_applies_default_timeout(monkeypatch):
    """Every session request gets a default timeout even when the call omits one."""
    from nekt.api import DEFAULT_TIMEOUT

    api = _api()
    captured: dict = {}

    # Intercept the underlying Session.request the subclass delegates to.
    def fake_super_request(self, *args, **kwargs):
        captured["timeout"] = kwargs.get("timeout")

        class _R:
            ok = True

            def json(self):
                return {}

        return _R()

    monkeypatch.setattr("requests.Session.request", fake_super_request)
    # Any session call with no explicit timeout should acquire the default.
    api._session.get("https://api.test/ping")
    assert captured["timeout"] == DEFAULT_TIMEOUT


def test_upload_file_missing_file(tmp_path):
    api = _api()
    with pytest.raises(FileUploadError, match="File not found"):
        api.upload_file("raw", "vol", str(tmp_path / "nope.pdf"))


def test_upload_file_no_presigned_urls(tmp_path, monkeypatch):
    f = tmp_path / "x.txt"
    f.write_text("a")
    api = _api()
    monkeypatch.setattr(api, "create_volume_file", lambda **kw: {"id": "fid", "presigned_url_list": []})
    with pytest.raises(FileUploadError, match="No presigned URLs"):
        api.upload_file("raw", "vol", str(f))
