"""Unit tests for NektAPI.iter_volume_files (paginated volume listing)."""

from __future__ import annotations

import pytest

from nekt.api import NektAPI
from nekt.exceptions import VolumeNotFoundError


def _api() -> NektAPI:
    return NektAPI(data_access_token="test-token", api_url="https://api.test")


class _Resp:
    def __init__(self, payload, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = ""

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def json(self):
        return self._payload


def _serve(api: NektAPI, monkeypatch, pages: list) -> list[dict]:
    """Serve `pages` in order, recording each request."""
    calls: list[dict] = []

    def fake_get(url, params=None, **kwargs):
        calls.append({"url": url, "params": params})
        page = pages[len(calls) - 1]
        return page if isinstance(page, _Resp) else _Resp(page)

    monkeypatch.setattr(api._session, "get", fake_get)
    return calls


def _file(name: str, **overrides) -> dict:
    return {
        "id": f"id-{name}",
        "name": name,
        "description": None,
        "file_size": 100,
        "file_type": "image/jpeg",
        "created_at": "2026-09-01T10:00:00-03:00",
        "updated_at": "2026-09-01T11:00:00-03:00",
        **overrides,
    }


def test_single_page(monkeypatch):
    api = _api()
    calls = _serve(api, monkeypatch, [{"results": [_file("a.jpg"), _file("b.jpg")], "next": None}])

    files = list(api.iter_volume_files("vol-123"))

    assert [f["name"] for f in files] == ["a.jpg", "b.jpg"]
    assert calls[0]["url"] == "https://api.test/api/v1/i/volumes/vol-123/files/"
    assert calls[0]["params"] == {"page_size": 100}


def test_pagination_follows_next_and_drops_params(monkeypatch):
    api = _api()
    next_url = "https://api.test/api/v1/i/volumes/vol-123/files/?page=2&page_size=100"
    calls = _serve(
        api,
        monkeypatch,
        [
            {"results": [_file("a.jpg")], "next": next_url},
            {"results": [_file("b.jpg")], "next": None},
        ],
    )

    files = list(api.iter_volume_files("vol-123"))

    assert [f["name"] for f in files] == ["a.jpg", "b.jpg"]
    # The `next` URL carries its own query string; re-sending params would duplicate them.
    assert calls[1] == {"url": next_url, "params": None}


def test_updated_since_is_filtered_server_side(monkeypatch):
    api = _api()
    calls = _serve(api, monkeypatch, [{"results": [], "next": None}])

    list(api.iter_volume_files("vol-123", updated_since="2026-06-01T00:00:00Z"))

    assert calls[0]["params"]["updated_at__gte"] == "2026-06-01T00:00:00Z"


def test_page_size_is_forwarded(monkeypatch):
    api = _api()
    calls = _serve(api, monkeypatch, [{"results": [], "next": None}])

    list(api.iter_volume_files("vol-123", page_size=25))

    assert calls[0]["params"]["page_size"] == 25


def test_it_is_lazy(monkeypatch):
    """Creating the generator must not call the API; a consumer drives it."""
    api = _api()
    calls = _serve(api, monkeypatch, [{"results": [_file("a.jpg")], "next": None}])

    generator = api.iter_volume_files("vol-123")
    assert calls == []

    next(generator)
    assert len(calls) == 1


def test_bare_list_payload_is_tolerated(monkeypatch):
    api = _api()
    _serve(api, monkeypatch, [[_file("a.jpg")]])

    assert [f["name"] for f in api.iter_volume_files("vol-123")] == ["a.jpg"]


def test_empty_volume_yields_nothing(monkeypatch):
    api = _api()
    _serve(api, monkeypatch, [{"results": [], "next": None}])

    assert list(api.iter_volume_files("vol-123")) == []


def test_missing_volume_raises(monkeypatch):
    api = _api()
    _serve(api, monkeypatch, [_Resp({"detail": "Not found."}, status_code=404)])

    with pytest.raises(VolumeNotFoundError):
        list(api.iter_volume_files("vol-123"))


def test_empty_identifier_rejected():
    with pytest.raises(ValueError, match="Volume identifier is required"):
        list(_api().iter_volume_files(""))
