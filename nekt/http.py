"""HTTP plumbing shared by every SDK call: timeouts and a hard wall-clock deadline.

A ``requests`` timeout does not bound a call. urllib3 resolves the hostname
(``socket.getaddrinfo``) *before* it applies the socket timeout, and
``getaddrinfo`` accepts no timeout at all — so a resolver that drops the query
blocks the caller forever. Because nothing raises, retry decorators never fire
and nothing is logged. On 2026-09-14 that wedged 8 pipeline runs for 15-18 hours
inside a single webhook POST (NEKT-5259).

:class:`DeadlineSession` closes that hole once, at the session boundary: every
request runs on a watchdog thread and the caller gets control back when the
deadline passes, as a retryable :class:`DeadlineExceeded`.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, TypeVar
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

T = TypeVar("T")

# (connect, read) timeouts for every HTTP call. Without an explicit timeout a
# stalled socket blocks forever and the retry policy never fires (Timeout is
# only raised when a timeout is set) — a single stuck presigned-URL PUT once
# froze a pipeline for days. Part uploads get a longer read timeout since the
# read clock also covers awaiting S3's response after sending up to 100 MB.
DEFAULT_TIMEOUT = (10, 120)
UPLOAD_PART_TIMEOUT = (10, 600)
# Downloads get the longer read timeout for the same reason part uploads do: the
# read clock covers streaming an arbitrarily large object off storage.
DOWNLOAD_TIMEOUT = (10, 600)

# Slack on top of connect + read before the watchdog gives up. It covers what
# the socket timeouts do not: name resolution, TLS, and the time between
# individual reads. DEFAULT_TIMEOUT therefore gets a 180s ceiling.
DEADLINE_HEADROOM_SECONDS = 50

# Keep-alive connections held per host. A Spark driver can make concurrent
# calls; a pool smaller than that silently opens (and resolves) new connections.
POOL_MAXSIZE = 16


class DeadlineExceeded(requests.exceptions.Timeout):
    """An HTTP call did not return within its wall-clock deadline.

    Subclasses ``requests.exceptions.Timeout`` so the existing retry policies
    (tenacity and backoff both retry ``Timeout``) treat it as transient: a
    resolver stall usually clears, and the next attempt should go ahead.
    """


def deadline_for(timeout: Any) -> float:
    """Wall-clock deadline for a request made with the given ``requests`` timeout."""
    if timeout is None:
        timeout = DEFAULT_TIMEOUT
    if isinstance(timeout, (tuple, list)):
        connect, read = timeout
        return float(connect or 0) + float(read or 0) + DEADLINE_HEADROOM_SECONDS
    # A scalar timeout applies to connect and to read separately.
    return 2 * float(timeout) + DEADLINE_HEADROOM_SECONDS


def call_with_deadline(fn: Callable[..., T], *args: Any, deadline: float, description: str = "", **kwargs: Any) -> T:
    """Run ``fn(*args, **kwargs)`` and give up after ``deadline`` seconds.

    The call runs on a fresh daemon thread per invocation. A thread blocked in C
    (``getaddrinfo``) cannot be killed, so a wedged one is abandoned: being a
    daemon it never holds the process open, and being per-call (not a pool
    worker) it cannot starve later calls. ``signal.alarm`` is not an option —
    it only works on the main thread, and the SDK runs in notebooks and workers.

    Raises:
        DeadlineExceeded: If ``fn`` has not returned within ``deadline``.
        BaseException: Whatever ``fn`` raised, re-raised in the caller.
    """
    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            outcome["value"] = fn(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001 - re-raised in the caller
            outcome["error"] = exc

    worker = threading.Thread(target=run, name="nekt-http-deadline", daemon=True)
    worker.start()
    worker.join(deadline)

    if worker.is_alive():
        what = description or getattr(fn, "__name__", repr(fn))
        logger.warning("HTTP call %s exceeded its %.0fs deadline (likely stuck resolving DNS); abandoning it", what, deadline)
        raise DeadlineExceeded(f"{what} exceeded {deadline:.0f}s deadline (likely stuck resolving DNS)")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


class DeadlineSession(requests.Session):
    """A pooled ``requests.Session`` in which no request can block indefinitely.

    Every request gets a timeout (``DEFAULT_TIMEOUT`` when the caller passes
    none) and a wall-clock deadline derived from it, so a slow-but-legitimate
    call sized with a long timeout (a 100 MB part upload) is not starved.

    With ``stream=True`` the deadline covers the call up to the response
    headers; the body is then read under the per-read socket timeout, which
    needs no further name resolution.
    """

    def __init__(self) -> None:
        super().__init__()
        adapter = HTTPAdapter(pool_connections=POOL_MAXSIZE, pool_maxsize=POOL_MAXSIZE)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def request(self, method: str | bytes, url: str | bytes, *args: Any, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = DEFAULT_TIMEOUT
        method_name = method.decode() if isinstance(method, bytes) else method
        url_text = url.decode() if isinstance(url, bytes) else url
        # Host only: presigned storage URLs carry credentials in the query string.
        description = f"{method_name.upper()} {urlsplit(url_text).netloc}"
        return call_with_deadline(
            super().request,
            method,
            url,
            *args,
            deadline=deadline_for(kwargs["timeout"]),
            description=description,
            **kwargs,
        )
