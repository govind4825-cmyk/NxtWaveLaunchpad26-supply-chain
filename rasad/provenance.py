"""HTTP fetching that records every call in raw_fetch.

Nothing RASAD acts on enters the system without passing through here, so every
hazard, forecast and population number can be traced back to the exact payload,
URL and time it came from.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx

from .config import get_settings


@dataclass
class FetchResult:
    raw_id: int | None
    url: str
    ok: bool
    status: int | None
    body: bytes
    content_type: str | None
    error: str | None
    unchanged: bool = False  # same bytes as the previous successful fetch of this URL

    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        import json

        return json.loads(self.body)


class Fetcher:
    def __init__(self, pool: asyncpg.Pool | None, client: httpx.AsyncClient | None = None):
        s = get_settings()
        self.pool = pool
        self.client = client or httpx.AsyncClient(
            timeout=s.http_timeout,
            follow_redirects=True,
            headers={"User-Agent": s.user_agent},
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def get(
        self,
        source: str,
        url: str,
        params: dict[str, Any] | None = None,
        retries: int = 2,
        method: str = "GET",
        data: dict[str, Any] | None = None,
    ) -> FetchResult:
        last_err: str | None = None
        resp: httpx.Response | None = None
        started = time.monotonic()
        for attempt in range(retries + 1):
            try:
                resp = await self.client.request(method, url, params=params, data=data)
                if resp.status_code >= 500 and attempt < retries:
                    last_err = f"HTTP {resp.status_code}"
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                break
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_err = f"{type(exc).__name__}: {exc}"
                resp = None
                if attempt < retries:
                    await asyncio.sleep(1.5 * (attempt + 1))
        elapsed = int((time.monotonic() - started) * 1000)
        full_url = str(resp.request.url) if resp is not None else _join(url, params)

        if resp is None:
            raw_id = await self._store(source, full_url, None, False, None, None, None, last_err, elapsed)
            return FetchResult(raw_id, full_url, False, None, b"", None, last_err)

        ok = 200 <= resp.status_code < 300
        body = resp.content
        sha = hashlib.sha256(body).hexdigest()
        ctype = resp.headers.get("content-type")
        err = None if ok else f"HTTP {resp.status_code}: {body[:300]!r}"
        unchanged = ok and await self._same_as_last(source, full_url, sha)
        raw_id = await self._store(
            source, full_url, resp.status_code, ok, ctype, None if unchanged else body, sha, err, elapsed
        )
        return FetchResult(raw_id, full_url, ok, resp.status_code, body, ctype, err, unchanged)

    async def _same_as_last(self, source: str, url: str, sha: str) -> bool:
        if self.pool is None:
            return False
        row = await self.pool.fetchrow(
            "SELECT sha256 FROM raw_fetch WHERE source=$1 AND url=$2 AND ok ORDER BY id DESC LIMIT 1",
            source,
            url,
        )
        return bool(row and row["sha256"] == sha)

    async def _store(self, source, url, status, ok, ctype, body, sha, err, elapsed) -> int | None:
        if self.pool is None:
            return None
        url = redact(url)
        err = redact(err) if err else err
        return await self.pool.fetchval(
            """INSERT INTO raw_fetch (source, url, http_status, ok, content_type, body, sha256, error, elapsed_ms)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id""",
            source, url, status, ok, ctype, body, sha, err, elapsed,
        )


async def raw_body(pool: asyncpg.Pool, raw_id: int) -> tuple[bytes, str | None, dict] | None:
    """Return the stored payload for a fetch, following the sha256 pointer when the
    body was not re-stored because it was unchanged."""
    row = await pool.fetchrow(
        "SELECT id, source, url, fetched_at, http_status, ok, content_type, body, sha256, error FROM raw_fetch WHERE id=$1",
        raw_id,
    )
    if row is None:
        return None
    body = row["body"]
    if body is None and row["sha256"]:
        body = await pool.fetchval(
            "SELECT body FROM raw_fetch WHERE sha256=$1 AND body IS NOT NULL ORDER BY id DESC LIMIT 1",
            row["sha256"],
        )
    meta = {
        "id": row["id"],
        "source": row["source"],
        "url": row["url"],
        "fetched_at": row["fetched_at"].isoformat(),
        "http_status": row["http_status"],
        "ok": row["ok"],
        "sha256": row["sha256"],
        "error": row["error"],
    }
    return body or b"", row["content_type"], meta


_SECRET = re.compile(r"(?i)([?&](?:key|api_key|apikey|token|access_token|password)=)[^&\s'\"]+")


def redact(text: str) -> str:
    """Strip API keys from URLs before they are stored or shown in the provenance viewer."""
    return _SECRET.sub(r"\1REDACTED", text)


def _join(url: str, params: dict[str, Any] | None) -> str:
    if not params:
        return url
    return str(httpx.URL(url, params=params))
