"""This node's Beckn identity, peer registry, signed sending and verified receiving.

Keys: an ed25519 key pair is generated on first start and kept in beckn_key. The node
registers itself as a peer (BAP at /beckn/bap, BPP at /beckn/bpp), so RASAD's buyer and
its supplier network talk the full signed protocol to each other. Other BPPs (an ONIX
adapter, a partner network) are added as peers with their URL and public key, or looked
up from a Beckn registry if RASAD_BECKN_REGISTRY_URL is set.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import asyncpg
import httpx

from ..config import get_settings
from .signing import new_keypair, parse_header, sign, verify

log = logging.getLogger(__name__)

VERSION = "1.1.0"


@dataclass
class Identity:
    subscriber_id: str
    unique_key_id: str
    private_key: bytes
    public_key: str

    @property
    def bap_uri(self) -> str:
        return get_settings().public_url.rstrip("/") + "/beckn/bap"

    @property
    def bpp_uri(self) -> str:
        return get_settings().public_url.rstrip("/") + "/beckn/bpp"


_identity: Identity | None = None


async def identity(pool: asyncpg.Pool) -> Identity:
    """Load or create this node's key pair, and keep its own BAP and BPP peer entries current."""
    global _identity
    s = get_settings()
    if _identity is not None and _identity.subscriber_id == s.beckn_subscriber_id:
        return _identity
    row = await pool.fetchrow("SELECT * FROM beckn_key WHERE subscriber_id=$1", s.beckn_subscriber_id)
    if row is None:
        priv, pub = new_keypair()
        kid = str(uuid.uuid4())
        await pool.execute(
            "INSERT INTO beckn_key (subscriber_id, unique_key_id, private_key, public_key) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
            s.beckn_subscriber_id, kid, priv, pub,
        )
        row = await pool.fetchrow("SELECT * FROM beckn_key WHERE subscriber_id=$1", s.beckn_subscriber_id)
    _identity = Identity(row["subscriber_id"], row["unique_key_id"], bytes(row["private_key"]), row["public_key"])
    for role, url in (("BAP", _identity.bap_uri), ("BPP", _identity.bpp_uri)):
        await pool.execute(
            """INSERT INTO beckn_peer (subscriber_id, unique_key_id, role, url, public_key) VALUES ($1,$2,$3,$4,$5)
               ON CONFLICT (subscriber_id, unique_key_id, role) DO UPDATE SET url=EXCLUDED.url, public_key=EXCLUDED.public_key, active=true""",
            _identity.subscriber_id, _identity.unique_key_id, role, url, _identity.public_key,
        )
    return _identity


def reset_identity_cache() -> None:
    global _identity
    _identity = None


def context(ident: Identity, action: str, transaction_id: str, *, bpp_id: str | None = None, bpp_uri: str | None = None,
            message_id: str | None = None, as_bpp: bool = False, base: dict | None = None) -> dict:
    s = get_settings()
    if base:  # a BPP answers within the context the BAP set, with a fresh timestamp
        ctx = dict(base)
        ctx.update({"action": action, "timestamp": _now(), "bpp_id": ident.subscriber_id, "bpp_uri": ident.bpp_uri})
        if message_id:
            ctx["message_id"] = message_id
        return ctx
    ctx = {
        "domain": s.beckn_domain,
        "location": {"country": {"code": "IND"}, "city": {"code": s.beckn_city}},
        "action": action,
        "version": VERSION,
        "bap_id": ident.subscriber_id,
        "bap_uri": ident.bap_uri,
        "transaction_id": transaction_id,
        "message_id": message_id or str(uuid.uuid4()),
        "timestamp": _now(),
        "ttl": "PT30S",
    }
    if bpp_id:
        ctx["bpp_id"], ctx["bpp_uri"] = bpp_id, bpp_uri
    return ctx


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


async def log_message(pool, direction, role, action, body, *, peer=None, url=None, signature_ok=None,
                      http_status=None, ack=None, error=None) -> int:
    ctx = body.get("context") or {}
    return await pool.fetchval(
        """INSERT INTO beckn_message (direction, role, action, transaction_id, message_id, peer, url, body, signature_ok, http_status, ack, error)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id""",
        direction, role, action, ctx.get("transaction_id"), ctx.get("message_id"), peer, url, body, signature_ok,
        http_status, ack, error,
    )


_client: httpx.AsyncClient | None = None


def client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=10)
    return _client


async def send(pool: asyncpg.Pool, role: str, url: str, action: str, body: dict, peer: str | None = None) -> tuple[bool, str | None]:
    """POST a signed Beckn message to {url}/{action}. Returns (acked, error)."""
    ident = await identity(pool)
    raw = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()
    headers = {"Content-Type": "application/json", "Authorization": sign(raw, ident.private_key, ident.subscriber_id, ident.unique_key_id)}
    target = url.rstrip("/") + "/" + action
    status, ack, err = None, None, None
    try:
        r = await client().post(target, content=raw, headers=headers)
        status = r.status_code
        try:
            resp = r.json()
            ack = (((resp.get("message") or {}).get("ack") or {}).get("status"))
            if resp.get("error"):
                err = json.dumps(resp["error"])[:500]
        except ValueError:
            err = f"non-JSON response ({r.status_code})"
    except httpx.HTTPError as exc:
        err = f"{type(exc).__name__}: {exc}"
    await log_message(pool, "out", role, action, body, peer=peer, url=target, http_status=status, ack=ack, error=err)
    return ack == "ACK", err


async def peer_key(pool: asyncpg.Pool, subscriber_id: str, unique_key_id: str) -> str | None:
    row = await pool.fetchrow(
        "SELECT public_key FROM beckn_peer WHERE subscriber_id=$1 AND unique_key_id=$2 AND active LIMIT 1",
        subscriber_id, unique_key_id,
    )
    if row:
        return row["public_key"]
    reg = get_settings().beckn_registry_url
    if not reg:
        return None
    try:   # Beckn registry lookup API
        r = await client().post(reg.rstrip("/") + "/lookup", json={"subscriber_id": subscriber_id, "ukId": unique_key_id})
        for entry in r.json() if r.status_code == 200 else []:
            if entry.get("subscriber_id") == subscriber_id and entry.get("signing_public_key"):
                return entry["signing_public_key"]
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("registry lookup failed: %s", exc)
    return None


async def check_signature(pool: asyncpg.Pool, raw: bytes, header: str | None) -> tuple[bool, str | None, str | None]:
    """Returns (ok, error, subscriber_id)."""
    auth = parse_header(header or "")
    if auth is None:
        return False, "missing or malformed Authorization header", None
    key = await peer_key(pool, auth.subscriber_id, auth.unique_key_id)
    if key is None:
        return False, f"unknown signer {auth.subscriber_id}|{auth.unique_key_id}", auth.subscriber_id
    ok, err = verify(raw, auth, key)
    return ok, err, auth.subscriber_id


def ack() -> dict:
    return {"message": {"ack": {"status": "ACK"}}}


def nack(code: str, message: str) -> dict:
    return {"message": {"ack": {"status": "NACK"}}, "error": {"code": code, "message": message}}
