"""Beckn message signing (BECKN-006): ed25519 over a signing string built from
(created), (expires) and a BLAKE2b-512 digest of the exact request body."""

from __future__ import annotations

import base64
import hashlib
import re
import time
from dataclasses import dataclass

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat

DEFAULT_VALIDITY_S = 3600
CLOCK_SKEW_S = 60


def digest(body: bytes) -> str:
    return base64.b64encode(hashlib.blake2b(body, digest_size=64).digest()).decode()


def signing_string(created: int, expires: int, body: bytes) -> str:
    return f"(created): {created}\n(expires): {expires}\ndigest: BLAKE-512={digest(body)}"


def new_keypair() -> tuple[bytes, str]:
    """(raw private key bytes, base64 raw public key) as Beckn registries expect."""
    k = Ed25519PrivateKey.generate()
    priv = k.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    pub = k.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return priv, base64.b64encode(pub).decode()


def sign(body: bytes, private_key: bytes, subscriber_id: str, unique_key_id: str,
         created: int | None = None, validity_s: int = DEFAULT_VALIDITY_S) -> str:
    created = created if created is not None else int(time.time())
    expires = created + validity_s
    key = Ed25519PrivateKey.from_private_bytes(private_key)
    sig = base64.b64encode(key.sign(signing_string(created, expires, body).encode())).decode()
    return (f'Signature keyId="{subscriber_id}|{unique_key_id}|ed25519",algorithm="ed25519",'
            f'created="{created}",expires="{expires}",headers="(created) (expires) digest",signature="{sig}"')


@dataclass
class ParsedAuth:
    subscriber_id: str
    unique_key_id: str
    algorithm: str
    created: int
    expires: int
    signature: str


_PARAM = re.compile(r'(\w+)="([^"]*)"')


def parse_header(header: str) -> ParsedAuth | None:
    if not header or not header.strip().startswith("Signature"):
        return None
    params = dict(_PARAM.findall(header))
    try:
        sub, kid, alg = params["keyId"].split("|")
        if alg != params.get("algorithm", alg):
            return None
        return ParsedAuth(sub, kid, alg, int(params["created"]), int(params["expires"]), params["signature"])
    except (KeyError, ValueError):
        return None


def verify(body: bytes, auth: ParsedAuth, public_key_b64: str, now: int | None = None) -> tuple[bool, str | None]:
    now = now if now is not None else int(time.time())
    if auth.algorithm != "ed25519":
        return False, f"unsupported algorithm {auth.algorithm}"
    if auth.created > now + CLOCK_SKEW_S:
        return False, "signature created in the future"
    if auth.expires < now - CLOCK_SKEW_S:
        return False, "signature expired"
    try:
        pub = Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key_b64))
        pub.verify(base64.b64decode(auth.signature), signing_string(auth.created, auth.expires, body).encode())
        return True, None
    except (InvalidSignature, ValueError) as exc:
        return False, f"bad signature: {type(exc).__name__}"
