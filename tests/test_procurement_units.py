"""Beckn signing, supplier grammar, catalog parsing and Warden decisions.

The digest vector is the one published in the Beckn signing spec (BECKN-006)."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from rasad.agents.procurer import parse_catalog
from rasad.beckn.signing import digest, new_keypair, parse_header, sign, verify
from rasad.suppliers import parse
from rasad.warden import Offer, review

NOW = datetime.now(UTC)


def test_digest_matches_beckn_spec_vector():
    assert digest(b"The quick brown fox jumps over the lazy dog") == \
        "qK3Uvd39k+SHfSdG5igXsRY2Sh+nvBSNlQkLxzM7NnP4JAHPeqLkyx7NkCluPxTLVBP47Xe+cwRbE5FM3NapGA=="


def test_sign_and_verify_roundtrip_and_tampering():
    priv, pub = new_keypair()
    body = json.dumps({"context": {"action": "search"}, "message": {}}).encode()
    header = sign(body, priv, "rasad.local", "k1", created=1_700_000_000, validity_s=600)
    auth = parse_header(header)
    assert (auth.subscriber_id, auth.unique_key_id, auth.algorithm) == ("rasad.local", "k1", "ed25519")
    assert verify(body, auth, pub, now=1_700_000_100) == (True, None)
    ok, err = verify(body + b" ", auth, pub, now=1_700_000_100)
    assert not ok and "bad signature" in err
    _, other_pub = new_keypair()
    assert not verify(body, auth, other_pub, now=1_700_000_100)[0]
    assert verify(body, auth, pub, now=1_700_001_000)[1] == "signature expired"
    assert verify(body, auth, pub, now=1_699_999_000)[1] == "signature created in the future"
    assert parse_header("Bearer abc") is None
    assert parse_header('Signature keyId="a|b|rsa",algorithm="ed25519",created="1",expires="2",signature="x"') is None


def test_supplier_grammar():
    c = parse("stock sabun 30 kg @ 120")
    assert (c.item, c.quantity, c.unit_price) == ("bathing_soap", 30000, 0.12)
    c = parse("STOCK drinking_water 5000 L @ 2")
    assert (c.item, c.quantity, c.unit_price) == ("drinking_water", 5000, 2)
    assert parse("stock rice 2 q").quantity == 200
    assert parse("yes 14").cmd == "accept" and parse("packed #3").cmd == "ready"
    r = parse("reject 9 truck broke down")
    assert (r.cmd, r.order_id, r.reason) == ("reject", 9, "truck broke down")
    assert parse("stock water 3 kg").cmd == "unknown"
    assert parse("").cmd == "unknown"


def test_parse_catalog_reads_offers():
    body = {"context": {"bpp_id": "b", "bpp_uri": "http://b"},
            "message": {"catalog": {"providers": [{
                "id": "7", "descriptor": {"name": "Shop"}, "locations": [{"id": "7:loc", "gps": "20.1,85.2"}],
                "items": [{"id": "7:drinking_water", "location_ids": ["7:loc"], "price": {"currency": "INR", "value": "2.0000"},
                           "quantity": {"available": {"count": 4000}}, "time": {"timestamp": "2026-11-01T10:00:00+00:00"}},
                          {"id": "7:cereal_ration", "location_ids": ["7:loc"], "quantity": {"available": {"count": 50}}}]}]}}}
    offers = parse_catalog(body, "m1")
    assert [(o.item_id, o.available, o.unit_price) for o in offers] == [("7:drinking_water", 4000, 2.0), ("7:cereal_ration", 50, None)]
    assert offers[0].lat == 20.1 and offers[0].search_message_id == "m1"


class SpendPool:
    def __init__(self, spent):
        self.spent = spent

    async def fetchval(self, *_a):
        return self.spent


def run(c):
    return asyncio.new_event_loop().run_until_complete(c)


def offer(**kw):
    base = dict(item="drinking_water", quantity=1000, unit_price=2.0, route_safe=True, eta_min=40,
                stock_at=NOW - timedelta(hours=1), search_message_id="m", need_basis={"x": 1})
    base.update(kw)
    return Offer(**base)


def test_warden_pass_approval_block():
    due = NOW + timedelta(hours=5)
    d, checks = run(review(SpendPool(0), offer(), 1000, due, NOW))
    assert d == "pass" and all(c["outcome"] == "pass" for c in checks)
    assert run(review(SpendPool(0), offer(unit_price=None), 1000, due, NOW))[0] == "approval"
    assert run(review(SpendPool(0), offer(unit_price=100.0), 1000, due, NOW))[0] == "approval"        # Rs 1,00,000 over order cap
    assert run(review(SpendPool(199_000), offer(), 1000, due, NOW))[0] == "approval"                  # daily cap
    assert run(review(SpendPool(0), offer(route_safe=None), 1000, due, NOW))[0] == "approval"
    assert run(review(SpendPool(0), offer(route_safe=False), 1000, due, NOW))[0] == "approval"
    assert run(review(SpendPool(0), offer(eta_min=600), 1000, due, NOW))[0] == "approval"             # late
    assert run(review(SpendPool(0), offer(quantity=2000), 1000, due, NOW))[0] == "block"              # > 1.5x need
    assert run(review(SpendPool(0), offer(item="whisky"), 1000, due, NOW))[0] == "block"
    assert run(review(SpendPool(0), offer(stock_at=NOW - timedelta(days=3)), 1000, due, NOW))[0] == "block"
    assert run(review(SpendPool(0), offer(need_basis=None), 1000, due, NOW))[0] == "block"


@pytest.mark.parametrize("text", ["stock water 5,000", "STOCK Water 5000 l", "stock pani 5000 litres"])
def test_water_variants(text):
    assert parse(text).quantity == 5000
