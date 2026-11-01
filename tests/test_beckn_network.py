"""Two independent RASAD nodes trading over signed Beckn messages.

Node A is the buyer (RASAD control room); node B is a partner network serving its own
supplier. Each runs as its own server process with its own database and key pair. The
test checks: signature rejection, search -> select -> init -> confirm across nodes, the
Warden holding orders for approval when routes cannot be verified, officer approval, the
supplier accepting on node B, and node A's need lines picking up the committed stock.

Needs two empty PostGIS databases:
  RASAD_TEST_DATABASE_URL, RASAD_TEST_DATABASE_URL_2
"""

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

import httpx
import pytest

DB_A = os.environ.get("RASAD_TEST_DATABASE_URL")
DB_B = os.environ.get("RASAD_TEST_DATABASE_URL_2")
pytestmark = pytest.mark.skipif(not (DB_A and DB_B), reason="two test databases not configured")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_node(db, port, sub_id, redis_db):
    env = {**os.environ, "RASAD_DATABASE_URL": db, "RASAD_EMBED_WORKER": "false", "RASAD_PUBLIC_URL": f"http://127.0.0.1:{port}",
           "RASAD_BECKN_SUBSCRIBER_ID": sub_id, "RASAD_REDIS_URL": f"redis://localhost:6379/{redis_db}",
           "RASAD_GRAPHHOPPER_URL": "http://127.0.0.1:9"}
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "rasad.api.main:app", "--port", str(port)], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(60):
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=1).status_code == 200:
                return proc
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    proc.kill()
    raise RuntimeError(f"node on {port} did not start")


def reset(db):
    import asyncpg

    async def go():
        c = await asyncpg.connect(db)
        await c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await c.close()
    asyncio.new_event_loop().run_until_complete(go())


@pytest.fixture(scope="module")
def nodes():
    reset(DB_A)
    reset(DB_B)
    pa, pb = free_port(), free_port()
    a = start_node(DB_A, pa, "rasad-a.test", 2)
    b = start_node(DB_B, pb, "partner-b.test", 3)
    yield f"http://127.0.0.1:{pa}", f"http://127.0.0.1:{pb}"
    for p in (a, b):
        p.terminate()
        p.wait(10)


def test_cross_node_procurement(nodes):
    A, B = nodes
    ia, ib = httpx.get(f"{A}/api/beckn/identity").json(), httpx.get(f"{B}/api/beckn/identity").json()

    # Before peering, B refuses A's signed search: A's key is unknown to it.
    body = json.dumps({"context": {"action": "search", "transaction_id": "t", "message_id": "m", "bap_uri": ia["bap_uri"]}, "message": {}})
    r = httpx.post(f"{B}/beckn/bpp/search", content=body, headers={"Content-Type": "application/json", "Authorization": 'Signature keyId="x|y|ed25519",algorithm="ed25519",created="1",expires="2",headers="(created) (expires) digest",signature="AA=="'})
    assert r.status_code == 401 and r.json()["message"]["ack"]["status"] == "NACK"

    httpx.post(f"{A}/api/beckn/peers", json={"subscriber_id": ib["subscriber_id"], "unique_key_id": ib["unique_key_id"], "role": "BPP",
                                            "url": ib["bpp_uri"], "public_key": ib["signing_public_key"]}).raise_for_status()
    httpx.post(f"{B}/api/beckn/peers", json={"subscriber_id": ia["subscriber_id"], "unique_key_id": ia["unique_key_id"], "role": "BAP",
                                            "url": ia["bap_uri"], "public_key": ia["signing_public_key"]}).raise_for_status()

    # Node B: a partner warehouse reports stock in plain text.
    sup = httpx.post(f"{B}/api/suppliers", json={"name": "Partner warehouse", "kind": "ngo_warehouse", "lat": 20.30, "lon": 85.82}).json()
    token = sup["link"].rsplit("/", 1)[1]
    for t in ("stock water 6000 L @ 0", "stock rice 100 kg @ 40"):
        assert "Stock updated" in httpx.post(f"{B}/api/supplier/{token}/message", json={"text": t}).json()["reply"]

    # Node A: a shelter with two open need lines (written directly; the Quartermaster is tested elsewhere).
    import asyncpg

    async def seed():
        c = await asyncpg.connect(DB_A)
        sid = await c.fetchval("INSERT INTO shelter (name, kind, geom, source) VALUES ('Test shelter', 'officer', ST_SetSRID(ST_MakePoint(85.84, 20.29),4326), 'officer') RETURNING id")
        for item, unit, q in (("drinking_water", "litres", 4500), ("cereal_ration", "kg", 140)):
            await c.execute(
                """INSERT INTO need (shelter_id, item, unit, quantity, people, people_basis, days, rate, needed_by, priority, basis)
                   VALUES ($1,$2,$3,$4,100,'headcount',3,'r',$5,100,'{"headcount": {"id": 1}}')""",
                sid, item, unit, q, datetime.now(UTC) + timedelta(hours=12))
        await c.close()
        return sid
    asyncio.new_event_loop().run_until_complete(seed())

    os.environ.update(RASAD_DATABASE_URL=DB_A, RASAD_PUBLIC_URL=A, RASAD_BECKN_SUBSCRIBER_ID="rasad-a.test",
                      RASAD_GRAPHHOPPER_URL="http://127.0.0.1:9", RASAD_REDIS_URL="redis://localhost:6379/2")
    from rasad import db as dbmod
    from rasad.agents.procurer import Procurer
    from rasad.beckn import node
    from rasad.config import get_settings

    get_settings.cache_clear()
    node.reset_identity_cache()
    dbmod._pool = None
    loop = asyncio.new_event_loop()
    pool = loop.run_until_complete(dbmod.get_pool())
    proc = Procurer(pool)

    # Run 1: offers found on node B, but routes cannot be verified (no routing engine): Warden holds both for approval.
    summary = loop.run_until_complete(proc.run())
    assert summary["awaiting_approval"] == 2 and summary["placed"] == 0
    rows = httpx.get(f"{A}/api/procurements").json()
    assert {r["status"] for r in rows} == {"awaiting_approval"}
    assert all(r["bpp_id"] == "partner-b.test" for r in rows)
    water = next(r for r in rows if r["item"] == "drinking_water")
    assert water["quantity"] == 4500 and water["total_price"] == 0
    assert any(c["check"] == "safe_route" and c["outcome"] == "approval" for c in water["checks"])

    # The officer approves the water and rejects the rice; run 2 places the approved order on node B.
    assert httpx.post(f"{A}/api/procurements/{water['id']}/approve", json={"by": "DM office"}).status_code == 200
    rice = next(r for r in rows if r["item"] == "cereal_ration")
    assert httpx.post(f"{A}/api/procurements/{rice['id']}/reject", json={"by": "DM office", "reason": "too costly"}).status_code == 200
    loop.run_until_complete(proc.run())
    water = next(r for r in httpx.get(f"{A}/api/procurements").json() if r["id"] == water["id"])
    assert water["status"] == "awaiting_supplier" and water["bpp_order_id"]

    # Node B's supplier accepts; B pushes on_status to A; A commits the stock against the need.
    reply = httpx.post(f"{B}/api/supplier/{token}/message", json={"text": f"ACCEPT {water['bpp_order_id']}"}).json()["reply"]
    assert "accepted" in reply
    for _ in range(40):
        water = next(r for r in httpx.get(f"{A}/api/procurements").json() if r["id"] == water["id"])
        if water["status"] == "accepted":
            break
        time.sleep(0.25)
    assert water["status"] == "accepted"

    async def committed():
        return await pool.fetchval("SELECT committed FROM need WHERE item='drinking_water'")
    assert loop.run_until_complete(committed()) == 4500

    # Every message on both nodes was signature-checked on receipt.
    for base in (A, B):
        inbound = [m for m in httpx.get(f"{base}/api/beckn/messages?limit=200").json() if m["direction"] == "in" and m["peer"] != "x"]
        assert inbound and all(m["signature_ok"] for m in inbound)
    loop.run_until_complete(proc.stop())
    loop.run_until_complete(dbmod.close_pool())
