"""End to end on an anvil fork of chain 4663 with the stand-in ERC-20: sign in, key, stream, grace, drop."""
import http.client
import json
import threading
import time

import pytest

from gate_fork import ANVIL, CHAIN_ID, STANDIN_TOKEN, Fork, fresh_key, upstream_available
from golden.anonymous import stub_runtime
from rhpools.lp_gate import Gate, GatePolicy, GateRefusal, Limits
from rhpools.lp_gate_siwe import build_message, personal_sign_hash
from rhpools.lp_server import Handler, LPHTTPServer
from test_gate import Clock
from test_gate_server import call, read_frames

pytestmark = pytest.mark.skipif(not ANVIL.exists() or not upstream_available(), reason="anvil or the local chain 4663 RPC is unavailable")

THRESHOLD = 1000 * 10**18


@pytest.fixture(scope="module")
def fork():
    with Fork() as instance:
        yield instance


def serve(tmp_path, fork, owner):
    clock = Clock(time.time())
    gate = Gate(tmp_path / "gate.sqlite", owner=owner, rpc_url=fork.url, hosts=frozenset({"rhpools.lol"}), clock=clock, limits=Limits(key_burst=100))
    runtime = stub_runtime()
    runtime.gate = gate
    handler = type("ForkHandler", (Handler,), {"runtime": runtime, "log_message": lambda *a: None})
    server = LPHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def stop():
        runtime.stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        gate.close()
    return server.server_address, gate, clock, stop


def signed_policy(key, version, clock, grace_s=120):
    parsed = GatePolicy.parse({
        "version": version, "token": STANDIN_TOKEN, "decimals": 18,
        "threshold": {"trade": str(THRESHOLD), "lp": str(THRESHOLD), "api": str(THRESHOLD), "flags": "0"},
        "grace_s": grace_s, "issued_at": int(clock.now),
    })
    return parsed, key.sign_msg_hash(parsed.digest(CHAIN_ID)).to_hex()


def sign_in(address, gate, key, clock):
    nonce = gate.nonce()
    stamp = lambda offset: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(clock.now + offset))
    message = build_message(
        domain="rhpools.lol", address=key.public_key.to_checksum_address(), statement=nonce["statement"],
        uri="https://rhpools.lol/", chain_id=CHAIN_ID, nonce=nonce["nonce"], issued_at=stamp(0), expiration_time=stamp(600),
    )
    signature = key.sign_msg_hash(personal_sign_hash(message)).to_hex()
    status, headers, body = call(address, "POST", "/api/gate/session", {"message": message, "signature": signature})
    assert status == 200, body
    return headers["Set-Cookie"].split(";", 1)[0].split("=", 1)[1], body


def test_holder_lifecycle_on_the_fork(tmp_path, fork):
    owner, holder, bystander = fresh_key(), fresh_key(), fresh_key()
    holder_address = holder.public_key.to_checksum_address()
    assert fork.rpc("eth_getCode", [holder_address, "latest"]) == "0x"
    fork.set_balance(holder_address, 2 * THRESHOLD)
    assert fork.balance_of(holder_address) == 2 * THRESHOLD
    fork.set_supply(1_000_000 * THRESHOLD)
    address, gate, clock, stop = serve(tmp_path, fork, owner.public_key.to_checksum_address())
    try:
        parsed, signature = signed_policy(bystander, 1, clock)
        with pytest.raises(GateRefusal) as refused:
            gate.apply_policy(parsed, signature, via="cli")
        assert refused.value.status == 403
        parsed, signature = signed_policy(owner, 1, clock)
        status, _, body = call(address, "POST", "/api/gate/policy", {"policy": parsed.public(), "signature": signature})
        assert status == 200 and body["policy"]["token"] == STANDIN_TOKEN
        assert [row["action"] for row in gate.audit()][::-1] == ["policy.apply"]

        secret, me = sign_in(address, gate, holder, clock)
        assert me["state"] == "holder" and me["features"] == ["trade", "lp", "api", "flags"]
        assert me["holding"]["balance_raw"] == str(2 * THRESHOLD)
        assert me["holding"]["total_supply_raw"] == str(1_000_000 * THRESHOLD)

        poor_secret, poor_me = sign_in(address, gate, bystander, clock)
        assert poor_me["state"] == "below" and poor_me["features"] == ["flags"]
        status, _, body = call(address, "GET", "/api/v1/pools", headers={"Authorization": "Bearer " + poor_secret})
        assert status == 403 and body["gate"]["have"] == "0"

        status, _, minted = call(address, "POST", "/api/gate/keys", {"op": "mint", "label": "fork-bot"}, {
            "Cookie": "__Host-rhp_session=" + secret, "Origin": "https://rhpools.lol", "Host": "rhpools.lol",
        })
        assert status == 200
        bearer = {"Authorization": "Bearer " + minted["secret"]}
        status, headers, body = call(address, "GET", "/api/lp/tape", headers=bearer)
        assert status == 200 and headers["X-RateLimit-Limit"] == "100" and headers["Cache-Control"] == "private, no-store"

        connection = http.client.HTTPConnection(*address, timeout=10)
        connection.request("GET", "/api/v1/stream?channel=both&view=terminal&current_only=1&owners=0", headers=bearer)
        response = connection.getresponse()
        assert response.status == 200
        assert [frame["event"] for frame in read_frames(response, 2)] == ["block", "activity"]

        fork.set_balance(holder_address, THRESHOLD - 1)
        clock.now += 31
        status, _, me = call(address, "GET", "/api/gate/me", headers=bearer)
        assert me["state"] == "below" and me["features"] == ["flags"]
        assert me["holding"]["balance_raw"] == str(THRESHOLD - 1)
        frames = read_frames(response, 2)
        assert frames[0]["event"] == "gate" and frames[0]["data"]["gate"]["state"] == "below"
        assert frames[1] is None
        connection.close()
        status, _, body = call(address, "GET", "/api/lp/tape", headers=bearer)
        assert status == 403 and body["gate"]["state"] == "below"
        status, _, me = call(address, "GET", "/api/gate/me", headers=bearer)
        assert me["state"] == "below" and me["features"] == ["flags"]
    finally:
        stop()
