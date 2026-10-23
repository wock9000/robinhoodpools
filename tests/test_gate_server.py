"""HTTP boundary of the gate: cookies, bearers, quota tiers, keyed SSE and WebSocket streams."""
from contextlib import contextmanager
import http.client
import json
import threading

import pytest
from websockets.sync.client import connect as ws_connect

from golden.anonymous import stub_runtime
from rhpools.lp_gate import Gate, Limits
from rhpools.lp_server import Handler, LPHTTPServer
from test_gate import (
    HOLDER, HOLDER_KEY, OTHER_KEY, OWNER, Clock, FakeRpc, signed_policy, siwe,
)

ORIGIN = {"Origin": "https://rhpools.lol", "Host": "rhpools.lol"}
JSON = {"Content-Type": "application/json"}


@contextmanager
def serving(tmp_path, limits=Limits(key_rps=10, key_burst=3)):
    clock, rpc = Clock(), FakeRpc()
    gate = Gate(tmp_path / "gate.sqlite", owner=OWNER, rpc_url="http://127.0.0.1:1", hosts=frozenset({"rhpools.lol"}), clock=clock, rpc=rpc, limits=limits)
    gate.clock, gate.rpc = clock, rpc
    runtime = stub_runtime()
    runtime.gate = gate
    handler = type("GateHandler", (Handler,), {"runtime": runtime, "log_message": lambda *a: None})
    server = LPHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address, gate
    finally:
        runtime.stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        gate.close()


def call(address, method, path, body=None, headers=None):
    connection = http.client.HTTPConnection(*address, timeout=5)
    try:
        connection.request(method, path, None if body is None else json.dumps(body), {**(JSON if body is not None else {}), **(headers or {})})
        response = connection.getresponse()
        raw = response.read()
        return response.status, dict(response.getheaders()), (json.loads(raw) if raw else None)
    finally:
        connection.close()


def browser_sign_in(address, gate, key=HOLDER_KEY, headers=ORIGIN):
    message, signature = siwe(key, gate)
    status, response_headers, body = call(address, "POST", "/api/gate/session", {"message": message, "signature": signature}, headers)
    assert status == 200, body
    cookie = response_headers["Set-Cookie"]
    secret = cookie.split(";", 1)[0].split("=", 1)[1]
    return secret, cookie, body


def entitle(gate, wallet=HOLDER, balance=5_000_000):
    gate.apply_policy(*signed_policy(grace_s=60), via="cli")
    gate.rpc.balances[wallet.lower()] = balance
    gate.clock.now += 31


def test_session_cookie_and_credential_transports(tmp_path):
    with serving(tmp_path) as (address, gate):
        secret, cookie, body = browser_sign_in(address, gate)
        assert cookie == f"__Host-rhp_session={secret}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age={Limits().session_ttl_s}"
        assert body["signed_in"] and body["wallet"] == HOLDER and body["state"] == "unset" and body["owner"] is False
        assert "secret" not in body
        status, headers, me = call(address, "GET", "/api/gate/me", headers={"Cookie": cookie.split(";")[0]})
        assert status == 200 and me["via"] == "cookie" and headers["Cache-Control"] == "private, no-store" and "ETag" not in headers
        status, _, me = call(address, "GET", "/api/gate/me", headers={"Authorization": "Bearer " + secret})
        assert me["via"] == "bearer"
        status, _, me = call(address, "GET", "/api/gate/me")
        assert status == 200 and me == {"signed_in": False, "policy": gate.policy().public()}
        status, _, body = call(address, "GET", "/api/gate/me", headers={"Authorization": "Bearer rhp_bogus"})
        assert status == 401 and body["gate"]["state"] == "invalid"
        status, _, body = call(address, "POST", "/api/gate/keys", {"op": "revoke_all"}, {"Cookie": cookie.split(";")[0]})
        assert status == 403
        status, _, body = call(address, "POST", "/api/gate/keys", {"op": "revoke_all"}, {"Authorization": "Bearer " + secret})
        assert status == 200 and body == {"revoked": 1}
        status, _, _ = call(address, "GET", "/api/gate/keys", headers={"Authorization": "Bearer " + secret})
        assert status == 401


def test_session_post_refuses_cross_site_origins_without_charging_valid_signins(tmp_path):
    with serving(tmp_path) as (address, gate):
        message, signature = siwe(HOLDER_KEY, gate)
        status, _, body = call(address, "POST", "/api/gate/session", {"message": message, "signature": signature}, {"Origin": "https://evil.example", "Host": "rhpools.lol"})
        assert status == 403
        status, _, body = call(address, "POST", "/api/gate/session", {"message": message, "signature": signature}, {"Content-Type": "text/plain"})
        assert status == 415
        for _ in range(6):
            message, signature = siwe(HOLDER_KEY, gate)
            status, _, body = call(address, "POST", "/api/gate/session", {"message": message, "signature": signature}, ORIGIN)
            assert status == 200
        assert [row["action"] for row in gate.audit()].count("session.sign_in") == 6


def test_keyed_rest_tier_carries_quota_headers_and_private_caching(tmp_path):
    with serving(tmp_path) as (address, gate):
        secret, cookie, _ = browser_sign_in(address, gate)
        bearer = {"Authorization": "Bearer " + secret}
        status, _, body = call(address, "GET", "/api/v1/pools", headers=bearer)
        assert status == 403 and body["gate"]["state"] == "unset"
        entitle(gate, balance=10)
        status, _, body = call(address, "GET", "/api/v1/pools", headers=bearer)
        assert status == 403 and body["gate"] == {"state": "below", "feature": "api", "need": "3000000", "have": "10", "grace_until": None}
        gate.rpc.balances[HOLDER.lower()] = 5_000_000
        gate.clock.now += 31
        status, headers, body = call(address, "GET", "/api/v1/pools", headers=bearer)
        assert status == 200 and body["route"] == "public.pools"
        assert headers["Cache-Control"] == "private, no-store" and "ETag" not in headers
        assert headers["Vary"] == "Accept-Encoding, Authorization, Cookie"
        assert (headers["X-RateLimit-Limit"], headers["X-RateLimit-Remaining"]) == ("3", "2")
        call(address, "GET", "/api/lp/tape", headers=bearer)
        call(address, "GET", "/api/lp/status", headers=bearer)
        status, headers, body = call(address, "GET", "/api/lp/status", headers=bearer)
        assert status == 429 and headers["Retry-After"] == "1" and body["gate"]["retry_after_ms"] > 0
        status, headers, _ = call(address, "GET", "/api/v1/pools", headers={"Cookie": cookie.split(";")[0]})
        assert status == 200 and headers["Cache-Control"].startswith("public") and "ETag" in headers
        status, headers, _ = call(address, "OPTIONS", "/api/gate/nonce")
        assert status == 204 and headers["Access-Control-Allow-Headers"] == "Accept, Content-Type, Authorization"
        status, headers, _ = call(address, "OPTIONS", "/api/v1/pools")
        assert headers["Access-Control-Allow-Headers"] == "Accept, Content-Type"


def test_keys_mint_only_from_browser_session_and_revocation_is_immediate(tmp_path):
    with serving(tmp_path) as (address, gate):
        secret, cookie, _ = browser_sign_in(address, gate)
        entitle(gate)
        session_cookie = {"Cookie": cookie.split(";")[0]}
        status, _, body = call(address, "POST", "/api/gate/keys", {"op": "mint", "label": "bot"}, session_cookie)
        assert status == 403 and body["error"] == "Configured same-origin request required"
        status, _, minted = call(address, "POST", "/api/gate/keys", {"op": "mint", "label": "bot", "ttl_s": 3600}, {**session_cookie, **ORIGIN})
        assert status == 200 and minted["secret"].startswith("rhp_") and minted["label"] == "bot"
        status, _, body = call(address, "POST", "/api/gate/keys", {"op": "mint", "label": "bot2"}, {"Authorization": "Bearer " + minted["secret"]})
        assert status == 403
        status, _, listing = call(address, "GET", "/api/gate/keys", headers=session_cookie)
        assert {key["key_id"] for key in listing["keys"]} >= {minted["key_id"]}
        assert all(minted["secret"] not in json.dumps(key) for key in listing["keys"])
        status, headers, _ = call(address, "GET", "/api/lp/tape", headers={"Authorization": "Bearer " + minted["secret"]})
        assert status == 200 and headers["X-RateLimit-Remaining"] == "2"
        status, _, body = call(address, "POST", "/api/gate/keys", {"op": "revoke", "key_id": minted["key_id"]}, {**session_cookie, **ORIGIN})
        assert body == {"revoked": True}
        status, _, body = call(address, "GET", "/api/lp/tape", headers={"Authorization": "Bearer " + minted["secret"]})
        assert status == 401
        status, headers, body = call(address, "POST", "/api/gate/logout", {}, {**session_cookie, **ORIGIN})
        assert status == 200 and headers["Set-Cookie"].startswith("__Host-rhp_session=; ") and "Max-Age=0" in headers["Set-Cookie"]
        status, _, me = call(address, "GET", "/api/gate/me", headers=session_cookie)
        assert me["signed_in"] is False


def test_policy_route_accepts_owner_only_and_audits(tmp_path):
    with serving(tmp_path) as (address, gate):
        parsed, signature = signed_policy(OTHER_KEY)
        status, _, body = call(address, "POST", "/api/gate/policy", {"policy": parsed.public(), "signature": signature})
        assert status == 403 and body["gate"]["reason"] == "signer is not the gate owner"
        parsed, signature = signed_policy()
        status, _, body = call(address, "POST", "/api/gate/policy", {"policy": parsed.public(), "signature": signature})
        assert status == 200 and body["applied"]["version"] == 1 and body["policy"]["token"] == parsed.token
        status, _, body = call(address, "GET", "/api/gate/policy")
        assert body["owner"] == OWNER and body["typed_data"]["primaryType"] == "GatePolicy"
        status, _, body = call(address, "POST", "/api/gate/policy", {"policy": {"version": "x"}, "signature": signature})
        assert status == 400
        assert [row["action"] for row in gate.audit()][::-1] == ["policy.apply"]


def test_policy_post_limits_ip_before_body_and_keeps_keyed_slot_free(tmp_path):
    with serving(tmp_path, Limits(policy_per_ip_per_min=1)) as (address, gate):
        parsed, signature = signed_policy(OTHER_KEY)
        status, _, _ = call(address, "POST", "/api/gate/policy", {"policy": parsed.public(), "signature": signature})
        assert status == 403
        parsed, signature = signed_policy()
        status, headers, body = call(address, "POST", "/api/gate/policy", {"policy": parsed.public(), "signature": signature})
        assert status == 429 and headers["Retry-After"] == "60"
        assert gate.policy().token is None
        status, _, body = call(address, "POST", "/api/gate/policy", {"policy": parsed.public(), "signature": signature},
                               {"CF-Connecting-IP": "198.51.100.23"})
        assert status == 200 and gate.policy().token == parsed.token
        gate._policy_slots.acquire()
        gate._policy_slots.acquire()
        try:
            status, _, body = call(address, "POST", "/api/gate/policy",
                                   {"policy": parsed.public(), "signature": signature},
                                   {"CF-Connecting-IP": "198.51.100.24"})
            assert status == 503
            message, signature = siwe(HOLDER_KEY, gate)
            status, _, body = call(address, "POST", "/api/gate/session",
                                   {"message": message, "signature": signature})
            assert status == 200 and body["wallet"] == HOLDER
        finally:
            gate._policy_slots.release()
            gate._policy_slots.release()


def test_policy_rejects_oversize_body_without_reading_it(tmp_path):
    with serving(tmp_path) as (address, gate):
        connection = http.client.HTTPConnection(*address, timeout=5)
        try:
            connection.putrequest("POST", "/api/gate/policy")
            connection.putheader("Content-Type", "application/json")
            connection.putheader("Content-Length", "3000")
            connection.endheaders()
            response = connection.getresponse()
            assert response.status == 413
            response.read()
        finally:
            connection.close()


def test_nonce_http_rate_limit_groups_ipv6_64(tmp_path):
    with serving(tmp_path, Limits(nonce_per_ip_per_min=1)) as (address, gate):
        status, _, first = call(address, "GET", "/api/gate/nonce", headers={"CF-Connecting-IP": "2001:db8:1:2::1"})
        assert status == 200
        status, _, body = call(address, "GET", "/api/gate/nonce", headers={"CF-Connecting-IP": "2001:db8:1:2::2"})
        assert status == 429
        message, signature = siwe(HOLDER_KEY, gate, nonce=first["nonce"])
        assert gate.sign_in(message, signature, label="holder", client_ip="2001:db8:1:2::2")[0].wallet == HOLDER


def read_frames(response, count):
    frames, current = [], {}
    while len(frames) < count:
        raw = response.readline()
        if not raw:
            frames.append(None)
            break
        line = raw.decode().rstrip("\n")
        if not line:
            if current:
                frames.append(current)
            current = {}
        elif line.startswith("event: "):
            current["event"] = line[7:]
        elif line.startswith("data: "):
            current["data"] = json.loads(line[6:])
    return frames


def test_keyed_sse_stream_requires_a_key_and_closes_after_grace(tmp_path):
    with serving(tmp_path) as (address, gate):
        status, _, body = call(address, "GET", "/api/v1/stream?channel=activity")
        assert status == 401
        secret, _, _ = browser_sign_in(address, gate)
        entitle(gate)
        connection = http.client.HTTPConnection(*address, timeout=5)
        connection.request("GET", "/api/v1/stream?channel=both&view=terminal&current_only=1&owners=0", headers={"Authorization": "Bearer " + secret})
        response = connection.getresponse()
        assert response.status == 200 and response.getheader("Content-Type") == "text/event-stream"
        first = read_frames(response, 2)
        assert [frame["event"] for frame in first] == ["block", "activity"]
        gate.rpc.balances[HOLDER.lower()] = 1
        gate.clock.now += 31
        gate.clock.now += 61
        frames = read_frames(response, 2)
        assert frames[0]["event"] == "gate" and frames[0]["data"]["gate"]["state"] == "below"
        assert frames[1] is None
        connection.close()


def test_keyed_websocket_stream_pumps_json_frames_and_closes_4403_on_revoke(tmp_path):
    with serving(tmp_path) as (address, gate):
        secret, _, _ = browser_sign_in(address, gate)
        entitle(gate)
        uri = f"ws://{address[0]}:{address[1]}/api/v1/stream?channel=both&view=terminal&current_only=1&owners=0"
        with pytest.raises(Exception) as refused:
            ws_connect(uri, additional_headers={"Authorization": "Bearer rhp_bogus"}, open_timeout=5)
        assert "401" in str(refused.value)
        with ws_connect(uri, additional_headers={"Authorization": "Bearer " + secret}, open_timeout=5) as socket:
            first = json.loads(socket.recv(timeout=5))
            assert first["event"] == "block" and first["data"]["number"] == 100 and first["id"] == "0:fe1:1"
            second = json.loads(socket.recv(timeout=5))
            assert second["event"] == "activity"
            gate.revoke_all(HOLDER)
            closing = json.loads(socket.recv(timeout=5))
            while closing["event"] != "gate":
                closing = json.loads(socket.recv(timeout=5))
            assert closing == {"event": "gate", "data": {"error": "credential revoked", "gate": {"state": "revoked"}}}
            with pytest.raises(Exception):
                socket.recv(timeout=5)
            assert socket.protocol.close_code == 4403
