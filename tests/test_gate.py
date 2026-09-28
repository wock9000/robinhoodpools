"""Gate core: owner-signed policy, SIWE sign-in, one credential, entitlement with grace."""
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path

import pytest
from eth_keys import keys
from eth_utils import keccak

from rhpools.lp_gate import (
    FEATURES, Gate, GatePolicy, GateRefusal, Holding, HoldingState, Limits, entitle,
)
from rhpools.lp_gate_cli import main as gate_cli
from rhpools.lp_gate_siwe import build_message, parse_message, personal_sign_hash

USDG = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
OWNER_KEY = keys.PrivateKey(keccak(text="rhpools gate test owner"))
HOLDER_KEY = keys.PrivateKey(keccak(text="rhpools gate test holder"))
OTHER_KEY = keys.PrivateKey(keccak(text="rhpools gate test other"))
OWNER = OWNER_KEY.public_key.to_checksum_address()
HOLDER = HOLDER_KEY.public_key.to_checksum_address()
OTHER = OTHER_KEY.public_key.to_checksum_address()
POLICY_JSON = {
    "version": 1, "token": USDG, "decimals": 6,
    "threshold": {"trade": "1000000", "lp": "2000000", "api": "3000000", "flags": "0"},
    "grace_s": 900, "issued_at": 1790000000,
}
CAST_POLICY_SIGNATURE = (
    "0x31d52bfe2e8971dd76ca1fd9a9c62d7877d88ffe46857b20ac83fde00196ee11"
    "0395c160ffa4d543aa373fbf9f6a6f89089f8cdc398fd0c08bc83e9c45bc16641c"
)
CAST_SIWE_SIGNATURE = (
    "0x78ad22659c1888cce3cb038ef2b964fc3a4846331a70f83a3e022ce42e95508e"
    "79e8b05cbcb96d9439451165194aba63e9ddc3b723d91086b8e7862fe3f191df1c"
)
STATEMENT = "Sign in to rhpools. No transaction, no fee."


class Clock:
    def __init__(self, now=1790000000.0):
        self.now = now

    def __call__(self):
        return self.now


class FakeRpc:
    def __init__(self):
        self.balances = {}
        self.code = {}
        self.erc1271 = {}
        self.block = 100
        self.down = False
        self.calls = 0

    def __call__(self, method, params):
        self.calls += 1
        if self.down:
            raise OSError("rpc down")
        if method == "eth_blockNumber":
            return hex(self.block)
        if method == "eth_getCode":
            return self.code.get(params[0].lower(), "0x")
        if method == "eth_call":
            data = params[0]["data"]
            if data.startswith("0x70a08231"):
                return "0x" + f"{self.balances.get('0x' + data[-40:].lower(), 0):064x}"
            if data.startswith("0x1626ba7e"):
                return self.erc1271.get(params[0]["to"].lower(), "0x" + "00" * 32)
        raise ValueError(method)


def sign(key, digest):
    return key.sign_msg_hash(digest).to_hex()


def policy(**overrides):
    return GatePolicy.parse({**POLICY_JSON, **overrides})


def signed_policy(key=OWNER_KEY, **overrides):
    parsed = policy(**overrides)
    return parsed, sign(key, parsed.digest())


@pytest.fixture
def gate(tmp_path):
    clock = Clock()
    rpc = FakeRpc()
    instance = Gate(
        tmp_path / "gate.sqlite", owner=OWNER, rpc_url="http://127.0.0.1:1",
        hosts=frozenset({"rhpools.lol"}), clock=clock, rpc=rpc, limits=Limits(keys_per_wallet=2, key_rps=10, key_burst=3),
    )
    instance.clock, instance.rpc = clock, rpc
    yield instance
    instance.close()


def siwe(key=HOLDER_KEY, gate=None, *, domain="rhpools.lol", uri="https://rhpools.lol/", chain_id=4663,
         nonce=None, expiration="2026-10-01T00:00:00Z", issued="2026-09-20T00:00:00Z", address=None):
    nonce = nonce or gate.nonce()["nonce"]
    message = build_message(
        domain=domain, address=address or key.public_key.to_checksum_address(), statement=STATEMENT,
        uri=uri, chain_id=chain_id, nonce=nonce, issued_at=issued, expiration_time=expiration,
    )
    return message, sign(key, personal_sign_hash(message))


def sign_in(gate, key=HOLDER_KEY, ip="10.0.0.1", **kw):
    message, signature = siwe(key, gate, **kw)
    return gate.sign_in(message, signature, label="test", client_ip=ip)


def audit_actions(gate):
    return [(row["action"], row["actor"]) for row in gate.audit()]

def test_nonce_flood_cannot_evict_pending_sign_in(gate):
    message, signature = siwe(HOLDER_KEY, gate)
    for _ in range(4096):
        try:
            gate.nonce()
        except GateRefusal as refusal:
            assert refusal.status == 503
            break
    principal, _ = gate.sign_in(message, signature, label="holder", client_ip="2001:db8::1")
    assert principal.wallet == HOLDER


def test_signin_garbage_from_many_ips_does_not_block_valid_wallet(gate):
    for index in range(65):
        with pytest.raises(GateRefusal):
            gate.sign_in("garbage", "0x", label="bad", client_ip=f"198.51.100.{index}")
    assert gate.audit() == []
    principal, _ = sign_in(gate)
    assert principal.wallet == HOLDER


def test_failed_signin_rate_limit_groups_ipv6_64_and_not_successes(gate):
    message, _ = siwe(HOLDER_KEY, gate)
    for index in range(5):
        with pytest.raises(GateRefusal) as refused:
            gate.sign_in(message, "0x", label="bad", client_ip=f"2001:db8:feed:1234::{index}")
        assert refused.value.status == 401
    message, signature = siwe(HOLDER_KEY, gate)
    with pytest.raises(GateRefusal) as refused:
        gate.sign_in(message, signature, label="holder", client_ip="2001:db8:feed:1234::99")
    assert refused.value.status == 429
    principal, _ = gate.sign_in(message, signature, label="holder", client_ip="2001:db8:feed:5678::1")
    assert principal.wallet == HOLDER


def test_nonowner_and_malformed_policy_refusals_never_write_audit(gate):
    parsed, signature = signed_policy(OTHER_KEY)
    for candidate in (signature, "broken"):
        with pytest.raises(GateRefusal):
            gate.apply_policy(parsed, candidate, via="web")
    assert gate.audit() == []


def test_fresh_below_threshold_cancels_grace_but_oracle_outage_keeps_it(gate):
    gate.apply_policy(*signed_policy(), via="cli")
    gate.rpc.balances[HOLDER.lower()] = 5_000_000
    assert gate.entitlement(HOLDER).has("api")
    gate.clock.now += 31
    gate.rpc.down = True
    assert gate.entitlement(HOLDER).has("api")
    gate.rpc.down = False
    gate.rpc.balances[HOLDER.lower()] = 0
    gate.clock.now += 31
    assert not gate.entitlement(HOLDER).has("api")



def test_policy_digest_matches_cast_wallet_sign(gate):
    applied = gate.apply_policy(policy(), CAST_POLICY_SIGNATURE, via="cli")
    assert applied.token == USDG and applied.threshold["api"] == 3000000
    assert gate.policy() == applied
    assert audit_actions(gate) == [("policy.apply", OWNER)]


def test_siwe_signature_matches_cast_wallet_sign():
    message = build_message(
        domain="rhpools.lol", address=OWNER, statement=STATEMENT, uri="https://rhpools.lol/", chain_id=4663,
        nonce="abcdef0123456789", issued_at="2026-09-27T00:00:00Z", expiration_time="2026-09-27T00:10:00Z",
    )
    from rhpools.lp_gate_siwe import recover_signer
    assert recover_signer(personal_sign_hash(message), CAST_SIWE_SIGNATURE) == OWNER
    parsed = parse_message(message)
    assert (parsed.domain, parsed.address, parsed.nonce, parsed.chain_id) == ("rhpools.lol", OWNER, "abcdef0123456789", 4663)


def test_policy_refusals_audit_only_owner_signed_attempts(gate):
    parsed, signature = signed_policy(OTHER_KEY)
    with pytest.raises(GateRefusal) as refused:
        gate.apply_policy(parsed, signature, via="web")
    assert refused.value.status == 403
    assert gate.policy().token is None
    parsed, signature = signed_policy(issued_at=1790000000 + 601)
    with pytest.raises(GateRefusal) as refused:
        gate.apply_policy(parsed, signature, via="web")
    assert refused.value.status == 400
    gate.apply_policy(*signed_policy(), via="web")
    parsed, signature = signed_policy(version=1, grace_s=1)
    with pytest.raises(GateRefusal) as refused:
        gate.apply_policy(parsed, signature, via="cli")
    assert refused.value.status == 409
    with pytest.raises(GateRefusal):
        gate.apply_policy(parsed, "0x" + "11" * 65, via="cli")
    assert [action for action, _ in audit_actions(gate)] == [
        "policy.refused", "policy.apply", "policy.refused",
    ]
    assert audit_actions(gate)[1][1] == OWNER


def test_policy_apply_is_idempotent_on_the_same_signature(gate):
    parsed, signature = signed_policy()
    gate.apply_policy(parsed, signature, via="web")
    assert gate.apply_policy(parsed, signature, via="cli") == parsed
    assert len([row for row in gate.audit() if row["action"] == "policy.apply"]) == 1


def test_owner_unset_refuses_every_signer(tmp_path):
    gate = Gate(tmp_path / "g.sqlite", owner=None, rpc_url="http://127.0.0.1:1", hosts=frozenset(), rpc=FakeRpc(), clock=Clock())
    with pytest.raises(GateRefusal) as refused:
        gate.apply_policy(*signed_policy(), via="cli")
    assert refused.value.payload["gate"]["reason"] == "gate owner not configured"
    gate.close()


def test_cli_applies_through_the_same_path(gate, tmp_path, capsys):
    parsed, signature = signed_policy(issued_at=int(time.time()))
    (tmp_path / "policy.json").write_text(json.dumps({**POLICY_JSON, "issued_at": parsed.issued_at}))
    db = str(tmp_path / "gate.sqlite")
    assert gate_cli(["--db", db, "--owner", OWNER, "policy", "typed-data", str(tmp_path / "policy.json")]) == 0
    assert json.loads(capsys.readouterr().out) == parsed.typed_data()
    assert gate_cli(["--db", db, "--owner", OTHER, "policy", "apply", str(tmp_path / "policy.json"), "--signature", signature]) == 2
    assert gate_cli(["--db", db, "--owner", OWNER, "policy", "apply", str(tmp_path / "policy.json"), "--signature", signature]) == 0
    gate.clock.now += 6
    assert gate.policy().version == 1
    assert gate_cli(["--db", db, "audit"]) == 0
    actions = [json.loads(line)["action"] for line in capsys.readouterr().out.splitlines() if line.startswith("{\"id\"")]
    assert actions == ["policy.apply"]


def test_entitle_rule_is_pure_and_covers_grace_and_threshold_zero():
    live = policy()
    holding = Holding(HOLDER, 2500000, 100, 1000.0)
    ent = entitle(live, HoldingState(HOLDER, holding), 1000.0)
    assert ent.features == {"trade", "lp", "flags"} and not ent.grace_until
    anchors = {"api": (900.0, 1)}
    ent = entitle(live, HoldingState(HOLDER, holding, anchors), 1000.0)
    assert not ent.has("api") and not ent.grace_until
    stale = Holding(HOLDER, 2500000, 100, 900.0)
    ent = entitle(live, HoldingState(HOLDER, stale, anchors), 1000.0)
    assert ent.has("api") and ent.grace_until == {"api": 1800.0}
    assert entitle(live, HoldingState(HOLDER, holding, anchors), 1801.0).has("api") is False
    assert entitle(live, HoldingState(HOLDER, holding, {"api": (999.0, 0)}), 1000.0).has("api") is False
    unset = policy(token=None)
    assert entitle(unset, HoldingState(HOLDER, holding, {"api": (999.0, 0)}), 1000.0).features == frozenset()
    assert entitle(live, HoldingState(HOLDER, None), 1000.0).features == {"flags"}


def test_open_policy_makes_every_signed_in_wallet_a_holder():
    open_policy = policy(threshold={"trade": "0", "lp": "0", "api": "0", "flags": "0"})
    ent = entitle(open_policy, HoldingState(HOLDER, Holding(HOLDER, 0, 100, 1000.0)), 1000.0)
    assert ent.features == {"trade", "lp", "api", "flags"}
    assert ent.state(open_policy) == "holder"


def test_entitlement_transitions_holder_below_and_outage_grace(gate):
    gate.apply_policy(*signed_policy(), via="cli")
    assert gate.entitlement(HOLDER).features == {"flags"}
    gate.rpc.balances[HOLDER.lower()] = 5_000_000
    gate.clock.now += 31
    assert gate.entitlement(HOLDER).features == set(FEATURES)
    gate.rpc.balances[HOLDER.lower()] = 10
    gate.clock.now += 31
    ent = gate.entitlement(HOLDER)
    assert ent.features == {"flags"} and ent.state(gate.policy()) == "below"
    gate.rpc.down = True
    gate.clock.now += 31
    assert gate.entitlement(HOLDER).features == {"flags"}


def test_grace_earned_under_an_older_policy_does_not_carry_over(gate):
    gate.apply_policy(*signed_policy(), via="cli")
    gate.rpc.balances[HOLDER.lower()] = 5_000_000
    assert gate.entitlement(HOLDER).has("api")
    gate.rpc.down = True
    gate.clock.now += 31
    assert "api" in gate.entitlement(HOLDER).grace_until
    gate.apply_policy(*signed_policy(version=2, issued_at=int(gate.clock.now)), via="cli")
    gate.clock.now += 31
    assert gate.entitlement(HOLDER).has("api") is False


def test_oracle_failure_keeps_a_holding_only_inside_grace(gate):
    gate.apply_policy(*signed_policy(), via="cli")
    gate.rpc.balances[HOLDER.lower()] = 5_000_000
    assert gate.entitlement(HOLDER).holding.balance_raw == 5_000_000
    gate.rpc.down = True
    gate.clock.now += 31
    assert gate.entitlement(HOLDER).holding.balance_raw == 5_000_000
    gate.clock.now += 900
    ent = gate.entitlement(HOLDER)
    assert ent.holding is None and ent.features == {"flags"}


def test_oracle_caches_per_wallet_for_thirty_seconds(gate):
    gate.apply_policy(*signed_policy(), via="cli")
    before = gate.rpc.calls
    for _ in range(5):
        gate.entitlement(HOLDER)
    assert gate.rpc.calls == before + 2
    gate.clock.now += 30
    gate.entitlement(HOLDER)
    assert gate.rpc.calls == before + 4


def test_sign_in_mints_a_session_and_refusals_are_specific(gate):
    principal, secret = sign_in(gate)
    assert principal.wallet == HOLDER and principal.kind == "session" and secret.startswith("rhp_")
    assert gate.resolve({"Cookie": f"__Host-rhp_session={secret}"}).key_id == principal.key_id
    assert gate.resolve({"Authorization": f"Bearer {secret}"}).via == "bearer"
    cases = {
        "domain": dict(domain="evil.example", uri="https://evil.example/"),
        "uri": dict(uri="https://evil.example/"),
        "chain": dict(chain_id=1),
        "expired": dict(expiration="2026-09-01T00:00:00Z"),
        "future": dict(issued="2030-01-01T00:00:00Z"),
        "nonce": dict(nonce="0123456789abcdef"),
        "address": dict(address=OTHER),
    }
    for name, overrides in cases.items():
        with pytest.raises(GateRefusal) as refused:
            sign_in(gate, ip="10.0.0." + str(len(name)), **overrides)
        assert refused.value.status == 401, name
    message, signature = siwe(HOLDER_KEY, gate)
    with pytest.raises(GateRefusal):
        gate.sign_in(message, signature[:10] + ("0" if signature[10] != "0" else "1") + signature[11:], label="x", client_ip="10.1.1.1")
    gate.sign_in(message, signature, label="x", client_ip="10.1.1.2")
    with pytest.raises(GateRefusal) as reused:
        gate.sign_in(message, signature, label="x", client_ip="10.1.1.3")
    assert "nonce" in reused.value.payload["gate"]["reason"]
    assert audit_actions(gate).count(("session.refused", HOLDER)) == 0


def test_sign_in_accepts_loopback_host_and_limits_failed_signatures(gate):
    message, signature = siwe(HOLDER_KEY, gate, domain="127.0.0.1:8196", uri="http://127.0.0.1:8196/")
    with pytest.raises(GateRefusal):
        gate.sign_in(message, signature, label="x", client_ip="9.9.9.9")
    message, signature = siwe(HOLDER_KEY, gate, domain="127.0.0.1:8196", uri="http://127.0.0.1:8196/")
    assert gate.sign_in(message, signature, label="x", client_ip="9.9.9.9", host="127.0.0.1:8196")[0].wallet == HOLDER
    for _ in range(5):
        message, _ = siwe(HOLDER_KEY, gate)
        with pytest.raises(GateRefusal) as refused:
            gate.sign_in(message, "0x", label="x", client_ip="9.9.9.9")
        assert refused.value.status == 401
    message, signature = siwe(HOLDER_KEY, gate)
    with pytest.raises(GateRefusal) as limited:
        gate.sign_in(message, signature, label="x", client_ip="9.9.9.9")
    assert limited.value.status == 429
    gate.clock.now += 61
    gate.sign_in(message, signature, label="x", client_ip="9.9.9.9")


def test_eip7702_delegated_eoa_and_erc1271_contract_wallets(gate):
    gate.rpc.code[HOLDER.lower()] = "0xef0100" + "ab" * 20
    assert sign_in(gate)[0].wallet == HOLDER
    contract = "0x" + "c0" * 20
    gate.rpc.code[contract] = "0x6080"
    message, signature = siwe(OTHER_KEY, gate, address=contract)
    with pytest.raises(GateRefusal):
        gate.sign_in(message, signature, label="x", client_ip="1.1.1.1")
    gate.rpc.erc1271[contract] = "0x1626ba7e" + "00" * 28
    message, signature = siwe(OTHER_KEY, gate, address=contract)
    assert gate.sign_in(message, signature, label="x", client_ip="1.1.1.2")[0].wallet.lower() == contract


def test_credentials_precedence_and_lifecycle(gate):
    gate.apply_policy(*signed_policy(), via="cli")
    gate.rpc.balances[HOLDER.lower()] = 5_000_000
    principal, secret = sign_in(gate)
    assert gate.resolve({}) is None
    assert gate.resolve({"Cookie": "__Host-rhp_session=rhp_nope; other=1"}) is None
    assert gate.resolve({"Authorization": "Basic abc", "Cookie": f"__Host-rhp_session={secret}"}).via == "cookie"
    with pytest.raises(GateRefusal) as invalid:
        gate.resolve({"Authorization": "Bearer rhp_nope", "Cookie": f"__Host-rhp_session={secret}"})
    assert invalid.value.status == 401
    with pytest.raises(GateRefusal):
        gate.mint_key(gate.resolve({"Authorization": f"Bearer {secret}"}), label="bot", ttl_s=3600)
    key1, secret1 = gate.mint_key(principal, label="bot-1", ttl_s=3600)
    key2, _ = gate.mint_key(principal, label="bot-2", ttl_s=10**9)
    assert key2.expires_at - int(gate.clock.now) == gate.limits.key_ttl_max_s
    with pytest.raises(GateRefusal):
        gate.mint_key(principal, label="bot-3", ttl_s=3600)
    with pytest.raises(GateRefusal):
        gate.mint_key(key1, label="bot-3", ttl_s=3600)
    listed = gate.keys(HOLDER)
    assert {record.key_id for record in listed} == {principal.key_id, key1.key_id, key2.key_id}
    assert all(secret1 not in json.dumps(record.public()) for record in listed)
    assert key1.key_id == hashlib.sha256(secret1.encode()).hexdigest()[:16]
    assert gate.revoke(key1.key_id, wallet=OTHER) is False
    assert gate.revoke(key1.key_id, wallet=HOLDER) is True
    assert gate.revoke(key1.key_id, wallet=HOLDER) is False
    with pytest.raises(GateRefusal):
        gate.resolve({"Authorization": f"Bearer {secret1}"})
    assert gate.resolve({"Cookie": f"__Host-rhp_session={secret}"}).wallet == HOLDER
    gate.clock.now += gate.limits.session_ttl_s + 1
    assert gate.resolve({"Cookie": f"__Host-rhp_session={secret}"}) is None
    assert audit_actions(gate).count(("key.revoke", HOLDER)) == 1


def test_require_recheck_admit_and_stream_slots(gate):
    with pytest.raises(GateRefusal) as anonymous:
        gate.require({}, "api")
    assert anonymous.value.status == 401
    principal, secret = sign_in(gate)
    headers = {"Authorization": f"Bearer {secret}"}
    with pytest.raises(GateRefusal) as unset:
        gate.require(headers, "api")
    assert unset.value.payload["gate"]["state"] == "unset"
    gate.apply_policy(*signed_policy(), via="cli")
    with pytest.raises(GateRefusal) as below:
        gate.require(headers, "api")
    assert below.value.payload["gate"] == {"state": "below", "feature": "api", "need": "3000000", "have": "0", "grace_until": None}
    gate.rpc.balances[HOLDER.lower()] = 3_000_000
    gate.clock.now += 31
    resolved, ent = gate.require(headers, "api")
    assert resolved.key_id == principal.key_id and ent.has("api")
    quotas = [gate.admit(resolved) for _ in range(3)]
    assert [quota.remaining for quota in quotas] == [2, 1, 0]
    with pytest.raises(GateRefusal) as limited:
        gate.admit(resolved)
    assert limited.value.status == 429 and limited.value.retry_after == 1
    gate.clock.now += 0.2
    assert gate.admit(resolved).remaining == 1
    with gate.stream_slot(resolved), gate.stream_slot(resolved), gate.stream_slot(resolved), gate.stream_slot(resolved):
        with pytest.raises(GateRefusal):
            with gate.stream_slot(resolved):
                pass
    with gate.stream_slot(resolved):
        pass
    gate.revoke(principal.key_id, wallet=HOLDER)
    with pytest.raises(GateRefusal) as revoked:
        gate.recheck(resolved, "api")
    assert revoked.value.payload["gate"]["state"] == "revoked"


def test_gate_never_imports_signing_or_the_market_store():
    src = Path(__file__).parent.parent / "src" / "rhpools"
    for name in ("lp_gate.py", "lp_gate_siwe.py", "lp_gate_cli.py", "lp_gate_ws.py"):
        text = (src / name).read_text()
        assert "PrivateKey" not in text and "sign_msg" not in text, name
        assert not re.search(r"lp_market_|workbench_market|lp_rpc", text), name


def test_cast_signs_the_same_policy_digest():
    try:
        subprocess.run(["cast", "--version"], capture_output=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("cast not installed; checked-in fixture signatures stand")
    parsed = policy()
    typed = Path("/tmp/rhp-gate-typed.json")
    typed.write_text(json.dumps(parsed.typed_data()))
    fresh = subprocess.check_output(
        ["cast", "wallet", "sign", "--private-key", OWNER_KEY.to_hex(), "--data", "--from-file", str(typed)],
    ).decode().strip()
    assert fresh == CAST_POLICY_SIGNATURE
