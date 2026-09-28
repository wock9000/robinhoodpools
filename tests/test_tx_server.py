import threading
from contextlib import contextmanager
from types import SimpleNamespace

from golden.anonymous import stub_runtime
from rhpools.lp_gate import Gate, Limits
from rhpools.lp_server import Handler, LPHTTPServer
from rhpools.tx_plan import SwapIntent, TxRefusal
from test_gate import HOLDER, OWNER, Clock, FakeRpc, signed_policy
from test_gate_server import ORIGIN, browser_sign_in, call

OTHER_WALLET = "0x" + "99" * 20
TOKEN = "0x" + "12" * 20
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
SWAP = {"kind": "swap", "side": "buy", "token": TOKEN, "quote_currency": USDG, "amount_in": "1000000", "slippage_bps": 100}


class FakeCore:
    enabled = True
    ttl_s = 60

    def __init__(self):
        self.intents, self.kinds = [], {}

    def quote(self, intent):
        self.intents.append(intent)
        if isinstance(intent, SwapIntent) and intent.token == "0x" + "00" * 20:
            raise TxRefusal("no_route", "no pool")
        quote_id = f"{len(self.intents):064x}"
        self.kinds[quote_id] = "swap" if isinstance(intent, SwapIntent) else "lp"
        return SimpleNamespace(to_json=lambda: {"quote_id": quote_id, "wallet": intent.wallet})

    def kind_of(self, quote_id):
        return self.kinds.get(quote_id)

    def prepare(self, quote_id, wallet, sigs, batched=False):
        return SimpleNamespace(to_json=lambda: {"quote_id": quote_id, "wallet": wallet, "permit": sigs.permit.hex() if sigs.permit else None})
    def history(self, wallet):
        self.history_wallet = wallet
        return {"rows": [{"hash": "0x" + "ab" * 32, "block": 42, "timestamp": 84,
                          "sent": [], "received": [], "via": "UR"}]}

    def balances(self, wallet, currencies):
        return {}

    def pool_view(self, pool_id, wallet, known):
        return {"wallet": wallet, "pool_id": pool_id}

    def receipt(self, tx_hash, wallet):
        return SimpleNamespace(to_json=lambda: {"wallet": wallet, "hash": tx_hash})


@contextmanager
def serving(tmp_path, core):
    clock, rpc = Clock(), FakeRpc()
    gate = Gate(tmp_path / "gate.sqlite", owner=OWNER, rpc_url="http://127.0.0.1:1", hosts=frozenset({"rhpools.lol"}), clock=clock, rpc=rpc, limits=Limits(key_rps=50, key_burst=50))
    gate.clock, gate.rpc = clock, rpc
    runtime = stub_runtime()
    runtime.gate, runtime.tx, runtime.tx_unavailable = gate, core, None if core else "fee recipient not configured"
    handler = type("TxHandler", (Handler,), {"runtime": runtime, "log_message": lambda *a: None})
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


def signed_in(address, gate, balance, **threshold):
    gate.apply_policy(*signed_policy(grace_s=60, **({"threshold": threshold} if threshold else {})), via="cli")
    gate.rpc.balances[HOLDER.lower()] = balance
    gate.clock.now += 31
    secret, _cookie, _body = browser_sign_in(address, gate)
    return {**ORIGIN, "Cookie": f"__Host-rhp_session={secret}"}


def test_quote_requires_a_signed_in_holder(tmp_path):
    with serving(tmp_path, FakeCore()) as (address, gate):
        assert call(address, "POST", "/api/tx/quote", SWAP, ORIGIN)[0] == 401
        headers = signed_in(address, gate, balance=10)
        status, _, body = call(address, "POST", "/api/tx/quote", SWAP, headers)
        assert status == 403 and body["gate"]["state"] == "below"


def test_quote_uses_the_session_wallet_not_the_body(tmp_path):
    core = FakeCore()
    with serving(tmp_path, core) as (address, gate):
        headers = signed_in(address, gate, balance=5_000_000)
        status, response_headers, body = call(address, "POST", "/api/tx/quote", {**SWAP, "wallet": OTHER_WALLET}, headers)
        assert status == 200 and body["wallet"] == HOLDER.lower()
        assert core.intents[0].wallet == HOLDER.lower() and response_headers["Cache-Control"] == "private, no-store"
        status, _, body = call(address, "POST", "/api/tx/quote", {**SWAP, "token": "0x" + "00" * 20}, headers)
        assert status == 422 and body["refusal"] == "no_route"


def test_cookie_posts_must_be_same_origin(tmp_path):
    with serving(tmp_path, FakeCore()) as (address, gate):
        headers = signed_in(address, gate, balance=5_000_000)
        foreign = {**headers, "Origin": "https://evil.example"}
        assert call(address, "POST", "/api/tx/quote", SWAP, foreign)[0] == 403


def test_lp_only_holder_cannot_prepare_a_swap_quote(tmp_path):
    core = FakeCore()
    with serving(tmp_path, core) as (address, gate):
        headers = signed_in(address, gate, balance=1_500_000, trade="3000000", lp="1000000", api="3000000", flags="0")
        assert call(address, "POST", "/api/tx/quote", SWAP, headers)[0] == 403
        swap_id = f"{7:064x}"
        core.kinds[swap_id] = "swap"
        status, _, _ = call(address, "POST", "/api/tx/prepare", {"quote_id": swap_id}, headers)
        assert status == 403
        lp = {"kind": "lp", "op": "collect", "pool_id": "0x" + "34" * 20, "slippage_bps": 50, "token_id": "5"}
        status, _, body = call(address, "POST", "/api/tx/quote", lp, headers)
        assert status == 200
        status, _, body = call(address, "POST", "/api/tx/prepare", {"quote_id": body["quote_id"], "permit_signature": "0x" + "ab" * 65}, headers)
        assert status == 200 and body["wallet"].lower() == HOLDER.lower() and body["permit"] == "ab" * 65


def test_trading_disabled_without_fee_recipient(tmp_path):
    with serving(tmp_path, None) as (address, gate):
        status, _, body = call(address, "GET", "/api/tx/status")
        assert status == 200 and body["enabled"] is False and body["reason"] == "fee recipient not configured"
        headers = signed_in(address, gate, balance=5_000_000)
        status, _, body = call(address, "POST", "/api/tx/quote", SWAP, headers)
        assert status == 422 and body["refusal"] == "trading_disabled"


def test_history_requires_trade_browser_session_and_uses_only_session_wallet(tmp_path):
    core = FakeCore()
    with serving(tmp_path, core) as (address, gate):
        path = "/api/tx/history?feature=trade&wallet=" + OTHER_WALLET
        assert call(address, "GET", path)[0] == 401
        headers = signed_in(address, gate, balance=5_000_000)
        assert call(address, "GET", "/api/tx/history?feature=lp", headers=headers)[0] == 400
        secret = headers["Cookie"].split("=", 1)[1]
        assert call(address, "GET", path, headers={"Authorization": "Bearer " + secret})[0] == 403
        status, _, key = call(address, "POST", "/api/gate/keys",
                              {"op": "mint", "label": "bot", "ttl_s": 3600}, headers)
        assert status == 200
        assert call(address, "GET", path, headers={"Authorization": "Bearer " + key["secret"]})[0] == 403
        status, response_headers, body = call(address, "GET", path, headers=headers)
        assert status == 200 and body["rows"][0]["via"] == "UR"
        assert core.history_wallet.lower() == HOLDER.lower()
        assert response_headers["Cache-Control"] == "private, no-store"


def test_tx_quota_charges_weighted_requests(tmp_path):
    core = FakeCore()
    with serving(tmp_path, core) as (address, gate):
        headers = signed_in(address, gate, balance=5_000_000)
        gate.limits = Limits(key_rps=1, key_burst=12)
        assert call(address, "POST", "/api/tx/quote", SWAP, headers)[0] == 200
        assert call(address, "GET", "/api/tx/history?feature=trade", headers=headers)[0] == 200
        assert call(address, "POST", "/api/tx/quote", SWAP, headers)[0] == 429
        assert len(core.intents) == 1


def test_tx_get_takes_api_slot(tmp_path):
    core = FakeCore()
    with serving(tmp_path, core) as (address, gate):
        headers = signed_in(address, gate, balance=5_000_000)
        from rhpools.lp_server import Handler
        slots = Handler.api_slots["keyed"]
        acquired = []
        while slots.acquire(False):
            acquired.append(True)
        try:
            assert call(address, "GET", "/api/tx/history?feature=trade", headers=headers)[0] == 503
        finally:
            for _ in acquired:
                slots.release()


def test_tx_prepare_and_pool_charge_five_tokens(tmp_path):
    core = FakeCore()
    with serving(tmp_path, core) as (address, gate):
        headers = signed_in(address, gate, balance=5_000_000)
        gate.limits = Limits(key_rps=1, key_burst=16)
        status, _, quote = call(address, "POST", "/api/tx/quote", SWAP, headers)
        assert status == 200
        assert call(address, "POST", "/api/tx/prepare", {"quote_id": quote["quote_id"]}, headers)[0] == 200
        assert call(address, "GET", "/api/tx/history?feature=trade", headers=headers)[0] == 200
        assert call(address, "GET", "/api/tx/pool?pool_id=" + TOKEN, headers=headers)[0] == 429


def test_tx_pool_five_tokens_leaves_one_for_receipt(tmp_path):
    with serving(tmp_path, FakeCore()) as (address, gate):
        headers = signed_in(address, gate, balance=5_000_000)
        gate.limits = Limits(key_rps=1, key_burst=6)
        assert call(address, "GET", "/api/tx/pool?pool_id=" + TOKEN, headers=headers)[0] == 200
        assert call(address, "GET", "/api/tx/receipt?hash=" + "ab" * 32, headers=headers)[0] == 200
        assert call(address, "GET", "/api/tx/balances?currencies=" + USDG, headers=headers)[0] == 429
