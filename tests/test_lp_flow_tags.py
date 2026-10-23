import json
import os
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from rhpools import lp_flow_tags as tags

FIXTURES = Path(__file__).parent / "fixtures" / "flow_tags"
EXPECTED = {
    "router_pons": ({"PONS", "FOMO"}, {"pons_hook", "relay_footprint", "fomo_fill"}),
    "router_plain": ({"FOMO"}, {"relay_footprint", "fomo_fill"}),
    "router_eoa_fill": (set(), {"relay_footprint"}),
    "entrypoint_sell": ({"PONS", "FOMO"}, {"pons_hook", "relay_footprint", "fomo_deposit", "fomo_user_op"}),
    "multicall_sell": ({"PONS"}, {"pons_hook", "relay_footprint"}),
    "direct_pons": ({"PONS"}, {"pons_hook"}),
    "direct_v3": (set(), set()),
}


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


def pool_of(data):
    pool = data["pool"]
    return tags.PoolIdentity(pool["id"], pool["protocol"], pool["hook"], data["launches_word0_nonzero"])


def envelope_of(data):
    return tags.envelope(data["tx"], data["block_time"], data["footprint_logs"])


def fomo_wallets_of(*fixtures):
    return frozenset(
        wallet for data in fixtures for wallet, code in data["wallet_codes"].items()
        if (code or "").lower() == tags.FOMO_WALLET_CODE
    )


def log_matches(log, query):
    if "address" in query and log["address"].lower() != query["address"].lower():
        return False
    for position, wanted in enumerate(query.get("topics") or []):
        if wanted is None:
            continue
        allowed = [wanted] if isinstance(wanted, str) else wanted
        if position >= len(log["topics"]) or log["topics"][position].lower() not in allowed:
            return False
    return True


class FakeRpc:
    def __init__(self, fixtures):
        self.transactions = {f["tx"]["hash"].lower(): f["tx"] for f in fixtures}
        self.logs = [log for f in fixtures for log in f["footprint_logs"]]
        self.launches = {f["pool"]["id"]: f["launches_word0_nonzero"] for f in fixtures}
        self.codes = {wallet: code for f in fixtures for wallet, code in f["wallet_codes"].items()}
        self.head = max((int(f["tx"]["blockNumber"], 16) for f in fixtures), default=0)
        self.calls = []

    def batch(self, calls):
        results = []
        for method, params in calls:
            self.calls.append((method, params))
            if method == "eth_blockNumber":
                results.append(hex(self.head))
            elif method == "eth_getTransactionByHash":
                results.append(self.transactions.get(params[0].lower()))
            elif method == "eth_getLogs":
                query = params[0]
                low, high = int(query["fromBlock"], 16), int(query["toBlock"], 16)
                if high > self.head:
                    raise RuntimeError('{"code": -32000, "message": "invalid block range params"}')
                results.append([
                    log for log in self.logs
                    if low <= int(log["blockNumber"], 16) <= high and log_matches(log, query)
                ])
            elif method == "eth_call":
                assert params[0]["to"] == tags.PONS_HOOK
                pool_id = "0x" + params[0]["data"][len(tags.LAUNCHES_SELECTOR):]
                word0 = "1" if self.launches.get(pool_id) else "0"
                results.append("0x" + word0.rjust(64, "0") + "0" * 64)
            elif method == "eth_getCode":
                results.append(self.codes.get(params[0].lower(), "0x"))
            else:
                raise AssertionError(method)
        return results

    def count(self, method):
        return sum(1 for item, _ in self.calls if item == method)


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_classify_recorded_envelopes(name):
    data = fixture(name)
    tag = tags.classify(pool_of(data), envelope_of(data), None, fomo_wallets_of(data))
    expected_tags, expected_basis = EXPECTED[name]
    assert set(tag.tags) == expected_tags
    assert set(tag.basis) == expected_basis
    assert tag.early_ms is None
    assert tag.tx_hash == data["tx"]["hash"].lower()


def test_relay_footprint_reads_deposit_order_ids_and_fills():
    data = fixture("entrypoint_sell")
    env = envelope_of(data)
    deposit = next(log for log in data["footprint_logs"] if log["address"].lower() == tags.DEPOSITORY)
    assert deposit["topics"][0] == tags.TOPIC_DEPOSIT
    assert env.deposit_order_ids == ("0x" + deposit["data"][2 + 3 * 64:2 + 4 * 64],)
    assert env.deposit_wallets and env.zero_gas_senders and env.router_order_id is None
    assert env.order_ids == env.deposit_order_ids

    removed = [dict(log, removed=True) for log in data["footprint_logs"]]
    anonymous = [{"address": tags.DEPOSITORY, "topics": [], "data": "0x"}]
    assert tags.relay_footprint(removed + anonymous) == tags.RelayFootprint((), (), (), ())
    quiet = tags.envelope(data["tx"], data["block_time"], [])
    assert "FOMO" not in tags.classify(pool_of(data), quiet, None, fomo_wallets_of(data)).tags


def test_router_order_id_is_the_calldata_suffix():
    data = fixture("router_pons")
    env = envelope_of(data)
    assert env.router_order_id == "0x" + data["tx"]["input"][-64:]
    assert env.order_ids == (env.router_order_id,)
    assert env.fill_recipients and not env.deposit_order_ids


def test_generic_relay_counterparties_are_not_fomo():
    eoa_fill, other_7702 = fixture("router_eoa_fill"), fixture("multicall_sell")
    for data in (eoa_fill, other_7702):
        tag = tags.classify(pool_of(data), envelope_of(data), None, fomo_wallets_of(data))
        assert "relay_footprint" in tag.basis and "FOMO" not in tag.tags, data["name"]
    assert tags.classify(pool_of(eoa_fill), envelope_of(eoa_fill), None, frozenset(envelope_of(eoa_fill).fill_recipients)).tags == {"FOMO"}


def test_one_transaction_two_pools_keeps_pons_per_pool():
    pons, plain = fixture("router_pons"), fixture("router_plain")
    env = envelope_of(pons)
    fomo = fomo_wallets_of(pons)
    assert tags.classify(pool_of(pons), env, None, fomo).tags == {"PONS", "FOMO"}
    assert tags.classify(pool_of(plain), env, None, fomo).tags == {"FOMO"}


def observation(kind, status="observed", index="1", order_id=None, at=0):
    return tags.ListenerObservation(index, kind, status, order_id, at)


def test_listener_evidence_and_early_ms():
    data = fixture("router_plain")
    env, pool = envelope_of(data), pool_of(data)
    first_seen = env.block_time * 1000 - 1400
    facts = tags.ListenerFacts((observation("destination_transfer"),), first_seen)
    tag = tags.classify(pool, env, facts, fomo_wallets_of(data))
    assert tag.basis == {"relay_footprint", "fomo_fill", "fomo_listener"}
    assert tag.early_ms == 1400
    assert tag.chain_fomo and tag.listener_fomo

    late = tags.ListenerFacts((observation("destination_transfer"),), env.block_time * 1000 + 5)
    assert tags.classify(pool, env, late, fomo_wallets_of(data)).early_ms is None

    retracted = tags.ListenerFacts((observation("destination_transfer", "retracted"),), None)
    assert "fomo_listener" not in tags.classify(pool, env, retracted, fomo_wallets_of(data)).basis

    generic_op = tags.ListenerFacts((observation("user_operation"),), None)
    assert "fomo_listener" not in tags.classify(pool, env, generic_op, fomo_wallets_of(data)).basis


def test_listener_alone_does_not_tag_fomo():
    data = fixture("direct_v3")
    env, pool = envelope_of(data), pool_of(data)
    facts = tags.ListenerFacts((observation("payment"),), env.block_time * 1000 - 300)
    tag = tags.classify(pool, env, facts)
    assert tag.tags == set() and tag.basis == {"fomo_listener"}
    assert not tag.chain_fomo and tag.listener_fomo


def test_fetch_envelopes_batches_transactions_and_footprint_logs():
    fixtures = [fixture(name) for name in EXPECTED]
    rpc = FakeRpc(fixtures)
    requests = [
        (f["tx"]["hash"].upper(), int(f["tx"]["blockNumber"], 16), f["block_time"]) for f in fixtures
    ]
    unique = {f["tx"]["hash"].lower() for f in fixtures}
    requests.append(("0x" + "ee" * 32, 1, 1))
    envelopes = tags.fetch_envelopes(rpc, requests)
    assert set(envelopes) == unique
    assert rpc.count("eth_getTransactionByHash") == len(unique) + 1
    assert rpc.count("eth_getTransactionReceipt") == 0
    log_queries = [params[0] for method, params in rpc.calls if method == "eth_getLogs"]
    assert len(log_queries) == 3 * len({block for _h, block, _t in requests})
    assert {q["fromBlock"] for q in log_queries} == {hex(block) for _h, block, _t in requests}
    assert all(q["fromBlock"] == q["toBlock"] for q in log_queries)
    for data in fixtures:
        assert envelopes[data["tx"]["hash"].lower()] == envelope_of(data), data["name"]
    assert tags.fetch_envelopes(rpc, []) == {}


def test_blocks_past_the_node_head_wait_instead_of_failing_the_batch():
    fixtures = [fixture(name) for name in EXPECTED]
    rpc = FakeRpc(fixtures)
    requests = [(f["tx"]["hash"], int(f["tx"]["blockNumber"], 16), f["block_time"]) for f in fixtures]
    ahead = ("0x" + "ab" * 32, rpc.head + 3, fixtures[0]["block_time"] + 6)
    envelopes = tags.fetch_envelopes(rpc, [*requests, ahead])
    assert set(envelopes) == {f["tx"]["hash"].lower() for f in fixtures}
    assert tags.fetch_envelopes(rpc, [ahead]) == {}


def test_pons_registry_reads_the_hook_once_per_pool(tmp_path):
    fixtures = [fixture("router_pons"), fixture("entrypoint_sell"), fixture("router_plain")]
    rpc = FakeRpc(fixtures)
    store = tags.TagStore(str(tmp_path / "tags.sqlite"))
    registry = tags.PonsRegistry(rpc, store)
    pools = [f["pool"] for f in fixtures]
    pons_pools = {p["id"] for p in pools if p["hook"] == tags.PONS_HOOK}
    identities = registry.identify(pools)
    assert rpc.count("eth_call") == len(pons_pools)
    assert {pool_id for pool_id, identity in identities.items() if identity.pons_registered} == pons_pools
    assert not identities[fixture("router_plain")["pool"]["id"]].pons_registered

    registry.identify(pools)
    assert rpc.count("eth_call") == len(pons_pools)
    rpc.launches = {}
    again = tags.PonsRegistry(rpc, store).identify(pools)
    assert rpc.count("eth_call") == len(pons_pools)
    assert {pool_id for pool_id, identity in again.items() if identity.pons_registered} == pons_pools

    unregistered = dict(pools[0], id="0x" + "77" * 32)
    assert not registry.identify([unregistered])[unregistered["id"]].pons_registered
    assert rpc.count("eth_call") == len(pons_pools) + 1
    assert store.pons_pools()[unregistered["id"]] is False
    store.close()


def test_tag_store_retention_upsert_and_agreement(tmp_path):
    store = tags.TagStore(str(tmp_path / "tags.sqlite"))
    now = 2_000_000_000
    fresh = tags.FlowTag("0xaa", "0x01", frozenset({"FOMO"}), frozenset({"fomo_fill", "fomo_listener"}), 120)
    chain_only = tags.FlowTag("0xbb", "0x01", frozenset({"FOMO", "PONS"}), frozenset({"fomo_fill", "pons_hook"}), None)
    listener_only = tags.FlowTag("0xcc", "0x02", frozenset({"FOMO"}), frozenset({"fomo_listener"}), None)
    stale = tags.FlowTag("0xdd", "0x02", frozenset(), frozenset(), None)
    store.put(
        [(fresh, 10, now - 60, True), (chain_only, 11, now - 60, True), (listener_only, 12, now - 60, True)],
        now=now,
    )
    store.put([(stale, 1, now - tags.RETENTION_S - 1, False)], now=now)
    assert store.get([("0xaa", "0x01"), ("0xdd", "0x02"), ("0xzz", "0x01")]) == {
        ("0xaa", "0x01"): fresh, ("0xdd", "0x02"): stale,
    }
    status = store.status()
    assert status["rows"] == 4 and status["pons"] == 1 and status["fomo"] == 3
    assert status["fomo_agreement"] == {
        "compared": 3, "agree_fomo": 1, "chain_only": 1, "listener_only": 1,
        "disagreement_rate": pytest.approx(2 / 3),
    }
    assert store.prune(now) == 1
    assert store.get([("0xdd", "0x02")]) == {}

    revised = tags.FlowTag("0xaa", "0x01", frozenset({"FOMO"}), frozenset({"fomo_fill"}), None)
    store.put([(revised, 10, now - 60, False)], now=now)
    assert store.get([("0xaa", "0x01")]) == {("0xaa", "0x01"): revised}
    assert store.status()["fomo_agreement"]["compared"] == 2
    store.close()


def test_observation_window_keeps_latest_status_and_evicts_by_event_time():
    window = tags.ObservationWindow(window_s=10)
    window.ingest([
        {"transaction_hash": "0xAB", "event_index": "3", "kind": "destination_transfer",
         "status": "observed", "order_id": "0xO1", "event_at_ms": 1_000},
        {"transaction_hash": "0xab", "event_index": "5", "kind": "payment",
         "status": "observed", "order_id": None, "event_at_ms": 2_000},
    ])
    assert [o.kind for o in window.observations("0xAB")] == ["destination_transfer", "payment"]
    assert window.observations("0xab")[0].order_id == "0xo1"
    window.ingest([{
        "transaction_hash": "0xab", "event_index": "3", "kind": "destination_transfer",
        "status": "retracted", "order_id": "0xo1", "event_at_ms": 2_500,
    }])
    assert [(o.event_index, o.status) for o in window.observations("0xab")] == [
        ("3", "retracted"), ("5", "observed"),
    ]
    window.ingest([{
        "transaction_hash": "0xcd", "event_index": "1", "kind": "payment",
        "status": "observed", "order_id": None, "event_at_ms": 12_400,
    }])
    assert window.observations("0xab") == (observation("destination_transfer", "retracted", "3", "0xo1", 2_500),)
    assert len(window) == 2 and window.latest_ms == 12_400
    window.ingest([{
        "transaction_hash": "0xcd", "event_index": "2", "kind": "payment",
        "status": "observed", "order_id": None, "event_at_ms": 13_000,
    }])
    assert window.observations("0xab") == () and len(window) == 2


class FakeLedger:
    def __init__(self, rows, instance="inst-1", fail_after=None):
        self.rows = sorted(rows, key=lambda row: row["sequence"])
        self.instance = instance
        self.statements = []
        self.fail_after = fail_after
        self.closed = False
        self.read_only = None

    @contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if self.fail_after is not None and len(self.statements) > self.fail_after:
            raise ConnectionError("socket closed")
        params = list(params or [])
        if sql.startswith("SET"):
            return _Result([])
        if "relay_listener_research_source_state" in sql:
            last = self.rows[-1]["sequence"] if self.rows else 0
            latest = max((row["event_at"] for row in self.rows), default=0)
            return _Result([(self.instance, last)] if latest >= params[0] else [])
        if "MIN(event_at_unix_ms)" in sql:
            wanted = set(params[0])
            firsts = {}
            for row in self.rows:
                if row.get("order_id") in wanted:
                    firsts[row["order_id"]] = min(firsts.get(row["order_id"], row["event_at"]), row["event_at"])
            return _Result(list(firsts.items()))
        if "sequence >= " in sql:
            for row in self.rows:
                if row["sequence"] >= params[1]:
                    return _Result([(row["event_at"],)])
            return _Result([])
        assert "sequence > " in sql
        selected = [row for row in self.rows if row["sequence"] > params[1]][:params[2]]
        return _Result([
            (
                row["sequence"], row["event_at"], row.get("order_id"), "fomo_observation",
                "robinhood", row["hash"], row["index"], row["kind"], "observed", row["block"],
            )
            for row in selected
        ])

    def close(self):
        self.closed = True


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


ORDER = "0x" + "order".encode().hex().rjust(64, "0")


def ledger_row(sequence, at, tx_hash, kind="destination_transfer", order_id=None, block=None):
    return {
        "sequence": sequence, "event_at": at, "hash": tx_hash, "index": str(sequence),
        "kind": kind, "order_id": order_id, "block": sequence if block is None else block,
    }


def test_postgres_listener_seeks_into_the_window_then_tails_by_cursor():
    now_ms = 10_000_000
    rows = [ledger_row(seq, now_ms - 120_000 + seq * 100, f"0x{seq:064x}") for seq in range(1, 1001)]
    rows.append(ledger_row(2000, now_ms - 500, "0xfill", order_id=ORDER, block=5_000))
    rows.append(ledger_row(0, now_ms - 9_000, "0xfill", kind="payment", order_id=ORDER))
    ledger = FakeLedger(rows)
    listener = tags.PostgresListener(
        "postgres://unit", window_s=60, tail_budget=10_000,
        clock=lambda: now_ms / 1000, connect=lambda _dsn: ledger,
    )
    data = fixture("router_plain")
    env = tags.envelope(dict(data["tx"], hash="0xfill", input=ORDER, blockNumber=hex(5_000)), now_ms // 1000, [])
    later = tags.envelope(dict(data["tx"], hash="0xlater", blockNumber=hex(5_001)), now_ms // 1000, [])
    assert env.order_ids == (ORDER,)
    facts = listener.facts([env, later])
    assert facts["0xfill"].covered and not facts["0xlater"].covered
    assert facts["0xlater"].observations == () and facts["0xlater"].order_first_seen_ms is None
    assert listener.state()["latest_block"] == 5_000
    queries = [s for s in ledger.statements if not s.startswith("SET")]
    assert ledger.statements.count("SET TRANSACTION READ ONLY") == len(queries)
    assert all(s.startswith("SET LOCAL statement_timeout = ") for s in ledger.statements[1::3])
    assert [o.event_index for o in facts["0xfill"].observations] == ["2000"]
    assert facts["0xfill"].order_first_seen_ms == now_ms - 9_000
    seeks = [s for s in ledger.statements if "sequence >= " in s]
    assert 5 <= len(seeks) <= 12
    tail_rows = listener.state()["observations"]
    assert tail_rows == 402
    state = listener.state()
    assert state["state"] == "connected" and state["instances"] == 1

    before = len(ledger.statements)
    ledger.rows.append(ledger_row(2001, now_ms - 100, "0xnext"))
    listener.facts([env])
    tails = [s for s in ledger.statements[before:] if "sequence > " in s]
    assert len(tails) == 1 and not [s for s in ledger.statements[before:] if "sequence >= " in s]
    assert listener.state()["observations"] == tail_rows + 1


def test_postgres_listener_reports_down_and_cools_off_before_reconnecting():
    clock = {"now": 100.0}
    attempts = []

    def connect(_dsn):
        attempts.append(clock["now"])
        if len(attempts) == 1:
            raise ConnectionError("refused")
        return FakeLedger([ledger_row(1, int(clock["now"] * 1000), "0xfill", order_id=ORDER)])

    listener = tags.PostgresListener(
        "postgres://unit", window_s=60, retry_s=30, clock=lambda: clock["now"], connect=connect,
    )
    data = fixture("router_plain")
    env = tags.envelope(dict(data["tx"], hash="0xfill", input=ORDER), 100, [])
    assert listener.facts([env]) == {}
    assert listener.state()["state"] == "down" and "refused" in listener.state()["reason"]
    clock["now"] = 120.0
    assert not listener.refresh()
    assert listener.facts([env]) == {} and len(attempts) == 1
    clock["now"] = 131.0
    facts = listener.facts([env])
    assert len(attempts) == 2 and listener.state()["state"] == "connected"
    assert facts["0xfill"].observations[0].kind == "destination_transfer"
    assert facts["0xfill"].order_first_seen_ms == 131_000
    assert not facts["0xfill"].covered
    listener.close()
    assert listener.state()["state"] == "idle"


def test_listener_from_env_reports_unset_and_missing_driver(monkeypatch):
    assert tags.listener_from_env({}).state() == {"state": "unset", "reason": None}
    monkeypatch.setitem(sys.modules, "psycopg", None)
    source = tags.listener_from_env({"RHP_LISTENER_DSN": "postgres://x"})
    assert source.state() == {"state": "unavailable", "reason": "psycopg is not installed"}
    assert source.facts([]) == {}


def test_flow_tagger_classifies_persists_and_reuses(tmp_path):
    fixtures = [fixture(name) for name in EXPECTED]
    rpc = FakeRpc(fixtures)
    store = tags.TagStore(str(tmp_path / "tags.sqlite"))
    pools = {f["pool"]["id"]: f["pool"] for f in fixtures}
    now = max(f["block_time"] for f in fixtures) + 60
    tagger = tags.FlowTagger(rpc, store, pools.get, clock=lambda: float(now))
    rows = [
        {"tx_hash": f["tx"]["hash"], "pool_id": f["pool"]["id"],
         "block_number": int(f["tx"]["blockNumber"], 16), "timestamp": f["block_time"]}
        for f in fixtures
    ]
    rows.append({"tx_hash": "0x" + "ee" * 32, "pool_id": "0x" + "ff" * 32, "block_number": 1, "timestamp": 1})
    rows.append({"tx_hash": rows[0]["tx_hash"], "pool_id": None, "block_number": 1, "timestamp": 1})
    result = tagger.tag(rows)
    assert [(set(t.tags), set(t.basis)) for t in result] == [EXPECTED[f["name"]] for f in fixtures]
    assert rpc.count("eth_getTransactionByHash") == len({f["tx"]["hash"] for f in fixtures})
    assert rpc.count("eth_getLogs") == 3 * len({int(f["tx"]["blockNumber"], 16) for f in fixtures})
    assert rpc.count("eth_getCode") == len({w for f in fixtures for w in envelope_of(f).wallets})
    assert rpc.count("eth_call") == len({f["pool"]["id"] for f in fixtures if f["pool"]["hook"] == tags.PONS_HOOK})

    calls = len(rpc.calls)
    assert tagger.tag(rows[:-2]) == result
    assert len(rpc.calls) == calls
    status = tagger.status()
    assert status["listener"] == {"state": "unset", "reason": None}
    assert status["rows"] == len(fixtures) and status["fomo_agreement"]["compared"] == 0
    assert status["pons_pools"]["registered"] == len({
        f["pool"]["id"] for f in fixtures if f["launches_word0_nonzero"]
    })
    store.close()


ANVIL = shutil.which("anvil") or os.path.expanduser("~/.foundry/bin/anvil")
UPSTREAM = "http://127.0.0.1:8547"


def _upstream_alive():
    import requests

    try:
        return requests.post(
            UPSTREAM, json={"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}, timeout=2,
        ).json().get("result") == hex(4663)
    except Exception:
        return False


@pytest.fixture
def anvil_fork():
    if not os.path.exists(ANVIL):
        pytest.skip("anvil is not installed")
    if not _upstream_alive():
        pytest.skip("upstream node is unreachable")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    process = subprocess.Popen(
        [ANVIL, "--fork-url", UPSTREAM, "--chain-id", "4663", "--port", str(port), "--silent"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with socket.socket() as probe:
                if probe.connect_ex(("127.0.0.1", port)) == 0:
                    break
            if process.poll() is not None:
                pytest.skip("anvil exited before serving")
            time.sleep(0.2)
        else:
            pytest.skip("anvil did not open its port")
        yield f"http://127.0.0.1:{port}"
    finally:
        process.kill()
        process.wait()


class HttpRpc:
    def __init__(self, url):
        self.url = url

    def batch(self, calls):
        import requests

        payload = [
            {"jsonrpc": "2.0", "id": index, "method": method, "params": list(params)}
            for index, (method, params) in enumerate(calls)
        ]
        body = requests.post(self.url, json=payload, timeout=120).json()
        body.sort(key=lambda item: item["id"])
        return [item.get("result") for item in body]


def test_fork_pons_hook_and_envelopes_match_recorded_fixtures(anvil_fork, tmp_path):
    rpc = HttpRpc(anvil_fork)
    fixtures = [fixture(name) for name in EXPECTED]
    store = tags.TagStore(str(tmp_path / "tags.sqlite"))
    identities = tags.PonsRegistry(rpc, store).identify(
        [f["pool"] for f in fixtures] + [{"id": "0x" + "42" * 32, "protocol": "v4", "hook": tags.PONS_HOOK}],
    )
    for data in fixtures:
        assert identities[data["pool"]["id"]].pons_registered == data["launches_word0_nonzero"], data["name"]
    assert not identities["0x" + "42" * 32].pons_registered
    envelopes = tags.fetch_envelopes(
        rpc, [(f["tx"]["hash"], int(f["tx"]["blockNumber"], 16), f["block_time"]) for f in fixtures],
    )
    for data in fixtures:
        assert envelopes[data["tx"]["hash"].lower()] == envelope_of(data), data["name"]
    store.close()
