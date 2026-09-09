"""Consumer-visible safeguards for quotes, canonical replay and backfill order."""
import json
import threading
import time

import pytest

from rhpools.lp_market_protocols import (
    TRANSFER_TOPIC, V3_POOL_CREATED_TOPIC, V3_SWAP_TOPIC,
    V4_INITIALIZE_TOPIC, V4_MODIFY_LIQUIDITY_TOPIC,
    UNISWAP_V3_POSITION_MANAGER,
)
from rhpools.lp_market_index import V3_BIRTH_REPAIR_PREFIXES
from rhpools.lp_market_accounting import AccountBook
from rhpools.lp_market_claims import PositionClaims
from rhpools.lp_market_service import LPMarketService, PriceProjection
from rhpools.lp_rpc import _WssRpc, build_rpc_factory
from rhpools.lp_market_store import CanonicalConflict, MarketStore
from rhpools.workbench_market import USDG, UNISWAP_V3_FACTORY

TOKEN = "0x" + "12" * 20
V3 = "0x" + "23" * 20
V4 = "0x" + "34" * 32
MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"


def header(number, timestamp, *, branch=0):
    return {"number": hex(number), "hash": "0x" + f"{number + branch:064x}",
            "parentHash": "0x" + f"{number - 1:064x}", "timestamp": hex(timestamp)}


def swap(block, pool_id, protocol, *, quote=101_000_000, index=0):
    sign = -1 if protocol == "v4" else 1
    return {
        "block_number": int(block["number"], 16), "block_hash": block["hash"],
        "tx_hash": "0x" + f"{int(block['hash'], 16) * 100 + index:064x}",
        "tx_index": 0, "log_index": index, "timestamp": int(block["timestamp"], 16),
        "pool_id": pool_id, "protocol": protocol, "kind": "swap", "owner": None,
        "custody": None, "position_key": None, "token_id": None,
        "tick_lower": None, "tick_upper": None, "liquidity_delta": None,
        "liquidity": "1000000000000", "sqrt_price_x96": str(1 << 96), "tick": 0,
        "fee_ppm": 10_000 if protocol == "v4" else 3_000,
        "amount0": str(-100_000_000 * sign), "amount1": str(quote * sign),
        "fee_amount0": None, "fee_amount1": None, "cashflow0": None, "cashflow1": None,
        "accounting_basis": "swap event", "identity_basis": "not a position", "data": {},
    }


def service(path):
    return LPMarketService(None, "http://127.0.0.1:1", path, start=False)


def pools():
    base = {"token0": TOKEN, "token1": USDG, "symbol0": "ASSET", "symbol1": "USDG",
            "decimals0": 6, "decimals1": 6, "tick_spacing": 1, "hook": None,
            "factory": UNISWAP_V3_FACTORY, "created_block": 1, "source": "factory-live"}
    return [{**base, "id": V3, "address": V3, "protocol": "v3", "fee_ppm": 3_000},
            {**base, "id": V4, "address": MANAGER, "protocol": "v4", "fee_ppm": 0x800000}]


def test_status_gap_uses_current_head_and_durable_cursor(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        indexed = header(100, 1_000)
        app.store.ingest([indexed], [], cursor={
            "from_block": 100, "to_block": 100, "block_number": 100,
            "block_hash": indexed["hash"], "timestamp": 1_000,
        })
        app.indexer._set_runtime(
            "live",
            state="live", head=101, head_timestamp=1_001, lag_s=0,
        )
        current = header(110, 1_010)
        app.indexer._publish_current_block(current, (), source="test")
        status = app.status()
        assert (status["head"], status["indexed_head"]) == (110, 100)
        assert (status["lag_blocks"], status["lag_s"], status["state"]) == (
            10, 10, "catching_up",
        )
        app.store.ingest([header(n, 900 + n) for n in range(101, 111)], [], cursor={
            "from_block": 101, "to_block": 110, "block_number": 110,
            "block_hash": current["hash"], "timestamp": 1_010,
        })
        caught_up = app.status()
        assert (caught_up["lag_blocks"], caught_up["lag_s"], caught_up["state"]) == (
            0, 0, "live",
        )
    finally:
        app.close()


def test_overview_refreshes_live_head_without_advancing_financial_coverage(
        tmp_path, monkeypatch):
    clock = [time.monotonic()]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        indexed = header(100, 1_000)
        app.store.ingest([indexed], [swap(indexed, V3, "v3")], cursor={
            "from_block": 100, "to_block": 100, "block_number": 100,
            "block_hash": indexed["hash"], "timestamp": 1_000,
        })
        app.indexer._publish_current_block(header(110, 1_010), (), source="test")
        first = app.overview({"window": "all"})
        assert first["status"]["head"] == 110
        clock[0] += 1
        app.indexer._publish_current_block(header(120, 1_020), (), source="test")
        refreshed = app.overview({"window": "all"})
        assert (refreshed["status"]["head"], refreshed["status"]["lag_blocks"]) == (120, 20)
        assert refreshed["coverage"]["to"] == 1_000
        assert refreshed["volume_usd"] == pytest.approx(101)
    finally:
        app.close()


def test_local_log_provider_never_answers_above_its_own_head(monkeypatch):
    local_url = "http://127.0.0.1:8547"
    public_url = "https://current.example.invalid"
    monkeypatch.setenv("LP_RPC_LOG_URLS", f"{local_url},{public_url}")
    monkeypatch.setenv("LP_RPC_DISABLE_ALCHEMY", "1")
    monkeypatch.setenv("LP_RPC_DISABLE_LOCAL_FALLBACK", "1")
    monkeypatch.delenv("RHP_RPC_URL", raising=False)
    monkeypatch.delenv("RHP_RPC_URLS", raising=False)
    factory = build_rpc_factory("", RuntimeError)
    client = factory("live")
    calls = []

    def response(source, item):
        method = item["method"]
        calls.append((source.url, method, item.get("params")))
        if method == "eth_chainId":
            result = hex(4663)
        elif method == "eth_blockNumber":
            result = "0x64"
        elif method == "eth_getLogs":
            result = [{"provider": "local" if source.url == local_url else "public"}]
        else:
            raise AssertionError(method)
        return {"jsonrpc": "2.0", "id": item["id"], "result": result}

    def post(source, payload):
        if isinstance(payload, list):
            return [response(source, item) for item in payload]
        return response(source, payload)

    monkeypatch.setattr(client, "_post", post)
    above_head = [("eth_getLogs", [{"fromBlock": "0x65", "toBlock": "0x65"}])]
    covered = [("eth_getLogs", [{"fromBlock": "0x60", "toBlock": "0x64"}])]
    try:
        assert client.batch(above_head) == [[{"provider": "public"}]]
        assert not any(url == local_url and method == "eth_getLogs" for url, method, _ in calls)
        assert client.batch(covered) == [[{"provider": "local"}]]
        local_status = next(
            row for row in factory.status()["logs"]["sources"]
            if row["name"] == "configured-logs-1"
        )
        assert local_status["failures"] == 0
    finally:
        factory.close()


def test_shared_wss_rejects_wrong_chain(monkeypatch):
    class Websocket:
        def __init__(self, *, chain=4663):
            self.chain = chain
            self.responses = []
            self.closed = False

        def send(self, raw):
            payload = json.loads(raw)
            assert payload["method"] == "eth_chainId"
            self.responses.append(json.dumps({
                "jsonrpc": "2.0", "id": payload["id"], "result": hex(self.chain),
            }))

        def recv(self, timeout):
            assert timeout > 0
            return self.responses.pop(0)

        def close(self):
            self.closed = True

    wrong_chain = Websocket(chain=1)
    monkeypatch.setattr(
        "websockets.sync.client.connect", lambda *_args, **_kwargs: wrong_chain,
    )
    client = _WssRpc(("wss://current.example.invalid",), RuntimeError, size=1)
    with pytest.raises(RuntimeError, match="WSS RPC exhausted"):
        client.call("eth_blockNumber", [])
    assert wrong_chain.closed is True
    client.close()



def test_activity_wss_partitions_provider_muted_topic_filter(tmp_path, monkeypatch):
    from rhpools import workbench_market as market

    class OfflineRpc:
        def call(self, *_args):
            raise market.RpcError("test state provider offline")

        def close(self):
            pass

    universe = market._Universe((), {}, {"v2": 0, "v3": 0, "v4": 0}, (), (), {})
    monkeypatch.setattr(market, "_load_universe", lambda: universe)
    catalog = market.MarketService(
        "http://127.0.0.1:1", data_dir=tmp_path,
        external_index=True, rpc=OfflineRpc(),
    )
    app = LPMarketService(catalog, "http://127.0.0.1:1", tmp_path / "market.sqlite", start=False)
    indexer = app.indexer
    block = header(100, int(time.time()))
    transaction = "0xb6e642be76be3969f99470bb1a5463c5382709a4fd33be5a3cb93e2d54a953bd"
    pool_id = "0x3a6a37c187621ef335d8bc2cff927a134713dce8bd2205115debc62c473851e6"
    token0 = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"  # gitleaks:allow -- public ERC-20 address
    token1 = "0x9c56f33c09555c738952e092fdbc99f492dd01ee"  # gitleaks:allow -- public ERC-20 address
    manager = "0x58daec3116aae6d93017baaea7749052e8a04fa7"

    def topic_address(address):
        return "0x" + address[2:].rjust(64, "0")

    activity_logs = [{
        "address": MANAGER, "blockNumber": "0x64", "blockHash": block["hash"],
        "transactionHash": transaction, "transactionIndex": "0x0", "logIndex": "0x1e",
        "topics": [
            V4_INITIALIZE_TOPIC, pool_id, topic_address(token0), topic_address(token1),
        ],
        "data": (
            "0x0000000000000000000000000000000000000000000000000000000000011170"
            "00000000000000000000000000000000000000000000000000000000000002bc"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000096c93fb50499b59557d86f6780dd08d"
            "000000000000000000000000000000000000000000000000000000000005c2fb"
        ),
    }, {
        "address": MANAGER, "blockNumber": "0x64", "blockHash": block["hash"],
        "transactionHash": transaction, "transactionIndex": "0x0", "logIndex": "0x21",
        "topics": [V4_MODIFY_LIQUIDITY_TOPIC, pool_id, topic_address(manager)],
        "data": (
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffff2778c"
            "00000000000000000000000000000000000000000000000000000000000d8874"
            "000000000000000000000000000000000000000000000000001aa1c2f817f7f0"
            "00000000000000000000000000000000000000000000000000000000001de806"
        ),
    }]

    class StopActivity(Exception):
        pass

    class Websocket:
        """Provider that acknowledges, but silently mutes, large topic OR filters."""

        def __init__(self):
            self.responses = []
            self.notifications = []
            self.timed_out = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def send(self, raw):
            payload = json.loads(raw)
            request_id = payload["id"]
            subscription = f"subscription-{request_id}"
            options = payload["params"][1]
            alternatives = list((options.get("topics") or [[]])[0])
            if len(alternatives) <= 7:
                for log in activity_logs:
                    if log["topics"][0] in alternatives:
                        self.notifications.append((subscription, log))
            self.responses.append(json.dumps({
                "jsonrpc": "2.0", "id": request_id, "result": subscription,
            }))

        def recv(self, timeout):
            assert timeout > 0
            if self.responses:
                return self.responses.pop(0)
            if self.notifications:
                subscription, log = self.notifications.pop(0)
                return json.dumps({
                    "jsonrpc": "2.0", "method": "eth_subscription",
                    "params": {"subscription": subscription, "result": log},
                })
            if not self.timed_out:
                self.timed_out = True
                raise TimeoutError
            raise StopActivity

    websocket = Websocket()
    monkeypatch.setattr(
        "websockets.sync.client.connect", lambda *_args, **_kwargs: websocket,
    )
    monkeypatch.setattr(
        indexer, "_schedule_current_receipts", lambda *_args, **_kwargs: None,
    )
    try:
        assert app.store.pool(pool_id) is None
        indexer._publish_current_block(block, (), source="test")
        cursor = indexer.feed_updates()
        with pytest.raises(StopActivity):
            indexer._activity_wss_once("wss://current.example.invalid")
        updates = app.stream_updates(
            {"kind": "lp", "channel": "activity"},
            cursor["sequence"], cursor["feed_epoch"],
        )
        rows = [
            row
            for event in updates["events"] if event["event"] == "activity"
            for row in event["data"]["rows"]
        ]
        assert [(row["pool_id"], row["kind"]) for row in rows] == [(pool_id, "add")]
        assert catalog.detail(pool_id)["pool"]["id"] == pool_id
    finally:
        app.close()
        catalog.close()


def test_wal_reader_snapshot_does_not_wait_for_writer_transaction(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite")
    writer_entered = threading.Event()
    release_writer = threading.Event()
    reader_done = threading.Event()
    failures = []

    def write():
        try:
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO metadata(key,value) VALUES('reader-test','1') "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
                )
                writer_entered.set()
                release_writer.wait(2)
        except BaseException as exc:
            failures.append(exc)

    def read_status():
        try:
            store.status()
        except BaseException as exc:
            failures.append(exc)
        finally:
            reader_done.set()

    writer = threading.Thread(target=write)
    reader = threading.Thread(target=read_status)
    try:
        writer.start()
        assert writer_entered.wait(1)
        reader.start()
        assert reader_done.wait(0.5), "WAL reader was serialized behind the writer lock"
    finally:
        release_writer.set()
        writer.join(2)
        reader.join(2)
        store.close()
    assert failures == []


def test_protocol_input_signs_and_duplicate_delivery_survive_restart(tmp_path):
    path = tmp_path / "market.sqlite"
    app = service(path)
    try:
        app.store.upsert_pools(pools())
        now = int(time.time()) - 60
        blocks = [header(99, now), header(100, now + 1)]
        events = [swap(blocks[0], V3, "v3"), swap(blocks[1], V4, "v4")]
        app.store.ingest(blocks, events)
        app.store.ingest(blocks, events)
        app.store.save_v3_balance(V3, 99, blocks[0]["hash"], "100000000", "200000000")
        app.close()
        app = service(path)
        rows = {row["id"]: row for row in app.pools({"window": "1h"})["rows"]}
        assert rows[V3]["volume_usd"] == pytest.approx(101)
        assert rows[V4]["volume_usd"] == pytest.approx(101)
        assert rows[V3]["fees_usd"] == pytest.approx(0.303)
        assert rows[V4]["fees_usd"] == pytest.approx(1.01)
        assert rows[V3]["swaps"] == rows[V4]["swaps"] == 1
        assert rows[V3]["tvl_usd"] == pytest.approx(300)
        assert rows[V3]["coverage"]["inventory_complete"] is False
        assert rows[V4]["tvl_usd"] is None  # Singleton balances cannot price this pool.
        assert app.overview({"window": "1h"})["swaps"] == 2
        tape = {
            row["pool_id"]: row
            for row in app.tape({"window": "1h", "kind": "all"})["rows"]
        }
        assert tape[V3]["pool_fee_ppm"] == 3_000
        assert tape[V3]["pool_fee_basis"] == "verified_static_pool_fee"
        assert tape[V4]["pool_fee_ppm"] is None
        assert tape[V4]["pool_fee_basis"] == "dynamic_fee_flag_not_a_rate"
        assert tape[V3]["fees_scope"] == tape[V4]["fees_scope"] == "gross_swap"
    finally:
        app.close()


def test_orphan_fees_disappear_and_replacement_branch_is_counted_once(tmp_path):
    path = tmp_path / "market.sqlite"
    app = service(path)
    try:
        app.store.upsert_pools(pools())
        now = int(time.time()) - 60
        blocks = [header(99, now), header(100, now + 1)]
        orphan = swap(blocks[1], V4, "v4")
        app.store.ingest(blocks, [swap(blocks[0], V3, "v3"), orphan])
        app.store.rollback(99)
        after_rollback = app.overview({"window": "1h"})
        assert after_rollback["volume_usd"] == pytest.approx(101)
        assert after_rollback["fees_usd"] == pytest.approx(0.303)
        replacement = header(100, now + 2, branch=10_000)
        event = swap(replacement, V4, "v4", quote=202_000_000)
        app.store.ingest([replacement], [event])
        app.close()
        app = service(path)
        overview = app.overview({"window": "1h"})
        assert overview["volume_usd"] == pytest.approx(303)
        assert overview["fees_usd"] == pytest.approx(2.323)
        tape = app.tape({"window": "1h", "kind": "all"})["rows"]
        assert {row["tx_hash"] for row in tape} == {event["tx_hash"], swap(blocks[0], V3, "v3")["tx_hash"]}
        assert orphan["tx_hash"] not in {row["tx_hash"] for row in tape}
    finally:
        app.close()


def test_backfill_insertion_order_does_not_reorder_live_tape(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        now = int(time.time()) - 60
        # Equal timestamps exercise the timestamp-index floor tie: the
        # backfilled lower block receives the highest insertion id.
        recent = [header(99, now), header(100, now)]
        app.store.ingest(recent, [swap(recent[0], V3, "v3"), swap(recent[1], V4, "v4")])
        old = header(98, now)
        app.store.ingest([old], [swap(old, V3, "v3")])
        tape = app.tape({"window": "1h", "kind": "all", "limit": 2})
        assert [row["block_number"] for row in tape["rows"]] == [100, 99]
        next_page = app.tape({"window": "1h", "kind": "all", "before": tape["cursor"]})
        assert [row["block_number"] for row in next_page["rows"]] == [98]
    finally:
        app.close()

@pytest.mark.parametrize(
    ("recent_kind", "params", "expected_rows"),
    (
        ("swap", {}, 0),
        ("add", {}, 1),
        ("swap", {"kind": "all", "q": V3}, 1),
    ),
)
def test_underfilled_tape_stays_within_window(
        tmp_path, recent_kind, params, expected_rows):
    path = tmp_path / f"market-{recent_kind}.sqlite"
    seed = MarketStore(path)
    now = int(time.time())
    old = header(1, now - 7_200)
    recent = header(1_000, now - 10)
    recent_event = {**swap(recent, V3, "v3"), "kind": recent_kind}
    try:
        seed.upsert_pools(pools())
        seed.ingest(
            [old, recent],
            [
                *(swap(old, V3, "v3", index=index) for index in range(2_000)),
                recent_event,
            ],
        )
    finally:
        seed.close()

    app = service(path)
    connection = app.store.read()
    connection.set_progress_handler(lambda: 1, 2_000)
    try:
        result = app.tape(
            {"window": "1h", **params},
            _status={
                "history_from": now - 7_200,
                "history_to": now,
                "revision": 1,
                "epoch": 0,
            },
        )
        assert len(result["rows"]) == expected_rows
        assert [row["block_number"] for row in result["rows"]] == (
            [1_000] if expected_rows else []
        )
        assert (result["cursor"] is not None) is bool(expected_rows)
    finally:
        connection.set_progress_handler(None, 0)
        app.close()

def test_backfilled_price_repairs_existing_flows_after_restart(tmp_path):
    path = tmp_path / "market.sqlite"
    app = service(path)
    try:
        app.store.upsert_pools(pools())
        now = int(time.time()) - 60
        old, recent = header(99, now), header(100, now + 1)
        deposit = {**swap(recent, V3, "v3"), "kind": "add", "sqrt_price_x96": None,
                   "cashflow0": "-1000000", "cashflow1": "0"}
        app.store.ingest([recent], [deposit])
        assert app.tape({"kind": "all"})["rows"][0]["deposit_usd"] is None
        app.store.ingest([old], [swap(old, V3, "v3")], lane="backfill")
        app.close()
        app = service(path)
        pending = app.store.pending_reprojections()
        app.store.reproject([event["id"] for event in pending])
        row = app.tape({"kind": "all"})["rows"][0]
        assert row["deposit_usd"] == pytest.approx(1)
        assert row["withdrawal_usd"] == 0
    finally:
        app.close()


def test_older_quote_anchor_reprices_cross_pool_successor(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        cross = {**pools()[0], "id": "0x" + "45" * 20, "address": "0x" + "45" * 20,
                 "token1": "0x" + "56" * 20, "symbol1": "OTHER"}
        app.store.upsert_pools([*pools(), cross])
        now = int(time.time()) - 60
        old, recent = header(99, now), header(100, now + 1)
        app.store.ingest([recent], [swap(recent, cross["id"], "v3")])
        assert app.tape({"kind": "all"})["rows"][0]["volume_usd"] is None
        anchor = {**swap(old, V3, "v3"), "sqrt_price_x96": str(2 << 96)}
        app.store.ingest([old], [anchor], lane="backfill")
        app.store.reproject([event["id"] for event in app.store.pending_reprojections()])
        row = app.tape({"kind": "all"})["rows"][0]
        assert row["volume_usd"] == pytest.approx(404)
        assert row["fees_usd"] == pytest.approx(1.212)
    finally:
        app.close()


def test_v2_reserve_checkpoint_prices_token_input_and_inventory(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        pool = {**pools()[0], "protocol": "v2"}
        app.store.upsert_pools([pool])
        block = header(100, int(time.time()) - 60)
        checkpoint = {**swap(block, V3, "v2"), "kind": "checkpoint", "sqrt_price_x96": None,
                      "tick": None, "liquidity": None,
                      "data": {"reserve0": "100000000", "reserve1": "200000000"}}
        trade = {**swap(block, V3, "v2", index=1), "sqrt_price_x96": None,
                 "tick": None, "liquidity": None, "amount0": "10000000", "amount1": "-18000000",
                 "data": {"input0": "10000000", "input1": "0"}}
        app.store.ingest([block], [checkpoint, trade])
        row = app.pools({"window": "1h"})["rows"][0]
        assert row["price"] == pytest.approx(2)
        assert row["volume_usd"] == pytest.approx(18)
        assert row["fees_usd"] == pytest.approx(0.06)
        assert row["tvl_usd"] == pytest.approx(400)
        assert app.tape({})["rows"] == []  # Reserve Sync is not an LP checkpoint.
    finally:
        app.close()


def lp_effect(block, protocol, kind, liquidity, amounts, before, after, *, key="position"):
    event = swap(block, V4 if protocol == "v4" else V3, protocol)
    event.update(kind=kind, position_key=f"{protocol}:{key}", owner=TOKEN, custody=TOKEN,
                 identity_basis="verified_owner", liquidity="1000000000000", liquidity_delta=str(liquidity),
                 tick_lower=-10, tick_upper=10, amount0=str(amounts[0]), amount1=str(amounts[1]),
                 data={"position_before": before, "position_after": after})
    cash = (-amounts[0], -amounts[1]) if kind == "add" else amounts if kind == "collect" else (0, 0)
    event.update(cashflow0=str(cash[0]), cashflow1=str(cash[1]))
    return event


def position_state(liquidity, owed0=0, owed1=0):
    return {"liquidity": str(liquidity), "tokens_owed0": str(owed0), "tokens_owed1": str(owed1),
            "claims_empty": owed0 == owed1 == 0}


def ingest_effects(app, blocks, events):
    transactions = [{"tx_hash": event["tx_hash"], "block_number": event["block_number"],
                     "block_hash": event["block_hash"], "payer": TOKEN, "gas_usd": 0.01}
                    for event in events]
    app.store.ingest(blocks, events, transactions=transactions)
    return transactions


def accounting_store(path, *, deferred):
    store = MarketStore(path)
    PriceProjection(store)
    book = AccountBook(store, deferred=deferred).install()
    store.upsert_pools(pools())
    return store, book


def traced_v4_episode(blocks, *, owner=TOKEN, key="deferred"):
    opened = lp_effect(
        blocks[0], "v4", "add", 1_000, (1_000_000, 1_000_000),
        position_state(0), position_state(1_000), key=key,
    )
    closed = lp_effect(
        blocks[1], "v4", "remove", -1_000, (1_000_000, 1_000_000),
        position_state(1_000), position_state(0), key=key,
    )
    for event, principal, cash, fees in (
        (opened, (-1_000_000, -1_000_000), (-1_000_000, -1_000_000), (0, 0)),
        (closed, (1_000_000, 1_000_000), (1_100_000, 1_000_000), (100_000, 0)),
    ):
        event.update(
            owner=owner, custody=MANAGER,
            cashflow0=str(cash[0]), cashflow1=str(cash[1]),
            fee_amount0=str(fees[0]), fee_amount1=str(fees[1]),
        )
        event["data"].update(
            trace_complete=True, fees_accrued_exact=True,
            principal_delta_exact=True,
            principal_delta={
                "amount0": str(principal[0]), "amount1": str(principal[1]),
            },
        )
    transactions = [
        {
            "tx_hash": event["tx_hash"],
            "block_number": event["block_number"],
            "block_hash": event["block_hash"],
            "payer": owner,
            "gas_usd": 0.01,
        }
        for event in (opened, closed)
    ]
    return opened, closed, transactions


def drain_accounting(book):
    while book.project_pending(limit=32):
        pass


def owner_open(block, owner, key):
    event = lp_effect(
        block, "v4", "add", 1_000, (1_000_000, 1_000_000),
        position_state(0), position_state(1_000), key=key,
    )
    event.update(owner=owner, custody=MANAGER)
    return event


def test_backfill_preserves_published_episode_ids_and_returns(tmp_path):
    path = tmp_path / "stable-episodes.sqlite"
    store, book = accounting_store(path, deferred=True)
    try:
        recent_blocks = [header(200 + index, 2_000 + index) for index in range(2)]
        recent_open, recent_close, recent_gas = traced_v4_episode(recent_blocks)
        store.ingest(
            recent_blocks, [recent_open, recent_close],
            transactions=[{**row, "gas_usd": None} for row in recent_gas],
        )
        drain_accounting(book)
        recent = book.closed({"window": "all"})["rows"][0]
        assert recent["net_pnl_usd"] is None
        store.enrich([recent_open, recent_close], transactions=recent_gas)
        drain_accounting(book)
        recent = book.closed({"window": "all"})["rows"][0]
        assert recent["net_pnl_usd"] == pytest.approx(0.08)

        older_blocks = [header(100 + index, 1_000 + index) for index in range(2)]
        older_open, older_close, older_gas = traced_v4_episode(older_blocks)
        store.ingest(
            older_blocks, [older_open, older_close], transactions=older_gas, lane="backfill",
        )
        drain_accounting(book)
        published = {
            int(row["opened_at"]): row
            for row in book.closed({"window": "all"})["rows"]
        }
        assert published[2000]["id"] == recent["id"]
        assert published[2000]["net_pnl_usd"] == pytest.approx(
            recent["net_pnl_usd"],
        )
        assert published[1000]["id"] != recent["id"]
        assert published[1000]["net_pnl_usd"] == pytest.approx(0.08)
        ids = {block: row["id"] for block, row in published.items()}

        orphan_block = header(202, 2_002)
        orphan = lp_effect(
            orphan_block, "v4", "checkpoint", 0, (0, 0),
            position_state(0), position_state(0), key="deferred",
        )
        store.ingest([orphan_block], [orphan], transactions=[{
            "tx_hash": orphan["tx_hash"],
            "block_number": orphan["block_number"],
            "block_hash": orphan["block_hash"],
            "payer": TOKEN,
            "gas_usd": 0.01,
        }])
        store.rollback(201)
        store.close()

        store, book = accounting_store(path, deferred=True)
        drain_accounting(book)
        replayed = {
            int(row["opened_at"]): row
            for row in book.closed({"window": "all"})["rows"]
        }
        assert {block: row["id"] for block, row in replayed.items()} == ids
        assert all(
            row["net_pnl_usd"] == pytest.approx(0.08)
            for row in replayed.values()
        )
        owner_episodes = {
            int(row["opened_at"]): row
            for position in book.owner(TOKEN, {"window": "all"})["positions"]
            for row in position["episodes"]
        }
        assert {
            block: row["id"] for block, row in owner_episodes.items()
        } == ids
        assert all(
            row["net_pnl_usd"] == pytest.approx(0.08)
            for row in owner_episodes.values()
        )
    finally:
        store.close()


def test_source_correction_removes_stale_episode_gas(tmp_path):
    blocks = [header(100 + index, 1_000 + index) for index in range(3)]
    opened, closed, transactions = traced_v4_episode(
        [blocks[0], blocks[2]], key="gas-correction",
    )
    intermediate = lp_effect(
        blocks[1], "v4", "checkpoint", 0, (0, 0),
        position_state(1_000), position_state(1_000), key="gas-correction",
    )
    intermediate.update(
        owner=TOKEN, custody=MANAGER,
        cashflow0="0", cashflow1="0", fee_amount0="0", fee_amount1="0",
    )
    intermediate["data"].update(
        trace_complete=True, fees_accrued_exact=True,
        principal_delta_exact=True,
        principal_delta={"amount0": "0", "amount1": "0"},
    )
    transactions.insert(1, {
        "tx_hash": intermediate["tx_hash"],
        "block_number": intermediate["block_number"],
        "block_hash": intermediate["block_hash"],
        "payer": TOKEN,
        "gas_usd": 0.01,
    })
    store, book = accounting_store(
        tmp_path / "gas-correction.sqlite", deferred=True,
    )
    try:
        store.ingest(
            blocks, [opened, intermediate, closed], transactions=transactions,
        )
        drain_accounting(book)
        before = book.closed({"window": "all"})["rows"][0]
        assert before["deposit_usd"] == pytest.approx(2)
        assert before["withdrawal_usd"] == pytest.approx(2)
        assert before["fees_usd"] == pytest.approx(0.1)
        assert before["gross_pnl_usd"] == pytest.approx(0.1)
        assert before["gas_usd"] == pytest.approx(0.03)
        assert before["net_pnl_usd"] == pytest.approx(0.07)

        store.enrich([{
            **intermediate,
            "position_key": None,
            "owner": None,
            "custody": None,
            "identity_basis": "unresolved_position",
        }])
        drain_accounting(book)

        after = book.closed({"window": "all"})["rows"][0]
        assert after["id"] == before["id"]
        assert after["deposit_usd"] == pytest.approx(before["deposit_usd"])
        assert after["withdrawal_usd"] == pytest.approx(before["withdrawal_usd"])
        assert after["fees_usd"] == pytest.approx(before["fees_usd"])
        assert after["gross_pnl_usd"] == pytest.approx(before["gross_pnl_usd"])
        assert after["gas_usd"] == pytest.approx(0.02)
        assert after["net_pnl_usd"] == pytest.approx(0.08)

        detail = book.owner(TOKEN, {"window": "all"})
        owner_episode = next(
            row
            for position in detail["positions"]
            if position["position_key"] == "v4:gas-correction"
            for row in position["episodes"]
            if row["id"] == before["id"]
        )
        assert owner_episode["gas_usd"] == pytest.approx(0.02)
        assert owner_episode["net_pnl_usd"] == pytest.approx(0.08)
        assert detail["summary"]["gas_usd"] == pytest.approx(0.02)
        assert detail["summary"]["net_pnl_usd"] == pytest.approx(0.08)
    finally:
        store.close()

def test_visible_wallet_recovers_missed_receipt_cost_without_new_activity(tmp_path):
    blocks = [header(100 + index, 1_000 + index) for index in range(2)]
    opened, closed, transactions = traced_v4_episode(blocks, key="late-cost")
    store, book = accounting_store(tmp_path / "late-cost.sqlite", deferred=True)
    try:
        store.ingest(blocks, [opened, closed])
        drain_accounting(book)
        # This reproduces persisted receipt evidence whose old accounting
        # publication missed its cost dependency.
        store.enrich([], transactions=transactions)
        assert book.owner(TOKEN, {"window": "all"})["summary"]["net_pnl_usd"] is None
        book.recover_receipt_costs(
            [row["tx_hash"] for row in transactions], epoch=-1,
        )
        assert book.owner(TOKEN, {"window": "all"})["summary"]["net_pnl_usd"] is None

        worker = PositionClaims(store, book, None, lambda: None)
        worker.request([TOKEN])
        worker._refresh(*worker._next())
        summary = book.owner(TOKEN, {"window": "all"})["summary"]
        assert summary["gas_usd"] == pytest.approx(0.02)
        assert summary["net_pnl_usd"] == pytest.approx(0.08)
        assert book.closed({"window": "all"})["rows"][0]["net_pnl_usd"] == pytest.approx(0.08)
    finally:
        store.close()



def test_deferred_accounting_does_not_hold_writer_and_drains_same_branch_prefix(
        tmp_path, monkeypatch):
    blocks = [header(100 + index, 1_000 + index) for index in range(2)]
    opened, closed, transactions = traced_v4_episode(blocks)
    store, book = accounting_store(
        tmp_path / "deferred.sqlite", deferred=True,
    )
    baseline, baseline_book = accounting_store(
        tmp_path / "synchronous.sqlite", deferred=False,
    )
    derived = threading.Event()
    release = threading.Event()
    committed = threading.Event()
    failures = []
    original = book._derive

    def paused(*args, **kwargs):
        result = original(*args, **kwargs)
        derived.set()
        release.wait(5)
        return result

    monkeypatch.setattr(book, "_derive", paused)
    try:
        inserted = store.ingest(
            blocks[:1], [opened], transactions=transactions[:1],
        )
        opened_id = int(inserted[0]["id"])
        projector = threading.Thread(
            target=lambda: book.project_pending(limit=1),
        )
        projector.start()
        assert derived.wait(2)

        def commit_arrival():
            try:
                store.ingest(
                    blocks[1:], [closed], transactions=transactions[1:],
                )
                store.enrich([{
                    "id": opened_id,
                    "accounting_basis": "same-epoch trace enrichment",
                }])
            except BaseException as error:
                failures.append(error)
            finally:
                committed.set()

        writer = threading.Thread(target=commit_arrival)
        writer.start()
        assert committed.wait(2)
        assert failures == []
        assert store.status()["indexed_events"] == 2
        assert store.status()["pending_accounting"] == 1
        release.set()
        writer.join(2)
        projector.join(2)
        assert not writer.is_alive()
        assert not projector.is_alive()

        prefix = book.positions({"owner": TOKEN})["rows"]
        assert len(prefix) == 1
        assert prefix[0]["status"] == "open"
        assert store.status()["pending_accounting"] == 1
        prefix_candidates = book.owner_candidates({"window": "all"})
        assert prefix_candidates["accounting_as_of"] is None
        prefix_owner = book.decorate_owner_activity(
            prefix_candidates["rows"], {"window": "all"},
        )[0]
        assert prefix_owner["financial_pending"] is True
        assert prefix_owner["financial_through_order"] is None

        monkeypatch.setattr(book, "_derive", original)
        drain_accounting(book)
        baseline.ingest(blocks, [opened, closed], transactions=transactions)
        deferred_row = book.closed({"window": "all"})["rows"][0]
        synchronous_row = baseline_book.closed({"window": "all"})["rows"][0]
        for field in (
            "status", "deposit_usd", "withdrawal_usd", "fees_usd",
            "gross_pnl_usd", "gas_usd", "net_pnl_usd",
        ):
            assert deferred_row[field] == synchronous_row[field]
        assert deferred_row["net_pnl_usd"] == pytest.approx(0.08)
        owner = book.decorate_owner_activity(
            book.owner_candidates({"window": "all"})["rows"],
            {"window": "all"},
        )[0]
        assert owner["financial_pending"] is False
        assert owner["financial_through_order"] == {
            "block_number": 101, "tx_index": 0, "log_index": 0,
        }
        assert owner["financial_through_as_of"] == 1_001
    finally:
        release.set()
        baseline.close()
        store.close()


def test_owner_financial_boundary_uses_the_aggregate_snapshot(
        tmp_path, monkeypatch):
    blocks = [header(100 + index, 1_000 + index) for index in range(2)]
    opened, closed, transactions = traced_v4_episode(
        blocks, key="owner-snapshot",
    )
    store, book = accounting_store(tmp_path / "market.sqlite", deferred=True)
    aggregate_read = threading.Event()
    release = threading.Event()
    failures = []
    captured = []
    original_gas = book._owner_gas_values

    def paused_gas(*args, **kwargs):
        result = original_gas(*args, **kwargs)
        aggregate_read.set()
        if not release.wait(5):
            raise TimeoutError("test did not release owner aggregate snapshot")
        return result

    monkeypatch.setattr(book, "_owner_gas_values", paused_gas)
    try:
        store.ingest(
            blocks[:1], [opened], transactions=transactions[:1],
        )
        drain_accounting(book)

        def read_candidates():
            try:
                captured.append(book.owner_candidates({"window": "all"}))
            except BaseException as error:
                failures.append(error)

        reader = threading.Thread(target=read_candidates)
        reader.start()
        assert aggregate_read.wait(2)
        store.ingest(
            blocks[1:], [closed], transactions=transactions[1:],
        )
        drain_accounting(book)
        release.set()
        reader.join(2)
        assert not reader.is_alive()
        assert failures == []

        stale = captured[0]["rows"][0]
        assert stale["closed_episodes"] == 0
        assert stale["financial_pending"] is False
        assert stale["financial_through_order"] == {
            "block_number": 100, "tx_index": 0, "log_index": 0,
        }
        decorated = book.decorate_owner_activity(
            captured[0]["rows"], {"window": "all"},
        )[0]
        assert decorated["closed_episodes"] == 0
        assert decorated["financial_pending"] is True
        assert decorated["financial_through_order"] is None
        latest = book.owner_candidates({"window": "all"})["rows"][0]
        assert latest["closed_episodes"] == 1
        assert latest["financial_through_order"] == {
            "block_number": 101, "tx_index": 0, "log_index": 0,
        }
    finally:
        release.set()
        store.close()


def test_deferred_accounting_rejects_prepared_orphan_branch(
        tmp_path, monkeypatch):
    first_block = header(100, 1_000)
    replacement_block = header(100, 1_001, branch=10_000)
    old_event = lp_effect(
        first_block, "v4", "add", 1_000, (1_000_000, 1_000_000),
        position_state(0), position_state(1_000), key="reorg",
    )
    replacement_owner = "0x" + "78" * 20
    replacement = lp_effect(
        replacement_block, "v4", "add", 1_000, (1_000_000, 1_000_000),
        position_state(0), position_state(1_000), key="reorg",
    )
    replacement.update(owner=replacement_owner, custody=MANAGER)
    store, book = accounting_store(tmp_path / "market.sqlite", deferred=True)
    derived = threading.Event()
    release = threading.Event()
    original = book._derive

    def paused(*args, **kwargs):
        result = original(*args, **kwargs)
        derived.set()
        release.wait(5)
        return result

    monkeypatch.setattr(book, "_derive", paused)
    try:
        store.ingest([first_block], [old_event])
        projector = threading.Thread(
            target=lambda: book.project_pending(limit=1),
        )
        projector.start()
        assert derived.wait(2)
        store.rollback(99)
        store.ingest([replacement_block], [replacement])
        release.set()
        projector.join(2)
        assert not projector.is_alive()
        assert book.positions({"owner": TOKEN})["rows"] == []

        monkeypatch.setattr(book, "_derive", original)
        drain_accounting(book)
        rows = book.positions({"owner": replacement_owner})["rows"]
        assert len(rows) == 1
        assert rows[0]["position_key"] == "v4:reorg"
        assert book.positions({"owner": TOKEN})["rows"] == []
    finally:
        release.set()
        store.close()


def test_deferred_accounting_restart_resumes_queue_and_clears_reassigned_key(
        tmp_path, monkeypatch):
    path = tmp_path / "market.sqlite"
    block = header(100, 1_000)
    event = lp_effect(
        block, "v4", "add", 1_000, (1_000_000, 1_000_000),
        position_state(0), position_state(1_000), key="before",
    )
    store, _book = accounting_store(path, deferred=True)
    inserted = store.ingest([block], [event])
    event_id = int(inserted[0]["id"])
    assert store.status()["pending_accounting"] == 1
    store.close()

    store = MarketStore(path)
    PriceProjection(store)
    book = AccountBook(store, deferred=True)

    def unexpected_replay(*_args, **_kwargs):
        raise AssertionError("deferred install replayed the ledger")

    monkeypatch.setattr(book, "_map_existing_events", unexpected_replay)
    monkeypatch.setattr(book, "_rebuild_position", unexpected_replay)
    try:
        book.install()
        assert store.status()["pending_accounting"] == 1
        drain_accounting(book)
        before = book.positions({"owner": TOKEN})["rows"]
        assert [row["position_key"] for row in before] == ["v4:before"]

        moved = {
            **event, "id": event_id, "position_key": "v4:after",
            "data": {**event["data"], "position_key": "v4:after"},
        }
        store.enrich([moved])
        assert store.status()["pending_accounting"] == 2
        drain_accounting(book)
        after = book.positions({"owner": TOKEN})["rows"]
        assert [row["position_key"] for row in after] == ["v4:after"]
        assert after[0]["liquidity"] == "1000"
    finally:
        store.close()


def test_deferred_nft_transfer_hints_mark_both_wallets_pending(
        tmp_path):
    sender = "0x" + "56" * 20
    recipient = "0x" + "78" * 20
    bystander = "0x" + "9a" * 20
    blocks = [header(100 + index, 1_000 + index) for index in range(4)]
    seeds = [
        owner_open(blocks[0], sender, "sender-seed"),
        owner_open(blocks[1], recipient, "recipient-seed"),
        owner_open(blocks[2], bystander, "bystander-seed"),
    ]
    store, book = accounting_store(tmp_path / "market.sqlite", deferred=True)
    position_key = f"nft:{MANAGER}:42"
    try:
        store.ingest(blocks[:3], seeds)
        drain_accounting(book)
        transfer = swap(blocks[3], V4, "v4")
        transfer.update(
            protocol="nft", kind="transfer", position_key=position_key,
            token_id="42", owner=recipient, custody=MANAGER,
            liquidity=None, liquidity_delta=None, amount0=None, amount1=None,
            cashflow0=None, cashflow1=None,
            data={
                "from": sender, "to": recipient,
                "manager_protocol": "v4",
            },
        )
        store.ingest(blocks[3:], [transfer])

        rows = {
            row["owner"]: row
            for row in book.owner_candidates({
                "window": "all", "protocol": "v4", "identity_scope": "wallets",
            })["rows"]
        }
        assert all(rows[owner]["positions"] == 1 for owner in rows)
        assert rows[sender]["financial_pending"] is True
        assert rows[recipient]["financial_pending"] is True
        assert rows[bystander]["financial_pending"] is False
    finally:
        store.close()


def test_historical_owner_reassignment_survives_identity_cursor(tmp_path):
    prior_owner = "0x" + "56" * 20
    reassigned_owner = "0x" + "78" * 20
    blocks = [header(100 + index, 1_000 + index) for index in range(2)]
    original = owner_open(blocks[0], prior_owner, "reassigned")
    other = owner_open(blocks[1], reassigned_owner, "existing")
    store, book = accounting_store(tmp_path / "market.sqlite", deferred=True)
    position_key = str(original["position_key"])
    try:
        inserted = store.ingest(blocks, [original, other])
        event_id = int(inserted[0]["id"])
        drain_accounting(book)
        with store.transaction() as conn:
            book._queue_position_keys(
                conn, {position_key: (100, 0, 0)},
            )
            conn.execute(
                "UPDATE lp_accounting_pending SET identity_cursor=? "
                "WHERE position_key=?",
                (event_id, position_key),
            )
            conn.execute(
                "DELETE FROM lp_accounting_pending_identities "
                "WHERE position_key=?",
                (position_key,),
            )

        store.enrich([{
            **original, "id": event_id, "owner": reassigned_owner,
        }])
        pending = store.read().execute(
            "SELECT identities_ready,identity_cursor "
            "FROM lp_accounting_pending WHERE position_key=?",
            (position_key,),
        ).fetchone()
        hinted_owners = {
            str(row[0])
            for row in store.read().execute(
                "SELECT identity "
                "FROM lp_accounting_pending_identities "
                "WHERE position_key=? AND kind='owner'",
                (position_key,),
            ).fetchall()
        }
        assert (
            int(pending["identities_ready"]),
            int(pending["identity_cursor"]),
        ) == (0, event_id)
        assert hinted_owners == {reassigned_owner}

        assert book.recover_pending_identities() is True
        ready = store.read().execute(
            "SELECT identities_ready FROM lp_accounting_pending "
            "WHERE position_key=?",
            (position_key,),
        ).fetchone()
        rows = {
            row["owner"]: row
            for row in book.owner_candidates({"window": "all"})["rows"]
        }
        assert int(ready["identities_ready"]) == 1
        assert rows[prior_owner]["financial_pending"] is True
        assert rows[reassigned_owner]["financial_pending"] is True
    finally:
        store.close()


def test_legacy_pending_identity_recovery_resumes_after_restart(
        tmp_path, monkeypatch):
    path = tmp_path / "market.sqlite"
    queued_owner = "0x" + "56" * 20
    unrelated_owner = "0x" + "78" * 20
    blocks = [header(100 + index, 1_000 + index) for index in range(2)]
    queued = owner_open(blocks[0], queued_owner, "legacy")
    unrelated = owner_open(blocks[1], unrelated_owner, "unrelated")
    store, book = accounting_store(path, deferred=True)
    position_key = str(queued["position_key"])
    store.ingest(blocks, [queued, unrelated])
    drain_accounting(book)
    with store.transaction() as conn:
        book._queue_position_keys(conn, {position_key: (100, 0, 0)})
        conn.execute(
            "DELETE FROM lp_accounting_pending_identities "
            "WHERE position_key=?",
            (position_key,),
        )
    store.close()

    store = MarketStore(path)
    PriceProjection(store)
    book = AccountBook(store, deferred=True)

    def unexpected_replay(*_args, **_kwargs):
        raise AssertionError("deferred restart replayed the ledger")

    monkeypatch.setattr(book, "_map_existing_events", unexpected_replay)
    monkeypatch.setattr(book, "_rebuild_position", unexpected_replay)
    try:
        book.install()
        before = {
            row["owner"]: row
            for row in book.owner_candidates({"window": "all"})["rows"]
        }
        assert before[queued_owner]["positions"] == 1
        assert before[unrelated_owner]["positions"] == 1
        assert before[queued_owner]["financial_pending"] is True
        assert before[unrelated_owner]["financial_pending"] is True

        assert book.recover_pending_identities() is True
        pending = store.read().execute(
            "SELECT identities_ready FROM lp_accounting_pending "
            "WHERE position_key=?",
            (position_key,),
        ).fetchone()
        after = {
            row["owner"]: row
            for row in book.owner_candidates({"window": "all"})["rows"]
        }
        assert int(pending["identities_ready"]) == 1
        assert after[queued_owner]["financial_pending"] is True
        assert after[unrelated_owner]["financial_pending"] is False
    finally:
        store.close()


def test_pending_identity_recovery_rejects_orphan_epoch(
        tmp_path, monkeypatch):
    old_owner = "0x" + "56" * 20
    replacement_owner = "0x" + "78" * 20
    old_block = header(100, 1_000)
    replacement_block = header(100, 1_001, branch=10_000)
    old_event = owner_open(old_block, old_owner, "identity-reorg")
    replacement = owner_open(
        replacement_block, replacement_owner, "identity-reorg",
    )
    store, book = accounting_store(tmp_path / "market.sqlite", deferred=True)
    position_key = str(old_event["position_key"])
    prepared = threading.Event()
    release = threading.Event()
    failures = []
    original_prepare = book._prepare_pending_identity_hints

    def paused_prepare():
        result = original_prepare()
        prepared.set()
        if not release.wait(5):
            raise TimeoutError("test did not release identity recovery")
        return result

    def recover():
        try:
            book.recover_pending_identities()
        except BaseException as error:
            failures.append(error)

    monkeypatch.setattr(
        book, "_prepare_pending_identity_hints", paused_prepare,
    )
    try:
        store.ingest([old_block], [old_event])
        with store.transaction() as conn:
            conn.execute(
                "UPDATE lp_accounting_pending SET identities_ready=0,"
                "identity_cursor=0 WHERE position_key=?",
                (position_key,),
            )
            conn.execute(
                "DELETE FROM lp_accounting_pending_identities "
                "WHERE position_key=?",
                (position_key,),
            )
        projector = threading.Thread(target=recover)
        projector.start()
        assert prepared.wait(2)
        store.rollback(99)
        store.ingest([replacement_block], [replacement])
        release.set()
        projector.join(2)
        assert not projector.is_alive()
        assert failures == []

        hinted_owners = {
            str(row[0])
            for row in store.read().execute(
                "SELECT identity "
                "FROM lp_accounting_pending_identities "
                "WHERE position_key=? AND kind='owner'",
                (position_key,),
            ).fetchall()
        }
        pending = store.read().execute(
            "SELECT identities_ready,identity_cursor "
            "FROM lp_accounting_pending WHERE position_key=?",
            (position_key,),
        ).fetchone()
        assert hinted_owners == {replacement_owner}
        assert (
            int(pending["identities_ready"]),
            int(pending["identity_cursor"]),
        ) == (0, 0)

        monkeypatch.setattr(
            book, "_prepare_pending_identity_hints", original_prepare,
        )
        assert book.recover_pending_identities() is True
        final_owners = {
            str(row[0])
            for row in store.read().execute(
                "SELECT identity "
                "FROM lp_accounting_pending_identities "
                "WHERE position_key=? AND kind='owner'",
                (position_key,),
            ).fetchall()
        }
        ready = store.read().execute(
            "SELECT identities_ready FROM lp_accounting_pending "
            "WHERE position_key=?",
            (position_key,),
        ).fetchone()
        assert final_owners == {replacement_owner}
        assert int(ready["identities_ready"]) == 1
    finally:
        release.set()
        store.close()


def test_v4_fee_action_before_same_tx_transfer_stays_with_prior_owner(
        tmp_path):
    app = service(tmp_path / "market.sqlite")
    prior_owner = "0x" + "56" * 20
    recipient = TOKEN
    position_key = f"nft:{MANAGER}:42"
    opened_block = header(99, 999)
    transferred_block = header(100, 1_000)
    opened = lp_effect(
        opened_block, "v4", "add", 1_000, (1_000_000, 1_000_000),
        position_state(0), position_state(1_000), key="unused",
    )
    opened.update(
        log_index=1, position_key=position_key, token_id="42",
        owner=prior_owner, custody=MANAGER,
        cashflow0="-1000000", cashflow1="-1000000",
        fee_amount0="0", fee_amount1="0",
    )
    opened["data"].update(
        trace_complete=True, fees_accrued_exact=True,
        principal_delta_exact=True,
        principal_delta={"amount0": "-1000000", "amount1": "-1000000"},
    )
    collected = lp_effect(
        transferred_block, "v4", "collect", 0, (100_000, 0),
        position_state(1_000, 100_000, 0),
        position_state(1_000), key="unused",
    )
    collected.update(
        log_index=0, position_key=position_key, token_id="42",
        owner=prior_owner, custody=MANAGER,
        cashflow0="100000", cashflow1="0",
        fee_amount0="100000", fee_amount1="0",
    )
    collected["data"].update(
        trace_complete=True, fees_accrued_exact=True,
        principal_delta_exact=True,
        principal_delta={"amount0": "0", "amount1": "0"},
    )

    def transfer(block, tx_hash, log_index, source, target, *, mint=False):
        event = swap(block, V4, "v4", index=log_index)
        event.update(
            tx_hash=tx_hash, log_index=log_index, protocol="nft",
            kind="transfer", position_key=position_key, token_id="42",
            owner=target, custody=MANAGER, liquidity_delta=None,
            amount0=None, amount1=None, cashflow0=None, cashflow1=None,
            data={
                "from": source, "to": target, "mint": mint,
                "manager_protocol": "v4",
            },
        )
        return event

    minted = transfer(
        opened_block, opened["tx_hash"], 0, "0x" + "0" * 40,
        prior_owner, mint=True,
    )
    delivered = transfer(
        transferred_block, collected["tx_hash"], 1, prior_owner, recipient,
    )
    try:
        app.store.upsert_pools(pools())
        app.store.ingest(
            [opened_block, transferred_block],
            [minted, opened, collected, delivered],
            transactions=[{
                "tx_hash": event["tx_hash"],
                "block_number": event["block_number"],
                "block_hash": event["block_hash"],
                "payer": prior_owner,
                "gas_usd": 0.01,
            } for event in (opened, collected)],
        )
        prior = app.book.owner(prior_owner, {"window": "all"})
        prior_episode = prior["positions"][0]["episodes"][0]
        assert prior_episode["observed_collected_fees_usd"] == pytest.approx(0.1)
        assert prior_episode["status"] == "transferred_out"

        received = app.book.owner(recipient, {"window": "all"})
        position = received["positions"][0]
        assert position["owner"] == recipient
        assert position["liquidity"] == "1000"
        recipient_episode = position["episodes"][0]
        assert recipient_episode["net_pnl_usd"] is None
        assert "transferred_basis_unknown" in (
            recipient_episode["coverage"]["reasons"]
        )
    finally:
        app.close()


def test_receipt_enrichment_adds_missed_logs_once_and_never_revives_orphans(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        block = header(100, int(time.time()) - 60)
        observed = swap(block, V3, "v3")
        discovered = swap(block, V4, "v4", index=1)
        discovered["tx_hash"] = observed["tx_hash"]
        app.store.ingest([block], [observed])
        receipt = {"tx_hash": observed["tx_hash"], "block_number": 100,
                   "block_hash": block["hash"], "payer": TOKEN, "gas_usd": 0.01}
        app.store.enrich([observed, discovered], transactions=[receipt])
        app.store.enrich([observed, discovered], transactions=[receipt])
        assert app.status()["indexed_events"] == 2
        assert app.overview({"window": "all"})["volume_usd"] == pytest.approx(202)
        app.store.rollback(99)
        with pytest.raises(CanonicalConflict):
            app.store.enrich([observed, discovered], transactions=[receipt])
        assert app.status()["indexed_events"] == 0
        assert app.overview({"window": "all"})["swaps"] == 0
    finally:
        app.close()


def test_empty_pool_limit_price_cannot_value_withdrawals_or_borrow_future_marks(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        blocks = [header(100 + i, int(time.time()) - 60 + i) for i in range(2)]
        closed = lp_effect(blocks[0], "v4", "remove", -1000,
                           (10_000_000, 20_000_000), position_state(1000), position_state(0))
        closed.update(sqrt_price_x96=None, tick=None, liquidity=None,
                      cashflow0="10000000", cashflow1="20000000")
        closed["data"]["pool_state_before"] = {
            "sqrt_price_x96": "1461446703485210103287273052203988822378723970341",
            "tick": 887271, "liquidity": "0",
        }
        ingest_effects(app, [blocks[0]], [closed])
        row = app.tape({"window": "all"})["rows"][0]
        assert row["price0_usd"] is None
        assert row["size_usd"] is None
        assert app.overview({"window": "all"})["net_deposits_usd"] is None

        app.store.ingest([blocks[1]], [swap(blocks[1], V4, "v4")])
        app.store.reproject([row["id"]])
        app.close()
        app = service(tmp_path / "market.sqlite")
        row = next(row for row in app.tape({"window": "all"})["rows"] if row["kind"] == "remove")
        assert row["price0_usd"] is None
        overview = app.overview({"window": "all"})
        assert overview["volume_usd"] == pytest.approx(101)
        assert overview["coverage"]["unpriced_flows"] == 1
    finally:
        app.close()


def test_flat_range_waits_for_final_claim_and_keeps_costs_after_rebuild(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        blocks = [header(100 + i, int(time.time()) - 60 + i) for i in range(4)]
        events = [
            lp_effect(blocks[0], "v3", "add", 1000, (1_000_000, 1_000_000), position_state(0), position_state(1000)),
            lp_effect(blocks[1], "v3", "remove", -1000, (1_000_000, 1_000_000), position_state(1000), position_state(0, 1_100_000, 1_000_000)),
            lp_effect(blocks[2], "v3", "collect", 0, (600_000, 500_000), position_state(0, 1_100_000, 1_000_000), position_state(0, 500_000, 500_000)),
            lp_effect(blocks[3], "v3", "collect", 0, (500_000, 500_000), position_state(0, 500_000, 500_000), position_state(0)),
        ]
        events[-1]["sqrt_price_x96"] = str(2 << 96)
        ingest_effects(app, blocks[:3], events[:3])
        assert app.book.closed({"window": "all"})["rows"] == []
        ingest_effects(app, blocks[3:], events[3:])
        row = app.book.closed({"window": "all"})["rows"][0]
        assert row["status"] == "complete"
        # Different collection marks leave the fee/principal dollar split unknown.
        assert row["fees_usd"] is None
        assert row["gas_usd"] == pytest.approx(0.04)
        assert row["net_pnl_usd"] == pytest.approx(1.56)
        later = header(104, int(time.time()) - 55)
        second = lp_effect(later, "v3", "add", 1000, (1_000_000, 1_000_000), position_state(0), position_state(1000))
        txs = ingest_effects(app, [later], [second])
        app.store.enrich([second], transactions=txs)
        assert app.book.closed({"window": "all"})["rows"][0]["net_pnl_usd"] == pytest.approx(1.56)
        app.store.rollback(102)
        assert app.book.closed({"window": "all"})["rows"] == []
        assert app.book.owner(TOKEN, {"window": "all"})["positions"][0]["status"] == "awaiting_claim"
    finally:
        app.close()

def test_v3_manager_log_order_recovers_complete_fees_without_overstating_wallet_total(
        tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        blocks = [header(100 + index, int(time.time()) - 60 + index)
                  for index in range(5)]
        position_key = f"nft:{UNISWAP_V3_POSITION_MANAGER}:42"

        def manager_transfer(block, source, target, *, burn, log_index):
            transfer = swap(block, V3, "v3", index=log_index)
            transfer.update(
                protocol="nft", kind="transfer", position_key=position_key,
                token_id="42", owner=target, custody=UNISWAP_V3_POSITION_MANAGER,
                liquidity_delta=None, amount0=None, amount1=None,
                cashflow0=None, cashflow1=None,
                data={
                    "from": source, "to": target,
                    "mint": not burn, "burn": burn,
                    "manager_protocol": "v3",
                },
            )
            return transfer

        opened = lp_effect(
            blocks[0], "v3", "add", 1_000, (1_000_000, 1_000_000),
            None, None, key="manager-order",
        )
        opened["data"] = {}
        opened["position_key"] = position_key
        minted = manager_transfer(
            blocks[0], "0x" + "0" * 40, TOKEN, burn=False, log_index=1,
        )
        minted["tx_hash"] = opened["tx_hash"]

        first_fees = lp_effect(
            blocks[1], "v3", "collect", 0, (100_000, 0),
            None, None, key="manager-order",
        )
        first_fees["data"] = {}
        first_fees["position_key"] = position_key
        removed = lp_effect(
            blocks[2], "v3", "remove", -1_000, (1_000_000, 1_000_000),
            None, None, key="manager-order",
        )
        removed["data"] = {}
        removed["position_key"] = position_key
        principal = lp_effect(
            blocks[3], "v3", "collect", 0, (1_000_000, 1_000_000),
            None, None, key="manager-order",
        )
        principal["data"] = {}
        principal["position_key"] = position_key
        burned = manager_transfer(
            blocks[3], TOKEN, None, burn=True, log_index=1,
        )
        burned["tx_hash"] = principal["tx_hash"]
        incomplete_v4 = lp_effect(
            blocks[4], "v4", "add", 1_000, (1_000_000, 1_000_000),
            position_state(0), position_state(1_000), key="pending-trace",
        )
        v3_events = [opened, minted, first_fees, removed, principal, burned]
        for event in v3_events:
            event["custody"] = UNISWAP_V3_POSITION_MANAGER
        second_key = f"nft:{UNISWAP_V3_POSITION_MANAGER}:43"
        second_v3_events = [
            {
                **event,
                "data": dict(event["data"]),
                "position_key": second_key,
                "token_id": "43",
                "tx_hash": f"0x{event['block_number'] + 10_000:064x}",
                "tx_index": 1,
            }
            for event in v3_events
        ]
        unrepairable = lp_effect(
            blocks[4], "v3", "add", 1_000, (1_000_000, 1_000_000),
            None, None, key="missing-mint",
        )
        unrepairable.update(
            position_key=f"nft:{UNISWAP_V3_POSITION_MANAGER}:0",
            owner="0x" + "e" * 40,
            custody=UNISWAP_V3_POSITION_MANAGER,
            tx_hash=f"0x{20_000:064x}", tx_index=2, data={},
        )
        events = [*v3_events, *second_v3_events, incomplete_v4, unrepairable]
        transactions = [
            {
                "tx_hash": event["tx_hash"],
                "block_number": event["block_number"],
                "block_hash": event["block_hash"],
                "payer": TOKEN,
                "gas_usd": 0.01,
            }
            for event in {
                event["tx_hash"]: event for event in events
            }.values()
        ]
        app.store.ingest(blocks, events, transactions=transactions)
        with app.store.transaction() as connection:
            connection.execute(
                "UPDATE lp_accounting_episodes SET history_complete=0,"
                "status='nft_burn_unsettled',claims_complete=0,"
                "fees_complete=0,fees_usd=NULL,gross_pnl_usd=NULL,"
                "net_pnl_usd=NULL WHERE position_key IN (?,?)",
                (position_key, second_key),
            )
        assert app.book.closed({"window": "all"})["rows"][0]["fees_usd"] is None
        assert app.store.repair_v3_birth_history(
            V3_BIRTH_REPAIR_PREFIXES, limit=1,
        ) is True
        assert sum(
            row["coverage"]["history"] == "full"
            for row in app.book.closed({"window": "all"})["rows"]
        ) == 1
        app.close()
        app = service(tmp_path / "market.sqlite")
        assert app.store.repair_v3_birth_history(
            V3_BIRTH_REPAIR_PREFIXES, limit=1,
        ) is True
        assert app.store.repair_v3_birth_history(
            V3_BIRTH_REPAIR_PREFIXES, limit=1,
        ) is False

        closed = app.book.closed({"window": "all"})["rows"]
        assert len(closed) == 2
        assert all(row["coverage"]["history"] == "full" for row in closed)
        assert closed[0]["status"] == "complete"
        assert closed[0]["coverage"]["history"] == "full"
        assert closed[0]["coverage"]["fees"] == "exact"
        assert closed[0]["fees_usd"] == pytest.approx(0.1)
        assert closed[0]["observed_collected_fees_usd"] == pytest.approx(0.1)
        assert closed[0]["gross_pnl_usd"] == pytest.approx(0.1)

        wallet = next(
            row for row in app.book.owners({"window": "all"})["rows"]
            if row["owner"] == TOKEN
        )
        assert wallet["fees_usd"] is None
        assert wallet["observed_collected_fees_usd"] == pytest.approx(0.2)
        assert wallet["coverage"]["observed_collected_fees"] == {
            "unit": "USDG_quote",
            "episodes": 2,
            "history_complete_episodes": 2,
            "total_episodes": 3,
            "complete": False,
        }
        assert wallet["coverage"]["financial_scope"] == (
            "lifetime_of_selected_episodes"
        )

        detail = app.book.owner(TOKEN, {"window": "all"})
        assert detail["summary"]["fees_usd"] is None
        assert detail["summary"]["observed_collected_fees_usd"] == pytest.approx(
            0.2,
        )
        assert detail["coverage"]["observed_collected_fees"]["complete"] is False
        historical = next(
            row for row in detail["positions"]
            if row["position_key"] == position_key
        )
        assert len(historical["ownership"]) == 1
        assert historical["ownership"][0]["acquired_by"] == "mint"
        assert historical["ownership"][0]["complete"] is True
    finally:
        app.close()


def test_closed_pages_qualified_fees_and_unicode_pair_queries(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        keys = ("high", "unicode", "low", "incomplete")
        blocks = [
            header(100 + index, int(time.time()) - 120 + index)
            for index in range(len(keys) * 2)
        ]
        events = []
        transactions = []
        for index, key in enumerate(keys):
            opened, closed, episode_transactions = traced_v4_episode(
                blocks[index * 2:index * 2 + 2], key=key,
            )
            events.extend((opened, closed))
            transactions.extend(episode_transactions)
        app.store.ingest(blocks, events, transactions=transactions)

        with app.store.transaction() as connection:
            for key, fees in zip(keys, (3.0, 2.0, 1.0, 99.0)):
                connection.execute(
                    "UPDATE lp_accounting_episodes SET fees_usd=? "
                    "WHERE position_key=?",
                    (fees, f"v4:{key}"),
                )
            connection.execute(
                "UPDATE lp_accounting_episodes SET history_complete=0 "
                "WHERE position_key=?",
                ("v4:incomplete",),
            )
            connection.execute(
                "UPDATE lp_accounting_episodes SET pool_id=? "
                "WHERE position_key IN (?,?)",
                (V3, "v4:high", "v4:low"),
            )
            connection.execute(
                "UPDATE pools SET symbol0=NULL,token0=?,"
                "symbol1=NULL,token1='' WHERE id=?",
                ("CAFÉ%_TOKEN_LONG", V4),
            )

        page = app.book.closed({
            "window": "all", "sort": "fees", "limit": 2, "offset": 1,
        })
        assert page["total"] == 4
        assert [
            row["position_key"] for row in page["rows"]
        ] == ["v4:unicode", "v4:low"]
        assert [row["fees_usd"] for row in page["rows"]] == [2.0, 1.0]

        exhausted = app.book.closed({
            "window": "all", "sort": "fees", "limit": 2, "offset": 99,
        })
        assert exhausted["rows"] == []
        assert exhausted["total"] == 4

        unicode_query = app.book.closed({
            "window": "all", "sort": "fees", "q": "FÉ%_",
        })
        assert unicode_query["total"] == 2
        assert [
            row["position_key"] for row in unicode_query["rows"]
        ] == ["v4:unicode", "v4:incomplete"]
        assert unicode_query["rows"][0]["pair"] == "CAFÉ%_TO/?"
        assert unicode_query["rows"][1]["fees_usd"] is None
        assert unicode_query["rows"][1][
            "observed_collected_fees_usd"
        ] == 99.0
        assert app.book.closed({
            "window": "all", "q": "asset%_",
        })["total"] == 0
    finally:
        app.close()


def test_fully_traced_v4_close_separates_principal_fees_and_net_profit(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        blocks = [header(100 + i, int(time.time()) - 60 + i) for i in range(2)]
        opened = lp_effect(blocks[0], "v4", "add", 1000, (1_000_000, 1_000_000), position_state(0), position_state(1000))
        closed = lp_effect(blocks[1], "v4", "remove", -1000, (1_000_000, 1_000_000), position_state(1000), position_state(0))
        for event, principal, cash, fees in (
            (opened, (-1_000_000, -1_000_000), (-1_000_000, -1_000_000), (0, 0)),
            (closed, (1_000_000, 1_000_000), (1_100_000, 1_000_000), (100_000, 0)),
        ):
            event.update(cashflow0=str(cash[0]), cashflow1=str(cash[1]),
                         fee_amount0=str(fees[0]), fee_amount1=str(fees[1]))
            event["data"].update(trace_complete=True, fees_accrued_exact=True, principal_delta_exact=True,
                                 principal_delta={"amount0": str(principal[0]), "amount1": str(principal[1])})
        ingest_effects(app, blocks, [opened, closed])
        row = app.book.closed({"window": "all"})["rows"][0]
        assert row["coverage"]["qualified"] is True
        assert row["withdrawal_usd"] == pytest.approx(2)
        assert row["fees_usd"] == pytest.approx(0.1)
        assert row["gross_pnl_usd"] == pytest.approx(0.1)
        assert row["net_pnl_usd"] == pytest.approx(0.08)
    finally:
        app.close()

def test_owner_summary_deduplicates_shared_transaction_costs_and_preserves_unknowns(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        owner2 = "0x" + "78" * 20
        blocks = [header(100 + i, int(time.time()) - 60 + i) for i in range(6)]
        opened_a = lp_effect(blocks[0], "v4", "add", 1000, (1_000_000, 1_000_000),
                             position_state(0), position_state(1000), key="a")
        opened_b = lp_effect(blocks[1], "v4", "add", 1000, (1_000_000, 1_000_000),
                             position_state(0), position_state(1000), key="b")
        closed_a = lp_effect(blocks[2], "v4", "remove", -1000, (1_000_000, 1_000_000),
                             position_state(1000), position_state(0), key="a")
        closed_b = lp_effect(blocks[2], "v4", "remove", -1000, (1_000_000, 1_000_000),
                             position_state(1000), position_state(0), key="b")
        closed_b.update(tx_hash=closed_a["tx_hash"], log_index=1)
        for event in (opened_a, opened_b, closed_a, closed_b):
            event["custody"] = MANAGER
            opening = event["kind"] == "add"
            event.update(
                cashflow0=str(-1_000_000 if opening else 1_100_000),
                cashflow1=str(-1_000_000 if opening else 1_000_000),
                fee_amount0=str(0 if opening else 100_000), fee_amount1="0",
            )
            principal = -1_000_000 if opening else 1_000_000
            event["data"].update(
                trace_complete=True, fees_accrued_exact=True, principal_delta_exact=True,
                principal_delta={"amount0": str(principal), "amount1": str(principal)},
            )

        incomplete_open = lp_effect(
            blocks[3], "v4", "add", 1000, (1_000_000, 1_000_000),
            position_state(0), position_state(1000), key="incomplete",
        )
        incomplete_close = lp_effect(
            blocks[4], "v4", "remove", -1000, (1_000_000, 1_000_000),
            position_state(1000), position_state(0), key="incomplete",
        )
        for event in (incomplete_open, incomplete_close):
            event.update(owner=owner2, custody=MANAGER)
        events = [opened_a, opened_b, closed_a, closed_b, incomplete_open, incomplete_close]
        unknown_owner = lp_effect(
            blocks[5], "v4", "add", 1000, (1_000_000, 1_000_000),
            position_state(0), position_state(1000), key="custody-only",
        )
        unknown_owner.update(owner=None, custody=MANAGER, identity_basis="custody_only")
        events.append(unknown_owner)

        def receipt(event, payer, gas):
            return {"tx_hash": event["tx_hash"], "block_number": event["block_number"],
                    "block_hash": event["block_hash"], "payer": payer, "gas_usd": gas}

        transactions = [
            receipt(opened_a, TOKEN, 0.01), receipt(opened_b, TOKEN, 0.01),
            receipt(closed_a, TOKEN, 0.01),
            receipt(incomplete_open, owner2, 0.02), receipt(incomplete_close, owner2, None),
        ]
        app.store.ingest(blocks, events, transactions=transactions)
        rows = app.book.owners({"window": "all", "limit": 20})["rows"]
        owner_rows = {row["owner"]: row for row in rows if row["owner"] is not None}

        assert owner_rows[TOKEN]["positions"] == 2
        assert owner_rows[TOKEN]["fees_usd"] == pytest.approx(0.2)
        assert owner_rows[TOKEN]["gross_pnl_usd"] == pytest.approx(0.2)
        assert owner_rows[TOKEN]["gas_usd"] == pytest.approx(0.03)
        assert owner_rows[TOKEN]["net_pnl_usd"] == pytest.approx(0.17)
        assert owner_rows[TOKEN]["coverage"]["cost_qualified"] is True
        assert owner_rows[owner2]["fees_usd"] is None
        assert owner_rows[owner2]["gas_usd"] is None
        assert owner_rows[owner2]["net_pnl_usd"] is None
        assert owner_rows[owner2]["coverage"]["cost_qualified"] is False

        custody_rows = [row for row in rows if row["custody"] == MANAGER]
        assert len(custody_rows) == 1
        custody = custody_rows[0]
        assert custody["custody"] == MANAGER
        assert custody["positions"] == 4
        assert custody["owner"] is None
        assert custody["gross_pnl_usd"] is custody["gas_usd"] is custody["net_pnl_usd"] is None
    finally:
        app.close()


def test_current_owner_activity_is_live_deduplicated_and_reorg_safe(
        tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        durable_time = int(time.time()) - 10_000
        durable_block = header(90, durable_time)
        durable = lp_effect(
            durable_block, "v3", "add", 1000, (1_000_000, 1_000_000),
            position_state(0), position_state(1000), key="durable",
        )
        durable["custody"] = MANAGER
        ingest_effects(app, [durable_block], [durable])
        app.store.ingest([durable_block], [], cursor={
            "from_block": 90, "to_block": 90, "block_number": 90,
            "block_hash": durable_block["hash"], "timestamp": durable_time,
        })

        current_owner = "0x" + "78" * 20
        current_time = int(time.time())
        current_block = header(100, current_time)
        current = lp_effect(
            current_block, "v4", "add", 1000, (1_000_000, 1_000_000),
            position_state(0), position_state(1000), key="current",
        )
        current.update(
            owner=current_owner, custody=MANAGER, pool=pools()[1],
        )
        app.observe_current_block(current_block)
        app.observe_current_events(current_block, (current,))

        result = app.owners({"window": "all", "sort": "activity", "limit": 20})
        live = next(row for row in result["rows"] if row["owner"] == current_owner)
        durable_row = next(row for row in result["rows"] if row["owner"] == TOKEN)
        assert live["positions"] is None
        assert live["open_positions"] is None
        assert live["gross_pnl_usd"] is live["net_pnl_usd"] is None
        assert live["activity"] == {
            "block_number": 100,
            "timestamp": current_time,
            "pool_id": V4,
            "pair": "ASSET / USDG",
            "kind": "add",
            "tx_hash": current["tx_hash"],
            "event_count": 1,
            "qualification": "provisional_canonical",
            "identity_match": "beneficial_owner",
        }
        assert durable_row["positions"] == 1
        assert durable_row["open_positions"] == 1
        assert durable_row["activity"]["qualification"] == "durable_canonical_index"
        assert result["accounting_as_of"] == durable_time
        assert result["current_activity"] == {
            "revision": 1, "epoch": 0, "head": 100, "observed_from": 100,
            "qualification": "provisional_canonical",
        }
        assert result["coverage"]["financials"]["current_stream_combined"] is False
        assert live["financials"]["pending"] is True
        assert live["coverage"]["post_accounting_activity"] == "pending"

        revision = result["current_activity"]["revision"]
        app.observe_current_events(current_block, (current,))
        replayed = app.owners({"window": "all", "sort": "activity", "limit": 20})
        replayed_live = next(
            row for row in replayed["rows"] if row["owner"] == current_owner
        )
        assert replayed["current_activity"]["revision"] == revision
        assert replayed_live["activity"]["event_count"] == 1
        envelope_revision = replayed["revision"]
        app.observe_current_block(header(101, current_time + 1))
        advanced = app.owners({
            "window": "all", "sort": "activity", "limit": 20,
        })
        assert advanced["revision"] == envelope_revision
        assert advanced["current_activity"]["revision"] == revision
        assert advanced["current_activity"]["head"] == 101

        enriched = {
            **current,
            "data": {**current["data"], "trace_complete": True},
        }
        app.observe_current_events(current_block, (enriched,))
        enriched_result = app.owners({
            "window": "all", "sort": "activity", "q": current_owner,
            "limit": 20,
        })
        assert enriched_result["total"] == 1
        assert enriched_result["rows"][0]["activity"]["event_count"] == 1
        assert enriched_result["current_activity"]["revision"] == revision + 1

        next_durable_block = header(91, durable_time + 1)
        next_durable = lp_effect(
            next_durable_block, "v3", "add", 1000,
            (1_000_000, 1_000_000), position_state(0),
            position_state(1000), key="durable-next",
        )
        next_durable["custody"] = MANAGER
        ingest_effects(app, [next_durable_block], [next_durable])
        app.store.ingest([next_durable_block], [], cursor={
            "from_block": 91, "to_block": 91, "block_number": 91,
            "block_hash": next_durable_block["hash"],
            "timestamp": durable_time + 1,
        })
        refreshed = app.owners({
            "window": "all", "sort": "activity", "q": TOKEN, "limit": 20,
        })
        assert refreshed["rows"][0]["positions"] == 2
        assert refreshed["accounting_as_of"] == durable_time + 1

        replacement = header(100, current_time + 1, branch=1_000)
        app.observe_current_block(replacement)
        after_reorg = app.owners({
            "window": "all", "sort": "activity", "limit": 20,
        })
        assert not any(row.get("owner") == current_owner for row in after_reorg["rows"])
        assert after_reorg["current_activity"]["head"] == 100
        assert after_reorg["current_activity"]["epoch"] == 1
    finally:
        app.close()


def test_owner_projection_is_shared_and_never_blocks_current_feed(
        tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    release = threading.Event()
    entered = threading.Event()
    try:
        block = header(100, int(time.time()))
        event = lp_effect(
            block, "v4", "add", 1000, (1_000_000, 1_000_000),
            position_state(0), position_state(1000), key="async-owner",
        )
        event.update(
            owner="0x" + "78" * 20, custody=MANAGER, pool=pools()[1],
        )
        app.observe_current_block(block)
        app.observe_current_events(block, (event,))
        cursor = app.indexer.feed_updates()
        original_candidates = app.book.owner_candidates
        calls = 0

        def slow_candidates(params):
            nonlocal calls
            calls += 1
            entered.set()
            assert release.wait(2)
            return original_candidates(params)

        monkeypatch.setattr(app.book, "owner_candidates", slow_candidates)
        params = {
            "window": "all", "owner_sort": "activity", "owner_limit": "200",
        }
        assert app.poll_owners(params) is None
        assert entered.wait(1)

        app.indexer._emit_feed("block", {
            "number": 101, "hash": "0x" + "65" * 32,
            "parent_hash": block["hash"], "timestamp": int(time.time()),
        })
        app.indexer._emit_current_activity(
            block, (event,), source="test", observe=False,
        )
        returned = threading.Event()
        streamed = {}

        def read_feed():
            streamed.update(app.stream_updates(
                {"kind": "lp", "channel": "both"},
                cursor["sequence"], cursor["feed_epoch"],
            ))
            returned.set()

        reader = threading.Thread(target=read_feed)
        reader.start()
        assert returned.wait(0.5), "current feed waited for owner SQL"
        reader.join(1)
        assert [item["event"] for item in streamed["events"]] == [
            "block", "activity",
        ]
        assert app.poll_owners(params) is None

        release.set()
        deadline = time.monotonic() + 2
        envelope = None
        while envelope is None and time.monotonic() < deadline:
            envelope = app.poll_owners(params)
            if envelope is None:
                time.sleep(0.01)
        assert envelope is not None
        assert len(envelope["rows"]) == 2
        assert envelope["rows"][0]["activity"]["kind"] == "add"
        assert app.owners({
            "window": "all", "sort": "activity", "limit": 200,
        }) == envelope
        assert calls == 1

        from types import SimpleNamespace
        from rhpools import lp_market_claims
        expired = time.monotonic() + 181
        monkeypatch.setattr(
            lp_market_claims, "time", SimpleNamespace(monotonic=lambda: expired),
        )
        assert app.claims._next() is None
        assert app.poll_owners(params, after_revision=envelope["revision"]) is None
        assert app.claims._next()[0] in {row["owner"] for row in envelope["rows"]}
        assert calls == 1
    finally:
        release.set()
        app.close()


def test_wallet_read_does_not_queue_behind_unrelated_frames(tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    release = threading.Event()
    entered = [threading.Event(), threading.Event()]
    returned = threading.Event()
    result = {}
    reader = None
    original = app.frame

    def slow_frame(params, *args, **kwargs):
        entered[int(params["q"])].set()
        assert release.wait(3)
        return original(params, *args, **kwargs)

    monkeypatch.setattr(app, "frame", slow_frame)
    try:
        block = header(100, int(time.time()))
        event = lp_effect(
            block, "v4", "add", 1_000, (1_000_000, 1_000_000),
            position_state(0), position_state(1_000),
        )
        app.observe_current_block(block)
        app.observe_current_events(block, (event,))
        for index in range(2):
            assert app.poll_frame({"window": "all", "q": str(index)}, -1, 0) is None
            assert entered[index].wait(1)

        def read_wallet():
            result.update(app.owners({"window": "all", "q": TOKEN}))
            returned.set()

        reader = threading.Thread(target=read_wallet)
        reader.start()
        assert returned.wait(1), "wallet read queued behind unrelated frame SQL"
        assert TOKEN in {row["owner"] for row in result["rows"]}
    finally:
        release.set()
        if reader is not None:
            reader.join(3)
        app.close()


@pytest.mark.parametrize("reorg", [False, True])
def test_completed_wallet_projection_is_deliverable_during_live_changes(
        tmp_path, monkeypatch, reorg):
    app = service(tmp_path / "market.sqlite")
    computed = threading.Event()
    release = threading.Event()
    original = app._owners_result

    def hold_completed(*args, **kwargs):
        result = original(*args, **kwargs)
        computed.set()
        assert release.wait(3)
        return result

    monkeypatch.setattr(app, "_owners_result", hold_completed)
    params = {"window": "all", "sort": "activity", "limit": 20}
    try:
        block = header(100, int(time.time()))
        first = lp_effect(
            block, "v4", "add", 1_000, (1_000_000, 1_000_000),
            position_state(0), position_state(1_000),
        )
        app.observe_current_block(block)
        app.observe_current_events(block, (first,))
        assert app.poll_owners(params) is None
        assert computed.wait(1)
        next_block = header(
            100 if reorg else 101, int(time.time()) + 1,
            branch=1_000 if reorg else 0,
        )
        app.observe_current_block(next_block)
        second = lp_effect(
            next_block, "v4", "add", 1_000, (1_000_000, 1_000_000),
            position_state(0), position_state(1_000), key="second",
        )
        second.update(owner="0x" + "78" * 20, custody=MANAGER)
        app.observe_current_events(next_block, (second,))
        release.set()
        if reorg:
            result = app.owners(params)
            assert TOKEN not in {row["owner"] for row in result["rows"]}
            assert result["current_activity"]["epoch"] == 1
        else:
            deadline = time.monotonic() + 0.5
            result = None
            while result is None and time.monotonic() < deadline:
                result = app.poll_owners(params)
                if result is None:
                    time.sleep(0.01)
            assert result is not None, "completed wallets were starved by new activity"
            assert result["current_activity"]["head"] == 100
            assert TOKEN in {row["owner"] for row in result["rows"]}
            assert app.poll_owners(params) == result, (
                "another consumer lost the completed wallet snapshot"
            )
            updated = None
            deadline = time.monotonic() + 3
            while updated is None and time.monotonic() < deadline:
                updated = app.poll_owners(params, result["revision"])
                if updated is None:
                    time.sleep(0.01)
            assert updated is not None, "cached wallets stopped refreshing"
            assert updated["current_activity"]["head"] == 101
            assert "0x" + "78" * 20 in {
                row["owner"] for row in updated["rows"]
            }
    finally:
        release.set()
        app.close()


def test_current_transfer_updates_both_beneficial_owners_without_moving_financials(
        tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        prior_owner = "0x" + "56" * 20
        new_owner = "0x" + "78" * 20
        block = header(100, int(time.time()))
        event = lp_effect(
            block, "v4", "transfer", 0, (0, 0),
            position_state(1000), position_state(1000), key="moved",
        )
        event.update(
            owner=new_owner, custody=MANAGER, pool=pools()[1],
            data={
                **event["data"], "prior_owner": prior_owner,
                "new_owner": new_owner,
            },
        )
        app.observe_current_block(block)
        app.observe_current_events(block, (event,))

        result = app.owners({"window": "1h", "sort": "activity", "limit": 20})
        beneficial = {
            row["owner"]: row for row in result["rows"]
            if row.get("owner") is not None
        }
        assert set(beneficial) == {prior_owner, new_owner}
        for row in beneficial.values():
            assert row["positions"] is None
            assert row["open_positions"] is None
            assert row["activity"]["kind"] == "transfer"
            assert row["activity"]["event_count"] == 1
            assert row["activity"]["identity_match"] == "beneficial_owner"
        custody = next(
            row for row in result["rows"]
            if row.get("owner") is None and row.get("custody") == MANAGER
        )
        assert custody["activity"]["identity_match"] == "custody"
        assert custody["gross_pnl_usd"] is None
        assert app.owners({
            "window": "1h", "protocol": "v3", "limit": 20,
        })["rows"] == []
        filtered = app.owners({
            "window": "1h", "q": prior_owner, "limit": 20,
        })
        assert [row["owner"] for row in filtered["rows"]] == [prior_owner]
        wallets = app.owners({
            "window": "1h", "sort": "activity", "identity_scope": "wallets",
            "limit": 1,
        })
        assert wallets["total"] == 2
        assert len(wallets["rows"]) == 1
        assert wallets["rows"][0]["owner"] in {prior_owner, new_owner}
        custody_only = app.owners({
            "window": "1h", "sort": "activity", "identity_scope": "custody",
            "limit": 1,
        })
        assert custody_only["total"] == 1
        assert custody_only["rows"][0]["owner"] is None
        assert custody_only["rows"][0]["custody"] == MANAGER
    finally:
        app.close()

def test_wallet_window_expires_at_exact_inclusive_boundary(tmp_path, monkeypatch):
    from rhpools import lp_market_accounting

    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        block = header(100, 1_000)
        opened = lp_effect(
            block, "v4", "add", 1_000, (1_000_000, 1_000_000),
            position_state(0), position_state(1_000),
        )
        app.store.ingest([block], [opened])
        clock = [4_600]
        monkeypatch.setattr(lp_market_accounting.time, "time", lambda: clock[0])
        included = app.book.owner_candidates({"window": "1h"})
        assert [
            row["owner"] for row in included["rows"] if row["owner"] is not None
        ] == [TOKEN]
        clock[0] = 4_601
        assert app.book.owner_candidates({"window": "1h"})["rows"] == []
    finally:
        app.close()


def test_wallet_snapshot_completes_while_indexer_appends(tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())

        def append(number, timestamp, key):
            block = header(number, timestamp)
            opened = lp_effect(
                block, "v4", "add", 1_000, (1_000_000, 1_000_000),
                position_state(0), position_state(1_000), key=key,
            )
            app.store.ingest([block], [opened], cursor={
                "from_block": number, "to_block": number, "block_number": number,
                "block_hash": block["hash"], "timestamp": timestamp,
            })

        append(100, 1_000, "first")
        original_gas = app.book._owner_gas_values
        appended = False

        def append_during_read(*args, **kwargs):
            nonlocal appended
            if not appended:
                appended = True
                append(101, 1_001, "second")
            return original_gas(*args, **kwargs)

        monkeypatch.setattr(app.book, "_owner_gas_values", append_during_read)
        snapshot = app.book.owner_candidates({"window": "all"})
        wallet = next(row for row in snapshot["rows"] if row["owner"] == TOKEN)
        assert wallet["positions"] == 1
        assert snapshot["accounting_as_of"] == 1_000
        refreshed = app.book.owner_candidates({"window": "all"})
        wallet = next(row for row in refreshed["rows"] if row["owner"] == TOKEN)
        assert wallet["positions"] == 2
        assert refreshed["accounting_as_of"] == 1_001
    finally:
        app.close()

def test_prior_unsettled_claim_cannot_become_new_episode_profit(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        blocks = [header(100 + i, int(time.time()) - 60 + i) for i in range(3)]
        events = [
            lp_effect(blocks[0], "v3", "add", 1000, (1_000_000, 1_000_000), position_state(0, 100_000_000, 0), position_state(1000, 100_000_000, 0)),
            lp_effect(blocks[1], "v3", "remove", -1000, (1_000_000, 1_000_000), position_state(1000, 100_000_000, 0), position_state(0, 101_000_000, 1_000_000)),
            lp_effect(blocks[2], "v3", "collect", 0, (101_000_000, 1_000_000), position_state(0, 101_000_000, 1_000_000), position_state(0)),
        ]
        ingest_effects(app, blocks, events)
        row = app.book.closed({"window": "all"})["rows"][0]
        assert row["coverage"]["qualified"] is False
        assert row["net_pnl_usd"] is None
    finally:
        app.close()


def test_universal_search_preserves_identifier_types_and_pair_intersection(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        block = header(100, int(time.time()) - 60)
        event = {**swap(block, V3, "v3"), "owner": V3}
        app.store.ingest([block], [event])
        app.store.ensure_search_index()
        rows, _ = app.store.search(V3)
        matches = {row["kind"]: row for row in rows if row["id"] == V3}
        assert matches["pool"]["href"] == f"/pool?id={V3}"
        assert matches["owner"]["href"] == f"/lp?owner={V3}"
        rows, _ = app.store.search("USDG / ASSET v3")
        assert [row["id"] for row in rows if row["kind"] == "pool"] == [V3]
        rows, _ = app.store.search(event["tx_hash"])
        assert any(row["kind"] == "transaction" and
                   row["href"] == f"https://robinscan.io/tx/{event['tx_hash']}"
                   for row in rows)
    finally:
        app.close()


def test_live_feed_reconnect_announces_replacement_without_replaying_orphan(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        indexer = app.indexer
        original = header(100, int(time.time()) - 1)
        indexer._publish_current_block(original, (), source="test")
        first = indexer.feed_updates()
        replacement = header(100, int(time.time()), branch=1000)
        indexer._publish_current_block(replacement, (), source="test")
        resumed = indexer.feed_updates(first["sequence"], first["feed_epoch"])
        assert resumed["reset"] is False
        assert [item["data"]["hash"] for item in resumed["events"]
                if item["event"] == "block"] == [replacement["hash"]]
        assert resumed["events"][0]["data"]["gap"]["replaced_hash"] == original["hash"]
        reconnect = indexer.feed_updates(0, "expired-process")
        assert reconnect["reset"] is True
        assert [item["data"]["hash"] for item in reconnect["events"]
                if item["event"] == "block"] == [replacement["hash"]]
        assert not any(item["event"] == "activity" for item in reconnect["events"])
        reconnect["events"][0]["data"]["gap"]["reconnect"]["reason"] = "mutated"
        isolated = indexer.feed_updates(0, "expired-process")
        assert isolated["events"][0]["data"]["gap"]["reconnect"]["reason"] == (
            "feed_cursor_unavailable"
        )
    finally:
        app.close()


def test_pool_sort_keeps_unpriced_metrics_last_in_both_directions(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        block = header(100, int(time.time()) - 1)
        app.store.ingest([block], [swap(block, V3, "v3")])
        for order in ("asc", "desc"):
            rows = app.pools({"window": "all", "sort": "volume", "order": order})["rows"]
            assert [(row["id"], row["volume_usd"]) for row in rows] == [
                (V3, pytest.approx(101)), (V4, None),
            ]
    finally:
        app.close()


def test_receipt_settlement_flows_are_not_v4_position_cashflows(tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        block = header(100, int(time.time()) - 1)
        event = {
            **swap(block, V4, "v4"), "kind": "add",
            "amount0": None, "amount1": None, "liquidity_delta": "100",
            "pool": {**pools()[1], "hook": "0x" + "00" * 20},
        }
        sender = "0x" + "56" * 20
        logs = [{"topics": [V4_MODIFY_LIQUIDITY_TOPIC]}]
        for index, (token, amount) in enumerate(((TOKEN, 100), (USDG, 200)), 1):
            logs.append({
                "address": token, "logIndex": hex(index),
                "topics": [TRANSFER_TOPIC, "0x" + sender[2:].zfill(64),
                           "0x" + MANAGER[2:].zfill(64)],
                "data": "0x" + f"{amount:064x}",
            })
        receipt = {
            "blockHash": block["hash"], "transactionHash": event["tx_hash"],
            "logs": logs,
        }
        indexer = app.indexer
        indexer._publish_current_block(block, (), source="test")
        indexer._observed_blocks[100][1].append(event)
        cursor = indexer.feed_updates()
        monkeypatch.setattr(indexer._clients["receipt"], "call", lambda *_: receipt)
        indexer._enrich_current_receipt((block["hash"], event["tx_hash"]), 100, "test")
        updates = app.stream_updates({"kind": "lp"}, cursor["sequence"], cursor["feed_epoch"])
        row = updates["events"][0]["data"]["rows"][0]
        assert row["flow_scope"] == "transaction"
        assert row["flow_complete"] is True
        assert [(item["token"], item["amount"]) for item in row["transaction_transfers"]] == [
            (TOKEN, "100"), (USDG, "200"),
        ]
        assert row["transaction_flow0"] == "-100"
        assert row["transaction_flow1"] == "-200"
        assert row["usdg_flow_usd"] == pytest.approx(-0.0002)
        assert row["cashflow_usd"] == pytest.approx(-0.0003)
        assert row["fees_usd"] is None
        assert row["cashflow0"] is None and row["cashflow1"] is None

        position_event = lp_effect(
            block, "v3", "collect", 0, (128_940, 16_115_520),
            position_state(1000), position_state(1000), key="direct-usdg",
        )
        position_event["pool"] = pools()[0]
        position_row = app._current_event_view(position_event)
        assert position_row["flow_scope"] == "position_event"
        assert position_row["cashflow1"] == "16115520"
        assert position_row["usdg_flow_usd"] == pytest.approx(16.11552)
        assert position_row["fees_usd"] is None
    finally:
        app.close()


def test_live_usdg_quote_uses_persisted_metadata_without_countertoken_scale(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.save_token_metadata(USDG, "USDG", 6)
        pool = app.indexer._normalize_pool({
            **pools()[0], "symbol0": None, "symbol1": None,
            "decimals0": None, "decimals1": None,
        })
        row = app._current_event_view({
            "pool": pool, "kind": "add", "flow_scope": "transaction",
            "transaction_flow0": None, "transaction_flow1": "-5000000",
            "cashflow0": None, "cashflow1": "99000000",
        })
        assert row["token1"]["symbol"] == "USDG"
        assert row["usdg_flow_usd"] == pytest.approx(-5)
        assert row.get("cashflow_usd") is None
    finally:
        app.close()


def test_current_fees_use_protocol_input_signs_and_static_rate_is_qualified(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        block = header(100, int(time.time()) - 1)
        rows = []
        for pool, protocol in zip(pools(), ("v3", "v4")):
            rows.append(app._current_event_view({
                **swap(block, pool["id"], protocol),
                "pool": pool,
            }))
        assert rows[0]["fees_usd"] == pytest.approx(0.303)
        assert rows[1]["fees_usd"] == pytest.approx(1.01)
        assert all(row["fees_scope"] == "gross_swap" for row in rows)
        assert all(
            row["fees_qualification"] == "observed_input_delta_x_event_fee"
            for row in rows
        )
        assert rows[0]["pool_fee_ppm"] == 3_000
        assert rows[0]["pool_fee_basis"] == "verified_static_pool_fee"
        assert rows[1]["pool_fee_ppm"] is None
        assert rows[1]["pool_fee_basis"] == "dynamic_fee_flag_not_a_rate"
    finally:
        app.close()


def test_current_usdg_input_fee_does_not_wait_for_countertoken_metadata(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        block = header(100, int(time.time()) - 1)
        incomplete = {**pools()[0], "decimals0": None}
        row = app._current_event_view({
            **swap(block, V3, "v3"),
            "pool": incomplete,
        })
        assert row["fees_usd"] == pytest.approx(0.303)
        assert row["price0_usd"] is None
        assert row["fees_qualification"] == "observed_input_delta_x_event_fee"
    finally:
        app.close()


def test_current_position_fee_preserves_signed_delta_and_requires_attribution(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        block = header(100, int(time.time()) - 1)
        observed = lp_effect(
            block, "v4", "remove", -100, (100_000, 0),
            position_state(100), position_state(0),
        )
        observed.update(
            pool=pools()[1], fee_amount0="-100000", fee_amount1="0",
            transaction_flow0="999999999", transaction_flow1="999999999",
        )
        observed["data"]["fees_accrued_exact"] = True
        row = app._current_event_view(observed)
        assert row["fees_usd"] == pytest.approx(-0.1)
        assert row["fees_scope"] == "position_event"
        assert row["fees_qualification"] == "observed_signed_position_fee_delta"
        assert row["flow_scope"] == "position_event"

        unattributed = {
            **observed,
            "data": {},
            "fee_amount0": "100000",
            "fee_amount1": "0",
        }
        row = app._current_event_view(unattributed)
        assert row["fees_usd"] is None
        assert row["fees_qualification"] == (
            "unavailable_without_position_fee_attribution"
        )
    finally:
        app.close()


def test_persisted_metadata_rematerializes_observed_tape_and_wallet_rows(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        unknown = {
            **pools()[1],
            "symbol0": None, "symbol1": None,
            "decimals0": None, "decimals1": None,
        }
        app.store.upsert_pools([unknown])
        block = header(100, int(time.time()))
        event = lp_effect(
            block, "v4", "add", 1000, (1_000_000, 1_000_000),
            position_state(0), position_state(1000),
        )
        event["pool"] = unknown
        item = {
            "sequence": 7,
            "data": {"feed_epoch": "metadata", "sequence": 7, "rows": [event]},
        }
        first_tape = app._current_feed_rows(item)[0]
        app.observe_current_block(block)
        app.observe_current_events(block, (event,))
        first_wallet = app._current_owner_snapshot({
            "window": "all", "protocol": "", "pool": "", "q": "",
        })["rows"][0]
        assert first_tape["pair"] == f"{TOKEN} / {USDG}"
        assert first_tape["token0"]["metadata_state"] == "unavailable"
        assert first_wallet["activity"]["pair"] == f"{TOKEN} / {USDG}"
        assert app._pair({"pool_id": V4}) == V4
        assert app._observer_pair({"pool_id": V4}) == V4

        app.store.save_token_metadata(TOKEN, "ASSET", 6)
        app.store.save_token_metadata(USDG, "USDG", 6)

        enriched_tape = app._current_feed_rows(item)[0]
        enriched_wallet = app._current_owner_snapshot({
            "window": "all", "protocol": "", "pool": "", "q": "",
        })["rows"][0]
        assert enriched_tape["pair"] == "ASSET / USDG"
        assert enriched_tape["token0"] == {
            "address": TOKEN, "symbol": "ASSET", "decimals": 6,
            "metadata_state": "complete",
        }
        assert enriched_wallet["activity"]["pair"] == "ASSET / USDG"
    finally:
        app.close()


def test_late_trace_enrichment_replaces_current_row_with_position_fees(
    tmp_path, monkeypatch,
):
    app = service(tmp_path / "market.sqlite")
    try:
        block = header(100, int(time.time()) - 1)
        initial = lp_effect(
            block, "v4", "remove", -100, (100_000, 0),
            position_state(100), position_state(0),
        )
        initial.update(
            pool=pools()[1], cashflow0=None, cashflow1=None,
            fee_amount0=None, fee_amount1=None,
            accounting_basis="pending_trace",
        )
        initial["data"] = {"trace_complete": False, "cashflow_basis": "pending_trace"}
        monkeypatch.setattr(
            app.indexer, "_decode_current", lambda *_args, **_kwargs: [initial],
        )
        cursor = app.indexer.feed_updates()
        app.indexer._publish_current_block(block, [], source="test")
        first = app.stream_updates(
            {"kind": "lp"}, cursor["sequence"], cursor["feed_epoch"],
        )
        initial_row = next(
            row for event in first["events"] if event["event"] == "activity"
            for row in event["data"]["rows"]
        )
        enriched = {
            **initial,
            "cashflow0": "100000",
            "cashflow1": "0",
            "fee_amount0": "10000",
            "fee_amount1": "0",
            "accounting_basis": "v4_modifyLiquidity_return",
            "data": {
                "trace_complete": True,
                "fees_accrued_exact": True,
                "fees_basis": "modifyLiquidity_return_exact_but_donate_inflatable",
            },
        }
        app.indexer._publish_enriched_current_events(
            [enriched], source="test+enrichment",
        )
        late = app.stream_updates(
            {"kind": "lp"}, first["sequence"], first["feed_epoch"],
        )
        late_row = next(
            row for event in late["events"] if event["event"] == "activity"
            for row in event["data"]["rows"]
        )
        assert late_row["id"] == initial_row["id"]
        assert late_row["fees_usd"] == pytest.approx(0.01)
        assert late_row["fees_scope"] == "position_event"
        assert late_row["flow_scope"] == "position_event"
        assert next(
            event["data"]["late"] for event in late["events"]
            if event["event"] == "activity"
        ) is True
    finally:
        app.close()


def test_concurrent_subscribers_share_one_current_projection(tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    release = threading.Event()
    entered = threading.Event()
    failures = []
    results = []
    calls = 0
    calls_lock = threading.Lock()
    try:
        block = header(100, int(time.time()) - 1)
        event = {**swap(block, V3, "v3"), "pool": pools()[0]}
        monkeypatch.setattr(
            app.indexer, "_decode_current", lambda *_args, **_kwargs: [event],
        )
        original = app._current_event_view

        def project(raw, *args, **kwargs):
            nonlocal calls
            with calls_lock:
                calls += 1
            entered.set()
            if not release.wait(2):
                raise TimeoutError("test did not release current projection")
            return original(raw, *args, **kwargs)

        monkeypatch.setattr(app, "_current_event_view", project)
        cursor = app.indexer.feed_updates()
        app.indexer._publish_current_block(block, [], source="test")

        def consume():
            try:
                results.append(app.stream_updates(
                    {"kind": "all"}, cursor["sequence"], cursor["feed_epoch"],
                ))
            except BaseException as exc:
                failures.append(exc)

        consumers = [threading.Thread(target=consume) for _ in range(6)]
        for consumer in consumers:
            consumer.start()
        assert entered.wait(1)
        release.set()
        for consumer in consumers:
            consumer.join(2)
        assert not any(consumer.is_alive() for consumer in consumers)
        assert failures == []
        assert calls == 1
        assert [
            event["data"]["rows"][0]["fees_usd"]
            for result in results
            for event in result["events"] if event["event"] == "activity"
        ] == pytest.approx([0.303] * 6)
    finally:
        release.set()
        app.close()


def test_existing_durable_pool_restores_workbench_without_checkpoint(
    tmp_path, monkeypatch,
):
    from eth_utils import keccak
    from rhpools import workbench_market as market

    class OfflineRpc:
        def call(self, *_args):
            raise market.RpcError("test state provider offline")

        def close(self):
            pass

    def word(value):
        return f"{value & ((1 << 256) - 1):064x}"

    token0, token1 = sorted((TOKEN, USDG))
    raw_key = "".join(
        word(value)
        for value in (int(token0, 16), int(token1, 16), 3000, 60, 0)
    )
    pool_id = "0x" + keccak(bytes.fromhex(raw_key)).hex()
    durable = {
        "id": pool_id, "address": MANAGER, "protocol": "v4",
        "token0": token0, "token1": token1,
        "symbol0": "ASSET", "symbol1": "USDG",
        "decimals0": 6, "decimals1": 6,
        "fee_ppm": 3000, "tick_spacing": 60,
        "hook": "0x" + "00" * 20, "factory": MANAGER,
        "created_block": 100, "source": "PositionManager.poolKeys",
        "metadata_json": {
            "configured_fee": 3000,
            "dynamic_fee": False,
            "discovery_basis": "full_poolKey_hash",
            "identity_verified_block": 100,
            "identity_verified_hash": header(100, 100)["hash"],
        },
    }
    path = tmp_path / "market.sqlite"
    store = MarketStore(path)
    store.upsert_pools([durable])
    store.close()
    universe = market._Universe(
        (), {}, {"v2": 0, "v3": 0, "v4": 0}, (), (), {},
    )
    monkeypatch.setattr(market, "_load_universe", lambda: universe)
    catalog = market.MarketService(
        "http://127.0.0.1:1", data_dir=tmp_path,
        external_index=True, rpc=OfflineRpc(),
    )
    app = LPMarketService(
        catalog, "http://127.0.0.1:1", path, start=False,
    )
    try:
        restored = catalog._pool_by_id(pool_id)
        assert restored is not None
        assert (restored.token0, restored.token1) == (token0, token1)
        detail = catalog.detail(pool_id)
        assert detail["pool"]["pair"] == "ASSET/USDG"
        assert detail["health"]["state"] == "warming"
    finally:
        app.close()
        catalog.close()

def test_legacy_dynamic_v4_pool_is_durable_in_details_and_catalog_after_restart(
    tmp_path, monkeypatch,
):
    from rhpools import workbench_market as market

    class OfflineRpc:
        def call(self, *_args):
            raise market.RpcError("test state provider offline")

        def close(self):
            pass

    pool_id = "0x9b6604eeffbad3b216199d63cad1b46d9665c1429207e1c442531fb7263d28a2"
    token0 = "0x322f0929c4625ed5bad873c95208d54e1c003b2d"  # gitleaks:allow -- public ERC-20 address
    token1 = "0xfe7e4b4850979ba7920ce786493b7371761f1e18"  # gitleaks:allow -- public ERC-20 address
    hook = "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544"
    legacy = {
        "id": pool_id,
        "address": MANAGER,
        "protocol": "v4",
        "token0": token0,
        "token1": token1,
        "symbol0": "TSLA",
        "symbol1": "LONGDOG",
        "decimals0": 18,
        "decimals1": 18,
        "fee_ppm": None,
        "tick_spacing": None,
        "hook": hook,
        "factory": MANAGER,
        "created_block": None,
        "source": "census",
        "metadata_json": {"dynamic_fee": True},
    }
    path = tmp_path / "market.sqlite"
    stored = MarketStore(path)
    stored.upsert_pools([legacy])
    stored.close()
    monkeypatch.setattr(
        market,
        "_load_universe",
        lambda: market._Universe(
            (), {}, {"v2": 0, "v3": 0, "v4": 0}, (), (), {},
        ),
    )

    catalog = market.MarketService(
        "http://127.0.0.1:1",
        data_dir=tmp_path,
        external_index=True,
        rpc=OfflineRpc(),
    )
    app = LPMarketService(
        catalog, "http://127.0.0.1:1", path, start=False,
    )
    try:
        assert catalog._pool_by_id(pool_id) is None

        app.indexer.request_pool_resolution(pool_id)

        durable = app.store.pool(pool_id)
        assert durable["fee_ppm"] is None
        assert durable["tick_spacing"] == 8
        assert json.loads(durable["metadata_json"]) == {
            "configured_fee": 0x800000,
            "dynamic_fee": True,
        }
        detail = catalog.detail(pool_id)
        assert detail["pool"]["pair"] == "TSLA/LONGDOG"
        listing = catalog.catalog({"q": "TSLA/LONGDOG"})
        assert listing["total"] == 1
        assert listing["rows"][0]["id"] == pool_id
        assert listing["rows"][0]["tick_spacing"] == 8

        app.store.rollback(0)
        assert app.store.pool(pool_id)["tick_spacing"] == 8
    finally:
        app.close()
        catalog.close()

    restored_catalog = market.MarketService(
        "http://127.0.0.1:1",
        data_dir=tmp_path,
        external_index=True,
        rpc=OfflineRpc(),
    )
    restored_app = LPMarketService(
        restored_catalog, "http://127.0.0.1:1", path, start=False,
    )
    try:
        restored = restored_catalog._pool_by_id(pool_id)
        assert restored is not None
        assert restored.tick_spacing == 8
        assert restored.dynamic_fee is True
        assert restored_catalog.detail(pool_id)["pool"]["pair"] == "TSLA/LONGDOG"
        assert restored_catalog.catalog({"q": pool_id})["rows"][0]["id"] == pool_id
    finally:
        restored_app.close()
        restored_catalog.close()



def test_market_observer_hooks_are_ordered_without_blocking_feed(tmp_path, monkeypatch):
    block_entered = threading.Event()
    release_block = threading.Event()
    events_seen = threading.Event()
    published = threading.Event()
    calls = []

    class Observer:
        def observe_current_block(self, observed):
            calls.append(("block", int(observed["number"], 16)))
            block_entered.set()
            if not release_block.wait(2):
                raise TimeoutError("test did not release block observer")

        def observe_current_events(self, observed, events):
            calls.append(("events", int(observed["number"], 16), len(events)))
            events_seen.set()

    app = LPMarketService(
        Observer(), "http://127.0.0.1:1", tmp_path / "market.sqlite", start=False,
    )
    try:
        block = header(100, int(time.time()) - 1)
        event = {**swap(block, V3, "v3"), "pool": pools()[0]}
        monkeypatch.setattr(
            app.indexer, "_decode_current", lambda *_args, **_kwargs: [event],
        )

        def publish():
            app.indexer._publish_current_block(block, [], source="test")
            published.set()

        publisher = threading.Thread(target=publish)
        publisher.start()
        assert block_entered.wait(1)
        assert published.wait(0.5), "market observer blocked current feed publication"
        assert not events_seen.is_set(), "event observer overtook blocked block observer"
        release_block.set()
        assert events_seen.wait(1)
        publisher.join(2)
        assert calls == [("block", 100), ("events", 100, 1)]
    finally:
        release_block.set()
        app.close()



@pytest.mark.parametrize(("matching_key", "delivery"), [
    (True, "late"), (False, "late"), (True, "initial"), (True, "inspector"),
    (True, "transaction"), (False, "transaction"),
    (True, "inspector_transaction"),
])
def test_cold_v4_activity_resolves_only_a_complete_verified_pool_key(
    tmp_path, monkeypatch, matching_key, delivery,
):
    from eth_utils import keccak
    from rhpools import workbench_market as market

    class OfflineRpc:
        def call(self, *_args):
            raise market.RpcError("test state provider offline")

        def close(self):
            pass

    universe = market._Universe((), {}, {"v2": 0, "v3": 0, "v4": 0}, (), (), {})
    monkeypatch.setattr(market, "_load_universe", lambda: universe)
    catalog = market.MarketService(
        "http://127.0.0.1:1", data_dir=tmp_path,
        external_index=True, rpc=OfflineRpc(),
    )
    app = LPMarketService(catalog, "http://127.0.0.1:1", tmp_path / "market.sqlite", start=False)
    indexer = app.indexer
    release = threading.Event()
    block = header(100, int(time.time()))
    from_input = delivery in {"transaction", "inspector_transaction"}
    inspector = delivery in {"inspector", "inspector_transaction"}
    current = header(101, int(time.time()) + 1) if delivery == "inspector_transaction" else block

    def word(value):
        return f"{value & ((1 << 256) - 1):064x}"

    token0, token1 = sorted((TOKEN, USDG))
    raw_key = "".join(word(value) for value in (int(token0, 16), int(token1, 16), 3000, 60, 0))
    pool_id = "0x" + keccak(bytes.fromhex(raw_key)).hex()
    log = {
        "address": MANAGER, "blockNumber": "0x64", "blockHash": block["hash"],
        "transactionHash": "0x" + "89" * 32, "transactionIndex": "0x0", "logIndex": "0x1",
        "topics": [V4_MODIFY_LIQUIDITY_TOPIC, pool_id, "0x" + word(int(TOKEN, 16))],
        "data": "0x" + "".join(word(value) for value in (-60, 60, 100, 7)),
    }
    transfers = [{
        **log, "address": token, "logIndex": hex(index + 2),
        "topics": [TRANSFER_TOPIC, "0x" + word(int(TOKEN, 16)),
                   "0x" + word(int(MANAGER, 16))],
        "data": "0x" + word(100),
    } for index, token in enumerate((token0, token1))]
    receipt = {
        "transactionHash": log["transactionHash"], "blockHash": block["hash"],
        "logs": [log, *transfers],
    }
    # A router supplies reversed currencies, two unrelated addresses, then
    # the fee/spacing/hook tuple. It does not embed a contiguous PoolKey.
    transaction = {
        "hash": log["transactionHash"], "blockHash": block["hash"], "blockNumber": block["number"],
        "input": "0x12345678" + "".join(word(value) for value in (
            int(token1, 16), int(token0, 16), int("77" * 20, 16), int("88" * 20, 16),
            3000 if matching_key else 3001, 60, 0,
        )),
    }

    def rpc(method, params):
        if method == "eth_getTransactionReceipt":
            return receipt if from_input else None
        if method == "eth_getTransactionByHash":
            return transaction if from_input else None
        if method == "eth_getBlockByNumber":
            return block
        if method != "eth_call":
            raise AssertionError(method)
        if not release.wait(3):
            raise TimeoutError("test did not release PoolKey lookup")
        expected = {
            "to": "0x58daec3116aae6d93017baaea7749052e8a04fa7",
            "data": "0x86b6be7d" + pool_id[2:52] + "00" * 7,
        }
        if params != [expected, current["number"]]:
            raise ValueError("invalid block-pinned poolKeys request")
        if from_input:
            return "0x" + "00" * 160
        return "0x" + (raw_key if matching_key else raw_key[:-1] + "1")

    monkeypatch.setattr(indexer._clients["pool"], "call", rpc)
    monkeypatch.setattr(indexer._clients["receipt"], "call", rpc)
    try:
        cursor = indexer.feed_updates()
        indexer._publish_current_block(
            current, [log] if delivery == "initial" else (), source="test",
        )
        if delivery in {"late", "transaction"}:
            indexer._publish_late_current_logs(100, [log], source="test")
        elif inspector:
            indexer.request_pool_resolution(pool_id, log["transactionHash"] if from_input else "")
        first = app.stream_updates({"kind": "lp"}, cursor["sequence"], cursor["feed_epoch"])
        initial = [row for event in first["events"] for row in event["data"].get("rows", [])]
        if not inspector:
            assert initial[0]["pool_id"] == pool_id
        with pytest.raises(ValueError, match="unknown pool id"):
            catalog.detail(pool_id)
        release.set()
        indexer._current_pool_executor.shutdown(wait=True)
        late = app.stream_updates({"kind": "lp"}, first["sequence"], first["feed_epoch"])
        rows = [row for event in late["events"] for row in event["data"].get("rows", [])]
        if matching_key:
            assert catalog.detail(pool_id)["pool"]["id"] == pool_id
            if inspector:
                assert initial == rows == []  # Lookup must not invent pool activity.
            else:
                resolved = [row for row in rows if row.get("token0", {}).get("address") == token0]
                assert resolved[-1]["token1"]["address"] == token1
                # A bounded indexer cache may evict a still-valid catalog pool.
                with indexer._cache_lock:
                    indexer._pool_cache.clear()
                next_block = header(101, int(time.time()) + 1)
                indexer._publish_current_block(next_block, [{
                    **log, "blockNumber": "0x65", "blockHash": next_block["hash"],
                }], source="test")
                replay = app.stream_updates({"kind": "lp"}, late["sequence"], late["feed_epoch"])
                replay_rows = [
                    row for event in replay["events"] for row in event["data"].get("rows", [])
                ]
                assert replay_rows[-1]["token0"]["address"] == token0
                assert replay_rows[-1]["token1"]["address"] == token1
        else:
            with pytest.raises(ValueError, match="unknown pool id"):
                catalog.detail(pool_id)
            assert not any(row.get("token0", {}).get("address") for row in rows)
    finally:
        release.set()
        app.close()
        catalog.close()


def test_slow_pool_resolution_does_not_block_known_current_activity(tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    entered = threading.Event()
    release = threading.Event()
    returned = threading.Event()
    indexer = app.indexer
    block = header(100, int(time.time()))
    unknown = "0x" + "45" * 20
    transaction = "0x" + "89" * 32
    base = {
        "blockNumber": "0x64", "blockHash": block["hash"],
        "transactionHash": transaction, "transactionIndex": "0x0",
    }

    def word(value):
        return f"{value & ((1 << 256) - 1):064x}"

    logs = [{
        **base, "address": "0x58daec3116aae6d93017baaea7749052e8a04fa7",
        "logIndex": "0x0", "data": "0x",
        "topics": [TRANSFER_TOPIC, "0x" + word(0),
                   "0x" + word(int(TOKEN, 16)), "0x" + word(7)],
    }, {
        **base, "address": unknown, "logIndex": "0x1",
        "topics": [V3_SWAP_TOPIC, "0x" + word(int(TOKEN, 16)),
                   "0x" + word(int(USDG, 16))],
        "data": "0x" + "".join(word(value) for value in (1, -1, 1 << 96, 100, 0)),
    }]

    def resolve(*_args):
        with indexer._identity_resolve_lock:
            entered.set()
            if not release.wait(3):
                raise TimeoutError("test did not release identity resolution")
            indexer._remember_pool({
                **pools()[0], "id": unknown, "address": unknown,
                "metadata_json": {"discovery_basis": "factory_creation_event"},
            })

    def publish():
        try:
            indexer._publish_late_current_logs(100, logs, source="test")
        finally:
            returned.set()

    monkeypatch.setattr(indexer, "_resolve_unknown_pools", resolve)
    indexer._publish_current_block(block, (), source="test")
    cursor = indexer.feed_updates()
    publisher = threading.Thread(target=publish)
    publisher.start()
    duplicate = None
    try:
        assert returned.wait(1), "identity RPC blocked current event delivery"
        assert entered.wait(1)
        first = app.stream_updates({"kind": "all"}, cursor["sequence"], cursor["feed_epoch"])
        rows = [row for event in first["events"] for row in event["data"].get("rows", [])]
        assert [row["kind"] for row in rows] == ["transfer"]
        returned.clear()
        duplicate = threading.Thread(target=publish)
        duplicate.start()
        assert returned.wait(1), "pending identity RPC blocked the receive loop"
        release.set()
        indexer.wait_feed(first["sequence"], timeout=2)
        late = app.stream_updates({"kind": "all"}, first["sequence"], first["feed_epoch"])
        rows = [row for event in late["events"] for row in event["data"].get("rows", [])]
        assert [(row["pool_id"], row["kind"]) for row in rows] == [(unknown, "swap")]
    finally:
        release.set()
        publisher.join(timeout=3)
        if duplicate is not None:
            duplicate.join(timeout=3)
        app.close()


def test_live_cursor_does_not_wait_for_archival_pool_metadata(tmp_path, monkeypatch):
    known = {**pools()[0], "symbol0": None, "symbol1": None,
             "decimals0": None, "decimals1": None, "source": "census"}

    class Catalog:
        universe = type("Universe", (), {"tokens": {}})()

        @staticmethod
        def _pool_by_id(pool_id):
            return known if pool_id == V3 else None

    app = LPMarketService(Catalog(), "http://127.0.0.1:1",
                          tmp_path / "market.sqlite", start=False)
    try:
        now = int(time.time()) - 1
        prior = header(99, now - 1)
        current = header(100, now)
        app.store.ingest([prior], [], lane="live", cursor={
            "from_block": 99, "to_block": 99, "block_number": 99,
            "block_hash": prior["hash"], "timestamp": now - 1,
        })

        def topic_address(address):
            return "0x" + address[2:].rjust(64, "0")

        def word(value):
            return f"{value & ((1 << 256) - 1):064x}"

        sender = "0x" + "56" * 20
        recipient = "0x" + "67" * 20
        new_pool = "0x" + "45" * 20
        tx = "0x" + "89" * 32
        creation = {
            "address": UNISWAP_V3_FACTORY, "blockNumber": "0x64",
            "blockHash": current["hash"], "transactionHash": tx,
            "transactionIndex": "0x0", "logIndex": "0x0",
            "topics": [V3_POOL_CREATED_TOPIC, topic_address(TOKEN),
                       topic_address(USDG), "0x" + word(3_000)],
            "data": "0x" + word(60) + word(int(new_pool, 16)),
        }

        def swap_log(pool_id, log_index):
            return {
                "address": pool_id, "blockNumber": "0x64",
                "blockHash": current["hash"], "transactionHash": tx,
                "transactionIndex": "0x0", "logIndex": hex(log_index),
                "topics": [V3_SWAP_TOPIC, topic_address(sender),
                           topic_address(recipient)],
                "data": "0x" + "".join(word(value) for value in (
                    100_000_000, -101_000_000, 1 << 96, 1_000_000, 0,
                )),
            }

        logs = [creation, swap_log(new_pool, 1), swap_log(V3, 2)]
        indexer = app.indexer
        monkeypatch.setattr(indexer._clients["live"], "call",
                            lambda method, _params: "0x64" if method == "eth_blockNumber" else None)
        monkeypatch.setattr(indexer, "_block",
                            lambda _lane, number: current if number == 100 else prior)
        monkeypatch.setattr(indexer, "_fetch_interval",
                            lambda _lane, _start, _end: (logs, {100: current}))
        monkeypatch.setattr(
            indexer, "_rpc_batch",
            lambda *_args, **_kwargs: pytest.fail("live decode attempted archival eth_call"),
        )

        assert indexer._scan_live_once() is True
        cursor = app.store.cursor("live")
        rows = app.store.read().execute(
            "SELECT pool_id,kind,owner,price0_usd FROM events "
            "WHERE block_number=100 ORDER BY log_index"
        ).fetchall()
        assert cursor["block_number"] == 100
        assert [(row["pool_id"], row["kind"]) for row in rows] == [
            (new_pool, "create"), (new_pool, "swap"), (V3, "swap"),
        ]
        assert all(row["owner"] is None and row["price0_usd"] is None for row in rows)
        assert app.store.status()["pending_metadata"] >= 2
    finally:
        app.close()
