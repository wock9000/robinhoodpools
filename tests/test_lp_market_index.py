"""Focused scanner throughput and canonical-safety regressions."""
from __future__ import annotations

import json
import logging
import threading
import time
from types import SimpleNamespace
import pytest

from rhpools.lp_market_index import (
    FACTORY_SELECTOR,
    FEED_SECONDARY_MAX_SECONDS,
    HEAD_REPLAY_LIMIT,
    FEE_SELECTOR,
    GET_PAIR_SELECTOR,
    TICK_SPACING_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    MAX_LOGS_PER_RESPONSE,
    LIVE_MAX_STORE_LOGS,
    REPROJECT_MAX_STORE_SECONDS,
    TOKEN_METADATA_INVALID_PREFIX,
    TOKEN_METADATA_INVALID_RECHECK_S,
    MarketIndexer,
    RpcError,
)
from rhpools.lp_market_store import CanonicalConflict, MarketStore
from rhpools.lp_market_protocols import (
    LIQUIDITY_SELECTOR,
    MODIFY_LIQUIDITY_SELECTOR,
    SWAP_SELECTOR,
    POSITIONS_SELECTOR,
    SLOT0_SELECTOR,
    V2_FACTORIES,
    V2_SYNC_TOPIC,
    V3_MINT_TOPIC,
    POOL_MANAGER,
    TRANSFER_TOPIC,
    V4_DONATE_TOPIC,
    V4_MODIFY_LIQUIDITY_TOPIC,
    V4_SWAP_TOPIC,
)


ZERO_HASH = "0x" + "00" * 32


def header(number: int, *, parent: str | None = None) -> dict[str, str]:
    return {
        "number": hex(number),
        "hash": "0x" + f"{number:064x}",
        "parentHash": parent or ("0x" + f"{max(0, number - 1):064x}"),
        "timestamp": hex(1_700_000_000 + number),
    }


class StaticRpc:
    def __init__(self, head: int = 0) -> None:
        self.head = head

    def call(self, method, params):
        if method == "eth_blockNumber":
            return hex(self.head)
        if method == "eth_getBlockByNumber":
            return header(int(params[0], 16))
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_getLogs":
            return []
        raise AssertionError(method)

    def batch(self, calls, **_kwargs):
        return [self.call(method, params) for method, params in calls]


class Market:
    universe = SimpleNamespace(tokens={})

    @staticmethod
    def _pool_by_id(_pool_id):
        return None


def indexer(store: MarketStore, rpc=None, **kwargs) -> MarketIndexer:
    return MarketIndexer(
        store, Market(), "http://unused.invalid", rpc=rpc or StaticRpc(),
        history_disk_reserve_bytes=0, **kwargs,
    )


def test_pool_lookup_work_is_per_unique_candidate_not_per_log(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    calls = []
    monkeypatch.setattr(
        scanner, "_pool", lambda candidate: calls.append(candidate),
    )
    emitter = "0x" + "12" * 20
    pool_id = "0x" + "34" * 32
    logs = [
        {"address": emitter, "topics": [ZERO_HASH, pool_id]}
        for _ in range(1_000)
    ]
    try:
        assert scanner._pools_for_logs(logs) == {}
        assert calls == [emitter, pool_id, "0x" + pool_id[-40:]]
    finally:
        scanner.close()
        store.close()


def test_dense_live_interval_grows_and_reports_scan_work(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store, StaticRpc(355))
    anchor = header(99)
    store.ingest(
        [anchor], [], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    logs = [{} for _ in range(1_000)]
    boundaries = {100: header(100), 355: header(355)}
    monkeypatch.setattr(
        scanner, "_fetch_interval",
        lambda _lane, start, end: (logs, {start: boundaries[start], end: boundaries[end]}),
    )
    monkeypatch.setattr(scanner, "_decode", lambda *_args, **_kwargs: [])
    try:
        assert scanner._scan_live_once() is True
        status = scanner.runtime_status()["live_scan"]
        assert status["blocks"] == 256
        assert status["logs"] == 1_000
        assert status["requested_chunk"] == 256
        assert status["next_chunk"] == 512
        assert status["to_block"] == 355
        assert status["blocks_per_second"] > 0
        assert set(("fetch_seconds", "decode_seconds", "store_seconds")) <= set(status)
    finally:
        scanner.close()
        store.close()


def test_dense_live_fetch_commits_a_bounded_whole_block_prefix(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store, StaticRpc(355))
    anchor = header(99)
    store.ingest(
        [anchor], [], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    per_block = LIVE_MAX_STORE_LOGS // 2
    logs = [
        {"blockNumber": hex(number)}
        for number in (100, 101, 102)
        for _ in range(per_block)
    ]
    headers = {
        number: header(number)
        for number in (100, 101, 102, 355)
    }
    monkeypatch.setattr(
        scanner,
        "_fetch_interval",
        lambda _lane, _start, _end: (logs, headers),
    )
    monkeypatch.setattr(scanner, "_decode", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        scanner,
        "_queue_deferred_pool_identities",
        lambda *_args, **_kwargs: 0,
    )
    try:
        assert scanner._scan_live_once() is True
        scan = scanner.runtime_status()["live_scan"]
        assert store.cursor("live")["block_number"] == 101
        assert scan["to_block"] == 101
        assert scan["logs"] == LIVE_MAX_STORE_LOGS
        assert scan["fetched_to_block"] == 355
        assert scan["fetched_logs"] == 3 * per_block
        assert scan["next_chunk"] == 2
    finally:
        scanner.close()
        store.close()

@pytest.mark.parametrize("counts,first_low", [((2, 2, 2), 101), ((2, 2, 5), 102)])
def test_dense_history_commits_whole_suffix_without_skipping_older_blocks(
    monkeypatch, counts, first_low,
):
    import rhpools.lp_market_index as module

    monkeypatch.setattr(module, "HISTORY_MAX_STORE_LOGS", 4)
    store = MarketStore(":memory:")
    scanner = indexer(store, StaticRpc(355))
    store.ingest([header(355)], [], lane="live", cursor={
        "block_number": 355, "block_hash": header(355)["hash"],
    })
    store.ingest([header(103)], [], lane="history", cursor={
        "next_to": 102, "target_block": 100, "complete": False,
    })
    scanner._history_chunk = 3
    logs = [
        {"blockNumber": hex(number)}
        for number, count in zip((100, 101, 102), counts)
        for _ in range(count)
    ]
    monkeypatch.setattr(scanner, "_fetch_interval", lambda _lane, start, end: (
        [log for log in logs if start <= int(log["blockNumber"], 16) <= end],
        {number: header(number) for number in range(start, end + 1)},
    ))
    monkeypatch.setattr(scanner, "_decode", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        scanner, "_queue_deferred_pool_identities", lambda *_args, **_kwargs: 0,
    )
    try:
        assert scanner._scan_history_once() is True
        assert store.cursor("history")["low_block"] == first_low
        assert store.cursor("history")["next_to"] == first_low - 1
        assert store.cursor("history")["complete"] is False
        for _ in range(2):
            if store.cursor("history")["complete"]:
                break
            scanner._scan_history_once()
        cursor = store.cursor("history")
        assert cursor["next_to"] == 99
        assert cursor["complete"] is True
        coverage = store.status()["coverage"]["history"]
        assert (coverage["from_block"], coverage["to_block"]) == (100, 102)
    finally:
        scanner.close()
        store.close()

def test_live_chunk_sizing_excludes_writer_lock_wait(tmp_path, monkeypatch):
    store = MarketStore(tmp_path / "market.sqlite")
    scanner = indexer(store, StaticRpc(355))
    anchor = header(99)
    store.ingest(
        [anchor], [], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    boundaries = {100: header(100), 355: header(355)}
    monkeypatch.setattr(
        scanner, "_fetch_interval",
        lambda _lane, start, end: ([], {start: boundaries[start], end: boundaries[end]}),
    )
    monkeypatch.setattr(
        "rhpools.lp_market_index.MAX_INTERVAL_STORE_SECONDS", 0.1,
    )
    held = threading.Event()
    release = threading.Event()

    def hold_writer():
        with store.transaction():
            held.set()
            release.wait(2)

    writer = threading.Thread(target=hold_writer)
    writer.start()
    try:
        assert held.wait(1)
        timer = threading.Timer(0.3, release.set)
        timer.start()
        assert scanner._scan_live_once() is True
        timer.join(1)
        scan = scanner.runtime_status()["live_scan"]
        assert scan["store_lock_wait_seconds"] >= 0.15
        assert scan["next_chunk"] == 512
    finally:
        release.set()
        writer.join(2)
        scanner.close()
        store.close()


def test_live_parent_mismatch_never_advances_cursor(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store, StaticRpc(100))
    anchor = header(99)
    store.ingest(
        [anchor], [], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    bad = header(100, parent=ZERO_HASH)
    recovered = []
    monkeypatch.setattr(
        scanner, "_fetch_interval", lambda *_args: ([], {100: bad}),
    )
    monkeypatch.setattr(
        scanner, "_recover_reorg",
        lambda cursor, reason, **_kwargs: recovered.append(reason),
    )
    try:
        assert scanner._scan_live_once() is True
        assert recovered == ["block 100 does not extend live cursor"]
        assert store.cursor("live")["block_number"] == 99
    finally:
        scanner.close()
        store.close()


def test_enrichment_rpc_failure_retries_without_dropping_job():
    class FailingReceiptRpc(StaticRpc):
        def call(self, method, params):
            if method == "eth_getTransactionReceipt":
                raise RpcError("receipt provider unavailable")
            return super().call(method, params)

    store = MarketStore(":memory:")
    scanner = indexer(store, FailingReceiptRpc())
    block = header(10)
    pending_event = {
        "block_number": 10,
        "block_hash": block["hash"],
        "tx_hash": "0x" + "ab" * 32,
        "tx_index": 0,
        "log_index": 0,
        "timestamp": int(block["timestamp"], 16),
        "pool_id": None,
        "protocol": "v3",
        "kind": "add",
        "owner": "0x" + "11" * 20,
        "data": {},
    }
    store.ingest([block], [pending_event])
    try:
        before = time.time()
        assert scanner._enrich_once() is True
        row = store.read().execute(
            "SELECT attempts,next_attempt,last_error FROM pending_enrichment",
        ).fetchone()
        assert row["attempts"] == 1
        assert before < row["next_attempt"] <= before + 301
        assert "receipt provider unavailable" in row["last_error"].lower()
        assert "enrichment" in scanner.runtime_status()["errors"]
    finally:
        scanner.close()
        store.close()

def test_unresolved_v3_factory_pool_enriches_without_batch_error():
    from eth_abi import encode

    from rhpools import _mc

    pool_address = "0x" + "34" * 20
    owner = "0x" + "12" * 20
    block = header(10)
    tx_hash = "0x" + "9a" * 32
    lower, upper = -60, 60
    mint_log = {
        "address": pool_address,
        "blockNumber": "0xa",
        "blockHash": block["hash"],
        "transactionHash": tx_hash,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "topics": [
            V3_MINT_TOPIC,
            "0x" + f"{int(owner, 16):064x}",
            "0x" + f"{lower & ((1 << 256) - 1):064x}",
            "0x" + f"{upper:064x}",
        ],
        "data": "0x" + encode(
            ["address", "uint128", "uint256", "uint256"], [owner, 100, 25, 50],
        ).hex(),
    }
    receipt = {
        "transactionHash": tx_hash,
        "blockNumber": "0xa",
        "blockHash": block["hash"],
        "transactionIndex": "0x0",
        "from": owner,
        "status": "0x1",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "logs": [mint_log],
    }

    def words(*values: int) -> str:
        return "0x" + "".join(f"{value & ((1 << 256) - 1):064x}" for value in values)

    slot0_result = words(1 << 96, 60, 0, 1, 1, 0, 1)
    liquidity_result = words(123)
    position_result = words(0, 0, 0, 0, 0)

    class CensusCatalogRpc(StaticRpc):
        def call(self, method, params):
            if method == "eth_getTransactionReceipt":
                return dict(receipt)
            if method == "eth_call":
                call_data = params[0]
                if call_data.get("to") == _mc.MULTICALL3:
                    return None
                data = str(call_data.get("data") or "")
                if data.startswith(SLOT0_SELECTOR):
                    return slot0_result
                if data.startswith(LIQUIDITY_SELECTOR):
                    return liquidity_result
                if data.startswith(POSITIONS_SELECTOR):
                    return position_result
                raise AssertionError(("eth_call", data[:10]))
            return super().call(method, params)

    class CensusMarket:
        universe = SimpleNamespace(tokens={})

        @staticmethod
        def _pool_by_id(pool_id):
            if str(pool_id).lower() == pool_address:
                return {
                    "id": pool_address,
                    "address": pool_address,
                    "protocol": "v3",
                    "source": "census",
                    "token0": "0x" + "aa" * 20,
                    "token1": "0x" + "bb" * 20,
                }
            return None



    store = MarketStore(":memory:")
    scanner = MarketIndexer(
        store, CensusMarket(), "http://unused.invalid", rpc=CensusCatalogRpc(),
        history_disk_reserve_bytes=0,
    )
    pending_event = {
        "block_number": 10,
        "block_hash": block["hash"],
        "tx_hash": tx_hash,
        "tx_index": 0,
        "log_index": 0,
        "timestamp": int(block["timestamp"], 16),
        "pool_id": pool_address,
        "protocol": "v3",
        "kind": "add",
        "owner": owner,
        "data": {},
    }
    store.ingest([block], [pending_event])
    try:
        pending = store.read().execute(
            "SELECT 1 FROM pending_enrichment WHERE tx_hash=?", (tx_hash,),
        ).fetchone()
        assert pending is not None
        deadline = time.monotonic() + 10
        while pending is not None and time.monotonic() < deadline:
            scanner._enrich_once()
            time.sleep(0.02)
            pending = store.read().execute(
                "SELECT 1 FROM pending_enrichment WHERE tx_hash=?", (tx_hash,),
            ).fetchone()
        assert pending is None
        assert "enrichment" not in scanner.runtime_status()["errors"]
        data = json.loads(store.read().execute(
            "SELECT data FROM events WHERE tx_hash=? AND kind='add'", (tx_hash,),
        ).fetchone()["data"])
        state = data["pool_state_before"]
        assert state["source"] == "factory_unresolved"
        assert state["pinned_block"] == 9
        assert state["liquidity"] == "123"
    finally:
        scanner.close()
        store.close()

def test_head_feed_secondary_session_is_bounded_and_fails_back(
    monkeypatch, caplog,
):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    primary, secondary = "wss://primary.example", "wss://secondary.example"
    scanner._head_wss_urls = (primary, secondary)
    calls: list[tuple[str, float | None]] = []
    started = time.monotonic()

    def fake_head_once(url, *, session_deadline=None):
        calls.append((url, session_deadline))
        if len(calls) >= 8:
            scanner._stop.set()
        if session_deadline is None:
            raise RpcError("primary head feed unavailable")
        return None

    monkeypatch.setattr(scanner, "_head_wss_once", fake_head_once)
    monkeypatch.setattr(scanner, "_reconcile_current_head", lambda *a, **k: None)
    with caplog.at_level(logging.INFO, logger="rhpools.lp_market_index"):
        scanner._head_run()
    try:
        assert [url for url, _ in calls] == [primary, secondary] * 4
        for url, deadline_value in calls:
            if url == primary:
                assert deadline_value is None
            else:
                assert (
                    started + FEED_SECONDARY_MAX_SECONDS - 5
                    <= deadline_value
                    <= started + FEED_SECONDARY_MAX_SECONDS + 5
                )
        assert "failing back to the primary feed" in caplog.text
        assert "rpc_poll:head" not in json.dumps(scanner.runtime_status())
    finally:
        scanner.close()
        store.close()


def test_activity_feed_secondary_session_is_bounded_and_fails_back(
    monkeypatch, caplog,
):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    primary, secondary = "wss://primary.example", "wss://secondary.example"
    scanner._head_wss_urls = (primary, secondary)
    calls: list[tuple[str, float | None]] = []
    started = time.monotonic()

    def fake_activity_once(url, *, session_deadline=None):
        calls.append((url, session_deadline))
        if len(calls) >= 6:
            scanner._stop.set()
        if session_deadline is None:
            raise RpcError("primary activity feed unavailable")
        return None

    monkeypatch.setattr(scanner, "_activity_wss_once", fake_activity_once)
    with caplog.at_level(logging.INFO, logger="rhpools.lp_market_index"):
        scanner._activity_run()
    try:
        assert [url for url, _ in calls] == [primary, secondary] * 3
        for url, deadline_value in calls:
            if url == primary:
                assert deadline_value is None
            else:
                assert (
                    started + FEED_SECONDARY_MAX_SECONDS - 5
                    <= deadline_value
                    <= started + FEED_SECONDARY_MAX_SECONDS + 5
                )
        assert "failing back to the primary feed" in caplog.text
    finally:
        scanner.close()
        store.close()


def test_activity_backlog_waits_for_durable_coverage_and_recovers(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    pending = {
        number: [{"blockNumber": hex(number)}]
        for number in range(1, HEAD_REPLAY_LIMIT * 2 + 2)
    }
    first_seen = {number: 0.0 for number in pending}
    try:
        with pytest.raises(RpcError, match="outside canonical replay"):
            scanner._flush_activity_logs(
                pending, first_seen, max(pending), source="test", force=True,
            )
        status = scanner.runtime_status()
        assert len(pending) == HEAD_REPLAY_LIMIT * 2 + 1
        assert "outside canonical replay" in status["errors"]["activity_feed"]
        assert status.get("activity_feed_stale_blocks_discarded", 0) == 0
        assert status["activity_feed_backpressure_disconnects"] == 1
        assert status["activity_feed_recovery_through"] == (
            HEAD_REPLAY_LIMIT * 2 + 1
        )

        durable = header(HEAD_REPLAY_LIMIT * 2 + 1)
        store.ingest([durable], [], lane="live", cursor={
            "block_number": HEAD_REPLAY_LIMIT * 2 + 1,
            "block_hash": durable["hash"],
            "timestamp": int(durable["timestamp"], 16),
        })
        scanner._flush_activity_logs(
            pending, first_seen, max(pending), source="test", force=True,
        )
        status = scanner.runtime_status()
        assert len(pending) == HEAD_REPLAY_LIMIT
        assert status["activity_feed_stale_blocks_discarded"] == (
            HEAD_REPLAY_LIMIT + 1
        )
        assert status["activity_feed_last_discard"]["durable_through"] == (
            HEAD_REPLAY_LIMIT * 2 + 1
        )
        assert "already covered by durable live index" in (
            status["errors"]["activity_feed"]
        )

        scanner._flush_activity_logs(
            pending, first_seen, max(pending), source="test", force=True,
        )
        durable_recovery = scanner.runtime_status()
        assert "activity_feed" not in durable_recovery["errors"]
        assert durable_recovery["activity_feed_stale_blocks_discarded"] == (
            HEAD_REPLAY_LIMIT + 1
        )
        assert durable_recovery["activity_feed_recovered_at"] > 0

        current = header(HEAD_REPLAY_LIMIT * 2 + 2)
        with scanner._feed_condition:
            scanner._observed_last_header = current
            scanner._observed_blocks[HEAD_REPLAY_LIMIT * 2 + 2] = (
                current, [],
            )
        pending[HEAD_REPLAY_LIMIT * 2 + 2] = [{
            "blockNumber": current["number"], "blockHash": current["hash"],
        }]
        first_seen[HEAD_REPLAY_LIMIT * 2 + 2] = 0.0

        def fail_publish(*_args, **_kwargs):
            raise RpcError("decode failed")

        monkeypatch.setattr(
            scanner, "_publish_late_current_logs", fail_publish,
        )
        scanner._flush_activity_logs(
            pending, first_seen, HEAD_REPLAY_LIMIT * 2 + 2,
            source="test", force=True,
        )
        assert HEAD_REPLAY_LIMIT * 2 + 2 in pending
        assert scanner.runtime_status()["errors"]["activity_feed"] == "decode failed"

        monkeypatch.setattr(
            scanner, "_publish_late_current_logs",
            lambda *_args, **_kwargs: 1,
        )
        scanner._flush_activity_logs(
            pending, first_seen, HEAD_REPLAY_LIMIT * 2 + 2,
            source="test", force=True,
        )
        recovered = scanner.runtime_status()
        assert HEAD_REPLAY_LIMIT * 2 + 2 not in pending
        assert "activity_feed" not in recovered["errors"]
        assert recovered["activity_feed_stale_blocks_discarded"] == (
            HEAD_REPLAY_LIMIT + 1
        )
        assert recovered["activity_feed_recovered_at"] > 0
    finally:
        scanner.close()
        store.close()


def test_history_progress_is_independent_of_unrunnable_enrichment(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    scanner._history_verified = True
    anchor = header(500)
    store.ingest(
        [anchor], [], lane="history",
        cursor={
            "next_to": 499,
            "low_block": 500,
            "block_hash": anchor["hash"],
            "target_block": 400,
            "target_timestamp": int(header(400)["timestamp"], 16),
            "target_pending": False,
            "origin_head": 500,
            "complete": False,
            "has_coverage": False,
        },
    )
    future = time.time() + 86_400
    with store.transaction() as connection:
        connection.executemany(
            "INSERT INTO pending_enrichment"
            "(tx_hash,block_number,block_hash,attempts,next_attempt,last_error,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            [
                (
                    "0x" + f"{offset + 1:064x}", 450, anchor["hash"], 10,
                    future, "trace RPC unavailable", time.time(), time.time(),
                )
                for offset in range(5_000)
            ],
        )
        store._set_metadata(connection, "pending_enrichment", 5_000)
    monkeypatch.setattr(
        scanner, "_fetch_interval",
        lambda _lane, start, end: ([], {start: header(start), end: header(end)}),
    )
    monkeypatch.setattr(scanner, "_decode", lambda *_args, **_kwargs: [])
    try:
        assert scanner._scan_history_once() is True
        cursor = store.cursor("history")
        assert 400 <= cursor["next_to"] < 499
        assert cursor["complete"] is False
        status = scanner.runtime_status()["history_scan"]
        assert status["blocks"] > 0
    finally:
        scanner.close()
        store.close()


def test_history_yields_to_urgent_live_debt_and_runs_near_head():
    store = MarketStore(":memory:")
    scanner = indexer(store, StaticRpc(5_000))
    scanner._history_verified = True
    anchor = header(500)
    store.ingest([anchor], [], lane="history", cursor={
        "next_to": 499, "low_block": 500, "block_hash": anchor["hash"],
        "target_block": 400, "target_timestamp": int(header(400)["timestamp"], 16),
        "target_pending": False, "origin_head": 500, "complete": False,
        "has_coverage": False,
    })
    store.ingest([anchor], [], lane="live", cursor={
        "block_number": 500, "block_hash": anchor["hash"],
        "timestamp": int(anchor["timestamp"], 16),
    })
    try:
        # A ten-second gap leaves room for archive progress.
        scanner._set_runtime(
            "head", head=510,
            head_timestamp=int(header(510)["timestamp"], 16),
        )
        assert scanner._scan_history_once() is True
        assert store.cursor("history")["next_to"] < 499
        assert scanner.runtime_status()["recent_catchup_priority"] is False
        # A gap several batches deep and older than the wall-clock window
        # hands the writer to live until it catches up.
        scanner._set_runtime(
            "head", head=5_000, head_timestamp=int(header(5_000)["timestamp"], 16),
        )
        before = store.cursor("history")["next_to"]
        assert scanner._scan_history_once() is False
        assert store.cursor("history")["next_to"] == before
        assert scanner.runtime_status()["recent_catchup_priority"] is True
        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] > 500
    finally:
        scanner.close()
        store.close()


def test_live_health_publishes_while_another_lane_holds_writer():
    store = MarketStore(":memory:")
    scanner = indexer(store)
    held = threading.Event()
    release = threading.Event()
    published = threading.Event()

    def hold_writer():
        with store.transaction():
            held.set()
            release.wait(5)

    def publish_head():
        scanner._set_runtime("head", head=200)
        published.set()

    writer = threading.Thread(target=hold_writer)
    publisher = threading.Thread(target=publish_head)
    writer.start()
    try:
        assert held.wait(1)
        publisher.start()
        assert published.wait(1)
        assert scanner.runtime_status()["head"] == 200
        assert not release.is_set()
    finally:
        release.set()
        writer.join(2)
        if publisher.ident is not None:
            publisher.join(2)
        scanner.close()
        store.close()


def test_history_defers_cold_pool_identity_to_current_canonical_state(monkeypatch):
    valid_pool = "0x" + "12" * 20
    spoof_pool = "0x" + "13" * 20
    token0 = "0x" + "21" * 20
    token1 = "0x" + "22" * 20
    factory = sorted(V2_FACTORIES)[0]
    valid_tx = "0x" + "31" * 32
    spoof_tx = "0x" + "32" * 32
    current_block = 100

    def word(address: str) -> str:
        return "0x" + address[2:].rjust(64, "0")

    class CurrentIdentityRpc(StaticRpc):
        def __init__(self) -> None:
            super().__init__(current_block)
            self.call_tags = []
            self.current_header_reads = 0

        def call(self, method, params):
            if method == "eth_getBlockByNumber":
                number = int(params[0], 16)
                if number == current_block:
                    self.current_header_reads += 1
                return header(number)
            if method == "eth_call":
                call, tag = params
                self.call_tags.append(tag)
                if tag != hex(current_block):
                    raise AssertionError("pruned historical state was requested")
                target = call["to"].lower()
                selector = call["data"][:10]
                if target == spoof_pool:
                    return word("0x" + "00" * 20)
                if target == valid_pool:
                    return {
                        FACTORY_SELECTOR: word(factory),
                        TOKEN0_SELECTOR: word(token0),
                        TOKEN1_SELECTOR: word(token1),
                        FEE_SELECTOR: "0x" + "00" * 32,
                        TICK_SPACING_SELECTOR: "0x" + "00" * 32,
                    }[selector]
                if target == factory and selector == GET_PAIR_SELECTOR:
                    return word(valid_pool)
                raise AssertionError((target, selector))
            return super().call(method, params)

    def sync_log(address: str, tx_hash: str) -> dict[str, object]:
        return {
            "address": address,
            "topics": [V2_SYNC_TOPIC],
            "data": "0x" + f"{123:064x}{456:064x}",
            "blockNumber": hex(9),
            "blockHash": header(9)["hash"],
            "transactionHash": tx_hash,
            "transactionIndex": "0x0",
            "logIndex": "0x0",
            "removed": False,
        }

    rpc = CurrentIdentityRpc()
    store = MarketStore(":memory:")
    scanner = indexer(store, rpc)
    scanner._history_verified = True
    anchor = header(10)
    store.ingest(
        [anchor], [], lane="history",
        cursor={
            "next_to": 9,
            "low_block": 10,
            "block_hash": anchor["hash"],
            "target_block": 9,
            "target_timestamp": int(header(9)["timestamp"], 16),
            "target_pending": False,
            "origin_head": 10,
            "complete": False,
            "has_coverage": False,
        },
    )
    logs = [
        sync_log(valid_pool, valid_tx),
        sync_log(spoof_pool, spoof_tx),
    ]
    monkeypatch.setattr(
        scanner, "_fetch_interval",
        lambda _lane, start, end: (
            logs, {start: header(start), end: header(end)},
        ),
    )
    try:
        assert scanner._scan_history_once() is True
        assert scanner.store.cursor("history")["complete"] is True
        assert rpc.call_tags == []
        queued = store.read().execute(
            "SELECT tx_hash,last_error FROM pending_enrichment "
            "ORDER BY tx_hash",
        ).fetchall()
        assert [row["tx_hash"] for row in queued] == [valid_tx, spoof_tx]
        assert all(
            row["last_error"].startswith("pool_identity_pending:")
            for row in queued
        )

        assert scanner._resolve_deferred_pool_identities_once() is True
        assert rpc.call_tags
        assert set(rpc.call_tags) == {hex(current_block)}

        event = store.read().execute(
            "SELECT pool_id,protocol,kind FROM events WHERE tx_hash=?",
            (valid_tx,),
        ).fetchone()
        assert tuple(event) == (valid_pool, "v2", "checkpoint")
        assert store.read().execute(
            "SELECT 1 FROM events WHERE tx_hash=?", (spoof_tx,),
        ).fetchone() is None
        assert store.pool(spoof_pool) is None

        pool = store.pool(valid_pool)
        metadata = json.loads(pool["metadata_json"])
        assert metadata["discovery_basis"] == (
            "pinned_factory_getPair_membership"
        )
        assert metadata["identity_state_basis"] == (
            "current_canonical_factory_membership"
        )
        assert metadata["identity_verified_block"] == current_block
        assert metadata["identity_verified_hash"] == header(current_block)["hash"]
        assert metadata["identity_observed_block"] == 9
        assert store.read().execute(
            "SELECT last_error FROM pending_enrichment WHERE tx_hash=?",
            (valid_tx,),
        ).fetchone()["last_error"] is None
        assert store.read().execute(
            "SELECT 1 FROM pending_enrichment WHERE tx_hash=?", (spoof_tx,),
        ).fetchone() is None
    finally:
        scanner.close()
        store.close()


@pytest.mark.parametrize("lane", ["live", "history"])
def test_scans_durably_replay_unregistered_v4_pool_keys(
    monkeypatch, lane,
):
    from eth_utils import keccak

    observed_number = 10 if lane == "live" else 9
    current_number = observed_number if lane == "live" else 100
    observed = header(observed_number)
    token0 = "0x" + "21" * 20
    token1 = "0x" + "22" * 20
    hook = "0x" + "44" * 20
    configured_fee = 0x800000
    spacing = 8
    custody = "0x" + "55" * 20
    tx_hash = "0x" + "61" * 32

    def word(value: int) -> str:
        return f"{value & ((1 << 256) - 1):064x}"

    raw_key = bytes.fromhex("".join(word(value) for value in (
        int(token0, 16), int(token1, 16), configured_fee, spacing,
        int(hook, 16),
    )))
    pool_id = "0x" + keccak(raw_key).hex()
    pool_log = {
        "address": POOL_MANAGER,
        "blockNumber": observed["number"],
        "blockHash": observed["hash"],
        "transactionHash": tx_hash,
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "topics": [
            V4_MODIFY_LIQUIDITY_TOPIC,
            pool_id,
            "0x" + word(int(custody, 16)),
        ],
        "data": "0x" + "".join(word(value) for value in (-10, 10, 100, 7)),
        "removed": False,
    }
    transfers = [
        {
            **pool_log,
            "address": token,
            "logIndex": hex(index + 2),
            "topics": [
                TRANSFER_TOPIC,
                "0x" + word(int(custody, 16)),
                "0x" + word(int(POOL_MANAGER, 16)),
            ],
            "data": "0x" + word(100),
        }
        for index, token in enumerate((token0, token1))
    ]
    receipt = {
        "transactionHash": tx_hash,
        "blockHash": observed["hash"],
        "logs": [pool_log, *transfers],
    }
    transaction = {
        "hash": tx_hash,
        "blockHash": observed["hash"],
        "blockNumber": observed["number"],
        # The currencies are intentionally reversed, so this is not a
        # contiguous PoolKey. The complete recovered key must still hash.
        "input": "0x12345678" + "".join(word(value) for value in (
            int(token1, 16), int(token0, 16), configured_fee, spacing,
            int(hook, 16),
        )),
    }

    class IdentityRpc(StaticRpc):
        def __init__(self):
            super().__init__(current_number)

        def call(self, method, params):
            if method == "eth_call":
                return "0x" + "00" * 160
            if method == "eth_getTransactionByHash":
                return transaction
            if method == "eth_getTransactionReceipt":
                return receipt
            return super().call(method, params)

    class PublishingMarket(Market):
        def __init__(self):
            self.published = []

        def register_index_pool(self, pool):
            self.published.append(dict(pool))

    store = MarketStore(":memory:")
    market = PublishingMarket()
    scanner = MarketIndexer(
        store, market, "http://unused.invalid", rpc=IdentityRpc(),
        history_disk_reserve_bytes=0,
    )
    if lane == "live":
        anchor = header(observed_number - 1)
        store.ingest([anchor], [], lane="live", cursor={
            "block_number": observed_number - 1,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        })
    else:
        scanner._history_verified = True
        anchor = header(observed_number + 1)
        store.ingest([anchor], [], lane="history", cursor={
            "next_to": observed_number,
            "low_block": observed_number + 1,
            "block_hash": anchor["hash"],
            "target_block": observed_number,
            "target_timestamp": int(observed["timestamp"], 16),
            "target_pending": False,
            "origin_head": observed_number + 1,
            "complete": False,
            "has_coverage": False,
        })
    monkeypatch.setattr(
        scanner, "_fetch_interval",
        lambda _lane, start, end: (
            [pool_log],
            {number: header(number) for number in {start, end, observed_number}},
        ),
    )
    try:
        scan = scanner._scan_live_once if lane == "live" else scanner._scan_history_once
        assert scan() is True
        queued = store.read().execute(
            "SELECT last_error FROM pending_enrichment WHERE tx_hash=?",
            (tx_hash,),
        ).fetchone()
        marker = scanner._pool_identity_marker(queued["last_error"])
        assert marker["addresses"] == [pool_id]
        assert store.pool(pool_id) is None
        # Simulate a database populated by an older indexer, before durable
        # V4 identity markers existed. Restart seeding must recover it too.
        with store.transaction() as connection:
            removed = connection.execute(
                "DELETE FROM pending_enrichment WHERE tx_hash=?",
                (tx_hash,),
            ).rowcount
            store._bump(connection, "pending_enrichment", -removed)
        assert scanner._pending_pool_identity_replays() == []
        # A restart also clears process-local identity suppression state.
        with scanner._identity_resolve_lock:
            scanner._identity_checked.clear()
        with scanner._current_receipt_lock:
            scanner._current_pool_failure_details[pool_id] = {
                "classification": "rpc",
                "error": "temporary state getter failure",
                "source": "test",
            }
            scanner._current_pool_failures[(pool_id, tx_hash)] = float("inf")
        scanner._publish_pool_resolution_failures()
        assert scanner.runtime_status()["pool_resolution_failure_count"] == 1

        assert scanner._resolve_deferred_pool_identities_once() is True
        pool = store.pool(pool_id)
        metadata = json.loads(pool["metadata_json"])
        assert (pool["token0"], pool["token1"]) == (token0, token1)
        assert pool["source"] == "transaction.PoolKey"
        assert metadata["configured_fee"] == configured_fee
        assert metadata["discovery_basis"] == "full_poolKey_hash"
        assert {
            row["address"] for row in store.pending_token_metadata()
        } == {token0, token1}
        assert store.read().execute(
            "SELECT COUNT(*) FROM pending_reprojection",
        ).fetchone()[0] == 1
        assert market.published[-1]["id"] == pool_id
        assert "pool_resolution" not in scanner.runtime_status()["errors"]

        tampered = {
            **pool,
            "id": "0x" + "99" * 32,
        }
        store.upsert_pools([tampered])
        market.published.clear()
        assert scanner._publish_stored_pool_page() is True
        assert [published["id"] for published in market.published] == [pool_id]
    finally:
        scanner.close()
        store.close()


@pytest.mark.parametrize("manager_method", ["modify", "swap", "donate"])
def test_poolkey_fallback_reads_bounded_nested_manager_trace(manager_method):
    from eth_utils import keccak

    observed = header(100)
    token0 = "0x" + "21" * 20
    token1 = "0x" + "22" * 20
    hook = "0x" + "44" * 20
    fee, spacing = 0x800000, 8
    tx_hash = "0x" + "61" * 32
    custody = "0x" + "55" * 20

    def word(value):
        return f"{value & ((1 << 256) - 1):064x}"

    key_words = (
        int(token0, 16), int(token1, 16), fee, spacing, int(hook, 16),
    )
    pool_id = "0x" + keccak(bytes.fromhex(
        "".join(word(value) for value in key_words)
    )).hex()
    if manager_method == "modify":
        topic = V4_MODIFY_LIQUIDITY_TOPIC
        event_values = (-10, 10, 100, 7)
    elif manager_method == "swap":
        topic = V4_SWAP_TOPIC
        event_values = (-100, 90, 1 << 96, 1_000_000, 0, 3000)
    else:
        topic = V4_DONATE_TOPIC
        event_values = (100, 200)
    pool_log = {
        "address": POOL_MANAGER,
        "blockNumber": observed["number"],
        "blockHash": observed["hash"],
        "transactionHash": tx_hash,
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "topics": [
            topic,
            pool_id,
            "0x" + word(int(custody, 16)),
        ],
        "data": "0x" + "".join(word(value) for value in event_values),
        "removed": False,
    }
    transaction = {
        "hash": tx_hash,
        "blockHash": observed["hash"],
        "blockNumber": observed["number"],
        "input": "0xdeadbeef",
    }
    receipt = {
        "transactionHash": tx_hash,
        "blockHash": observed["hash"],
        "logs": [pool_log],
    }
    if manager_method == "modify":
        manager_input = MODIFY_LIQUIDITY_SELECTOR + "".join(
            word(value) for value in (
                *key_words, -10, 10, 100, 7, 10 * 32, 0,
            )
        )
    elif manager_method == "swap":
        manager_input = SWAP_SELECTOR + "".join(
            word(value) for value in (
                *key_words, 1, -100, 1, 9 * 32, 0,
            )
        )
    else:
        donate_selector = "0x" + keccak(
            text=(
                "donate((address,address,uint24,int24,address),"
                "uint256,uint256,bytes)"
            )
        ).hex()[:8]
        manager_input = donate_selector + "".join(
            word(value) for value in (
                *key_words, 100, 200, 8 * 32, 0,
            )
        )
    manager_input += "00" * 32
    trace = {
        "type": "CALL",
        "to": "0x" + "99" * 20,
        "input": transaction["input"],
        "calls": [{
            "type": "CALL",
            "to": POOL_MANAGER,
            "input": manager_input,
            "output": "0x" + "00" * 64,
        }],
    }

    class TraceRpc(StaticRpc):
        def __init__(self):
            super().__init__(100)
            self.trace_error = None
            self.on_trace = None

        def call(self, method, params):
            if method == "eth_getTransactionByHash":
                return transaction
            if method == "eth_getTransactionReceipt":
                return receipt
            if method == "debug_traceTransaction":
                if self.trace_error is not None:
                    raise self.trace_error
                if self.on_trace is not None:
                    self.on_trace()
                return trace
            return super().call(method, params)

    store = MarketStore(":memory:")
    rpc = TraceRpc()
    scanner = indexer(store, rpc)
    with scanner._feed_condition:
        scanner._observed_blocks[100] = (observed, [])
    try:
        resolved, failures = scanner._resolve_current_v4_inputs({
            pool_id: ([pool_log], tx_hash),
        })
        assert failures == {}
        assert resolved[pool_id]["source"] == "trace.PoolKey"
        assert (resolved[pool_id]["token0"], resolved[pool_id]["token1"]) == (
            token0, token1,
        )

        rpc.trace_error = RpcError("trace RPC unavailable: Goldsky timeout")
        resolved, failures = scanner._resolve_current_v4_inputs({
            pool_id: ([pool_log], tx_hash),
        })
        assert resolved == {}
        assert "trace RPC unavailable: Goldsky timeout" in str(
            failures[pool_id]
        )

        rpc.trace_error = None

        def reorg_during_trace():
            replaced = dict(observed)
            replaced["hash"] = "0x" + "ff" * 32
            with scanner._feed_condition:
                scanner._observed_blocks[100] = (replaced, [])

        rpc.on_trace = reorg_during_trace
        resolved, failures = scanner._resolve_current_v4_inputs({
            pool_id: ([pool_log], tx_hash),
        })
        assert resolved == {}
        assert isinstance(failures[pool_id], CanonicalConflict)
    finally:
        scanner.close()
        store.close()


def test_interval_headers_and_logs_use_overlapping_bounded_lanes():
    header_started = threading.Event()
    log_started = threading.Event()
    created: dict[str, list[object]] = {}

    class Rpc:
        def __init__(self, lane: str, ordinal: int) -> None:
            self.lane = lane
            self.ordinal = ordinal

        def call(self, method, params):
            if method == "eth_getLogs":
                assert header_started.wait(1)
                log_started.set()
                return []
            if method == "eth_getBlockByNumber":
                return header(int(params[0], 16))
            if method == "eth_chainId":
                return hex(4663)
            raise AssertionError(method)

        def batch(self, calls):
            if self.lane == "live" and self.ordinal == 1:
                header_started.set()
                assert log_started.wait(1)
            return [self.call(method, params) for method, params in calls]

        def close(self):
            pass

    def factory(lane: str):
        ordinal = len(created.setdefault(lane, []))
        client = Rpc(lane, ordinal)
        created[lane].append(client)
        return client

    store = MarketStore(":memory:")
    scanner = indexer(store, factory)
    try:
        logs, headers = scanner._fetch_interval("live", 10, 20)
        assert logs == []
        assert set(headers) == {10, 20}
        assert scanner._fetch_metrics["live"]["header_log_overlap"] is True
    finally:
        scanner.close()
        store.close()


def test_event_headers_and_end_recheck_share_one_ordered_post_log_batch():
    class RecordingRpc(StaticRpc):
        def __init__(self):
            super().__init__()
            self.logs_complete = False
            self.header_batches = []

        def call(self, method, params):
            if method == "eth_getLogs":
                self.logs_complete = True
                if params[0].get("address"):
                    return []
                return [
                    {
                        "blockNumber": hex(number),
                        "blockHash": header(number)["hash"],
                        "transactionHash": "0x" + f"{number:064x}",
                        "transactionIndex": hex(index),
                        "logIndex": hex(index),
                    }
                    for index, number in enumerate((12, 11))
                ]
            return super().call(method, params)

        def batch(self, calls):
            specifications = list(calls)
            numbers = tuple(
                int(params[0], 16) for _method, params in specifications
            )
            self.header_batches.append((self.logs_complete, numbers))
            return [header(number) for number in numbers]

    rpc = RecordingRpc()
    store = MarketStore(":memory:")
    scanner = indexer(store, rpc)
    try:
        logs, headers = scanner._fetch_interval("live", 10, 20)
        assert [int(log["blockNumber"], 16) for log in logs] == [11, 12]
        assert set(headers) == {10, 11, 12, 20}
        assert rpc.header_batches == [
            (False, (10, 20)),
            (True, (11, 12, 20)),
        ]
        assert scanner._fetch_metrics["live"]["event_header_count"] == 2
        assert scanner._fetch_metrics["live"]["end_verify_batched"] is True
    finally:
        scanner.close()
        store.close()


def archive_factory(created: dict[str, list], *, fork_at: int | None = None):
    """Lane clients: the archive answers logs and event headers, providers answer boundaries."""

    class LaneRpc(StaticRpc):
        def __init__(self, lane: str) -> None:
            super().__init__(head=10_000)
            self.lane = lane
            self.log_queries = []

        def call(self, method, params):
            if method == "eth_getLogs":
                assert self.lane == "archive", "provider log sources must stay idle"
                low, high = int(params[0]["fromBlock"], 16), int(params[0]["toBlock"], 16)
                self.log_queries.append((low, high))
                if params[0].get("address"):
                    return []
                if high - low + 1 > 4:
                    # A page over the safety cap must be split, not trusted.
                    return [{"blockNumber": hex(low)}] * MAX_LOGS_PER_RESPONSE
                return [
                    {
                        "blockNumber": hex(number),
                        "blockHash": header(number)["hash"],
                        "transactionHash": "0x" + f"{number:064x}",
                        "transactionIndex": "0x0",
                        "logIndex": "0x0",
                    }
                    for number in range(low, high + 1)
                ]
            if method == "eth_getBlockByNumber" and self.lane == "archive":
                number = int(params[0], 16)
                if number == fork_at:
                    return header(number, parent=header(number - 1)["hash"]) | {
                        "hash": "0x" + "ff" * 32,
                    }
            return super().call(method, params)

        def batch(self, calls):
            return [self.call(method, params) for method, params in calls]

        def close(self):
            pass

    def factory(lane: str):
        client = LaneRpc(lane)
        created.setdefault(lane, []).append(client)
        return client

    return factory


def test_archive_interval_pages_logs_locally_and_frames_them_with_canonical_headers():
    created: dict[str, list] = {}
    store = MarketStore(":memory:")
    scanner = indexer(store, archive_factory(created))
    # 1000 logs per block sizes pages at 8 blocks; the fake overflows any page
    # wider than 4 blocks, so each page splits once more.
    scanner._history_density = 1_000.0
    try:
        logs, headers = scanner._fetch_interval("history", 100, 115)
        assert [int(log["blockNumber"], 16) for log in logs] == list(range(100, 116))
        assert set(headers) == set(range(100, 116))
        pages = sorted(
            query for client in created["archive"] for query in client.log_queries
        )
        assert pages == sorted([
            (100, 107), (108, 115), (100, 107), (108, 115),
            (100, 103), (104, 107), (108, 111), (112, 115),
        ])
        assert all(not client.log_queries for client in created["history"])
        metrics = scanner._fetch_metrics["history"]
        assert metrics["source"] == "archive"
        assert metrics["event_header_count"] == 14
    finally:
        scanner.close()
        store.close()


def test_archive_interval_rejects_a_boundary_the_canonical_source_disputes():
    created: dict[str, list] = {}
    store = MarketStore(":memory:")
    scanner = indexer(store, archive_factory(created, fork_at=115))
    try:
        with pytest.raises(CanonicalConflict, match="canonical header 115"):
            scanner._fetch_interval("history", 100, 115)
    finally:
        scanner.close()
        store.close()




def test_interval_end_is_rechecked_after_logs_before_commit():
    class FlippingRpc(StaticRpc):
        def __init__(self):
            super().__init__()
            self.logs_complete = False

        def call(self, method, params):
            if method == "eth_getLogs":
                self.logs_complete = True
                return []
            return super().call(method, params)

        def batch(self, calls):
            results = []
            for method, params in calls:
                assert method == "eth_getBlockByNumber"
                block = header(int(params[0], 16))
                if self.logs_complete and int(params[0], 16) == 20:
                    block["hash"] = "0x" + "ff" * 32
                results.append(block)
            return results

    store = MarketStore(":memory:")
    scanner = indexer(store, FlippingRpc())
    try:
        with pytest.raises(CanonicalConflict, match="changed during fetch"):
            scanner._fetch_interval("live", 10, 20)
    finally:
        scanner.close()
        store.close()


def test_checkpoint_maintenance_continues_while_projection_is_blocked(
    tmp_path, monkeypatch,
):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    scanner = indexer(store, maintenance_interval_s=0.01, wal_reset_bytes=1)
    first_checkpoint_started = threading.Event()
    first_checkpoint_release = threading.Event()
    projection_started = threading.Event()
    projection_release = threading.Event()
    real_checkpoint = store.checkpoint
    first = True
    monkeypatch.setattr("rhpools.lp_market_index.WAL_RESET_RETRY_S", 0)

    def checkpoint(mode="PASSIVE", *, drain_readers=False):
        nonlocal first
        if first:
            first = False
            first_checkpoint_started.set()
            first_checkpoint_release.wait(2)
        return real_checkpoint(mode, drain_readers=drain_readers)

    def block_projection():
        projection_started.set()
        projection_release.wait(2)
        return False

    def idle_worker():
        scanner._stop.wait(2)

    monkeypatch.setattr(store, "checkpoint", checkpoint)
    monkeypatch.setattr(scanner, "_bootstrap", lambda: (0, header(0), 0.0))
    monkeypatch.setattr(scanner, "_reproject_once", block_projection)
    for name in (
        "_live_run", "_history_run", "_enrichment_run",
        "_metadata_run", "_source_repair_run",
    ):
        monkeypatch.setattr(scanner, name, idle_worker)
    try:
        scanner.start()
        assert first_checkpoint_started.wait(1)
        assert scanner.runtime_status()["storage_pause_reasons"] == [
            "assessment_pending"
        ]
        assert not projection_started.is_set()
        first_checkpoint_release.set()
        assert projection_started.wait(1)
        with store.transaction() as connection:
            connection.execute("CREATE TABLE checkpoint_probe(value TEXT)")
            connection.execute("INSERT INTO checkpoint_probe VALUES('committed')")
        committed_at = time.time()
        deadline = time.monotonic() + 1
        while True:
            checkpoint_status = scanner.runtime_status().get("wal_checkpoint") or {}
            if (
                checkpoint_status.get("observed_at", 0) >= committed_at
                and checkpoint_status.get("backlog_bytes") == 0
            ):
                break
            assert time.monotonic() < deadline, checkpoint_status
            time.sleep(0.01)
        assert store.read().execute(
            "SELECT value FROM checkpoint_probe"
        ).fetchone()[0] == "committed"
        assert not projection_release.is_set()
    finally:
        projection_release.set()
        first_checkpoint_release.set()
        scanner.close()
        store.close()


def test_storage_pressure_hysteresis_pauses_and_resumes_history(
    tmp_path, monkeypatch,
):
    disk_pause = 1_000
    disk_resume = 1_200
    wal_pause = 100
    wal_resume = 20
    wal_reset = 500
    store = MarketStore(tmp_path / "market.sqlite")
    scanner = MarketIndexer(
        store,
        Market(),
        "http://unused.invalid",
        rpc=StaticRpc(),
        history_disk_reserve_bytes=disk_pause,
        storage_resume_bytes=disk_resume,
        wal_backlog_pause_bytes=wal_pause,
        wal_backlog_resume_bytes=wal_resume,
        wal_reset_bytes=wal_reset,
    )
    scanner._history_verified = True
    anchor = header(500)
    store.ingest(
        [anchor],
        [],
        lane="history",
        cursor={
            "next_to": 499,
            "low_block": 500,
            "block_hash": anchor["hash"],
            "target_block": 492,
            "target_timestamp": int(header(492)["timestamp"], 16),
            "target_pending": False,
            "origin_head": 500,
            "complete": False,
            "has_coverage": True,
        },
    )
    passive_results = iter((
        {
            "busy": 0, "log_frames": 25, "checkpointed_frames": 1,
            "backlog_bytes": wal_pause, "wal_bytes": wal_reset,
        },
        {
            "busy": 0, "log_frames": 30, "checkpointed_frames": 2,
            "backlog_bytes": wal_pause + 1, "wal_bytes": wal_reset,
        },
        {
            "busy": 0, "log_frames": 0, "checkpointed_frames": 0,
            "backlog_bytes": 0, "wal_bytes": wal_reset,
        },
        {
            "busy": 0, "log_frames": 0, "checkpointed_frames": 0,
            "backlog_bytes": 0, "wal_bytes": 0,
        },
        {
            "busy": 0, "log_frames": 0, "checkpointed_frames": 0,
            "backlog_bytes": 0, "wal_bytes": 0,
        },
    ))
    last_passive: dict[str, int] = {}

    def checkpoint(mode="PASSIVE", *, drain_readers=False):
        nonlocal last_passive
        if mode == "RESTART":
            return {
                "busy": int(last_passive["backlog_bytes"] > 0),
                "log_frames": 0,
                "checkpointed_frames": 0,
                "backlog_bytes": 0,
                "wal_bytes": wal_reset,
                "log_bytes": 0,
                "active_reader_snapshots": 0,
                "reader_drain_pending": 0,
            }
        last_passive = next(passive_results)
        last_passive.update(
            log_bytes=last_passive["log_frames"] * 4,
            active_reader_snapshots=0, reader_drain_pending=0,
        )
        return last_passive

    free_bytes = iter((2_000, 2_000, 900, 1_100, disk_resume))
    monkeypatch.setattr(store, "checkpoint", checkpoint)
    monkeypatch.setattr(
        "rhpools.lp_market_index.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=next(free_bytes)),
    )
    monkeypatch.setattr(
        scanner,
        "_fetch_interval",
        lambda _lane, start, end: (
            [],
            {start: header(start), end: header(end)},
        ),
    )
    monkeypatch.setattr(scanner, "_decode", lambda *_args, **_kwargs: [])

    try:
        scanner._storage_maintenance_once()
        status = scanner.runtime_status()
        assert status["storage_pause_reasons"] == ["wal_backlog"]
        assert status["storage_pressure"]["wal_backlog"] is True
        assert status["storage_pressure"]["free_space"] is False
        assert status["wal_checkpoint"]["backlog_bytes"] == wal_pause
        assert scanner._scan_history_once() is False
        assert store.cursor("history")["next_to"] == 499

        scanner._storage_maintenance_once()
        status = scanner.runtime_status()
        assert status["storage_paused"] is True
        assert status["wal_checkpoint"]["progress_frames"] == 1
        assert status["wal_checkpoint"]["backlog_reduction_bytes"] == 0
        assert status["wal_checkpoint"]["stalled"] is False

        scanner._storage_maintenance_once()
        status = scanner.runtime_status()
        assert status["storage_pause_reasons"] == ["free_space"]
        assert status["storage_pressure"]["wal_backlog"] is False
        assert status["storage_pressure"]["free_space"] is True

        scanner._storage_maintenance_once()
        assert scanner.runtime_status()["storage_paused"] is True

        scanner._storage_maintenance_once()
        status = scanner.runtime_status()
        assert status["storage_paused"] is False
        assert status["storage_pause_reasons"] == []
        assert status["storage_free_bytes"] == disk_resume
        assert status["bulk_work"] == "running"
        assert "storage" not in status["errors"]
        assert scanner._scan_history_once() is True
        assert store.cursor("history")["next_to"] == 491
    finally:
        scanner.close()
        store.close()


def test_wal_backpressure_preserves_live_ingestion(monkeypatch):
    wal_pause = 100
    store = MarketStore(":memory:")
    scanner = indexer(
        store,
        StaticRpc(100),
        wal_backlog_pause_bytes=wal_pause,
        wal_backlog_resume_bytes=20,
        wal_reset_bytes=1_000,
    )
    anchor = header(99)
    store.ingest(
        [anchor],
        [],
        lane="live",
        cursor={
            "block_number": 99,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    monkeypatch.setattr(
        store,
        "checkpoint",
        lambda mode="PASSIVE", *, drain_readers=False: {
            "busy": int(mode != "PASSIVE"),
            "log_frames": 25,
            "checkpointed_frames": 0,
            "backlog_bytes": wal_pause,
            "wal_bytes": wal_pause,
            "log_bytes": wal_pause,
            "active_reader_snapshots": 0,
            "reader_drain_pending": 0,
        },
    )
    monkeypatch.setattr(
        scanner,
        "_fetch_interval",
        lambda _lane, start, end: (
            [],
            {start: header(start), end: header(end)},
        ),
    )
    monkeypatch.setattr(scanner, "_decode", lambda *_args, **_kwargs: [])

    try:
        scanner._storage_maintenance_once()
        assert scanner.runtime_status()["storage_paused"] is True
        assert scanner._scan_history_once() is False
        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] == 100
        assert scanner.runtime_status()["storage_pause_reasons"] == [
            "wal_backlog"
        ]
    finally:
        scanner.close()
        store.close()


def test_checkpoint_contention_pauses_bulk_until_metrics_recover(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store, StaticRpc(), wal_reset_bytes=0)
    result = {
        "busy": 1, "log_frames": 0, "checkpointed_frames": 0,
        "backlog_bytes": 0, "wal_bytes": 2 * 1024**3,
        "log_bytes": 0,
        "active_reader_snapshots": 0,
        "reader_drain_pending": 0,
    }

    def checkpoint(_mode):
        scanner._stop.set()
        return result

    monkeypatch.setattr(store, "checkpoint", checkpoint)
    try:
        scanner._maintenance_run()
        assert scanner.runtime_status()["storage_paused"] is True
        assert "checkpoint_error" in scanner.runtime_status()["storage_pause_reasons"]
        assert scanner._bulk_work_allowed() is False

        result["busy"] = 0
        scanner._stop.clear()
        scanner._storage_maintenance_once()
        assert scanner._bulk_work_allowed() is True
        assert scanner.runtime_status()["storage_pause_reasons"] == []
    finally:
        scanner.close()
        store.close()



def test_busy_wal_reset_retries_after_reader_releases(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    scanner = indexer(store, StaticRpc(), wal_reset_bytes=1)
    reader = store.read()
    try:
        with store.transaction() as connection:
            connection.execute("CREATE TABLE reset_probe(value INTEGER)")
            connection.execute("INSERT INTO reset_probe VALUES(7)")
        reader.execute("BEGIN")
        assert reader.execute("SELECT value FROM reset_probe").fetchone()[0] == 7
        scanner._storage_maintenance_once()
        assert "reader_drain" in scanner.runtime_status()["storage_pause_reasons"]
        wal = store.path.with_name(store.path.name + "-wal")
        allocated = wal.stat().st_size

        reader.rollback()
        scanner._storage_maintenance_once()
        assert wal.stat().st_size == allocated
        with store.transaction() as connection:
            connection.execute("INSERT INTO reset_probe VALUES(8)")
        assert [row[0] for row in reader.execute(
            "SELECT value FROM reset_probe ORDER BY value"
        )] == [7, 8]
        assert wal.stat().st_size == allocated
        assert store.checkpoint()["log_bytes"] < allocated
    finally:
        reader.rollback()
        scanner.close()
        store.close()




def test_reader_drain_preserves_live_ingestion_and_snapshot_consistency(
    tmp_path, monkeypatch,
):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    scanner = indexer(
        store, StaticRpc(100), wal_backlog_pause_bytes=100,
        wal_backlog_resume_bytes=20, wal_reset_bytes=1,
    )
    anchor = header(99)
    store.ingest(
        [anchor], [], lane="live",
        cursor={
            "block_number": 99, "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    old_read_started = threading.Event()
    release_old_read = threading.Event()
    new_read_started = threading.Event()
    new_read_finished = threading.Event()
    failures = []
    observed = []

    def old_reader():
        try:
            with store.reader_snapshot():
                assert store.cursor("live")["block_number"] == 99
                old_read_started.set()
                release_old_read.wait(2)
                assert store.cursor("live")["block_number"] == 99
        except BaseException as exc:
            failures.append(exc)

    def new_reader():
        try:
            new_read_started.set()
            with store.reader_snapshot():
                observed.append(store.cursor("live")["block_number"])
            new_read_finished.set()
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(
        scanner, "_fetch_interval",
        lambda _lane, start, end: ([], {start: header(start), end: header(end)}),
    )
    monkeypatch.setattr(scanner, "_decode", lambda *_args, **_kwargs: [])
    original = threading.Thread(target=old_reader)
    queued = threading.Thread(target=new_reader)
    try:
        original.start()
        assert old_read_started.wait(1)
        with store.transaction() as connection:
            connection.execute(
                "INSERT INTO metadata(key,value) VALUES('drain-probe','1')"
            )
        scanner._storage_maintenance_once()
        assert scanner.runtime_status()["storage_paused"] is True
        queued.start()
        assert new_read_started.wait(1)
        assert not new_read_finished.wait(0.05)

        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] == 100
        release_old_read.set()
        original.join(1)
        assert not original.is_alive()
        scanner._storage_maintenance_once()
        assert new_read_finished.wait(1)
        assert observed == [100]
        assert scanner.runtime_status()["storage_paused"] is False
        assert failures == []
    finally:
        release_old_read.set()
        store.cancel_checkpoint_drain()
        original.join(2)
        if queued.ident is not None:
            queued.join(2)
        scanner.close()
        store.close()


def test_metadata_rejections_revalidate_slowly_without_masking_rpc_health(
    monkeypatch,
):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    invalid_address = "0x" + "a1" * 20
    rpc_address = "0x" + "b2" * 20
    with store.transaction() as connection:
        connection.executemany(
            "INSERT INTO pending_token_metadata(address) VALUES(?)",
            [(invalid_address,), (rpc_address,)],
        )
        store._bump(connection, "pending_metadata", 2)

    invalid_symbol = (
        "0x"
        + f"{32:064x}"
        + f"{257:064x}"
        + (b"x" * 257).hex()
    )

    def fail(address):
        if address == invalid_address:
            return scanner._decode_token_symbol(invalid_symbol), 18
        raise RpcError("provider unavailable")

    monkeypatch.setattr(scanner, "_fetch_token_metadata", fail)
    before = time.time()
    try:
        assert scanner._metadata_once() is True
        queued = {
            row["address"]: dict(row)
            for row in store.read().execute(
                "SELECT * FROM pending_token_metadata ORDER BY address"
            )
        }
        assert queued[invalid_address]["last_error"].startswith(
            TOKEN_METADATA_INVALID_PREFIX
        )
        assert not queued[rpc_address]["last_error"].startswith(
            TOKEN_METADATA_INVALID_PREFIX
        )
        assert (
            queued[invalid_address]["next_attempt"] - before
            >= TOKEN_METADATA_INVALID_RECHECK_S - 1
        )
        assert queued[rpc_address]["next_attempt"] - before <= 301

        status = scanner.runtime_status()
        assert set(status["metadata_failures"]) == {
            invalid_address, rpc_address,
        }
        assert (
            status["metadata_failures"][invalid_address]["classification"]
            == "invalid_metadata"
        )
        assert status["metadata_failures"][rpc_address]["classification"] == "rpc"
        assert rpc_address in status["errors"]["metadata"]

        with store.transaction() as connection:
            connection.execute(
                "UPDATE pending_token_metadata SET next_attempt=0 WHERE address=?",
                (rpc_address,),
            )
        monkeypatch.setattr(
            scanner, "_fetch_token_metadata", lambda _address: ("RPC", 18),
        )
        assert scanner._metadata_once() is True
        recovered = scanner.runtime_status()
        assert "metadata" not in recovered["errors"]
        assert set(recovered["metadata_failures"]) == {invalid_address}
        assert tuple(store.read().execute(
            "SELECT symbol,decimals FROM token_metadata WHERE address=?",
            (rpc_address,),
        ).fetchone()) == ("RPC", 18)
    finally:
        scanner.close()
        store.close()


def test_reverted_token_metadata_is_recorded_per_token_not_global(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    symbol_revert = "0x" + "c3" * 20
    decimals_revert = "0x" + "d4" * 20
    revert_result = {
        "error": {"code": 3, "message": "execution reverted", "data": "0x"},
    }
    symbol_result = "0x" + "41" * 8
    decimals_result = "0x" + f"{18:064x}"
    with store.transaction() as connection:
        connection.executemany(
            "INSERT INTO pending_token_metadata(address) VALUES(?)",
            [(symbol_revert,), (decimals_revert,)],
        )
        store._bump(connection, "pending_metadata", 2)

    def fake_batch(calls, _client):
        address = calls[0][1][0]["to"]
        if address == symbol_revert:
            return [revert_result, decimals_result]
        return [symbol_result, revert_result]

    monkeypatch.setattr(scanner, "_rpc_state_batch", fake_batch)
    try:
        assert scanner._metadata_once() is True
        status = scanner.runtime_status()
        assert "metadata" not in status["errors"]
        assert {
            address: detail["classification"]
            for address, detail in status["metadata_failures"].items()
        } == {
            symbol_revert: "invalid_metadata",
            decimals_revert: "invalid_metadata",
        }
        rows = store.read().execute(
            "SELECT address,last_error FROM pending_token_metadata "
            "ORDER BY address"
        ).fetchall()
        assert all(
            row["last_error"].startswith(TOKEN_METADATA_INVALID_PREFIX)
            for row in rows
        )
    finally:
        scanner.close()
        store.close()


def test_accounting_worker_clears_stale_error_after_recovery():
    store = MarketStore(":memory:")
    scanner = indexer(store)

    def stop_and_work():
        scanner._stop.set()
        return True

    scanner._accounting_projector = stop_and_work
    scanner._accounting_recovery = stop_and_work
    try:
        scanner._set_runtime("accounting", error="interrupted")
        scanner._set_runtime(
            "identity_recovery", error="reader snapshot exceeded its deadline",
        )


        scanner._initialized.set()
        scanner._accounting_run()
        assert "accounting" not in scanner.runtime_status()["errors"]
        scanner._stop.clear()
        scanner._accounting_recovery_run()
        errors = scanner.runtime_status()["errors"]
        assert "identity_recovery" not in errors
    finally:
        scanner.close()
        store.close()




def test_poolkey_input_reads_preserve_receipt_capability_failures(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    exhausted = "0x" + "11" * 32
    absent = "0x" + "22" * 32
    missing = "0x" + "55" * 32
    calls = []

    def batch_results(client, specifications):
        assert client is scanner._clients["pool"]
        calls.extend(specifications)
        return [
            RpcError("receipts RPC exhausted: local timeout"), {},
            None, {},
        ]

    monkeypatch.setattr(scanner, "_batch_results_on", batch_results)
    monkeypatch.setattr(
        scanner, "_rpc_state_batch",
        lambda *_args, **_kwargs: pytest.fail(
            "transaction and receipt reads used the state batching path"
        ),
    )
    try:
        resolved, failures = scanner._resolve_current_v4_inputs({
            exhausted: ((), "0x" + "33" * 32),
            absent: ((), "0x" + "44" * 32),
            missing: ((), ""),
        })
        assert resolved == {}
        assert str(failures[exhausted]) == (
            "receipts RPC exhausted: local timeout"
        )
        assert str(failures[absent]) == "PoolKey transaction was not found"
        assert str(failures[missing]) == (
            "PoolKey transaction hash is unavailable"
        )
        assert [method for method, _params in calls] == [
            "eth_getTransactionByHash", "eth_getTransactionReceipt",
            "eth_getTransactionByHash", "eth_getTransactionReceipt",
        ]
    finally:
        scanner.close()
        store.close()


def test_pool_resolution_reports_address_and_recovers_health(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    pool_id = "0x" + "12" * 32
    observed = header(100)
    with scanner._feed_condition:
        scanner._observed_blocks[100] = (observed, [])

    monkeypatch.setattr(
        scanner, "_resolve_current_v4_pools",
        lambda *_args: ({}, {pool_id: RpcError("provider unavailable")}),
    )
    try:
        scanner._resolve_current_pool(
            pool_id, 100, observed["hash"], [], "test", "0x" + "34" * 32,
        )
        failed = scanner.runtime_status()
        assert pool_id in failed["errors"]["pool_resolution"]
        assert failed["pool_resolution_failures"][pool_id]["classification"] == "rpc"

        monkeypatch.setattr(
            scanner, "_resolve_current_v4_inputs",
            lambda *_args: ({pool_id: {"id": pool_id}}, {}),
        )
        monkeypatch.setattr(store, "upsert_pools", lambda _pools: None)
        monkeypatch.setattr(store, "pool", lambda _pool_id: {"id": pool_id})
        monkeypatch.setattr(scanner, "_stored_pool", lambda row: dict(row))
        monkeypatch.setattr(scanner, "_remember_pool", lambda _pool: None)
        monkeypatch.setattr(scanner, "_publish_event_pools", lambda _events: None)
        monkeypatch.setattr(
            scanner, "_publish_late_current_logs", lambda *_args, **_kwargs: None,
        )
        scanner._resolve_current_pool(
            pool_id, 100, observed["hash"], [], "test", "0x" + "34" * 32,
        )
        recovered = scanner.runtime_status()
        assert "pool_resolution" not in recovered["errors"]
        assert recovered["pool_resolution_failures"] == {}
    finally:
        scanner.close()
        store.close()


def test_durable_identity_recovery_clears_only_its_pool_failure(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    first, second = "0x" + "12" * 32, "0x" + "34" * 32
    observed = header(100)
    with scanner._feed_condition:
        scanner._observed_blocks[100] = (observed, [])

    def fail(pool_ids, *_args):
        pool_id = next(iter(pool_ids))
        return {}, {pool_id: RpcError(f"{pool_id} unavailable")}

    monkeypatch.setattr(scanner, "_resolve_current_v4_pools", fail)
    try:
        for pool_id in (first, second):
            scanner._resolve_current_pool(
                pool_id, 100, observed["hash"], [], "test",
                "0x" + "56" * 32,
            )
        failed = scanner.runtime_status()
        assert failed["pool_resolution_failure_count"] == 2

        scanner._clear_current_pool_resolution_failure(first)
        partial = scanner.runtime_status()
        assert partial["pool_resolution_failure_count"] == 1
        assert set(partial["pool_resolution_failures"]) == {second}
        assert second in partial["errors"]["pool_resolution"]

        scanner._clear_current_pool_resolution_failure(second)
        recovered = scanner.runtime_status()
        assert recovered["pool_resolution_failure_count"] == 0
        assert recovered["pool_resolution_failures"] == {}
        assert "pool_resolution" not in recovered["errors"]
    finally:
        scanner.close()
        store.close()


def test_reprojection_batch_sizes_from_complete_writer_transaction(monkeypatch):
    clock = SimpleNamespace(now=10.0)

    class Store:
        active = False

        def pending_reprojections(self, limit):
            assert limit == 128
            return [
                {"id": event_id, "reprojection_attempts": 0}
                for event_id in range(limit)
            ]

        def transaction(self):
            owner = self

            class Transaction:
                def __enter__(self):
                    owner.active = True

                def __exit__(self, exc_type, exc, traceback):
                    clock.now += REPROJECT_MAX_STORE_SECONDS * 2
                    owner.active = False

            return Transaction()

        def reproject(self, event_ids):
            assert self.active
            assert len(event_ids) == 128

    scanner = MarketIndexer.__new__(MarketIndexer)
    scanner.store = Store()
    scanner._reproject_batch = 128
    observed = {}
    scanner._set_runtime = lambda _lane, **values: observed.update(values)
    monkeypatch.setattr("rhpools.lp_market_index.time.monotonic", lambda: clock.now)

    assert scanner._reproject_once() is True
    assert 1 <= scanner._reproject_batch <= 64
    assert observed["reprojection_store_seconds"] == pytest.approx(
        REPROJECT_MAX_STORE_SECONDS * 2
    )
    assert observed["reprojection_writer_wait_seconds"] == 0
    assert observed["reprojection_next_batch"] == scanner._reproject_batch

