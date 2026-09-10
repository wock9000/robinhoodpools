"""Focused scanner throughput and canonical-safety regressions."""
from __future__ import annotations

from contextlib import contextmanager
import json
import threading
import time
from types import SimpleNamespace
import pytest

from rhpools.lp_market_index import (
    FACTORY_SELECTOR,
    GET_PAIR_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    MarketIndexer,
    RpcError,
)
from rhpools.lp_market_store import CanonicalConflict, MarketStore
from rhpools.lp_market_protocols import (
    V2_FACTORIES,
    V2_SYNC_TOPIC,
    POOL_MANAGER,
    TRANSFER_TOPIC,
    V4_MODIFY_LIQUIDITY_TOPIC,
)


ZERO_HASH = "0x" + "00" * 32


def header(number: int, *, parent: str | None = None) -> dict[str, str]:
    return {
        "number": hex(number),
        "hash": "0x" + f"{number:064x}",
        "parentHash": parent or ("0x" + f"{max(0, number - 1):064x}"),
        "timestamp": hex(1_700_000_000 + number),
    }


def indexed_event(number: int, tx_byte: str) -> dict:
    block = header(number)
    return {
        "block_number": number,
        "block_hash": block["hash"],
        "tx_hash": "0x" + tx_byte * 32,
        "tx_index": 0,
        "log_index": 0,
        "timestamp": int(block["timestamp"], 16),
        "pool_id": None,
        "protocol": "v3",
        "kind": "add",
        "owner": "0x" + "11" * 20,
        "data": {},
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

    def batch(self, calls, *, allow_reverts=False):
        results = []
        for method, params in calls:
            try:
                results.append(self.call(method, params))
            except RpcError as exc:
                if allow_reverts and method == "eth_call" and (
                    exc.code == 3 or "execution reverted" in str(exc).lower()
                ):
                    results.append({"error": str(exc)})
                else:
                    raise
        return results


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


def test_live_writer_and_history_recovery_cannot_deadlock(tmp_path):
    from eth_abi import encode
    from eth_utils import keccak

    store = MarketStore(tmp_path / "market.sqlite")
    scanner = indexer(store)
    token0, token1, hook = "0x" + "11" * 20, "0x" + "22" * 20, "0x" + "00" * 20
    pool_id = "0x" + keccak(encode(
        ["address", "address", "uint24", "int24", "address"],
        [token0, token1, 3000, 8, hook],
    )).hex()
    store.upsert_pools([{
        "id": pool_id, "protocol": "v4", "address": POOL_MANAGER,
        "token0": token0, "token1": token1, "fee_ppm": 3000,
        "tick_spacing": None, "hook": hook, "factory": POOL_MANAGER,
        "source": "census", "metadata_json": {"dynamic_fee": False},
    }])
    history_waiting = threading.Event()
    writer_lock = store.lock
    failures = []
    recovered = []

    class ObservedWriterLock:
        def __enter__(self):
            if threading.current_thread().name == "history-recovery":
                history_waiting.set()
            if not writer_lock.acquire(timeout=2):
                raise TimeoutError("history recovery was blocked by live ingestion")
            return self

        def __exit__(self, *_args):
            writer_lock.release()

    def recover_history():
        try:
            recovered.append(scanner._pool(pool_id))
        except Exception as exc:
            failures.append(exc)
        finally:
            store.close_reader()

    store.lock = ObservedWriterLock()
    history = threading.Thread(target=recover_history, name="history-recovery")
    try:
        with store.transaction():
            history.start()
            assert history_waiting.wait(2)
            live = scanner._pool(pool_id)
            assert live["tick_spacing"] == 8
        history.join(3)
        assert not history.is_alive()
        assert failures == []
        assert recovered[0]["id"] == pool_id
        assert recovered[0]["tick_spacing"] == 8
        assert store.pool(pool_id)["tick_spacing"] == 8
    finally:
        history.join(3)
        scanner.close()
        store.close()


def test_complete_v4_pool_lookup_does_not_wait_for_writer(tmp_path):
    from eth_abi import encode
    from eth_utils import keccak

    store = MarketStore(tmp_path / "market.sqlite")
    scanner = indexer(store)
    token0, token1, hook = (
        "0x" + "11" * 20,
        "0x" + "22" * 20,
        "0x" + "00" * 20,
    )
    pool_id = "0x" + keccak(encode(
        ["address", "address", "uint24", "int24", "address"],
        [token0, token1, 3000, 8, hook],
    )).hex()
    store.upsert_pools([{
        "id": pool_id, "protocol": "v4", "address": POOL_MANAGER,
        "token0": token0, "token1": token1, "fee_ppm": 3000,
        "tick_spacing": 8, "hook": hook, "factory": POOL_MANAGER,
        "source": "census",
        "metadata_json": {
            "configured_fee": 3000,
            "dynamic_fee": False,
        },
    }])
    writer_held = threading.Event()
    release_writer = threading.Event()
    lookup_done = threading.Event()
    result = []
    failures = []

    def hold_writer():
        with store.transaction():
            writer_held.set()
            release_writer.wait(2)

    def lookup_pool():
        try:
            result.append(scanner._pool(pool_id))
        except Exception as exc:
            failures.append(exc)
        finally:
            store.close_reader()
            lookup_done.set()

    writer = threading.Thread(target=hold_writer)
    lookup = threading.Thread(target=lookup_pool)
    writer.start()
    try:
        assert writer_held.wait(1)
        lookup.start()
        assert lookup_done.wait(1), (
            "complete canonical pool lookup waited for an unrelated writer"
        )
        assert failures == []
        assert result[0]["id"] == pool_id
        assert result[0]["tick_spacing"] == 8
    finally:
        release_writer.set()
        lookup.join(2)
        writer.join(2)
        scanner.close()
        store.close()


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
    clock = SimpleNamespace(now=100.0, depth=0)
    transaction = store.transaction

    @contextmanager
    def queued_transaction():
        outer = clock.depth == 0
        if outer:
            clock.now += 30.0
        clock.depth += 1
        try:
            with transaction() as connection:
                yield connection
        finally:
            clock.depth -= 1
            if outer:
                clock.now += 0.01

    monkeypatch.setattr(store, "transaction", queued_transaction)
    monkeypatch.setattr(
        "rhpools.lp_market_index.time",
        SimpleNamespace(monotonic=lambda: clock.now, time=time.time),
    )
    try:
        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] == 355
        scan = scanner.runtime_status()["live_scan"]
        assert scan["store_lock_wait_seconds"] == 30.0
        assert scan["store_seconds"] == pytest.approx(0.01)
        assert scan["next_chunk"] > scan["requested_chunk"]
    finally:
        scanner.close()
        store.close()


def test_stale_provider_head_retains_indexed_history_until_catchup():
    store = MarketStore(":memory:")
    rpc = StaticRpc(98)
    scanner = indexer(store, rpc)
    anchor = header(99)
    event = indexed_event(99, "ab")
    store.ingest(
        [anchor], [event], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    try:
        assert scanner._scan_live_once() is False
        assert scanner.runtime_status()["state"] == "degraded"
        cursor = store.cursor("live")
        assert cursor["block_number"] == 99
        assert cursor["block_hash"] == anchor["hash"]
        assert store.read().execute(
            "SELECT COUNT(*) FROM events WHERE tx_hash=?",
            (event["tx_hash"],),
        ).fetchone()[0] == 1

        rpc.head = 100
        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] == 100
        assert scanner.runtime_status()["state"] == "live"
        assert store.read().execute(
            "SELECT COUNT(*) FROM events WHERE tx_hash=?",
            (event["tx_hash"],),
        ).fetchone()[0] == 1
    finally:
        scanner.close()
        store.close()


def test_reorg_check_retains_cursor_when_provider_drops_before_header():
    class DroppingRpc(StaticRpc):
        def __init__(self):
            super().__init__(99)
            self.head_reads = 0

        def call(self, method, params):
            if method == "eth_blockNumber":
                self.head_reads += 1
                return hex(99 if self.head_reads == 1 else 98)
            if method == "eth_getBlockByNumber" and int(params[0], 16) == 99:
                return None
            return super().call(method, params)

    store = MarketStore(":memory:")
    scanner = indexer(store, DroppingRpc())
    anchor = header(99)
    event = indexed_event(99, "ac")
    store.ingest(
        [anchor], [event], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    try:
        scanner._recover_reorg(
            store.cursor("live"), "concurrent live cursor update",
        )
        assert store.cursor("live")["block_number"] == 99
        assert store.read().execute(
            "SELECT COUNT(*) FROM events WHERE tx_hash=?",
            (event["tx_hash"],),
        ).fetchone()[0] == 1
    finally:
        scanner.close()
        store.close()


def test_same_height_conflict_survives_head_drop_during_recovery():
    canonical_99 = {
        **header(99),
        "hash": "0x" + "aa" * 32,
    }

    class RecedingReorgRpc(StaticRpc):
        def __init__(self):
            super().__init__(99)
            self.head_reads = 0

        def call(self, method, params):
            if method == "eth_blockNumber":
                self.head_reads += 1
                return hex(99 if self.head_reads == 1 else 98)
            if method == "eth_getBlockByNumber" and int(params[0], 16) == 99:
                return canonical_99
            return super().call(method, params)

    store = MarketStore(":memory:")
    scanner = indexer(store, RecedingReorgRpc())
    prior = header(98)
    orphan = header(99)
    event = indexed_event(99, "bc")
    store.ingest(
        [prior, orphan], [event], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": orphan["hash"],
            "timestamp": int(orphan["timestamp"], 16),
        },
    )
    try:
        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] == 98
        assert store.read().execute(
            "SELECT COUNT(*) FROM events WHERE tx_hash=?",
            (event["tx_hash"],),
        ).fetchone()[0] == 0
        assert scanner.runtime_status()["reorg"]["ancestor"] == 98
    finally:
        scanner.close()
        store.close()


def test_live_parent_mismatch_rolls_back_to_canonical_ancestor():
    canonical_99 = {
        **header(99),
        "hash": "0x" + "cc" * 32,
    }
    canonical_100 = header(100, parent=canonical_99["hash"])

    class ReorgRpc(StaticRpc):
        def call(self, method, params):
            if method == "eth_getBlockByNumber":
                number = int(params[0], 16)
                if number == 99:
                    return canonical_99
                if number == 100:
                    return canonical_100
            return super().call(method, params)

    store = MarketStore(":memory:")
    scanner = indexer(store, ReorgRpc(100))
    prior = header(98)
    orphan = header(99)
    event = indexed_event(99, "cd")
    store.ingest(
        [prior, orphan], [event], lane="live",
        cursor={
            "block_number": 99,
            "block_hash": orphan["hash"],
            "timestamp": int(orphan["timestamp"], 16),
        },
    )
    try:
        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] == 98
        assert store.read().execute(
            "SELECT COUNT(*) FROM events WHERE tx_hash=?",
            (event["tx_hash"],),
        ).fetchone()[0] == 0
    finally:
        scanner.close()
        store.close()


@pytest.mark.parametrize("blocked_trace", [False, True], ids=["unavailable-trace", "slow-trace"])
def test_live_gap_and_v4_trace_cannot_block_v3_position_accounting(
    tmp_path, blocked_trace,
):
    from eth_abi import encode
    from rhpools.lp_market_protocols import (
        LIQUIDITY_SELECTOR, POSITIONS_SELECTOR, SLOT0_SELECTOR, V3_MINT_TOPIC,
    )
    from rhpools.lp_market_service import LPMarketService
    from rhpools.workbench_market import USDG, UNISWAP_V3_FACTORY

    owner, pool, token = ("0x" + byte * 20 for byte in ("11", "22", "33"))
    v4_tx, v3_tx = ("0x" + byte * 32 for byte in ("ab", "cd"))
    trace_started, release_trace = threading.Event(), threading.Event()
    blocks = {tx: header(n) for tx, n in ((v4_tx, 20), (v3_tx, 21))}
    raw_logs = {}
    for tx_hash, block in blocks.items():
        raw_logs[tx_hash] = {
            "blockNumber": block["number"], "blockHash": block["hash"],
            "transactionHash": tx_hash, "transactionIndex": "0x0", "logIndex": "0x0",
            "address": POOL_MANAGER if tx_hash == v4_tx else pool,
            "topics": (
                [V4_MODIFY_LIQUIDITY_TOPIC, "0x" + "44" * 32, "0x" + owner[2:].zfill(64)]
                if tx_hash == v4_tx else [
                    V3_MINT_TOPIC, "0x" + owner[2:].zfill(64),
                    "0x" + "00" * 32, "0x" + f"{10:064x}",
                ]
            ),
            "data": "0x" + encode(
                ["address", "uint128", "uint256", "uint256"],
                [owner, 10, 1_000_000, 2_000_000],
            ).hex(),
        }

    class ReceiptRpc(StaticRpc):
        @staticmethod
        def status():
            return {"trace": {"active": None, "configured": False, "sources": []}}

        def call(self, method, params):
            if method == "eth_getTransactionReceipt":
                tx_hash = params[0]
                block = blocks[tx_hash]
                return {
                    "transactionHash": tx_hash, "blockNumber": block["number"],
                    "blockHash": block["hash"], "transactionIndex": "0x0",
                    "status": "0x1", "from": owner, "gasUsed": hex(21_000),
                    "effectiveGasPrice": hex(1_000_000_000),
                    "logs": [raw_logs[tx_hash]],
                }
            if method == "eth_getTransactionByHash":
                return {"hash": params[0], "from": owner, "gasPrice": hex(1_000_000_000)}
            if method == "debug_traceTransaction":
                assert params[0] == v4_tx
                trace_started.set()
                if blocked_trace:
                    assert release_trace.wait(5)
                raise RpcError("trace RPC unavailable")
            if method == "eth_call":
                selector = params[0]["data"][:10]
                if selector == POSITIONS_SELECTOR:
                    values = [10, 0, 0, 3, 5] if params[1] == "0x15" else [0] * 5
                elif selector == SLOT0_SELECTOR:
                    values = [1 << 96, 0, 0, 1, 1, 0, 1]
                elif selector == LIQUIDITY_SELECTOR:
                    values = [1_000]
                else:
                    raise AssertionError(selector)
                return "0x" + encode(["uint256"] * len(values), values).hex()
            return super().call(method, params)

    app = LPMarketService(Market(), "http://unused.invalid", tmp_path / "receipts.sqlite", start=False)
    scanner = indexer(app.store, ReceiptRpc())
    try:
        app.store.upsert_pools([{
            "id": pool, "address": pool, "protocol": "v3",
            "token0": token, "token1": USDG, "decimals0": 6, "decimals1": 6,
            "symbol0": "ASSET", "symbol1": "USDG", "fee_ppm": 3_000,
            "tick_spacing": 1, "factory": UNISWAP_V3_FACTORY,
            "created_block": 1, "source": "factory-live",
            "metadata_json": {"discovery_basis": "factory_creation_event"},
        }])
        events = scanner._decode("live", [raw_logs[v3_tx]], {21: blocks[v3_tx]}, resolve_unknown=False)
        app.store.ingest(list(blocks.values()), [
            {**indexed_event(20, "ab"), "protocol": "v4"}, *events,
        ], cursor={"block_number": 21, "block_hash": blocks[v3_tx]["hash"]})
        assert app.book.positions({"pool": pool})["rows"][0]["liquidity"] is None
        scanner._publish_current_block(header(2_000), (), source="test")
        scanner._initialized.set()
        worker = threading.Thread(target=scanner._enrichment_run)
        scanner._threads.append(worker)
        worker.start()
        assert trace_started.wait(2)
        deadline = time.monotonic() + 3
        while True:
            position = app.book.positions({"pool": pool})["rows"][0]
            if position["liquidity"] == "10" or time.monotonic() >= deadline:
                break
            time.sleep(0.01)
        assert (
            position["liquidity"], position["tokens_owed0"], position["tokens_owed1"],
        ) == ("10", "3", "5")
        assert position["coverage"]["history"] == "full"
        assert [row["tx_hash"] for row in app.store.read().execute(
            "SELECT tx_hash FROM pending_enrichment"
        )] == [v4_tx]
    finally:
        release_trace.set()
        scanner.close()
        app.close()


def test_small_trace_refills_rotate_history_requested_and_recent(monkeypatch):
    from concurrent.futures import Future

    monkeypatch.setattr("rhpools.lp_market_index.ENRICH_TRACE_INFLIGHT_LIMIT", 8)
    store = MarketStore(":memory:")
    scanner = indexer(store)
    numbers = [*range(1, 257), 500, 699, 700, 701]
    store.ingest([header(number) for number in numbers], [
        {
            **indexed_event(number, "11"),
            "tx_hash": header(number)["hash"],
            "protocol": "v3" if number == 701 else "v4",
        }
        for number in numbers
    ])
    store.prioritize_enrichment([header(500)["hash"]])
    submitted = {False: [], True: []}

    def capture(lane):
        def submit(_function, rows):
            submitted[lane].append(tuple(rows))
            return Future()

        return submit

    monkeypatch.setattr(scanner._enrichment_executor, "submit", capture(False))
    monkeypatch.setattr(scanner._trace_enrichment_executor, "submit", capture(True))
    occupied = tuple(dict(row) for row in store.read().execute(
        "SELECT * FROM pending_enrichment WHERE block_number<=6 ORDER BY block_number",
    ))
    scanner._enrichment_jobs[tuple(row["tx_hash"] for row in occupied)] = (
        occupied, Future(), time.monotonic(), True,
    )
    try:
        scanner._fill_enrichment_jobs()
        first_trace = tuple(row for batch in submitted[True] for row in batch)
        first_regular = tuple(row for batch in submitted[False] for row in batch)
        assert [row["block_number"] for row in first_trace] == [7, 500]
        assert [row["block_number"] for row in first_regular] == [701]

        first_trace_hashes = tuple(row["tx_hash"] for row in first_trace)
        scanner._enrichment_jobs.pop(first_trace_hashes)
        submitted[True].clear()
        scanner._fill_enrichment_jobs(first_trace_hashes)
        second_trace = tuple(row for batch in submitted[True] for row in batch)
        assert [row["block_number"] for row in second_trace] == [700, 699]
    finally:
        scanner.close()
        store.close()


def test_identity_recovery_waits_for_initialization_and_outlives_blocked_accounting(
    tmp_path, monkeypatch,
):
    store = MarketStore(tmp_path / "market.sqlite")
    bootstrap_started = threading.Event()
    release_bootstrap = threading.Event()
    accounting_started = threading.Event()
    release_accounting = threading.Event()
    accounting_finished = threading.Event()
    recovery_called = threading.Event()
    recovery_initialization = []

    def project_accounting():
        accounting_started.set()
        release_accounting.wait()
        accounting_finished.set()
        return False

    def recover_identities():
        recovery_initialization.append(scanner._initialized.is_set())
        recovery_called.set()
        return False

    scanner = indexer(
        store,
        accounting_projector=project_accounting,
        accounting_recovery=recover_identities,
    )
    bootstrap = scanner._bootstrap

    def blocked_bootstrap():
        bootstrap_started.set()
        release_bootstrap.wait()
        return bootstrap()

    monkeypatch.setattr(scanner, "_bootstrap", blocked_bootstrap)
    workers = ()
    try:
        scanner.start(deferred=True)
        workers = tuple(scanner._threads)
        assert bootstrap_started.wait(2)
        assert accounting_started.wait(2)
        assert not recovery_called.wait(0.1)

        release_bootstrap.set()
        assert recovery_called.wait(2)
        assert recovery_initialization
        assert all(recovery_initialization)
        assert not accounting_finished.is_set()
    finally:
        release_bootstrap.set()
        release_accounting.set()
        scanner.close()
        store.close()

    assert workers
    assert all(not worker.is_alive() for worker in workers)


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
        cursor = store.cursor("history")
        while not cursor["complete"]:
            previous_next = cursor["next_to"]
            assert scanner._scan_history_once() is True
            cursor = store.cursor("history")
            assert previous_next > cursor["next_to"] >= 399
        assert cursor["next_to"] == 399
        assert cursor["complete"] is True
        assert store.status()["pending_enrichment"] == 5_000
    finally:
        scanner.close()
        store.close()


@pytest.mark.parametrize("live_head", [501, 1_100])
def test_recent_ledger_gap_yields_history_until_live_progresses(live_head):
    store = MarketStore(":memory:")
    scanner = indexer(store, StaticRpc(live_head))
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
    scanner._set_runtime("head", head=live_head)
    try:
        assert scanner._scan_history_once() is False
        assert store.cursor("history")["next_to"] == 499
        assert scanner._scan_live_once() is True
        assert store.cursor("live")["block_number"] > 500
        while store.cursor("live")["block_number"] < live_head:
            assert scanner._scan_history_once() is False
            assert scanner._scan_live_once() is True
        assert scanner._scan_history_once() is True
        assert store.cursor("history")["next_to"] < 499
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


def test_requested_identity_replay_keeps_regular_enrichment_interest(monkeypatch):
    store = MarketStore(":memory:")
    scanner = indexer(store)
    identity_marker = scanner._encode_pool_identity_marker({
        "addresses": ["0x" + "12" * 20],
        "logs": [],
    })
    target_tx = "0x" + f"{50:064x}"
    regular_tx = "0x" + f"{51:064x}"
    identity_numbers = [*range(1, 33), 50, *range(68, 100)]
    now = time.time()
    with store.transaction() as connection:
        connection.executemany(
            "INSERT INTO pending_enrichment"
            "(tx_hash,block_number,block_hash,attempts,next_attempt,last_error,"
            "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            [
                (
                    "0x" + f"{number:064x}",
                    number,
                    header(number)["hash"],
                    0,
                    0,
                    identity_marker,
                    now,
                    now,
                )
                for number in identity_numbers
            ] + [
                (
                    regular_tx,
                    51,
                    header(51)["hash"],
                    0,
                    0,
                    None,
                    now,
                    now,
                ),
            ],
        )
        store._set_metadata(
            connection, "pending_enrichment", len(identity_numbers) + 1,
        )
    store.prioritize_enrichment([target_tx, regular_tx])

    def unavailable(_method, _params):
        raise RpcError("identity RPC unavailable")

    monkeypatch.setattr(scanner, "_seed_deferred_v4_pool_identities", lambda: 0)
    monkeypatch.setattr(scanner._clients["pool"], "call", unavailable)
    scanner._pool_identity_replay_turn = 1
    try:
        assert scanner._resolve_deferred_pool_identities_once() is True
        assert [
            row["tx_hash"] for row in store.read().execute(
                "SELECT tx_hash FROM pending_enrichment WHERE next_attempt>0",
            )
        ] == [target_tx]
        assert [
            row["tx_hash"]
            for row in store.requested_enrichments(8, identity=False)
        ] == [regular_tx]
    finally:
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
        store.mark_enrichment_error(tx_hash, queued["last_error"], delay=60)
        waiting = dict(store.read().execute(
            "SELECT * FROM pending_enrichment WHERE tx_hash=?", (tx_hash,),
        ).fetchone())
        assert scanner._resolve_deferred_pool_identities_once() is False
        assert dict(store.read().execute(
            "SELECT * FROM pending_enrichment WHERE tx_hash=?", (tx_hash,),
        ).fetchone()) == waiting
        # Simulate a database populated by an older indexer, before durable
        # V4 identity markers existed. Restart seeding must recover it too.
        with store.transaction() as connection:
            removed = connection.execute(
                "DELETE FROM pending_enrichment WHERE tx_hash=?",
                (tx_hash,),
            ).rowcount
            store._bump(connection, "pending_enrichment", -removed)
        assert scanner._pending_pool_identity_replays() == []
        scanner.close()
        scanner = MarketIndexer(
            store, market, "http://unused.invalid", rpc=IdentityRpc(),
            history_disk_reserve_bytes=0,
        )

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

        def batch(self, calls, *, allow_reverts=False):
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
def test_interval_end_is_rechecked_after_logs_before_commit():
    class FlippingRpc(StaticRpc):
        def call(self, method, params):
            if method == "eth_getLogs":
                return []
            if method == "eth_getBlockByNumber":
                block = header(int(params[0], 16))
                block["hash"] = "0x" + "ff" * 32
                return block
            return super().call(method, params)

        def batch(self, calls, *, allow_reverts=False):
            return [
                header(int(params[0], 16))
                if method == "eth_getBlockByNumber"
                else self.call(method, params)
                for method, params in calls
            ]

    store = MarketStore(":memory:")
    scanner = indexer(store, FlippingRpc())
    try:
        with pytest.raises(CanonicalConflict, match="changed during fetch"):
            scanner._fetch_interval("live", 10, 20)
    finally:
        scanner.close()
        store.close()


def test_metadata_and_balance_batches_isolate_invalid_tokens(tmp_path):
    from rhpools.lp_market_index import (
        BALANCE_OF_SELECTOR, DECIMALS_SELECTOR, SYMBOL_SELECTOR, RpcError,
    )

    token, quote, invalid = (
        "0x" + byte * 20 for byte in ("11", "22", "33")
    )
    healthy_pool, reverting_pool = "0x" + "44" * 20, "0x" + "55" * 20

    class TokenRpc(StaticRpc):
        def call(self, method, params):
            if method != "eth_call":
                return super().call(method, params)
            address, selector = params[0]["to"], params[0]["data"][:10]
            if selector == SYMBOL_SELECTOR:
                return "0x" + {
                    token: b"ASSET", quote: b"USDG", invalid: b"INVALID",
                }[address].ljust(32, b"\x00").hex()
            if selector == DECIMALS_SELECTOR:
                return hex(256 if address == invalid else 6)
            if selector == BALANCE_OF_SELECTOR:
                if address == invalid:
                    raise RpcError("execution reverted")
                return hex(1_000_000)
            raise AssertionError(selector)

    store = MarketStore(tmp_path / "batched-state.sqlite")
    scanner = indexer(store, TokenRpc(), v3_balances=True)
    try:
        store.upsert_pools([
            {
                "id": pool_id, "protocol": "v3", "address": pool_id,
                "token0": asset, "token1": quote,
            }
            for pool_id, asset in (
                (healthy_pool, token), (reverting_pool, invalid),
            )
        ])
        block = header(10)
        store.ingest([block], [])
        scanner._metadata_once()
        assert store.pool(healthy_pool)["decimals0"] == 6
        assert store.pool(reverting_pool)["decimals0"] is None
        assert [
            tuple(row) for row in store.read().execute(
                "SELECT address,attempts FROM pending_token_metadata"
            )
        ] == [(invalid, 1)]

        store.queue_v3_balances(
            [healthy_pool, reverting_pool], 10, block["hash"],
        )
        scanner._balances_once()
        assert [
            row["pool_id"] for row in store.read().execute(
                "SELECT pool_id FROM pool_balances"
            )
        ] == [healthy_pool]
        assert [
            tuple(row) for row in store.read().execute(
                "SELECT pool_id,attempts FROM pending_balances"
            )
        ] == [(reverting_pool, 1)]
        assert store.status()["pending_balances"] == 1
    finally:
        scanner.close()
        store.close()


def test_balance_multicall_keeps_pins_failures_and_zero_distinct(tmp_path):
    from eth_abi import decode as abi_decode, encode as abi_encode

    token, quote, invalid = ("0x" + byte * 20 for byte in ("11", "22", "33"))
    healthy_pool, reverting_pool = ("0x" + byte * 20 for byte in ("44", "55"))
    blocks = {header(number)["hash"]: number for number in (9, 10)}

    class SnapshotRpc(StaticRpc):
        def batch(self, calls, *, allow_reverts=False):
            return [self.call(method, params) for method, params in calls]

        def call(self, method, params):
            if method != "eth_call":
                return super().call(method, params)
            request, pin = params
            assert pin["requireCanonical"] is True
            number = blocks[pin["blockHash"]]
            packed = abi_decode(
                ["(address,bool,bytes)[]"], bytes.fromhex(request["data"][10:]),
            )[0]
            values = []
            for target, _allow_failure, _data in packed:
                if target == invalid and number == 9:
                    values.append((False, b""))
                else:
                    value = 0 if number == 10 and target == token else number * 100
                    values.append((True, value.to_bytes(32, "big")))
            return "0x" + abi_encode(["(bool,bytes)[]"], [values]).hex()

    store = MarketStore(tmp_path / "pinned-balances.sqlite")
    scanner = indexer(store, SnapshotRpc(), v3_balances=True)
    try:
        store.upsert_pools([
            {
                "id": pool_id, "protocol": "v3", "address": pool_id,
                "token0": asset, "token1": quote,
            }
            for pool_id, asset in (
                (healthy_pool, token), (reverting_pool, invalid),
            )
        ])
        store.ingest([header(9), header(10)], [])
        for number in (9, 10):
            store.queue_v3_balances(
                [healthy_pool, reverting_pool], number, header(number)["hash"],
            )
        scanner._balances_once()
        assert [
            tuple(row) for row in store.read().execute(
                "SELECT pool_id,block_number,balance0,balance1 FROM pool_balances "
                "ORDER BY pool_id,block_number"
            )
        ] == [
            (healthy_pool, 9, "900", "900"),
            (healthy_pool, 10, "0", "1000"),
            (reverting_pool, 10, "1000", "1000"),
        ]
        assert [
            tuple(row) for row in store.read().execute(
                "SELECT pool_id,block_number FROM pending_balances"
            )
        ] == [(reverting_pool, 9)]
        metadata = json.loads(store.pool(healthy_pool)["metadata_json"])
        assert (metadata["balance_block"], metadata["balance0"]) == (10, "0")
    finally:
        scanner.close()
        store.close()

def test_balance_lane_ignores_projection_checkpoint_and_isolates_reorg(
    tmp_path, monkeypatch,
):
    token0, token1 = ("0x" + byte * 20 for byte in ("11", "22"))
    healthy_pool, orphaned_pool = ("0x" + byte * 20 for byte in ("33", "44"))
    checkpoint_started = threading.Event()
    release_checkpoint = threading.Event()
    balance_started = threading.Event()
    release_balance = threading.Event()
    created = {}

    class LaneRpc:
        def __init__(self, lane):
            self.lane = lane

        def call(self, method, params):
            if method == "eth_chainId":
                return hex(4663)
            if method == "eth_call":
                return hex(1_000_000)
            raise AssertionError((self.lane, method))

        def batch(self, calls, *, allow_reverts=False):
            if self.lane == "balance":
                balance_started.set()
                assert release_balance.wait(2)
            return [self.call(method, params) for method, params in calls]

        def close(self):
            pass

    def factory(lane):
        client = LaneRpc(lane)
        created.setdefault(lane, []).append(client)
        return client

    def blocked_checkpoint():
        checkpoint_started.set()
        assert release_checkpoint.wait(2)

    store = MarketStore(tmp_path / "balance-reorg.sqlite")
    scanner = indexer(store, factory, v3_balances=True)
    healthy_block = header(10)
    orphaned_block = header(11, parent=healthy_block["hash"])
    store.upsert_pools([
        {
            "id": pool_id, "protocol": "v3", "address": pool_id,
            "token0": token0, "token1": token1,
        }
        for pool_id in (healthy_pool, orphaned_pool)
    ])
    store.ingest([healthy_block, orphaned_block], [])
    store.queue_v3_balances([healthy_pool], 10, healthy_block["hash"])
    store.queue_v3_balances([orphaned_pool], 11, orphaned_block["hash"])
    monkeypatch.setattr(store, "checkpoint", blocked_checkpoint)
    scanner._initialized.set()
    workers = [
        threading.Thread(target=scanner._projection_run),
        threading.Thread(target=scanner._balance_run),
    ]
    scanner._threads.extend(workers)
    for worker in workers:
        worker.start()
    try:
        assert checkpoint_started.wait(1)
        assert balance_started.wait(1), (
            "balance RPC waited for the projection checkpoint"
        )
        store.rollback(10)
        release_balance.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and store.status()["pending_balances"]:
            time.sleep(0.01)
        assert [
            tuple(row) for row in store.read().execute(
                "SELECT pool_id,block_number FROM pool_balances "
                "ORDER BY pool_id,block_number"
            )
        ] == [(healthy_pool, 10)]
        assert store.status()["pending_balances"] == 0
    finally:
        release_balance.set()
        release_checkpoint.set()
        scanner.close()
        store.close()


def test_v4_receipt_trace_and_pinned_state_produce_exact_financial_evidence():
    from eth_abi import encode
    from eth_utils import keccak
    from rhpools.lp_market_protocols import (
        MODIFY_LIQUIDITY_SELECTOR,
        STATE_VIEW_LIQUIDITY_SELECTOR,
        STATE_VIEW_POSITION_SELECTOR,
        STATE_VIEW_SLOT0_SELECTOR,
        V4_POSITION_MANAGER,
    )

    token0, token1 = ("0x" + byte * 20 for byte in ("11", "22"))
    owner = "0x" + "33" * 20
    hook = "0x" + "00" * 20
    fee, spacing, token_id = 3_000, 8, 42
    observed = header(10)
    tx_hash = "0x" + "44" * 32
    pool_id = "0x" + keccak(encode(
        ["address", "address", "uint24", "int24", "address"],
        [token0, token1, fee, spacing, hook],
    )).hex()

    def topic(value):
        return "0x" + f"{value:064x}"

    common = {
        "blockNumber": observed["number"],
        "blockHash": observed["hash"],
        "transactionHash": tx_hash,
        "transactionIndex": "0x0",
    }
    core = {
        **common,
        "address": POOL_MANAGER,
        "logIndex": "0x0",
        "topics": [
            V4_MODIFY_LIQUIDITY_TOPIC,
            pool_id,
            topic(int(V4_POSITION_MANAGER, 16)),
        ],
        "data": "0x" + encode(
            ["int24", "int24", "int256", "bytes32"],
            [-60, 60, 100, token_id.to_bytes(32, "big")],
        ).hex(),
    }
    transfer = {
        **common,
        "address": V4_POSITION_MANAGER,
        "logIndex": "0x1",
        "topics": [
            TRANSFER_TOPIC,
            topic(0),
            topic(int(owner, 16)),
            topic(token_id),
        ],
        "data": "0x",
    }
    calldata = MODIFY_LIQUIDITY_SELECTOR + encode(
        [
            "(address,address,uint24,int24,address)",
            "(int24,int24,int256,bytes32)",
            "bytes",
        ],
        [
            (token0, token1, fee, spacing, hook),
            (-60, 60, 100, token_id.to_bytes(32, "big")),
            b"",
        ],
    ).hex()
    mask = (1 << 128) - 1

    def balance_delta(amount0, amount1):
        return (
            ((amount0 & mask) << 128) | (amount1 & mask)
        ).to_bytes(32, "big").hex()

    trace = {
        "type": "CALL",
        "from": V4_POSITION_MANAGER,
        "to": POOL_MANAGER,
        "input": calldata,
        "output": "0x" + balance_delta(-1_000, -2_000) + balance_delta(3, 5),
        "logs": [core],
    }
    receipt = {
        "transactionHash": tx_hash,
        "blockNumber": observed["number"],
        "blockHash": observed["hash"],
        "transactionIndex": "0x0",
        "status": "0x1",
        "from": owner,
        "gasUsed": hex(21_000),
        "effectiveGasPrice": hex(1_000_000_000),
        "logs": [core, transfer],
    }

    class EnrichmentRpc(StaticRpc):
        def call(self, method, params):
            if method == "eth_getTransactionReceipt":
                return receipt
            if method == "eth_getTransactionByHash":
                raise AssertionError("complete receipt must not fetch a body")
            if method == "debug_traceTransaction":
                return trace
            if method == "eth_getBlockByNumber":
                return observed
            if method == "eth_call":
                calldata_ = params[0]["data"]
                if calldata_.startswith(STATE_VIEW_POSITION_SELECTOR):
                    values = [100, 1, 2]
                elif calldata_.startswith(STATE_VIEW_SLOT0_SELECTOR):
                    values = [1 << 96, 0, 0, fee]
                elif calldata_.startswith(STATE_VIEW_LIQUIDITY_SELECTOR):
                    values = [1_000]
                else:
                    raise AssertionError(calldata_)
                return "0x" + encode(
                    ["uint256"] * len(values), values,
                ).hex()
            return super().call(method, params)

    store = MarketStore(":memory:")
    rpc = EnrichmentRpc()
    scanner = indexer(store, rpc)
    try:
        store.upsert_pools([{
            "id": pool_id,
            "protocol": "v4",
            "address": POOL_MANAGER,
            "token0": token0,
            "token1": token1,
            "fee_ppm": fee,
            "tick_spacing": spacing,
            "hook": hook,
            "factory": POOL_MANAGER,
            "source": "test",
        }])
        pending = {
            "tx_hash": tx_hash,
            "block_number": 10,
            "block_hash": observed["hash"],
            "attempts": 0,
            "generation": 0,
        }
        _row, events, gas, error = scanner._enrich_transactions([pending])[0]
        assert error is None
        action = next(row for row in events if row["protocol"] == "v4")
        assert (
            action["cashflow0"],
            action["cashflow1"],
            action["fee_amount0"],
            action["fee_amount1"],
        ) == ("-1000", "-2000", "3", "5")
        assert action["data"]["trace_complete"] is True
        assert action["data"]["position_before"]["absence_basis"] == (
            "same_receipt_verified_v4_manager_mint"
        )
        assert action["data"]["position_after"]["liquidity"] == "100"
        assert action["data"]["pool_state_before"]["liquidity"] == "1000"
        assert gas["payer"] == owner
        assert gas["gas_native"] == str(21_000 * 1_000_000_000)
    finally:
        scanner.close()
        store.close()


def test_v4_zero_state_requires_same_receipt_burn_proof():
    from eth_abi import encode
    from rhpools.lp_market_protocols import (
        V4_POSITION_MANAGER,
        decode_position_state_results,
        nft_position_key,
        position_state_requests,
    )

    owner = "0x" + "11" * 20
    block_hash = "0x" + "22" * 32
    tx_hash = "0x" + "33" * 32
    pool_id = "0x" + "44" * 32
    core_key = "0x" + "55" * 32
    token_id = 42
    position_key = nft_position_key(V4_POSITION_MANAGER, token_id)
    common = {
        "block_number": 10,
        "block_hash": block_hash,
        "tx_hash": tx_hash,
        "tx_index": 0,
        "timestamp": 1_000,
        "pool_id": pool_id,
        "owner": owner,
        "custody": V4_POSITION_MANAGER,
        "position_key": position_key,
        "token_id": str(token_id),
        "tick_lower": -60,
        "tick_upper": 60,
    }
    action = {
        **common,
        "protocol": "v4",
        "kind": "remove",
        "log_index": 0,
        "liquidity_delta": "-100",
        "data": {"core_position_key": core_key},
    }
    burn = {
        **common,
        "protocol": "nft",
        "kind": "transfer",
        "log_index": 1,
        "liquidity_delta": None,
        "data": {
            "manager_protocol": "v4",
            "prior_owner": owner,
            "new_owner": "0x" + "00" * 20,
            "burn": True,
            "core_position_key": core_key,
        },
    }
    requests = [
        request
        for request in position_state_requests([action, burn])
        if request["correlation"]["decoder"] == "v4_state_view_position"
    ]
    results = [
        "0x" + encode(
            ["uint128", "uint256", "uint256"],
            [100, 1, 2]
            if request["correlation"]["field"] == "position_before"
            else [0, 0, 0],
        ).hex()
        for request in requests
    ]
    updates = decode_position_state_results(requests, results)
    assert updates
    for update in updates:
        assert update["data"]["position_after"]["exists"] is None
        assert update["data"]["position_after"]["nft_exists"] is False
        assert update["data"]["position_after"]["nft_absence_basis"] == (
            "same_receipt_verified_v4_manager_burn"
        )

    unproven_requests = [
        request
        for request in position_state_requests([action])
        if request["correlation"]["decoder"] == "v4_state_view_position"
    ]
    unproven = decode_position_state_results(
        unproven_requests,
        ["0x" + encode(
            ["uint128", "uint256", "uint256"], [0, 0, 0],
        ).hex()] * len(unproven_requests),
    )
    assert unproven[0]["data"]["position_after"]["exists"] is None
    assert "nft_exists" not in unproven[0]["data"]["position_after"]




