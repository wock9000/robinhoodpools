"""Background work must preserve the current-index latency budget."""

import threading
from types import SimpleNamespace

from rhpools.lp_market_index import (
    HISTORY_MAX_INTERVAL_STORE_SECONDS,
    MarketIndexer,
)


def test_background_work_uses_adaptive_live_batch_as_recent_gap_threshold():
    index = MarketIndexer.__new__(MarketIndexer)
    cursor = {"block_number": 1000}
    index.store = SimpleNamespace(cursor=lambda _lane: cursor)
    index._feed_condition = threading.Condition()
    index._status_lock = threading.RLock()
    index._observed_last_header = None
    index._live_chunk = 64
    deep = index._live_chunk  # Without timestamps, use the conservative block budget.
    index._runtime_status = {"head": 1000 + deep + 1}

    assert index._recent_catchup_pending()
    assert index._runtime_status["recent_catchup_lag_blocks"] == deep + 1
    assert index._runtime_status["history_scheduling"] == "recent_gap_first"

    index._runtime_status["head"] = 1000 + deep
    assert not index._recent_catchup_pending()
    assert index._runtime_status["history_scheduling"] == "concurrent"

    index._runtime_status["head"] = 1002
    assert not index._recent_catchup_pending()

    cursor["timestamp"] = 100
    index._runtime_status.update({"head": 1064, "head_timestamp": 131})
    assert index._recent_catchup_pending()
    assert index._runtime_status["recent_catchup_lag_seconds"] == 31

    index._runtime_status["head_timestamp"] = 130
    assert not index._recent_catchup_pending()


def test_history_rechecks_recent_gap_after_rpc_preparation():
    from rhpools.lp_market_store import MarketStore

    def header(number):
        return {
            "number": hex(number),
            "hash": "0x" + f"{number:064x}",
            "parentHash": "0x" + f"{number - 1:064x}",
            "timestamp": hex(1_700_000_000 + number),
        }

    class RacingRpc:
        def call(self, method, params):
            if method == "eth_getLogs":
                index._set_runtime("head", head=2_000)
                return []
            if method == "eth_getBlockByNumber":
                return header(int(params[0], 16))
            if method == "eth_chainId":
                return hex(4663)
            raise AssertionError(method)

        def batch(self, calls):
            return [self.call(method, params) for method, params in calls]

    store = MarketStore(":memory:")
    market = SimpleNamespace(
        universe=SimpleNamespace(tokens={}),
        _pool_by_id=lambda _pool_id: None,
    )
    index = MarketIndexer(
        store,
        market,
        "http://unused.invalid",
        rpc=RacingRpc(),
        history_disk_reserve_bytes=0,
    )
    index._history_verified = True
    anchor = header(500)
    store.ingest(
        [anchor],
        [],
        lane="live",
        cursor={
            "block_number": 500,
            "block_hash": anchor["hash"],
            "timestamp": int(anchor["timestamp"], 16),
        },
    )
    store.ingest(
        [anchor],
        [],
        lane="history",
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
    index._set_runtime("head", head=500)
    try:
        assert index._scan_history_once() is False
        assert store.cursor("history")["next_to"] == 499
    finally:
        index.close()
        store.close()


def test_background_batch_growth_respects_observed_write_time():
    index = MarketIndexer.__new__(MarketIndexer)
    index._clients = {}
    sample_blocks = 8
    sample_seconds = HISTORY_MAX_INTERVAL_STORE_SECONDS * 0.9
    index._history_chunk = sample_blocks
    index._clients = {}

    index._resize_after_success("history", 128, sample_seconds, sample_blocks)

    predicted_seconds = index._history_chunk * sample_seconds / sample_blocks
    assert predicted_seconds <= HISTORY_MAX_INTERVAL_STORE_SECONDS
