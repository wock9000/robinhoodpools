import threading
import time
from collections import OrderedDict
from dataclasses import replace
from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_utils import keccak

from rhpools import workbench_market as market


POOL = market._Pool(
    "0x" + "11" * 20, "0x" + "11" * 20, "v3",
    "0x" + "22" * 20, "0x" + "33" * 20, 500, 10,
)


def header(number, block_byte, parent_byte, timestamp):
    return {
        "number": hex(number),
        "hash": "0x" + block_byte * 64,
        "parentHash": "0x" + parent_byte * 64,
        "timestamp": hex(timestamp),
    }


def service():
    instance = object.__new__(market.MarketService)
    instance._invalid_index_pools = set()
    instance._selection_wake = threading.Event()
    instance._lock = threading.RLock()
    instance._detail_cond = threading.Condition(instance._lock)
    instance._detail_revision = 0
    instance._current_blocks = OrderedDict()
    instance._current_observation_epoch = 1
    instance._current_observation_error = None
    instance._head = None
    instance._head_read_at = 0.0
    instance._head_error = None
    instance._chain_verified = False
    instance._selected = OrderedDict()
    instance._page_metadata = OrderedDict()
    instance._board = {}
    instance._board_health = {"state": "disabled"}
    instance.universe = SimpleNamespace(tokens={
        POOL.token0: market._Token("USDG", 6, "chain"),
        POOL.token1: market._Token("NVDA", 18, "chain"),
    })
    return instance


def test_stream_cursor_survives_pool_eviction_and_reselection(monkeypatch):
    monkeypatch.setattr(market, "MAX_SELECTIONS", 1)
    instance = service()
    instance._stop = threading.Event()
    instance._discovered = {}
    another = replace(POOL, id="0x" + "44" * 20, address="0x" + "44" * 20)
    instance.universe.by_id = {POOL.id: POOL, another.id: another}
    first = instance._selection(POOL.id, None)
    instance._publish_detail(first, {"block": 100, "revision": 1})
    cursor = instance.detail(POOL.id)["revision"]
    instance._selection(another.id, None)
    replacement = instance._selection(POOL.id, None)
    instance._publish_detail(replacement, {"block": 102, "revision": 1})
    update = instance.wait_detail(POOL.id, None, cursor, timeout=0)
    assert update is not None
    assert update["block"] == 102
    assert update["revision"] > cursor


def test_bounded_pool_log_reads_preserve_both_sides_of_chunk_boundaries():
    events = [
        {"blockNumber": hex(block), "transactionIndex": "0x0", "logIndex": "0x0"}
        for block in (100, 227, 228, 355, 356, 358)
    ]

    class BoundedRpc:
        def call(self, method, params):
            query = params[0]
            first, last = int(query["fromBlock"], 16), int(query["toBlock"], 16)
            if last - first >= 128:
                raise market.RpcError("historical request exceeded the responsive log window")
            return list(reversed([
                event for event in events if first <= int(event["blockNumber"], 16) <= last
            ]))

    instance = service()
    instance.rpc = BoundedRpc()
    result = instance._logs(market._Selection(POOL, None), 100, 358)
    assert result == events


def test_pair_search_accepts_both_quote_orientations_and_protocol_terms():
    instance = service()
    assert instance._matches(POOL, "nvda/usdg", None)
    assert instance._matches(POOL, "usdg / nvda v3", None)
    assert not instance._matches(POOL, "nvda / weth", None)
    assert not instance._matches(POOL, "nvda", "v4")


def test_index_publication_wakes_cold_pool_and_preserves_partial_metadata():
    instance = service()
    instance._discovered = {}
    instance._catalog_cache = OrderedDict()
    instance._index_publication_revision = 0
    instance.universe.by_id = {}
    token0 = "0x" + "66" * 20
    token1 = "0x" + "77" * 20
    manager = "0x" + "55" * 20
    pool_id = market._v4_pool_id(market._Pool(
        "", manager, "v4", token0, token1, 3000, 60,
        "0x" + "00" * 20,
    ))
    result = []
    waiter = threading.Thread(
        target=lambda: result.append(instance.wait_pool(pool_id, 1)),
    )
    waiter.start()
    instance.register_index_pool({
        "id": pool_id,
        "address": manager,
        "protocol": "v4",
        "token0": token0,
        "token1": token1,
        "symbol0": "ASSET",
        "symbol1": None,
        "decimals0": None,
        "decimals1": 18,
        "fee_ppm": 3000,
        "tick_spacing": 60,
        "hook": "0x" + "00" * 20,
        "factory": manager,
        "source": "durable-index",
        "metadata_json": {"dynamic_fee": False},
    })
    waiter.join(1)
    assert result[0].id == pool_id
    row = instance._catalog_row(result[0], {}, time.time())
    assert row["pair"] == f"ASSET/{token1}"
    assert row["token0"] == {
        "address": "0x" + "66" * 20,
        "symbol": "ASSET",
        "decimals": None,
        "metadata_state": "partial",
        "metadata_source": "durable-index",
    }
    assert row["token1"]["address"] == "0x" + "77" * 20
    assert row["token1"]["symbol"] is None
    assert row["token1"]["metadata_state"] == "partial"


def test_absence_from_ranked_board_is_unknown_not_zero_activity():
    instance = service()
    instance._board_health = {"state": "live", "uptime_s": 100_000}
    row = instance._catalog_row(POOL, {}, 1_000_000)
    assert row["state"] == "unobserved"
    assert row["swaps_1h"] is None
    assert row["tvl_usd"] is None


@pytest.mark.parametrize("snapshot_age,block_age", [(10, 0), (0, 30)])
def test_cached_pool_detail_stays_readable_without_republishing_for_wall_clock_age(
    snapshot_age, block_age,
):
    instance = service()
    instance._discovered = {}
    instance.universe.by_id = {POOL.id: POOL}
    selection = market._Selection(POOL, None)
    selection.snapshot = {
        "as_of": time.time() - snapshot_age, "block": 123,
        "block_timestamp": time.time() - block_age,
        "pool": {"id": POOL.id}, "health": {"state": "live", "error": None},
        "liquidity": {"active": "100", "curve": []},
    }
    instance._selected = OrderedDict([((POOL.id, None), selection)])
    result = []
    completed = threading.Event()

    def read_snapshot():
        try:
            result.append(instance.detail(POOL.id))
        finally:
            completed.set()

    selection.lock.acquire()
    reader = threading.Thread(target=read_snapshot)
    reader.start()
    try:
        assert completed.wait(2), "An RPC writer blocked the cached pool response"
        assert result[0]["block"] == 123
        assert result[0]["health"]["state"] == "live"
        assert result[0]["liquidity"]["active"] == "100"
    finally:
        selection.lock.release()
        reader.join(timeout=2)


def test_status_revision_changes_once_per_actual_publication():
    instance = service()
    selection = market._Selection(POOL, None)
    instance._detail_revision = 5
    selection.snapshot_revision = 5
    selection.snapshot = {
        "revision": 5,
        "as_of": 1.0,
        "pool": {"id": POOL.id, "state": "live"},
        "health": {
            "state": "live",
            "error": None,
            "reorgs": 0,
            "last_reorg": None,
            "refresh_failures": 0,
            "reconnects": 0,
        },
    }

    instance._mark_selection_error(selection, "stale", "head stopped")
    published = selection.snapshot["as_of"]
    cursor = selection.snapshot["revision"]
    assert cursor > 5
    assert published > 1.0

    instance._mark_selection_error(selection, "stale", "head stopped")
    assert selection.snapshot["revision"] == cursor
    assert selection.snapshot["as_of"] == published


def test_selection_scheduler_prioritizes_visible_pool_and_pauses_hidden_cache():
    instance = service()
    now = time.monotonic()
    hidden = market._Selection(POOL, None)
    hidden.last_used = now - market.SELECTION_ACTIVE_S - 0.1
    hidden.snapshot = {"block": 1}
    current_pool = replace(POOL, id="0x" + "44" * 20, address="0x" + "44" * 20)
    current = market._Selection(current_pool, None)
    current.last_used = now - 0.1
    instance._selected = OrderedDict([
        ((hidden.pool.id, None), hidden),
        ((current.pool.id, None), current),
    ])

    assert instance._next_selection(now) is current
    current.last_refresh = now
    assert instance._next_selection(now) is None
    assert any(selection is hidden for selection in instance._selected.values())

    hidden.last_used = now
    assert instance._next_selection(now) is hidden


def test_hot_view_cannot_starve_another_pools_first_load_or_refresh():
    instance = service()
    now = time.monotonic()
    cold = market._Selection(POOL, None)
    cold.last_used = now - 0.1
    hot_pool = replace(POOL, id="0x" + "44" * 20, address="0x" + "44" * 20)
    hot = market._Selection(hot_pool, None)
    hot.last_used = now
    hot.last_refresh = now - 1
    hot.snapshot = {"block": 1}
    instance._selected = OrderedDict([
        ((cold.pool.id, None), cold),
        ((hot.pool.id, None), hot),
    ])

    assert instance._next_selection(now) is cold
    cold.snapshot = {"block": 2}
    cold.last_refresh = now
    assert instance._next_selection(now + 0.1) is hot
    hot.last_refresh = now + 0.1
    hot.last_used = now + 0.5
    instance._selected.move_to_end((hot.pool.id, None))
    assert instance._next_selection(now + 0.5) is cold


def test_slow_head_refresh_is_cached_from_rpc_completion(monkeypatch):
    instance = service()
    instance._head = None
    instance._head_read_at = 0.0
    instance._head_error = None
    instance._chain_verified = False
    rpc_batches = []

    class Rpc:
        def batch(self, calls):
            rpc_batches.append(calls)
            return [hex(market.CHAIN_ID), {
                "number": "0x64",
                "hash": "0x" + "55" * 32,
                "timestamp": "0x3e8",
            }]

    instance.rpc = Rpc()
    times = iter((10.0, 16.0, 16.1))
    monkeypatch.setattr(market.time, "monotonic", lambda: next(times))

    instance._refresh_head()
    assert instance._head_header()["number"] == "0x64"
    assert len(rpc_batches) == 1


def test_missing_dynamic_v4_spacing_is_recovered_from_complete_pool_key_hash():
    pool_id = "0x9b6604eeffbad3b216199d63cad1b46d9665c1429207e1c442531fb7263d28a2"
    token0 = "0x322f0929c4625ed5bad873c95208d54e1c003b2d"  # gitleaks:allow -- public ERC-20 address
    token1 = "0xfe7e4b4850979ba7920ce786493b7371761f1e18"  # gitleaks:allow -- public ERC-20 address
    hook = "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544"
    census = market._Pool(
        pool_id, market.POOL_MANAGER, "v4", token0, token1,
        None, None, hook, True, market.POOL_MANAGER,
    )

    recovered = market._resolve_v4_pool_key(census)

    assert recovered.tick_spacing == 8
    assert market._v4_pool_id(recovered) == pool_id
    with pytest.raises(market.RpcError):
        market._resolve_v4_pool_key(replace(census, tick_spacing=10))
    with pytest.raises(market.RpcError):
        market._resolve_v4_pool_key(replace(census, token1="0x" + "44" * 20))


def test_unavailable_tick_bitmap_is_not_published_as_zero_liquidity():
    instance = service()
    instance._pinned_state_batch = lambda calls, _block_tag: ["0x"] * len(calls)

    with pytest.raises(market.RpcError, match="tick bitmap is malformed"):
        instance._load_curve_state(market._Selection(POOL, None), 0, "0x64")


def test_broad_spacing_curve_clips_bitmap_edges_to_tickmath_domain_with_real_depth():
    pool = market._Pool(
        "0xaea2f72582d266e691adaf034020c3a807862542f74084390a48990215b2302f",
        market.POOL_MANAGER,
        "v4",
        POOL.token0,
        POOL.token1,
        800000,
        8000,
        market.NATIVE,
    )
    instance = service()
    selection = market._Selection(pool, None)
    instance._selection_superseded = lambda _selection: False
    bitmap_batches = iter((
        [
            f"0x{1 << 146:064x}",
            f"0x{1 << 110:064x}",
        ],
        [
            f"0x{100:064x}{100:064x}",
            f"0x{100:064x}{(-100) % (1 << 256):064x}",
        ],
    ))
    instance._pinned_state_batch = lambda _calls, _block_tag: next(bitmap_batches)

    assert instance._load_curve_state(selection, 0, "0x64", (-1, 0))
    curve = instance._concentrated_curve(selection, {"tick": 0, "liquidity": 100})

    assert [(point["tick"], point["liquidity"]) for point in curve] == [
        (-market.MAX_TICK, "0"),
        (-880000, "100"),
        (0, "100"),
        (880000, "0"),
        (market.MAX_TICK, "0"),
    ]


def test_one_minute_metrics_require_timestamp_coverage_and_use_stable_leg():
    pool = replace(POOL, token0=market.USDG)
    instance = service()
    instance.universe.tokens = {
        pool.token0: market._Token("USDG", 6, "chain"),
        pool.token1: market._Token("NVDA", 18, "chain"),
    }
    selection = market._Selection(pool, None)
    selection.cursor = 1
    selection.cursor_hash = "0x" + "55" * 32
    selection.swap_coverage_start_block = 1
    selection.swap_coverage_start_timestamp = 950
    selection.swap_coverage_through_block = 1
    selection.swap_coverage_through_hash = selection.cursor_hash
    selection.swaps.append({
        "timestamp": 970,
        "amount0_raw": "100000000",
        "amount1_raw": "-1000000000000000000",
        "fee_ppm": 500,
        "tick": 0,
        "active_liquidity": "1000",
    })

    warming, coverage = instance._lp_summary(
        selection, {"liquidity": 1000}, [], 0, 1000, 1.0, 1000,
    )
    assert coverage["swaps_1m"]["covered_span_s"] == 50
    assert not coverage["swaps_1m"]["complete"]
    assert warming["swaps_1m"] is None
    assert warming["volume_1m_usd"] is None
    assert warming["pool_fees_1m_usd"] is None

    selection.swap_coverage_start_timestamp = 900
    complete, coverage = instance._lp_summary(
        selection, {"liquidity": 1000}, [], 0, 1000, 1.0, 1000,
    )
    assert coverage["swaps_1m"]["complete"]
    assert complete["swaps_1m"] == 1
    assert complete["volume_1m_usd"] == 100.0
    assert complete["pool_fees_1m_usd"] == 0.05


def test_verified_current_swap_is_visible_before_exact_history_and_reorg_resets_it():
    pool = replace(POOL, token0=market.USDG)
    instance = service()
    instance.universe.tokens = {
        pool.token0: market._Token("USDG", 6, "chain"),
        pool.token1: market._Token("NVDA", 18, "chain"),
    }
    first = header(100, "a", "9", 939)
    current = header(101, "b", "a", 1_000)
    instance.observe_current_block(first)
    instance.observe_current_events(first, [])
    instance.observe_current_block(current)
    event = {
        "block_number": 101,
        "block_hash": current["hash"],
        "tx_hash": "0x" + "c" * 64,
        "tx_index": 2,
        "log_index": 3,
        "timestamp": 1_000,
        "pool_id": pool.id,
        "protocol": "v3",
        "kind": "swap",
        "amount0": "100000000",
        "amount1": "-1000000000000000000",
        "sqrt_price_x96": str(market.Q96),
        "liquidity": "1000",
        "tick": 0,
        "fee_ppm": None,
        "data": {},
    }
    instance.observe_current_events(current, [event])
    selection = market._Selection(pool, None)
    selection.cursor = 101
    selection.cursor_hash = current["hash"]
    selection.history_error = (
        "recent event history is loading off the live state lane"
    )

    assert instance._sync_current_events(selection, 101)
    assert selection.lp_events[0]["ours"] is False
    live, coverage = instance._lp_summary(
        selection, {"liquidity": 1_000}, [], 0, 1_000, 1.0, 1_000,
    )
    assert live["swaps_1m"] == 1
    assert live["volume_1m_usd"] == 100.0
    assert live["pool_fees_1m_usd"] is None
    assert not coverage["swaps_1m"]["complete"]
    assert (
        coverage["swaps_1m"]["qualification"]
        == "verified_current_observations_lower_bound"
    )

    instance.observe_current_events(current, [{
        **event,
        "fee_ppm": 500,
        "data": {"enriched": True},
    }])
    instance._sync_current_events(selection, 101)
    assert len(selection.swaps) == 1
    enriched, _coverage = instance._lp_summary(
        selection, {"liquidity": 1_000}, [], 0, 1_000, 1.0, 1_000,
    )
    assert enriched["pool_fees_1m_usd"] == 0.05

    replacement = header(101, "d", "a", 1_000)
    instance.observe_current_block(replacement)
    with pytest.raises(market.ReorgDetected, match="same-height"):
        instance._sync_current_events(selection, 101)


def test_v4_activity_preserves_unknown_amounts_and_late_enrichment_identity():
    instance = service()
    pool = replace(POOL, id="0x" + "44" * 32, address=market.POOL_MANAGER, kind="v4")
    selection = market._Selection(pool, None)
    current = header(101, "b", "a", 1_000)
    custody = "0x" + "55" * 20
    log = {
        "address": market.POOL_MANAGER,
        "blockNumber": "0x65",
        "blockHash": current["hash"],
        "transactionHash": "0x" + "c" * 64,
        "transactionIndex": "0x2",
        "logIndex": "0x3",
        "topics": [market.V4_MODIFY_TOPIC, pool.id, "0x" + custody[2:].zfill(64)],
        "data": "0x" + encode(
            ["int24", "int24", "int256", "bytes32"], [-20, 20, 1000, bytes(32)],
        ).hex(),
    }
    historical = instance._derive_lp_events(selection, [log], {101: 1_000})
    assert [(row["kind"], row["lo"], row["hi"]) for row in historical] == [
        ("add", -20, 20),
    ]
    assert historical[0]["amount0"] is None
    assert historical[0]["amount1_raw"] is None
    instance._merge_lp_events(selection, historical)
    event = {
        "block_number": 101, "block_hash": current["hash"], "timestamp": 1_000,
        "tx_hash": log["transactionHash"], "tx_index": 2, "log_index": 3,
        "pool_id": pool.id, "protocol": "v4", "kind": "add", "custody": custody,
        "tick_lower": -20, "tick_upper": 20, "liquidity_delta": "1000",
        "amount0": None, "amount1": None, "data": {},
    }
    instance.observe_current_block(current)
    instance.observe_current_events(current, [
        event,
        {**event, "pool_id": "0x" + "66" * 32, "log_index": 4, "amount0": "999000000"},
    ])
    instance._sync_current_events(selection, 101)
    following = header(102, "d", "b", 1_001)
    instance.observe_current_block(following)
    instance.observe_current_events(following, [])
    instance._sync_current_events(selection, 102)
    instance.observe_current_events(current, [{**event, "amount0": "1500000", "amount1": "0"}])
    instance._sync_current_events(selection, 102)
    instance._merge_lp_events(selection, historical)
    assert len(selection.lp_events) == 1
    assert selection.lp_events[0]["amount0"] == "1.5"
    assert selection.lp_events[0]["amount1"] == "0"
    assert selection.lp_events[0]["ours"] is False


def test_lifecycle_event_reloads_shape_but_ordinary_swap_reuses_tick_topology():
    instance = service()
    prior = header(100, "a", "9", 1_000)
    lifecycle_head = header(101, "b", "a", 1_001)
    swap_head = header(102, "c", "b", 1_002)
    headers = {100: prior, 101: lifecycle_head, 102: swap_head}

    class Rpc:
        def call(self, method, params):
            assert method == "eth_getBlockByNumber"
            return headers[int(params[0], 16)]

    selection = market._Selection(POOL, None)
    selection.cursor = 100
    selection.cursor_hash = prior["hash"]
    selection.core = {
        "sqrt": market.Q96,
        "tick": 0,
        "liquidity": 100,
    }
    selection.core_block = 100
    selection.core_hash = prior["hash"]
    selection.curve_words = instance._viewport_words(selection, 0)
    selection.curve_state_block = 100
    selection.curve_state_hash = prior["hash"]
    selection.curve_verified_through_block = 100
    selection.tick_net = {-20: 100, 20: -100}
    selection.snapshot = {"liquidity": {"active": "100"}}
    instance.rpc = Rpc()
    instance._selection_superseded = lambda _selection: False
    current_header = [lifecycle_head]
    instance._head_header = lambda: current_header[0]
    cores = {
        101: {"sqrt": market.Q96, "tick": 0, "liquidity": 100},
        102: {
            "sqrt": market.sqrt_ratio_at_tick(30),
            "tick": 30,
            "liquidity": 0,
        },
    }
    instance._read_core = (
        lambda _selection, block_tag, seed: cores[int(block_tag, 16)]
    )
    loads = []

    def load_curve(_selection, _tick, block_tag, words=None):
        loads.append(block_tag)
        _selection.curve_words = (
            instance._viewport_words(_selection, _tick)
            if words is None else words
        )
        _selection.tick_net = {
            -100: 50,
            -50: -50,
            -20: 100,
            20: -100,
        }
        return True

    instance._load_curve_state = load_curve
    instance._snapshot = lambda selected, observed, core: {
        "block": int(observed["number"], 16),
        "liquidity": {
            "active": str(core["liquidity"]),
            "curve": instance._concentrated_curve(selected, core),
        },
    }
    instance.observe_current_block(lifecycle_head)
    instance.observe_current_events(lifecycle_head, [{
        "block_number": 101,
        "block_hash": lifecycle_head["hash"],
        "tx_hash": "0x" + "d" * 64,
        "tx_index": 0,
        "log_index": 1,
        "timestamp": 1_001,
        "pool_id": POOL.id,
        "protocol": "v3",
        "kind": "add",
        "custody": "0x" + "44" * 20,
        "tick_lower": -100,
        "tick_upper": -50,
        "liquidity_delta": "50",
        "amount0": "1",
        "amount1": "0",
    }])

    assert instance._advance_selection(selection)
    first_curve = selection.snapshot["liquidity"]["curve"]
    assert (-100, "50") in [
        (point["tick"], point["liquidity"]) for point in first_curve
    ]
    assert loads == ["0x65"]

    current_header[0] = swap_head
    instance.observe_current_block(swap_head)
    instance.observe_current_events(swap_head, [{
        "block_number": 102,
        "block_hash": swap_head["hash"],
        "tx_hash": "0x" + "e" * 64,
        "tx_index": 0,
        "log_index": 1,
        "timestamp": 1_002,
        "pool_id": POOL.id,
        "protocol": "v3",
        "kind": "swap",
        "amount0": "1",
        "amount1": "-1",
        "sqrt_price_x96": str(market.sqrt_ratio_at_tick(30)),
        "liquidity": "0",
        "tick": 30,
        "fee_ppm": 500,
        "data": {},
    }])

    assert instance._advance_selection(selection)
    assert loads == ["0x65"]
    assert selection.snapshot["block"] == 102
    assert selection.snapshot["liquidity"]["curve"] != first_curve
    assert selection.curve_verified_through_block == 101


def test_lp_allocation_value_respects_inverted_usdg_token0_quote():
    pool = replace(POOL, token0=market.USDG)
    instance = service()
    instance.universe.tokens = {
        pool.token0: market._Token("USDG", 6, "chain"),
        pool.token1: market._Token("NVDA", 18, "chain"),
    }
    selection = market._Selection(pool, "0x" + "44" * 20)
    selection.history_complete = True
    selection.position_state[(-10, 10)] = (100, 0, 0)
    selection.swap_coverage_start_timestamp = 900
    positions = [{
        "amount0_raw": "100000000",
        "amount1_raw": "1000000000000000000",
    }]

    lp, _coverage = instance._lp_summary(
        selection, {"liquidity": 1000}, positions, 0, 1000, 0.005, 1000,
    )

    assert lp["our_active_liquidity"] == "100"
    assert lp["active_share_pct"] == 10.0
    assert lp["amount0"] == "100"
    assert lp["amount1"] == "1"
    assert lp["value_usd"] == 300.0
    assert lp["in_range"] == 1


def test_owner_history_rpc_does_not_hold_the_live_selection_lock():
    entered = threading.Event()
    release = threading.Event()

    class SlowHistoryRpc:
        def call(self, method, params):
            assert method == "eth_getLogs"
            entered.set()
            assert release.wait(2)
            return []

        def batch(self, calls):
            return [
                {"number": hex(int(params[0], 16)), "hash": f"hash-{int(params[0], 16)}"}
                for _method, params in calls
            ]

    instance = service()
    instance._maintenance_rpc = SlowHistoryRpc()
    instance._maintenance_chain_verified = True
    selection = market._Selection(POOL, "0x" + "44" * 20)
    selection.needs_seed = False
    selection.cursor = 100
    selection.cursor_hash = "hash-100"
    selection.history_floor = 50
    selection.history_frontier_hash = "hash-50"
    result = []
    worker = threading.Thread(
        target=lambda: result.append(instance._scan_older_positions(selection)),
    )
    worker.start()
    try:
        assert entered.wait(2)
        assert selection.lock.acquire(blocking=False)
        selection.lock.release()
    finally:
        release.set()
        worker.join(timeout=2)

    assert result == [True]
    assert selection.history_complete
    assert selection.history_floor == 0


def test_new_service_exposes_census_before_background_rpc_is_available(monkeypatch, tmp_path):
    universe = market._Universe(
        (POOL,), {POOL.id: POOL}, {"v2": 0, "v3": 1, "v4": 0},
        (), (), service().universe.tokens,
    )
    monkeypatch.setattr(market, "_load_universe", lambda: universe)
    monkeypatch.setattr(
        market.threading, "Thread",
        lambda **kwargs: SimpleNamespace(start=lambda: None, join=lambda **kwargs: None),
    )
    with market.MarketService("http://127.0.0.1:1", data_dir=tmp_path) as instance:
        catalog = instance.catalog({})
        assert catalog["total"] == 1
        assert catalog["rows"][0]["id"] == POOL.id


@pytest.mark.parametrize(
    ("baseline_claim", "baseline_equity", "current_claim", "collected", "burned"),
    [
        # A burn first moves principal into tokensOwed; collecting it later is
        # a withdrawal of starting equity, not fee income or profit.
        (0, 100, 0, 100, 100),
        # Collecting a claim already present at the observation baseline also
        # leaves both interval earnings and marked PnL unchanged.
        (10, 10, 0, 10, 0),
    ],
)
def test_collections_and_burned_principal_do_not_manufacture_profit(
    baseline_claim, baseline_equity, current_claim, collected, burned,
):
    accounting = {
        "baseline_claim0": baseline_claim,
        "baseline_claim1": 0,
        "baseline_equity0": baseline_equity,
        "baseline_equity1": 0,
        "deposited0": 0,
        "deposited1": 0,
        "burn_principal0": burned,
        "burn_principal1": 0,
        "collected0": collected,
        "collected1": 0,
    }

    assert market._interval_amounts(
        accounting, 0, 0, current_claim, 0,
    ) == (0, 0, 0, 0)


def test_fee_claim_uses_uint256_wrapping_for_lazy_growth():
    from rhpools.lp_math import fee_claim

    q128 = 1 << 128
    claim0, claim1, lazy0, lazy1 = fee_claim(
        3,
        (1 << 256) - q128,
        0,
        4,
        0,
        q128,
        0,
        0,
        0,
        0,
        0,
        0,
        -10,
        10,
    )

    assert (claim0, claim1, lazy0, lazy1) == (10, 0, 6, 0)


@pytest.mark.parametrize(
    "reason",
    [
        "same-height block was replaced",
        "selected cursor fell 513 blocks behind; bounded reseed required",
    ],
)
def test_gap_or_reorg_invalidates_observation_baselines(reason):
    selection = market._Selection(POOL, "0x" + "44" * 20)
    key = (selection.owner, -10, 10)
    selection.cursor = 123
    selection.participant_ranges[key] = 100
    selection.participant_state[key] = {"liquidity": 5}
    selection.participant_accounting[key] = {"status": "valid"}
    selection.snapshot = {
        "participants": [{
            "fees_earned_usd": 1.0,
            "fees_earned0": "1",
            "fees_earned1": "0",
            "pnl_usd": 1.0,
            "pnl_since_block": 100,
            "pnl_since_timestamp": 1_000,
            "accounting_status": "valid_since_observation",
        }],
        "coverage": {"accounting": {"state": "live_interval"}},
    }

    market.MarketService._invalidate_participant_accounting(selection, reason)

    assert selection.participant_accounting == {}
    assert selection.participant_state == {}
    row = selection.snapshot["participants"][0]
    assert row["fees_earned_usd"] is None
    assert row["fees_earned0"] is None
    assert row["fees_earned1"] is None
    assert row["pnl_usd"] is None
    assert row["accounting_status"] == "baseline_invalidated"
    assert selection.snapshot["coverage"]["accounting"]["reset_reason"] == reason


def test_foreign_mint_event_reports_watched_owner_dilution():
    instance = service()
    watched = "0x" + "44" * 20
    rival = "0x" + "55" * 20
    selection = market._Selection(POOL, watched)
    selection.history_complete = True
    selection.ranges[(-10, 10)] = 1
    selection.participant_ranges[(watched, -10, 10)] = 1
    selection.participant_state[(watched, -10, 10)] = {"liquidity": 10}
    selection.snapshot = {
        "spot": {"tick": 0, "sqrt_price_x96": str(market.Q96)},
        "liquidity": {"active": "100"},
    }
    topic = lambda address: "0x" + address[2:].rjust(64, "0")
    signed = lambda value: "0x" + f"{value % (1 << 256):064x}"
    data = "0x" + "".join(f"{value:064x}" for value in (0, 5, 2, 3))
    log = {
        "topics": [
            market.V3_MINT_TOPIC,
            topic(rival),
            signed(-10),
            signed(10),
        ],
        "data": data,
        "blockNumber": "0x2",
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "transactionHash": "0x" + "66" * 32,
    }

    event = instance._derive_lp_events(selection, [log], {2: 1_000})[0]

    assert event["owner"] == rival
    assert event["share_before_pct"] == 10.0
    assert event["share_after_pct"] == pytest.approx(10 / 105 * 100)


def test_participant_token_earnings_survive_without_usd_valuation():
    instance = service()
    owner = "0x" + "44" * 20
    selection = market._Selection(POOL, owner)
    key = (owner, -10, 10)
    principal0, principal1 = market.principal_raw(10, market.Q96, -10, 10)
    selection.participant_state[key] = {
        "liquidity": 10,
        "claim0": 5,
        "claim1": 7,
    }
    selection.participant_accounting[key] = {
        "status": "valid",
        "baseline_block": 1,
        "baseline_timestamp": 100,
        "baseline_claim0": 2,
        "baseline_claim1": 3,
        "baseline_equity0": principal0 + 2,
        "baseline_equity1": principal1 + 3,
        "deposited0": 0,
        "deposited1": 0,
        "burn_principal0": 0,
        "burn_principal1": 0,
        "collected0": 0,
        "collected1": 0,
    }

    row = instance._participant_rows(
        selection,
        {"sqrt": market.Q96, "tick": 0, "liquidity": 100},
        1.0,
    )[0]

    assert row["fees_earned0"] == "0.000003"
    assert row["fees_earned1"] == "0.000000000000000004"
    assert row["uncollected0"] == "0.000005"
    assert row["uncollected1"] == "0.000000000000000007"
    assert row["fees_earned_usd"] is None
    assert row["pnl_usd"] is None


def test_live_tracked_closed_range_remains_visible_after_principal_collection():
    instance = service()
    owner = "0x" + "44" * 20
    selection = market._Selection(POOL, owner)
    key = (owner, -10, 10)
    selection.participant_ranges[key] = 1
    selection.participant_state[key] = {
        "liquidity": 0,
        "claim0": 0,
        "claim1": 0,
    }
    selection.participant_accounting[key] = {
        "status": "valid",
        "baseline_block": 1,
        "baseline_timestamp": 100,
        "baseline_claim0": 0,
        "baseline_claim1": 0,
        "baseline_equity0": 100,
        "baseline_equity1": 0,
        "deposited0": 0,
        "deposited1": 0,
        "burn_principal0": 100,
        "burn_principal1": 0,
        "collected0": 100,
        "collected1": 0,
    }

    rows = instance._participant_rows(
        selection,
        {"sqrt": market.Q96, "tick": 0, "liquidity": 100},
        1.0,
    )

    assert len(rows) == 1
    assert rows[0]["liquidity"] == "0"
    assert rows[0]["fees_earned0"] == "0"
    assert rows[0]["accounting_status"] == "valid_since_observation"


def test_marked_pnl_includes_price_change_without_fee_income():
    instance = service()
    pool = replace(POOL, token1=market.USDG)
    instance.universe.tokens = {
        pool.token0: market._Token("ASSET", 6, "chain"),
        pool.token1: market._Token("USDG", 6, "chain"),
    }
    owner = "0x" + "44" * 20
    selection = market._Selection(pool, owner)
    key = (owner, 20_000, 20_100)
    liquidity = 10**12
    selection.participant_state[key] = {"liquidity": liquidity, "claim0": 0, "claim1": 0}
    instance._establish_accounting_baselines(
        selection, [key], {"sqrt": market.Q96}, 1, 100,
    )
    principal0, _ = market.principal_raw(liquidity, market.Q96, key[1], key[2])

    row = instance._participant_rows(
        selection, {"sqrt": 2 * market.Q96, "tick": 13_864, "liquidity": 0}, 4.0,
    )[0]

    assert row["pnl_usd"] == pytest.approx(3 * principal0 / 10**6)
    assert row["fees_earned_usd"] == 0


def test_deposits_and_withdrawals_keep_their_event_time_marks():
    instance = service()
    pool = replace(POOL, token1=market.USDG)
    instance.universe.tokens = {
        pool.token0: market._Token("ASSET", 6, "chain"),
        pool.token1: market._Token("USDG", 6, "chain"),
    }
    owner = "0x" + "44" * 20
    selection = market._Selection(pool, owner)
    key = (owner, 20_000, 20_100)
    selection.participant_state[key] = {"liquidity": 0, "claim0": 0, "claim1": 0}
    selection.snapshot = {"spot": {"price_token1_per_token0": 1.0}}
    instance._establish_accounting_baselines(
        selection, [key], {"sqrt": market.Q96}, 1, 100,
    )

    def event(topic, words, index):
        return {
            "topics": [topic, "0x" + owner[2:].rjust(64, "0"),
                       hex(key[1]), hex(key[2])],
            "data": "0x" + "".join(f"{word:064x}" for word in words),
            "blockNumber": "0x2", "logIndex": hex(index),
        }

    instance._apply_accounting_logs(selection, [
        event(market.V3_MINT_TOPIC, (0, 100, 100_000_000, 0), 0),
        event(market.V3_SWAP_TOPIC, (0, 0, 2 * market.Q96, 0, 13_864), 1),
        event(market.V3_BURN_TOPIC, (100, 100_000_000, 0), 2),
        event(market.V3_COLLECT_TOPIC, (0, 100_000_000, 0), 3),
    ])
    row = instance._participant_rows(
        selection, {"sqrt": 2 * market.Q96, "tick": 13_864, "liquidity": 0}, 4.0,
    )[0]

    assert row["pnl_usd"] == 300.0
    assert row["fees_earned_usd"] == 0


def test_zero_burn_before_mint_is_fee_checkpoint_not_range_move():
    instance = service()
    owner = "0x" + "44" * 20
    selection = market._Selection(POOL, owner)
    selection.snapshot = {
        "spot": {"tick": 0, "sqrt_price_x96": str(market.Q96)},
        "liquidity": {"active": "100"},
    }
    topics = ["0x" + owner[2:].rjust(64, "0"),
              hex((-10) % (1 << 256)), hex(10)]
    logs = [{
        "topics": [topic, *topics],
        "data": "0x" + "".join(f"{word:064x}" for word in words),
        "blockNumber": "0x2", "transactionIndex": "0x0",
        "transactionHash": "0x" + "66" * 32, "logIndex": hex(index),
    } for index, (topic, words) in enumerate((
        (market.V3_BURN_TOPIC, (0, 0, 0)),
        (market.V3_MINT_TOPIC, (0, 5, 2, 3)),
    ))]

    events = instance._derive_lp_events(selection, logs, {2: 100})

    assert [event["kind"] for event in events] == ["checkpoint", "add"]
