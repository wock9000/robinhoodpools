"""Pruning old raw price/reserve samples must not change any answer.

Every test builds the same 41-day history (V3 anchor, V4, V2 and a cross pool
with LP positions opened and closed across the 35-day cutoff), copies the
database, prunes one copy and compares the copies.
"""
import sqlite3
import time
from unittest import mock

import pytest

from rhpools.lp_market_service import LPMarketService, PriceProjection
from rhpools.lp_market_store import SAMPLE_PRUNE_TABLES
from rhpools.workbench_market import USDG, UNISWAP_V3_FACTORY

DAY = 86_400
RETENTION_DAYS = 35
TOKEN = "0x" + "12" * 20
OTHER = "0x" + "56" * 20
OWNER = "0x" + "78" * 20
V3 = "0x" + "23" * 20
CROSS = "0x" + "45" * 20
V2 = "0x" + "67" * 20
V4 = "0x" + "34" * 32
MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
FIRST_BLOCK = 1_000
# Fields that describe when or how fast an answer was served, not the answer.
VOLATILE = frozenset({"as_of", "status", "latency", "latency_ms", "served_at", "generated_at"})
ACCOUNTING_TABLES = (
    "lp_accounting_effects", "lp_accounting_episodes", "lp_accounting_positions",
    "lp_accounting_tx_costs", "lp_accounting_event_keys", "lp_ownership_intervals",
)
PROJECTION_TABLES = ("lp_pool_buckets", "lp_price_marks", "lp_pool_state")
EVENT_PRICING = (
    "id,price0_usd,price1_usd,volume_usd,fees_usd,deposit_usd,withdrawal_usd,"
    "pricing_basis,data"
)


def service(path):
    return LPMarketService(None, "http://127.0.0.1:1", path, start=False)


def header(number, timestamp):
    return {"number": hex(number), "hash": "0x" + f"{number:064x}",
            "parentHash": "0x" + f"{number - 1:064x}", "timestamp": hex(timestamp)}


def event(block, pool, protocol, kind, index, **fields):
    row = {
        "block_number": int(block["number"], 16), "block_hash": block["hash"],
        "tx_hash": "0x" + f"{int(block['hash'], 16) * 100 + index:064x}",
        "tx_index": index, "log_index": index, "timestamp": int(block["timestamp"], 16),
        "pool_id": pool, "protocol": protocol, "kind": kind, "owner": None,
        "custody": None, "position_key": None, "token_id": None,
        "tick_lower": None, "tick_upper": None, "liquidity_delta": None,
        "liquidity": None, "sqrt_price_x96": None, "tick": None,
        "fee_ppm": 3_000, "amount0": "0", "amount1": "0",
        "fee_amount0": None, "fee_amount1": None, "cashflow0": None, "cashflow1": None,
        "accounting_basis": "fixture", "identity_basis": "not a position", "data": {},
    }
    row.update(fields)
    return row


def swap(block, pool, protocol, index, step):
    sign = -1 if protocol == "v4" else 1
    return event(
        block, pool, protocol, "swap", index,
        liquidity="1000000000000", sqrt_price_x96=str((1 << 96) * (1000 + step) // 1000),
        tick=0, fee_ppm=10_000 if protocol == "v4" else 3_000,
        amount0=str(-100_000_000 * sign), amount1=str((100_000_000 + 1_000 * step) * sign),
        accounting_basis="swap event",
    )


def position_state(liquidity, owed0=0, owed1=0):
    return {"liquidity": str(liquidity), "tokens_owed0": str(owed0), "tokens_owed1": str(owed1),
            "claims_empty": owed0 == owed1 == 0}


def lp(block, pool, protocol, kind, index, key, liquidity, amounts, before, after):
    """An LP effect without its own price: valuing it needs the pool's sample."""
    cash = ((-amounts[0], -amounts[1]) if kind == "add"
            else amounts if kind == "collect" else (0, 0))
    return event(
        block, pool, protocol, kind, index, owner=OWNER, custody=OWNER,
        position_key=f"{protocol}:{key}", identity_basis="verified_owner",
        liquidity="1000000000000", liquidity_delta=str(liquidity), tick=0,
        tick_lower=-10, tick_upper=10, amount0=str(amounts[0]), amount1=str(amounts[1]),
        cashflow0=str(cash[0]), cashflow1=str(cash[1]),
        data={"position_before": position_state(*before), "position_after": position_state(*after)},
    )


def v2_trade(block, index, step):
    reserves = {"reserve0": str(100_000_000 + 10_000 * step), "reserve1": str(200_000_000 - 5_000 * step)}
    return [
        event(block, V2, "v2", "checkpoint", index, data=reserves),
        event(block, V2, "v2", "swap", index + 1, amount0="10000000", amount1="-18000000",
              data={"input0": "10000000", "input1": "0"}),
    ]


def pools():
    base = {"token0": TOKEN, "token1": USDG, "symbol0": "ASSET", "symbol1": "USDG",
            "decimals0": 6, "decimals1": 6, "tick_spacing": 1, "hook": None,
            "factory": UNISWAP_V3_FACTORY, "created_block": FIRST_BLOCK, "source": "factory-live"}
    return [
        {**base, "id": V3, "address": V3, "protocol": "v3", "fee_ppm": 3_000},
        {**base, "id": V4, "address": MANAGER, "protocol": "v4", "fee_ppm": 0x800000},
        {**base, "id": V2, "address": V2, "protocol": "v2", "fee_ppm": 3_000},
        {**base, "id": CROSS, "address": CROSS, "protocol": "v3", "fee_ppm": 3_000,
         "token1": OTHER, "symbol1": "OTHER"},
    ]


def day_events(blocks, day, step):
    """Swaps in the first block; LP effects one minute later in the second."""
    trade, effects = blocks
    events = [swap(trade, V3, "v3", 0, step), swap(trade, CROSS, "v3", 1, step),
              swap(trade, V4, "v4", 2, step), *v2_trade(trade, 3, step)]
    for pool, protocol, base in ((V3, "v3", 0), (V4, "v4", 10)):
        key = f"c{day + day % 2}"
        if day % 2 == 0:
            events.append(lp(effects, pool, protocol, "add", base, key, 1000,
                             (1_000_000, 1_000_000), (0,), (1000,)))
        else:
            events.append(lp(effects, pool, protocol, "remove", base, key, -1000,
                             (1_000_000, 1_000_000), (1000,), (0, 1_050_000, 1_000_000)))
            events.append(lp(effects, pool, protocol, "collect", base + 1, key, 0,
                             (1_050_000, 1_000_000), (0, 1_050_000, 1_000_000), (0,)))
    return events


def receipts(events):
    return [{"tx_hash": e["tx_hash"], "block_number": e["block_number"],
             "block_hash": e["block_hash"], "payer": OWNER, "gas_usd": 0.01}
            for e in events if e["kind"] in {"add", "remove", "collect"}]


def build_history(path, now):
    """40 days and today, two blocks a day, ingested in block order like the live lane."""
    app = service(path)
    try:
        app.store.upsert_pools(pools())
        number = FIRST_BLOCK
        for day in range(40, -1, -1):
            start = now - day * DAY - 7_200
            blocks = (header(number, start), header(number + 1, start + 60))
            events = day_events(blocks, day, 40 - day)
            app.store.ingest(list(blocks), events, transactions=receipts(events))
            number += 2
    finally:
        app.close()
    return number


def copy_database(source, target):
    with sqlite3.connect(source) as src, sqlite3.connect(target) as dst:
        src.backup(dst)


def prune_all(app, cutoff):
    batches = 0
    while True:
        batch = app.store.prune_sample_batch(cutoff, limit=7, pool_limit=2)
        assert not batch.busy
        batches += 1
        if batch.complete:
            return batches


def normalize(value):
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items() if key not in VOLATILE}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    return value


def api_snapshot(app, at):
    """Every read the samples can feed, answered at one frozen wall-clock time."""
    answers = {}
    with mock.patch("time.time", return_value=float(at)):
        for window in ("1h", "24h", "7d", "30d", "all"):
            answers[f"overview:{window}"] = app.overview({"window": window})
            for sort in ("fees", "volume", "tvl", "active_tvl", "change", "price", "flow", "activity", "lps"):
                answers[f"pools:{window}:{sort}"] = app.pools({"window": window, "sort": sort})
            answers[f"tape:{window}"] = app.tape({"window": window, "kind": "all", "limit": 500})
            answers[f"owners:{window}"] = app.book.owners({"window": window})
            answers[f"closed:{window}"] = app.book.closed({"window": window})
            answers[f"owner:{window}"] = app.book.owner(OWNER, {"window": window})
        answers["positions"] = app.book.positions({})
        answers["dislocations"] = app.dislocations({"min_bps": "0", "min_depth_usd": "0", "max_stale_s": "0"})
    return normalize(answers)


def differences(expected, actual, path=""):
    """Every path where two answers differ, so a failure names the field."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        return [
            found
            for key in sorted(set(expected) | set(actual))
            for found in differences(expected.get(key), actual.get(key), f"{path}/{key}")
        ]
    if isinstance(expected, list) and isinstance(actual, list) and len(expected) == len(actual):
        return [
            found
            for index, (left, right) in enumerate(zip(expected, actual))
            for found in differences(left, right, f"{path}[{index}]")
        ]
    return [] if expected == actual else [(path, expected, actual)]


def table_rows(app, table, where="1"):
    return app.store.read().execute(
        f"SELECT * FROM {table} WHERE {where} ORDER BY 1,2,3"
    ).fetchall()


def ledger(app):
    rows = {table: [tuple(row) for row in table_rows(app, table)]
            for table in (*ACCOUNTING_TABLES, *PROJECTION_TABLES)}
    rows["events"] = [tuple(row) for row in app.store.read().execute(
        f"SELECT {EVENT_PRICING} FROM events ORDER BY id"
    ).fetchall()]
    return rows


def drain_reprojection(app):
    # Small batches interleave oldest and newest pending events like the
    # adaptive reprojection lane does.
    while rows := app.store.pending_reprojections(8):
        app.store.reproject([row["id"] for row in rows])


@pytest.fixture
def twins(tmp_path):
    now = int(time.time())
    control, pruned = tmp_path / "control.sqlite", tmp_path / "pruned.sqlite"
    next_block = build_history(control, now)
    copy_database(control, pruned)
    return now, control, pruned, next_block


def test_prune_keeps_first_and_newest_old_sample_of_each_pool(twins):
    now, control, pruned, _next_block = twins
    cutoff = now - RETENTION_DAYS * DAY
    reference = service(control)
    app = service(pruned)
    try:
        boundary = app.store.read().execute(
            "SELECT MIN(block_number) FROM events WHERE timestamp>=?", (cutoff,),
        ).fetchone()[0]
        assert prune_all(app, cutoff) > 1
        assert PriceProjection._sample_prune_block(app.store.read()) == boundary
        for table in SAMPLE_PRUNE_TABLES:
            before = table_rows(reference, table)
            kept = {row["event_id"] for row in table_rows(app, table)}
            expected = {row["event_id"] for row in before if row["block_number"] >= boundary}
            for pool in {row["pool_id"] for row in before}:
                old = [row for row in before if row["pool_id"] == pool and row["block_number"] < boundary]
                if old:
                    expected |= {old[0]["event_id"], old[-1]["event_id"]}
            assert kept == expected
            assert len(kept) < len(before)
        # The sweep is idempotent: a second pass finds nothing more to delete.
        assert app.store.prune_sample_batch(cutoff).removed == 0
    finally:
        app.close()
        reference.close()


def test_prune_leaves_every_api_answer_unchanged(twins):
    now, control, pruned, _next_block = twins
    app = service(pruned)
    try:
        before = api_snapshot(app, now)
        prune_all(app, now - RETENTION_DAYS * DAY)
        app.close()
        app = service(pruned)
        after = api_snapshot(app, now)
    finally:
        app.close()
    reference = service(control)
    try:
        reference_answers = api_snapshot(reference, now)
    finally:
        reference.close()
    assert differences(before, after) == []
    assert differences(reference_answers, after) == []
    assert differences(reference_answers, before) == []
    assert before["pools:all:change"]["rows"][0]["price_change_pct"] is not None
    assert before["pools:30d:tvl"]["rows"][0]["tvl_usd"] is not None


def test_prune_leaves_reprojection_and_accounting_unchanged(twins, monkeypatch):
    now, control, pruned, next_block = twins
    app = service(pruned)
    try:
        prune_all(app, now - RETENTION_DAYS * DAY)
    finally:
        app.close()

    fallbacks = []
    sample_event = PriceProjection._sample_event

    def counted(*args, **kwargs):
        fallbacks.append(args[1]["id"])
        return sample_event(*args, **kwargs)

    monkeypatch.setattr(PriceProjection, "_sample_event", staticmethod(counted))

    def exercise(path):
        """Apply the same later history to a database; snapshot after each step."""
        app = service(path)
        steps = []

        def snapshot(step):
            steps.append((step, ledger(app), api_snapshot(app, now)))

        try:
            # Re-price only the events that read a sample (LP effects and V2
            # trades), as a position-key repair would; the swaps that wrote
            # those samples are not re-run first.
            with app.store.transaction() as conn:
                queued = conn.execute(
                    "INSERT OR IGNORE INTO pending_reprojection"
                    "(event_id,block_number,tx_index,log_index) "
                    "SELECT id,block_number,tx_index,log_index FROM events "
                    "WHERE kind IN ('add','remove','collect') "
                    "OR (protocol='v2' AND kind='swap')"
                ).rowcount
                app.store._bump(conn, "pending_reprojection", queued)
            drain_reprojection(app)
            snapshot("reprice sample readers")
            # Archive backfill below every existing block, priced later.
            old = header(FIRST_BLOCK - 2, now - 41 * DAY)
            backfill = [swap(old, V3, "v3", 0, -5), *v2_trade(old, 1, -5),
                        lp(old, V3, "v3", "add", 3, "old", 1000,
                           (1_000_000, 1_000_000), (0,), (1000,))]
            app.store.ingest([old], backfill, lane="backfill", project=False,
                             transactions=receipts(backfill))
            drain_reprojection(app)
            snapshot("archive backfill")
            # Token metadata arriving re-prices every event of its pools.
            app.store.save_token_metadata(TOKEN, "ASSET", 6)
            drain_reprojection(app)
            snapshot("token metadata")
            # A live day closes a position opened before the cutoff.
            blocks = (header(next_block, now - 600), header(next_block + 1, now - 540))
            events = [*day_events(blocks, 0, 41),
                      lp(blocks[1], V3, "v3", "remove", 20, "old", -1000,
                         (1_000_000, 1_000_000), (1000,), (0, 1_050_000, 1_000_000))]
            app.store.ingest(list(blocks), events, transactions=receipts(events))
            snapshot("live day")
            app.store.rollback(next_block)
            drain_reprojection(app)
            snapshot("reorg")
            return steps
        finally:
            app.close()

    expected = exercise(control)
    control_fallbacks = len(fallbacks)
    actual = exercise(pruned)
    for (step, expected_ledger, expected_api), (_step, actual_ledger, actual_api) in zip(
        expected, actual, strict=True,
    ):
        for table in expected_ledger:
            assert actual_ledger[table] == expected_ledger[table], (step, table)
        assert differences(expected_api, actual_api) == [], step
    assert control_fallbacks == 0
    assert len(fallbacks) > 0
    # The comparison covers LP effects valued from pre-cutoff samples.
    cutoff = now - RETENTION_DAYS * DAY
    reference = service(control)
    try:
        assert reference.store.read().execute(
            "SELECT COUNT(*) FROM events WHERE kind='add' AND timestamp<? "
            "AND deposit_usd IS NOT NULL", (cutoff,),
        ).fetchone()[0] >= 3
    finally:
        reference.close()


def test_sample_pruning_skips_busy_writer(twins):
    import threading

    now, _control, pruned, _next_block = twins
    app = service(pruned)
    entered, release = threading.Event(), threading.Event()

    def hold_writer():
        with app.store.transaction():
            entered.set()
            assert release.wait(10)

    worker = threading.Thread(target=hold_writer)
    try:
        worker.start()
        assert entered.wait(10)
        batch = app.store.prune_sample_batch(now - RETENTION_DAYS * DAY)
        assert batch.busy and batch.removed == 0 and not batch.complete
    finally:
        release.set()
        worker.join(timeout=2)
        app.close()


def test_maintenance_tick_sweeps_and_reports_progress(twins):
    _now, _control, pruned, _next_block = twins
    app = service(pruned)
    try:
        before = sum(
            app.store.read().execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in SAMPLE_PRUNE_TABLES
        )
        while not app.indexer._sample_prune_tick()[0]:
            pass
        after = sum(
            app.store.read().execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in SAMPLE_PRUNE_TABLES
        )
        status = app.indexer.runtime_status()
        assert status["sample_prune_removed"] == before - after > 0
        assert status["sample_prune_retention_days"] == RETENTION_DAYS
        assert status["sample_prune_swept_at"] > 0
    finally:
        app.close()


def test_prune_rest_counts_writer_time_not_writer_waits(twins):
    import threading

    from rhpools.lp_market_index import SAMPLE_PRUNE_MIN_REST_S, SAMPLE_PRUNE_REST_S

    now, _control, pruned, _next_block = twins
    app = service(pruned)
    entered, release = threading.Event(), threading.Event()

    def hold_writer():
        with app.store.transaction():
            entered.set()
            release.wait(10)
            time.sleep(0.5)

    worker = threading.Thread(target=hold_writer)
    try:
        worker.start()
        assert entered.wait(10)
        release.set()
        started = time.monotonic()
        batch = app.store.prune_sample_batch(now - RETENTION_DAYS * DAY, blocking=True)
        waited_and_worked = time.monotonic() - started
        assert not batch.busy and batch.removed > 0
        assert waited_and_worked >= 0.5 > batch.seconds > 0

        indexer = app.indexer
        assert indexer._sample_prune_rest(True, 0.2) == SAMPLE_PRUNE_REST_S
        # Unknown or catching up: three times the writer-held time, floored.
        assert indexer._sample_prune_rest(False, 0.2) == pytest.approx(0.6)
        with indexer._status_lock:
            indexer._runtime_status["recent_catchup_priority"] = True
        assert indexer._sample_prune_rest(False, 0.2) == pytest.approx(0.6)
        assert indexer._sample_prune_rest(False, 0.0) == SAMPLE_PRUNE_MIN_REST_S
        # Caught up: the sweep runs freely.
        with indexer._status_lock:
            indexer._runtime_status["recent_catchup_priority"] = False
        assert indexer._sample_prune_rest(False, 0.2) == SAMPLE_PRUNE_MIN_REST_S
    finally:
        release.set()
        worker.join(timeout=5)
        app.close()
