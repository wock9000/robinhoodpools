"""Valuation caches must preserve current evidence and independent WAL reads."""
import math
from concurrent.futures import ThreadPoolExecutor

import pytest

from rhpools.lp_market_accounting import AccountBook
from rhpools.lp_market_store import MarketStore
from rhpools.lp_math import sqrt_ratio_at_tick

POOL_ID = "pool-a"
OWNER = "0x" + "11" * 20


def insert_position(conn, key, liquidity):
    conn.execute(
        "INSERT INTO lp_accounting_positions("
        "position_key,pool_id,protocol,owner,tick_lower,tick_upper,"
        "liquidity,liquidity_known,pending_known,owed_known,"
        "active_episode_id,status,history_complete,first_block,"
        "first_timestamp,last_block,last_tx_index,last_log_index,"
        "last_timestamp,state_json"
        ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (key, POOL_ID, "v3", OWNER, -10, 10, str(liquidity), 1, 0, 0,
         f"episode-{key}", "active", 1, 1, 100, 1, 0, 0, 100, "{}"),
    )


@pytest.fixture
def inventory(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite")
    book = AccountBook(store).install()
    with store.transaction() as conn:
        conn.execute(
            "CREATE TABLE lp_pool_state("
            "pool_id TEXT PRIMARY KEY,block_number INTEGER,tx_index INTEGER,"
            "log_index INTEGER,timestamp INTEGER,tick INTEGER,sqrt_price_x96 TEXT,"
            "price0_usd REAL,price1_usd REAL)"
        )
        conn.execute(
            "INSERT INTO pools("
            "id,protocol,address,token0,token1,decimals0,decimals1,created_block"
            ") VALUES(?,?,?,?,?,?,?,?)",
            (POOL_ID, "v3", POOL_ID, "token0", "token1", 18, 18, 1),
        )
        conn.execute(
            "INSERT INTO blocks(number,hash,parent_hash,timestamp) "
            "VALUES(1,'block-1','block-0',100)"
        )
        conn.execute(
            "INSERT INTO coverage_intervals("
            "lane,start_block,end_block,start_hash,end_hash"
            ") VALUES('live',1,1,'block-1','block-1')"
        )
        conn.execute(
            "INSERT INTO lp_pool_state VALUES(?,?,?,?,?,?,?,?,?)",
            (POOL_ID, 1, 0, 0, 100, -20, str(sqrt_ratio_at_tick(-20)), 2.0, 3.0),
        )
        insert_position(conn, "position-a", 10**18)
    try:
        yield store, book
    finally:
        store.close()


def test_pool_stats_refreshes_marks_metadata_and_coverage(inventory):
    store, book = inventory
    initial = book.pool_stats([POOL_ID])[POOL_ID]
    assert initial["lp_count"] == 1
    assert initial["open_positions"] == 1
    assert initial["complete_inventory"] is True
    assert initial["observed_active_tvl_usd"] == 0.0

    with store.transaction() as conn:
        conn.execute("UPDATE lp_pool_state SET price0_usd=4.0")
    repriced = book.pool_stats([POOL_ID])[POOL_ID]
    assert repriced["observed_principal_usd"] == pytest.approx(
        initial["observed_principal_usd"] * 2
    )

    with store.transaction() as conn:
        conn.execute("UPDATE pools SET decimals0=17 WHERE id=?", (POOL_ID,))
    remapped = book.pool_stats([POOL_ID])[POOL_ID]
    assert remapped["observed_principal_usd"] == pytest.approx(
        repriced["observed_principal_usd"] * 10
    )

    with store.transaction() as conn:
        conn.execute("UPDATE lp_pool_state SET tick=0,sqrt_price_x96=?", (str(sqrt_ratio_at_tick(0)),))
    active = book.pool_stats([POOL_ID])[POOL_ID]
    assert active["observed_active_tvl_usd"] == active["observed_principal_usd"]

    with store.transaction() as conn:
        conn.execute("DELETE FROM coverage_intervals")
    assert book.pool_stats([POOL_ID])[POOL_ID]["complete_inventory"] is False


def test_uncommitted_inventory_does_not_block_or_poison_readers(inventory):
    store, book = inventory
    before = book.pool_stats([POOL_ID])[POOL_ID]
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(RuntimeError, match="abort inventory write"):
            with store.transaction() as conn:
                conn.execute(
                    "UPDATE lp_accounting_positions SET liquidity=?",
                    (str(2 * 10**18),),
                )
                book._invalidate_cache(conn, [POOL_ID])
                during = executor.submit(
                    book.pool_stats, [POOL_ID],
                ).result(timeout=2)
                assert during[POOL_ID] == before
                raise RuntimeError("abort inventory write")
        assert book.pool_stats([POOL_ID])[POOL_ID] == before

        with store.transaction() as conn:
            conn.execute(
                "UPDATE lp_accounting_positions SET liquidity=?",
                (str(2 * 10**18),),
            )
            book._invalidate_cache(conn, [POOL_ID])
            during = executor.submit(
                book.pool_stats, [POOL_ID],
            ).result(timeout=2)
            assert during[POOL_ID] == before
        after = executor.submit(book.pool_stats, [POOL_ID]).result(timeout=2)[
            POOL_ID
        ]
    assert after["observed_principal_usd"] == pytest.approx(
        before["observed_principal_usd"] * 2
    )


def test_small_positions_are_not_lost_beside_large_positions(inventory):
    store, book = inventory
    with store.transaction() as conn:
        conn.execute("UPDATE lp_pool_state SET tick=0,sqrt_price_x96=?", (str(sqrt_ratio_at_tick(0)),))
    small = book.pool_stats([POOL_ID])[POOL_ID]["observed_principal_usd"]
    with store.transaction() as conn:
        conn.execute("UPDATE lp_accounting_positions SET liquidity=?", (str(10**34),))
        book._invalidate_cache(conn, [POOL_ID])
    large = book.pool_stats([POOL_ID])[POOL_ID]["observed_principal_usd"]
    with store.transaction() as conn:
        for index in range(100):
            insert_position(conn, f"small-{index}", 10**18)
        book._invalidate_cache(conn, [POOL_ID])
    combined = book.pool_stats([POOL_ID])[POOL_ID]
    expected = math.fsum([large] + [small] * 100)
    assert combined["observed_principal_usd"] == expected
    assert combined["observed_active_tvl_usd"] == expected


def test_pool_stats_respects_an_existing_reader_snapshot(inventory):
    store, book = inventory
    before = book.pool_stats([POOL_ID])[POOL_ID]
    reader = store.read()
    reader.execute("BEGIN")
    reader.execute("SELECT price0_usd FROM lp_pool_state").fetchone()
    with store.transaction() as conn:
        conn.execute("UPDATE lp_pool_state SET price0_usd=4.0")
    assert book.pool_stats([POOL_ID])[POOL_ID] == before
    # The caller's snapshot must survive repeated accounting reads.
    assert book.pool_stats([POOL_ID])[POOL_ID] == before
    reader.rollback()
    assert book.pool_stats([POOL_ID])[POOL_ID]["observed_principal_usd"] == pytest.approx(
        before["observed_principal_usd"] * 2
    )


APPEND_POSITION = "v4:deferred-append"


def accounting_header(number):
    return {
        "number": hex(number),
        "hash": "0x" + f"{number:064x}",
        "parentHash": "0x" + f"{number - 1:064x}",
        "timestamp": hex(1_700_000_000 + number),
    }


def accounting_state(liquidity):
    return {
        "liquidity": str(liquidity),
        "tokens_owed0": "0",
        "tokens_owed1": "0",
        "claims_empty": True,
    }


def accounting_event(block, kind, liquidity_delta, before, after, usd):
    adding = kind == "add"
    cashflow = -100 if adding else 100
    return {
        "block_number": int(block["number"], 16),
        "block_hash": block["hash"],
        "tx_hash": "0x" + f"{int(block['number'], 16):064x}",
        "tx_index": 0,
        "log_index": 0,
        "timestamp": int(block["timestamp"], 16),
        "pool_id": None,
        "protocol": "v4",
        "kind": kind,
        "owner": OWNER,
        "custody": OWNER,
        "position_key": APPEND_POSITION,
        "token_id": "42",
        "tick_lower": -10,
        "tick_upper": 10,
        "liquidity": str(after),
        "liquidity_delta": str(liquidity_delta),
        "amount0": "0",
        "amount1": "0",
        "cashflow0": str(cashflow),
        "cashflow1": "0",
        "fee_amount0": "0",
        "fee_amount1": "0",
        "deposit_usd": usd if adding else 0.0,
        "withdrawal_usd": 0.0 if adding else usd,
        "fees_usd": 0.0,
        "accounting_basis": "complete v4 trace",
        "identity_basis": "verified_owner",
        "data": {
            "position_before": accounting_state(before),
            "position_after": accounting_state(after),
            "trace_complete": True,
            "fees_accrued_exact": True,
            "principal_delta_exact": True,
            "principal_delta": {
                "amount0": str(cashflow),
                "amount1": "0",
            },
        },
    }


def accounting_history():
    blocks = [accounting_header(number) for number in range(100, 104)]
    events = [
        accounting_event(blocks[0], "add", 100, 0, 100, 1.0),
        accounting_event(blocks[1], "remove", -100, 100, 0, 2.0),
        accounting_event(blocks[2], "add", 200, 0, 200, 3.0),
        accounting_event(blocks[3], "remove", -200, 200, 0, 4.0),
    ]
    return blocks, events


def drain_accounting(book):
    while book.project_pending(limit=32):
        pass


def accounting_projection(store):
    reader = store.read()
    return {
        "positions": [
            tuple(row) for row in reader.execute(
                "SELECT * FROM lp_accounting_positions ORDER BY position_key"
            )
        ],
        "episodes": [
            tuple(row) for row in reader.execute(
                "SELECT * FROM lp_accounting_episodes ORDER BY id"
            )
        ],
        "effects": [
            tuple(row) for row in reader.execute(
                "SELECT * FROM lp_accounting_effects ORDER BY event_id"
            )
        ],
        "ownership": [
            tuple(row) for row in reader.execute(
                "SELECT * FROM lp_ownership_intervals "
                "ORDER BY position_key,ordinal"
            )
        ],
    }


def test_deferred_later_block_append_matches_full_synchronous_replay(tmp_path):
    deferred = MarketStore(tmp_path / "deferred-append.sqlite")
    full = MarketStore(tmp_path / "full-replay.sqlite")
    deferred_book = AccountBook(deferred, deferred=True).install()
    AccountBook(full).install()
    try:
        blocks, events = accounting_history()
        deferred.ingest(blocks[:2], events[:2])
        drain_accounting(deferred_book)
        deferred.ingest(blocks[2:3], events[2:3])

        drain_accounting(deferred_book)

        full_blocks, full_events = accounting_history()
        full.ingest(full_blocks[:3], full_events[:3])
        assert accounting_projection(deferred) == accounting_projection(full)
        assert deferred.read().execute(
            "SELECT COUNT(*) FROM lp_accounting_effects"
        ).fetchone()[0] == 3
        assert deferred.read().execute(
            "SELECT COUNT(*) FROM lp_accounting_episodes"
        ).fetchone()[0] == 2
    finally:
        full.close()
        deferred.close()


def test_older_enrichment_mixed_with_new_events_forces_full_replay(tmp_path):
    deferred = MarketStore(tmp_path / "mixed-deferred.sqlite")
    synchronous = MarketStore(tmp_path / "mixed-synchronous.sqlite")
    deferred_book = AccountBook(deferred, deferred=True).install()
    AccountBook(synchronous).install()
    try:
        blocks, events = accounting_history()
        inserted = deferred.ingest(blocks[:2], events[:2])
        first_event_id = int(inserted[0]["id"])
        drain_accounting(deferred_book)
        deferred.ingest(blocks[2:3], events[2:3])

        deferred.enrich([{
            "id": first_event_id,
            "deposit_usd": 9.0,
            "accounting_basis": "same-order financial enrichment",
        }])
        deferred.ingest(blocks[3:], events[3:])
        drain_accounting(deferred_book)

        sync_blocks, sync_events = accounting_history()
        sync_inserted = synchronous.ingest(sync_blocks[:2], sync_events[:2])
        synchronous.ingest(sync_blocks[2:3], sync_events[2:3])
        synchronous.enrich([{
            "id": int(sync_inserted[0]["id"]),
            "deposit_usd": 9.0,
            "accounting_basis": "same-order financial enrichment",
        }])
        synchronous.ingest(sync_blocks[3:], sync_events[3:])

        assert accounting_projection(deferred) == accounting_projection(
            synchronous
        )
        first_episode = deferred.read().execute(
            "SELECT deposit_usd FROM lp_accounting_episodes "
            "WHERE position_key=? AND ordinal=1",
            (APPEND_POSITION,),
        ).fetchone()
        assert first_episode["deposit_usd"] == pytest.approx(9.0)
    finally:
        synchronous.close()
        deferred.close()
