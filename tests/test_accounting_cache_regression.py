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
    assert book.pool_stats([POOL_ID])[POOL_ID]["observed_principal_usd"] == pytest.approx(
        before["observed_principal_usd"] * 2
    )
