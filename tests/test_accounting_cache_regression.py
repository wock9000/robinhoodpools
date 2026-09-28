"""Valuation caches must preserve current evidence and independent WAL reads."""
import math
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

import rhpools.lp_market_accounting as accounting_module
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
        with store.transaction() as conn:
            conn.execute("UPDATE lp_accounting_positions SET liquidity=?", (str(2 * 10**18),))
            book._invalidate_cache(conn, [POOL_ID])
            during = executor.submit(book.pool_stats, [POOL_ID]).result(timeout=2)
            assert during[POOL_ID] == before
        after = book.pool_stats([POOL_ID])[POOL_ID]
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
    reader.rollback()
    assert book.pool_stats([POOL_ID])[POOL_ID]["observed_principal_usd"] == pytest.approx(
        before["observed_principal_usd"] * 2
    )



def test_owner_count_uses_small_covering_window_index(inventory):
    store, book = inventory
    now = int(accounting_module.time.time())
    with store.transaction() as conn:
        required = {
            row[1]: 0 if row[2] in ("INTEGER", "REAL") else "0"
            for row in conn.execute("PRAGMA table_info(lp_accounting_episodes)")
            if row[3]
        }
        fields = list(dict.fromkeys((*required, "id", "owner", "custody")))

        def episode(index):
            values = {
                **required,
                "id": f"episode-{index:05d}",
                "position_key": f"position-{index:05d}" + "x" * 80,
                "ordinal": 1,
                "status": "active",
                "accounting_basis": "test",
                "qualifiers": "[]",
                "last_timestamp": now if index else now - 40 * 86_400,
                "owner": f"0x{index % 20:040x}",
                "custody": f"0x{index % 15 + 100:040x}",
            }
            return [values[field] for field in fields]

        conn.executemany(
            "INSERT INTO lp_accounting_episodes("
            + ",".join(fields) + ") VALUES("
            + ",".join("?" for _ in fields) + ")",
            (episode(index) for index in range(5000)),
        )
    assert book.owner_count("30d") == 35
    assert book.owner_count("all") == 35
    sizes = {
        name: pages for name, pages in store.read().execute(
            "SELECT name,COUNT(*) FROM dbstat WHERE name IN ("
            "'lp_accounting_episodes_owner_count_window',"
            "'lp_accounting_episodes_owner_window_cover') GROUP BY name"
        )
    }
    assert sizes["lp_accounting_episodes_owner_count_window"] < (
        sizes["lp_accounting_episodes_owner_window_cover"] * 3 // 4
    )

def test_superseded_inventory_cost_follows_the_page_not_the_queue(inventory):
    store, book = inventory
    with store.transaction() as conn:
        conn.executemany(
            "INSERT INTO lp_accounting_pending("
            "position_key,generation,requested_revision,requested_epoch,"
            "priority_block,priority_tx_index,priority_log_index"
            ") VALUES(?,1,1,0,2,0,0)",
            [("position-a",)] + [(f"elsewhere-{index:06d}",) for index in range(20_000)],
        )
    with store.reader_snapshot() as conn:
        steps = 0

        def bound_scan():
            nonlocal steps
            steps += 1
            return int(steps > 50)

        conn.set_progress_handler(bound_scan, 100)
        try:
            superseded = book._superseded_pool_inventory(conn, [POOL_ID])
        finally:
            conn.set_progress_handler(None, 0)
    assert superseded == {POOL_ID: {(10**18, -10, 10): 1}}


def test_owner_position_page_values_only_selected_positions(inventory, monkeypatch):
    store, book = inventory
    with store.transaction() as conn:
        for index in range(4096):
            insert_position(conn, f"position-{index:05d}", 10**18)
    progress = 0

    def bound_reader():
        nonlocal progress
        progress += 1
        return int(progress > 4000)

    reader_snapshot = store.reader_snapshot

    @contextmanager
    def bounded_reader(seconds=None):
        with reader_snapshot(seconds) as connection:
            connection.set_progress_handler(bound_reader, 100)
            try:
                yield connection
            finally:
                connection.set_progress_handler(None, 0)

    monkeypatch.setattr(store, "reader_snapshot", bounded_reader)
    page = book.positions({"owner": OWNER, "limit": 10, "offset": 20})
    assert page["total"] == 4097
    assert [row["position_key"] for row in page["rows"]] == [
        f"position-{index:05d}" for index in range(20, 30)
    ]

def test_preparation_reader_deadline_releases_its_wal_snapshot(
        tmp_path, monkeypatch):
    path = tmp_path / "market.sqlite"
    store = MarketStore(path)
    monkeypatch.setattr(
        accounting_module, "_PREPARATION_SNAPSHOT_SECONDS", 0.0,
    )
    monkeypatch.setattr(
        accounting_module, "_PREPARATION_PROGRESS_STEPS", 1,
    )
    reader = accounting_module._PreparationReader(str(path))
    try:
        with pytest.raises(sqlite3.OperationalError) as failure:
            with reader.reader_snapshot() as connection:
                connection.execute(
                    "WITH RECURSIVE values_(n) AS ("
                    "SELECT 1 UNION ALL SELECT n+1 FROM values_ WHERE n<1000000"
                    ") SELECT SUM(n) FROM values_"
                ).fetchone()
        assert failure.value.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT
        assert not reader.read().in_transaction
    finally:
        reader.read().close()
        store.close()
