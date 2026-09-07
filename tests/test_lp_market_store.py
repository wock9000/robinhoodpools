"""Focused durable-store batching and canonical-safety regressions."""
from __future__ import annotations

from types import SimpleNamespace
import pytest

from rhpools.lp_market_store import CanonicalConflict, MarketStore
from rhpools.lp_market_accounting import AccountBook


def header(number: int, *, parent: str | None = None) -> dict[str, str]:
    return {
        "number": hex(number),
        "hash": "0x" + f"{number:064x}",
        "parentHash": parent or ("0x" + f"{max(0, number - 1):064x}"),
        "timestamp": hex(1_700_000_000 + number),
    }


def event(block: dict[str, str], log_index: int) -> dict[str, object]:
    return {
        "block_number": int(block["number"], 16),
        "block_hash": block["hash"],
        "tx_hash": "0x" + "ab" * 32,
        "tx_index": 0,
        "log_index": log_index,
        "timestamp": int(block["timestamp"], 16),
        "pool_id": None,
        "protocol": "v3",
        "kind": "swap",
        "owner": "0x" + "11" * 20,
        "custody": "0x" + "22" * 20,
        "position_key": "shared-position",
        "token_id": "7",
        "data": {},
    }


def test_catalog_replay_and_restart_keep_search_counts_exact(tmp_path):
    path = tmp_path / "market.sqlite"
    pool = SimpleNamespace(id="0x" + "31" * 20, kind="v3", token0="0x11", token1="0x22")
    tokens = {"0x11": SimpleNamespace(symbol="AAA"), "0x22": SimpleNamespace(symbol="USDG")}
    store = MarketStore(path)
    try:
        store.upsert_catalog_pools([pool, pool], tokens)
        store.upsert_catalog_pools([pool], tokens)
        assert store.search_catalog("v3")[1] == 1
        assert store.search_catalog("AAA USDG")[1] == 1
        tokens["0x11"].symbol = "BBB"
        store.upsert_catalog_pools([pool], tokens)
        assert store.search_catalog("AAA USDG") == ([], 0)
        rows, total = store.search_catalog("BBB USDG")
        assert total == 1
        assert rows[0]["label"] == "BBB / USDG"

        # Persist the inflated counters produced by older discovery replays.
        with store.transaction() as connection:
            connection.execute("UPDATE lp_catalog_pairs SET pools=7")
            connection.execute(
                "UPDATE metadata SET value=? WHERE key='catalog_search_protocol_counts'",
                ('{"v3":7}',),
            )
        store.close()
        store = MarketStore(path)
        assert store.search_catalog("v3")[1] == 1
        assert store.search_catalog("BBB USDG")[1] == 1
    finally:
        store.close()


def test_reopen_preserves_durable_counts_and_initializes_only_missing_counts(tmp_path):
    path = tmp_path / "market.sqlite"
    pool = {
        "id": "0x" + "33" * 32,
        "protocol": "v4",
        "address": "0x" + "44" * 20,
        "token0": "0x" + "11" * 20,
        "token1": "0x" + "22" * 20,
        "symbol0": None,
        "symbol1": None,
        "decimals0": None,
        "decimals1": None,
        "fee_ppm": 500,
        "tick_spacing": 5,
        "hook": None,
        "factory": "0x" + "44" * 20,
        "created_block": 10,
        "source": "test",
        "metadata_json": {},
    }
    store = MarketStore(path)
    try:
        store.upsert_pools([pool])
        with store.transaction() as connection:
            connection.execute(
                "UPDATE metadata SET value='777' WHERE key='indexed_events'",
            )
            connection.execute(
                "DELETE FROM metadata WHERE key='indexed_pools'",
            )
        store.close()

        store = MarketStore(path)
        status = store.status()
        assert status["indexed_events"] == 777
        assert status["indexed_pools"] == 1
    finally:
        store.close()


def test_repeated_search_entities_do_not_duplicate_search_results():
    store = MarketStore(":memory:")
    block = header(10)
    try:
        inserted = store.ingest(
            [block], [event(block, offset) for offset in range(50)],
        )
        assert len(inserted) == 50
        results, total = store.search("shared-position")
        assert total == 1
        assert [(row["kind"], row["id"]) for row in results] == [
            ("position", "shared-position"),
        ]
    finally:
        store.close()


def test_search_batch_skips_noop_updates_and_keeps_late_enrichment():
    store = MarketStore(":memory:")
    initial = {
        "position_key": "late-position",
        "protocol": "v3",
        "token_id": None,
        "pool_id": None,
    }
    try:
        with store.transaction() as connection:
            store._index_event_search_batch(connection, [initial])
        changes = store.connection.total_changes
        with store.transaction() as connection:
            store._index_event_search_batch(connection, [initial])
        assert store.connection.total_changes == changes

        enriched = {
            **initial,
            "token_id": "73",
            "pool_id": "0x" + "33" * 20,
        }
        with store.transaction() as connection:
            store._index_event_search_batch(connection, [enriched])

        results, total = store.search("73")
        assert total == 1
        assert results == [{
            "kind": "position",
            "id": "late-position",
            "label": "Position 73",
            "subtitle": "V3 POSITION · late-position",
            "href": "/lp?q=late-position",
        }]
    finally:
        store.close()


def test_search_batch_writes_roll_back_with_the_caller_transaction():
    store = MarketStore(":memory:")
    try:
        with pytest.raises(RuntimeError, match="abort search write"):
            with store.transaction() as connection:
                store._index_event_search_batch(
                    connection,
                    [{"position_key": "rolled-back-position", "protocol": "v4"}],
                )
                raise RuntimeError("abort search write")

        assert store.search("rolled-back-position") == ([], 0)
    finally:
        store.close()


def test_adjacent_headers_must_extend_the_same_canonical_chain():
    store = MarketStore(":memory:")
    first = header(10)
    second = header(11, parent="0x" + "ff" * 32)
    try:
        with pytest.raises(CanonicalConflict, match="does not extend supplied block"):
            store.ingest([first, second], [])
        assert store.read().execute("SELECT COUNT(*) FROM blocks").fetchone()[0] == 0
    finally:
        store.close()


def test_episode_gas_deduplicates_transactions_and_preserves_unknowns():
    store = MarketStore(":memory:")
    book = AccountBook(store).install()
    owner = "0x" + "11" * 20
    priced_tx = "0x" + "01" * 32
    first_block = header(10)
    first_events = [event(first_block, offset) for offset in range(2)]
    for row in first_events:
        row.update({
            "kind": "add",
            "tx_hash": priced_tx,
            "liquidity_delta": "1",
        })
    try:
        store.ingest(
            [first_block],
            first_events,
            transactions=[{
                "tx_hash": priced_tx,
                "block_number": 10,
                "block_hash": first_block["hash"],
                "payer": owner,
                "gas_usd": 3.5,
            }],
        )
        row = store.read().execute(
            "SELECT id,gas_usd FROM lp_accounting_episodes",
        ).fetchone()
        assert row["gas_usd"] == 3.5
        owners = book.owners({"window": "all", "limit": 10})
        beneficial = next(
            candidate for candidate in owners["rows"]
            if candidate["owner"] == owner
        )
        assert beneficial["gas_usd"] == 3.5
        assert book.owner_count("all") == owners["total"] == 2

        second_block = header(11)
        unknown_tx = "0x" + "02" * 32
        unknown_event = event(second_block, 0)
        unknown_event.update({
            "kind": "add",
            "tx_hash": unknown_tx,
            "liquidity_delta": "1",
        })
        store.ingest([second_block], [unknown_event])

        row = store.read().execute(
            "SELECT gas_usd FROM lp_accounting_episodes WHERE id=?",
            (row["id"],),
        ).fetchone()
        assert row["gas_usd"] is None
        beneficial = next(
            candidate for candidate in book.owners({"window": "all"})["rows"]
            if candidate["owner"] == owner
        )
        assert beneficial["gas_usd"] is None
    finally:
        store.close()


def test_late_position_rebuild_keeps_effects_gas_and_committed_revision():
    store = MarketStore(":memory:")
    AccountBook(store).install()
    owner = "0x" + "11" * 20
    later_block = header(11)
    later = event(later_block, 0)
    later.update({
        "kind": "checkpoint",
        "tx_hash": "0x" + "02" * 32,
        "liquidity_delta": "0",
    })
    earlier_block = header(10)
    earlier = event(earlier_block, 0)
    earlier.update({
        "kind": "add",
        "tx_hash": "0x" + "01" * 32,
        "liquidity_delta": "1",
    })
    try:
        store.ingest(
            [later_block], [later],
            transactions=[{
                "tx_hash": later["tx_hash"],
                "block_number": 11,
                "block_hash": later_block["hash"],
                "payer": owner,
                "gas_usd": 2.0,
            }],
        )
        store.ingest(
            [earlier_block], [earlier], lane="history",
            transactions=[{
                "tx_hash": earlier["tx_hash"],
                "block_number": 10,
                "block_hash": earlier_block["hash"],
                "payer": owner,
                "gas_usd": 1.0,
            }],
        )

        effects = store.read().execute(
            "SELECT block_number FROM lp_accounting_effects "
            "WHERE position_key='shared-position' ORDER BY block_number",
        ).fetchall()
        position = store.read().execute(
            "SELECT first_block,last_block FROM lp_accounting_positions "
            "WHERE position_key='shared-position'",
        ).fetchone()
        episode = store.read().execute(
            "SELECT gas_usd FROM lp_accounting_episodes "
            "WHERE position_key='shared-position'",
        ).fetchone()
        accounting_meta = dict(store.read().execute(
            "SELECT key,value FROM lp_accounting_meta",
        ).fetchall())
        events_revision = store.read().execute(
            "SELECT value FROM metadata WHERE key='events_revision'",
        ).fetchone()[0]

        assert [row["block_number"] for row in effects] == [10, 11]
        assert tuple(position) == (10, 11)
        assert episode["gas_usd"] == 3.0
        assert accounting_meta["applied_revision"] == events_revision
        assert accounting_meta["dirty"] == "0"
    finally:
        store.close()


def test_accounting_install_bulk_maps_preexisting_position_events():
    store = MarketStore(":memory:")
    block = header(10)
    raw_event = event(block, 0)
    raw_event["position_key"] = " Mixed-Position "
    try:
        store.ingest([block], [raw_event])
        AccountBook(store).install()

        mapping = store.read().execute(
            "SELECT position_key,token_id FROM lp_accounting_event_keys",
        ).fetchone()
        position = store.read().execute(
            "SELECT position_key FROM lp_accounting_positions",
        ).fetchone()
        assert (mapping["position_key"], mapping["token_id"]) == (
            "mixed-position", "7",
        )
        assert position["position_key"] == "mixed-position"
    finally:
        store.close()


def test_pool_metadata_token_tracks_only_committed_material_changes():
    store = MarketStore(":memory:")
    token0 = "0x" + "11" * 20
    token1 = "0x" + "22" * 20
    pool = {
        "id": "0x" + "33" * 32,
        "protocol": "v4",
        "address": "0x" + "44" * 20,
        "token0": token0,
        "token1": token1,
        "symbol0": None,
        "symbol1": None,
        "decimals0": None,
        "decimals1": None,
        "fee_ppm": 500,
        "tick_spacing": 5,
        "hook": None,
        "factory": "0x" + "44" * 20,
        "created_block": 10,
        "source": "test",
        "metadata_json": {"configured_fee": 500},
    }
    try:
        assert store.pool_metadata_token == 0
        store.upsert_pools([pool])
        assert store.pool_metadata_token == 1

        store.upsert_pools([pool])
        assert store.pool_metadata_token == 1

        store.save_token_metadata(token0, "AAA", 18)
        assert store.pool_metadata_token == 2
        store.save_token_metadata(token0, "AAA", 18)
        assert store.pool_metadata_token == 2

        aborted = {**pool, "id": "0x" + "55" * 32, "created_block": 20}
        with pytest.raises(RuntimeError, match="abort pool write"):
            with store.transaction() as connection:
                store.upsert_pools([aborted], connection)
                raise RuntimeError("abort pool write")
        assert store.pool_metadata_token == 2
        assert store.pool(aborted["id"]) is None

        store.rollback(9)
        assert store.pool_metadata_token == 3
        assert store.pool(pool["id"]) is None
    finally:
        store.close()

def test_close_cancels_running_reader_without_losing_committed_data(tmp_path):
    import sqlite3
    import threading

    path = tmp_path / "market.sqlite"
    store = MarketStore(path)
    block = header(10)
    store.ingest([block], [event(block, 0)])
    running = threading.Event()
    cleanup = threading.Event()
    closed = threading.Event()
    errors = []

    def read_forever():
        connection = store.read()

        def progress():
            running.set()
            return int(cleanup.is_set())

        connection.set_progress_handler(progress, 1000)
        try:
            connection.execute(
                "WITH RECURSIVE work(n) AS "
                "(VALUES(0) UNION ALL SELECT n+1 FROM work WHERE n<1000000000) "
                "SELECT SUM(n) FROM work"
            ).fetchone()
        except sqlite3.OperationalError as exc:
            errors.append(exc.sqlite_errorcode)

    def close_store():
        store.close()
        closed.set()

    reader = threading.Thread(target=read_forever, daemon=True)
    closer = threading.Thread(target=close_store, daemon=True)
    reader.start()
    try:
        assert running.wait(2)
        closer.start()
        assert closed.wait(2), "shutdown waited for an abandoned reader"
    finally:
        cleanup.set()
        reader.join(2)
        if closer.ident is not None:
            closer.join(2)
        store.close()
    assert errors == [sqlite3.SQLITE_INTERRUPT]
    with MarketStore(path) as reopened:
        rows = reopened.read().execute(
            "SELECT block_number,tx_hash FROM events"
        ).fetchall()
        assert [tuple(row) for row in rows] == [(10, event(block, 0)["tx_hash"])]
