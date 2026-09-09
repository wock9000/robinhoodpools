"""Focused durable-store batching and canonical-safety regressions."""
from __future__ import annotations

import sqlite3
from types import SimpleNamespace
import pytest

from rhpools.lp_market_store import CanonicalConflict, MarketStore
from rhpools.lp_market_accounting import AccountBook
from rhpools.lp_market_protocols import core_position_key


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


def v4_core_effect(
        block, index, raw_key, kind, liquidity_delta, before, after, *,
        cashflow=(0, 0), fees=(0, 0), deposit_usd=0.0,
        withdrawal_usd=0.0, fees_usd=0.0):
    row = event(block, index)
    row.update({
        "tx_hash": "0x" + f"{10_000 + index:064x}",
        "pool_id": None,
        "protocol": "v4",
        "kind": kind,
        "position_key": raw_key,
        "identity_basis": "verified_owner",
        "tick_lower": -10,
        "tick_upper": 10,
        "liquidity": str(after["liquidity"]),
        "liquidity_delta": str(liquidity_delta),
        "amount0": "0",
        "amount1": "0",
        "cashflow0": str(cashflow[0]),
        "cashflow1": str(cashflow[1]),
        "fee_amount0": str(fees[0]),
        "fee_amount1": str(fees[1]),
        "deposit_usd": deposit_usd,
        "withdrawal_usd": withdrawal_usd,
        "fees_usd": fees_usd,
        "accounting_basis": "complete v4 trace",
        "data": {
            "core_position_key": raw_key,
            "position_before": before,
            "position_after": after,
            "trace_complete": True,
            "fees_accrued_exact": True,
            "principal_delta_exact": True,
            "principal_delta": {
                "amount0": str(cashflow[0] - fees[0]),
                "amount1": str(cashflow[1] - fees[1]),
            },
        },
    })
    return row


def drain_accounting(book):
    while book.project_pending(limit=32):
        pass


def test_financial_queues_do_not_starve_recent_work_or_history(tmp_path):
    with MarketStore(tmp_path / "financial-queues.sqlite") as store:
        pool_id = "0x" + "31" * 20
        store.upsert_pools([{
            "id": pool_id, "protocol": "v3", "address": pool_id,
            "token0": "0x" + "11" * 20, "token1": "0x" + "22" * 20,
        }])
        blocks = [header(number) for number in range(1, 49)]
        store.ingest(blocks, [
            {
                **event(block, 0), "pool_id": pool_id, "kind": "add",
                "tx_hash": "0x" + f"{index:064x}",
            }
            for index, block in enumerate(blocks, 1)
        ])
        with store.transaction() as connection:
            connection.execute(
                "UPDATE pending_enrichment SET last_error="
                "'pool_identity_pending:{}' WHERE block_number<=8"
            )
            connection.execute(
                "INSERT INTO pending_reprojection"
                "(event_id,block_number,tx_index,log_index) "
                "SELECT id,block_number,tx_index,log_index FROM events"
            )
            for block in blocks:
                store.queue_v3_balances(
                    [pool_id], int(block["number"], 16), block["hash"],
                )
            for table in (
                "pending_enrichment", "pending_reprojection", "pending_balances",
            ):
                connection.execute(
                    f"UPDATE {table} SET next_attempt=1e99 "
                    "WHERE block_number IN (1,9,48)"
                )

        for selected, oldest in (
            (store.pending_enrichments(8), 10),
            (store.pending_reprojections(8), 2),
            (store.pending_v3_balances(8), 2),
        ):
            numbers = [row["block_number"] for row in selected]
            assert numbers == sorted(set(numbers))
            assert len(numbers) == 8
            assert numbers[0] == oldest
            assert numbers[-1] == 47
            assert not {1, 9, 48}.intersection(numbers)
        store.prioritize_enrichment("0x" + f"{number:064x}" for number in (20, 21, 48))
        requested = store.pending_enrichments(8)
        numbers = {row["block_number"] for row in requested}
        assert {10, 11, 20, 21, 47} <= numbers
        assert 48 not in numbers


def test_enrichment_exclusions_do_not_consume_the_selection_limit(tmp_path):
    with MarketStore(tmp_path / "enrichment-exclusions.sqlite") as store:
        blocks = [header(number) for number in range(1, 13)]
        store.ingest(blocks, [
            {
                **event(block, 0),
                "kind": "add",
                "tx_hash": "0x" + f"{number:064x}",
            }
            for number, block in enumerate(blocks, 1)
        ])

        excluded = {
            "0x" + f"{number:064x}" for number in (1, 12)
        }
        selected = store.pending_enrichments(4, exclude=excluded)

        assert [row["block_number"] for row in selected] == [2, 9, 10, 11]


def test_enrichment_uses_the_newest_requested_interest(tmp_path):
    with MarketStore(tmp_path / "enrichment-interest.sqlite") as store:
        blocks = [header(number) for number in range(1, 13)]
        store.ingest(blocks, [
            {
                **event(block, 0),
                "kind": "add",
                "tx_hash": "0x" + f"{number:064x}",
            }
            for number, block in enumerate(blocks, 1)
        ])
        hashes = {
            number: "0x" + f"{number:064x}" for number in (5, 6)
        }

        store.prioritize_enrichment([hashes[5], hashes[6]])
        selected = store.pending_enrichments(4, prioritized=True)
        assert {row["block_number"] for row in selected} == {1, 6, 11, 12}

        store.prioritize_enrichment([hashes[5]])
        selected = store.pending_enrichments(4, prioritized=True)
        assert {row["block_number"] for row in selected} == {1, 5, 11, 12}


def test_requested_identity_work_preserves_the_normal_lane_interest(tmp_path):
    with MarketStore(tmp_path / "enrichment-identity-interest.sqlite") as store:
        blocks = [header(number) for number in range(1, 13)]
        store.ingest(blocks, [
            {
                **event(block, 0),
                "kind": "add",
                "tx_hash": "0x" + f"{number:064x}",
            }
            for number, block in enumerate(blocks, 1)
        ])
        hashes = {
            number: "0x" + f"{number:064x}" for number in (5, 6)
        }
        with store.transaction() as connection:
            connection.execute(
                "UPDATE pending_enrichment SET "
                "last_error='pool_identity_pending:{}' WHERE tx_hash=?",
                (hashes[5],),
            )
        store.prioritize_enrichment([hashes[5], hashes[6]])

        assert [
            row["block_number"]
            for row in store.requested_enrichments(1, identity=True)
        ] == [5]
        assert [
            row["block_number"]
            for row in store.requested_enrichments(1)
        ] == [6]


def test_bulk_ingest_preserves_conflicts_under_sqlite_parameter_limit(tmp_path):
    path = tmp_path / "bounded-inserts.sqlite"
    block = header(10)
    rows = [
        {**event(block, index), "tx_hash": "0x" + f"{index + 1:064x}"}
        for index in range(70)
    ]
    with MarketStore(path) as store:
        store.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 128)
        store.ingest(
            [block], rows + [rows[0]], lane="live",
            cursor={"block_number": 10, "block_hash": block["hash"]},
        )
        assert store.ingest([block], rows) == []
        assert [
            row["log_index"] for row in store.read().execute(
                "SELECT log_index FROM events ORDER BY log_index"
            )
        ] == list(range(70))
        assert store.status()["indexed_events"] == 70

    with MarketStore(path) as store:
        assert store.cursor("live")["block_hash"] == block["hash"]
        found, total = store.search(str(rows[-1]["tx_hash"]))
        assert total == 1
        assert found[0]["id"] == rows[-1]["tx_hash"]


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


def test_legacy_core_position_repair_is_bounded_resumable_and_atomic(tmp_path):
    path = tmp_path / "core-position-repair.sqlite"
    raw_key = "0x" + "ca" * 32
    v3_pool = "0x" + "31" * 20
    v4_pool = "0x" + "42" * 32
    v3_key = core_position_key("v3", v3_pool, raw_key)
    v4_key = core_position_key("v4", v4_pool, raw_key)
    blocks = [header(number) for number in range(10, 16)]
    cursor = {
        "from_block": 10,
        "to_block": 15,
        "block_number": 15,
        "block_hash": blocks[-1]["hash"],
    }
    rows = []
    for index, block in enumerate(blocks):
        protocol = "v3" if index < 3 else "v4"
        pool_id = v3_pool if protocol == "v3" else v4_pool
        rows.append({
            **event(block, index),
            "tx_hash": "0x" + f"{index + 1:064x}",
            "pool_id": None if index == 5 else pool_id,
            "protocol": protocol,
            "position_key": raw_key,
            "amount0": str(100 + index),
            "fees_usd": index + 0.25,
            "accounting_basis": f"evidence-{index}",
            "data": {
                "core_position_key": raw_key,
                "evidence": {"trace": index, "source": "receipt"},
            },
        })

    store = MarketStore(path)
    try:
        book = AccountBook(store, deferred=True).install()
        inserted = store.ingest(blocks, rows, lane="live", cursor=cursor)
        cursor = store.cursor("live")
        with store.transaction() as connection:
            connection.execute("DELETE FROM lp_accounting_pending")
            store._set_metadata(connection, "pending_accounting", 0)
            connection.execute(
                "UPDATE lp_accounting_event_keys SET position_key=?",
                (raw_key,),
            )
            book._queue_position_keys(connection, {raw_key: (15, 0, 5)})
        event_ids = [int(row["id"]) for row in inserted]
        source_before = [
            tuple(row) for row in store.read().execute(
                "SELECT id,block_number,block_hash,tx_hash,amount0,fees_usd,"
                "accounting_basis,data FROM events "
                "ORDER BY block_number,tx_index,log_index"
            )
        ]
        raw_generation = int(store.read().execute(
            "SELECT generation FROM lp_accounting_pending WHERE position_key=?",
            (raw_key,),
        ).fetchone()[0])

        assert store.repair_legacy_core_position_keys(limit=2) is True
        assert [
            row["position_key"] for row in store.read().execute(
                "SELECT position_key FROM events "
                "ORDER BY block_number,tx_index,log_index"
            )
        ] == [v3_key, v3_key, raw_key, raw_key, raw_key, raw_key]
        assert [
            row["position_key"] for row in store.read().execute(
                "SELECT position_key FROM lp_accounting_event_keys "
                "ORDER BY event_id"
            )
        ] == [v3_key, v3_key, raw_key, raw_key, raw_key, raw_key]
        checkpoint = store._metadata(
            store.read(), "core_position_key_source_repair_v1", {},
        )
        assert checkpoint["after_position_key"] == "0x"
        assert checkpoint["repaired_events"] == 2
        assert {
            row["id"] for row in store.read().execute(
                "SELECT id FROM lp_search_entities WHERE kind='position'"
            )
        } == {raw_key, v3_key}
        first_pending = {
            row["position_key"]: int(row["generation"])
            for row in store.read().execute(
                "SELECT position_key,generation FROM lp_accounting_pending"
            )
        }
        assert first_pending[raw_key] > raw_generation
        assert v3_key in first_pending
        assert store.cursor("live") == cursor
        store.close()

        store = MarketStore(path)
        AccountBook(store, deferred=True).install()
        checkpoint = store._metadata(
            store.read(), "core_position_key_source_repair_v1", {},
        )
        assert checkpoint["after_position_key"] == "0x"
        assert checkpoint["repaired_events"] == 2

        assert store.repair_legacy_core_position_keys(limit=2) is True
        assert [
            row["position_key"] for row in store.read().execute(
                "SELECT position_key FROM events "
                "ORDER BY block_number,tx_index,log_index"
            )
        ] == [v3_key, v3_key, v3_key, v4_key, raw_key, raw_key]
        assert store.repair_legacy_core_position_keys(limit=2) is True
        checkpoint = store._metadata(
            store.read(), "core_position_key_source_repair_v1", {},
        )
        assert checkpoint["after_position_key"] == raw_key
        assert checkpoint["repaired_events"] == 5

        assert store.repair_legacy_core_position_keys(limit=2) is True
        reset_checkpoint = store._metadata(
            store.read(), "core_position_key_source_repair_v1", {},
        )
        assert reset_checkpoint["after_position_key"] == "0x"
        assert reset_checkpoint["cycles"] == 1
        assert "exhausted_revision" not in reset_checkpoint
        assert store.repair_legacy_core_position_keys(limit=2) is False
        exhausted = store._metadata(
            store.read(), "core_position_key_source_repair_v1", {},
        )
        assert exhausted["exhausted_revision"] == store.status()["events_revision"]
        assert store.repair_legacy_core_position_keys(limit=2) is False
        assert store._metadata(
            store.read(), "core_position_key_source_repair_v1", {},
        ) == exhausted

        store.enrich([{"id": event_ids[-1], "pool_id": v4_pool}])
        assert store.repair_legacy_core_position_keys(limit=1) is True
        assert store.repair_legacy_core_position_keys(limit=1) is True
        assert store.repair_legacy_core_position_keys(limit=1) is False
        assert [
            row["position_key"] for row in store.read().execute(
                "SELECT position_key FROM events "
                "ORDER BY block_number,tx_index,log_index"
            )
        ] == [v3_key, v3_key, v3_key, v4_key, v4_key, v4_key]
        assert [
            tuple(row) for row in store.read().execute(
                "SELECT id,block_number,block_hash,tx_hash,amount0,fees_usd,"
                "accounting_basis,data FROM events "
                "ORDER BY block_number,tx_index,log_index"
            )
        ] == source_before
        assert [
            int(row["id"]) for row in store.read().execute(
                "SELECT id FROM events ORDER BY block_number,tx_index,log_index"
            )
        ] == event_ids
        assert {
            row["id"] for row in store.read().execute(
                "SELECT id FROM lp_search_entities WHERE kind='position'"
            )
        } == {v3_key, v4_key}
        assert store.read().execute(
            "SELECT 1 FROM lp_search_terms "
            "WHERE kind='position' AND id=? LIMIT 1",
            (raw_key,),
        ).fetchone() is None
        assert [
            row["position_key"] for row in store.read().execute(
                "SELECT position_key FROM lp_accounting_event_keys "
                "ORDER BY event_id"
            )
        ] == [v3_key, v3_key, v3_key, v4_key, v4_key, v4_key]
        pending = {
            row["position_key"]: (
                int(row["generation"]), int(row["requested_revision"])
            )
            for row in store.read().execute(
                "SELECT position_key,generation,requested_revision "
                "FROM lp_accounting_pending"
            )
        }
        assert set(pending) == {raw_key, v3_key, v4_key}
        assert pending[raw_key][0] > first_pending[raw_key]
        events_revision = store.status()["events_revision"]
        assert pending[v4_key][1] == events_revision
        assert all(
            0 < requested_revision <= events_revision
            for _generation, requested_revision in pending.values()
        )
        assert store.cursor("live") == cursor
        assert store.read().execute(
            "SELECT COUNT(*) FROM events "
            "WHERE position_key>=? AND position_key<? "
            "AND LENGTH(position_key)=66",
            ("0x", "0y"),
        ).fetchone()[0] == 0
    finally:
        store.close()


def test_partial_core_position_repair_unqualifies_legacy_episode_money(tmp_path):
    raw_key = "0x" + "d1" * 32
    first_pool = "0x" + "31" * 32
    second_pool = "0x" + "42" * 32
    first_key = core_position_key("v4", first_pool, raw_key)
    second_key = core_position_key("v4", second_pool, raw_key)
    owner = "0x" + "11" * 20
    blocks = [header(number) for number in range(10, 14)]
    empty = {
        "liquidity": "0", "tokens_owed0": "0", "tokens_owed1": "0",
        "claims_empty": True,
    }
    funded = {
        "liquidity": "1000", "tokens_owed0": "0", "tokens_owed1": "0",
        "claims_empty": True,
    }
    events = []
    for offset in (0, 2):
        events.extend([
            v4_core_effect(
                blocks[offset], offset, raw_key, "add", 1000, empty, funded,
                cashflow=(-10_000_000, -10_000_000), deposit_usd=20.0,
            ),
            v4_core_effect(
                blocks[offset + 1], offset + 1, raw_key, "remove", -1000,
                funded, empty, cashflow=(11_000_000, 10_000_000),
                fees=(1_000_000, 0), withdrawal_usd=21.0, fees_usd=1.0,
            ),
        ])
    transactions = [{
        "tx_hash": row["tx_hash"],
        "block_number": row["block_number"],
        "block_hash": row["block_hash"],
        "payer": owner,
        "gas_usd": 0.1,
    } for row in events]

    store = MarketStore(tmp_path / "partial-core-position-repair.sqlite")
    book = AccountBook(store).install()
    try:
        store.upsert_pools([
            {
                "id": pool_id, "protocol": "v4", "address": "0x" + "22" * 20,
                "token0": "0x" + "51" * 20, "token1": "0x" + "62" * 20,
                "decimals0": 6, "decimals1": 6, "created_block": 10,
            }
            for pool_id in (first_pool, second_pool)
        ])
        store.ingest(blocks, events, transactions=transactions)

        # AccountBook qualifies pool-aware raw keys at ingestion. Attach the
        # canonical source scopes after projection to recreate the durable
        # pre-scope ledger with two closed episodes under one raw identity.
        with store.transaction() as connection:
            connection.execute(
                "UPDATE events SET pool_id=CASE WHEN block_number<=11 THEN ? ELSE ? END "
                "WHERE position_key=?",
                (first_pool, second_pool, raw_key),
            )
            connection.execute(
                "UPDATE lp_accounting_episodes "
                "SET pool_id=CASE WHEN opened_block=10 THEN ? ELSE ? END "
                "WHERE position_key=?",
                (first_pool, second_pool, raw_key),
            )
            connection.execute(
                "UPDATE lp_accounting_positions SET pool_id=? WHERE position_key=?",
                (second_pool, raw_key),
            )

        original = book.closed({"window": "all"})["rows"]
        assert len(original) == 2
        assert all(row["position_key"] == raw_key for row in original)
        assert all(row["coverage"]["qualified"] for row in original)
        assert all(row["coverage"]["cost_qualified"] for row in original)
        baseline_owner = book.owner(owner, {"window": "all"})
        assert baseline_owner["summary"]["gross_pnl_usd"] == pytest.approx(2.0)
        assert baseline_owner["summary"]["net_pnl_usd"] == pytest.approx(1.6)
        store.close()
        store = MarketStore(tmp_path / "partial-core-position-repair.sqlite")
        book = AccountBook(store, deferred=True).install()

        assert store.repair_legacy_core_position_keys(limit=2) is True
        assert book.project_pending(limit=1) is True

        partial = book.closed({"window": "all"})
        migrated = next(
            row for row in partial["rows"] if row["position_key"] == first_key
        )
        unresolved = [
            row for row in partial["rows"] if row["position_key"] == raw_key
        ]
        assert migrated["coverage"]["qualified"] is True
        assert migrated["gross_pnl_usd"] == pytest.approx(1.0)
        assert migrated["net_pnl_usd"] == pytest.approx(0.8)
        assert unresolved
        assert all(row["coverage"]["qualified"] is False for row in unresolved)
        assert all(row["coverage"]["cost_qualified"] is False for row in unresolved)
        assert all(row[field] is None for row in unresolved for field in (
            "fees_usd", "gross_pnl_usd", "gas_usd", "net_pnl_usd", "return_pct",
        ))

        partial_owner = book.owner(owner, {"window": "all"})
        assert partial_owner["summary"]["gross_pnl_usd"] is None
        assert partial_owner["summary"]["net_pnl_usd"] is None
        assert partial_owner["summary"]["fees_usd"] is None
        assert partial_owner["summary"]["win_rate"] is None
        assert partial_owner["coverage"]["qualified"] is False
        owner_row = book.owners({
            "window": "all", "identity_scope": "wallets",
        })["rows"][0]
        assert owner_row["gross_pnl_usd"] is None
        assert owner_row["net_pnl_usd"] is None
        assert owner_row["fees_usd"] is None
        assert owner_row["volume_usd"] is None
        assert owner_row["win_rate"] is None
        assert owner_row["coverage"]["qualified"] is False

        assert store.repair_legacy_core_position_keys(limit=2) is True
        drain_accounting(book)

        repaired = book.closed({"window": "all"})
        assert repaired["total"] == 2
        assert {row["position_key"] for row in repaired["rows"]} == {
            first_key, second_key,
        }
        assert all(row["coverage"]["qualified"] for row in repaired["rows"])
        assert all(row["coverage"]["cost_qualified"] for row in repaired["rows"])
        assert all(row["gross_pnl_usd"] == pytest.approx(1.0)
                   for row in repaired["rows"])
        assert all(row["net_pnl_usd"] == pytest.approx(0.8)
                   for row in repaired["rows"])
        final_owner = book.owner(owner, {"window": "all"})
        assert final_owner["summary"]["positions"] == 2
        assert final_owner["summary"]["closed_episodes"] == 2
        assert final_owner["summary"]["gross_pnl_usd"] == pytest.approx(2.0)
        assert final_owner["summary"]["gas_usd"] == pytest.approx(0.4)
        assert final_owner["summary"]["net_pnl_usd"] == pytest.approx(1.6)
        assert final_owner["summary"]["fees_usd"] == pytest.approx(2.0)
        assert final_owner["summary"]["win_rate"] == pytest.approx(100.0)
        assert final_owner["coverage"]["qualified"] is True
        assert final_owner["coverage"]["cost_qualified"] is True
        final_owner_row = book.owners({
            "window": "all", "identity_scope": "wallets",
        })["rows"][0]
        assert final_owner_row["gross_pnl_usd"] == pytest.approx(2.0)
        assert final_owner_row["gas_usd"] == pytest.approx(0.4)
        assert final_owner_row["net_pnl_usd"] == pytest.approx(1.6)
        assert final_owner_row["fees_usd"] == pytest.approx(2.0)
        assert final_owner_row["volume_usd"] == pytest.approx(82.0)
        assert final_owner_row["win_rate"] == pytest.approx(100.0)
        assert final_owner_row["coverage"]["qualified"] is True
        assert raw_key not in {
            row["position_key"] for row in book.positions()["rows"]
        }
    finally:
        store.close()


def test_partial_core_position_repair_does_not_double_pool_inventory(tmp_path):
    raw_key = "0x" + "d2" * 32
    pool_id = "0x" + "73" * 32
    scoped_key = core_position_key("v4", pool_id, raw_key)
    liquidity = 10**18
    blocks = [header(number) for number in range(10, 140)]
    empty = {
        "liquidity": "0", "tokens_owed0": "0", "tokens_owed1": "0",
        "claims_empty": True,
    }
    funded = {
        "liquidity": str(liquidity), "tokens_owed0": "0", "tokens_owed1": "0",
        "claims_empty": True,
    }
    events = [
        v4_core_effect(
            blocks[0], 0, raw_key, "add", liquidity, empty, funded,
        ),
        *[
            v4_core_effect(
                block, index, raw_key, "checkpoint", 0, funded, funded,
            )
            for index, block in enumerate(blocks[1:], 1)
        ],
    ]

    store = MarketStore(tmp_path / "partial-core-position-inventory.sqlite")
    book = AccountBook(store).install()
    try:
        store.upsert_pools([{
            "id": pool_id, "protocol": "v4", "address": "0x" + "22" * 20,
            "token0": "0x" + "51" * 20, "token1": "0x" + "62" * 20,
            "decimals0": 6, "decimals1": 6, "created_block": 10,
        }])
        with store.transaction() as connection:
            connection.execute(
                "CREATE TABLE lp_pool_state("
                "pool_id TEXT PRIMARY KEY,block_number INTEGER NOT NULL,"
                "tx_index INTEGER NOT NULL,log_index INTEGER NOT NULL,"
                "timestamp INTEGER NOT NULL,sqrt_price_x96 TEXT,tick INTEGER,"
                "liquidity TEXT,price0_usd REAL,price1_usd REAL)"
            )
            connection.execute(
                "INSERT INTO lp_pool_state VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    pool_id, 139, 0, 129, 1_700_000_139, str(1 << 96), 0,
                    str(liquidity), 1.0, 1.0,
                ),
            )
        store.ingest(blocks, events, cursor={
            "from_block": 10,
            "to_block": 139,
            "block_number": 139,
            "block_hash": blocks[-1]["hash"],
        })
        with store.transaction() as connection:
            connection.execute(
                "UPDATE events SET pool_id=? WHERE position_key=?",
                (pool_id, raw_key),
            )
            connection.execute(
                "UPDATE lp_accounting_positions SET pool_id=? WHERE position_key=?",
                (pool_id, raw_key),
            )

        baseline = book.pool_stats([pool_id])[pool_id]
        assert baseline["open_positions"] == 1
        assert baseline["complete_inventory"] is True
        assert baseline["observed_principal_usd"] is not None
        assert baseline["observed_principal_usd"] > 0
        assert baseline["observed_active_tvl_usd"] == pytest.approx(
            baseline["observed_principal_usd"],
        )
        store.close()
        store = MarketStore(tmp_path / "partial-core-position-inventory.sqlite")
        book = AccountBook(store, deferred=True).install()

        assert store.repair_legacy_core_position_keys(limit=128) is True
        assert book.project_pending(limit=1) is True

        positions = {
            row["position_key"]: row
            for row in book.positions({"status": "open"})["rows"]
        }
        assert set(positions) == {raw_key, scoped_key}
        assert positions[raw_key]["liquidity"] is None
        assert positions[raw_key]["principal_usd"] is None
        assert positions[raw_key]["equity_usd"] is None
        assert positions[raw_key]["coverage"]["qualified"] is False
        assert int(positions[scoped_key]["liquidity"]) == liquidity
        partial = book.pool_stats([pool_id])[pool_id]
        assert partial["open_positions"] == 2
        assert partial["complete_inventory"] is False
        assert partial["observed_principal_usd"] == pytest.approx(
            baseline["observed_principal_usd"],
        )
        assert partial["observed_active_tvl_usd"] == pytest.approx(
            baseline["observed_active_tvl_usd"],
        )

        assert store.repair_legacy_core_position_keys(limit=128) is True
        drain_accounting(book)

        repaired_positions = book.positions({"status": "open"})["rows"]
        assert len(repaired_positions) == 1
        assert repaired_positions[0]["position_key"] == scoped_key
        repaired = book.pool_stats([pool_id])[pool_id]
        assert repaired["open_positions"] == 1
        assert repaired["complete_inventory"] is True
        assert repaired["observed_principal_usd"] == pytest.approx(
            baseline["observed_principal_usd"],
        )
        assert repaired["observed_active_tvl_usd"] == pytest.approx(
            baseline["observed_active_tvl_usd"],
        )
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


def test_schema_migrations_preserve_durable_accounting_state(tmp_path):
    path = tmp_path / "schema-migration.sqlite"
    block = header(10)
    store = MarketStore(path)
    try:
        AccountBook(store).install()
        store.ingest(
            [block],
            [event(block, 0)],
            cursor={
                "from_block": 10,
                "to_block": 10,
                "block_number": 10,
                "block_hash": block["hash"],
            },
        )
        cursor = store.cursor("live")
        accounting_revision = store.read().execute(
            "SELECT value FROM lp_accounting_meta "
            "WHERE key='applied_revision'"
        ).fetchone()[0]
        with store.transaction() as connection:
            connection.execute("DROP TABLE lp_accounting_pool_generations")
            connection.execute(
                "DROP INDEX lp_accounting_episodes_last_timestamp"
            )
            connection.execute(
                "DROP INDEX lp_accounting_positions_active_inventory"
            )
            connection.execute("PRAGMA user_version=4")
        store.close()

        store = MarketStore(path)
        reader = store.read()
        assert [
            tuple(row) for row in reader.execute(
                "SELECT block_hash,position_key FROM events"
            )
        ] == [(block["hash"], "shared-position")]
        assert store.cursor("live") == cursor
        assert [
            tuple(row) for row in reader.execute(
                "SELECT lane,start_block,end_block FROM coverage_intervals"
            )
        ] == [("live", 10, 10)]
        assert [
            tuple(row) for row in reader.execute(
                "SELECT position_key FROM lp_accounting_positions"
            )
        ] == [("shared-position",)]
        assert [
            tuple(row) for row in reader.execute(
                "SELECT position_key FROM lp_accounting_event_keys"
            )
        ] == [("shared-position",)]
        assert reader.execute(
            "SELECT value FROM lp_accounting_meta "
            "WHERE key='applied_revision'"
        ).fetchone()[0] == accounting_revision
        assert reader.execute(
            "SELECT COUNT(*) FROM lp_accounting_pool_generations"
        ).fetchone()[0] == 0

        position_before_v5 = tuple(reader.execute(
            "SELECT position_key,pool_id,active_episode_id,status,"
            "history_complete,state_json FROM lp_accounting_positions"
        ).fetchone())
        with store.transaction() as connection:
            connection.execute(
                "INSERT INTO lp_accounting_pool_generations(pool_id,generation) "
                "VALUES('preserved-pool',7)"
            )
            connection.execute(
                "DROP INDEX lp_accounting_positions_active_inventory"
            )
            connection.execute("PRAGMA user_version=5")
        store.close()

        store = MarketStore(path)
        reader = store.read()
        assert tuple(reader.execute(
            "SELECT position_key,pool_id,active_episode_id,status,"
            "history_complete,state_json FROM lp_accounting_positions"
        ).fetchone()) == position_before_v5
        assert [
            tuple(row) for row in reader.execute(
                "SELECT pool_id,generation FROM lp_accounting_pool_generations"
            )
        ] == [("preserved-pool", 7)]
        assert store.cursor("live") == cursor
        assert reader.execute(
            "SELECT value FROM lp_accounting_meta "
            "WHERE key='applied_revision'"
        ).fetchone()[0] == accounting_revision

        with store.transaction() as connection:
            store._queue_enrichment(connection, [{**event(block, 0), "kind": "add"}])
            connection.execute("DROP INDEX pending_enrichment_financial_order_idx")
            connection.execute("PRAGMA user_version=6")
        store.close()
        store = MarketStore(path)
        reader = store.read()
        assert store.pending_enrichments(1)[0]["tx_hash"] == event(block, 0)["tx_hash"]
        assert store.status()["pending_enrichment"] == 1
        assert store.cursor("live") == cursor
        assert tuple(reader.execute(
            "SELECT position_key,pool_id,active_episode_id,status,"
            "history_complete,state_json FROM lp_accounting_positions"
        ).fetchone()) == position_before_v5
        assert reader.execute(
            "SELECT value FROM lp_accounting_meta WHERE key='applied_revision'"
        ).fetchone()[0] == accounting_revision

        with store.transaction() as connection:
            connection.execute("DELETE FROM lp_accounting_pending")
            connection.execute(
                "INSERT INTO lp_accounting_pending("
                "id,position_key,generation,requested_revision,requested_epoch,"
                "priority_block,priority_tx_index,priority_log_index"
                ") VALUES(41,'shared-position',7,19,3,10,2,5)"
            )
        def committed_state(connection: sqlite3.Connection) -> dict[str, object]:
            return {
                "events": [
                    tuple(row) for row in connection.execute(
                        "SELECT id,block_hash,position_key,revision FROM events"
                    )
                ],
                "coverage": [
                    tuple(row) for row in connection.execute(
                        "SELECT lane,start_block,end_block,start_hash,end_hash "
                        "FROM coverage_intervals"
                    )
                ],
                "position": tuple(connection.execute(
                    "SELECT position_key,pool_id,active_episode_id,status,"
                    "history_complete,state_json FROM lp_accounting_positions"
                ).fetchone()),
                "event_keys": [
                    tuple(row) for row in connection.execute(
                        "SELECT event_id,position_key,token_id "
                        "FROM lp_accounting_event_keys"
                    )
                ],
                "accounting_meta": [
                    tuple(row) for row in connection.execute(
                        "SELECT key,value FROM lp_accounting_meta ORDER BY key"
                    )
                ],
                "pool_generations": [
                    tuple(row) for row in connection.execute(
                        "SELECT pool_id,generation "
                        "FROM lp_accounting_pool_generations ORDER BY pool_id"
                    )
                ],
                "pending": tuple(connection.execute(
                    "SELECT id,position_key,generation,requested_revision,"
                    "requested_epoch,priority_block,priority_tx_index,"
                    "priority_log_index FROM lp_accounting_pending"
                ).fetchone()),
            }

        committed_before_v9 = committed_state(reader)
        with store.transaction() as connection:
            connection.execute("DROP TABLE lp_accounting_pending_identities")
            connection.execute(
                "DROP INDEX lp_accounting_pending_identity_bootstrap"
            )
            connection.execute("DROP INDEX lp_accounting_pending_recent")
            connection.execute(
                "ALTER TABLE lp_accounting_pending "
                "RENAME TO lp_accounting_pending_v9"
            )
            connection.executescript("""
                CREATE TABLE lp_accounting_pending(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_key TEXT NOT NULL UNIQUE,
                    generation INTEGER NOT NULL,
                    requested_revision INTEGER NOT NULL,
                    requested_epoch INTEGER NOT NULL,
                    priority_block INTEGER NOT NULL,
                    priority_tx_index INTEGER NOT NULL,
                    priority_log_index INTEGER NOT NULL
                );
                INSERT INTO lp_accounting_pending(
                    id,position_key,generation,requested_revision,requested_epoch,
                    priority_block,priority_tx_index,priority_log_index
                )
                SELECT
                    id,position_key,generation,requested_revision,requested_epoch,
                    priority_block,priority_tx_index,priority_log_index
                FROM lp_accounting_pending_v9;
                DROP TABLE lp_accounting_pending_v9;
                CREATE INDEX lp_accounting_pending_recent
                    ON lp_accounting_pending(
                        priority_block DESC,priority_tx_index DESC,
                        priority_log_index DESC,id DESC
                    );
                PRAGMA user_version=8;
            """)
        store.close()

        store = MarketStore(path)
        reader = store.read()
        committed_after_v9 = committed_state(reader)
        assert committed_after_v9 == committed_before_v9
        assert store.cursor("live") == cursor
        assert tuple(reader.execute(
            "SELECT identities_ready,identity_cursor "
            "FROM lp_accounting_pending WHERE position_key='shared-position'"
        ).fetchone()) == (0, 0)

        with store.transaction() as connection:
            store._install_accounting_pending_identities(connection)
            store._install_accounting_pending_identities(connection)
            connection.execute(
                "INSERT INTO lp_accounting_pending_identities("
                "position_key,kind,identity,protocol,pool_id,timestamp"
                ") VALUES('shared-position','owner','0xowner','v3','',123)"
            )
            connection.execute(
                "DELETE FROM lp_accounting_pending "
                "WHERE position_key='shared-position'"
            )
        assert reader.execute(
            "SELECT COUNT(*) FROM lp_accounting_pending_identities"
        ).fetchone()[0] == 0
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


def test_reprojection_refreshes_new_position_search_terms():
    with MarketStore(":memory:") as store:
        block = header(10)
        inserted = store.ingest([block], [event(block, 0)])
        pool_id = "0x" + "33" * 20
        assert store.search(pool_id) == ([], 0)

        def resolve_pool(_connection, events):
            for row in events:
                row["pool_id"] = pool_id

        store.register_projection(resolve_pool, lambda _connection, _ancestor: None)
        store.reproject([inserted[0]["id"]])
        results, total = store.search(pool_id)
        assert total == 1
        assert [(row["kind"], row["id"]) for row in results] == [
            ("position", "shared-position"),
        ]


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
        finally:
            store.close_reader()

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


def test_rollback_repairs_only_orphan_search_identities(tmp_path):
    path = tmp_path / "search-reorg.sqlite"
    with MarketStore(path) as store:
        first, orphan = header(10), header(11)
        canonical = event(first, 0)
        changed = event(orphan, 0)
        changed.update({"tx_hash": "0x" + "cd" * 32, "token_id": "99"})
        removed = event(orphan, 1)
        removed.update({
            "tx_hash": "0x" + "ef" * 32,
            "owner": "0x" + "33" * 20,
            "custody": "0x" + "44" * 20,
            "position_key": "orphan-position",
        })
        store.ingest([first], [canonical])
        store.ingest([orphan], [changed, removed])
        store.ensure_search_index()
        owner_before = store.search(str(canonical["owner"]))
        store.rollback(10)
        assert store.search(str(canonical["owner"])) == owner_before
        for value in (changed["tx_hash"], removed["tx_hash"], removed["owner"],
                      removed["custody"], removed["position_key"]):
            assert store.search(str(value)) == ([], 0)
        position = store.search("shared-position")[0]
        assert [(row["kind"], row["label"]) for row in position] == [
            ("position", "Position 7"),
        ]
        assert store.search("99") == ([], 0)
    with MarketStore(path) as reopened:
        assert reopened.search(str(canonical["owner"])) == owner_before
        assert reopened.search("orphan-position") == ([], 0)


def test_rollback_keeps_shared_tokens_from_unknown_age_cross_protocol_pool():
    shared_token = "0x" + "55" * 20
    orphan_token = "0x" + "66" * 20
    surviving = {
        "id": "0x" + "77" * 20, "address": "0x" + "77" * 20,
        "protocol": "v2", "token0": shared_token, "token1": "0x" + "88" * 20,
        "symbol0": "SHARED", "symbol1": "USDG", "created_block": None,
    }
    orphan = {
        "id": "0x" + "99" * 20, "address": "0x" + "99" * 20,
        "protocol": "v3", "token0": shared_token, "token1": orphan_token,
        "symbol0": "SHARED", "symbol1": "ORPHAN", "created_block": 11,
    }
    with MarketStore(":memory:") as store:
        store.upsert_pools([surviving, orphan])
        store.ensure_search_index()
        store.rollback(10)
        assert store.search(orphan["id"]) == ([], 0)
        assert store.search(orphan_token) == ([], 0)
        assert store.search("ORPHAN") == ([], 0)
        shared = store.search(shared_token)[0]
        assert {(row["kind"], row["id"]) for row in shared} == {
            ("token", shared_token), ("pool", surviving["id"]),
        }
        assert store.search(surviving["id"])[1] == 1
