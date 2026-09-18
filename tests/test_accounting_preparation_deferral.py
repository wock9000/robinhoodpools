"""Oversized pending positions defer instead of wedging the accounting lane."""
from __future__ import annotations

import sqlite3
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool

from rhpools.lp_market_accounting import (
    AccountBook,
    PreparationDeadlineError,
    _snapshot_deadline_error,
)
from rhpools.lp_market_store import MarketStore


def test_snapshot_deadline_error_matches_interrupt_and_deadline():
    assert _snapshot_deadline_error(
        PreparationDeadlineError(
            "accounting preparation snapshot exceeded its deadline",
        )
    )
    assert _snapshot_deadline_error(sqlite3.OperationalError("interrupted"))
    assert _snapshot_deadline_error(
        RuntimeError("reader snapshot exceeded its deadline")
    )
    assert not _snapshot_deadline_error(ValueError("provider unavailable"))


def test_deadline_preparation_defers_instead_of_wedging(monkeypatch):
    store = MarketStore(":memory:")
    book = AccountBook(store, deferred=True).install()
    try:
        def fake_prepare(position_key, budget_scale=1.0):
            if position_key == "poison":
                raise PreparationDeadlineError(
                    "accounting preparation snapshot exceeded its deadline",
                )
            return None

        monkeypatch.setattr(book, "_prepare_pending", fake_prepare)
        pending = [{"position_key": "poison"}, {"position_key": "ok"}]
        assert list(book._prepared_pending(pending)) == [None, None]
        assert book._deferred_position_keys() == {"poison"}
        assert book._position_budget_scale("poison") == 2.0
        assert book._position_budget_scale("ok") == 1.0
    finally:
        store.close()


def test_pool_preparation_isolates_broken_worker(monkeypatch):
    store = MarketStore(":memory:")
    book = AccountBook(store, deferred=True, preparation_workers=2).install()
    try:
        def submit(_position_key):
            future = Future()
            if _position_key == "poison":
                future.set_exception(BrokenProcessPool("worker died"))
            else:
                future.set_result(None)
            return future

        monkeypatch.setattr(book, "_submit_preparation", submit)
        pending = [
            {"position_key": "poison"},
            {"position_key": "queued"},
            {"position_key": "fine"},
        ]
        # Out-of-order reap: results that already completed are kept; only the
        # broken position and still-waiting work defer when the pool dies.
        assert list(book._prepared_pending(pending)) == [None, None, None]
        assert book._deferred_position_keys() == {"poison"}
    finally:
        store.close()


def test_pending_selection_skips_deferred_positions():
    store = MarketStore(":memory:")
    book = AccountBook(store, deferred=True).install()
    try:
        with store.transaction() as connection:
            for index, key in enumerate(("defer-me", "fresh-a", "fresh-b")):
                connection.execute(
                    "INSERT INTO lp_accounting_pending("
                    "position_key,generation,requested_revision,requested_epoch,"
                    "priority_block,priority_tx_index,priority_log_index"
                    ") VALUES(?,?,0,0,?,0,0)",
                    (key, index, 100 - index),
                )
        book._defer_position("defer-me")
        rows = book._pending_rows(8)
        assert [row["position_key"] for row in rows] == ["fresh-a", "fresh-b"]
    finally:
        store.close()


def test_pending_selection_spends_the_pass_budget_on_cheap_replays_first():
    store = MarketStore(":memory:")
    book = AccountBook(store, deferred=True).install()
    try:
        with store.transaction() as connection:
            for key, priority in (
                ("cheap-g", 10), ("cheap-h", 9),
                ("heavy-0", 1005), ("heavy-1", 1004), ("heavy-2", 1003),
                ("heavy-3", 1002), ("heavy-4", 1001), ("heavy-5", 1000),
                ("cheap-a", 500), ("cheap-b", 499), ("cheap-c", 498),
                ("cheap-d", 497), ("cheap-e", 496), ("cheap-f", 495),
            ):
                connection.execute(
                    "INSERT INTO lp_accounting_pending("
                    "position_key,generation,requested_revision,requested_epoch,"
                    "priority_block,priority_tx_index,priority_log_index"
                    ") VALUES(?,0,0,0,?,0,0)",
                    (key, priority),
                )
            connection.executemany(
                "INSERT INTO lp_accounting_event_keys("
                "event_id,position_key,token_id) VALUES(?,?,NULL)",
                (
                    (event_id, f"heavy-{index}")
                    for index in range(6)
                    for event_id in range(
                        index * 10_005 + 1, index * 10_005 + 10_002,
                    )
                ),
            )
        keys = [row["position_key"] for row in book._pending_rows(8)]
        heavies = [key for key in keys if key.startswith("heavy")]
        assert heavies == ["heavy-0"]
        assert keys[-1] == "heavy-0"
        assert all(key in keys for key in ("cheap-a", "cheap-g"))
        again = [row["position_key"] for row in book._pending_rows(8)]
        assert not [key for key in again if key.startswith("heavy")]
    finally:
        store.close()


def test_append_only_rows_cost_their_unprojected_tail():
    store = MarketStore(":memory:")
    book = AccountBook(store, deferred=True).install()
    try:
        with store.transaction() as connection:
            connection.execute(
                "INSERT INTO lp_accounting_pending("
                "position_key,generation,append_only,requested_revision,"
                "requested_epoch,priority_block,priority_tx_index,"
                "priority_log_index) VALUES('tail',0,1,0,0,1,0,0)",
            )
            connection.executemany(
                "INSERT INTO lp_accounting_event_keys("
                "event_id,position_key,token_id) VALUES(?,'tail',NULL)",
                ((event_id,) for event_id in range(1, 3_001)),
            )
            connection.executemany(
                "INSERT INTO lp_accounting_effects(event_id,position_key,"
                "tx_hash,block_number,tx_index,log_index,timestamp,kind,"
                "exact,basis) VALUES(?,'tail','0xab',1,0,0,1,'add',1,'exact')",
                ((event_id,) for event_id in range(1, 2_998)),
            )
        rows = book._pending_rows(8)
        assert [row["position_key"] for row in rows] == ["tail"]
        assert rows[0]["replay_events"] == 3
    finally:
        store.close()


def test_expensive_replays_cool_down_after_preparation():
    store = MarketStore(":memory:")
    book = AccountBook(store, deferred=True).install()
    try:
        with store.transaction() as connection:
            for key, priority in (("whale", 2), ("minnow", 1)):
                connection.execute(
                    "INSERT INTO lp_accounting_pending("
                    "position_key,generation,requested_revision,requested_epoch,"
                    "priority_block,priority_tx_index,priority_log_index"
                    ") VALUES(?,0,0,0,?,0,0)",
                    (key, priority),
                )
            connection.executemany(
                "INSERT INTO lp_accounting_event_keys("
                "event_id,position_key,token_id) VALUES(?,'whale',NULL)",
                ((event_id,) for event_id in range(1, 2_001)),
            )
        rows = book._pending_rows(8)
        assert [row["position_key"] for row in rows] == ["minnow", "whale"]
        book._prepare_pending = lambda key, scale=1.0: object()
        list(book._prepared_pending(rows))
        assert book._cooled_position_keys() == {"whale"}
        assert [row["position_key"] for row in book._pending_rows(8)] == [
            "minnow",
        ]
        book.prioritize_positions(["whale"])
        assert "whale" in [row["position_key"] for row in book._pending_rows(8)]
    finally:
        store.close()


def test_snapshot_budget_grows_with_event_count_and_failures():
    store = MarketStore(":memory:")
    book = AccountBook(store, deferred=True).install()
    try:
        with store.transaction() as connection:
            connection.executemany(
                "INSERT INTO lp_accounting_event_keys("
                "event_id,position_key,token_id) VALUES(?,?,NULL)",
                ((event_id, "big") for event_id in range(1, 200_001)),
            )
        assert book._position_event_count("big") == 200_000
        assert book._position_snapshot_seconds("big", 1.0) == 100.0
        assert book._position_snapshot_seconds("big", 5.0) == 300.0
        assert book._position_snapshot_seconds("missing", 3.0) is None
    finally:
        store.close()
