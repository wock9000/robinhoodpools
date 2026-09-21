"""Focused durable-store batching and canonical-safety regressions."""
from __future__ import annotations
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

import rhpools.lp_market_store as market_store_module
from rhpools.lp_market_store import (
    CanonicalConflict,
    MarketStore,
    MarketStoreError,
)
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


def test_live_writer_admission_precedes_queued_background():
    store = MarketStore(":memory:")
    holder_entered = threading.Event()
    release_holder = threading.Event()
    order = []
    failures = []

    def writer(priority, label, hold=False):
        try:
            with store.writer_priority(priority):
                with store.transaction():
                    if hold:
                        holder_entered.set()
                        release_holder.wait(2)
                    else:
                        order.append(label)
        except BaseException as exc:
            failures.append(exc)

    holder = threading.Thread(
        target=writer, args=("background", "holder", True),
    )
    background = threading.Thread(
        target=writer, args=("background", "background"),
    )
    live = threading.Thread(target=writer, args=("live", "live"))
    try:
        holder.start()
        assert holder_entered.wait(1)
        background.start()
        with store._writer_condition:
            assert store._writer_condition.wait_for(
                lambda: len(store._writer_waiters) == 1,
                timeout=1,
            )
        store.set_writer_pressure("live")
        live.start()
        with store._writer_condition:
            assert store._writer_condition.wait_for(
                lambda: len(store._writer_waiters) == 2,
                timeout=1,
            )
        release_holder.set()
        holder.join(2)
        live.join(2)
        assert order == ["live"]
        assert background.is_alive()

        store.set_writer_pressure(None)
        background.join(2)
        assert failures == []
        assert order == ["live", "background"]
    finally:
        release_holder.set()
        store.set_writer_pressure(None)
        for thread in (holder, background, live):
            if thread.ident is not None:
                thread.join(2)
        store.close()


def test_canonical_ingest_commits_before_background_search_catalog():
    store = MarketStore(":memory:")
    block = header(10)
    record = event(block, 0)
    try:
        store.ensure_search_index()
        inserted = store.ingest(
            [block],
            [record],
            cursor={
                "from_block": 10,
                "to_block": 10,
                "block_number": 10,
                "block_hash": block["hash"],
            },
        )

        assert len(inserted) == 1
        assert store.cursor("live")["block_number"] == 10
        assert store.read().execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert store.search(record["tx_hash"]) == ([], 0)
        assert store.search_index_status() == {
            "state": "warming",
            "phase": "catching_up",
            "ready": False,
            "indexed_through_event": 0,
            "events_total": 1,
        }

        store.build_search_index(threading.Event(), batch_size=1)

        assert store.search_index_status() == {
            "state": "ready",
            "phase": "ready",
            "ready": True,
            "indexed_through_event": 1,
            "events_total": 1,
        }
        assert [row["id"] for row in store.search(record["tx_hash"])[0]] == [
            record["tx_hash"],
        ]
    finally:
        store.close()


def test_legacy_search_index_baselines_durable_tail_cursor(tmp_path):
    path = tmp_path / "market.sqlite"
    store = MarketStore(path)
    block = header(10)
    record = event(block, 0)
    try:
        store.ingest([block], [record])
        store.build_search_index(threading.Event())
    finally:
        store.close()

    legacy = sqlite3.connect(path)
    try:
        legacy.execute("DELETE FROM metadata WHERE key='search_index_cursor'")
        legacy.execute("DELETE FROM metadata WHERE key='search_index_state'")
        legacy.execute("PRAGMA user_version=15")
        legacy.commit()
    finally:
        legacy.close()

    store = MarketStore(path)
    try:
        assert store.connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert store.search_index_status() == {
            "state": "ready",
            "phase": "ready",
            "ready": True,
            "indexed_through_event": 1,
            "events_total": 1,
        }
        assert [row["id"] for row in store.search(record["tx_hash"])[0]] == [
            record["tx_hash"],
        ]
    finally:
        store.close()


def test_checkpoint_recovers_after_external_reader_releases_snapshot(tmp_path):
    path = tmp_path / "market.sqlite"
    store = MarketStore(path, checkpoint_on_commit=False)
    snapshot = None
    try:
        cleared = store.checkpoint("TRUNCATE")
        assert set(cleared) == {
            "busy", "log_frames", "checkpointed_frames", "log_bytes",
            "backlog_bytes", "wal_bytes", "active_reader_snapshots",
            "reader_drain_pending",
        }
        assert cleared["busy"] == 0

        snapshot = sqlite3.connect(path, isolation_level=None)
        snapshot.execute("PRAGMA query_only=ON")
        snapshot.execute("BEGIN")
        assert snapshot.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0

        block = header(10)
        records = [event(block, index) for index in range(3)]
        assert len(store.ingest([block], records)) == len(records)

        progress = store.checkpoint()
        assert progress["busy"] == 0
        assert progress["log_frames"] > progress["checkpointed_frames"]
        assert progress["backlog_bytes"] > 0
        assert progress["log_bytes"] >= progress["backlog_bytes"]
        assert progress["wal_bytes"] >= progress["backlog_bytes"]

        blocked_reset = store.checkpoint("TRUNCATE")
        assert blocked_reset["busy"] == 1
        assert blocked_reset["backlog_bytes"] > 0

        snapshot.rollback()
        snapshot.close()
        snapshot = None

        recovered = store.checkpoint("TRUNCATE")
        assert recovered["busy"] == 0
        assert recovered["log_frames"] == recovered["checkpointed_frames"]
        assert recovered["backlog_bytes"] == 0
        assert recovered["wal_bytes"] == 0
        assert [
            (row["block_number"], row["log_index"])
            for row in store.read().execute(
                "SELECT block_number,log_index FROM events ORDER BY log_index"
            ).fetchall()
        ] == [(10, 0), (10, 1), (10, 2)]

        store.close()
        store = MarketStore(path, checkpoint_on_commit=False)
        assert store.read().execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0] == len(records)
    finally:
        if snapshot is not None:
            snapshot.rollback()
            snapshot.close()
        store.close()


def test_checkpoint_handles_non_wal_memory_store_and_rejects_full_mode():
    store = MarketStore(":memory:")
    try:
        assert store.checkpoint() == {
            "busy": 0,
            "log_frames": 0,
            "log_bytes": 0,
            "checkpointed_frames": 0,
            "backlog_bytes": 0,
            "wal_bytes": 0,
            "active_reader_snapshots": 0,
            "reader_drain_pending": 0,
        }
        with store.reader_snapshot():
            managed = store.checkpoint("TRUNCATE", drain_readers=True)
            assert managed["active_reader_snapshots"] == 1
            assert managed["reader_drain_pending"] == 0
        with pytest.raises(ValueError):
            store.checkpoint("FULL")
    finally:
        store.close()


def test_restart_checkpoint_does_not_wait_for_active_writer(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    writer_entered = threading.Event()
    release_writer = threading.Event()
    checkpoint_done = threading.Event()
    checkpoint_results = []
    failures = []

    store.checkpoint("TRUNCATE")
    with store.transaction() as connection:
        connection.execute(
            "INSERT INTO metadata(key,value) VALUES('checkpoint-seed','1')"
        )

    def write():
        try:
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE metadata SET value='2' WHERE key='checkpoint-seed'"
                )
                writer_entered.set()
                release_writer.wait(2)
        except BaseException as exc:
            failures.append(exc)

    def checkpoint():
        try:
            checkpoint_results.append(
                store.checkpoint("RESTART")
            )
        except BaseException as exc:
            failures.append(exc)
        finally:
            checkpoint_done.set()

    writer = threading.Thread(target=write)
    checkpointer = threading.Thread(target=checkpoint)
    try:
        writer.start()
        assert writer_entered.wait(1)
        checkpointer.start()
        assert checkpoint_done.wait(
            0.5
        ), "RESTART checkpoint waited behind the active writer"
        assert failures == []
        assert checkpoint_results[0]["busy"] == 1
        assert checkpoint_results[0]["reader_drain_pending"] == 0
        release_writer.set()
        writer.join(2)
        assert failures == []
        assert store.checkpoint("RESTART", drain_readers=True)["busy"] == 0
        with store.reader_snapshot() as connection:
            assert connection.execute(
                "SELECT value FROM metadata WHERE key='checkpoint-seed'"
            ).fetchone()[0] == "2"
        with store.transaction() as connection:
            connection.execute(
                "UPDATE metadata SET value='3' WHERE key='checkpoint-seed'"
            )
            nested = store.checkpoint("RESTART", drain_readers=True)
            assert nested["busy"] == 1
            assert nested["log_frames"] == -1
    finally:
        release_writer.set()
        writer.join(2)
        checkpointer.join(6)
        store.close()
    assert failures == []


@pytest.mark.parametrize("live_pressure", [False, True])
def test_managed_reset_queues_writer_without_blocking_passive_checkpoint(
        tmp_path, live_pressure):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    writer_entered = threading.Event()
    release_writer = threading.Event()
    checkpoint_done = threading.Event()
    results, failures = [], []
    with store.transaction() as connection:
        connection.execute("INSERT INTO metadata(key,value) VALUES('seed','1')")

    def write():
        try:
            with store.transaction() as connection:
                connection.execute("UPDATE metadata SET value='2' WHERE key='seed'")
                writer_entered.set()
                release_writer.wait(3)
        except BaseException as exc:
            failures.append(exc)

    def reset():
        try:
            results.append(store.checkpoint("RESTART", drain_readers=True))
        except BaseException as exc:
            failures.append(exc)
        finally:
            checkpoint_done.set()

    writer = threading.Thread(target=write)
    checkpointer = threading.Thread(target=reset)
    try:
        writer.start()
        assert writer_entered.wait(1)
        if live_pressure:
            store.set_writer_pressure("live")
        checkpointer.start()
        deadline = time.monotonic() + 1
        while True:
            passive = store.checkpoint("PASSIVE")
            if passive["reader_drain_pending"] or time.monotonic() >= deadline:
                break
            checkpoint_done.wait(0.001)
        assert passive["busy"] == 0
        assert passive["reader_drain_pending"] == 1
        assert not checkpoint_done.wait(0.1), "managed reset did not queue a writer turn"
        release_writer.set()
        assert checkpoint_done.wait(1)
        assert failures == []
        assert results[0]["busy"] == 0
        assert results[0]["reader_drain_pending"] == 0
        with store.reader_snapshot() as connection:
            assert connection.execute(
                "SELECT value FROM metadata WHERE key='seed'"
            ).fetchone()[0] == "2"
    finally:
        store.set_writer_pressure(None)
        release_writer.set()
        writer.join(3)
        checkpointer.join(3)
        store.close()
    assert failures == []


def test_managed_reset_writer_deadline_reopens_snapshot_admission(tmp_path, monkeypatch):
    monkeypatch.setattr(market_store_module, "_READER_DRAIN_SECONDS", 0.05)
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    writer_entered = threading.Event()
    release_writer = threading.Event()
    failures = []
    with store.transaction() as connection:
        connection.execute("INSERT INTO metadata(key,value) VALUES('seed','1')")

    def write():
        try:
            with store.transaction() as connection:
                connection.execute("UPDATE metadata SET value='2' WHERE key='seed'")
                writer_entered.set()
                release_writer.wait(3)
        except BaseException as exc:
            failures.append(exc)

    writer = threading.Thread(target=write)
    try:
        writer.start()
        assert writer_entered.wait(1)
        result = store.checkpoint("RESTART", drain_readers=True)
        assert result["busy"] == 1
        assert result["reader_drain_pending"] == 0
        assert writer.is_alive()
        with store.reader_snapshot() as connection:
            assert connection.execute(
                "SELECT value FROM metadata WHERE key='seed'"
            ).fetchone()[0] == "1"
    finally:
        release_writer.set()
        writer.join(3)
        store.close()
    assert failures == []


def test_restart_checkpoint_excludes_concurrent_application_writer(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        market_store_module, "_CHECKPOINT_BUSY_TIMEOUT_MS", 1_000,
    )
    path = tmp_path / "market.sqlite"
    store = MarketStore(path, checkpoint_on_commit=False)
    external = None
    checkpoint_done = threading.Event()
    writer_done = threading.Event()
    checkpoint_results = []
    failures = []

    def checkpoint():
        try:
            checkpoint_results.append(
                store.checkpoint("RESTART", drain_readers=True)
            )
        except BaseException as exc:
            failures.append(exc)
        finally:
            checkpoint_done.set()

    def write():
        try:
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE metadata SET value='2' "
                    "WHERE key='checkpoint-seed'"
                )
        except BaseException as exc:
            failures.append(exc)
        finally:
            writer_done.set()

    checkpointer = threading.Thread(target=checkpoint)
    writer = threading.Thread(target=write)
    try:
        store.checkpoint("TRUNCATE")
        external = sqlite3.connect(path, isolation_level=None)
        external.execute("PRAGMA query_only=ON")
        external.execute("BEGIN")
        external.execute("SELECT COUNT(*) FROM events").fetchone()
        with store.transaction() as connection:
            connection.execute(
                "INSERT INTO metadata(key,value) "
                "VALUES('checkpoint-seed','1')"
            )

        checkpointer.start()
        with store._writer_condition:
            assert store._writer_condition.wait_for(
                lambda: store._writer_active,
                timeout=1,
            )
        writer.start()
        assert not writer_done.wait(
            0.1
        ), "writer entered while RESTART owned writer exclusion"

        external.rollback()
        external.close()
        external = None
        assert checkpoint_done.wait(2)
        assert writer_done.wait(2)
        assert failures == []
        assert checkpoint_results[0]["busy"] == 0
        assert checkpoint_results[0]["backlog_bytes"] == 0
        assert store.read().execute(
            "SELECT value FROM metadata WHERE key='checkpoint-seed'"
        ).fetchone()[0] == "2"
    finally:
        if external is not None:
            external.rollback()
            external.close()
        if checkpointer.ident is not None:
            checkpointer.join(2)
        if writer.ident is not None:
            writer.join(2)
        store.close()


def test_managed_reader_drain_preserves_snapshot_and_reclaims_wal(tmp_path):
    store = MarketStore(
        tmp_path / "market.sqlite",
        checkpoint_on_commit=False,
    )
    existing_started = threading.Event()
    release_existing = threading.Event()
    existing_finished = threading.Event()
    queued_started = threading.Event()
    queued_admitted = threading.Event()
    existing_counts = []
    queued_counts = []
    failures = []

    def hold_existing_snapshot():
        try:
            with store.reader_snapshot() as connection:
                before = connection.execute(
                    "SELECT COUNT(*) FROM events"
                ).fetchone()[0]
                existing_started.set()
                if not release_existing.wait(3):
                    raise AssertionError("existing snapshot was not released")
                after = connection.execute(
                    "SELECT COUNT(*) FROM events"
                ).fetchone()[0]
                existing_counts.append((before, after))
        except BaseException as exc:
            failures.append(exc)
        finally:
            store.close_reader()
            existing_finished.set()

    def read_after_reset():
        try:
            store.read()
            queued_started.set()
            with store.reader_snapshot() as connection:
                queued_counts.append(
                    connection.execute(
                        "SELECT COUNT(*) FROM events"
                    ).fetchone()[0]
                )
                queued_admitted.set()
        except BaseException as exc:
            failures.append(exc)
        finally:
            store.close_reader()

    existing = threading.Thread(target=hold_existing_snapshot)
    queued = threading.Thread(target=read_after_reset)
    try:
        assert store.checkpoint("TRUNCATE")["busy"] == 0
        existing.start()
        assert existing_started.wait(1)

        first = header(10)
        first_events = [event(first, index) for index in range(3)]
        assert len(store.ingest([first], first_events)) == 3

        deferred = store.checkpoint("TRUNCATE", drain_readers=True)
        assert deferred["busy"] == 1
        assert deferred["active_reader_snapshots"] == 1
        assert deferred["reader_drain_pending"] == 1
        assert deferred["log_frames"] == -1

        queued.start()
        assert queued_started.wait(1)
        assert not queued_admitted.wait(0.1)

        second = header(11)
        assert len(store.ingest([second], [event(second, 0)])) == 1
        passive = store.checkpoint()
        assert passive["busy"] == 0
        assert passive["active_reader_snapshots"] == 1
        assert passive["reader_drain_pending"] == 1
        assert passive["log_frames"] > passive["checkpointed_frames"]

        release_existing.set()
        assert existing_finished.wait(1)
        assert existing_counts == [(0, 0)]

        recovered = store.checkpoint("TRUNCATE", drain_readers=True)
        assert recovered["busy"] == 0
        assert recovered["reader_drain_pending"] == 0
        assert recovered["backlog_bytes"] == 0
        assert recovered["wal_bytes"] == 0
        assert queued_admitted.wait(1)
        queued.join(1)
        assert queued_counts == [4]
        assert failures == []
    finally:
        release_existing.set()
        existing.join(3)
        queued.join(3)
        store.close()


def test_checkpoint_drain_finishes_running_query_before_reset(tmp_path, monkeypatch):
    monkeypatch.setattr(market_store_module, "_READER_PROGRESS_STEPS", 1)
    store = MarketStore(
        tmp_path / "market.sqlite",
        checkpoint_on_commit=False,
    )
    entered = threading.Event()
    release = threading.Event()
    queued_started = threading.Event()
    queued_admitted = threading.Event()
    query_values = []
    queued_counts = []
    failures = []

    def run_query():
        def hold(value):
            entered.set()
            if not release.wait(2):
                raise AssertionError("blocked read was not released")
            return value

        try:
            with store.reader_snapshot() as connection:
                connection.create_function("hold_read", 1, hold)
                query_values.extend(
                    row[0] for row in connection.execute(
                        "SELECT hold_read(value) FROM frame_probe"
                    ).fetchall()
                )
        except BaseException as exc:
            failures.append(exc)
        finally:
            store.close_reader()

    def read_after_reset():
        try:
            store.read()
            queued_started.set()
            with store.reader_snapshot() as connection:
                queued_counts.append(
                    connection.execute(
                        "SELECT COUNT(*) FROM frame_probe"
                    ).fetchone()[0]
                )
                queued_admitted.set()
        except BaseException as exc:
            failures.append(exc)
        finally:
            store.close_reader()

    reader = threading.Thread(target=run_query)
    queued = threading.Thread(target=read_after_reset)
    try:
        with store.transaction() as connection:
            connection.execute("CREATE TABLE frame_probe(value INTEGER)")
            connection.execute("INSERT INTO frame_probe VALUES(1)")
        store.checkpoint("TRUNCATE")

        reader.start()
        assert entered.wait(1)
        with store.transaction() as connection:
            connection.execute("INSERT INTO frame_probe VALUES(2)")

        deferred = store.checkpoint("RESTART", drain_readers=True)
        assert deferred["active_reader_snapshots"] == 1
        assert deferred["reader_drain_pending"] == 1

        queued.start()
        assert queued_started.wait(1)
        assert not queued_admitted.wait(0.1)

        release.set()
        reader.join(2)
        assert query_values == [1]
        assert failures == []

        recovered = store.checkpoint("RESTART", drain_readers=True)
        assert recovered["busy"] == 0
        assert recovered["active_reader_snapshots"] == 0
        assert recovered["reader_drain_pending"] == 0
        assert recovered["backlog_bytes"] == 0
        assert queued_admitted.wait(1)
        queued.join(2)
        assert queued_counts == [2]
        assert failures == []
    finally:
        release.set()
        if reader.ident is not None:
            reader.join(2)
        if queued.ident is not None:
            queued.join(2)
        store.close()


def test_checkpoint_drain_interrupts_only_snapshot_outliving_grace(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(market_store_module, "_READER_PROGRESS_STEPS", 1)
    monkeypatch.setattr(market_store_module, "_READER_DRAIN_SECONDS", 0.05)
    monkeypatch.setattr(market_store_module, "_READER_DRAIN_GRACE_SECONDS", 0.05)
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    entered = threading.Event()
    release = threading.Event()
    queued_admitted = threading.Event()
    query_failures = []
    queued_failures = []
    queued_counts = []

    def run_query():
        def hold(value):
            if not entered.is_set():
                entered.set()
                if not release.wait(2):
                    raise AssertionError("blocked read was not released")
            return value

        try:
            with store.reader_snapshot(5) as connection:
                connection.create_function("hold_read", 1, hold)
                connection.execute(
                    "SELECT hold_read(value) FROM frame_probe"
                ).fetchall()
        except BaseException as exc:
            query_failures.append(exc)
        finally:
            store.close_reader()

    def read_after_grace():
        try:
            with store.reader_snapshot() as connection:
                queued_counts.append(
                    connection.execute(
                        "SELECT COUNT(*) FROM frame_probe"
                    ).fetchone()[0]
                )
                queued_admitted.set()
        except BaseException as exc:
            queued_failures.append(exc)
        finally:
            store.close_reader()

    reader = threading.Thread(target=run_query)
    queued = threading.Thread(target=read_after_grace)
    try:
        with store.transaction() as connection:
            connection.execute("CREATE TABLE frame_probe(value INTEGER)")
            connection.executemany(
                "INSERT INTO frame_probe VALUES(?)",
                ((value,) for value in range(100)),
            )
        store.checkpoint("TRUNCATE")

        reader.start()
        assert entered.wait(1)
        with store.transaction() as connection:
            connection.execute("INSERT INTO frame_probe VALUES(100)")
        deferred = store.checkpoint("TRUNCATE", drain_readers=True)
        assert deferred["reader_drain_pending"] == 1

        queued.start()
        assert queued_admitted.wait(1)
        release.set()
        reader.join(2)

        assert queued_counts == [101]
        assert queued_failures == []
        assert len(query_failures) == 1
        assert isinstance(query_failures[0], sqlite3.OperationalError)
        assert query_failures[0].sqlite_errorcode == sqlite3.SQLITE_INTERRUPT
        recovered = store.checkpoint("TRUNCATE", drain_readers=True)
        assert recovered["busy"] == 0
        assert recovered["reader_drain_pending"] == 0
    finally:
        release.set()
        if reader.ident is not None:
            reader.join(2)
        if queued.ident is not None:
            queued.join(2)
        store.close()


def test_reader_snapshot_nesting_borrows_transactions_without_ending_them(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    try:
        with store.reader_snapshot() as outer:
            outer.execute("SELECT COUNT(*) FROM events").fetchone()
            with store.reader_snapshot() as nested:
                assert nested is outer
                assert nested.in_transaction
                metrics = store.checkpoint()
                assert metrics["active_reader_snapshots"] == 1
            assert outer.in_transaction
        assert not outer.in_transaction

        borrowed = store.read()
        borrowed.execute("BEGIN")
        with store.reader_snapshot() as connection:
            assert connection is borrowed
            connection.execute("SELECT COUNT(*) FROM events").fetchone()
        assert borrowed.in_transaction
        assert store.checkpoint()["active_reader_snapshots"] == 0
        borrowed.rollback()
    finally:
        store.close()


def test_cancel_checkpoint_drain_admits_waiting_snapshot(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    queued_started = threading.Event()
    queued_admitted = threading.Event()
    failures = []

    def queued_reader():
        try:
            store.read()
            queued_started.set()
            with store.reader_snapshot() as connection:
                connection.execute("SELECT COUNT(*) FROM events").fetchone()
                queued_admitted.set()
        except BaseException as exc:
            failures.append(exc)
        finally:
            store.close_reader()

    queued = threading.Thread(target=queued_reader)
    try:
        with store.reader_snapshot() as active:
            active.execute("SELECT COUNT(*) FROM events").fetchone()
            deferred = store.checkpoint("TRUNCATE", drain_readers=True)
            assert deferred["reader_drain_pending"] == 1
            queued.start()
            assert queued_started.wait(1)
            assert not queued_admitted.wait(0.1)
            store.cancel_checkpoint_drain()
            assert queued_admitted.wait(1)
        queued.join(1)
        assert store.checkpoint()["reader_drain_pending"] == 0
        assert failures == []
    finally:
        store.cancel_checkpoint_drain()
        queued.join(3)
        store.close()


def test_waiting_snapshot_expires_abandoned_reader_drain(tmp_path, monkeypatch):
    monkeypatch.setattr(market_store_module, "_READER_DRAIN_SECONDS", 0.1)
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    queued_started = threading.Event()
    queued_admitted = threading.Event()
    release_queued = threading.Event()
    failures = []

    def queued_reader():
        try:
            store.read()
            queued_started.set()
            with store.reader_snapshot() as connection:
                connection.execute("SELECT COUNT(*) FROM events").fetchone()
                queued_admitted.set()
                if not release_queued.wait(2):
                    raise AssertionError("queued snapshot was not released")
        except BaseException as exc:
            failures.append(exc)
        finally:
            store.close_reader()

    queued = threading.Thread(target=queued_reader)
    try:
        with store.reader_snapshot() as active:
            active.execute("SELECT COUNT(*) FROM events").fetchone()
            deferred = store.checkpoint("TRUNCATE", drain_readers=True)
            assert deferred["reader_drain_pending"] == 1
            queued.start()
            assert queued_started.wait(1)
            assert queued_admitted.wait(1)
            metrics = store.checkpoint()
            assert metrics["reader_drain_pending"] == 0
            retry = store.checkpoint("TRUNCATE", drain_readers=True)
            assert retry["reader_drain_pending"] == 0
            assert metrics["active_reader_snapshots"] == 2
        release_queued.set()
        queued.join(1)
        assert failures == []
    finally:
        release_queued.set()
        queued.join(3)
        store.close()


def test_close_wakes_snapshot_waiting_for_reader_drain(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    active_started = threading.Event()
    release_active = threading.Event()
    queued_started = threading.Event()
    queued_done = threading.Event()
    active_failures = []
    queued_failures = []

    def active_reader():
        try:
            with store.reader_snapshot() as connection:
                connection.execute("SELECT COUNT(*) FROM events").fetchone()
                active_started.set()
                if not release_active.wait(3):
                    raise AssertionError("active snapshot was not released")
        except BaseException as exc:
            active_failures.append(exc)
        finally:
            store.close_reader()

    def queued_reader():
        try:
            store.read()
            queued_started.set()
            with store.reader_snapshot():
                raise AssertionError("closed store admitted a queued snapshot")
        except BaseException as exc:
            queued_failures.append(exc)
        finally:
            store.close_reader()
            queued_done.set()

    active = threading.Thread(target=active_reader)
    queued = threading.Thread(target=queued_reader)
    try:
        active.start()
        assert active_started.wait(1)
        deferred = store.checkpoint("TRUNCATE", drain_readers=True)
        assert deferred["reader_drain_pending"] == 1
        queued.start()
        assert queued_started.wait(1)
        assert not queued_done.wait(0.1)

        store.close()
        assert queued_done.wait(1)
        assert len(queued_failures) == 1
        assert isinstance(queued_failures[0], MarketStoreError)
    finally:
        release_active.set()
        active.join(3)
        queued.join(3)
        store.close()
    assert active_failures == []


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
        store.build_search_index(threading.Event())
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


def test_writer_owned_work_does_not_wait_behind_reader_drain(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite", checkpoint_on_commit=False)
    finished = threading.Event()
    failures = []
    with store.transaction() as connection:
        connection.execute("CREATE TABLE nested_reader_probe(value INTEGER)")
        connection.execute("INSERT INTO nested_reader_probe VALUES(1)")
    reader = store.read()
    reader.execute("BEGIN")
    assert reader.execute("SELECT value FROM nested_reader_probe").fetchone()[0] == 1

    def write():
        try:
            with store.transaction() as connection:
                connection.execute("UPDATE nested_reader_probe SET value=2")
                with store.reader_snapshot() as snapshot:
                    assert snapshot.execute(
                        "SELECT value FROM nested_reader_probe"
                    ).fetchone()[0] == 1
        except BaseException as exc:
            failures.append(exc)
        finally:
            finished.set()

    writer = threading.Thread(target=write)
    try:
        assert store.checkpoint("TRUNCATE", drain_readers=True)["busy"] == 1
        writer.start()
        assert finished.wait(0.5), "reader admission stranded an active writer"
        assert failures == []
        assert reader.execute(
            "SELECT value FROM nested_reader_probe"
        ).fetchone()[0] == 1
        reader.rollback()
        assert reader.execute(
            "SELECT value FROM nested_reader_probe"
        ).fetchone()[0] == 2
    finally:
        store.cancel_checkpoint_drain()
        reader.rollback()
        if writer.ident is not None:
            writer.join(2)
        store.close()


def test_unprojected_ingest_rolls_activity_into_buckets_exactly_once(tmp_path):
    from rhpools.lp_market_service import BUCKET_FIELDS, LPMarketService, PriceProjection
    from rhpools.workbench_market import USDG

    app = LPMarketService(None, "http://127.0.0.1:1", tmp_path / "market.sqlite", start=False)
    pool = "0x" + "23" * 20
    try:
        app.store.upsert_pools([{
            "id": pool, "address": pool, "protocol": "v3",
            "token0": "0x" + "12" * 20, "token1": USDG,
            "symbol0": "ASSET", "symbol1": "USDG", "decimals0": 6, "decimals1": 6,
            "tick_spacing": 1, "hook": None,
            "factory": "0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
            "created_block": 1, "source": "factory-live",
        }])
        # Two archive intervals share the minute at 1_700_000_100..159:
        # block 118 (swap) lands in the first, block 100..117 in the second.
        blocks = {number: header(number) for number in (100, 105, 118)}

        def archive_event(number, kind, index):
            row = event(blocks[number], index)
            row.update({
                "pool_id": pool, "kind": kind, "position_key": None,
                "owner": None, "custody": None, "token_id": None,
                "tx_hash": "0x" + f"{number * 10 + index:064x}",
                "sqrt_price_x96": str(1 << 96), "liquidity": "1000",
                "amount0": "-100", "amount1": "101",
            })
            return row

        newer = [archive_event(118, "swap", 0)]
        older = [
            archive_event(100, "swap", 0), archive_event(100, "add", 1),
            archive_event(105, "remove", 0), archive_event(105, "swap", 1),
        ]
        app.store.ingest([blocks[118]], newer, lane="history", project=False)
        app.store.ingest([blocks[100], blocks[105]], older, lane="history", project=False)
        app.store.ingest([blocks[100], blocks[105]], older, lane="history", project=False)
        columns = ",".join(BUCKET_FIELDS)
        counted = ("events", "swaps", "adds", "removes", "flows")

        def buckets():
            return [
                dict(row) for row in app.store.read().execute(
                    f"SELECT resolution,bucket,pool_id,{columns},max_block "
                    "FROM lp_pool_buckets ORDER BY resolution,bucket",
                ).fetchall()
            ]

        rolled = buckets()
        minute = 1_700_000_100 // 60 * 60
        assert [
            (row["resolution"], row["bucket"], *(row[name] for name in counted), row["max_block"])
            for row in rolled
        ] == [
            (60, minute, 5, 3, 1, 1, 2, 118),
            (3600, minute // 3600 * 3600, 5, 3, 1, 1, 2, 118),
            (86400, minute // 86400 * 86400, 5, 3, 1, 1, 2, 118),
        ]
        # The per-event recompute of the same minute finds nothing to change.
        with app.store.transaction() as connection:
            PriceProjection._bucket(connection, pool, minute)
        assert buckets() == rolled
        # Pricing later adds only USD to the rolled counts.
        priced = app.store.pending_reprojections(1)
        app.store.reproject([int(priced[0]["id"])])
        after = buckets()
        assert [
            [row[name] for name in counted] for row in after
        ] == [
            [row[name] for name in counted] for row in rolled
        ]
        assert after[0]["volume_usd"] > 0
    finally:
        app.close()


def test_background_search_preserves_identity_and_block_queries():
    store = MarketStore(":memory:")
    try:
        block = header(4_500_000)
        archive = event(block, 0) | {"tx_hash": "0x" + "cd" * 32}
        live = event(header(4_500_001), 0)
        store.ingest([block], [archive], lane="history", project=False)
        store.ingest([header(4_500_001)], [live], lane="live")
        store.build_search_index(threading.Event())
        assert [row["id"] for row in store.search("0x" + "cd" * 32)[0]] == ["0x" + "cd" * 32]
        assert [row["id"] for row in store.search("0x" + "11" * 20)[0]] == ["0x" + "11" * 20]
        assert [row["id"] for row in store.search("7")[0]] == ["shared-position"]
        assert [row["id"] for row in store.search("4500001")[0]] == [
            live["tx_hash"],
        ]
    finally:
        store.close()
