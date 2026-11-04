"""The owner-activity rollup answers exactly what owner_candidates scans for."""
from __future__ import annotations

import itertools
import random
import time

import pytest

from rhpools.lp_market_accounting import AccountBook
from rhpools.lp_market_store import MarketStore

OWNERS = ["0x" + f"{index:040x}" for index in range(1, 7)]
CUSTODIES = ["0x" + f"{index:040x}" for index in range(101, 104)]
POSITIONS = [f"v3:pool:{index}" for index in range(1, 9)]
DAY = 86_400


def _insert_episode(connection, **overrides):
    fields = {
        row[1]: 0 if row[2] in ("INTEGER", "REAL") else "0"
        for row in connection.execute("PRAGMA table_info(lp_accounting_episodes)")
        if row[3]
    }
    fields.update(accounting_basis="test", qualifiers="[]", status="complete")
    fields.update(overrides)
    columns = list(fields)
    connection.execute(
        "INSERT INTO lp_accounting_episodes(" + ",".join(columns) + ") VALUES("
        + ",".join("?" for _ in columns) + ")",
        [fields[column] for column in columns],
    )


def _seed(store, seed=7):
    rng = random.Random(seed)
    now = int(time.time())
    ages = (600, DAY // 2, 3 * DAY, 12 * DAY, 45 * DAY)
    with store.transaction() as connection:
        counter = itertools.count(1)
        for episode_id in range(1, 120):
            position = rng.choice(POSITIONS)
            closed = rng.random() < 0.7
            complete = rng.random() < 0.6
            priced = rng.random() < 0.8
            gross = rng.uniform(-50, 80) if rng.random() < 0.75 else None
            _insert_episode(
                connection,
                id=f"ep{episode_id}",
                position_key=position,
                ordinal=episode_id,
                protocol=rng.choice(["v3", "v4", None]),
                owner=rng.choice(OWNERS + [None]),
                custody=rng.choice(CUSTODIES + [None, None]),
                status="complete" if closed else "open",
                closed_at=now - rng.choice(ages) if closed else None,
                last_timestamp=now - rng.choice(ages),
                history_complete=int(complete),
                fees_complete=int(rng.random() < 0.7),
                pricing_complete=int(priced),
                identity_complete=1,
                fees_usd=rng.uniform(0, 5) if rng.random() < 0.8 else None,
                deposit_usd=rng.uniform(10, 500) if priced else None,
                proceeds_usd=rng.uniform(10, 500) if priced else None,
                gross_pnl_usd=gross,
                gas_usd=rng.uniform(0.1, 2) if rng.random() < 0.4 else None,
            )
            for _ in range(rng.randint(1, 3)):
                tx_hash = f"0x{rng.randint(1, 60):064x}"
                connection.execute(
                    "INSERT OR IGNORE INTO lp_accounting_effects("
                    "event_id,position_key,episode_id,tx_hash,block_number,"
                    "tx_index,log_index,timestamp,kind,exact,basis"
                    ") VALUES(?,?,?,?,10,0,0,100,'add',1,'test')",
                    (next(counter), position, f"ep{episode_id}", tx_hash),
                )
        for number in range(1, 61):
            if rng.random() < 0.6:
                connection.execute(
                    "INSERT INTO lp_accounting_tx_costs(tx_hash,payer,owner,"
                    "gas_usd,attribution,block_number) VALUES(?,?,?,?,'exact',1)",
                    (
                        f"0x{number:064x}", OWNERS[0], rng.choice(OWNERS),
                        rng.uniform(0.01, 1) if rng.random() < 0.8 else None,
                    ),
                )


@pytest.fixture
def ledger(tmp_path):
    store = MarketStore(tmp_path / "market.sqlite")
    book = AccountBook(store, deferred=True).install()
    _seed(store)
    try:
        yield store, book
    finally:
        store.close()


def _comparable(result):
    def rounded(value):
        if isinstance(value, float):
            return round(value, 6)
        if isinstance(value, dict):
            return {key: rounded(item) for key, item in value.items()}
        return value

    return sorted(
        (rounded(row) for row in result["rows"]),
        key=lambda row: (row.get("owner") or "", row.get("custody") or ""),
    )


def test_rollup_is_absent_until_a_build_is_published(ledger):
    _store, book = ledger
    assert book.owner_activity("30d") is None
    assert book._publish_owner_activity(book._owner_activity_snapshot(int(time.time())))
    served = book.owner_activity("30d")
    assert served is not None
    assert served["total"] == len(served["rows"]) > 0
    assert served["built_at"] > 0
    assert served["accounting_revision"] == book.owners_revision


@pytest.mark.parametrize("window", ["1h", "24h", "7d", "30d", "all"])
@pytest.mark.parametrize("protocol", ["", "v3", "v4"])
def test_rollup_matches_owner_candidates(ledger, window, protocol):
    _store, book = ledger
    now = int(time.time())
    book._publish_owner_activity(book._owner_activity_snapshot(now))
    for identity_scope in ("all", "wallets", "custody"):
        params = {
            "window": window, "protocol": protocol,
            "identity_scope": identity_scope,
        }
        book._owners_cache.clear()
        expected = book.owner_candidates(params)
        served = book.owner_activity(
            window, protocol=protocol, identity_scope=identity_scope,
        )
        assert served is not None
        assert _comparable(served) == _comparable(expected)
        assert served["valid_until"] == expected["valid_until"]
        assert served["coverage"] == expected["coverage"]


def test_rollup_publish_writes_only_changed_rows_and_survives_reload(ledger):
    store, book = ledger
    now = int(time.time())
    book._publish_owner_activity(book._owner_activity_snapshot(now))
    with store.transaction() as connection:
        connection.execute(
            "UPDATE lp_accounting_episodes SET gross_pnl_usd=999 WHERE id='ep1'",
        )
    before = store.read().execute(
        "SELECT COUNT(*) FROM lp_accounting_owner_activity",
    ).fetchone()[0]
    revision = book.owners_revision
    assert book._publish_owner_activity(book._owner_activity_snapshot(now))
    assert book.owners_revision == revision + 1
    reloaded = AccountBook(store, deferred=True)
    reloaded._installed = True
    served = reloaded.owner_activity("all")
    assert served is not None
    assert _comparable(served) == _comparable(book.owner_activity("all"))
    assert store.read().execute(
        "SELECT COUNT(*) FROM lp_accounting_owner_activity",
    ).fetchone()[0] == before


def test_rollup_from_another_epoch_is_not_served(ledger):
    store, book = ledger
    snapshot = book._owner_activity_snapshot(int(time.time()))
    assert book._publish_owner_activity(snapshot)
    with store.transaction() as connection:
        store._set_metadata(connection, "epoch", 99)
    assert book.owner_activity("7d") is None
    assert not book._publish_owner_activity(snapshot)


def test_rows_that_aged_out_since_the_build_are_not_served(ledger, monkeypatch):
    _store, book = ledger
    now = int(time.time())
    book._publish_owner_activity(book._owner_activity_snapshot(now))
    fresh = book.owner_activity("1h")
    assert fresh is not None and fresh["valid_until"] is not None
    monkeypatch.setattr(
        "rhpools.lp_market_accounting.time.time",
        lambda: float(fresh["valid_until"]),
    )
    later = book.owner_activity("1h")
    assert later is not None
    assert later["total"] < fresh["total"]
    assert later["valid_until"] is None or later["valid_until"] > fresh["valid_until"]


def test_replica_build_survives_a_ledger_scan_longer_than_a_default_read(ledger, tmp_path, monkeypatch):
    import rhpools.lp_market_store as market_store
    from rhpools.lp_owner_rollup import RollupReplica

    store, book = ledger
    monkeypatch.setattr(market_store, "_READER_SNAPSHOT_SECONDS", 0.2)
    slow_scan = (
        "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<4000000) "
        "SELECT NULL FROM n WHERE x<0"
    )
    started = time.monotonic()
    store.read().execute(slow_scan).fetchall()
    assert time.monotonic() - started > 0.2
    monkeypatch.setattr(
        AccountBook, "_stale_position_keys",
        staticmethod(lambda conn: [str(row[0]) for row in conn.execute(slow_scan)]),
    )
    replica = RollupReplica(tmp_path, reserve_bytes=0)
    try:
        replica.synchronize(book)
        snapshot = book._owner_activity_snapshot(replica.built_at, replica=replica)
    finally:
        replica.close()
    assert book._publish_owner_activity(snapshot)
    assert book.owner_activity("24h")["rows"]


class _DeferredPool:
    """Runs a submitted build only when its future is first polled, so the
    accounting loop's prune can run in between, as with a real worker."""

    class _Future:
        def __init__(self, fn, args):
            self._call = (fn, args)
            self._outcome = None

        def done(self):
            if self._outcome is None:
                fn, args = self._call
                try:
                    self._outcome = (fn(*args), None)
                except Exception as exc:
                    self._outcome = (None, exc)
            return True

        def result(self):
            self.done()
            value, error = self._outcome
            if error is not None:
                raise error
            return value

    def submit(self, fn, *args):
        return self._Future(fn, args)

    def shutdown(self, **_kwargs):
        pass


@pytest.fixture
def rollup_worker(ledger, monkeypatch):
    import rhpools.lp_market_accounting as accounting

    store, book = ledger
    monkeypatch.setattr(store, "_stop", None, raising=False)
    monkeypatch.setattr(accounting, "_preparation_book", book)
    monkeypatch.setattr(accounting, "_rollup_replica", None)
    monkeypatch.setattr(accounting, "_OWNER_ACTIVITY_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(book, "_create_preparation_pool", lambda *_args: _DeferredPool())
    book._storage_reserve_bytes = 1
    try:
        yield store, book
    finally:
        if accounting._rollup_replica is not None:
            accounting._rollup_replica.close()


def _journal(store):
    connection = store.read()
    rows, = connection.execute("SELECT COUNT(*) FROM lp_owner_rollup_journal").fetchone()
    ack = connection.execute(
        "SELECT value FROM lp_accounting_meta WHERE key='owner_rollup_ack'",
    ).fetchone()
    head, = connection.execute(
        "SELECT seq FROM sqlite_sequence WHERE name='lp_owner_rollup_journal'",
    ).fetchone()
    return rows, None if ack is None else int(ack[0]), int(head)


def _churn(store):
    with store.transaction() as connection:
        connection.execute(
            "UPDATE lp_accounting_episodes SET gross_pnl_usd=COALESCE(gross_pnl_usd,0)+1",
        )


def _prune(store):
    from rhpools.lp_owner_rollup import prune_journal

    while True:
        with store.transaction() as connection:
            if not prune_journal(connection, limit=64):
                return


def _build(store, book):
    """One accounting-loop rollup cycle: submit, prune while it runs, collect."""
    book._rollup_generation = -1
    assert book._advance_rollup() is False
    _prune(store)
    published = book._advance_rollup()
    _prune(store)
    return published


def test_failed_replica_builds_do_not_freeze_the_journal(rollup_worker, monkeypatch):
    from types import SimpleNamespace

    from rhpools.lp_owner_rollup import ReplicaSpaceError
    import rhpools.lp_market_accounting as accounting

    store, book = rollup_worker
    assert _build(store, book)
    rows, ack, head = _journal(store)
    assert (rows, ack) == (0, head)
    with monkeypatch.context() as full_disk:
        # Production: the replica's free-space check trips on every attempt,
        # so no build publishes and owner_rollup_ack stops moving.
        full_disk.setattr(
            "rhpools.lp_owner_rollup.shutil.disk_usage",
            lambda _path: SimpleNamespace(free=0),
        )
        accounting._rollup_replica._next_space_check = 0.0
        for _ in range(3):
            _churn(store)
            with pytest.raises(ReplicaSpaceError):
                _build(store, book)
        rows, ack, head = _journal(store)
        assert ack == head
        assert rows == 0
    _churn(store)
    assert _build(store, book)
    rows, ack, head = _journal(store)
    assert (rows, ack) == (0, head)
    served = _comparable(book.owner_activity("all"))
    assert book._publish_owner_activity(book._owner_activity_snapshot(int(time.time())))
    assert served == _comparable(book.owner_activity("all"))


def test_a_replica_kept_across_a_deadline_still_finds_its_journal(rollup_worker, monkeypatch):
    from rhpools.lp_market_accounting import PreparationDeadlineError

    store, book = rollup_worker
    assert _build(store, book)

    def deadline(*_args, **_kwargs):
        raise PreparationDeadlineError("owner activity snapshot exceeded its deadline")

    with monkeypatch.context() as slow:
        slow.setattr(book, "_owner_activity_snapshot", deadline)
        _churn(store)
        assert _build(store, book) is False
    _churn(store)
    _, before, head = _journal(store)
    assert before < head
    assert _build(store, book)
    rows, ack, head = _journal(store)
    assert (rows, ack) == (0, head)


class _PoolRecorder:
    """Stands in for ProcessPoolExecutor and keeps each pool's worker setup."""

    created: list[tuple] = []

    def __init__(self, **kwargs):
        self.created.append(kwargs["initargs"])

    def submit(self, *_args):
        return None


def _pool_setups(store, monkeypatch, **options):
    """Worker initargs for the (rollup, preparation) pools of one book."""
    import rhpools.lp_market_accounting as accounting

    monkeypatch.setattr(accounting, "ProcessPoolExecutor", _PoolRecorder)
    _PoolRecorder.created = []
    book = AccountBook(store, deferred=True, preparation_workers=3, **options)
    book._advance_rollup()
    book._create_preparation_pool(3)
    rollup, preparation = _PoolRecorder.created
    return rollup, preparation


def test_only_the_rollup_pool_takes_the_replica_directory_and_reserve(ledger, tmp_path, monkeypatch):
    store, _book = ledger
    replica_dir = tmp_path / "replica"
    replica_dir.mkdir()
    database_dir = str(store.path.resolve().parent)
    rollup, preparation = _pool_setups(store, monkeypatch, storage_reserve_bytes=160)
    assert rollup[2:] == preparation[2:] == (160, database_dir)
    rollup, preparation = _pool_setups(
        store, monkeypatch, storage_reserve_bytes=160,
        rollup_replica_dir=replica_dir, rollup_replica_reserve_bytes=20,
    )
    assert rollup[2:] == (20, str(replica_dir.resolve()))
    assert preparation[2:] == (160, database_dir)
    with pytest.raises(ValueError):
        AccountBook(store, rollup_replica_dir=tmp_path / "missing")


def test_the_writer_reserve_does_not_follow_the_replica_reserve(tmp_path):
    from rhpools.lp_market_service import LPMarketService

    replica_dir = tmp_path / "replica"
    replica_dir.mkdir()
    app = LPMarketService(
        None, "http://127.0.0.1:1", tmp_path / "market.sqlite", start=False,
        history_disk_reserve_bytes=160, rollup_replica_dir=replica_dir,
        rollup_replica_reserve_bytes=20,
    )
    try:
        assert app.indexer.history_disk_reserve_bytes == 160
        assert app.book._storage_reserve_bytes == 160
        assert app.book._rollup_reserve_bytes == 20
    finally:
        app.close()


def test_replica_settings_default_to_the_database_directory_and_disk_reserve(monkeypatch):
    from rhpools.lp_server import parser

    monkeypatch.delenv("LP_ROLLUP_REPLICA_DIR", raising=False)
    monkeypatch.delenv("LP_ROLLUP_REPLICA_RESERVE_GIB", raising=False)
    monkeypatch.setenv("LP_DISK_RESERVE_GIB", "160")
    args = parser().parse_args([])
    assert (args.disk_reserve_gib, args.rollup_replica_dir, args.rollup_replica_reserve_gib) == (
        160.0, None, None,
    )
    monkeypatch.setenv("LP_ROLLUP_REPLICA_DIR", "/data/disk1/rhpools-rollup")
    monkeypatch.setenv("LP_ROLLUP_REPLICA_RESERVE_GIB", "20")
    args = parser().parse_args([])
    assert (args.disk_reserve_gib, args.rollup_replica_dir, args.rollup_replica_reserve_gib) == (
        160.0, "/data/disk1/rhpools-rollup", 20.0,
    )


@pytest.mark.parametrize("fits", [True, False])
def test_rollup_worker_builds_in_and_reserves_space_on_the_replica_directory(
    ledger, tmp_path, monkeypatch, fits,
):
    import os
    import shutil
    from types import SimpleNamespace

    import rhpools.lp_market_accounting as accounting
    from rhpools.lp_owner_rollup import ReplicaSpaceError

    store, _book = ledger
    replica_dir = (tmp_path / "replica").resolve()
    replica_dir.mkdir()
    free = 50 * 1024**3
    # Exactly one of the two reserves exceeds the replica filesystem's free
    # space; the build must follow the replica reserve.
    writer_reserve, replica_reserve = (
        (free + 1, free - 1) if fits else (free - 1, free + 1)
    )
    rollup_setup, _preparation = _pool_setups(
        store, monkeypatch, storage_reserve_bytes=writer_reserve,
        rollup_replica_dir=replica_dir, rollup_replica_reserve_bytes=replica_reserve,
    )
    monkeypatch.setattr(accounting, "_preparation_book", None)
    monkeypatch.setattr(accounting, "_rollup_replica", None)
    monkeypatch.setattr(accounting, "_worker_temp_directory", None)
    measured = []
    disk_usage = shutil.disk_usage

    def measured_disk_usage(path):
        measured.append(os.fspath(path))
        if os.fspath(path) == str(replica_dir):
            return SimpleNamespace(free=free)
        return disk_usage(path)

    monkeypatch.setattr("rhpools.lp_owner_rollup.shutil.disk_usage", measured_disk_usage)
    accounting._initialize_preparation_worker(*rollup_setup)
    reader = accounting._preparation_book.store.read()
    try:
        if not fits:
            with pytest.raises(ReplicaSpaceError):
                accounting._owner_activity_in_worker(0)
            assert set(measured) == {str(replica_dir)}
            return
        snapshot = accounting._owner_activity_in_worker(0)
        assert snapshot["rows"] and snapshot["journal_through"] > 0
        assert measured and set(measured) == {str(replica_dir)}
        replica = accounting._rollup_replica.connection
        replica.execute("PRAGMA cache_size=2")
        replica.execute("CREATE TABLE pad(blob)")
        replica.executemany(
            "INSERT INTO pad VALUES(randomblob(4096))", ((),) * 256,
        )
        if os.path.isdir("/proc/self/fd"):
            opened = {
                os.readlink(f"/proc/self/fd/{fd}") for fd in os.listdir("/proc/self/fd")
                if os.path.islink(f"/proc/self/fd/{fd}")
            }
            assert any(target.startswith(str(replica_dir) + os.sep) for target in opened)
    finally:
        if accounting._rollup_replica is not None:
            accounting._rollup_replica.close()
        reader.execute("PRAGMA temp_store_directory=''")
        reader.close()
