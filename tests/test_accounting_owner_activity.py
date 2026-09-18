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
