"""Gas completeness and transaction identity survive batched cost projection."""
from __future__ import annotations

from rhpools.lp_market_accounting import AccountBook
from rhpools.lp_market_store import MarketStore


def test_missing_and_duplicate_transaction_costs_remain_exact():
    store = MarketStore(":memory:")
    try:
        book = AccountBook(store).install()
        with store.transaction() as connection:
            for episode_id, position_key in (("ep1", "p1"), ("ep2", "p2")):
                fields = {
                    row[1]: 0 if row[2] in ("INTEGER", "REAL") else "0"
                    for row in connection.execute(
                        "PRAGMA table_info(lp_accounting_episodes)"
                    )
                    if row[3]
                }
                fields.update(
                    id=episode_id,
                    position_key=position_key,
                    ordinal=1,
                    status="closed",
                    accounting_basis="test",
                    qualifiers="[]",
                )
                columns = list(fields)
                connection.execute(
                    "INSERT INTO lp_accounting_episodes("
                    + ",".join(columns)
                    + ") VALUES("
                    + ",".join("?" for _ in columns)
                    + ")",
                    [fields[column] for column in columns],
                )

            for event_id, position_key, episode_id, tx_hash in (
                (1, "p1", "ep1", "a"),
                (2, "p1", "ep1", "a"),
                (3, "p1", "ep1", "b"),
                (4, "p1", None, "b"),
                (5, "p1", "ep1", "c"),
                (6, "p2", "ep2", "c"),
            ):
                connection.execute(
                    "INSERT INTO lp_accounting_effects("
                    "event_id,position_key,episode_id,tx_hash,block_number,"
                    "tx_index,log_index,timestamp,kind,exact,basis"
                    ") VALUES(?,?,?,?,10,0,?,100,'add',1,'test')",
                    (event_id, position_key, episode_id, tx_hash, event_id),
                )

            book._rebuild_tx_cost_batch(connection, ["a", "b", "c"])
            costs = {
                row["tx_hash"]: dict(row)
                for row in connection.execute("SELECT * FROM lp_accounting_tx_costs")
            }
            assert costs["a"]["position_key"] == "p1"
            assert costs["a"]["episode_id"] == "ep1"
            assert costs["b"]["episode_id"] == "ep1"
            assert costs["c"]["position_key"] is None
            assert costs["c"]["episode_id"] is None
            assert all(
                row["gas_usd"] is None
                and row["attribution"] == "missing_transaction"
                for row in costs.values()
            )

            book._refresh_episode_cost_batch(connection, ["ep1", "ep2"])
            assert all(
                row[0] is None
                for row in connection.execute(
                    "SELECT gas_usd FROM lp_accounting_episodes"
                )
            )

            connection.execute(
                "DELETE FROM lp_accounting_effects WHERE tx_hash IN ('b','c')"
            )
            connection.execute(
                "UPDATE lp_accounting_tx_costs "
                "SET gas_usd=2.5,attribution='exact',episode_id='ep1' "
                "WHERE tx_hash='a'"
            )
            book._refresh_episode_cost_batch(connection, ["ep1"])
            assert connection.execute(
                "SELECT gas_usd FROM lp_accounting_episodes WHERE id='ep1'"
            ).fetchone()[0] == 2.5

            connection.execute(
                "UPDATE lp_accounting_tx_costs SET gas_usd=NULL WHERE tx_hash='a'"
            )
            book._refresh_episode_cost_batch(connection, ["ep1"])
            assert connection.execute(
                "SELECT gas_usd FROM lp_accounting_episodes WHERE id='ep1'"
            ).fetchone()[0] is None
    finally:
        store.close()


def test_pool_financial_scope_returns_only_matching_pending_identities():
    store = MarketStore(":memory:")
    try:
        book = AccountBook(store).install()
        target = "0x" + "11" * 20
        owner = "0x" + "22" * 20
        custody = "0x" + "33" * 20
        other_owner = "0x" + "44" * 20
        with store.transaction() as connection:
            connection.executemany(
                "INSERT INTO lp_accounting_pending("
                "position_key,generation,requested_revision,requested_epoch,"
                "priority_block,priority_tx_index,priority_log_index,identities_ready"
                ") VALUES(?,1,1,0,1,0,0,1)",
                (("target-position",), ("other-position",)),
            )
            connection.executemany(
                "INSERT INTO lp_accounting_pending_identities("
                "position_key,kind,identity,protocol,pool_id,timestamp"
                ") VALUES(?,?,?,?,?,?)",
                (
                    ("target-position", "scope", "", "v3", target, 100),
                    ("target-position", "scope", "", "v2", target, 100),
                    ("target-position", "owner", owner, "v3", target, 100),
                    ("target-position", "custody", custody, "v3", target, 100),
                    ("other-position", "scope", "", "v3", "other", 50),
                    ("other-position", "owner", other_owner, "v3", "other", 50),
                ),
            )
            connection.execute(
                "INSERT INTO lp_accounting_meta(key,value) "
                "VALUES('bootstrap_phase','complete') "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
            )
        with store.reader_snapshot() as reader:
            _, _, pending_owners, pending_custodies, complete = (
                book._scoped_owner_financial_state(
                    reader, {"pool": target}, None,
                )
            )
            _, _, recent_owners, recent_custodies, recent_complete = (
                book._scoped_owner_financial_state(reader, {}, 75)
            )
        assert complete
        assert pending_owners == {owner}
        assert pending_custodies == {custody}
        assert recent_complete
        assert recent_owners == {owner}
        assert recent_custodies == {custody}
    finally:
        store.close()


def test_owner_reads_do_not_walk_irrelevant_event_history():
    store = MarketStore(":memory:")
    try:
        book = AccountBook(store).install()
        owner = "0x" + "11" * 20
        event_count = 4_096
        with store.transaction() as connection:
            connection.executemany(
                "INSERT INTO events("
                "block_number,block_hash,tx_hash,tx_index,log_index,timestamp,"
                "protocol,kind,owner,position_key,data,revision"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    (
                        number, f"block-{number}", f"tx-{number}", 0, 0,
                        number, "v3", "transfer", owner, "position", "{}", 1,
                    )
                    for number in range(1, event_count + 1)
                ),
            )
            connection.executemany(
                "INSERT INTO lp_accounting_event_keys("
                "event_id,position_key"
                ") VALUES(?,'position')",
                ((number,) for number in range(1, event_count + 1)),
            )
            connection.execute(
                "INSERT INTO lp_ownership_intervals("
                "position_key,ordinal,owner,identity_basis,acquired_by,"
                "start_block,start_tx_index,start_log_index,start_timestamp,"
                "end_block,end_tx_index,end_log_index,end_timestamp,complete"
                ") VALUES('position',1,?,'verified_owner','tx-1',"
                "1,0,0,1,?,0,0,?,1)",
                (owner, event_count, event_count),
            )

        with store.reader_snapshot() as reader:
            boundary_steps = 0

            def bound_boundary():
                nonlocal boundary_steps
                boundary_steps += 1
                return int(boundary_steps > 500)

            reader.set_progress_handler(bound_boundary, 1)
            through, as_of, _, _, _ = book._scoped_owner_financial_state(
                reader, {"protocol": "v4"}, None,
            )
            reader.set_progress_handler(None, 0)
            assert through == {
                "block_number": event_count,
                "tx_index": 0,
                "log_index": 0,
            }
            assert as_of == event_count

            activity_steps = 0

            def bound_activity():
                nonlocal activity_steps
                activity_steps += 1
                return int(activity_steps > 1_000)

            reader.set_progress_handler(bound_activity, 1)
            activities = book._historical_owner_activity(
                reader, [{"owner": owner}], {"window": "all"},
            )
            reader.set_progress_handler(None, 0)
            assert activities[0]["block_number"] == event_count
    finally:
        store.close()
