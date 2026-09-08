"""Replay selection must preserve oldest-ready order across migration and backoff."""
from rhpools.lp_market_index import MarketIndexer
from rhpools.lp_market_store import MarketStore


def test_migrated_identity_queue_preserves_data_and_ready_order(tmp_path, monkeypatch):
    now = 1_700_000_000
    monkeypatch.setattr("rhpools.lp_market_index.time.time", lambda: now)
    path = tmp_path / "market.sqlite"
    store = MarketStore(path)
    with store.transaction() as conn:
        store._set_metadata(conn, "epoch", 7)
        store._set_metadata(conn, "cursor:live", {
            "block_number": 100, "block_hash": "block-100", "timestamp": now,
        })
        rows = [
            (f"tx-{number:03}", number, "block", 0, 0, "receipt_unavailable", now, now)
            for number in range(16)
        ]
        rows.extend(
            (f"tx-{number:03}", number, "block", 2, ready,
             f"pool_identity_pending:{number}", now, now)
            for number, ready in (
                (16, now + 5), (17, now + 1), (18, now - 1),
                (19, 0), (20, now), (21, -1), (22, now + 60), (23, now - 20),
            )
        )
        conn.executemany("INSERT INTO pending_enrichment VALUES(?,?,?,?,?,?,?,?)", rows)
        # Reconstruct the previous queue schema, then use the ordinary open path.
        conn.execute("DROP INDEX pending_enrichment_identity_immediate_idx")
        conn.execute("DROP INDEX pending_enrichment_identity_retry_ready_idx")
        conn.execute(
            "CREATE INDEX pending_enrichment_identity_order_idx "
            "ON pending_enrichment(block_number,tx_hash,next_attempt) "
            "WHERE last_error GLOB 'pool_identity_pending:*'"
        )
        conn.execute("PRAGMA user_version=3")
    before = [tuple(row) for row in store.read().execute(
        "SELECT * FROM pending_enrichment ORDER BY block_number,tx_hash"
    )]
    store.close()

    store = MarketStore(path)
    indexer = object.__new__(MarketIndexer)
    indexer.store = store
    try:
        assert [tuple(row) for row in store.read().execute(
            "SELECT * FROM pending_enrichment ORDER BY block_number,tx_hash"
        )] == before
        status = store.status()
        assert status["epoch"] == 7
        assert status["indexed_head"] == 100
        assert [row["block_number"] for row in indexer._pending_pool_identity_replays(4)] == [18, 19, 20, 21]
        with store.transaction() as conn:
            conn.execute("UPDATE pending_enrichment SET next_attempt=? WHERE tx_hash='tx-018'", (now + 30,))
        assert [row["block_number"] for row in indexer._pending_pool_identity_replays(4)] == [19, 20, 21, 23]
        with store.transaction() as conn:
            conn.execute("DELETE FROM pending_enrichment WHERE block_number<16")
        assert [row["block_number"] for row in indexer._pending_pool_identity_replays(4)] == [19, 20, 21, 23]
        assert [row["block_number"] for row in indexer._pending_pool_identity_replays(1)] == [19]
    finally:
        store.close()
