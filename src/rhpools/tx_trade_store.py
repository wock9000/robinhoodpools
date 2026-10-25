from __future__ import annotations

import json
from contextlib import contextmanager
import sqlite3
import time
from pathlib import Path
from typing import Any


class TradeStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS trades (
                hash TEXT PRIMARY KEY,
                wallet TEXT NOT NULL,
                block INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('swap', 'lp')),
                sent TEXT NOT NULL,
                received TEXT NOT NULL,
                via TEXT NOT NULL,
                fee TEXT,
                created_at INTEGER NOT NULL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS trades_wallet_block ON trades(wallet, block DESC)")

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def save(self, wallet: str, row: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute("""INSERT INTO trades
                (hash, wallet, block, timestamp, kind, sent, received, via, fee, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET
                    wallet=excluded.wallet, block=excluded.block, timestamp=excluded.timestamp,
                    kind=excluded.kind, sent=excluded.sent, received=excluded.received,
                    via=excluded.via, fee=excluded.fee""",
                (row["hash"], wallet, row["block"], row["timestamp"], row["kind"],
                 json.dumps(row["sent"]), json.dumps(row["received"]), row["via"],
                 json.dumps(row["fee"]) if row["fee"] is not None else None, int(time.time())))

    def history(self, wallet: str, before: int | None = None) -> list[dict[str, Any]]:
        with self._connect() as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("""SELECT hash, block, timestamp, kind, sent, received, via, fee
                FROM trades WHERE wallet = ? AND (? IS NULL OR block < ?)
                ORDER BY block DESC, hash DESC LIMIT 51""", (wallet, before, before)).fetchall()
        return [{"hash": row["hash"], "block": row["block"], "timestamp": row["timestamp"],
                 "kind": row["kind"], "sent": json.loads(row["sent"]),
                 "received": json.loads(row["received"]), "via": row["via"],
                 "fee": json.loads(row["fee"]) if row["fee"] else None, "source": "rhpools"}
                for row in rows]
