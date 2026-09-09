"""Durable canonical event store for the LP market terminal."""
from __future__ import annotations

import contextlib
import json
import math
import re
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from itertools import chain, islice
from pathlib import Path
from typing import Any


ProjectionApply = Callable[[sqlite3.Connection, list[dict[str, Any]]], None]
ProjectionRollback = Callable[[sqlite3.Connection, int], None]

EVENT_COLUMNS = (
    "block_number", "block_hash", "tx_hash", "tx_index", "log_index",
    "timestamp", "pool_id", "protocol", "kind", "owner", "custody",
    "position_key", "token_id", "tick_lower", "tick_upper",
    "liquidity_delta", "liquidity", "sqrt_price_x96", "tick", "fee_ppm",
    "amount0", "amount1", "fee_amount0", "fee_amount1", "cashflow0",
    "cashflow1", "price0_usd", "price1_usd", "volume_usd", "fees_usd",
    "deposit_usd", "withdrawal_usd", "pricing_basis", "accounting_basis",
    "identity_basis", "data", "revision",
)
INTEGER_EVENT_COLUMNS = frozenset({
    "block_number", "tx_index", "log_index", "timestamp", "tick_lower",
    "tick_upper", "tick", "fee_ppm", "revision",
})
DECIMAL_EVENT_COLUMNS = frozenset({
    "liquidity_delta", "liquidity", "sqrt_price_x96", "amount0", "amount1",
    "fee_amount0", "fee_amount1", "cashflow0", "cashflow1",
})
FLOAT_EVENT_COLUMNS = frozenset({
    "price0_usd", "price1_usd", "volume_usd", "fees_usd", "deposit_usd",
    "withdrawal_usd",
})
ADDRESS_EVENT_COLUMNS = frozenset({"owner", "custody"})
REQUIRED_EVENT_COLUMNS = (
    "block_number", "block_hash", "tx_hash", "tx_index", "log_index",
    "timestamp", "protocol", "kind",
)
LP_ENRICHMENT_KINDS = frozenset({
    "add", "remove", "collect", "checkpoint", "donate", "fee", "transfer",
})
POOL_COLUMNS = (
    "id", "protocol", "address", "token0", "token1", "symbol0", "symbol1",
    "decimals0", "decimals1", "fee_ppm", "tick_spacing", "hook", "factory",
    "created_block", "source", "metadata_json",
)
TRANSACTION_COLUMNS = (
    "tx_hash", "block_number", "block_hash", "payer", "gas_used", "gas_price",
    "gas_native", "gas_usd", "status", "data",
)


SEARCH_KINDS = frozenset({
    "pool", "token", "protocol", "owner", "custody", "transaction", "position",
})
_SEARCH_WORD_RE = re.compile(r"[a-z0-9]+")


class MarketStoreError(RuntimeError):
    """Base class for durable market-store failures."""


class CanonicalConflict(MarketStoreError):
    """A block number is already associated with a different canonical hash."""


def _json(value: Any) -> str:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True,
    )


def _decode_json(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default


def _integer(value: Any, name: str, *, nullable: bool = False) -> int | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith(("0x", "-0x")) else int(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer") from exc
    raise ValueError(f"{name} must be an integer")


def _decimal_text(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an exact integer decimal string")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        try:
            int(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be an exact integer decimal string") from exc
        return value
    raise ValueError(f"{name} must be an exact integer decimal string")


def _finite_float(value: Any, name: str) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _lower(value: Any) -> str | None:
    return None if value is None else str(value).lower()

def _batches(values: Iterable[Any], size: int = 500) -> Iterator[tuple[Any, ...]]:
    iterator = iter(values)
    while batch := tuple(islice(iterator, size)):
        yield batch


def _insert_rows(
    connection: sqlite3.Connection,
    prefix: str,
    rows: Iterable[Sequence[Any]],
    *,
    columns: int,
    suffix: str = "",
) -> None:
    # One SQLite step per batch avoids a GIL handoff for every inserted row.
    size = max(1, min(
        500, connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) // columns,
    ))
    placeholders = "(" + ",".join("?" for _ in range(columns)) + ")"
    statement = ""
    for batch in _batches(rows, size):
        if not statement or len(batch) != size:
            statement = (
                prefix + " VALUES "
                + ",".join([placeholders] * len(batch)) + suffix
            )
        connection.execute(statement, tuple(chain.from_iterable(batch)))



def _header_number(header: Mapping[str, Any]) -> int:
    value = header["number"]
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


def _header_timestamp(header: Mapping[str, Any]) -> int:
    value = header["timestamp"]
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


class MarketStore:
    """One WAL writer with serialized atomic projections and cheap readers.

    ``blocks`` contains both boundaries of every committed coverage interval and
    the actual header for each event block.  The coverage table records the
    contiguous interval those sparse anchors verify.

    A managed indexer may disable commit-time checkpoints when it owns the
    periodic checkpoint lane. WAL commits remain fully synchronized.
    """

    def __init__(self, path: str | Path, *, checkpoint_on_commit: bool = True) -> None:
        self.path = Path(path) if str(path) != ":memory:" else Path(":memory:")
        self.lock = threading.RLock()
        self._reader_lock = threading.Lock()
        self._local = threading.local()
        self._readers: dict[int, sqlite3.Connection] = {}
        self._projections: list[tuple[ProjectionApply, ProjectionRollback, bool]] = []
        self._closed = False
        self._change_token = 0
        self._pool_metadata_token = 0
        self._checkpoint_on_commit = checkpoint_on_commit
        if str(path) == ":memory:":
            self._database = f"file:lp-market-{id(self):x}?mode=memory&cache=shared"
            self._uri = True
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._database = str(self.path)
            self._uri = False
        self.connection = self._connect(writer=True)
        self._initialize()

    def _connect(self, *, writer: bool) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
            uri=self._uri,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA foreign_keys=ON")
        # The writer benefits from a large projection working set.  Reader
        # pages are also available through the shared mmap, so keep each
        # thread-local private cache small under concurrent HTTP traffic.
        connection.execute(f"PRAGMA cache_size=-{1048576 if writer else 1024}")
        connection.execute("PRAGMA mmap_size=34359738368")
        if writer:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            # Managed services checkpoint on their existing projection lane,
            # including a large inherited WAL, rather than before HTTP startup.
            # Standalone stores retain SQLite's automatic checkpoint fallback.
            pages = 65536 if self._checkpoint_on_commit else 0
            connection.execute(f"PRAGMA wal_autocheckpoint={pages}")
            connection.execute("PRAGMA journal_size_limit=268435456")
        else:
            connection.execute("PRAGMA query_only=ON")
        return connection

    def _initialize(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS pools(
            id TEXT PRIMARY KEY, protocol TEXT NOT NULL, address TEXT NOT NULL,
            token0 TEXT NOT NULL, token1 TEXT NOT NULL, symbol0 TEXT, symbol1 TEXT,
            decimals0 INTEGER, decimals1 INTEGER, fee_ppm INTEGER,
            tick_spacing INTEGER, hook TEXT, factory TEXT, created_block INTEGER,
            source TEXT, metadata_json TEXT
        );
        CREATE INDEX IF NOT EXISTS pools_created_idx ON pools(created_block, id);
        CREATE INDEX IF NOT EXISTS pools_token0_idx ON pools(token0);
        CREATE INDEX IF NOT EXISTS pools_token1_idx ON pools(token1);
        CREATE TABLE IF NOT EXISTS pool_provenance(
            pool_id TEXT PRIMARY KEY, observed_block INTEGER NOT NULL,
            observed_hash TEXT NOT NULL, basis TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS pool_provenance_block_idx
            ON pool_provenance(observed_block, pool_id);
        CREATE TABLE IF NOT EXISTS blocks(
            number INTEGER PRIMARY KEY, hash TEXT NOT NULL UNIQUE,
            parent_hash TEXT NOT NULL, timestamp INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL, block_hash TEXT NOT NULL,
            tx_hash TEXT NOT NULL, tx_index INTEGER NOT NULL, log_index INTEGER NOT NULL,
            timestamp INTEGER NOT NULL, pool_id TEXT, protocol TEXT NOT NULL,
            kind TEXT NOT NULL, owner TEXT, custody TEXT, position_key TEXT,
            token_id TEXT, tick_lower INTEGER, tick_upper INTEGER,
            liquidity_delta TEXT, liquidity TEXT, sqrt_price_x96 TEXT, tick INTEGER,
            fee_ppm INTEGER, amount0 TEXT, amount1 TEXT, fee_amount0 TEXT,
            fee_amount1 TEXT, cashflow0 TEXT, cashflow1 TEXT, price0_usd REAL,
            price1_usd REAL, volume_usd REAL, fees_usd REAL, deposit_usd REAL,
            withdrawal_usd REAL, pricing_basis TEXT, accounting_basis TEXT,
            identity_basis TEXT, data TEXT, revision INTEGER NOT NULL,
            UNIQUE(block_hash, tx_hash, log_index)
        );
        CREATE INDEX IF NOT EXISTS events_pool_time_idx
            ON events(pool_id, timestamp DESC, id DESC);
        CREATE INDEX IF NOT EXISTS events_kind_id_idx ON events(kind, id DESC);
        CREATE INDEX IF NOT EXISTS events_position_order_idx
            ON events(position_key, block_number, tx_index, log_index)
            WHERE position_key IS NOT NULL;
        CREATE INDEX IF NOT EXISTS events_owner_time_idx
            ON events(owner, timestamp DESC, id DESC) WHERE owner IS NOT NULL;
        CREATE INDEX IF NOT EXISTS events_custody_time_idx
            ON events(custody, timestamp DESC, id DESC) WHERE custody IS NOT NULL;
        CREATE INDEX IF NOT EXISTS events_block_idx
            ON events(block_number, tx_index, log_index);
        CREATE INDEX IF NOT EXISTS events_revision_id_idx ON events(revision, id);
        CREATE INDEX IF NOT EXISTS events_tx_log_idx ON events(tx_hash, log_index);
        CREATE TABLE IF NOT EXISTS transactions(
            tx_hash TEXT PRIMARY KEY, block_number INTEGER NOT NULL,
            block_hash TEXT NOT NULL, payer TEXT, gas_used TEXT, gas_price TEXT,
            gas_native TEXT, gas_usd REAL, status INTEGER, data TEXT
        );
        CREATE INDEX IF NOT EXISTS transactions_block_idx ON transactions(block_number);
        CREATE TABLE IF NOT EXISTS metadata(
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS coverage_intervals(
            lane TEXT NOT NULL, start_block INTEGER NOT NULL, end_block INTEGER NOT NULL,
            start_hash TEXT NOT NULL, end_hash TEXT NOT NULL,
            PRIMARY KEY(lane, start_block, end_block),
            CHECK(start_block <= end_block)
        );
        CREATE INDEX IF NOT EXISTS coverage_lane_end_idx
            ON coverage_intervals(lane, end_block);
        CREATE TABLE IF NOT EXISTS pending_enrichment(
            tx_hash TEXT PRIMARY KEY, block_number INTEGER NOT NULL,
            block_hash TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL DEFAULT 0, last_error TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL,
            generation INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS pending_enrichment_ready_idx
            ON pending_enrichment(next_attempt, block_number, tx_hash);
        CREATE INDEX IF NOT EXISTS pending_enrichment_order_idx
            ON pending_enrichment(block_number, tx_hash, next_attempt);
        CREATE TABLE IF NOT EXISTS token_metadata(
            address TEXT PRIMARY KEY, symbol TEXT NOT NULL, decimals INTEGER NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pending_token_metadata(
            address TEXT PRIMARY KEY, attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL DEFAULT 0, last_error TEXT
        );
        CREATE INDEX IF NOT EXISTS pending_token_metadata_ready_idx
            ON pending_token_metadata(next_attempt, address);
        CREATE TABLE IF NOT EXISTS pending_pool_unpublish(
            pool_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pending_reprojection(
            event_id INTEGER PRIMARY KEY, block_number INTEGER NOT NULL,
            tx_index INTEGER NOT NULL, log_index INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL DEFAULT 0, last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS pending_balances(
            pool_id TEXT NOT NULL, block_number INTEGER NOT NULL,
            block_hash TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL DEFAULT 0, last_error TEXT,
            PRIMARY KEY(pool_id, block_number)
        );
        CREATE INDEX IF NOT EXISTS pending_balances_ready_idx
            ON pending_balances(next_attempt, block_number, pool_id);
        CREATE INDEX IF NOT EXISTS pending_balances_order_idx
            ON pending_balances(block_number, pool_id, next_attempt);
        CREATE TABLE IF NOT EXISTS pool_balances(
            pool_id TEXT NOT NULL, block_number INTEGER NOT NULL,
            block_hash TEXT NOT NULL, balance0 TEXT NOT NULL, balance1 TEXT NOT NULL,
            PRIMARY KEY(pool_id, block_number)
        );
        CREATE INDEX IF NOT EXISTS pool_balances_latest_idx
            ON pool_balances(pool_id, block_number DESC);
        CREATE TABLE IF NOT EXISTS lp_accounting_pool_generations(
            pool_id TEXT PRIMARY KEY,
            generation INTEGER NOT NULL CHECK(generation >= 0)
        );
        """
        with self.lock:
            self.connection.executescript(schema + """
                    CREATE TABLE IF NOT EXISTS lp_search_entities(
                        kind TEXT NOT NULL, id TEXT NOT NULL, label TEXT NOT NULL,
                        subtitle TEXT NOT NULL, href TEXT NOT NULL, rank INTEGER NOT NULL,
                        PRIMARY KEY(kind,id)
                    );
                    CREATE INDEX IF NOT EXISTS lp_search_entities_rank_idx
                        ON lp_search_entities(rank,kind,id);
                    CREATE TABLE IF NOT EXISTS lp_search_terms(
                        term TEXT COLLATE NOCASE NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL,
                        weight INTEGER NOT NULL, PRIMARY KEY(term,kind,id),
                        FOREIGN KEY(kind,id) REFERENCES lp_search_entities(kind,id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS lp_search_terms_entity_idx
                        ON lp_search_terms(kind,id,weight,term);
                    CREATE TABLE IF NOT EXISTS lp_catalog_search(
                        id TEXT PRIMARY KEY, protocol TEXT NOT NULL,
                        token0 TEXT NOT NULL, token1 TEXT NOT NULL,
                        symbol0 TEXT, symbol1 TEXT,
                        label TEXT NOT NULL, subtitle TEXT NOT NULL, href TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_protocol_idx
                        ON lp_catalog_search(protocol COLLATE NOCASE,id);
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_token0_idx
                        ON lp_catalog_search(token0 COLLATE NOCASE,id);
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_token1_idx
                        ON lp_catalog_search(token1 COLLATE NOCASE,id);
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_id_nocase_idx
                        ON lp_catalog_search(id COLLATE NOCASE);
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_symbol0_idx
                        ON lp_catalog_search(symbol0 COLLATE NOCASE,id);
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_symbol1_idx
                        ON lp_catalog_search(symbol1 COLLATE NOCASE,id);
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_pair01_idx
                        ON lp_catalog_search(symbol0 COLLATE NOCASE,symbol1 COLLATE NOCASE,id);
                    CREATE INDEX IF NOT EXISTS lp_catalog_search_pair10_idx
                        ON lp_catalog_search(symbol1 COLLATE NOCASE,symbol0 COLLATE NOCASE,id);
                    CREATE TABLE IF NOT EXISTS lp_catalog_tokens(
                        address TEXT PRIMARY KEY, symbol TEXT
                    );
                    CREATE INDEX IF NOT EXISTS lp_catalog_tokens_symbol_idx
                        ON lp_catalog_tokens(symbol COLLATE NOCASE,address);
                    CREATE INDEX IF NOT EXISTS lp_catalog_tokens_address_nocase_idx
                        ON lp_catalog_tokens(address COLLATE NOCASE);
                    CREATE TABLE IF NOT EXISTS lp_catalog_pairs(
                        symbol0 TEXT NOT NULL, symbol1 TEXT NOT NULL, pools INTEGER NOT NULL,
                        PRIMARY KEY(symbol0,symbol1)
                    );
                    CREATE INDEX IF NOT EXISTS lp_catalog_pairs_01_idx
                        ON lp_catalog_pairs(symbol0 COLLATE NOCASE,symbol1 COLLATE NOCASE);
                    CREATE INDEX IF NOT EXISTS lp_catalog_pairs_10_idx
                        ON lp_catalog_pairs(symbol1 COLLATE NOCASE,symbol0 COLLATE NOCASE);
                    """)
            if int(self.connection.execute("PRAGMA user_version").fetchone()[0]) < 2:
                self.connection.executescript("""
                    DROP INDEX IF EXISTS events_position_order_idx;
                    DROP INDEX IF EXISTS events_owner_time_idx;
                    CREATE INDEX events_position_order_idx
                        ON events(position_key,block_number,tx_index,log_index)
                        WHERE position_key IS NOT NULL;
                    CREATE INDEX events_owner_time_idx
                        ON events(owner,timestamp DESC,id DESC) WHERE owner IS NOT NULL;
                    PRAGMA user_version=2;
                """)
            if int(self.connection.execute("PRAGMA user_version").fetchone()[0]) < 3:
                reprojection_columns = {
                    str(row["name"])
                    for row in self.connection.execute(
                        "PRAGMA table_info(pending_reprojection)"
                    ).fetchall()
                }
                for column in ("block_number", "tx_index", "log_index"):
                    if column not in reprojection_columns:
                        self.connection.execute(
                            f"ALTER TABLE pending_reprojection ADD COLUMN "
                            f"{column} INTEGER NOT NULL DEFAULT 0"
                        )
                self.connection.execute(
                    "UPDATE pending_reprojection SET "
                    "block_number=(SELECT block_number FROM events "
                    "WHERE events.id=pending_reprojection.event_id),"
                    "tx_index=(SELECT tx_index FROM events "
                    "WHERE events.id=pending_reprojection.event_id),"
                    "log_index=(SELECT log_index FROM events "
                    "WHERE events.id=pending_reprojection.event_id)"
                )
                self.connection.execute("PRAGMA user_version=3")
            if int(self.connection.execute("PRAGMA user_version").fetchone()[0]) < 4:
                self.connection.executescript("""
                    DROP INDEX IF EXISTS pending_enrichment_identity_order_idx;
                    CREATE INDEX IF NOT EXISTS pending_enrichment_identity_immediate_idx
                        ON pending_enrichment(block_number,tx_hash)
                        WHERE next_attempt<=0
                        AND last_error GLOB 'pool_identity_pending:*';
                    CREATE INDEX IF NOT EXISTS pending_enrichment_identity_retry_ready_idx
                        ON pending_enrichment(next_attempt,block_number,tx_hash)
                        WHERE next_attempt>0
                        AND last_error GLOB 'pool_identity_pending:*';
                    PRAGMA user_version=4;
                """)
            if int(self.connection.execute("PRAGMA user_version").fetchone()[0]) < 5:
                self.connection.execute(
                    "CREATE TABLE IF NOT EXISTS lp_accounting_pool_generations("
                    "pool_id TEXT PRIMARY KEY,"
                    "generation INTEGER NOT NULL CHECK(generation >= 0))"
                )
                accounting_installed = self.connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='lp_accounting_episodes'"
                ).fetchone() is not None
                if accounting_installed:
                    self.connection.execute(
                        "CREATE INDEX IF NOT EXISTS "
                        "lp_accounting_episodes_last_timestamp "
                        "ON lp_accounting_episodes(last_timestamp)"
                    )
                self.connection.execute("PRAGMA user_version=5")
            if int(self.connection.execute("PRAGMA user_version").fetchone()[0]) < 6:
                accounting_installed = self.connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='lp_accounting_positions'"
                ).fetchone() is not None
                if accounting_installed:
                    self.connection.execute(
                        "CREATE INDEX IF NOT EXISTS "
                        "lp_accounting_positions_active_inventory "
                        "ON lp_accounting_positions("
                        "pool_id,owner,custody,protocol,liquidity,liquidity_known,"
                        "tick_lower,tick_upper,principal_usd,history_complete) "
                        "WHERE active_episode_id IS NOT NULL"
                    )
                self.connection.execute("PRAGMA user_version=6")
            if int(self.connection.execute("PRAGMA user_version").fetchone()[0]) < 7:
                self.connection.execute(
                    "CREATE INDEX IF NOT EXISTS pending_enrichment_financial_order_idx "
                    "ON pending_enrichment(block_number,tx_hash,next_attempt) "
                    "WHERE last_error IS NULL "
                    "OR last_error NOT GLOB 'pool_identity_pending:*'"
                )
                self.connection.execute("PRAGMA user_version=7")
            if int(self.connection.execute("PRAGMA user_version").fetchone()[0]) < 8:
                enrichment_columns = {
                    str(row["name"])
                    for row in self.connection.execute(
                        "PRAGMA table_info(pending_enrichment)"
                    ).fetchall()
                }
                if "generation" not in enrichment_columns:
                    self.connection.execute(
                        "ALTER TABLE pending_enrichment ADD COLUMN "
                        "generation INTEGER NOT NULL DEFAULT 0"
                    )
                self.connection.execute("PRAGMA user_version=8")
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS pending_reprojection_order_idx "
                "ON pending_reprojection(block_number,tx_index,log_index,event_id)"
            )
            with self.transaction() as connection:
                connection.execute(
                    "INSERT OR IGNORE INTO pool_provenance"
                    "(pool_id,observed_block,observed_hash,basis) "
                    "SELECT p.id,p.created_block,b.hash,p.source FROM pools p "
                    "JOIN blocks b ON b.number=p.created_block "
                    "WHERE p.created_block IS NOT NULL AND "
                    "(p.source LIKE 'first_observed_event_%' "
                    "OR p.source='PoolManager.modifyLiquidity trace')"
                )
                connection.execute(
                    "UPDATE pools SET created_block=NULL WHERE "
                    "source LIKE 'first_observed_event_%' "
                    "OR source='PoolManager.modifyLiquidity trace'"
                )
                connection.execute(
                    "INSERT OR IGNORE INTO token_metadata(address,symbol,decimals,updated_at) "
                    "SELECT token0,symbol0,decimals0,? FROM pools "
                    "WHERE symbol0 IS NOT NULL AND decimals0 IS NOT NULL "
                    "UNION SELECT token1,symbol1,decimals1,? FROM pools "
                    "WHERE symbol1 IS NOT NULL AND decimals1 IS NOT NULL",
                    (time.time(), time.time()),
                )
                connection.execute(
                    "UPDATE pools SET "
                    "symbol0=COALESCE(symbol0,(SELECT symbol FROM token_metadata "
                    "WHERE address=pools.token0)),"
                    "decimals0=COALESCE(decimals0,(SELECT decimals FROM token_metadata "
                    "WHERE address=pools.token0)) "
                    "WHERE (symbol0 IS NULL OR decimals0 IS NULL) "
                    "AND EXISTS(SELECT 1 FROM token_metadata WHERE address=pools.token0)"
                )
                connection.execute(
                    "UPDATE pools SET "
                    "symbol1=COALESCE(symbol1,(SELECT symbol FROM token_metadata "
                    "WHERE address=pools.token1)),"
                    "decimals1=COALESCE(decimals1,(SELECT decimals FROM token_metadata "
                    "WHERE address=pools.token1)) "
                    "WHERE (symbol1 IS NULL OR decimals1 IS NULL) "
                    "AND EXISTS(SELECT 1 FROM token_metadata WHERE address=pools.token1)"
                )
                connection.execute(
                    "INSERT OR IGNORE INTO pending_token_metadata(address) "
                    "SELECT token0 FROM pools WHERE symbol0 IS NULL OR decimals0 IS NULL "
                    "UNION SELECT token1 FROM pools WHERE symbol1 IS NULL OR decimals1 IS NULL",
                )
                count_tables = {
                    "indexed_events": "events",
                    "indexed_pools": "pools",
                    "indexed_transactions": "transactions",
                    "pending_enrichment": "pending_enrichment",
                    "pending_balances": "pending_balances",
                    "pending_metadata": "pending_token_metadata",
                    "pending_pool_unpublish": "pending_pool_unpublish",
                    "pending_reprojection": "pending_reprojection",
                }
                default_keys = (
                    "revision", "epoch", "pending_accounting", *count_tables,
                )
                marks = ",".join("?" for _ in default_keys)
                existing = {
                    str(row[0]) for row in connection.execute(
                        f"SELECT key FROM metadata WHERE key IN ({marks})",
                        default_keys,
                    ).fetchall()
                }
                defaults = {
                    key: 0 for key in (
                        "revision", "epoch", "pending_accounting",
                    )
                    if key not in existing
                }
                for key, table in count_tables.items():
                    if key not in existing:
                        defaults[key] = connection.execute(
                            f"SELECT COUNT(*) FROM {table}",
                        ).fetchone()[0]
                connection.executemany(
                    "INSERT INTO metadata(key,value) VALUES(?,?)",
                    ((key, _json(value)) for key, value in defaults.items()),
                )
                catalog_counts = {
                    row["protocol"]: int(row["pools"])
                    for row in connection.execute(
                        "SELECT protocol,COUNT(*) AS pools FROM lp_catalog_search "
                        "GROUP BY protocol"
                    )
                }
                if self._metadata(connection, "catalog_search_protocol_counts", {}) != catalog_counts:
                    # Older discovery replays counted an existing pool again.
                    connection.execute("DELETE FROM lp_catalog_pairs")
                    connection.execute(
                        "INSERT INTO lp_catalog_pairs(symbol0,symbol1,pools) "
                        "SELECT symbol0,symbol1,COUNT(*) FROM lp_catalog_search "
                        "WHERE symbol0 IS NOT NULL AND symbol1 IS NOT NULL "
                        "GROUP BY symbol0,symbol1"
                    )
                    self._set_metadata(connection, "catalog_search_protocol_counts", catalog_counts)

    def read(self) -> sqlite3.Connection:
        """Return the query-only connection owned by the calling thread."""
        if self._closed:
            raise MarketStoreError("market store is closed")
        connection = getattr(self._local, "reader", None)
        if connection is not None:
            return connection
        connection = self._connect(writer=False)
        ident = threading.get_ident()
        with self._reader_lock:
            if self._closed:
                connection.close()
                raise MarketStoreError("market store is closed")
            self._local.reader = connection
            displaced = self._readers.get(ident)
            if displaced is not None and displaced is not connection:
                displaced.close()
            self._readers[ident] = connection
        return connection

    def close_reader(self) -> None:
        """Close and unregister the calling thread's query-only connection."""
        ident = threading.get_ident()
        connection = getattr(self._local, "reader", None)
        if connection is None:
            return
        del self._local.reader
        with self._reader_lock:
            if self._readers.get(ident) is connection:
                self._readers.pop(ident, None)
        connection.close()

    def checkpoint(self) -> None:
        """Flush committed WAL pages without acquiring the ledger writer lock."""
        self.read().execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Serialize a writer transaction; nested calls share the outer commit."""
        if self._closed:
            raise MarketStoreError("market store is closed")
        with self.lock:
            depth = getattr(self._local, "write_depth", 0)
            if depth:
                self._local.write_depth = depth + 1
                try:
                    yield self.connection
                finally:
                    self._local.write_depth -= 1
                return
            self._local.write_depth = 1
            self._local.pool_metadata_dirty = False
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
            except BaseException:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()
                self._change_token += 1
                if self._local.pool_metadata_dirty:
                    self._pool_metadata_token += 1
            finally:
                self._local.write_depth = 0
                self._local.pool_metadata_dirty = False

    @property
    def change_token(self) -> int:
        return self._change_token

    @property
    def pool_metadata_token(self) -> int:
        """Change token scoped to committed pool and token metadata writes."""
        return self._pool_metadata_token

    def _mark_pool_metadata_changed(self) -> None:
        if not getattr(self._local, "write_depth", 0):
            raise RuntimeError("pool metadata changes require a writer transaction")
        self._local.pool_metadata_dirty = True

    def register_projection(
        self, apply: ProjectionApply, rollback: ProjectionRollback, *,
        persists_events: bool = False,
    ) -> None:
        """Register an atomic projection.

        ``persists_events`` is only for callbacks that durably write every
        mutation they make to the supplied event dictionaries.
        """
        if not callable(apply) or not callable(rollback):
            raise TypeError("projection callbacks must be callable")
        with self.lock:
            projection = (apply, rollback, bool(persists_events))
            if projection not in self._projections:
                self._projections.append(projection)


    def _metadata(self, connection: sqlite3.Connection, key: str, default: Any = None) -> Any:
        row = connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return _decode_json(row[0], default) if row is not None else default

    def _set_metadata(self, connection: sqlite3.Connection, key: str, value: Any) -> None:
        connection.execute(
            "INSERT INTO metadata(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, _json(value)),
        )

    def _bump(self, connection: sqlite3.Connection, key: str, amount: int) -> int:
        value = int(self._metadata(connection, key, 0)) + amount
        if value < 0:
            raise MarketStoreError(f"counter {key} became negative")
        self._set_metadata(connection, key, value)
        return value

    def _next_revision(self, connection: sqlite3.Connection) -> int:
        return self._bump(connection, "revision", 1)

    @staticmethod
    def _search_tokens(*values: Any) -> set[str]:
        terms: set[str] = set()
        for value in values:
            text = str(value or "").strip().lower()
            if not text:
                continue
            terms.add(text[:256])
            terms.update(word[:256] for word in _SEARCH_WORD_RE.findall(text))
        return terms

    @classmethod
    def _put_search_entities(
        cls, connection: sqlite3.Connection,
        entities: Iterable[tuple[str, str, str, str, str, int, Iterable[Any]]],
    ) -> None:
        entity_rows: list[tuple[str, str, str, str, str, int]] = []
        term_rows: list[tuple[str, str, str, int]] = []
        for kind, entity_id, label, subtitle, href, rank, terms in entities:
            if kind not in SEARCH_KINDS:
                raise ValueError(f"unsupported search kind {kind}")
            normalized_id = str(entity_id).strip().lower()[:512]
            if not normalized_id:
                continue
            entity_rows.append((
                kind, normalized_id, str(label)[:512], str(subtitle)[:1000],
                str(href)[:1500], int(rank),
            ))
            normalized_terms = cls._search_tokens(
                normalized_id, label, subtitle, *terms,
            )
            term_rows.extend(
                (term, kind, normalized_id, 0 if term == normalized_id else 10)
                for term in normalized_terms
            )
        if not entity_rows:
            return
        entity_rows.sort(key=lambda row: (row[0], row[1]))
        stable_rows = {
            (row[0], row[1]): row
            for row in entity_rows
            if row[0] in {"owner", "custody"}
        }
        unchanged: set[tuple[str, str]] = set()
        for batch in _batches(tuple(stable_rows), 250):
            predicates = " OR ".join("(kind=? AND id=?)" for _ in batch)
            values = tuple(value for key in batch for value in key)
            for stored in connection.execute(
                "SELECT kind,id,label,subtitle,href,rank "
                f"FROM lp_search_entities WHERE {predicates}",
                values,
            ):
                key = (str(stored["kind"]), str(stored["id"]))
                if tuple(stored) == stable_rows[key]:
                    unchanged.add(key)
        if unchanged:
            entity_rows = [
                row for row in entity_rows if (row[0], row[1]) not in unchanged
            ]
            term_rows = [
                row for row in term_rows if (row[1], row[2]) not in unchanged
            ]
        if not entity_rows:
            return
        term_rows.sort()
        _insert_rows(
            connection,
            "INSERT INTO lp_search_entities(kind,id,label,subtitle,href,rank)",
            entity_rows, columns=6,
            suffix=" ON CONFLICT(kind,id) DO UPDATE SET "
            "label=excluded.label,subtitle=excluded.subtitle,href=excluded.href,"
            "rank=excluded.rank WHERE "
            "lp_search_entities.label IS NOT excluded.label OR "
            "lp_search_entities.subtitle IS NOT excluded.subtitle OR "
            "lp_search_entities.href IS NOT excluded.href OR "
            "lp_search_entities.rank IS NOT excluded.rank",
        )
        if term_rows:
            _insert_rows(
                connection,
                "INSERT OR IGNORE INTO lp_search_terms(term,kind,id,weight)",
                term_rows, columns=4,
            )

    @classmethod
    def _put_search_entity(
        cls, connection: sqlite3.Connection, *, kind: str, entity_id: str,
        label: str, subtitle: str, href: str, rank: int, terms: Iterable[Any],
    ) -> None:
        cls._put_search_entities(
            connection,
            ((kind, entity_id, label, subtitle, href, rank, terms),),
        )

    @staticmethod
    def _query_value(value: Any) -> str:
        from urllib.parse import quote
        return quote(str(value), safe="")

    @classmethod
    def _pool_search_entities(
        cls, row: Mapping[str, Any],
    ) -> Iterator[tuple[str, str, str, str, str, int, Iterable[Any]]]:
        pool_id = str(row.get("id") or "").lower()
        if not pool_id:
            return
        symbol0 = str(row.get("symbol0") or row.get("token0") or "?")
        symbol1 = str(row.get("symbol1") or row.get("token1") or "?")
        protocol = str(row.get("protocol") or "unknown").lower()
        factory = str(row.get("factory") or "").lower()
        factory_names = {
            "0x1f7d7550b1b028f7571e69a784071f0205fd2efa": "Uniswap V3",
            "0x1ac9db4a2608ba45d6127b1737949b51bb54b7f3": "Slipstream",
            "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865": "Pancake V3",
            "0x0fbfcf9fa4f9c56b0f40a671ad40e0805a091865": "Pancake V3",
            "0xece6ecd61177336ea6fb9b17937ac439d85ee20b": "Giga V3",
        }
        venue = (
            "Uniswap V4"
            if protocol == "v4"
            else factory_names.get(factory, protocol.upper())
        )
        pair = f"{symbol0} / {symbol1}"
        yield (
            "pool", pool_id, pair, f"{venue} POOL · {pool_id}",
            f"/pool?id={cls._query_value(pool_id)}", 0,
            (protocol, venue, factory, row.get("address"), row.get("token0"),
             row.get("token1"), symbol0, symbol1, pair),
        )
        for address, symbol in (
            (row.get("token0"), row.get("symbol0")),
            (row.get("token1"), row.get("symbol1")),
        ):
            address = str(address or "").lower()
            if not address:
                continue
            token_label = str(symbol or address)
            yield (
                "token", address, token_label, f"TOKEN · {address}",
                f"/lp?q={cls._query_value(address)}", 1, (symbol, address),
            )
        protocol_id = venue.lower().replace(" ", "-")
        yield (
            "protocol", protocol_id, venue, f"INDEXED {protocol.upper()} PROTOCOL",
            f"/lp?protocol={cls._query_value(protocol)}", 3,
            (protocol, venue, factory),
        )

    @classmethod
    def _index_pool_search(
        cls, connection: sqlite3.Connection, row: Mapping[str, Any],
    ) -> None:
        cls._put_search_entities(connection, cls._pool_search_entities(row))

    @classmethod
    def _event_search_entities(
        cls, row: Mapping[str, Any],
    ) -> Iterator[tuple[str, str, str, str, str, int, Iterable[Any]]]:
        tx_hash = str(row.get("tx_hash") or "").lower()
        if tx_hash:
            yield (
                "transaction", tx_hash, tx_hash,
                f"TRANSACTION · BLOCK {int(row.get('block_number') or 0)}",
                f"https://robinscan.io/tx/{tx_hash}", 0, (tx_hash,),
            )
        for kind, value, rank in (
            ("owner", row.get("owner"), 4),
            ("custody", row.get("custody"), 5),
        ):
            address = str(value or "").lower()
            if address:
                yield (
                    kind, address, address,
                    ("LP OWNER" if kind == "owner" else "LP CUSTODY") +
                    " · indexed canonical activity",
                    f"/lp?owner={cls._query_value(address)}", rank, (address,),
                )
        position_key = str(row.get("position_key") or "").lower()
        if position_key:
            token_id = row.get("token_id")
            label = f"Position {token_id}" if token_id is not None else position_key
            protocol = str(row.get("protocol") or "").upper()
            yield (
                "position", position_key, label,
                f"{protocol} POSITION · {position_key}",
                f"/lp?q={cls._query_value(position_key)}", 2,
                (position_key, token_id, row.get("pool_id")),
            )

    @classmethod
    def _index_event_search(
        cls, connection: sqlite3.Connection, row: Mapping[str, Any],
    ) -> None:
        cls._put_search_entities(connection, cls._event_search_entities(row))

    @classmethod
    def _index_event_search_batch(
        cls, connection: sqlite3.Connection, rows: Iterable[Mapping[str, Any]],
    ) -> None:
        entities: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            tx_hash = str(row.get("tx_hash") or "").lower()
            if tx_hash:
                entities.setdefault(
                    ("transaction", tx_hash),
                    {
                        "tx_hash": tx_hash,
                        "block_number": row.get("block_number"),
                    },
                )
            for kind in ("owner", "custody"):
                address = str(row.get(kind) or "").lower()
                if address:
                    entities.setdefault(
                        (kind, address),
                        {
                            kind: address,
                            "block_number": row.get("block_number"),
                        },
                    )
            position_key = str(row.get("position_key") or "").lower()
            if position_key:
                entities.setdefault(
                    ("position", position_key),
                    {
                        "position_key": position_key,
                        "token_id": row.get("token_id"),
                        "pool_id": row.get("pool_id"),
                        "protocol": row.get("protocol"),
                    },
                )
        search_entities = (
            search_entity
            for key in sorted(entities)
            for search_entity in cls._event_search_entities(entities[key])
        )
        cls._put_search_entities(connection, search_entities)


    def ensure_search_index(self) -> None:
        """Build the typed search catalog once, then maintain it on writes."""
        with self.transaction() as connection:
            if int(self._metadata(connection, "search_index_version", 0)) >= 1:
                return
            self._rebuild_search_index(connection)
            self._set_metadata(connection, "search_index_version", 1)
            maximum = connection.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0]
            self._set_metadata(connection, "search_index_cursor", int(maximum))
            self._set_metadata(connection, "search_index_state", "ready")
    def search_index_status(self) -> dict[str, Any]:
        connection = self.read()
        version = int(self._metadata(connection, "search_index_version", 0))
        cursor = int(self._metadata(connection, "search_index_cursor", 0))
        total = int(connection.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0])
        phase = str(self._metadata(
            connection, "search_index_state", "ready" if version >= 1 else "warming",
        ))
        return {
            "state": "ready" if version >= 1 else "warming",
            "phase": phase,
            "ready": version >= 1,
            "indexed_through_event": cursor,
            "events_total": total,
        }

    def build_search_index(self, stop: threading.Event, *, batch_size: int = 1000) -> None:
        """Resume a bounded background catalog migration without blocking startup."""
        with self.transaction() as connection:
            if int(self._metadata(connection, "search_index_version", 0)) >= 1:
                return
            state = self._metadata(connection, "search_index_state", "warming")
            if state != "building":
                connection.execute("DELETE FROM lp_search_entities")
                self._set_metadata(connection, "search_index_cursor", 0)
                self._set_metadata(connection, "search_index_state", "building")
                for protocol, label in (
                    ("v2", "V2"), ("v3", "V3"), ("v4", "Uniswap V4"),
                ):
                    self._put_search_entity(
                        connection, kind="protocol", entity_id=protocol, label=label,
                        subtitle=f"INDEXED {protocol.upper()} PROTOCOL",
                        href=f"/lp?protocol={protocol}", rank=3,
                        terms=(protocol, label,
                               "uniswap" if protocol in {"v3", "v4"} else None),
                    )
                for row in connection.execute("SELECT * FROM pools"):
                    self._index_pool_search(connection, dict(row))
        bounded = max(100, min(int(batch_size), 5000))
        while not stop.is_set():
            complete = False
            with self.transaction() as connection:
                if int(self._metadata(connection, "search_index_version", 0)) >= 1:
                    return
                cursor = int(self._metadata(connection, "search_index_cursor", 0))
                rows = connection.execute(
                    "SELECT id,tx_hash,block_number,owner,custody,position_key,"
                    "token_id,pool_id,protocol FROM events WHERE id>? ORDER BY id LIMIT ?",
                    (cursor, bounded),
                ).fetchall()
                self._index_event_search_batch(
                    connection, (dict(row) for row in rows),
                )
                if rows:
                    self._set_metadata(connection, "search_index_cursor", int(rows[-1]["id"]))
                else:
                    self._set_metadata(connection, "search_index_version", 1)
                    self._set_metadata(connection, "search_index_state", "ready")
                    complete = True
            if complete:
                return
            stop.wait(0.01)

    @classmethod
    def _rebuild_search_index(cls, connection: sqlite3.Connection) -> None:
        connection.execute("DELETE FROM lp_search_entities")
        for protocol, label in (
            ("v2", "V2"), ("v3", "V3"), ("v4", "Uniswap V4"),
        ):
            cls._put_search_entity(
                connection, kind="protocol", entity_id=protocol, label=label,
                subtitle=f"INDEXED {protocol.upper()} PROTOCOL",
                href=f"/lp?protocol={protocol}", rank=3,
                terms=(protocol, label,
                       "uniswap" if protocol in {"v3", "v4"} else None),
            )
        for row in connection.execute("SELECT * FROM pools"):
            cls._index_pool_search(connection, dict(row))
        for row in connection.execute(
            "SELECT tx_hash,MIN(block_number) AS block_number FROM events GROUP BY tx_hash"
        ):
            cls._index_event_search(connection, dict(row))
        for column, kind in (("owner", "owner"), ("custody", "custody")):
            for row in connection.execute(
                f"SELECT {column},MIN(block_number) AS block_number FROM events "
                f"WHERE {column} IS NOT NULL GROUP BY {column}"
            ):
                cls._index_event_search(
                    connection,
                    {kind: row[column], "block_number": row["block_number"]},
                )
        for row in connection.execute(
            "SELECT position_key,MAX(token_id) AS token_id,MAX(pool_id) AS pool_id,"
            "MAX(protocol) AS protocol FROM events WHERE position_key IS NOT NULL "
            "GROUP BY position_key"
        ):
            cls._index_event_search(connection, dict(row))

    def catalog_search_status(self, expected_signature: str | None = None) -> dict[str, Any]:
        connection = self.read()
        signature = str(self._metadata(connection, "catalog_search_signature", ""))
        pending = str(self._metadata(connection, "catalog_search_pending_signature", ""))
        cursor = int(self._metadata(connection, "catalog_search_cursor", 0))
        indexed = int(connection.execute("SELECT COUNT(*) FROM lp_catalog_search").fetchone()[0])
        ready = bool(signature) and (expected_signature is None or signature == expected_signature)
        return {
            "state": "ready" if ready else "warming",
            "ready": ready,
            "signature": signature,
            "pending_signature": pending,
            "indexed_pools": indexed,
            "cursor": cursor,
        }

    def _upsert_catalog_batch(
        self, connection: sqlite3.Connection, pools: Sequence[Any], tokens: Mapping[str, Any],
    ) -> None:
        existing = {}
        ids = [str(pool.id).lower() for pool in pools]
        for batch in _batches(ids):
            placeholders = ",".join("?" for _ in batch)
            for row in connection.execute(
                "SELECT id,protocol,token0,token1,symbol0,symbol1,label,subtitle,href "
                f"FROM lp_catalog_search WHERE id IN ({placeholders})", batch,
            ):
                existing[row["id"]] = tuple(row)
        pool_rows = []
        token_rows: dict[str, str | None] = {}
        batch_counts: dict[str, int] = {}
        pair_counts: dict[tuple[str, str], int] = {}
        for pool in pools:
            token0 = str(pool.token0).lower()
            token1 = str(pool.token1).lower()
            meta0 = tokens.get(token0)
            meta1 = tokens.get(token1)
            symbol0 = str(getattr(meta0, "symbol", "") or "")[:32] or None
            symbol1 = str(getattr(meta1, "symbol", "") or "")[:32] or None
            label0 = symbol0 or token0[:10]
            label1 = symbol1 or token1[:10]
            pool_id = str(pool.id).lower()
            protocol = str(pool.kind).lower()
            row = (
                pool_id, protocol, token0, token1, symbol0, symbol1,
                f"{label0} / {label1}",
                f"{protocol.upper()} VERIFIED CATALOG POOL · {pool_id}",
                f"/pool?id={self._query_value(pool_id)}",
            )
            previous = existing.get(pool_id)
            if previous == row:
                continue
            if previous is not None:
                old_protocol = previous[1]
                batch_counts[old_protocol] = batch_counts.get(old_protocol, 0) - 1
                if previous[4] and previous[5]:
                    pair = (previous[4], previous[5])
                    pair_counts[pair] = pair_counts.get(pair, 0) - 1
            batch_counts[protocol] = batch_counts.get(protocol, 0) + 1
            if symbol0 and symbol1:
                pair = (symbol0, symbol1)
                pair_counts[pair] = pair_counts.get(pair, 0) + 1
            pool_rows.append(row)
            existing[pool_id] = row
            token_rows[token0] = symbol0 or token_rows.get(token0)
            token_rows[token1] = symbol1 or token_rows.get(token1)
        if not pool_rows:
            return
        connection.executemany(
            "INSERT OR REPLACE INTO lp_catalog_search "
            "(id,protocol,token0,token1,symbol0,symbol1,label,subtitle,href) "
            "VALUES(?,?,?,?,?,?,?,?,?)", pool_rows,
        )
        connection.executemany(
            "INSERT INTO lp_catalog_tokens(address,symbol) VALUES(?,?) "
            "ON CONFLICT(address) DO UPDATE SET symbol=COALESCE(excluded.symbol,symbol) "
            "WHERE excluded.symbol IS NOT NULL AND excluded.symbol IS NOT symbol",
            list(token_rows.items()),
        )
        connection.executemany(
            "INSERT INTO lp_catalog_pairs(symbol0,symbol1,pools) VALUES(?,?,?) "
            "ON CONFLICT(symbol0,symbol1) DO UPDATE SET pools=pools+excluded.pools",
            [(left, right, count) for (left, right), count in pair_counts.items() if count],
        )
        connection.executemany(
            "DELETE FROM lp_catalog_pairs WHERE symbol0=? AND symbol1=? AND pools<=0",
            [pair for pair, count in pair_counts.items() if count < 0],
        )
        if any(batch_counts.values()):
            protocol_counts = self._metadata(connection, "catalog_search_protocol_counts", {})
            if not isinstance(protocol_counts, dict):
                protocol_counts = {}
            for protocol, count in batch_counts.items():
                total = int(protocol_counts.get(protocol, 0)) + count
                if total:
                    protocol_counts[protocol] = total
                else:
                    protocol_counts.pop(protocol, None)
            self._set_metadata(connection, "catalog_search_protocol_counts", protocol_counts)

    def upsert_catalog_pools(
        self, pools: Sequence[Any], tokens: Mapping[str, Any],
    ) -> None:
        if not pools:
            return
        with self.transaction() as connection:
            self._upsert_catalog_batch(connection, pools, tokens)

    def build_catalog_search(
        self, stop: threading.Event, pools: Sequence[Any], tokens: Mapping[str, Any],
        signature: str, *, batch_size: int = 250,
    ) -> None:
        """Persist the verified workbench census incrementally, never as events."""
        signature = str(signature)
        bounded = max(100, min(int(batch_size), 1000))
        with self.transaction() as connection:
            current = str(self._metadata(connection, "catalog_search_signature", ""))
            pending = str(self._metadata(connection, "catalog_search_pending_signature", ""))
            if current == signature:
                return
            if pending != signature:
                connection.execute("DELETE FROM lp_catalog_search")
                connection.execute("DELETE FROM lp_catalog_tokens")
                connection.execute("DELETE FROM lp_catalog_pairs")
                self._set_metadata(connection, "catalog_search_cursor", 0)
                self._set_metadata(connection, "catalog_search_protocol_counts", {})
                self._set_metadata(connection, "catalog_search_pending_signature", signature)
                self._set_metadata(connection, "catalog_search_state", "building")
        while not stop.is_set():
            complete = False
            with self.transaction() as connection:
                if str(self._metadata(connection, "catalog_search_signature", "")) == signature:
                    return
                if str(self._metadata(connection, "catalog_search_pending_signature", "")) != signature:
                    return
                cursor = int(self._metadata(connection, "catalog_search_cursor", 0))
                batch = pools[cursor:cursor + bounded]
                self._upsert_catalog_batch(connection, batch, tokens)
                cursor += len(batch)
                self._set_metadata(connection, "catalog_search_cursor", cursor)
                if cursor >= len(pools):
                    self._set_metadata(connection, "catalog_search_signature", signature)
                    self._set_metadata(connection, "catalog_search_pending_signature", "")
                    self._set_metadata(connection, "catalog_search_state", "ready")
                    complete = True
            if complete:
                return
            stop.wait(0.02)

    def search_catalog(
        self, query: str, limit: int = 30,
    ) -> tuple[list[dict[str, Any]], int]:
        """Indexed prefix intersection over verified workbench census rows."""
        raw = str(query or "").strip().lower()[:128]
        words = list(dict.fromkeys(_SEARCH_WORD_RE.findall(raw)))[:8]
        if not words:
            return [], 0

        connection = self.read()
        bounded = max(1, min(int(limit), 30))
        pool_rows: list[sqlite3.Row] = []
        token_rows: list[sqlite3.Row] = []
        pool_count = token_count = 0

        def pool_query(where: str, args: Sequence[str]) -> None:
            nonlocal pool_count
            pool_count += int(connection.execute(
                "SELECT COUNT(*) FROM lp_catalog_search WHERE " + where, args,
            ).fetchone()[0])
            pool_rows.extend(connection.execute(
                "SELECT 'pool' AS kind,id,label,subtitle,href FROM lp_catalog_search "
                "WHERE " + where + " ORDER BY id LIMIT ?", [*args, bounded],
            ).fetchall())

        def token_query(where: str, args: Sequence[str]) -> None:
            nonlocal token_count
            token_count += int(connection.execute(
                "SELECT COUNT(*) FROM lp_catalog_tokens WHERE " + where, args,
            ).fetchone()[0])
            token_rows.extend(connection.execute(
                "SELECT 'token' AS kind,address AS id,COALESCE(symbol,address) AS label,"
                "'TOKEN · '||address AS subtitle,'/lp?q='||address AS href "
                "FROM lp_catalog_tokens WHERE " + where + " ORDER BY address LIMIT ?",
                [*args, bounded],
            ).fetchall())

        if len(words) == 1 and words[0] in {"v2", "v3", "v4"}:
            protocol = words[0]
            counts = self._metadata(connection, "catalog_search_protocol_counts", {})
            if isinstance(counts, Mapping) and protocol in counts:
                pool_count = int(counts[protocol])
                pool_rows.extend(connection.execute(
                    "SELECT 'pool' AS kind,id,label,subtitle,href FROM lp_catalog_search "
                    "WHERE protocol=? COLLATE NOCASE ORDER BY id LIMIT ?",
                    (protocol, bounded),
                ).fetchall())
            else:
                pool_query("protocol=? COLLATE NOCASE", (protocol,))
        elif all(word.startswith("0x") for word in words):
            word = words[0]
            upper = word + "\U0010ffff"
            for column in ("id", "token0", "token1"):
                pool_query(
                    f"{column}>=? COLLATE NOCASE AND {column}<? COLLATE NOCASE",
                    (word, upper),
                )
            token_query(
                "address>=? COLLATE NOCASE AND address<? COLLATE NOCASE",
                (word, upper),
            )
        elif len(words) == 1:
            word = words[0]
            upper = word + "\U0010ffff"
            for column in ("symbol0", "symbol1"):
                pool_query(
                    f"{column}>=? COLLATE NOCASE AND {column}<? COLLATE NOCASE",
                    (word, upper),
                )
            token_query(
                "symbol>=? COLLATE NOCASE AND symbol<? COLLATE NOCASE",
                (word, upper),
            )
        else:
            first, second = words[:2]
            first_upper, second_upper = first + "\U0010ffff", second + "\U0010ffff"
            for left, right in (("symbol0", "symbol1"), ("symbol1", "symbol0")):
                where = (
                    f"{left}>=? COLLATE NOCASE AND {left}<? COLLATE NOCASE AND "
                    f"{right}>=? COLLATE NOCASE AND {right}<? COLLATE NOCASE"
                )
                args = (first, first_upper, second, second_upper)
                pool_count += int(connection.execute(
                    "SELECT COALESCE(SUM(pools),0) FROM lp_catalog_pairs WHERE " + where,
                    args,
                ).fetchone()[0])
                pairs = connection.execute(
                    "SELECT symbol0,symbol1 FROM lp_catalog_pairs WHERE " + where
                    + " ORDER BY symbol0,symbol1 LIMIT ?", [*args, bounded],
                ).fetchall()
                for pair in pairs:
                    pool_rows.extend(connection.execute(
                        "SELECT 'pool' AS kind,id,label,subtitle,href "
                        "FROM lp_catalog_search WHERE symbol0=? COLLATE NOCASE "
                        "AND symbol1=? COLLATE NOCASE ORDER BY id LIMIT ?",
                        (pair["symbol0"], pair["symbol1"], bounded),
                    ).fetchall())

        rows = []
        seen = set()
        for row in (*pool_rows, *token_rows):
            value = dict(row)
            key = (value["kind"], value["id"])
            if key not in seen:
                seen.add(key)
                rows.append(value)
        rows.sort(key=lambda row: (
            str(row["id"]).lower() != raw,
            0 if row["kind"] == "pool" else 1,
            str(row["id"]),
        ))
        return rows[:bounded], pool_count + token_count

    def search(
        self, query: str, limit: int = 30,
    ) -> tuple[list[dict[str, Any]], int]:
        """Prefix-token intersection over the catalog; never scans accounting."""
        raw = str(query or "").strip().lower()[:128]
        tokens = list(dict.fromkeys(_SEARCH_WORD_RE.findall(raw)))[:8]
        if not tokens:
            return [], 0
        matches: list[str] = []
        args: list[Any] = []
        weights: list[str] = []
        for index, token in enumerate(tokens):
            alias = f"m{index}"
            match = (
                "(SELECT DISTINCT kind,id FROM lp_search_terms "
                "WHERE term>=? COLLATE NOCASE AND term<? COLLATE NOCASE) "
                + alias
            )
            matches.append(
                match if index == 0 else
                f"JOIN {match} ON {alias}.kind=m0.kind AND {alias}.id=m0.id"
            )
            args.extend((token, token + "\U0010ffff"))
            weights.append(
                "(SELECT MIN(weight) FROM lp_search_terms "
                "WHERE kind=m0.kind AND id=m0.id "
                "AND term>=? COLLATE NOCASE AND term<? COLLATE NOCASE)"
            )
        # Count matching identities from the term index, without fetching entity
        # rows or weights. Ranking reads weights from the existing entity index.
        base = " FROM " + " ".join(matches)
        connection = self.read()
        total = int(connection.execute("SELECT COUNT(*)" + base, args).fetchone()[0])
        bounded = max(1, min(int(limit), 30))
        exact_shape = raw if len(tokens) == 1 else ""
        label_shape = "".join(tokens)
        rows = connection.execute(
            "SELECT e.kind,e.id,e.label,e.subtitle,e.href" + base +
            " JOIN lp_search_entities e ON e.kind=m0.kind AND e.id=m0.id"
            " ORDER BY (e.id=?) DESC,"
            "(LOWER(REPLACE(REPLACE(REPLACE(e.label,' ',''),'/',''),'-',''))=?) DESC,"
            f"({'+'.join(weights)}) ASC,e.rank,e.kind,e.id LIMIT ?",
            [*args, exact_shape, label_shape, *args, bounded],
        ).fetchall()
        return [dict(row) for row in rows], total

    def update_status(self, **values: Any) -> None:
        """Atomically merge inexpensive runtime status maintained by the indexer."""
        with self.transaction() as connection:
            current = self._metadata(connection, "runtime_status", {})
            if not isinstance(current, dict):
                current = {}
            current.update(values)
            current["as_of"] = int(time.time())
            self._set_metadata(connection, "runtime_status", current)


    @staticmethod
    def _pool_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = {column: row.get(column) for column in POOL_COLUMNS}
        if not result["id"] or not result["protocol"] or not result["address"]:
            raise ValueError("pool requires id, protocol, and address")
        if not result["token0"] or not result["token1"]:
            raise ValueError("pool requires token0 and token1")
        for key in ("id", "protocol", "address", "token0", "token1", "hook", "factory"):
            result[key] = _lower(result[key])
        for key in ("decimals0", "decimals1", "fee_ppm", "tick_spacing", "created_block"):
            result[key] = _integer(result[key], key, nullable=True)
        for key in ("decimals0", "decimals1"):
            if result[key] is not None and not 0 <= result[key] <= 255:
                raise ValueError(f"pool {key} is outside uint8")
        if result["protocol"] not in {"v2", "v3", "v4"}:
            raise ValueError("pool protocol must be v2, v3, or v4")
        expected_id_length = 66 if result["protocol"] == "v4" else 42
        if len(result["id"]) != expected_id_length or not result["id"].startswith("0x"):
            raise ValueError("pool id has the wrong protocol-specific shape")
        for key in ("address", "token0", "token1"):
            value = result[key]
            if len(value) != 42 or not value.startswith("0x"):
                raise ValueError(f"pool {key} must be a 20-byte address")
        metadata = result["metadata_json"]
        if metadata is not None and not isinstance(metadata, str):
            metadata = _json(metadata)
        result["metadata_json"] = metadata
        return result

    def _upsert_pools(self, connection: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> int:
        normalized = [(raw, self._pool_row(raw)) for raw in rows]
        if not normalized:
            return 0
        assignments = ",".join(
            f"{column}=COALESCE(pools.{column},excluded.{column})"
            for column in POOL_COLUMNS
            if column not in {"id", "created_block", "metadata_json"}
        )
        sql = (
            f"INSERT INTO pools({','.join(POOL_COLUMNS)}) "
            f"VALUES({','.join('?' for _ in POOL_COLUMNS)}) "
            "ON CONFLICT(id) DO UPDATE SET "
            f"{assignments},metadata_json=COALESCE(excluded.metadata_json,pools.metadata_json),"
            "created_block=CASE "
            "WHEN pools.created_block IS NULL THEN excluded.created_block "
            "WHEN excluded.created_block IS NULL THEN pools.created_block "
            "ELSE MIN(pools.created_block,excluded.created_block) END"
        )
        pool_ids = list(dict.fromkeys(row["id"] for _raw, row in normalized))
        token_ids = list(dict.fromkeys(
            row[f"token{side}"] for _raw, row in normalized for side in (0, 1)
        ))
        existing_pools: dict[str, dict[str, Any]] = {}
        for batch in _batches(pool_ids):
            marks = ",".join("?" for _ in batch)
            existing_pools.update({
                str(row["id"]): dict(row)
                for row in connection.execute(
                    f"SELECT * FROM pools WHERE id IN ({marks})", batch,
                ).fetchall()
            })
        known_tokens: dict[str, Mapping[str, Any] | None] = {
            token: None for token in token_ids
        }
        for batch in _batches(token_ids):
            marks = ",".join("?" for _ in batch)
            for row in connection.execute(
                f"SELECT address,symbol,decimals FROM token_metadata "
                f"WHERE address IN ({marks})", batch,
            ).fetchall():
                known_tokens[str(row["address"])] = row
        provenance: dict[str, tuple[int, str, str]] = {}
        for batch in _batches(pool_ids):
            marks = ",".join("?" for _ in batch)
            provenance.update({
                str(row["pool_id"]): (
                    int(row["observed_block"]), str(row["observed_hash"]), str(row["basis"]),
                )
                for row in connection.execute(
                    f"SELECT pool_id,observed_block,observed_hash,basis "
                    f"FROM pool_provenance WHERE pool_id IN ({marks})", batch,
                ).fetchall()
            })
        pending_metadata: set[str] = set()
        for batch in _batches(token_ids):
            marks = ",".join("?" for _ in batch)
            pending_metadata.update(str(row[0]) for row in connection.execute(
                f"SELECT address FROM pending_token_metadata WHERE address IN ({marks})", batch,
            ).fetchall())
        pending_unpublish: set[str] = set()
        for batch in _batches(pool_ids):
            marks = ",".join("?" for _ in batch)
            pending_unpublish.update(str(row[0]) for row in connection.execute(
                f"SELECT pool_id FROM pending_pool_unpublish WHERE pool_id IN ({marks})", batch,
            ).fetchall())

        inserted = 0
        queued_metadata = 0
        resolved_metadata = 0
        resolved_unpublish = 0
        queued_reprojection = 0
        processed_tokens: set[str] = set()
        for raw, row in normalized:
            for side in (0, 1):
                known = known_tokens[row[f"token{side}"]]
                if known is not None:
                    row[f"symbol{side}"] = known["symbol"]
                    row[f"decimals{side}"] = known["decimals"]
            existing = existing_pools.get(row["id"])
            identity_activated = existing is None
            if existing is not None:
                for field in (
                    "protocol", "address", "token0", "token1", "fee_ppm",
                    "tick_spacing", "hook", "factory",
                ):
                    if (
                        existing[field] is not None
                        and row[field] is not None
                        and existing[field] != row[field]
                    ):
                        raise ValueError(
                            f"pool {row['id']} has conflicting {field}: "
                            f"{existing[field]} != {row[field]}"
                        )
                prior_metadata = _decode_json(existing["metadata_json"], {})
                incoming_metadata = _decode_json(row["metadata_json"], {})
                verified_bases = {
                    "factory_creation_event",
                    "verified_workbench_catalog",
                    "pinned_factory_getPair_membership",
                    "pinned_factory_getPool_membership",
                    "full_poolKey_hash",
                }
                identity_activated = (
                    (
                        existing["factory"] is None
                        and row["factory"] is not None
                    )
                    or (
                        (
                            not isinstance(prior_metadata, dict)
                            or prior_metadata.get("discovery_basis") not in verified_bases
                        )
                        and isinstance(incoming_metadata, dict)
                        and incoming_metadata.get("discovery_basis") in verified_bases
                    )
                )
                if isinstance(prior_metadata, dict) or isinstance(incoming_metadata, dict):
                    merged_metadata: dict[str, Any] = {}
                    if isinstance(incoming_metadata, dict):
                        merged_metadata.update(incoming_metadata)
                    if isinstance(prior_metadata, dict):
                        merged_metadata.update(prior_metadata)
                    if isinstance(incoming_metadata, dict):
                        for key in (
                            "discovery_basis", "identity_verified_block",
                            "identity_verified_hash", "pool_family",
                        ):
                            if key in incoming_metadata:
                                merged_metadata[key] = incoming_metadata[key]
                    row["metadata_json"] = _json(merged_metadata) if merged_metadata else None
            observed_block = raw.get("_observed_block")
            observed_hash = raw.get("_observed_hash")
            if observed_block is not None and observed_hash is not None:
                observed_block = _integer(observed_block, "pool observed block")
                observed_hash = str(observed_hash).lower()
                if len(observed_hash) != 66 or not observed_hash.startswith("0x"):
                    raise ValueError("pool observed hash must be a 32-byte hash")
                prior_observation = provenance.get(row["id"])
                if prior_observation is None or observed_block < prior_observation[0]:
                    basis = str(raw.get("_observation_basis") or "canonical_event")
                    connection.execute(
                        "INSERT INTO pool_provenance(pool_id,observed_block,observed_hash,basis) "
                        "VALUES(?,?,?,?) ON CONFLICT(pool_id) DO UPDATE SET "
                        "observed_block=excluded.observed_block,"
                        "observed_hash=excluded.observed_hash,basis=excluded.basis",
                        (row["id"], observed_block, observed_hash, basis),
                    )
                    provenance[row["id"]] = (observed_block, observed_hash, basis)

            final_pool = dict(row)
            if existing is not None:
                for column in POOL_COLUMNS:
                    if column == "id":
                        final_pool[column] = existing[column]
                    elif column == "created_block":
                        old, new = existing[column], row[column]
                        final_pool[column] = (
                            new if old is None else old if new is None else min(old, new)
                        )
                    elif column == "metadata_json":
                        final_pool[column] = (
                            row[column] if row[column] is not None else existing[column]
                        )
                    else:
                        final_pool[column] = (
                            existing[column] if existing[column] is not None else row[column]
                        )
            changed = existing is None or any(
                existing[column] != final_pool[column] for column in POOL_COLUMNS
            )
            if changed:
                connection.execute(sql, tuple(row[column] for column in POOL_COLUMNS))
                self._index_pool_search(connection, final_pool)
                self._mark_pool_metadata_changed()
            existing_pools[row["id"]] = final_pool
            if identity_activated:
                queued_reprojection += max(connection.execute(
                    "INSERT OR IGNORE INTO pending_reprojection"
                    "(event_id,block_number,tx_index,log_index,"
                    "attempts,next_attempt,last_error) "
                    "SELECT id,block_number,tx_index,log_index,0,0,NULL "
                    "FROM events WHERE pool_id=?",
                    (row["id"],),
                ).rowcount, 0)
            if row["id"] in pending_unpublish:
                resolved_unpublish += max(connection.execute(
                    "DELETE FROM pending_pool_unpublish WHERE pool_id=?", (row["id"],),
                ).rowcount, 0)
                pending_unpublish.discard(row["id"])
            for side in (0, 1):
                token = row[f"token{side}"]
                symbol = row[f"symbol{side}"]
                decimals = row[f"decimals{side}"]
                if token in processed_tokens:
                    continue
                known = known_tokens[token]
                if known is not None:
                    processed_tokens.add(token)
                    continue
                if symbol is not None and decimals is not None:
                    processed_tokens.add(token)
                    connection.execute(
                        "INSERT INTO token_metadata(address,symbol,decimals,updated_at) "
                        "VALUES(?,?,?,?)",
                        (token, str(symbol), int(decimals), time.time()),
                    )
                    known_tokens[token] = {
                        "address": token, "symbol": str(symbol), "decimals": int(decimals),
                    }
                    self._mark_pool_metadata_changed()
                    connection.execute(
                        "UPDATE pools SET symbol0=COALESCE(symbol0,?),"
                        "decimals0=COALESCE(decimals0,?) "
                        "WHERE token0=? AND (symbol0 IS NULL OR decimals0 IS NULL)",
                        (str(symbol), int(decimals), token),
                    )
                    connection.execute(
                        "UPDATE pools SET symbol1=COALESCE(symbol1,?),"
                        "decimals1=COALESCE(decimals1,?) "
                        "WHERE token1=? AND (symbol1 IS NULL OR decimals1 IS NULL)",
                        (str(symbol), int(decimals), token),
                    )
                    if token in pending_metadata:
                        resolved_metadata += max(connection.execute(
                            "DELETE FROM pending_token_metadata WHERE address=?", (token,),
                        ).rowcount, 0)
                        pending_metadata.discard(token)
                elif token not in pending_metadata:
                    queued_metadata += max(connection.execute(
                        "INSERT OR IGNORE INTO pending_token_metadata(address) VALUES(?)",
                        (token,),
                    ).rowcount, 0)
                    pending_metadata.add(token)
            if existing is None:
                inserted += 1
        if inserted:
            self._bump(connection, "indexed_pools", inserted)
        if queued_metadata:
            self._bump(connection, "pending_metadata", queued_metadata)
        if resolved_metadata:
            self._bump(connection, "pending_metadata", -resolved_metadata)
        if resolved_unpublish:
            self._bump(connection, "pending_pool_unpublish", -resolved_unpublish)
        if queued_reprojection:
            self._bump(connection, "pending_reprojection", queued_reprojection)
        return inserted

    def upsert_pools(
        self, rows: Iterable[Mapping[str, Any]], conn: sqlite3.Connection | None = None,
    ) -> int:
        materialized = list(rows)
        if conn is not None:
            return self._upsert_pools(conn, materialized)
        with self.transaction() as connection:
            inserted = self._upsert_pools(connection, materialized)
            if materialized:
                self._next_revision(connection)
            return inserted

    @staticmethod
    def _pool_from_event(event: Mapping[str, Any]) -> Mapping[str, Any] | None:
        pool = event.get("pool")
        if not isinstance(pool, Mapping):
            data = event.get("data")
            pool = data.get("pool") if isinstance(data, Mapping) else None
        if not isinstance(pool, Mapping):
            return None
        result = dict(pool)
        if event.get("block_number") is not None and event.get("block_hash") is not None:
            result["_observed_block"] = int(event["block_number"])
            result["_observed_hash"] = str(event["block_hash"]).lower()
            result["_observation_basis"] = str(
                result.get("source") or "canonical_event"
            )
        return result

    @classmethod
    def _pools_from_events(
        cls, events: Iterable[Mapping[str, Any]],
    ) -> list[Mapping[str, Any]]:
        pools: dict[str, Mapping[str, Any]] = {}
        for event in events:
            pool = cls._pool_from_event(event)
            if pool is not None:
                pools.setdefault(str(pool.get("id") or "").lower(), pool)
        return list(pools.values())

    @staticmethod
    def _event_data(event: Mapping[str, Any]) -> str | None:
        known = set(EVENT_COLUMNS) | {"id", "pool"}
        supplied = event.get("data")
        if isinstance(supplied, str):
            decoded = _decode_json(supplied, supplied)
        else:
            decoded = supplied
        extras = {key: value for key, value in event.items() if key not in known}
        if isinstance(decoded, Mapping):
            payload: Any = dict(decoded)
            payload.update(extras)
        elif extras:
            payload = {"value": decoded, **extras} if decoded is not None else extras
        else:
            payload = decoded
        return None if payload is None else _json(payload)

    @classmethod
    def _event_row(cls, event: Mapping[str, Any], revision: int) -> dict[str, Any]:
        for key in REQUIRED_EVENT_COLUMNS:
            if event.get(key) is None:
                raise ValueError(f"event requires {key}")
        row: dict[str, Any] = {}
        for column in EVENT_COLUMNS:
            value = revision if column == "revision" else event.get(column)
            if column in INTEGER_EVENT_COLUMNS:
                value = _integer(value, column, nullable=column not in REQUIRED_EVENT_COLUMNS)
            elif column in DECIMAL_EVENT_COLUMNS:
                value = _decimal_text(value, column)
            elif column in FLOAT_EVENT_COLUMNS:
                value = _finite_float(value, column)
            elif column in ADDRESS_EVENT_COLUMNS:
                value = _lower(value)
            elif column in {"block_hash", "tx_hash", "pool_id", "protocol"}:
                value = _lower(value)
            elif column == "token_id":
                value = None if value is None else str(value)
            elif column == "data":
                value = cls._event_data(event)
            row[column] = value
        if row["block_number"] < 0 or row["tx_index"] < 0 or row["log_index"] < 0:
            raise ValueError("event chain coordinates must be nonnegative")
        if len(row["block_hash"]) != 66 or not row["block_hash"].startswith("0x"):
            raise ValueError("event block_hash must be a 32-byte hash")
        if len(row["tx_hash"]) != 66 or not row["tx_hash"].startswith("0x"):
            raise ValueError("event tx_hash must be a 32-byte hash")
        if row["protocol"] not in {"v2", "v3", "v4", "nft"}:
            raise ValueError("event protocol is unsupported")
        if row["kind"] not in {
            "swap", "add", "remove", "collect", "checkpoint", "transfer",
            "create", "donate", "fee",
        }:
            raise ValueError("event kind is unsupported")
        if row["pool_id"] is not None:
            pool_protocol = row["protocol"]
            if pool_protocol == "nft":
                pool_protocol = _decode_json(row["data"], {}).get("manager_protocol")
                if pool_protocol not in {"v3", "v4"}:
                    raise ValueError("NFT pool identity requires its verified manager protocol")
            expected_pool_length = 66 if pool_protocol == "v4" else 42
            if len(row["pool_id"]) != expected_pool_length or not row["pool_id"].startswith("0x"):
                raise ValueError("event pool_id has the wrong protocol-specific shape")
        for field in ADDRESS_EVENT_COLUMNS:
            if row[field] is not None and (
                len(row[field]) != 42 or not row[field].startswith("0x")
            ):
                raise ValueError(f"event {field} must be a 20-byte address")
        return row

    @staticmethod
    def _transaction_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = {column: row.get(column) for column in TRANSACTION_COLUMNS}
        for required in ("tx_hash", "block_number", "block_hash"):
            if result[required] is None:
                raise ValueError(f"transaction requires {required}")
        result["tx_hash"] = _lower(result["tx_hash"])
        result["block_hash"] = _lower(result["block_hash"])
        result["payer"] = _lower(result["payer"])
        result["block_number"] = _integer(result["block_number"], "block_number")
        result["status"] = _integer(result["status"], "status", nullable=True)
        for name in ("gas_used", "gas_price", "gas_native"):
            result[name] = _decimal_text(result[name], name)
        result["gas_usd"] = _finite_float(result["gas_usd"], "gas_usd")
        if result["data"] is not None and not isinstance(result["data"], str):
            result["data"] = _json(result["data"])
        if len(result["tx_hash"]) != 66 or not result["tx_hash"].startswith("0x"):
            raise ValueError("transaction tx_hash must be a 32-byte hash")
        if len(result["block_hash"]) != 66 or not result["block_hash"].startswith("0x"):
            raise ValueError("transaction block_hash must be a 32-byte hash")
        return result

    def _upsert_transactions(
        self, connection: sqlite3.Connection, rows: Iterable[Mapping[str, Any]],
    ) -> int:
        inserted = 0
        assignments = ",".join(
            f"{column}=COALESCE(excluded.{column},transactions.{column})"
            for column in TRANSACTION_COLUMNS if column != "tx_hash"
        )
        sql = (
            f"INSERT INTO transactions({','.join(TRANSACTION_COLUMNS)}) "
            f"VALUES({','.join('?' for _ in TRANSACTION_COLUMNS)}) "
            f"ON CONFLICT(tx_hash) DO UPDATE SET {assignments}"
        )
        for raw in rows:
            row = self._transaction_row(raw)
            canonical = connection.execute(
                "SELECT hash FROM blocks WHERE number=?", (row["block_number"],),
            ).fetchone()
            if canonical is None or canonical[0] != row["block_hash"]:
                raise CanonicalConflict(f"transaction {row['tx_hash']} is not in a canonical stored block")
            existed = connection.execute(
                "SELECT 1 FROM transactions WHERE tx_hash=?", (row["tx_hash"],),
            ).fetchone()
            connection.execute(sql, tuple(row[column] for column in TRANSACTION_COLUMNS))
            self._index_event_search(connection, row)
            if existed is None:
                inserted += 1
        if inserted:
            self._bump(connection, "indexed_transactions", inserted)
        return inserted

    def _store_headers(
        self, connection: sqlite3.Connection, headers: Iterable[Mapping[str, Any]],
    ) -> dict[int, dict[str, Any]]:
        normalized: dict[int, dict[str, Any]] = {}
        for raw in headers:
            number = _header_number(raw)
            header = {
                "number": number,
                "hash": str(raw["hash"]).lower(),
                "parent_hash": str(raw.get("parentHash", raw.get("parent_hash"))).lower(),
                "timestamp": _header_timestamp(raw),
            }
            if (
                number < 0
                or header["timestamp"] < 0
                or len(header["hash"]) != 66
                or not header["hash"].startswith("0x")
                or len(header["parent_hash"]) != 66
                or not header["parent_hash"].startswith("0x")
            ):
                raise ValueError("malformed block header")
            prior = normalized.get(number)
            if prior is not None and prior != header:
                raise CanonicalConflict(f"conflicting supplied headers for block {number}")
            normalized[number] = header
        ordered_numbers = sorted(normalized)
        for previous_number, number in zip(ordered_numbers, ordered_numbers[1:]):
            if (
                number == previous_number + 1
                and normalized[number]["parent_hash"] != normalized[previous_number]["hash"]
            ):
                raise CanonicalConflict(
                    f"block {number} does not extend supplied block {previous_number}"
                )
        existing_headers: dict[int, sqlite3.Row] = {}
        for batch in _batches(ordered_numbers):
            marks = ",".join("?" for _ in batch)
            existing_headers.update({
                int(row["number"]): row
                for row in connection.execute(
                    f"SELECT number,hash,parent_hash,timestamp FROM blocks "
                    f"WHERE number IN ({marks})", batch,
                ).fetchall()
            })
        pending: list[tuple[Any, ...]] = []
        for number in ordered_numbers:
            header = normalized[number]
            existing = existing_headers.get(number)
            if existing is not None and (
                existing["hash"] != header["hash"]
                or existing["parent_hash"] != header["parent_hash"]
                or int(existing["timestamp"]) != header["timestamp"]
            ):
                raise CanonicalConflict(
                    f"block {number} conflicts with its stored canonical header"
                )
            if existing is None:
                pending.append((
                    number, header["hash"], header["parent_hash"], header["timestamp"],
                ))
        _insert_rows(
            connection,
            "INSERT INTO blocks(number,hash,parent_hash,timestamp)",
            pending, columns=4,
        )
        return normalized

    def _merge_coverage(
        self,
        connection: sqlite3.Connection,
        lane: str,
        start: int,
        end: int,
        start_hash: str,
        end_hash: str,
    ) -> None:
        overlaps = connection.execute(
            "SELECT start_block,end_block,start_hash,end_hash FROM coverage_intervals "
            "WHERE lane=? AND start_block<=? AND end_block>=? ORDER BY start_block",
            (lane, end + 1, start - 1),
        ).fetchall()
        merged_start, merged_end = start, end
        merged_start_hash, merged_end_hash = start_hash, end_hash
        for row in overlaps:
            if row["start_block"] < merged_start:
                merged_start, merged_start_hash = row["start_block"], row["start_hash"]
            if row["end_block"] > merged_end:
                merged_end, merged_end_hash = row["end_block"], row["end_hash"]
        if overlaps:
            connection.executemany(
                "DELETE FROM coverage_intervals WHERE lane=? AND start_block=? AND end_block=?",
                [(lane, row["start_block"], row["end_block"]) for row in overlaps],
            )
        connection.execute(
            "INSERT INTO coverage_intervals(lane,start_block,end_block,start_hash,end_hash) "
            "VALUES(?,?,?,?,?)",
            (lane, merged_start, merged_end, merged_start_hash, merged_end_hash),
        )

    def _queue_enrichment(
        self, connection: sqlite3.Connection, events: Iterable[Mapping[str, Any]],
    ) -> None:
        now = time.time()
        seen: set[str] = set()
        rows: list[tuple[Any, ...]] = []
        for event in events:
            if event.get("kind") not in LP_ENRICHMENT_KINDS:
                continue
            tx_hash = str(event["tx_hash"]).lower()
            if tx_hash in seen:
                continue
            seen.add(tx_hash)
            rows.append((
                tx_hash, int(event["block_number"]),
                str(event["block_hash"]).lower(),
                0, 0.0, None, now, now,
            ))
        if not rows:
            return
        before = connection.total_changes
        connection.executemany(
            "INSERT OR IGNORE INTO pending_enrichment"
            "(tx_hash,block_number,block_hash,attempts,next_attempt,last_error,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            rows,
        )
        inserted = connection.total_changes - before
        if inserted:
            self._bump(connection, "pending_enrichment", inserted)

    def _persist_projection_mutations(
        self, connection: sqlite3.Connection,
        events: list[dict[str, Any]], revision: int,
    ) -> list[dict[str, Any]]:
        if events:
            self._set_metadata(connection, "events_revision", revision)
        assignments = ",".join(f"{column}=?" for column in EVENT_COLUMNS)
        rows = [self._event_row(event, revision) for event in events]
        connection.executemany(
            f"UPDATE events SET {assignments} WHERE id=?",
            (
                tuple(row[column] for column in EVENT_COLUMNS)
                + (int(event["id"]),)
                for event, row in zip(events, rows)
            ),
        )
        return rows

    def ingest(
        self,
        headers: Iterable[Mapping[str, Any]] | Mapping[int, Mapping[str, Any]],
        events: Iterable[Mapping[str, Any]],
        *,
        lane: str = "live",
        cursor: Mapping[str, Any] | None = None,
        transactions: Iterable[Mapping[str, Any]] = (),
    ) -> list[dict[str, Any]]:
        """Commit an entire fetched interval, cursor, events, and projections."""
        if not lane:
            raise ValueError("lane must be non-empty")
        supplied_headers = list(headers.values()) if isinstance(headers, Mapping) else list(headers)
        supplied_events = [dict(event) for event in events]
        supplied_transactions = list(transactions)
        with self.transaction() as connection:
            if cursor is not None:
                prior_cursor = self._metadata(connection, f"cursor:{lane}", None)
                expected_number = cursor.get("_expected_block_number")
                expected_hash = cursor.get("_expected_block_hash")
                expected_next_to = cursor.get("_expected_next_to")
                expected_epoch = cursor.get("_expected_epoch")
                if (
                    expected_epoch is not None
                    and int(self._metadata(connection, "epoch", 0)) != int(expected_epoch)
                ):
                    raise CanonicalConflict(f"{lane} epoch moved while an interval was fetched")
                if (
                    expected_number is not None
                    and (
                        not isinstance(prior_cursor, Mapping)
                        or int(prior_cursor.get("block_number", -1)) != int(expected_number)
                    )
                ):
                    raise CanonicalConflict(f"{lane} cursor moved while an interval was fetched")
                if (
                    expected_hash is not None
                    and (
                        not isinstance(prior_cursor, Mapping)
                        or str(prior_cursor.get("block_hash") or "").lower()
                        != str(expected_hash).lower()
                    )
                ):
                    raise CanonicalConflict(f"{lane} cursor hash moved while an interval was fetched")
                if (
                    expected_next_to is not None
                    and (
                        not isinstance(prior_cursor, Mapping)
                        or int(prior_cursor.get("next_to", -1)) != int(expected_next_to)
                    )
                ):
                    raise CanonicalConflict(f"{lane} history cursor moved while an interval was fetched")
            normalized_headers = self._store_headers(connection, supplied_headers)
            if cursor:
                start = _integer(cursor.get("from_block", cursor.get("start_block")), "from_block", nullable=True)
                end = _integer(cursor.get("to_block", cursor.get("end_block", cursor.get("block_number"))), "to_block", nullable=True)
                if start is not None and end is not None:
                    if start > end:
                        raise ValueError("cursor coverage interval is reversed")
                    if start not in normalized_headers or end not in normalized_headers:
                        raise ValueError("coverage cursor requires both boundary headers")
                    self._merge_coverage(
                        connection, lane, start, end,
                        normalized_headers[start]["hash"], normalized_headers[end]["hash"],
                    )
            revision = self._next_revision(connection)
            pools = self._pools_from_events(supplied_events)
            if pools:
                self._upsert_pools(connection, pools)
            inserted_events: list[dict[str, Any]] = []
            inserted_rows: list[dict[str, Any]] = []
            insert_sql = f"INSERT INTO events({','.join(EVENT_COLUMNS)})"
            prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
            stored_blocks: dict[int, sqlite3.Row | None] = {}
            for event in supplied_events:
                row = self._event_row(event, revision)
                block = normalized_headers.get(row["block_number"])
                if block is None:
                    block_number = int(row["block_number"])
                    if block_number not in stored_blocks:
                        stored_blocks[block_number] = connection.execute(
                            "SELECT hash,timestamp FROM blocks WHERE number=?", (block_number,),
                        ).fetchone()
                    stored = stored_blocks[block_number]
                    if stored is None:
                        raise ValueError(f"event block {row['block_number']} has no supplied header")
                    block_hash, timestamp = stored["hash"], stored["timestamp"]
                else:
                    block_hash, timestamp = block["hash"], block["timestamp"]
                if row["block_hash"] != block_hash or row["timestamp"] != timestamp:
                    raise CanonicalConflict("event block hash/timestamp does not match stored header")
                prepared.append((event, row))
            before = connection.total_changes
            _insert_rows(
                connection, insert_sql,
                (tuple(row[column] for column in EVENT_COLUMNS) for _event, row in prepared),
                columns=len(EVENT_COLUMNS),
                suffix=" ON CONFLICT(block_hash,tx_hash,log_index) DO NOTHING",
            )
            inserted_count = connection.total_changes - before
            if inserted_count:
                inserted_ids = {
                    (str(row["block_hash"]), str(row["tx_hash"]), int(row["log_index"])): int(row["id"])
                    for row in connection.execute(
                        "SELECT id,block_hash,tx_hash,log_index FROM events WHERE revision=?",
                        (revision,),
                    ).fetchall()
                }
                for event, row in prepared:
                    identity = (
                        str(row["block_hash"]), str(row["tx_hash"]), int(row["log_index"]),
                    )
                    event_id = inserted_ids.pop(identity, None)
                    if event_id is None:
                        continue
                    event["id"] = event_id
                    event["revision"] = revision
                    inserted_events.append(event)
                    inserted_rows.append(row)
            inserted_events.sort(key=lambda event: (
                int(event["block_number"]), int(event["tx_index"]),
                int(event["log_index"]), int(event["id"]),
            ))
            if inserted_events:
                self._bump(connection, "indexed_events", len(inserted_events))
                self._set_metadata(connection, "events_revision", revision)
                self._queue_enrichment(connection, inserted_events)
                if lane == "live":
                    self._set_metadata(connection, "live_revision", revision)
            self._upsert_transactions(connection, supplied_transactions)
            if cursor is not None:
                stored_cursor = {
                    key: value for key, value in cursor.items()
                    if not str(key).startswith("_expected_")
                }
                stored_cursor["lane"] = lane
                self._set_metadata(connection, f"cursor:{lane}", stored_cursor)
            search_rows = inserted_rows
            if inserted_events:
                for apply, _rollback, _persists_events in self._projections:
                    apply(connection, inserted_events)
                if any(not item[2] for item in self._projections):
                    search_rows = self._persist_projection_mutations(
                        connection, inserted_events, revision,
                    )
                else:
                    search_rows = [
                        self._event_row(event, revision) for event in inserted_events
                    ]
            self._index_event_search_batch(connection, search_rows)
            return inserted_events

    def _current_event(self, connection: sqlite3.Connection, event: Mapping[str, Any]) -> dict[str, Any] | None:
        if event.get("id") is not None:
            row = connection.execute("SELECT * FROM events WHERE id=?", (int(event["id"]),)).fetchone()
        else:
            row = connection.execute(
                "SELECT * FROM events WHERE block_hash=? AND tx_hash=? AND log_index=?",
                (
                    str(event["block_hash"]).lower(), str(event["tx_hash"]).lower(),
                    _integer(event["log_index"], "log_index"),
                ),
            ).fetchone()
        if row is None:
            if event.get("id") is not None:
                raise CanonicalConflict("enrichment target is no longer canonical")
            return None
        current = dict(row)
        current["data"] = _decode_json(current.get("data"), current.get("data"))
        return current

    @staticmethod
    def _merge_event(current: dict[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
        merged = dict(current)
        for key, value in update.items():
            if key == "data" and isinstance(value, Mapping):
                prior = merged.get("data")
                combined = dict(prior) if isinstance(prior, Mapping) else {}
                combined.update(value)
                merged["data"] = combined
            elif key in EVENT_COLUMNS or key in {"id", "pool"}:
                merged[key] = value
        return merged

    def enrich(
        self,
        events: Iterable[Mapping[str, Any]],
        transactions: Iterable[Mapping[str, Any]] = (),
    ) -> list[dict[str, Any]]:
        """Merge receipt-discovered canonical rows and projections atomically."""
        updates = list(events)
        transaction_rows = list(transactions)
        if not updates and not transaction_rows:
            return []
        receipts = {
            str(row["tx_hash"]).lower(): (
                _integer(row["block_number"], "block_number"), str(row["block_hash"]).lower(),
            )
            for row in transaction_rows
        }
        with self.transaction() as connection:
            revision = self._next_revision(connection)
            enriched: list[dict[str, Any]] = []
            inserted = 0
            insert_sql = None
            for update in updates:
                current = self._current_event(connection, update)
                merged = self._merge_event(current, update) if current is not None else dict(update)
                row = self._event_row(merged, revision)
                if current is not None and any(
                    current[field] != row[field]
                    for field in ("block_number", "block_hash", "tx_hash", "tx_index", "log_index", "timestamp")
                ):
                    raise CanonicalConflict("enrichment changed canonical event identity")
                if current is None and receipts.get(row["tx_hash"]) != (row["block_number"], row["block_hash"]):
                    raise CanonicalConflict("new enrichment event lacks its canonical transaction receipt")
                canonical = connection.execute(
                    "SELECT hash,timestamp FROM blocks WHERE number=?", (row["block_number"],),
                ).fetchone()
                if canonical is None or canonical["hash"] != row["block_hash"] or canonical["timestamp"] != row["timestamp"]:
                    raise CanonicalConflict("enrichment belongs to an orphaned block")
                if current is None:
                    if insert_sql is None:
                        insert_sql = (
                            f"INSERT INTO events({','.join(EVENT_COLUMNS)}) "
                            f"VALUES({','.join('?' for _ in EVENT_COLUMNS)})"
                        )
                    result = connection.execute(insert_sql, tuple(row[column] for column in EVENT_COLUMNS))
                    merged["id"] = result.lastrowid
                    inserted += 1
                merged["revision"] = revision
                enriched.append(merged)
            if inserted:
                self._bump(connection, "indexed_events", inserted)
            represented_ids = {int(event["id"]) for event in enriched}
            transaction_hashes = {
                str(row["tx_hash"]).lower()
                for row in transaction_rows
                if row.get("tx_hash") is not None
            }
            for tx_hash in sorted(transaction_hashes):
                rows = connection.execute(
                    "SELECT * FROM events WHERE tx_hash=? "
                    "ORDER BY block_number,tx_index,log_index,id",
                    (tx_hash,),
                ).fetchall()
                for row in rows:
                    if int(row["id"]) in represented_ids:
                        continue
                    current = dict(row)
                    current["data"] = _decode_json(
                        current.get("data"), current.get("data"),
                    )
                    current["revision"] = revision
                    enriched.append(current)
                    represented_ids.add(int(current["id"]))
            enriched.sort(key=lambda event: (
                int(event["block_number"]), int(event["tx_index"]),
                int(event["log_index"]), int(event["id"]),
            ))
            pools = self._pools_from_events(enriched)
            if pools:
                self._upsert_pools(connection, pools)
            search_rows: list[dict[str, Any]] = []
            if enriched:
                search_rows = self._persist_projection_mutations(
                    connection, enriched, revision,
                )
            self._upsert_transactions(connection, transaction_rows)
            if enriched:
                for apply, _rollback, _persists_events in self._projections:
                    apply(connection, enriched)
                if any(not item[2] for item in self._projections):
                    search_rows = self._persist_projection_mutations(
                        connection, enriched, revision,
                    )
                else:
                    search_rows = [self._event_row(event, revision) for event in enriched]
            self._index_event_search_batch(connection, search_rows)
            completed = {
                str(row["tx_hash"]).lower() for row in transaction_rows if row.get("tx_hash")
            }
            completed.update(str(event["tx_hash"]).lower() for event in enriched)
            if completed:
                placeholders = ",".join("?" for _ in completed)
                before = connection.total_changes
                connection.execute(
                    f"DELETE FROM pending_enrichment WHERE tx_hash IN ({placeholders})",
                    tuple(completed),
                )
                removed = connection.total_changes - before
                if removed:
                    self._bump(connection, "pending_enrichment", -removed)
            return enriched

    def pending_enrichments(
        self, limit: int = 16, *, now: float | None = None,
    ) -> list[dict[str, Any]]:
        # Reserve historical progress without making current financials wait
        # for the entire backfill. Identity discovery has its own worker.
        limit = max(1, min(int(limit), 256))
        historical = max(1, limit // 4)
        due = time.time() if now is None else now
        rows = self.read().execute(
            "WITH oldest AS ("
            "SELECT block_number,tx_hash FROM pending_enrichment "
            "INDEXED BY pending_enrichment_financial_order_idx "
            "WHERE next_attempt<=? AND (last_error IS NULL "
            "OR last_error NOT GLOB 'pool_identity_pending:*') "
            "ORDER BY block_number,tx_hash LIMIT ?"
            "),newest AS ("
            "SELECT block_number,tx_hash FROM pending_enrichment "
            "INDEXED BY pending_enrichment_financial_order_idx "
            "WHERE next_attempt<=? AND (last_error IS NULL "
            "OR last_error NOT GLOB 'pool_identity_pending:*') "
            "ORDER BY block_number DESC,tx_hash DESC LIMIT ?"
            "),selected AS (SELECT * FROM oldest UNION SELECT * FROM newest) "
            "SELECT p.* FROM selected s JOIN pending_enrichment p "
            "ON p.tx_hash=s.tx_hash ORDER BY s.block_number,s.tx_hash",
            (due, historical, due, limit - historical),
        ).fetchall()
        return [dict(row) for row in rows]

    def pending_token_metadata(self, limit: int = 16) -> list[dict[str, Any]]:
        rows = self.read().execute(
            "SELECT * FROM pending_token_metadata WHERE next_attempt<=? "
            "ORDER BY address LIMIT ?",
            (time.time(), max(1, min(int(limit), 128))),
        ).fetchall()
        return [dict(row) for row in rows]

    def save_token_metadata(
        self, address: str, symbol: str, decimals: int,
    ) -> dict[str, Any] | None:
        address = address.lower()
        symbol = str(symbol).strip()
        decimals = int(decimals)
        if len(address) != 42 or not address.startswith("0x"):
            raise ValueError("token address must be a 20-byte address")
        if not symbol or len(symbol) > 256 or not 0 <= decimals <= 255:
            raise ValueError("token symbol/decimals are invalid")
        with self.transaction() as connection:
            prior = connection.execute(
                "SELECT symbol,decimals FROM token_metadata WHERE address=?",
                (address,),
            ).fetchone()
            token_changed = (
                prior is None
                or str(prior["symbol"]) != symbol
                or int(prior["decimals"]) != decimals
            )
            connection.execute(
                "INSERT INTO token_metadata(address,symbol,decimals,updated_at) "
                "VALUES(?,?,?,?) ON CONFLICT(address) DO UPDATE SET "
                "symbol=excluded.symbol,decimals=excluded.decimals,"
                "updated_at=excluded.updated_at WHERE "
                "token_metadata.symbol<>excluded.symbol OR "
                "token_metadata.decimals<>excluded.decimals",
                (address, symbol, decimals, time.time()),
            )
            pool_updates = connection.execute(
                "UPDATE pools SET symbol0=COALESCE(symbol0,?),"
                "decimals0=COALESCE(decimals0,?) "
                "WHERE token0=? AND (symbol0 IS NULL OR decimals0 IS NULL)",
                (symbol, decimals, address),
            ).rowcount
            pool_updates += connection.execute(
                "UPDATE pools SET symbol1=COALESCE(symbol1,?),"
                "decimals1=COALESCE(decimals1,?) "
                "WHERE token1=? AND (symbol1 IS NULL OR decimals1 IS NULL)",
                (symbol, decimals, address),
            ).rowcount
            if token_changed or pool_updates:
                self._mark_pool_metadata_changed()
            queued = connection.execute(
                "INSERT OR IGNORE INTO pending_reprojection"
                "(event_id,block_number,tx_index,log_index,"
                "attempts,next_attempt,last_error) "
                "SELECT e.id,e.block_number,e.tx_index,e.log_index,0,0,NULL "
                "FROM events e JOIN pools p ON p.id=e.pool_id "
                "WHERE p.token0=? OR p.token1=?",
                (address, address),
            ).rowcount
            if queued:
                self._bump(connection, "pending_reprojection", queued)
            removed = connection.execute(
                "DELETE FROM pending_token_metadata WHERE address=?", (address,),
            ).rowcount
            if removed:
                self._bump(connection, "pending_metadata", -removed)
            self._next_revision(connection)
            row = connection.execute(
                "SELECT * FROM pools WHERE token0=? OR token1=? ORDER BY id LIMIT 1",
                (address, address),
            ).fetchone()
            return dict(row) if row is not None else None

    def mark_token_metadata_error(self, address: str, error: str, *, delay: float) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE pending_token_metadata SET attempts=attempts+1,"
                "next_attempt=?,last_error=? WHERE address=?",
                (time.time() + max(0.0, delay), str(error)[:1000], address.lower()),
            )

    def pending_reprojections(self, limit: int = 128) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 512))
        historical = max(1, limit // 4)
        due = time.time()
        rows = self.read().execute(
            "WITH oldest AS ("
            "SELECT event_id FROM pending_reprojection "
            "INDEXED BY pending_reprojection_order_idx WHERE next_attempt<=? "
            "ORDER BY block_number,tx_index,log_index,event_id LIMIT ?"
            "),newest AS ("
            "SELECT event_id FROM pending_reprojection "
            "INDEXED BY pending_reprojection_order_idx WHERE next_attempt<=? "
            "ORDER BY block_number DESC,tx_index DESC,log_index DESC,event_id DESC LIMIT ?"
            "),selected AS (SELECT * FROM oldest UNION SELECT * FROM newest) "
            "SELECT e.*,q.attempts AS reprojection_attempts,q.next_attempt "
            "FROM selected s JOIN pending_reprojection q ON q.event_id=s.event_id "
            "JOIN events e ON e.id=q.event_id "
            "ORDER BY q.block_number,q.tx_index,q.log_index,q.event_id",
            (due, historical, due, limit - historical),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            event = dict(row)
            event["data"] = _decode_json(event.get("data"), event.get("data"))
            result.append(event)
        return result

    def reproject(self, event_ids: Iterable[int]) -> list[dict[str, Any]]:
        values = tuple(dict.fromkeys(int(event_id) for event_id in event_ids))
        if not values:
            return []
        with self.transaction() as connection:
            placeholders = ",".join("?" for _ in values)
            rows = connection.execute(
                f"SELECT * FROM events WHERE id IN ({placeholders}) "
                "ORDER BY block_number,tx_index,log_index,id",
                values,
            ).fetchall()
            revision = self._next_revision(connection)
            events = [dict(row) for row in rows]
            for event in events:
                event["data"] = _decode_json(event.get("data"), event.get("data"))
                event["revision"] = revision
            search_rows: list[dict[str, Any]] = []
            if events:
                search_rows = self._persist_projection_mutations(
                    connection, events, revision,
                )
                for apply, _rollback, _persists_events in self._projections:
                    apply(connection, events)
                if any(not item[2] for item in self._projections):
                    search_rows = self._persist_projection_mutations(
                        connection, events, revision,
                    )
                else:
                    search_rows = [self._event_row(event, revision) for event in events]
            self._index_event_search_batch(connection, search_rows)
            removed = connection.execute(
                f"DELETE FROM pending_reprojection WHERE event_id IN ({placeholders})",
                values,
            ).rowcount
            if removed:
                self._bump(connection, "pending_reprojection", -removed)
            return events

    def repair_v3_birth_history(
        self, prefixes: Sequence[str], *, limit: int = 32,
    ) -> bool:
        """Repair indexed births newest-first with bounded reads and writes."""
        reader = self.read()
        checkpoint = self._metadata(reader, "v3_birth_history_repair_v2", {})
        if checkpoint.get("complete"):
            return False
        if reader.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='lp_accounting_positions'"
        ).fetchone() is None:
            return False
        repair_limit = max(1, min(int(limit), 128))
        after = int(checkpoint.get("after_event_id", (1 << 63) - 1))
        page = reader.execute(
            "SELECT MIN(id) AS first_id,COUNT(*) AS count FROM "
            "(SELECT id FROM events INDEXED BY events_kind_id_idx "
            "WHERE kind='add' AND id<=? ORDER BY id DESC LIMIT ?)",
            (after, max(512, repair_limit * 256)),
        ).fetchone()
        if page["first_id"] is None:
            with self.transaction() as connection:
                self._set_metadata(
                    connection, "v3_birth_history_repair_v2",
                    {**checkpoint, "complete": True},
                )
                connection.execute(
                    "DELETE FROM metadata WHERE key='v3_birth_history_repair_v1'",
                )
            return False
        prefix_clause = " OR ".join("ka.position_key GLOB ?" for _ in prefixes) or "0"
        repairs = reader.execute(
            "SELECT MAX(a.id) AS event_id FROM events a "
            "INDEXED BY events_kind_id_idx "
            "JOIN lp_accounting_event_keys ka ON ka.event_id=a.id "
            "WHERE a.kind='add' AND a.id>=? AND a.id<=? "
            f"AND ({prefix_clause}) "
            "AND EXISTS(SELECT 1 FROM lp_accounting_episodes ep "
            "WHERE ep.position_key=ka.position_key AND ep.protocol='v3' "
            "AND ep.history_complete=0) "
            "AND EXISTS(SELECT 1 FROM lp_accounting_event_keys kt "
            "JOIN events t ON t.id=kt.event_id "
            "WHERE kt.position_key=ka.position_key AND t.block_hash=a.block_hash "
            "AND t.tx_hash=a.tx_hash AND t.kind='transfer' "
            "AND t.log_index>a.log_index AND json_extract(t.data,'$.mint')=1) "
            "GROUP BY ka.position_key ORDER BY event_id DESC LIMIT ?",
            (page["first_id"], after, *(prefix + "*" for prefix in prefixes), repair_limit),
        ).fetchall()
        next_after = (
            int(repairs[-1]["event_id"])
            if len(repairs) == repair_limit else int(page["first_id"])
        ) - 1
        with self.transaction() as connection:
            self.reproject(int(row["event_id"]) for row in repairs)
            self._set_metadata(connection, "v3_birth_history_repair_v2", {
                "after_event_id": next_after,
                "scanned_events": int(checkpoint.get("scanned_events", 0)) + int(page["count"]),
                "repaired_positions": int(checkpoint.get("repaired_positions", 0)) + len(repairs),
                "complete": False,
            })
            if not checkpoint:
                connection.execute(
                    "DELETE FROM metadata WHERE key='v3_birth_history_repair_v1'",
                )
        return True

    def mark_reprojection_error(
        self, event_ids: Iterable[int], error: str, *, delay: float,
    ) -> None:
        values = tuple(dict.fromkeys(int(event_id) for event_id in event_ids))
        if not values:
            return
        with self.transaction() as connection:
            placeholders = ",".join("?" for _ in values)
            connection.execute(
                "UPDATE pending_reprojection SET attempts=attempts+1,next_attempt=?,"
                f"last_error=? WHERE event_id IN ({placeholders})",
                (time.time() + max(0.0, delay), str(error)[:1000], *values),
            )

    def pending_pool_unpublishes(self, limit: int = 256) -> list[str]:
        rows = self.read().execute(
            "SELECT pool_id FROM pending_pool_unpublish ORDER BY epoch,pool_id LIMIT ?",
            (max(1, min(int(limit), 1024)),),
        ).fetchall()
        return [str(row["pool_id"]) for row in rows]

    def complete_pool_unpublishes(self, pool_ids: Iterable[str]) -> None:
        values = tuple(dict.fromkeys(str(pool_id).lower() for pool_id in pool_ids))
        if not values:
            return
        with self.transaction() as connection:
            placeholders = ",".join("?" for _ in values)
            removed = connection.execute(
                f"DELETE FROM pending_pool_unpublish WHERE pool_id IN ({placeholders})",
                values,
            ).rowcount
            if removed:
                self._bump(connection, "pending_pool_unpublish", -removed)


    def mark_enrichment_error(self, tx_hash: str, error: str, *, delay: float) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE pending_enrichment SET attempts=attempts+1,"
                "next_attempt=?,last_error=?,updated_at=?,"
                "generation=generation+1 WHERE tx_hash=?",
                (
                    time.time() + max(0.0, delay), str(error)[:1000],
                    time.time(), tx_hash.lower(),
                ),
            )

    def queue_v3_balances(
        self, pool_ids: Iterable[str], block_number: int, block_hash: str,
    ) -> None:
        values = tuple(dict.fromkeys(str(pool_id).lower() for pool_id in pool_ids))
        if not values:
            return
        block_number = int(block_number)
        block_hash = block_hash.lower()
        with self.transaction() as connection:
            before = connection.total_changes
            connection.executemany(
                "INSERT OR IGNORE INTO pending_balances"
                "(pool_id,block_number,block_hash,attempts,next_attempt,last_error) "
                "VALUES(?,?,?,?,?,?)",
                (
                    (pool_id, block_number, block_hash, 0, 0.0, None)
                    for pool_id in values
                ),
            )
            inserted = connection.total_changes - before
            if inserted:
                self._bump(connection, "pending_balances", inserted)

    def pending_v3_balances(self, limit: int = 16) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 128))
        historical = max(1, limit // 4)
        due = time.time()
        rows = self.read().execute(
            "WITH oldest AS ("
            "SELECT block_number,pool_id FROM pending_balances "
            "INDEXED BY pending_balances_order_idx WHERE next_attempt<=? "
            "ORDER BY block_number,pool_id LIMIT ?"
            "),newest AS ("
            "SELECT block_number,pool_id FROM pending_balances "
            "INDEXED BY pending_balances_order_idx WHERE next_attempt<=? "
            "ORDER BY block_number DESC,pool_id DESC LIMIT ?"
            "),selected AS (SELECT * FROM oldest UNION SELECT * FROM newest) "
            "SELECT p.*,q.token0,q.token1,q.protocol FROM selected s "
            "JOIN pending_balances p ON p.pool_id=s.pool_id AND p.block_number=s.block_number "
            "JOIN pools q ON q.id=p.pool_id ORDER BY s.block_number,s.pool_id",
            (due, historical, due, limit - historical),
        ).fetchall()
        return [dict(row) for row in rows]

    def save_v3_balance(
        self, pool_id: str, block_number: int, block_hash: str, balance0: Any, balance1: Any,
    ) -> None:
        pool_id = pool_id.lower()
        with self.transaction() as connection:
            pool = connection.execute(
                "SELECT protocol,metadata_json FROM pools WHERE id=?", (pool_id,),
            ).fetchone()
            block = connection.execute(
                "SELECT hash,timestamp FROM blocks WHERE number=?", (int(block_number),),
            ).fetchone()
            if pool is None or pool["protocol"] != "v3":
                raise ValueError("per-pool balance snapshots are only valid for V3 pools")
            if block is None or block["hash"] != block_hash.lower():
                raise CanonicalConflict("balance snapshot belongs to an orphaned block")
            connection.execute(
                "INSERT INTO pool_balances(pool_id,block_number,block_hash,balance0,balance1) "
                "VALUES(?,?,?,?,?) ON CONFLICT(pool_id,block_number) DO UPDATE SET "
                "block_hash=excluded.block_hash,balance0=excluded.balance0,balance1=excluded.balance1",
                (
                    pool_id, int(block_number), block_hash.lower(),
                    _decimal_text(balance0, "balance0"), _decimal_text(balance1, "balance1"),
                ),
            )
            metadata = _decode_json(pool["metadata_json"], {})
            if not isinstance(metadata, dict):
                metadata = {}
            prior_block = metadata.get("balance_block")
            if prior_block is None or int(prior_block) <= int(block_number):
                metadata.update({
                    "balance0": _decimal_text(balance0, "balance0"),
                    "balance1": _decimal_text(balance1, "balance1"),
                    "balance_block": int(block_number),
                    "balance_timestamp": int(block["timestamp"]),
                })
                serialized = _json(metadata)
                if serialized != pool["metadata_json"]:
                    connection.execute(
                        "UPDATE pools SET metadata_json=? WHERE id=?",
                        (serialized, pool_id),
                    )
                    self._mark_pool_metadata_changed()
            removed = connection.execute(
                "DELETE FROM pending_balances WHERE pool_id=? AND block_number=?",
                (pool_id, int(block_number)),
            ).rowcount
            if removed:
                self._bump(connection, "pending_balances", -removed)
            self._next_revision(connection)

    def mark_v3_balance_error(
        self, pool_id: str, block_number: int, error: str, *, delay: float,
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE pending_balances SET attempts=attempts+1,next_attempt=?,last_error=? "
                "WHERE pool_id=? AND block_number=?",
                (time.time() + max(0.0, delay), str(error)[:1000], pool_id.lower(), int(block_number)),
            )

    def _rollback_search_index(
        self, connection: sqlite3.Connection, ancestor: int,
        removable_pool_where: str,
    ) -> None:
        """Repair only identities touched by orphaned events and pools."""
        # Read the orphan suffix before deleting it. Each surviving lookup uses
        # the identity index, not a scan of all historical canonical events.
        for kind, column, index in (
            ("transaction", "tx_hash", "events_tx_log_idx"),
            ("owner", "owner", "events_owner_time_idx"),
            ("custody", "custody", "events_custody_time_idx"),
            ("position", "position_key", "events_position_order_idx"),
        ):
            orphaned = connection.execute(
                f"SELECT DISTINCT {column} FROM events INDEXED BY events_block_idx "
                f"WHERE block_number>? AND {column} IS NOT NULL",
                (ancestor,),
            )
            while batch := orphaned.fetchmany(500):
                identities = [str(row[0]) for row in batch]
                marks = ",".join("?" for _ in identities)
                connection.executemany(
                    "DELETE FROM lp_search_entities WHERE kind=? AND id=?",
                    ((kind, identity) for identity in identities),
                )
                fields = (
                    "position_key,MAX(token_id) AS token_id,"
                    "MAX(pool_id) AS pool_id,MAX(protocol) AS protocol"
                    if kind == "position"
                    else f"{column},MIN(block_number) AS block_number"
                )
                surviving = connection.execute(
                    f"SELECT {fields} FROM events INDEXED BY {index} "
                    f"WHERE {column} IN ({marks}) AND block_number<=? "
                    f"GROUP BY {column}",
                    (*identities, ancestor),
                )
                self._index_event_search_batch(
                    connection, (dict(row) for row in surviving),
                )

        affected: set[tuple[str, str]] = set()
        for row in connection.execute(
            f"SELECT * FROM pools WHERE {removable_pool_where}",
            (ancestor, ancestor),
        ):
            pool = dict(row)
            affected.update(
                (entity[0], entity[1]) for entity in self._pool_search_entities(pool)
            )
        if not affected:
            return
        connection.executemany(
            "DELETE FROM lp_search_entities WHERE kind=? AND id=?",
            sorted(affected),
        )
        # Token and venue identities can be shared with surviving pools.
        for row in connection.execute(
            "SELECT id,protocol,address,token0,token1,symbol0,symbol1,factory "
            f"FROM pools WHERE NOT COALESCE(({removable_pool_where}),0)",
            (ancestor, ancestor),
        ):
            self._put_search_entities(
                connection,
                (
                    entity for entity in self._pool_search_entities(dict(row))
                    if (entity[0], entity[1]) in affected
                ),
            )
        for protocol, label in (("v2", "V2"), ("v3", "V3"), ("v4", "Uniswap V4")):
            if ("protocol", protocol) in affected:
                self._put_search_entity(
                    connection, kind="protocol", entity_id=protocol, label=label,
                    subtitle=f"INDEXED {protocol.upper()} PROTOCOL",
                    href=f"/lp?protocol={protocol}", rank=3,
                    terms=(protocol, label, "uniswap" if protocol in {"v3", "v4"} else None),
                )

    def rollback(
        self, ancestor: int, *, header: Mapping[str, Any] | None = None,
    ) -> None:
        """Atomically remove everything above the verified common ancestor."""
        ancestor = _integer(ancestor, "ancestor")
        if ancestor < 0:
            raise ValueError("ancestor must be nonnegative")
        if header is not None and _header_number(header) != ancestor:
            raise ValueError("rollback header does not match ancestor")
        with self.transaction() as connection:
            for _apply, rollback, _persists_events in reversed(self._projections):
                rollback(connection, ancestor)
            if header is not None:
                self._store_headers(connection, [header])
            removed_events = connection.execute(
                "SELECT COUNT(*) FROM events WHERE block_number>?", (ancestor,),
            ).fetchone()[0]
            removed_transactions = connection.execute(
                "SELECT COUNT(*) FROM transactions WHERE block_number>?", (ancestor,),
            ).fetchone()[0]
            removed_pending = connection.execute(
                "SELECT COUNT(*) FROM pending_enrichment WHERE block_number>?", (ancestor,),
            ).fetchone()[0]
            removed_balance_pending = connection.execute(
                "SELECT COUNT(*) FROM pending_balances WHERE block_number>?", (ancestor,),
            ).fetchone()[0]
            removed_reprojections = connection.execute(
                "SELECT COUNT(*) FROM pending_reprojection WHERE block_number>?",
                (ancestor,),
            ).fetchone()[0]
            removable_pool_where = (
                "created_block>? OR (created_block IS NULL AND id IN "
                "(SELECT pool_id FROM pool_provenance WHERE observed_block>?))"
            )
            self._rollback_search_index(connection, ancestor, removable_pool_where)
            removed_pools = connection.execute(
                f"SELECT COUNT(*) FROM pools WHERE {removable_pool_where}",
                (ancestor, ancestor),
            ).fetchone()[0]
            queued_unpublish = connection.execute(
                "INSERT OR IGNORE INTO pending_pool_unpublish(pool_id,epoch) "
                f"SELECT id,? FROM pools WHERE {removable_pool_where}",
                (
                    int(self._metadata(connection, "epoch", 0)) + 1,
                    ancestor, ancestor,
                ),
            ).rowcount
            if queued_unpublish:
                self._bump(connection, "pending_pool_unpublish", queued_unpublish)
            connection.execute(
                "DELETE FROM pending_reprojection WHERE block_number>?", (ancestor,),
            )
            connection.execute("DELETE FROM events WHERE block_number>?", (ancestor,))
            connection.execute("DELETE FROM transactions WHERE block_number>?", (ancestor,))
            connection.execute("DELETE FROM pending_enrichment WHERE block_number>?", (ancestor,))
            connection.execute("DELETE FROM pool_balances WHERE block_number>?", (ancestor,))
            connection.execute("DELETE FROM pending_balances WHERE block_number>?", (ancestor,))
            connection.execute(
                f"DELETE FROM pools WHERE {removable_pool_where}",
                (ancestor, ancestor),
            )
            pool_metadata_changed = bool(removed_pools)
            connection.execute(
                "DELETE FROM pool_provenance WHERE NOT EXISTS "
                "(SELECT 1 FROM pools WHERE pools.id=pool_provenance.pool_id)"
            )
            removed_token_pending = connection.execute(
                "DELETE FROM pending_token_metadata "
                "WHERE NOT EXISTS (SELECT 1 FROM pools "
                "WHERE pools.token0=pending_token_metadata.address "
                "OR pools.token1=pending_token_metadata.address)",
            ).rowcount
            balanced_pools = connection.execute(
                "SELECT id,metadata_json FROM pools "
                "WHERE metadata_json LIKE '%\"balance_block\"%'",
            ).fetchall()
            for pool in balanced_pools:
                metadata = _decode_json(pool["metadata_json"], {})
                if (
                    isinstance(metadata, dict)
                    and metadata.get("balance_block") is not None
                    and int(metadata["balance_block"]) > ancestor
                ):
                    latest = connection.execute(
                        "SELECT b.balance0,b.balance1,b.block_number,k.timestamp "
                        "FROM pool_balances b JOIN blocks k ON k.number=b.block_number "
                        "WHERE b.pool_id=? ORDER BY b.block_number DESC LIMIT 1",
                        (pool["id"],),
                    ).fetchone()
                    for key in ("balance0", "balance1", "balance_block", "balance_timestamp"):
                        metadata.pop(key, None)
                    if latest is not None:
                        metadata.update({
                            "balance0": latest["balance0"],
                            "balance1": latest["balance1"],
                            "balance_block": latest["block_number"],
                            "balance_timestamp": latest["timestamp"],
                        })
                    connection.execute(
                        "UPDATE pools SET metadata_json=? WHERE id=?",
                        (_json(metadata) if metadata else None, pool["id"]),
                    )
                    pool_metadata_changed = True
            if pool_metadata_changed:
                self._mark_pool_metadata_changed()
            connection.execute("DELETE FROM blocks WHERE number>?", (ancestor,))
            connection.execute("DELETE FROM coverage_intervals WHERE start_block>?", (ancestor,))
            ancestor_row = connection.execute(
                "SELECT hash FROM blocks WHERE number=?", (ancestor,),
            ).fetchone()
            if ancestor_row is None:
                connection.execute(
                    "DELETE FROM coverage_intervals WHERE end_block>?", (ancestor,),
                )
            else:
                connection.execute(
                    "UPDATE coverage_intervals SET end_block=?,end_hash=? WHERE end_block>?",
                    (ancestor, ancestor_row["hash"], ancestor),
                )
            for key in ("live", "history"):
                cursor = self._metadata(connection, f"cursor:{key}", None)
                if isinstance(cursor, dict):
                    coordinate = cursor.get("block_number")
                    if coordinate is None:
                        coordinate = cursor.get("to_block") if key == "live" else cursor.get("low_block")
                    if coordinate is not None and int(coordinate) > ancestor:
                        block = connection.execute(
                            "SELECT hash FROM blocks WHERE number=?", (ancestor,),
                        ).fetchone()
                        replacement = {
                            "lane": key, "block_number": ancestor,
                            "block_hash": block["hash"] if block else None,
                        }
                        self._set_metadata(connection, f"cursor:{key}", replacement)
            self._bump(connection, "indexed_events", -removed_events)
            self._bump(connection, "indexed_transactions", -removed_transactions)
            self._bump(connection, "pending_enrichment", -removed_pending)
            self._bump(connection, "pending_balances", -removed_balance_pending)
            self._bump(connection, "pending_reprojection", -removed_reprojections)
            if removed_token_pending:
                self._bump(connection, "pending_metadata", -removed_token_pending)
            self._bump(connection, "indexed_pools", -removed_pools)
            self._bump(connection, "epoch", 1)
            self._next_revision(connection)
            self._set_metadata(connection, "last_reorg", {
                "ancestor": ancestor, "at": int(time.time()), "removed_events": removed_events,
            })

    def cursor_state(self, lane: str) -> tuple[dict[str, Any] | None, int]:
        row = self.read().execute(
            "SELECT (SELECT value FROM metadata WHERE key=?) AS cursor,"
            "(SELECT value FROM metadata WHERE key='epoch') AS epoch",
            (f"cursor:{lane}",),
        ).fetchone()
        value = _decode_json(row["cursor"], None) if row and row["cursor"] is not None else None
        epoch = _decode_json(row["epoch"], 0) if row and row["epoch"] is not None else 0
        return (value if isinstance(value, dict) else None, int(epoch))

    def cursor(self, lane: str) -> dict[str, Any] | None:
        return self.cursor_state(lane)[0]

    def pool(self, pool_id: str) -> dict[str, Any] | None:
        row = self.read().execute("SELECT * FROM pools WHERE id=?", (pool_id.lower(),)).fetchone()
        return dict(row) if row is not None else None

    def status(self) -> dict[str, Any]:
        """Return counters and cursor/coverage metadata without table scans."""
        connection = self.read()
        keys = connection.execute("SELECT key,value FROM metadata").fetchall()
        metadata = {row["key"]: _decode_json(row["value"], row["value"]) for row in keys}
        runtime = metadata.get("runtime_status")
        status = dict(runtime) if isinstance(runtime, dict) else {}
        live = metadata.get("cursor:live")
        history = metadata.get("cursor:history")
        intervals = connection.execute(
            "SELECT lane,MIN(start_block) AS first,MAX(end_block) AS last "
            "FROM coverage_intervals GROUP BY lane",
        ).fetchall()
        coverage: dict[str, dict[str, Any]] = {}
        for row in intervals:
            first_header = connection.execute(
                "SELECT timestamp FROM blocks WHERE number=?", (row["first"],),
            ).fetchone()
            last_header = connection.execute(
                "SELECT timestamp FROM blocks WHERE number=?", (row["last"],),
            ).fetchone()
            coverage[row["lane"]] = {
                "from_block": row["first"],
                "to_block": row["last"],
                "from": first_header["timestamp"] if first_header else None,
                "to": last_header["timestamp"] if last_header else None,
            }
        indexed_head = live.get("block_number") if isinstance(live, dict) else None
        if indexed_head is None and isinstance(live, dict):
            indexed_head = live.get("to_block")
        history_from = (
            coverage.get("history", {}).get("from")
            if "history" in coverage else coverage.get("live", {}).get("from")
        )
        history_to = (
            coverage.get("live", {}).get("to")
            if "live" in coverage else coverage.get("history", {}).get("to")
        )
        status.update({
            "revision": int(metadata.get("revision", 0)),
            "epoch": int(metadata.get("epoch", 0)),
            "indexed_head": indexed_head,
            "indexed_events": int(metadata.get("indexed_events", 0)),
            "indexed_pools": int(metadata.get("indexed_pools", 0)),
            "indexed_transactions": int(metadata.get("indexed_transactions", 0)),
            "pending_enrichment": int(metadata.get("pending_enrichment", 0)),
            "pending_balances": int(metadata.get("pending_balances", 0)),
            "pending_metadata": int(metadata.get("pending_metadata", 0)),
            "pending_pool_unpublish": int(metadata.get("pending_pool_unpublish", 0)),
            "pending_reprojection": int(metadata.get("pending_reprojection", 0)),
            "pending_accounting": int(metadata.get("pending_accounting", 0)),
            "history_from": history_from,
            "history_to": history_to,
            "history_target": history.get("target_timestamp") if isinstance(history, dict) else None,
            "history_target_block": history.get("target_block") if isinstance(history, dict) else None,
            "backfill": bool(isinstance(history, dict) and not history.get("complete", False)),
            "coverage": coverage,
            "last_reorg": metadata.get("last_reorg"),
        })
        status["events_revision"] = int(metadata.get("events_revision", 0))
        status["live_revision"] = int(metadata.get("live_revision", 0))
        head = status.get("head")
        if isinstance(head, int) and isinstance(indexed_head, int):
            status["lag_blocks"] = max(0, head - indexed_head)
        return status

    def close(self) -> None:
        with self.lock:
            if self._closed:
                return
            self._closed = True
            with self._reader_lock:
                readers = tuple(self._readers.items())
                self._readers.clear()
            # Interrupt active statements, but let live reader threads close
            # their own connections. Cross-thread close can clear SQLite's
            # error state before the interrupted call builds its exception.
            for _ident, connection in readers:
                try:
                    connection.interrupt()
                except sqlite3.Error:
                    pass
            current_ident = threading.get_ident()
            active_idents = {thread.ident for thread in threading.enumerate()}
            for ident, connection in readers:
                if ident != current_ident and ident in active_idents:
                    continue
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            self.connection.close()

    def __enter__(self) -> "MarketStore":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()


__all__ = ["CanonicalConflict", "MarketStore", "MarketStoreError"]
