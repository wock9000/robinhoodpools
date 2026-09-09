"""Durable owner and position accounting for the LP market index.

The projection deliberately treats unknown history, identity, prices, traces and
transaction costs as unknown.  It never turns a missing observation into a zero.
All raw token arithmetic is performed with Python integers; floats are used only
for already-priced USD presentation values.
"""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from collections.abc import Mapping
from contextlib import contextmanager
from functools import lru_cache
from itertools import groupby
import json
import math
import sqlite3
import threading
import time
from typing import Any, Iterable, NamedTuple, Sequence

from .lp_math import principal_raw


_ZERO_ADDRESS = "0x" + "0" * 40
_SCHEMA_VERSION = 3
_MAX_LIMIT = 200
_MISSING = object()
# Each retained position is only (liquidity, lower tick, upper tick). Pools
# beyond the global position budget are valued from the cursor but not cached.
_POOL_INVENTORY_CACHE_POOLS = 128
_POOL_INVENTORY_CACHE_POSITIONS = 100_000
_POOL_RESULT_CACHE_ENTRIES = 512


class _PoolInventory(NamedTuple):
    """Compact immutable inputs needed to revalue one pool's open positions."""

    positions: tuple[tuple[int | None, int | None, int | None], ...]
    lp_count: int
    history_complete: bool



_SCHEMA = """
CREATE TABLE IF NOT EXISTS lp_accounting_event_keys (
    event_id INTEGER PRIMARY KEY,
    position_key TEXT NOT NULL,
    token_id TEXT
);
CREATE INDEX IF NOT EXISTS lp_accounting_event_keys_position
    ON lp_accounting_event_keys(position_key, event_id);
CREATE INDEX IF NOT EXISTS lp_accounting_event_keys_token
    ON lp_accounting_event_keys(token_id, position_key);

CREATE TABLE IF NOT EXISTS lp_ownership_intervals (
    position_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    token_id TEXT,
    owner TEXT NOT NULL,
    custody TEXT,
    identity_basis TEXT NOT NULL,
    acquired_by TEXT NOT NULL,
    start_block INTEGER NOT NULL,
    start_tx_index INTEGER NOT NULL,
    start_log_index INTEGER NOT NULL,
    start_timestamp INTEGER NOT NULL,
    end_block INTEGER,
    end_tx_index INTEGER,
    end_log_index INTEGER,
    end_timestamp INTEGER,
    complete INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(position_key, ordinal)
);
CREATE INDEX IF NOT EXISTS lp_ownership_intervals_owner
    ON lp_ownership_intervals(owner, start_timestamp, end_timestamp);

CREATE TABLE IF NOT EXISTS lp_accounting_positions (
    position_key TEXT PRIMARY KEY,
    token_id TEXT,
    pool_id TEXT,
    protocol TEXT,
    owner TEXT,
    custody TEXT,
    identity_basis TEXT,
    tick_lower INTEGER,
    tick_upper INTEGER,
    liquidity TEXT,
    liquidity_known INTEGER NOT NULL,
    pending_principal0 TEXT,
    pending_principal1 TEXT,
    pending_known INTEGER NOT NULL,
    tokens_owed0 TEXT,
    tokens_owed1 TEXT,
    owed_known INTEGER NOT NULL,
    active_episode_id TEXT,
    status TEXT NOT NULL,
    history_complete INTEGER NOT NULL,
    first_block INTEGER NOT NULL,
    first_timestamp INTEGER NOT NULL,
    last_block INTEGER NOT NULL,
    last_tx_index INTEGER NOT NULL,
    last_log_index INTEGER NOT NULL,
    last_timestamp INTEGER NOT NULL,
    principal0 TEXT,
    principal1 TEXT,
    principal_usd REAL,
    uncollected_fees_usd REAL,
    equity_usd REAL,
    valuation_block INTEGER,
    valuation_timestamp INTEGER,
    valuation_basis TEXT,
    state_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS lp_accounting_positions_owner
    ON lp_accounting_positions(owner, status, last_timestamp);
CREATE INDEX IF NOT EXISTS lp_accounting_positions_custody
    ON lp_accounting_positions(custody, status, last_timestamp);
CREATE INDEX IF NOT EXISTS lp_accounting_positions_pool
    ON lp_accounting_positions(pool_id, status, last_timestamp);
CREATE INDEX IF NOT EXISTS lp_accounting_positions_active_inventory
    ON lp_accounting_positions(
        pool_id,owner,custody,protocol,liquidity,liquidity_known,
        tick_lower,tick_upper,principal_usd,history_complete
    ) WHERE active_episode_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS lp_accounting_episodes (
    id TEXT PRIMARY KEY,
    position_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    token_id TEXT,
    pool_id TEXT,
    protocol TEXT,
    owner TEXT,
    custody TEXT,
    identity_basis TEXT,
    tick_lower INTEGER,
    tick_upper INTEGER,
    opened_block INTEGER NOT NULL,
    opened_tx_index INTEGER NOT NULL,
    opened_log_index INTEGER NOT NULL,
    opened_at INTEGER NOT NULL,
    closed_block INTEGER,
    closed_tx_index INTEGER,
    closed_log_index INTEGER,
    closed_at INTEGER,
    last_timestamp INTEGER NOT NULL,
    status TEXT NOT NULL,
    history_complete INTEGER NOT NULL,
    identity_complete INTEGER NOT NULL,
    transferred_basis INTEGER NOT NULL,
    ambiguous_reentry INTEGER NOT NULL,
    trace_complete INTEGER NOT NULL,
    claims_complete INTEGER NOT NULL,
    cashflow_complete INTEGER NOT NULL,
    pricing_complete INTEGER NOT NULL,
    fees_complete INTEGER NOT NULL,
    deposit0 TEXT NOT NULL,
    deposit1 TEXT NOT NULL,
    proceeds0 TEXT NOT NULL,
    proceeds1 TEXT NOT NULL,
    principal_withdrawal0 TEXT NOT NULL,
    principal_withdrawal1 TEXT NOT NULL,
    fees0 TEXT NOT NULL,
    fees1 TEXT NOT NULL,
    deposit_usd REAL,
    proceeds_usd REAL,
    withdrawal_usd REAL,
    fees_usd REAL,
    close_price0_usd REAL,
    close_price1_usd REAL,
    current_equity_usd REAL,
    lp_value_usd REAL,
    hold_value_usd REAL,
    lp_vs_hold_usd REAL,
    gross_pnl_usd REAL,
    gas_usd REAL,
    net_pnl_usd REAL,
    return_pct REAL,
    accounting_basis TEXT NOT NULL,
    qualifiers TEXT NOT NULL,
    UNIQUE(position_key, ordinal)
);
CREATE INDEX IF NOT EXISTS lp_accounting_episodes_owner
    ON lp_accounting_episodes(owner, closed_at, opened_at);
CREATE INDEX IF NOT EXISTS lp_accounting_episodes_custody
    ON lp_accounting_episodes(custody, closed_at, opened_at);
CREATE INDEX IF NOT EXISTS lp_accounting_episodes_pool
    ON lp_accounting_episodes(pool_id, closed_at, opened_at);
CREATE INDEX IF NOT EXISTS lp_accounting_episodes_last_timestamp
    ON lp_accounting_episodes(last_timestamp);

CREATE TABLE IF NOT EXISTS lp_accounting_effects (
    event_id INTEGER PRIMARY KEY,
    position_key TEXT NOT NULL,
    episode_id TEXT,
    owner TEXT,
    custody TEXT,
    tx_hash TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    tx_index INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    kind TEXT NOT NULL,
    deposit0 TEXT,
    deposit1 TEXT,
    proceeds0 TEXT,
    proceeds1 TEXT,
    principal_withdrawal0 TEXT,
    principal_withdrawal1 TEXT,
    fees0 TEXT,
    fees1 TEXT,
    deposit_usd REAL,
    proceeds_usd REAL,
    withdrawal_usd REAL,
    fees_usd REAL,
    exact INTEGER NOT NULL,
    basis TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS lp_accounting_effects_position
    ON lp_accounting_effects(position_key, block_number, tx_index, log_index);
CREATE INDEX IF NOT EXISTS lp_accounting_effects_episode
    ON lp_accounting_effects(episode_id, tx_hash);
CREATE INDEX IF NOT EXISTS lp_accounting_effects_tx
    ON lp_accounting_effects(tx_hash, episode_id);

CREATE TABLE IF NOT EXISTS lp_accounting_tx_costs (
    tx_hash TEXT PRIMARY KEY,
    payer TEXT,
    owner TEXT,
    position_key TEXT,
    episode_id TEXT,
    gas_usd REAL,
    attribution TEXT NOT NULL,
    block_number INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS lp_accounting_tx_costs_owner
    ON lp_accounting_tx_costs(owner, block_number);

CREATE TABLE IF NOT EXISTS lp_accounting_pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position_key TEXT NOT NULL UNIQUE,
    generation INTEGER NOT NULL,
    requested_revision INTEGER NOT NULL,
    requested_epoch INTEGER NOT NULL,
    priority_block INTEGER NOT NULL,
    priority_tx_index INTEGER NOT NULL,
    priority_log_index INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS lp_accounting_pending_recent
    ON lp_accounting_pending(
        priority_block DESC,priority_tx_index DESC,priority_log_index DESC,id DESC
    );

CREATE TABLE IF NOT EXISTS lp_accounting_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

"""

_EVENT_COLUMNS = (
    "id", "block_number", "block_hash", "tx_hash", "tx_index", "log_index",
    "timestamp", "pool_id", "protocol", "kind", "owner", "custody",
    "position_key", "token_id", "tick_lower", "tick_upper", "liquidity_delta",
    "liquidity", "sqrt_price_x96", "tick", "fee_ppm", "amount0", "amount1",
    "fee_amount0", "fee_amount1", "cashflow0", "cashflow1", "price0_usd",
    "price1_usd", "volume_usd", "fees_usd", "deposit_usd", "withdrawal_usd",
    "pricing_basis", "accounting_basis", "identity_basis", "data", "revision",
)
_EVENT_SELECT = ",".join(f"e.{name}" for name in _EVENT_COLUMNS)
_EFFECT_COLUMNS = (
    "event_id", "position_key", "episode_id", "owner", "custody", "tx_hash",
    "block_number", "tx_index", "log_index", "timestamp", "kind", "deposit0",
    "deposit1", "proceeds0", "proceeds1", "principal_withdrawal0",
    "principal_withdrawal1", "fees0", "fees1", "deposit_usd", "proceeds_usd",
    "withdrawal_usd", "fees_usd", "exact", "basis",
)
_EFFECT_MUTABLE_COLUMNS = _EFFECT_COLUMNS[1:]
_EFFECT_WRITE_SQL = (
    f"INSERT INTO lp_accounting_effects({','.join(_EFFECT_COLUMNS)}) "
    f"VALUES({','.join('?' for _ in _EFFECT_COLUMNS)}) "
    "ON CONFLICT(event_id) DO UPDATE SET "
    + ",".join(
        f"{column}=excluded.{column}" for column in _EFFECT_MUTABLE_COLUMNS
    )
    + " WHERE "
    + " OR ".join(
        f"{column} IS NOT excluded.{column}"
        for column in _EFFECT_MUTABLE_COLUMNS
    )
)


def _dict_rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [item[0] for item in cursor.description or ()]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


class _ReplayWrites:
    """Coalesce and pre-diff a full position replay outside the writer."""

    _GROUPS = {
        "ownership": (
            "lp_ownership_intervals",
            lambda row: (str(row[0]), int(row[1])),
            "DELETE FROM lp_ownership_intervals WHERE position_key=? AND ordinal=?",
        ),
        "episodes": (
            "lp_accounting_episodes",
            lambda row: str(row[0]),
            "DELETE FROM lp_accounting_episodes WHERE id=?",
        ),
        "effects": (
            "lp_accounting_effects",
            lambda row: int(row[0]),
            "DELETE FROM lp_accounting_effects WHERE event_id=?",
        ),
        "positions": (
            "lp_accounting_positions",
            lambda row: str(row[0]),
            "DELETE FROM lp_accounting_positions WHERE position_key=?",
        ),
    }

    def __init__(self, position_key: str) -> None:
        self.position_key = position_key
        self._statements: dict[str, str] = {}
        self._rows: dict[str, dict[Any, tuple[Any, ...]]] = {
            group: {} for group in self._GROUPS
        }
        self._prepared_deletes: dict[str, list[tuple[Any, ...]]] | None = None
        self._prepared_rows: dict[str, list[tuple[Any, ...]]] | None = None
        self._affected_txs: set[str] = set()
        self._changed_episodes: set[str] = set()

    def add(
        self, group: str, key: Any, sql: str, row: tuple[Any, ...],
    ) -> None:
        if self._prepared_rows is not None:
            raise RuntimeError("cannot append to a prepared replay")
        prior = self._statements.setdefault(group, sql)
        if prior != sql:
            raise RuntimeError(f"conflicting replay statement for {group}")
        self._rows[group][key] = row

    def prepare(self, conn: sqlite3.Connection) -> None:
        """Load and diff existing rows on the caller's consistent snapshot."""
        if self._prepared_rows is not None:
            return
        deletes: dict[str, list[tuple[Any, ...]]] = {
            group: [] for group in self._GROUPS
        }
        changed: dict[str, list[tuple[Any, ...]]] = {
            group: [] for group in self._GROUPS
        }
        owner_changed_episodes: set[str] = set()
        for group in ("ownership", "episodes", "positions"):
            table, row_key, _delete_sql = self._GROUPS[group]
            desired = self._rows[group]
            existing: dict[Any, tuple[Any, ...]] = {
                row_key(row): tuple(row)
                for row in conn.execute(
                    f"SELECT * FROM {table} WHERE position_key=?",
                    (self.position_key,),
                ).fetchall()
            }
            stale = sorted(existing.keys() - desired.keys())
            deletes[group] = [
                key if isinstance(key, tuple) else (key,) for key in stale
            ]
            changed_keys = [
                key for key, row in desired.items()
                if existing.get(key) != row
            ]
            changed[group] = [desired[key] for key in changed_keys]
            if group == "episodes":
                self._changed_episodes.update(
                    str(key) for key in changed_keys
                )
                owner_changed_episodes.update(
                    str(key) for key in changed_keys
                    if key not in existing
                    or existing[key][6] != desired[key][6]
                )

        desired_effects = self._rows["effects"]
        desired_ids = sorted(int(key) for key in desired_effects)
        existing_effects: dict[int, tuple[Any, ...]] = {}
        for batch in _batches(desired_ids):
            marks = ",".join("?" for _ in batch)
            existing_effects.update({
                int(row[0]): tuple(row)
                for row in conn.execute(
                    f"SELECT {','.join(_EFFECT_COLUMNS)} "
                    f"FROM lp_accounting_effects WHERE event_id IN ({marks})",
                    batch,
                ).fetchall()
            })
        existing_ids = {
            int(row[0]) for row in conn.execute(
                "SELECT event_id FROM lp_accounting_effects "
                "WHERE position_key=?",
                (self.position_key,),
            ).fetchall()
        }
        stale_effects = sorted(existing_ids - desired_effects.keys())
        deletes["effects"] = [(event_id,) for event_id in stale_effects]
        for batch in _batches(stale_effects):
            marks = ",".join("?" for _ in batch)
            self._affected_txs.update(str(row[0]) for row in conn.execute(
                "SELECT DISTINCT tx_hash FROM lp_accounting_effects "
                f"WHERE event_id IN ({marks}) AND tx_hash<>''",
                batch,
            ).fetchall())
        changed_effects = [
            event_id for event_id in desired_ids
            if existing_effects.get(event_id) != desired_effects[event_id]
        ]
        changed["effects"] = [
            desired_effects[event_id] for event_id in changed_effects
        ]
        for event_id in changed_effects:
            row = desired_effects[event_id]
            prior = existing_effects.get(event_id)
            if prior is not None and prior[5]:
                self._affected_txs.add(str(prior[5]))
            if row[5]:
                self._affected_txs.add(str(row[5]))
        for batch in _batches(sorted(owner_changed_episodes)):
            marks = ",".join("?" for _ in batch)
            self._affected_txs.update(str(row[0]) for row in conn.execute(
                "SELECT DISTINCT tx_hash FROM lp_accounting_effects "
                f"WHERE episode_id IN ({marks}) AND tx_hash<>''",
                batch,
            ).fetchall())
        self._prepared_deletes = deletes
        self._prepared_rows = changed

    def flush(
        self, conn: sqlite3.Connection,
    ) -> tuple[set[str], set[str]]:
        if self._prepared_rows is None or self._prepared_deletes is None:
            self.prepare(conn)
        assert self._prepared_rows is not None
        assert self._prepared_deletes is not None
        for group in ("effects", "ownership", "episodes", "positions"):
            delete_rows = self._prepared_deletes[group]
            if delete_rows:
                conn.executemany(self._GROUPS[group][2], delete_rows)
            changed_rows = self._prepared_rows[group]
            statement = self._statements.get(group)
            if changed_rows and statement is not None:
                conn.executemany(statement, changed_rows)
        return set(self._affected_txs), set(self._changed_episodes)


class _PreparedProjection(NamedTuple):
    position_key: str
    generation: int
    epoch: int
    writes: _ReplayWrites


def _batches(values: Sequence[Any], size: int = 500) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _one(cursor: sqlite3.Cursor) -> dict[str, Any] | None:
    rows = _dict_rows(cursor)
    return rows[0] if rows else None


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        if isinstance(parsed, dict):
            return parsed
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return dict(value) if isinstance(value, Mapping) else {}


@lru_cache(maxsize=4096)
def _normalized_address(value: str) -> str | None:
    normalized = value.strip().lower()
    if len(normalized) == 40 and all(
        char in "0123456789abcdef" for char in normalized
    ):
        normalized = "0x" + normalized
    if len(normalized) != 42 or not normalized.startswith("0x"):
        return None
    if not all(char in "0123456789abcdef" for char in normalized[2:]):
        return None
    return normalized


def _address(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if len(value) not in (40, 42):
        value = value.strip()
        if len(value) not in (40, 42):
            return None
    return _normalized_address(value)


def _raw_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value and (value.isdecimal() or
                      (value[0] == "-" and value[1:].isdecimal())):
            return int(value)
    return None


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _flag(value: Any) -> bool:
    return value is True or value == 1 or value == "1" or value == "true"


def _order(event: Mapping[str, Any]) -> tuple[int, int, int, int]:
    return (
        int(event.get("block_number") or 0),
        int(event.get("tx_index") or 0),
        int(event.get("log_index") or 0),
        int(event.get("id") or 0),
    )


def _event_position_state(data: Mapping[str, Any], side: str) -> Mapping[str, Any] | None:
    state = data.get(f"position_{side}")
    return state if isinstance(state, Mapping) else None


def _state_number(state: Mapping[str, Any] | None, *names: str) -> int | None:
    if state is None:
        return None
    for name in names:
        if name in state:
            return _raw_int(state.get(name))
    return None


def _token_value(amount0: int, amount1: int, price0: Any, price1: Any,
                 decimals0: int | None, decimals1: int | None) -> float | None:
    if amount0 == 0 and amount1 == 0:
        return 0.0
    p0, p1 = _finite_float(price0), _finite_float(price1)
    if (amount0 and (p0 is None or decimals0 is None)) or (
            amount1 and (p1 is None or decimals1 is None)):
        return None
    value = 0.0
    if amount0:
        value += amount0 * p0 / (10 ** decimals0)  # type: ignore[operator]
    if amount1:
        value += amount1 * p1 / (10 ** decimals1)  # type: ignore[operator]
    return value if math.isfinite(value) else None


def _nullable_sum(values: Iterable[float | None]) -> float | None:
    items = list(values)
    if not items:
        return 0.0
    if any(item is None for item in items):
        return None
    return float(sum(item for item in items if item is not None))


def _limit_offset(params: Mapping[str, Any] | None,
                  default: int = 50) -> tuple[int, int]:
    params = params or {}
    try:
        limit = int(params.get("limit", default))
    except (TypeError, ValueError):
        limit = default
    try:
        offset = int(params.get("offset", 0))
    except (TypeError, ValueError):
        offset = 0
    return max(1, min(_MAX_LIMIT, limit)), max(0, offset)


def _cutoff(params: Mapping[str, Any] | None) -> int | None:
    window = str((params or {}).get("window") or "all").lower()
    seconds = {"1h": 3600, "24h": 86400, "7d": 604800, "30d": 2592000}
    if window not in seconds:
        return None
    # SQLite computes against wall clock, avoiding a mutable module clock.
    return seconds[window]


def _pair(pool: Mapping[str, Any]) -> str:
    left = pool.get("symbol0") or str(pool.get("token0") or "?")[:8]
    right = pool.get("symbol1") or str(pool.get("token1") or "?")[:8]
    return f"{left}/{right}"


class AccountBook:
    """Canonical, reorg-safe LP ownership and episode projection."""

    def __init__(self, store: Any, *, deferred: bool = False):
        self.store = store
        self.deferred = bool(deferred)
        self._installed = False
        self._projection_lock = threading.Lock()
        self._cache_lock = threading.RLock()
        self._pool_cache: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
        self._pool_inventory_cache: OrderedDict[
            str, tuple[int, _PoolInventory]
        ] = OrderedDict()
        self._pool_inventory_positions = 0
        self._owners_generation = 0
        self._owners_cache: OrderedDict[
            tuple[Any, ...], tuple[int, int | None, tuple[dict[str, Any], ...], dict[str, Any]]
        ] = OrderedDict()
        self._owner_activity_cache: OrderedDict[
            tuple[Any, ...], dict[str, Any] | None
        ] = OrderedDict()

    @property
    def owners_revision(self) -> int:
        with self._cache_lock:
            return self._owners_generation

    @staticmethod
    def _store_metadata_int(conn: sqlite3.Connection, key: str) -> int:
        try:
            row = conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        except sqlite3.OperationalError:
            return 0
        if row is None:
            return 0
        try:
            return int(json.loads(row[0]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return 0

    @staticmethod
    def _accounting_meta(
        conn: sqlite3.Connection, key: str, default: str = "",
    ) -> str:
        row = conn.execute(
            "SELECT value FROM lp_accounting_meta WHERE key=?", (key,),
        ).fetchone()
        return default if row is None else str(row[0])

    @staticmethod
    def _set_accounting_meta(
        conn: sqlite3.Connection, key: str, value: Any,
    ) -> None:
        conn.execute(
            "INSERT INTO lp_accounting_meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def install(self) -> "AccountBook":
        """Create the projection and register its atomic ledger callbacks."""
        if self._installed:
            return self
        lock = getattr(self.store, "lock", threading.RLock())
        with lock:
            if self._installed:
                return self
            self.store.connection.executescript(_SCHEMA)
            with self.store.transaction() as conn:
                prior = {
                    str(row[0]): str(row[1])
                    for row in conn.execute(
                        "SELECT key,value FROM lp_accounting_meta").fetchall()
                }
                current_revision = self._store_metadata_int(conn, "events_revision")
                current_epoch = self._store_metadata_int(conn, "epoch")
                pending = int(conn.execute(
                    "SELECT COUNT(*) FROM lp_accounting_pending",
                ).fetchone()[0])
                clean = (
                    prior.get("schema_version") == str(_SCHEMA_VERSION)
                    and prior.get("applied_revision") == str(current_revision)
                    and prior.get("applied_epoch") == str(current_epoch)
                    and prior.get("dirty") == "0"
                    and pending == 0
                )
                if self.deferred:
                    phase = prior.get("bootstrap_phase")
                    if clean:
                        phase = "complete"
                    elif phase not in {"events", "stale", "positions", "complete"}:
                        if pending:
                            phase = "complete"
                        else:
                            ledger = conn.execute(
                                "SELECT EXISTS(SELECT 1 FROM events LIMIT 1) OR "
                                "EXISTS(SELECT 1 FROM lp_accounting_positions LIMIT 1)"
                            ).fetchone()[0]
                            phase = "events" if ledger else "complete"
                            self._set_accounting_meta(
                                conn, "bootstrap_event_cursor", 0,
                            )
                            self._set_accounting_meta(
                                conn, "bootstrap_position_cursor", "",
                            )
                    self._set_accounting_meta(conn, "bootstrap_phase", phase)
                    self._set_accounting_meta(
                        conn, "schema_version", _SCHEMA_VERSION,
                    )
                    if clean or (phase == "complete" and pending == 0):
                        self._set_accounting_meta(
                            conn, "applied_revision", current_revision,
                        )
                        self._set_accounting_meta(
                            conn, "applied_epoch", current_epoch,
                        )
                        self._set_accounting_meta(conn, "dirty", 0)
                    else:
                        self._set_accounting_meta(conn, "dirty", 1)
                else:
                    if not clean:
                        self._map_existing_events(conn)
                        keys = {str(row[0]) for row in conn.execute(
                            "SELECT DISTINCT position_key "
                            "FROM lp_accounting_event_keys"
                        ).fetchall()}
                        keys.update(str(row[0]) for row in conn.execute(
                            "SELECT position_key FROM lp_accounting_positions"
                        ).fetchall())
                        for key in sorted(keys):
                            self._rebuild_position(conn, key)
                        self._rebuild_tx_costs(conn, None)
                        self._refresh_episode_costs(conn, None)
                    conn.execute("DELETE FROM lp_accounting_pending")
                    for name, value in (
                        ("schema_version", _SCHEMA_VERSION),
                        ("applied_revision", current_revision),
                        ("applied_epoch", current_epoch),
                        ("bootstrap_phase", "complete"),
                        ("dirty", 0),
                    ):
                        self._set_accounting_meta(conn, name, value)
                    pending = 0
                self.store._set_metadata(
                    conn, "pending_accounting", pending,
                )
            self.store.register_projection(
                self._apply, self._rollback, persists_events=True,
            )
            self._installed = True
        return self

    def _map_existing_events(self, conn: sqlite3.Connection) -> None:
        # Keep recovery bounded by SQLite's page cache.  Fetching every mapped
        # event into Python previously made an unclean startup retain gigabytes
        # of allocator arenas after the rebuild had finished.
        conn.execute(
            "INSERT INTO lp_accounting_event_keys(event_id,position_key,token_id) "
            "SELECT id,LOWER(TRIM(position_key)),"
            "CASE WHEN token_id IS NULL THEN NULL ELSE CAST(token_id AS TEXT) END "
            "FROM events WHERE position_key IS NOT NULL AND TRIM(position_key)<>'' "
            "ON CONFLICT(event_id) DO UPDATE SET "
            "position_key=excluded.position_key,token_id=excluded.token_id "
            "WHERE lp_accounting_event_keys.position_key IS NOT excluded.position_key "
            "OR lp_accounting_event_keys.token_id IS NOT excluded.token_id"
        )

    @staticmethod
    def _event_position_key(event: Mapping[str, Any]) -> str:
        key = str(event.get("position_key") or "").strip().lower()
        if not key:
            key = str(
                _json(event.get("data")).get("position_key") or ""
            ).strip().lower()
        return key

    def _queue_position_keys(
        self,
        conn: sqlite3.Connection,
        priorities: Mapping[str, tuple[int, int, int]],
        *,
        revision: int | None = None,
        epoch: int | None = None,
    ) -> None:
        if not priorities:
            self._finish_if_idle(conn)
            return
        requested_revision = (
            self._store_metadata_int(conn, "events_revision")
            if revision is None else int(revision)
        )
        requested_epoch = (
            self._store_metadata_int(conn, "epoch")
            if epoch is None else int(epoch)
        )
        rows = [
            (
                key, 0, requested_revision, requested_epoch,
                int(order[0]), int(order[1]), int(order[2]),
            )
            for key, order in sorted(priorities.items())
        ]
        before = conn.total_changes
        conn.executemany(
            "INSERT OR IGNORE INTO lp_accounting_pending("
            "position_key,generation,requested_revision,requested_epoch,"
            "priority_block,priority_tx_index,priority_log_index"
            ") VALUES(?,?,?,?,?,?,?)",
            rows,
        )
        inserted = conn.total_changes - before
        conn.executemany(
            "UPDATE lp_accounting_pending SET generation=generation+1,"
            "requested_revision=MAX(requested_revision,?),requested_epoch=?,"
            "priority_block=MAX(priority_block,?),"
            "priority_tx_index=MAX(priority_tx_index,?),"
            "priority_log_index=MAX(priority_log_index,?) WHERE position_key=?",
            (
                (
                    requested_revision, requested_epoch,
                    int(order[0]), int(order[1]), int(order[2]), key,
                )
                for key, order in sorted(priorities.items())
            ),
        )
        if inserted:
            self.store._bump(conn, "pending_accounting", inserted)
        self._set_accounting_meta(conn, "dirty", 1)

    def _finish_if_idle(
        self, conn: sqlite3.Connection, *, epoch: int | None = None,
    ) -> None:
        if self._accounting_meta(conn, "bootstrap_phase") != "complete":
            return
        if conn.execute(
            "SELECT 1 FROM lp_accounting_pending LIMIT 1",
        ).fetchone() is not None:
            return
        self._set_accounting_meta(
            conn, "applied_revision",
            self._store_metadata_int(conn, "events_revision"),
        )
        self._set_accounting_meta(
            conn, "applied_epoch",
            self._store_metadata_int(conn, "epoch") if epoch is None else epoch,
        )
        self._set_accounting_meta(conn, "dirty", 0)

    def _resume_bootstrap(self, limit: int) -> bool:
        batch_size = max(64, min(2048, int(limit) * 32))
        with self.store.transaction() as conn:
            phase = self._accounting_meta(conn, "bootstrap_phase", "complete")
            if phase == "complete":
                return False
            if phase == "events":
                cursor = int(self._accounting_meta(
                    conn, "bootstrap_event_cursor", "0",
                ))
                rows = _dict_rows(conn.execute(
                    "SELECT id,position_key,token_id,data,block_number,"
                    "tx_index,log_index,revision FROM events "
                    "WHERE id>? ORDER BY id LIMIT ?",
                    (cursor, batch_size),
                ))
                ids = [int(row["id"]) for row in rows]
                old_mapping: dict[int, str] = {}
                for batch in _batches(ids):
                    marks = ",".join("?" for _ in batch)
                    old_mapping.update({
                        int(item[0]): str(item[1])
                        for item in conn.execute(
                            "SELECT event_id,position_key "
                            "FROM lp_accounting_event_keys "
                            f"WHERE event_id IN ({marks})",
                            batch,
                        ).fetchall()
                    })
                mapped_rows = []
                mapped_ids: set[int] = set()
                priorities: dict[str, tuple[int, int, int]] = {}
                for row in rows:
                    event_id = int(row["id"])
                    order = (
                        int(row["block_number"]), int(row["tx_index"]),
                        int(row["log_index"]),
                    )
                    old_key = old_mapping.get(event_id)
                    if old_key:
                        priorities[old_key] = max(
                            priorities.get(old_key, order), order,
                        )
                    key = self._event_position_key(row)
                    if not key:
                        continue
                    mapped_ids.add(event_id)
                    mapped_rows.append((
                        event_id, key,
                        None if row.get("token_id") is None
                        else str(row["token_id"]),
                    ))
                    priorities[key] = max(priorities.get(key, order), order)
                stale_ids = sorted(set(old_mapping) - mapped_ids)
                for batch in _batches(stale_ids):
                    marks = ",".join("?" for _ in batch)
                    conn.execute(
                        "DELETE FROM lp_accounting_event_keys "
                        f"WHERE event_id IN ({marks})",
                        batch,
                    )
                conn.executemany(
                    "INSERT INTO lp_accounting_event_keys("
                    "event_id,position_key,token_id) VALUES(?,?,?) "
                    "ON CONFLICT(event_id) DO UPDATE SET "
                    "position_key=excluded.position_key,"
                    "token_id=excluded.token_id",
                    mapped_rows,
                )
                self._queue_position_keys(conn, priorities)
                if rows:
                    self._set_accounting_meta(
                        conn, "bootstrap_event_cursor", int(rows[-1]["id"]),
                    )
                else:
                    self._set_accounting_meta(conn, "bootstrap_phase", "stale")
                return True
            if phase == "stale":
                rows = conn.execute(
                    "SELECT k.event_id,k.position_key "
                    "FROM lp_accounting_event_keys k "
                    "LEFT JOIN events e ON e.id=k.event_id "
                    "WHERE e.id IS NULL ORDER BY k.event_id LIMIT ?",
                    (batch_size,),
                ).fetchall()
                if rows:
                    priorities = {
                        str(row["position_key"]): (0, 0, int(row["event_id"]))
                        for row in rows
                    }
                    self._queue_position_keys(conn, priorities)
                    conn.executemany(
                        "DELETE FROM lp_accounting_event_keys WHERE event_id=?",
                        ((int(row["event_id"]),) for row in rows),
                    )
                else:
                    self._set_accounting_meta(
                        conn, "bootstrap_phase", "positions",
                    )
                return True
            cursor = self._accounting_meta(
                conn, "bootstrap_position_cursor", "",
            )
            rows = conn.execute(
                "SELECT position_key,last_block,last_tx_index,last_log_index "
                "FROM lp_accounting_positions WHERE position_key>? "
                "ORDER BY position_key LIMIT ?",
                (cursor, batch_size),
            ).fetchall()
            if rows:
                self._queue_position_keys(conn, {
                    str(row["position_key"]): (
                        int(row["last_block"]), int(row["last_tx_index"]),
                        int(row["last_log_index"]),
                    )
                    for row in rows
                })
                self._set_accounting_meta(
                    conn, "bootstrap_position_cursor",
                    str(rows[-1]["position_key"]),
                )
            else:
                self._set_accounting_meta(conn, "bootstrap_phase", "complete")
                self._finish_if_idle(conn)
            return True

    def _pending_rows(self, limit: int) -> list[dict[str, Any]]:
        historical = max(1, limit // 4)
        recent = max(0, limit - historical)
        conn = self.store.read()
        selected: list[dict[str, Any]] = []
        if recent:
            selected.extend(_dict_rows(conn.execute(
                "SELECT * FROM lp_accounting_pending "
                "ORDER BY priority_block DESC,priority_tx_index DESC,"
                "priority_log_index DESC,id DESC LIMIT ?",
                (recent,),
            )))
        seen = {str(row["position_key"]) for row in selected}
        oldest = _dict_rows(conn.execute(
            "SELECT * FROM lp_accounting_pending ORDER BY id LIMIT ?",
            (historical + len(seen),),
        ))
        selected.extend(
            row for row in oldest
            if str(row["position_key"]) not in seen
        )
        return selected[:limit]

    def _prepare_pending(self, position_key: str) -> _PreparedProjection | None:
        with self._reader() as conn:
            pending = conn.execute(
                "SELECT generation FROM lp_accounting_pending "
                "WHERE position_key=?",
                (position_key,),
            ).fetchone()
            if pending is None:
                return None
            epoch = self._store_metadata_int(conn, "epoch")
            events = self._event_rows(conn, position_key)
            writes = _ReplayWrites(position_key)
            if events:
                self._derive(
                    conn, position_key, events, None,
                    refresh_values=False, writes=writes, flush_writes=False,
                )
            writes.prepare(conn)
            return _PreparedProjection(
                position_key, int(pending["generation"]), epoch, writes,
            )


    def _publish_pending(self, prepared: _PreparedProjection) -> bool:
        with self.store.transaction() as conn:
            if self._store_metadata_int(conn, "epoch") != prepared.epoch:
                return False
            pending = conn.execute(
                "SELECT generation FROM lp_accounting_pending "
                "WHERE position_key=?",
                (prepared.position_key,),
            ).fetchone()
            if pending is None:
                return False
            # Projection publication is serialized. Same-epoch event changes
            # therefore cannot race a newer accounting publish: commit this
            # coherent older snapshot, and let the generation-guarded delete
            # retain the key for the newer snapshot. Reorgs are rejected above.
            affected_pools = {
                str(row[0]).lower()
                for row in conn.execute(
                    "SELECT pool_id FROM lp_accounting_positions "
                    "WHERE position_key=? AND pool_id IS NOT NULL",
                    (prepared.position_key,),
                ).fetchall()
            }
            affected_txs, changed_episodes = prepared.writes.flush(conn)
            affected_pools.update(
                str(row[0]).lower()
                for row in conn.execute(
                    "SELECT pool_id FROM lp_accounting_positions "
                    "WHERE position_key=? AND pool_id IS NOT NULL",
                    (prepared.position_key,),
                ).fetchall()
            )
            self._refresh_position_values(conn, [prepared.position_key])
            self._rebuild_tx_costs(conn, affected_txs)
            self._refresh_episode_costs(
                conn, affected_txs, episode_ids=changed_episodes,
            )
            removed = conn.execute(
                "DELETE FROM lp_accounting_pending "
                "WHERE position_key=? AND generation=?",
                (prepared.position_key, prepared.generation),
            ).rowcount
            if removed:
                self.store._bump(conn, "pending_accounting", -removed)
            self._finish_if_idle(conn)
            self._invalidate_cache(conn, affected_pools)
            return True

    def project_pending(self, limit: int = 32) -> bool:
        """Prepare bounded queued histories off-writer, then publish atomically."""
        if not self.deferred:
            return False
        bounded = max(1, min(int(limit), 128))
        with self._projection_lock:
            worked = self._resume_bootstrap(bounded)
            pending = self._pending_rows(bounded)
            if pending:
                worked = True
            for row in pending:
                prepared = self._prepare_pending(str(row["position_key"]))
                if prepared is not None:
                    self._publish_pending(prepared)
            return worked

    def _apply(
        self, conn: sqlite3.Connection,
        events: Sequence[Mapping[str, Any]],
    ) -> None:
        if not events:
            return
        inventory_pools: set[str] = set()
        ids = [
            int(event["id"]) for event in events
            if event.get("id") is not None
        ]
        old_mapping: dict[int, str] = {}
        affected_txs: set[str] = set()
        for batch in _batches(ids):
            marks = ",".join("?" for _ in batch)
            old_mapping.update({
                int(row[0]): str(row[1])
                for row in conn.execute(
                    "SELECT event_id,position_key "
                    "FROM lp_accounting_event_keys "
                    f"WHERE event_id IN ({marks})",
                    batch,
                ).fetchall()
            })
            if not self.deferred:
                affected_txs.update(str(row[0]) for row in conn.execute(
                    "SELECT DISTINCT tx_hash FROM lp_accounting_effects "
                    f"WHERE event_id IN ({marks})",
                    batch,
                ).fetchall())
        old_keys = set(old_mapping.values())
        new_keys: set[str] = set()
        ids_by_key: dict[str, list[int]] = defaultdict(list)
        mapped_rows: list[tuple[int, str, str | None]] = []
        mapped_ids: set[int] = set()
        for event in events:
            raw_event_id = event.get("id")
            if raw_event_id is None:
                continue
            event_id = int(raw_event_id)
            key = self._event_position_key(event)
            if not key:
                # Token ids collide across managers; only a manager-qualified
                # protocol position key can safely connect a transfer.
                continue
            token_id = event.get("token_id")
            mapped_rows.append((
                event_id, key,
                None if token_id is None else str(token_id),
            ))
            mapped_ids.add(event_id)
            new_keys.add(key)
            ids_by_key[key].append(event_id)
            if not self.deferred and event.get("tx_hash"):
                affected_txs.add(str(event["tx_hash"]).lower())
        stale_ids = sorted(set(old_mapping) - mapped_ids)
        for batch in _batches(stale_ids):
            marks = ",".join("?" for _ in batch)
            conn.execute(
                "DELETE FROM lp_accounting_event_keys "
                f"WHERE event_id IN ({marks})",
                batch,
            )
        conn.executemany(
            "INSERT INTO lp_accounting_event_keys("
            "event_id,position_key,token_id) VALUES(?,?,?) "
            "ON CONFLICT(event_id) DO UPDATE SET "
            "position_key=excluded.position_key,token_id=excluded.token_id",
            mapped_rows,
        )
        keys = sorted(old_keys | new_keys)
        if self.deferred:
            priorities: dict[str, tuple[int, int, int]] = {}
            for event in events:
                if event.get("id") is None:
                    continue
                event_id = int(event["id"])
                order = _order(event)[:3]
                for key in (
                    old_mapping.get(event_id),
                    self._event_position_key(event),
                ):
                    if key:
                        priorities[key] = max(
                            priorities.get(key, order), order,
                        )
                if event.get("pool_id"):
                    inventory_pools.add(str(event["pool_id"]).lower())
            for batch in _batches(keys):
                marks = ",".join("?" for _ in batch)
                inventory_pools.update(
                    str(row[0]).lower()
                    for row in conn.execute(
                        "SELECT pool_id FROM lp_accounting_positions "
                        f"WHERE position_key IN ({marks}) "
                        "AND pool_id IS NOT NULL",
                        batch,
                    ).fetchall()
                )
            self._queue_position_keys(conn, priorities)
            self._invalidate_cache(
                conn, inventory_pools, owners=bool(priorities),
            )
            return

        prior_positions = {}
        effect_keys: set[str] = set()
        for batch in _batches(keys):
            marks = ",".join("?" for _ in batch)
            for row in _dict_rows(conn.execute(
                "SELECT * FROM lp_accounting_positions "
                f"WHERE position_key IN ({marks})",
                batch,
            )):
                prior_positions[str(row["position_key"])] = row
                if row.get("pool_id") is not None:
                    inventory_pools.add(str(row["pool_id"]).lower())
            effect_keys.update(str(row[0]) for row in conn.execute(
                "SELECT DISTINCT position_key FROM lp_accounting_effects "
                f"WHERE position_key IN ({marks})",
                batch,
            ).fetchall())
        changed_episodes: set[str] = set()
        for key in keys:
            new_ids = ids_by_key.get(key, [])
            full_rebuild = key in old_keys
            if not full_rebuild:
                full_rebuild = not self._append_position(
                    conn, key, new_ids, refresh_values=False,
                    position=prior_positions.get(key),
                )
            if full_rebuild:
                replay_txs, replay_episodes = self._rebuild_position(
                    conn, key, refresh_values=False,
                    clear=(
                        key in old_keys or key in prior_positions
                        or key in effect_keys
                    ),
                )
                affected_txs.update(replay_txs)
                changed_episodes.update(replay_episodes)
        for batch in _batches(keys):
            marks = ",".join("?" for _ in batch)
            inventory_pools.update(str(row[0]).lower() for row in conn.execute(
                "SELECT pool_id FROM lp_accounting_positions "
                f"WHERE position_key IN ({marks}) AND pool_id IS NOT NULL",
                batch,
            ).fetchall())
        self._refresh_position_values(conn, keys)
        self._rebuild_tx_costs(conn, affected_txs)
        self._refresh_episode_costs(
            conn, affected_txs, episode_ids=changed_episodes,
        )
        for name, value in (
            ("applied_revision", self._store_metadata_int(conn, "events_revision")),
            ("applied_epoch", self._store_metadata_int(conn, "epoch")),
            ("dirty", 0),
        ):
            self._set_accounting_meta(conn, name, value)
        self._invalidate_cache(
            conn, inventory_pools, owners=bool(old_keys or new_keys),
        )

    def _rollback(self, conn: sqlite3.Connection, ancestor_number: int) -> None:
        ancestor = int(ancestor_number)
        if self.deferred:
            self._rollback_deferred(conn, ancestor)
            return
        affected_pools = {str(row[0]).lower() for row in conn.execute(
            "SELECT DISTINCT pool_id FROM lp_accounting_positions "
            "WHERE last_block>? AND pool_id IS NOT NULL", (ancestor,),
        ).fetchall()}
        keys = {str(row[0]) for row in conn.execute(
            "SELECT DISTINCT position_key FROM lp_accounting_effects "
            "WHERE block_number>?", (ancestor,)).fetchall()}
        keys.update(str(row[0]) for row in conn.execute(
            "SELECT position_key FROM lp_accounting_positions WHERE last_block>?",
            (ancestor,),).fetchall())
        txs = {str(row[0]) for row in conn.execute(
            "SELECT DISTINCT tx_hash FROM lp_accounting_effects WHERE block_number>?",
            (ancestor,),).fetchall()}
        # This is correct whether the store invokes rollback projections before or
        # after deleting raw rows: rebuilding is explicitly capped at the ancestor.
        for key in sorted(keys):
            txs.update(str(row[0]) for row in conn.execute(
                "SELECT DISTINCT tx_hash FROM lp_accounting_effects "
                "WHERE position_key=? AND tx_hash<>''", (key,),
            ).fetchall())
            self._rebuild_position(conn, key, ancestor)
            txs.update(str(row[0]) for row in conn.execute(
                "SELECT DISTINCT tx_hash FROM lp_accounting_effects "
                "WHERE position_key=? AND tx_hash<>''", (key,),
            ).fetchall())
        for batch in _batches(sorted(keys)):
            marks = ",".join("?" for _ in batch)
            affected_pools.update(str(row[0]).lower() for row in conn.execute(
                f"SELECT pool_id FROM lp_accounting_positions "
                f"WHERE position_key IN ({marks}) AND pool_id IS NOT NULL", batch,
            ).fetchall())
        conn.execute(
            "DELETE FROM lp_accounting_event_keys WHERE event_id IN "
            "(SELECT id FROM events WHERE block_number>?)", (ancestor,),
        )
        conn.execute(
            "DELETE FROM lp_accounting_event_keys WHERE event_id NOT IN (SELECT id FROM events)"
        )
        self._rebuild_tx_costs(conn, txs)
        self._refresh_episode_costs(conn, txs)
        self._invalidate_cache(conn, affected_pools, owners=bool(keys))

        conn.execute(
            "INSERT INTO lp_accounting_meta(key,value) VALUES('dirty','1') "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
        )

    def _rollback_deferred(
        self, conn: sqlite3.Connection, ancestor: int,
    ) -> None:
        keys = {
            str(row[0]) for row in conn.execute(
                "SELECT DISTINCT k.position_key "
                "FROM lp_accounting_event_keys k "
                "JOIN events e ON e.id=k.event_id "
                "WHERE e.block_number>?",
                (ancestor,),
            ).fetchall()
        }
        keys.update(str(row[0]) for row in conn.execute(
            "SELECT DISTINCT position_key FROM lp_accounting_effects "
            "WHERE block_number>?",
            (ancestor,),
        ).fetchall())
        keys.update(str(row[0]) for row in conn.execute(
            "SELECT position_key FROM lp_accounting_positions "
            "WHERE last_block>?",
            (ancestor,),
        ).fetchall())
        affected_pools: set[str] = set()
        affected_txs: set[str] = set()
        for batch in _batches(sorted(keys)):
            marks = ",".join("?" for _ in batch)
            affected_pools.update(
                str(row[0]).lower()
                for row in conn.execute(
                    "SELECT pool_id FROM lp_accounting_positions "
                    f"WHERE position_key IN ({marks}) "
                    "AND pool_id IS NOT NULL",
                    batch,
                ).fetchall()
            )
            affected_txs.update(
                str(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT tx_hash FROM lp_accounting_effects "
                    f"WHERE position_key IN ({marks}) AND tx_hash<>''",
                    batch,
                ).fetchall()
            )
        next_epoch = self._store_metadata_int(conn, "epoch") + 1
        self._queue_position_keys(
            conn,
            {key: (ancestor + 1, 0, 0) for key in keys},
            epoch=next_epoch,
        )
        conn.execute(
            "DELETE FROM lp_accounting_event_keys WHERE event_id IN "
            "(SELECT id FROM events WHERE block_number>?)",
            (ancestor,),
        )
        conn.execute(
            "DELETE FROM lp_accounting_event_keys "
            "WHERE event_id NOT IN (SELECT id FROM events)"
        )
        for batch in _batches(sorted(keys)):
            marks = ",".join("?" for _ in batch)
            for table in (
                "lp_accounting_effects", "lp_ownership_intervals",
                "lp_accounting_episodes", "lp_accounting_positions",
            ):
                conn.execute(
                    f"DELETE FROM {table} WHERE position_key IN ({marks})",
                    batch,
                )
        for batch in _batches(sorted(affected_txs)):
            marks = ",".join("?" for _ in batch)
            conn.execute(
                "DELETE FROM lp_accounting_tx_costs "
                f"WHERE tx_hash IN ({marks})",
                batch,
            )
        if not keys:
            self._finish_if_idle(conn, epoch=next_epoch)
        self._invalidate_cache(
            conn, affected_pools, owners=bool(keys),
        )

    def _invalidate_cache(
            self, conn: sqlite3.Connection, pool_ids: Iterable[str], *,
            owners: bool = True,
    ) -> None:
        pools = sorted({str(item).lower() for item in pool_ids})
        conn.executemany(
            "INSERT INTO lp_accounting_pool_generations(pool_id,generation) "
            "VALUES(?,1) ON CONFLICT(pool_id) DO UPDATE SET "
            "generation=lp_accounting_pool_generations.generation+1",
            ((pool_id,) for pool_id in pools),
        )
        if owners:
            with self._cache_lock:
                self._owners_generation += 1
                self._owners_cache.clear()
                self._owner_activity_cache.clear()

    def _event_rows(self, conn: sqlite3.Connection, key: str,
                    ids: Sequence[int] | None = None,
                    ancestor: int | None = None) -> list[dict[str, Any]]:
        clauses = ["k.position_key=?"]
        args: list[Any] = [key]
        if ids is not None:
            if not ids:
                return []
            clauses.append("e.id IN (" + ",".join("?" for _ in ids) + ")")
            args.extend(ids)
        if ancestor is not None:
            clauses.append("e.block_number<=?")
            args.append(ancestor)
        sql = (
            f"SELECT {_EVENT_SELECT} FROM lp_accounting_event_keys k "
            "JOIN events e ON e.id=k.event_id WHERE " + " AND ".join(clauses) +
            " ORDER BY e.block_number,e.tx_index,e.log_index,e.id"
        )
        return _dict_rows(conn.execute(sql, args))

    def _append_position(
        self, conn: sqlite3.Connection, key: str, event_ids: Sequence[int], *,
        refresh_values: bool = True,
        position: Mapping[str, Any] | None | object = _MISSING,
    ) -> bool:
        if not event_ids:
            return False
        if position is _MISSING:
            position = _one(conn.execute(
                "SELECT * FROM lp_accounting_positions WHERE position_key=?", (key,),
            ))
        if not isinstance(position, Mapping):
            return False
        marks = ",".join("?" for _ in event_ids)
        if conn.execute(
            f"SELECT 1 FROM lp_accounting_effects WHERE event_id IN ({marks}) LIMIT 1",
            list(event_ids),).fetchone() is not None:
            return False
        events = self._event_rows(conn, key, event_ids)
        if len(events) != len(event_ids):
            return False
        last = (int(position["last_block"]), int(position["last_tx_index"]),
                int(position["last_log_index"]), -1)
        # Pinned before/after snapshots are block-boundary reads.  A second
        # position event in the same block must replay the whole block so those
        # snapshots are applied only at the block edges.
        if any(_order(event)[:3] <= last[:3] or _order(event)[0] == last[0]
               for event in events):
            return False
        state = _json(position.get("state_json"))
        if not state or state.get("version") != _SCHEMA_VERSION:
            return False
        self._derive(conn, key, events, state, refresh_values=refresh_values)
        return True

    def _rebuild_position(
        self, conn: sqlite3.Connection, key: str, ancestor: int | None = None, *,
        refresh_values: bool = True, clear: bool = True,
    ) -> tuple[set[str], set[str]]:
        events = self._event_rows(conn, key, ancestor=ancestor)
        if events:
            return self._derive(
                conn, key, events, None, refresh_values=refresh_values,
                writes=_ReplayWrites(key) if clear else None,
            )
        if clear:
            affected_txs = {
                str(row[0]) for row in conn.execute(
                    "SELECT DISTINCT tx_hash FROM lp_accounting_effects "
                    "WHERE position_key=? AND tx_hash<>''", (key,),
                ).fetchall()
            }
            conn.execute("DELETE FROM lp_accounting_effects WHERE position_key=?", (key,))
            conn.execute("DELETE FROM lp_ownership_intervals WHERE position_key=?", (key,))
            conn.execute("DELETE FROM lp_accounting_episodes WHERE position_key=?", (key,))
            conn.execute("DELETE FROM lp_accounting_positions WHERE position_key=?", (key,))
            return affected_txs, set()
        return set(), set()

    @staticmethod
    def _new_state(key: str, event: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "version": _SCHEMA_VERSION,
            "position_key": key,
            "token_id": None if event.get("token_id") is None else str(event.get("token_id")),
            "pool_id": event.get("pool_id"),
            "protocol": event.get("protocol"),
            "tick_lower": event.get("tick_lower"),
            "tick_upper": event.get("tick_upper"),
            "liquidity": None,
            "liquidity_known": False,
            "pending0": None,
            "pending1": None,
            "pending_known": False,
            "owed0": None,
            "owed_block": None,
            "owed1": None,
            "owed_known": False,
            "settled": False,
            "owner": None,
            "custody": None,
            "identity_basis": "unknown",
            "minted_to_owner": False,
            "history_complete": False,
            "episode_ordinal": 0,
            "active_episode": None,
            "ownership_ordinal": 0,
            "active_ownership": None,
            "first_order": list(_order(event)),
            "first_timestamp": int(event.get("timestamp") or 0),
            "last_order": list(_order(event)),
            "last_timestamp": int(event.get("timestamp") or 0),
        }

    def _derive(
        self, conn: sqlite3.Connection, key: str,
        events: Sequence[Mapping[str, Any]], state: dict[str, Any] | None,
        *, refresh_values: bool = True,
        writes: _ReplayWrites | None = None,
        flush_writes: bool = True,
    ) -> tuple[set[str], set[str]]:
        if state is None:
            state = self._new_state(key, events[0])
        pool_cache: dict[str, dict[str, Any]] = {}
        state_events: dict[int, list[int]] = defaultdict(list)
        prepared_events: list[dict[str, Any]] = []
        for index, source_event in enumerate(events):
            event = dict(source_event)
            data = _json(event.get("data"))
            event["data"] = data
            prepared_events.append(event)
            if (_event_position_state(data, "before") is not None
                    or _event_position_state(data, "after") is not None):
                state_events[int(event.get("block_number") or 0)].append(index)
        # V3 manager transactions emit the pool event before the ERC-721 mint
        # and the final Collect before the ERC-721 burn.  Those verified
        # transfers prove the missing boundary state even though their logs
        # follow the financial event.
        first_add_by_tx: dict[tuple[int, int, str], dict[str, Any]] = {}
        last_collect_by_tx: dict[tuple[int, int, str], dict[str, Any]] = {}
        for event in prepared_events:
            tx = (
                int(event.get("block_number") or 0),
                int(event.get("tx_index") or 0),
                str(event.get("tx_hash") or "").lower(),
            )
            kind = str(event.get("kind") or "").lower()
            if kind == "add":
                first_add_by_tx.setdefault(tx, event)
            elif kind == "collect":
                last_collect_by_tx[tx] = event
            elif kind == "transfer":
                data = event["data"]
                if _flag(data.get("mint")) and tx in first_add_by_tx:
                    first_add_by_tx[tx]["data"]["_verified_mint_after"] = True
                if _flag(data.get("burn")) and tx in last_collect_by_tx:
                    last_collect_by_tx[tx]["data"]["_verified_burn_after"] = True
        for index, event in enumerate(prepared_events):
            data = event["data"]
            indices = state_events.get(int(event.get("block_number") or 0), [])
            if indices:
                if index != indices[0]:
                    data.pop("position_before", None)
                if index != indices[-1]:
                    data.pop("position_after", None)
            pool_id = event.get("pool_id") or state.get("pool_id")
            if pool_id and pool_id not in pool_cache:
                pool_cache[str(pool_id)] = self._pool_row(conn, str(pool_id))
            pool = pool_cache.get(str(pool_id), {}) if pool_id else {}
            self._process_event(conn, state, event, data, pool, writes)
        active = state.get("active_episode")
        if isinstance(active, Mapping):
            self._save_episode(conn, dict(active), writes)
        self._save_position(conn, state, writes)
        replay_changes = (
            writes.flush(conn)
            if writes is not None and flush_writes else (set(), set())
        )
        if refresh_values and (writes is None or flush_writes):
            self._refresh_position_value(conn, key)
            active = state.get("active_episode")
            if isinstance(active, Mapping):
                self._refresh_episode_value(conn, str(active["id"]))
        return replay_changes

    def _pool_row(self, conn: sqlite3.Connection, pool_id: str) -> dict[str, Any]:
        try:
            return _one(conn.execute("SELECT * FROM pools WHERE id=?", (pool_id,))) or {}
        except sqlite3.OperationalError:
            return {}

    @staticmethod
    def _event_owner(
        event: Mapping[str, Any], data: Mapping[str, Any],
    ) -> tuple[str | None, str | None, str]:
        owner = _address(event.get("owner"))
        custody = _address(event.get("custody"))
        basis = str(
            event.get("identity_basis")
            or data.get("identity_basis")
            or ("verified" if owner else "custody" if custody else "unknown")
        )
        return owner, custody, basis

    @staticmethod
    def _transfer_parties(
        event: Mapping[str, Any], data: Mapping[str, Any],
    ) -> tuple[str | None, str | None]:
        source = None
        target = None
        for name in ("from", "from_address", "prior_owner", "previous_owner", "sender"):
            source = _address(data.get(name))
            if source:
                break
        for name in ("to", "to_address", "new_owner", "recipient"):
            target = _address(data.get(name))
            if target:
                break
        if target is None:
            target = _address(event.get("owner"))
        if source is None and _flag(data.get("mint")):
            source = _ZERO_ADDRESS
        if target is None and _flag(data.get("burn")):
            target = _ZERO_ADDRESS
        return source, target

    def _process_transfer(
        self, conn: sqlite3.Connection, state: dict[str, Any],
        event: Mapping[str, Any], data: Mapping[str, Any],
        writes: _ReplayWrites | None = None,
    ) -> str | None:
        source, target = self._transfer_parties(event, data)
        target = None if target == _ZERO_ADDRESS else target
        source_zero = source == _ZERO_ADDRESS
        current_owner = _address(state.get("owner"))
        order = _order(event)
        timestamp = int(event.get("timestamp") or 0)
        ownership = state.get("active_ownership")
        if isinstance(ownership, Mapping) and (target != current_owner or target is None):
            finished = dict(ownership)
            finished.update({
                "end_block": order[0], "end_tx_index": order[1],
                "end_log_index": order[2], "end_timestamp": timestamp,
                "complete": bool(source is None or current_owner is None or source == current_owner),
            })
            self._save_ownership(conn, finished, writes)
            state["active_ownership"] = None
        active = state.get("active_episode")
        regular_transfer = not source_zero and target is not None and target != current_owner
        burn = target is None and source is not None and source != _ZERO_ADDRESS
        if isinstance(active, Mapping) and regular_transfer:
            old = dict(active)
            old.update({
                "closed_block": order[0], "closed_tx_index": order[1],
                "closed_log_index": order[2], "closed_at": timestamp,
                "last_timestamp": timestamp, "status": "transferred_out",
                "claims_complete": False,
            })
            self._save_episode(conn, old, writes)
            state["active_episode"] = None
            state["settled"] = False
        elif isinstance(active, Mapping) and burn:
            old = dict(active)
            if (state.get("liquidity_known") and int(state.get("liquidity") or 0) == 0
                    and state.get("pending_known")
                    and int(state.get("pending0") or 0) == 0
                    and int(state.get("pending1") or 0) == 0):
                old.update({
                    "closed_block": order[0], "closed_tx_index": order[1],
                    "closed_log_index": order[2], "closed_at": timestamp,
                    "last_timestamp": timestamp, "status": "complete",
                    "claims_complete": True,
                    "close_price0_usd": _finite_float(event.get("price0_usd")),
                    "close_price1_usd": _finite_float(event.get("price1_usd")),
                })
                state["settled"] = True
            else:
                old.update({
                    "closed_block": order[0], "closed_tx_index": order[1],
                    "closed_log_index": order[2], "closed_at": timestamp,
                    "last_timestamp": timestamp, "status": "nft_burn_unsettled",
                    "claims_complete": False,
                })
                state["settled"] = False
            self._save_episode(conn, old, writes)
            state["active_episode"] = None
        state["owner"] = target
        event_owner, custody, basis = self._event_owner(event, data)
        if target is not None:
            state["identity_basis"] = basis if event_owner == target else "verified_nft_transfer"
            if custody:
                state["custody"] = custody
            if (
                source_zero and target == current_owner
                and isinstance(ownership, Mapping)
                and ownership.get("acquired_by") == "mint"
            ):
                state["minted_to_owner"] = True
                state["settled"] = False
                return source
            if source == current_owner and target == current_owner:
                state["minted_to_owner"] = False
                return source
            state["ownership_ordinal"] = int(state.get("ownership_ordinal") or 0) + 1
            acquired = "mint" if source_zero else "transfer"
            interval = {
                "position_key": state["position_key"],
                "ordinal": state["ownership_ordinal"],
                "token_id": state.get("token_id") or event.get("token_id"),
                "owner": target,
                "custody": state.get("custody"),
                "identity_basis": state["identity_basis"],
                "acquired_by": acquired,
                "start_block": order[0], "start_tx_index": order[1],
                "start_log_index": order[2], "start_timestamp": timestamp,
                "end_block": None, "end_tx_index": None, "end_log_index": None,
                "end_timestamp": None, "complete": True,
            }
            state["active_ownership"] = interval
            self._save_ownership(conn, interval, writes)
            state["minted_to_owner"] = source_zero
            if source_zero:
                state["settled"] = False
            has_transferred_inventory = bool(
                not state.get("liquidity_known")
                or int(state.get("liquidity") or 0) > 0
                or not state.get("pending_known")
                or int(state.get("pending0") or 0) != 0
                or int(state.get("pending1") or 0) != 0
                or not state.get("owed_known")
                or int(state.get("owed0") or 0) != 0
                or int(state.get("owed1") or 0) != 0
            )
            if regular_transfer and has_transferred_inventory:
                state["active_episode"] = self._new_episode(
                    state, event, history_complete=bool(state.get("history_complete")),
                    transferred_basis=True,
                )
                state["settled"] = False
        elif burn:
            state["identity_basis"] = "burned"
        return source

    def _update_identity(
        self, conn: sqlite3.Connection, state: dict[str, Any],
        event: Mapping[str, Any], data: Mapping[str, Any],
        writes: _ReplayWrites | None = None,
    ) -> None:
        owner, custody, basis = self._event_owner(event, data)
        if custody:
            state["custody"] = custody
        if owner is None:
            return
        current = _address(state.get("owner"))
        if current is None:
            state["owner"] = owner
            state["identity_basis"] = basis
            active = state.get("active_episode")
            if isinstance(active, dict) and active.get("owner") is None:
                active["owner"] = owner
                active["identity_basis"] = basis
                active["identity_complete"] = True
            if state.get("active_ownership") is None:
                order = _order(event)
                state["ownership_ordinal"] = int(state.get("ownership_ordinal") or 0) + 1
                verified_mint = _flag(data.get("_verified_mint_after"))
                interval = {
                    "position_key": state["position_key"],
                    "ordinal": state["ownership_ordinal"],
                    "token_id": state.get("token_id") or event.get("token_id"),
                    "owner": owner, "custody": state.get("custody"),
                    "identity_basis": basis,
                    "acquired_by": "mint" if verified_mint else "observed",
                    "start_block": order[0], "start_tx_index": order[1],
                    "start_log_index": order[2],
                    "start_timestamp": int(event.get("timestamp") or 0),
                    "end_block": None, "end_tx_index": None,
                    "end_log_index": None, "end_timestamp": None,
                    "complete": verified_mint,
                }
                state["active_ownership"] = interval
                self._save_ownership(conn, interval, writes)
        elif current != owner:
            active = state.get("active_episode")
            if isinstance(active, dict):
                active["identity_complete"] = False
                active["identity_conflict"] = True

    def _process_event(
        self, conn: sqlite3.Connection, state: dict[str, Any],
        event: Mapping[str, Any], data: Mapping[str, Any],
        pool: Mapping[str, Any], writes: _ReplayWrites | None = None,
    ) -> None:
        kind = str(event.get("kind") or "unknown").lower()
        protocol = str(event.get("protocol") or state.get("protocol") or "unknown").lower()
        if protocol != "nft":
            state["protocol"] = protocol
        if event.get("pool_id"):
            state["pool_id"] = str(event["pool_id"])
        if event.get("token_id") is not None:
            state["token_id"] = str(event["token_id"])
        if event.get("tick_lower") is not None:
            state["tick_lower"] = int(event["tick_lower"])
        if event.get("tick_upper") is not None:
            state["tick_upper"] = int(event["tick_upper"])
        transfer_source = None
        if kind == "transfer" or protocol == "nft":
            transfer_source = self._process_transfer(
                conn, state, event, data, writes,
            )
        else:
            self._update_identity(conn, state, event, data, writes)
        before = _event_position_state(data, "before")
        after = _event_position_state(data, "after")
        before_liq = _state_number(before, "liquidity")
        after_liq = _state_number(after, "liquidity")
        delta = _raw_int(event.get("liquidity_delta"))
        if delta is not None:
            if kind == "add" and delta < 0:
                delta = -delta
            elif kind == "remove" and delta > 0:
                delta = -delta
        verified_mint = _flag(data.get("_verified_mint_after"))
        previous_known = bool(state.get("liquidity_known"))
        previous_liq = int(state.get("liquidity") or 0) if previous_known else None
        if verified_mint:
            previous_liq = 0
            previous_known = True
        if before_liq is not None:
            previous_liq = before_liq
            previous_known = True
        if after_liq is None and previous_known and delta is not None:
            after_liq = previous_liq + delta  # type: ignore[operator]
        if after_liq is not None and after_liq < 0:
            # A malformed or mismatched delta cannot establish position state.
            after_liq = None
        before_owed0 = _state_number(before, "tokens_owed0", "tokensOwed0")
        before_owed1 = _state_number(before, "tokens_owed1", "tokensOwed1")
        claims_known_nonempty = bool(
            before is not None and before.get("claims_empty") is False
            or before_owed0 is not None and before_owed0 > 0
            or before_owed1 is not None and before_owed1 > 0
        )
        before_claims_empty = (
            before.get("claims_empty") is True if before is not None else False
        )
        if before_owed0 is not None and before_owed1 is not None:
            before_claims_empty = before_owed0 == 0 and before_owed1 == 0
        prior_claims_empty = bool(
            verified_mint
            or (
                not claims_known_nonempty
                and (
                    before_claims_empty
                    or state.get("minted_to_owner")
                    or state.get("settled")
                    or (
                        state.get("pending_known")
                        and int(state.get("pending0") or 0) == 0
                        and int(state.get("pending1") or 0) == 0
                        and state.get("owed_known")
                        and int(state.get("owed0") or 0) == 0
                        and int(state.get("owed1") or 0) == 0
                    )
                )
            )
        )
        zero_liquidity_proven = bool(
            verified_mint
            or previous_known and previous_liq == 0
            or _flag(data.get("zero_baseline"))
            or state.get("minted_to_owner") and kind == "add" and previous_liq in (None, 0)
        )
        zero_proven = zero_liquidity_proven and prior_claims_empty
        if after_liq is not None:
            state["liquidity"] = str(after_liq)
            state["liquidity_known"] = True
        elif delta is not None and not previous_known:
            state["liquidity"] = None
            state["liquidity_known"] = False
        if zero_proven:
            state["history_complete"] = True
            ownership = state.get("active_ownership")
            if isinstance(ownership, dict) and ownership.get("acquired_by") == "observed":
                ownership["complete"] = True
                self._save_ownership(conn, ownership, writes)
            if not state.get("pending_known"):
                state["pending0"] = "0"
                state["pending1"] = "0"
                state["pending_known"] = True
            if verified_mint:
                state["owed0"] = "0"
                state["owed1"] = "0"
                state["owed_known"] = True
                state["owed_block"] = int(event.get("block_number") or 0)
        active = state.get("active_episode")
        begins = kind == "add" and delta is not None and delta > 0 and (
            active is None or previous_liq == 0)
        ambiguous_reentry = bool(
            begins and zero_liquidity_proven and not prior_claims_empty
        )
        if begins and isinstance(active, Mapping):
            old = dict(active)
            settled_prior = (
                state.get("pending_known") and int(state.get("pending0") or 0) == 0
                and int(state.get("pending1") or 0) == 0 and old.get("claims_complete")
            )
            if not settled_prior:
                ambiguous_reentry = True
                old.update({
                    "closed_block": _order(event)[0], "closed_tx_index": _order(event)[1],
                    "closed_log_index": _order(event)[2],
                    "closed_at": int(event.get("timestamp") or 0),
                    "last_timestamp": int(event.get("timestamp") or 0),
                    "status": "ambiguous_reentry", "ambiguous_reentry": True,
                    "claims_complete": False,
                })
                self._save_episode(conn, old, writes)
            state["active_episode"] = None
            active = None
        if begins and active is None:
            state["settled"] = False
            active = self._new_episode(
                state, event,
                history_complete=zero_proven and bool(state.get("history_complete")),
                transferred_basis=False,
            )
            if ambiguous_reentry:
                active["ambiguous_reentry"] = True
                active["fees_complete"] = False
                active["status"] = "ambiguous_reentry"
            state["active_episode"] = active
        elif active is None and kind in ("remove", "collect", "checkpoint"):
            active = self._new_episode(
                state, event, history_complete=False, transferred_basis=False,
            )
            active["status"] = "partial_history"
            state["active_episode"] = active
        effect = self._financial_effect(state, event, data, pool, active)
        if isinstance(active, dict):
            self._apply_effect(active, effect, event)
        self._update_claim_state(state, event, before, after, effect)
        active = state.get("active_episode")
        if isinstance(active, dict):
            current_liq = int(state.get("liquidity") or 0) if state.get(
                "liquidity_known") else None
            if kind == "remove" and delta == 0:
                active["status"] = "open" if current_liq else active.get("status", "open")
            elif current_liq == 0:
                if self._claims_settled(state, protocol, data):
                    active.update({
                        "closed_block": _order(event)[0],
                        "closed_tx_index": _order(event)[1],
                        "closed_log_index": _order(event)[2],
                        "closed_at": int(event.get("timestamp") or 0),
                        "last_timestamp": int(event.get("timestamp") or 0),
                        "status": "complete", "claims_complete": True,
                        "close_price0_usd": _finite_float(event.get("price0_usd")),
                        "close_price1_usd": _finite_float(event.get("price1_usd")),
                    })
                    if (
                        protocol == "v3"
                        and active.get("history_complete")
                        and not active.get("transferred_basis")
                        and not active.get("ambiguous_reentry")
                        and active.get("release_complete")
                    ):
                        released0 = int(active.get("released0") or 0)
                        released1 = int(active.get("released1") or 0)
                        proceeds0 = int(active.get("proceeds0") or 0)
                        proceeds1 = int(active.get("proceeds1") or 0)
                        if proceeds0 >= released0 and proceeds1 >= released1:
                            active["principal_withdrawal0"] = released0
                            active["principal_withdrawal1"] = released1
                            active["fees0"] = proceeds0 - released0
                            active["fees1"] = proceeds1 - released1
                    self._save_episode(conn, active, writes)
                    state["settled"] = True
                    state["active_episode"] = None
                else:
                    active["status"] = "awaiting_claim"
                    active["claims_complete"] = False
            elif current_liq is not None and current_liq > 0:
                active["status"] = "open"
        if kind == "transfer" or protocol == "nft":
            effect["gas_owner"] = (
                None if transfer_source == _ZERO_ADDRESS else transfer_source
            )
            effect["episode_id"] = None
        else:
            effect["episode_id"] = (
                state.get("active_episode", {}).get("id")
                if isinstance(state.get("active_episode"), Mapping)
                else active.get("id") if isinstance(active, Mapping) else None
            )
        self._save_effect(conn, state, event, effect, writes)
        state["last_order"] = list(_order(event))
        state["last_timestamp"] = int(event.get("timestamp") or 0)

    def _new_episode(self, state: Mapping[str, Any], event: Mapping[str, Any],
                     *, history_complete: bool,
                     transferred_basis: bool) -> dict[str, Any]:
        ordinal = int(state.get("episode_ordinal") or 0) + 1
        # state is always a mutable dict at call sites.
        state["episode_ordinal"] = ordinal  # type: ignore[index]
        protocol = str(state.get("protocol") or event.get("protocol") or "unknown")
        order = _order(event)
        owner = _address(state.get("owner"))
        episode_id = f"{state['position_key']}:{ordinal}"
        return {
            "id": episode_id, "position_key": state["position_key"],
            "ordinal": ordinal, "token_id": state.get("token_id"),
            "pool_id": state.get("pool_id"), "protocol": protocol,
            "owner": owner, "custody": _address(state.get("custody")),
            "identity_basis": state.get("identity_basis") or "unknown",
            "tick_lower": state.get("tick_lower"), "tick_upper": state.get("tick_upper"),
            "opened_block": order[0], "opened_tx_index": order[1],
            "opened_log_index": order[2], "opened_at": int(event.get("timestamp") or 0),
            "closed_block": None, "closed_tx_index": None, "closed_log_index": None,
            "closed_at": None, "last_timestamp": int(event.get("timestamp") or 0),
            "status": "open", "history_complete": bool(history_complete),
            "identity_complete": owner is not None,
            "transferred_basis": bool(transferred_basis),
            "ambiguous_reentry": False, "identity_conflict": False,
            "trace_complete": True, "claims_complete": False,
            "cashflow_complete": True, "pricing_complete": True,
            "fees_complete": protocol in ("v3", "v4") and not transferred_basis,
            "deposit0": 0, "deposit1": 0, "proceeds0": 0, "proceeds1": 0,
            "released0": 0, "released1": 0, "release_complete": True,
            "principal_withdrawal0": 0, "principal_withdrawal1": 0,
            "fees0": 0, "fees1": 0,
            "deposit_usd": 0.0, "proceeds_usd": 0.0,
            "withdrawal_usd": 0.0, "fees_usd": 0.0,
            "close_price0_usd": None, "close_price1_usd": None,
            "accounting_basis": "canonical_events",
        }

    def _financial_effect(
        self, state: Mapping[str, Any], event: Mapping[str, Any],
        data: Mapping[str, Any], pool: Mapping[str, Any],
        active: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        kind = str(event.get("kind") or "unknown").lower()
        protocol = str(state.get("protocol") or event.get("protocol") or "unknown").lower()
        amount0 = _raw_int(event.get("amount0"))
        amount1 = _raw_int(event.get("amount1"))
        fee0 = _raw_int(event.get("fee_amount0"))
        fee1 = _raw_int(event.get("fee_amount1"))
        result: dict[str, Any] = {
            "deposit0": 0, "deposit1": 0, "proceeds0": 0, "proceeds1": 0,
            "released0": 0, "released1": 0,
            "principal_withdrawal0": 0, "principal_withdrawal1": 0,
            "fees0": 0, "fees1": 0, "deposit_usd": 0.0,
            "proceeds_usd": 0.0, "withdrawal_usd": 0.0, "fees_usd": 0.0,
            "cashflow_exact": True, "pricing_exact": True,
            "fees_exact": True, "allocation_exact": True, "release_exact": True,
            "trace_exact": True,
            "basis": str(event.get("accounting_basis") or "canonical_event"),
        }
        if protocol == "v4" and kind in ("add", "remove", "collect", "checkpoint"):
            trace_ok = _flag(data.get("trace_complete"))
            cash0 = _raw_int(event.get("cashflow0"))
            cash1 = _raw_int(event.get("cashflow1"))
            if cash0 is None:
                cash0 = _state_number(data.get("caller_delta") if isinstance(
                    data.get("caller_delta"), Mapping) else None, "amount0")
            if cash1 is None:
                cash1 = _state_number(data.get("caller_delta") if isinstance(
                    data.get("caller_delta"), Mapping) else None, "amount1")
            exact_fees = _flag(data.get("fees_accrued_exact"))
            accrued = data.get("fees_accrued") if isinstance(data.get("fees_accrued"), Mapping) else None
            if fee0 is None:
                fee0 = _state_number(accrued, "amount0")
            if fee1 is None:
                fee1 = _state_number(accrued, "amount1")
            result["trace_exact"] = trace_ok
            if not trace_ok or cash0 is None or cash1 is None:
                result["cashflow_exact"] = False
                result["pricing_exact"] = False
            else:
                result["deposit0"], result["deposit1"] = max(0, -cash0), max(0, -cash1)
                result["proceeds0"], result["proceeds1"] = max(0, cash0), max(0, cash1)
            principal_delta = (
                data.get("principal_delta")
                if isinstance(data.get("principal_delta"), Mapping) else None
            )
            principal0 = _state_number(principal_delta, "amount0")
            principal1 = _state_number(principal_delta, "amount1")
            if (_flag(data.get("principal_delta_exact"))
                    and principal0 is not None and principal1 is not None):
                result["principal_withdrawal0"] = max(0, principal0)
                result["principal_withdrawal1"] = max(0, principal1)
            else:
                result["allocation_exact"] = False
            if not exact_fees or fee0 is None or fee1 is None:
                result["fees_exact"] = False
            else:
                result["fees0"], result["fees1"] = max(0, fee0), max(0, fee1)
            result["basis"] = str(data.get("cashflow_basis") or "v4_caller_delta")
        elif protocol == "v3":
            if kind == "add":
                if amount0 is None or amount1 is None:
                    result["cashflow_exact"] = False
                    result["pricing_exact"] = False
                else:
                    result["deposit0"], result["deposit1"] = abs(amount0), abs(amount1)
            elif kind == "collect":
                if amount0 is None or amount1 is None:
                    result["cashflow_exact"] = False
                    result["pricing_exact"] = False
                    result["fees_exact"] = False
                    result["allocation_exact"] = False
                else:
                    collected0, collected1 = abs(amount0), abs(amount1)
                    result["proceeds0"], result["proceeds1"] = collected0, collected1
                    pending_known = bool(state.get("pending_known"))
                    pending0 = int(state.get("pending0") or 0) if pending_known else None
                    pending1 = int(state.get("pending1") or 0) if pending_known else None
                    if fee0 is not None and fee1 is not None:
                        fees0, fees1 = max(0, fee0), max(0, fee1)
                        principal0 = max(0, collected0 - min(collected0, fees0))
                        principal1 = max(0, collected1 - min(collected1, fees1))
                    else:
                        after_state = _event_position_state(data, "after")
                        final_settlement = bool(
                            (
                                after_state is not None
                                and after_state.get("claims_empty") is True
                                or _flag(data.get("_verified_burn_after"))
                            )
                            and pending0 is not None and pending1 is not None
                            and collected0 >= pending0 and collected1 >= pending1
                        )
                        fees_only = pending0 == 0 and pending1 == 0
                        if pending0 is not None and pending1 is not None and (
                                final_settlement or fees_only):
                            principal0, principal1 = pending0, pending1
                            fees0, fees1 = collected0 - principal0, collected1 - principal1
                        else:
                            principal0 = principal1 = fees0 = fees1 = 0
                            result["fees_exact"] = False
                            result["allocation_exact"] = False
                    result["principal_withdrawal0"] = principal0
                    result["principal_withdrawal1"] = principal1
                    result["fees0"], result["fees1"] = fees0, fees1
            elif kind == "remove":
                # Burn only converts liquidity principal into tokensOwed.  It is
                # not a wallet receipt and therefore never enters proceeds.
                if amount0 is None or amount1 is None:
                    result["fees_exact"] = False
                    result["release_exact"] = False
                else:
                    result["released0"], result["released1"] = abs(amount0), abs(amount1)
            result["basis"] = "v3_mint_burn_collect"
        elif protocol == "v2":
            result["fees_exact"] = False
            if kind == "add":
                if amount0 is None or amount1 is None:
                    result["cashflow_exact"] = False
                    result["pricing_exact"] = False
                else:
                    result["deposit0"], result["deposit1"] = abs(amount0), abs(amount1)
            elif kind == "remove":
                if amount0 is None or amount1 is None:
                    result["cashflow_exact"] = False
                    result["pricing_exact"] = False
                else:
                    result["proceeds0"], result["proceeds1"] = abs(amount0), abs(amount1)
                    result["principal_withdrawal0"] = result["proceeds0"]
                    result["principal_withdrawal1"] = result["proceeds1"]
            result["basis"] = "v2_observed_cashflow"
        elif kind in ("add", "remove", "collect"):
            cash0, cash1 = _raw_int(event.get("cashflow0")), _raw_int(event.get("cashflow1"))
            if cash0 is None or cash1 is None:
                result["cashflow_exact"] = False
                result["pricing_exact"] = False
            else:
                result["deposit0"], result["deposit1"] = max(0, -cash0), max(0, -cash1)
                result["proceeds0"], result["proceeds1"] = max(0, cash0), max(0, cash1)
            result["fees_exact"] = False
        decimals0 = _raw_int(pool.get("decimals0"))
        decimals1 = _raw_int(pool.get("decimals1"))
        for prefix, field in (
            ("deposit", "deposit_usd"), ("proceeds", "proceeds_usd"),
            ("principal_withdrawal", "withdrawal_usd"), ("fees", "fees_usd"),
        ):
            value = _token_value(
                int(result[f"{prefix}0"]), int(result[f"{prefix}1"]),
                event.get("price0_usd"), event.get("price1_usd"),
                decimals0, decimals1,
            )
            if value is None:
                if prefix == "deposit":
                    value = _finite_float(event.get("deposit_usd"))
                elif prefix == "proceeds":
                    value = _finite_float(event.get("withdrawal_usd"))
                elif prefix == "fees":
                    value = _finite_float(event.get("fees_usd"))
                elif prefix == "principal_withdrawal":
                    withdrawal_value = _finite_float(event.get("withdrawal_usd"))
                    fee_value = _finite_float(event.get("fees_usd"))
                    if protocol == "v3" and kind == "collect":
                        value = (
                            max(0.0, withdrawal_value - fee_value)
                            if withdrawal_value is not None and fee_value is not None else None
                        )
                    else:
                        value = withdrawal_value
            result[field] = value
            if value is None and (result[f"{prefix}0"] or result[f"{prefix}1"]):
                result["pricing_exact"] = False
        if not result.get("allocation_exact"):
            result["withdrawal_usd"] = None
            if not result.get("fees_exact"):
                result["fees_usd"] = None
        return result

    @staticmethod
    def _apply_effect(episode: dict[str, Any], effect: Mapping[str, Any],
                      event: Mapping[str, Any]) -> None:
        for name in ("deposit0", "deposit1", "proceeds0", "proceeds1",
                     "released0", "released1", "principal_withdrawal0",
                     "principal_withdrawal1", "fees0", "fees1"):
            episode[name] = int(episode.get(name) or 0) + int(effect.get(name) or 0)
        for name in ("deposit_usd", "proceeds_usd", "withdrawal_usd", "fees_usd"):
            if episode.get(name) is not None:
                value = effect.get(name)
                episode[name] = None if value is None else float(episode[name]) + float(value)
        if not effect.get("cashflow_exact"):
            episode["cashflow_complete"] = False
        if not effect.get("pricing_exact"):
            episode["pricing_complete"] = False
        if not effect.get("fees_exact"):
            episode["fees_complete"] = False
        if not effect.get("release_exact"):
            episode["release_complete"] = False
        if not effect.get("trace_exact"):
            episode["trace_complete"] = False
        episode["accounting_basis"] = str(effect.get("basis") or episode["accounting_basis"])
        episode["last_timestamp"] = int(event.get("timestamp") or episode["last_timestamp"])

    @staticmethod
    def _update_claim_state(state: dict[str, Any], event: Mapping[str, Any],
                            before: Mapping[str, Any] | None,
                            after: Mapping[str, Any] | None,
                            effect: Mapping[str, Any]) -> None:
        kind = str(event.get("kind") or "").lower()
        protocol = str(state.get("protocol") or event.get("protocol") or "").lower()
        if protocol == "v3":
            if kind == "remove":
                amount0, amount1 = _raw_int(event.get("amount0")), _raw_int(event.get("amount1"))
                if state.get("pending_known") and amount0 is not None and amount1 is not None:
                    state["pending0"] = str(int(state.get("pending0") or 0) + abs(amount0))
                    state["pending1"] = str(int(state.get("pending1") or 0) + abs(amount1))
                elif amount0 is None or amount1 is None:
                    state["pending_known"] = False
                    state["pending0"] = state["pending1"] = None
            elif kind == "collect":
                if state.get("pending_known") and effect.get("allocation_exact"):
                    state["pending0"] = str(max(0, int(state.get("pending0") or 0) - int(
                        effect.get("principal_withdrawal0") or 0)))
                    state["pending1"] = str(max(0, int(state.get("pending1") or 0) - int(
                        effect.get("principal_withdrawal1") or 0)))
                elif not effect.get("allocation_exact"):
                    state["pending_known"] = False
                    state["pending0"] = state["pending1"] = None
        owed0 = _state_number(after, "tokens_owed0", "tokensOwed0")
        owed1 = _state_number(after, "tokens_owed1", "tokensOwed1")
        if after is not None and after.get("claims_empty") is True:
            owed0 = 0 if owed0 is None else owed0
            owed1 = 0 if owed1 is None else owed1
        event_data = event.get("data")
        if isinstance(event_data, Mapping) and _flag(
                event_data.get("_verified_burn_after")):
            owed0 = 0
            owed1 = 0
        if owed0 is not None and owed1 is not None:
            state["owed0"], state["owed1"] = str(owed0), str(owed1)
            state["owed_known"] = True
            state["owed_block"] = int(event.get("block_number") or 0)
            if owed0 == 0 and owed1 == 0 and str(state.get("liquidity") or "") == "0":
                state["pending0"] = state["pending1"] = "0"
                state["pending_known"] = True

    @staticmethod
    def _claims_settled(state: Mapping[str, Any], protocol: str,
                        data: Mapping[str, Any]) -> bool:
        if not state.get("liquidity_known") or int(state.get("liquidity") or 0) != 0:
            return False
        if protocol == "v2":
            return True
        if protocol == "v4":
            return _flag(data.get("trace_complete"))
        return bool(
            state.get("pending_known") and int(state.get("pending0") or 0) == 0
            and int(state.get("pending1") or 0) == 0
            and state.get("owed_known") and int(state.get("owed0") or 0) == 0
            and int(state.get("owed1") or 0) == 0
        )

    def _save_effect(
        self, conn: sqlite3.Connection, state: Mapping[str, Any],
        event: Mapping[str, Any], effect: Mapping[str, Any],
        writes: _ReplayWrites | None = None,
    ) -> None:
        sql = _EFFECT_WRITE_SQL
        row = (
            int(event["id"]), state["position_key"], effect.get("episode_id"),
            _address(effect.get("gas_owner") or state.get("owner")),
            _address(state.get("custody")),
            str(event.get("tx_hash") or "").lower(), int(event.get("block_number") or 0),
            int(event.get("tx_index") or 0), int(event.get("log_index") or 0),
            int(event.get("timestamp") or 0), str(event.get("kind") or "unknown"),
            str(effect.get("deposit0")) if effect.get("cashflow_exact") else None,
            str(effect.get("deposit1")) if effect.get("cashflow_exact") else None,
            str(effect.get("proceeds0")) if effect.get("cashflow_exact") else None,
            str(effect.get("proceeds1")) if effect.get("cashflow_exact") else None,
            str(effect.get("principal_withdrawal0"))
            if effect.get("cashflow_exact") and effect.get("allocation_exact") else None,
            str(effect.get("principal_withdrawal1"))
            if effect.get("cashflow_exact") and effect.get("allocation_exact") else None,
            str(effect.get("fees0")) if effect.get("fees_exact") else None,
            str(effect.get("fees1")) if effect.get("fees_exact") else None,
            effect.get("deposit_usd"), effect.get("proceeds_usd"),
            effect.get("withdrawal_usd"), effect.get("fees_usd"),
            int(bool(effect.get("cashflow_exact") and effect.get("pricing_exact")
                     and effect.get("trace_exact") and effect.get("allocation_exact"))),
            str(effect.get("basis") or "unknown"),
        )
        if writes is None:
            conn.execute(sql, row)
        else:
            writes.add("effects", int(event["id"]), sql, row)

    @staticmethod
    def _save_ownership(
        conn: sqlite3.Connection, interval: Mapping[str, Any],
        writes: _ReplayWrites | None = None,
    ) -> None:
        sql = (
            "INSERT OR REPLACE INTO lp_ownership_intervals("
            "position_key,ordinal,token_id,owner,custody,identity_basis,acquired_by,"
            "start_block,start_tx_index,start_log_index,start_timestamp,end_block,"
            "end_tx_index,end_log_index,end_timestamp,complete) VALUES("
            + ",".join("?" for _ in range(16)) + ")"
        )
        row = (
            interval["position_key"], int(interval["ordinal"]), interval.get("token_id"),
            interval["owner"], interval.get("custody"), interval["identity_basis"],
            interval["acquired_by"], int(interval["start_block"]),
            int(interval["start_tx_index"]), int(interval["start_log_index"]),
            int(interval["start_timestamp"]), interval.get("end_block"),
            interval.get("end_tx_index"), interval.get("end_log_index"),
            interval.get("end_timestamp"), int(bool(interval.get("complete"))),
        )
        if writes is None:
            conn.execute(sql, row)
        else:
            writes.add(
                "ownership",
                (str(interval["position_key"]), int(interval["ordinal"])),
                sql, row,
            )

    @staticmethod
    def _episode_qualifiers(episode: Mapping[str, Any]) -> list[str]:
        reasons: list[str] = []
        for flag, reason in (
            (not bool(episode.get("history_complete")), "partial_history"),
            (not bool(episode.get("identity_complete")), "unknown_identity"),
            (bool(episode.get("transferred_basis")), "transferred_basis_unknown"),
            (bool(episode.get("ambiguous_reentry")), "ambiguous_reentry"),
            (not bool(episode.get("trace_complete")), "missing_trace"),
            (not bool(episode.get("cashflow_complete")), "incomplete_cashflows"),
            (not bool(episode.get("pricing_complete")), "incomplete_pricing"),
        ):
            if flag and reason not in reasons:
                reasons.append(reason)
        if episode.get("status") == "awaiting_claim":
            reasons.append("awaiting_claim")
        if episode.get("status") == "nft_burn_unsettled":
            reasons.append("unsettled_claims")
        return reasons

    def _save_episode(
        self, conn: sqlite3.Connection, episode: Mapping[str, Any],
        writes: _ReplayWrites | None = None,
    ) -> None:
        qualifiers = self._episode_qualifiers(episode)
        sql = (
            "INSERT OR REPLACE INTO lp_accounting_episodes("
            "id,position_key,ordinal,token_id,pool_id,protocol,owner,custody,identity_basis,"
            "tick_lower,tick_upper,opened_block,opened_tx_index,opened_log_index,opened_at,"
            "closed_block,closed_tx_index,closed_log_index,closed_at,last_timestamp,status,"
            "history_complete,identity_complete,transferred_basis,ambiguous_reentry,"
            "trace_complete,claims_complete,cashflow_complete,pricing_complete,fees_complete,"
            "deposit0,deposit1,proceeds0,proceeds1,principal_withdrawal0,"
            "principal_withdrawal1,fees0,fees1,deposit_usd,proceeds_usd,withdrawal_usd,fees_usd,"
            "close_price0_usd,close_price1_usd,current_equity_usd,lp_value_usd,hold_value_usd,"
            "lp_vs_hold_usd,gross_pnl_usd,gas_usd,net_pnl_usd,return_pct,accounting_basis,qualifiers) "
            "VALUES(" + ",".join("?" for _ in range(54)) + ")"
        )
        row = (
            episode["id"], episode["position_key"], int(episode["ordinal"]),
            episode.get("token_id"), episode.get("pool_id"), episode.get("protocol"),
            _address(episode.get("owner")), _address(episode.get("custody")),
            episode.get("identity_basis"), episode.get("tick_lower"),
            episode.get("tick_upper"), int(episode["opened_block"]),
            int(episode["opened_tx_index"]), int(episode["opened_log_index"]),
            int(episode["opened_at"]), episode.get("closed_block"),
            episode.get("closed_tx_index"), episode.get("closed_log_index"),
            episode.get("closed_at"), int(episode["last_timestamp"]), episode["status"],
            int(bool(episode.get("history_complete"))),
            int(bool(episode.get("identity_complete"))),
            int(bool(episode.get("transferred_basis"))),
            int(bool(episode.get("ambiguous_reentry"))),
            int(bool(episode.get("trace_complete"))),
            int(bool(episode.get("claims_complete"))),
            int(bool(episode.get("cashflow_complete"))),
            int(bool(episode.get("pricing_complete"))),
            int(bool(episode.get("fees_complete"))),
            str(episode.get("deposit0") or 0), str(episode.get("deposit1") or 0),
            str(episode.get("proceeds0") or 0), str(episode.get("proceeds1") or 0),
            str(episode.get("principal_withdrawal0") or 0),
            str(episode.get("principal_withdrawal1") or 0),
            str(episode.get("fees0") or 0), str(episode.get("fees1") or 0),
            episode.get("deposit_usd"), episode.get("proceeds_usd"),
            episode.get("withdrawal_usd"), episode.get("fees_usd"),
            episode.get("close_price0_usd"), episode.get("close_price1_usd"),
            episode.get("current_equity_usd"), episode.get("lp_value_usd"),
            episode.get("hold_value_usd"), episode.get("lp_vs_hold_usd"),
            episode.get("gross_pnl_usd"), episode.get("gas_usd"),
            episode.get("net_pnl_usd"), episode.get("return_pct"),
            episode.get("accounting_basis") or "canonical_events",
            json.dumps(qualifiers, separators=(",", ":")),
        )
        if writes is None:
            conn.execute(sql, row)
        else:
            writes.add("episodes", str(episode["id"]), sql, row)

    def _save_position(
        self, conn: sqlite3.Connection, state: Mapping[str, Any],
        writes: _ReplayWrites | None = None,
    ) -> None:
        active = state.get("active_episode")
        active_id = active.get("id") if isinstance(active, Mapping) else None
        if active_id:
            status = str(active.get("status") or "open")
        elif state.get("liquidity_known") and int(state.get("liquidity") or 0) == 0:
            status = "closed"
        else:
            status = "unresolved"
        settled = bool(state.get("settled") and status == "closed")
        first_order = state["first_order"]
        last_order = state["last_order"]
        sql = (
            "INSERT OR REPLACE INTO lp_accounting_positions("
            "position_key,token_id,pool_id,protocol,owner,custody,identity_basis,tick_lower,"
            "tick_upper,liquidity,liquidity_known,pending_principal0,pending_principal1,"
            "pending_known,tokens_owed0,tokens_owed1,owed_known,active_episode_id,status,"
            "history_complete,first_block,first_timestamp,last_block,last_tx_index,last_log_index,"
            "last_timestamp,principal0,principal1,principal_usd,uncollected_fees_usd,equity_usd,"
            "valuation_block,valuation_timestamp,valuation_basis,state_json) VALUES("
            + ",".join("?" for _ in range(35)) + ")"
        )
        row = (
            state["position_key"], state.get("token_id"), state.get("pool_id"),
            state.get("protocol"), _address(state.get("owner")),
            _address(state.get("custody")), state.get("identity_basis"),
            state.get("tick_lower"), state.get("tick_upper"), state.get("liquidity"),
            int(bool(state.get("liquidity_known"))), state.get("pending0"),
            state.get("pending1"), int(bool(state.get("pending_known"))),
            state.get("owed0"), state.get("owed1"), int(bool(state.get("owed_known"))),
            active_id, status, int(bool(state.get("history_complete"))),
            int(first_order[0]), int(state["first_timestamp"]), int(last_order[0]),
            int(last_order[1]), int(last_order[2]), int(state["last_timestamp"]),
            "0" if settled else None, "0" if settled else None,
            0.0 if settled else None, 0.0 if settled else None,
            0.0 if settled else None, None, None,
            "settled_zero" if settled else "unvalued",
            json.dumps(state, separators=(",", ":"), sort_keys=True),
        )
        if writes is None:
            conn.execute(sql, row)
        else:
            writes.add("positions", str(state["position_key"]), sql, row)

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone() is not None

    def _current_values(self, conn: sqlite3.Connection, *,
                        position_key: str | None = None,
                        position_keys: Sequence[str] | None = None) -> dict[str, dict[str, Any]]:
        if not self._table_exists(conn, "lp_pool_state"):
            return {}
        clauses = ["p.active_episode_id IS NOT NULL"]
        args: list[Any] = []
        if position_key is not None:
            clauses.append("p.position_key=?")
            args.append(position_key)
        if position_keys is not None:
            if not position_keys:
                return {}
            clauses.append(
                "p.position_key IN (" + ",".join("?" for _ in position_keys) + ")"
            )
            args.extend(position_keys)
        rows = _dict_rows(conn.execute(
            "SELECT p.position_key,p.protocol,p.liquidity,p.liquidity_known,"
            "p.tick_lower,p.tick_upper,p.pending_principal0,p.pending_principal1,"
            "p.pending_known,p.tokens_owed0,p.tokens_owed1,p.owed_known,"
            "s.block_number AS mark_block,s.timestamp AS mark_timestamp,"
            "s.sqrt_price_x96 AS mark_sqrt,s.tick AS mark_tick,"
            "s.price0_usd AS mark_price0,s.price1_usd AS mark_price1,"
            "m.decimals0,m.decimals1 "
            "FROM lp_accounting_positions p JOIN lp_pool_state s ON s.pool_id=p.pool_id "
            "LEFT JOIN pools m ON m.id=p.pool_id WHERE " + " AND ".join(clauses), args))
        values: dict[str, dict[str, Any]] = {}
        for row in rows:
            value = self._value_position_row(row)
            values[str(row["position_key"])] = value
        return values

    @staticmethod
    def _value_position_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = {
            "principal0": None, "principal1": None, "principal_usd": None,
            "claim_principal_usd": None, "uncollected_fees_usd": None, "equity_usd": None,
            "valuation_block": row.get("mark_block"),
            "valuation_timestamp": row.get("mark_timestamp"),
            "valuation_basis": "unknown",
            "_price0_usd": row.get("mark_price0"),
            "_price1_usd": row.get("mark_price1"),
            "_decimals0": row.get("decimals0"),
            "_decimals1": row.get("decimals1"),
            "_active_at_mark": None,
        }
        liquidity = _raw_int(row.get("liquidity")) if row.get("liquidity_known") else None
        sqrt = _raw_int(row.get("mark_sqrt"))
        lower, upper = _raw_int(row.get("tick_lower")), _raw_int(row.get("tick_upper"))
        mark_tick = _raw_int(row.get("mark_tick"))
        if lower is not None and upper is not None and mark_tick is not None:
            result["_active_at_mark"] = lower <= mark_tick < upper
        protocol = str(row.get("protocol") or "")
        if liquidity is None or sqrt is None or lower is None or upper is None or protocol not in (
                "v3", "v4"):
            return result
        try:
            amount0, amount1 = principal_raw(liquidity, sqrt, lower, upper)
        except (TypeError, ValueError, OverflowError):
            return result
        result["principal0"], result["principal1"] = str(amount0), str(amount1)
        principal = _token_value(
            amount0, amount1, row.get("mark_price0"), row.get("mark_price1"),
            _raw_int(row.get("decimals0")), _raw_int(row.get("decimals1")),
        )
        result["principal_usd"] = principal
        result["valuation_basis"] = "pool_current_v3_integer_principal"
        fees: float | None = None
        claim_principal: float | None = None
        pending0 = _raw_int(row.get("pending_principal0"))
        pending1 = _raw_int(row.get("pending_principal1"))
        if row.get("pending_known") and pending0 is not None and pending1 is not None:
            claim_principal = _token_value(
                pending0, pending1, row.get("mark_price0"), row.get("mark_price1"),
                _raw_int(row.get("decimals0")), _raw_int(row.get("decimals1")),
            )
        # tokensOwed excludes lazy fee growth while liquidity is active.  It
        # proves the full outstanding claim only after liquidity reaches zero.
        fee_state_current = liquidity == 0
        if fee_state_current and row.get("owed_known") and row.get("pending_known"):
            owed0, owed1 = _raw_int(row.get("tokens_owed0")), _raw_int(row.get("tokens_owed1"))
            if None not in (owed0, owed1, pending0, pending1):
                fees = _token_value(
                    max(0, owed0 - pending0), max(0, owed1 - pending1),  # type: ignore[operator]
                    row.get("mark_price0"), row.get("mark_price1"),
                    _raw_int(row.get("decimals0")), _raw_int(row.get("decimals1")),
                )
        result["claim_principal_usd"] = claim_principal
        result["uncollected_fees_usd"] = fees
        result["equity_usd"] = (
            principal + claim_principal + fees
            if principal is not None and claim_principal is not None and fees is not None
            else None
        )
        return result

    def _refresh_position_value(self, conn: sqlite3.Connection, key: str) -> None:
        self._refresh_position_values(conn, [key])

    def _refresh_position_values(
        self, conn: sqlite3.Connection, keys: Sequence[str],
    ) -> None:
        values = self._current_values(conn, position_keys=keys)
        conn.executemany(
            "UPDATE lp_accounting_positions SET principal0=?,principal1=?,principal_usd=?,"
            "uncollected_fees_usd=?,equity_usd=?,valuation_block=?,valuation_timestamp=?,"
            "valuation_basis=? WHERE position_key=?",
            (
                (
                    value["principal0"], value["principal1"], value["principal_usd"],
                    value["uncollected_fees_usd"], value["equity_usd"],
                    value["valuation_block"], value["valuation_timestamp"],
                    value["valuation_basis"], key,
                )
                for key, value in values.items()
            ),
        )

    def _refresh_episode_value(
        self, conn: sqlite3.Connection, episode_id: str,
        current_values: Mapping[str, Mapping[str, Any]] | None = None,
        episode: Mapping[str, Any] | None = None,
    ) -> None:
        if episode is None:
            episode = _one(conn.execute(
                "SELECT e.*,p.active_episode_id,m.decimals0,m.decimals1 "
                "FROM lp_accounting_episodes e LEFT JOIN lp_accounting_positions p "
                "ON p.position_key=e.position_key "
                "LEFT JOIN pools m ON m.id=e.pool_id WHERE e.id=?",
                (episode_id,),
            ))
        if episode is None:
            return
        current_equity: float | None = 0.0 if episode.get("status") == "complete" else None
        mark0, mark1 = episode.get("close_price0_usd"), episode.get("close_price1_usd")
        if episode.get("active_episode_id") == episode_id:
            value = current_values.get(str(episode["position_key"])) if (
                current_values is not None
            ) else self._current_values(
                conn, position_key=str(episode["position_key"]),
            ).get(str(episode["position_key"]))
            if value:
                current_equity = _finite_float(value.get("equity_usd"))
                mark0 = value.get("_price0_usd")
                mark1 = value.get("_price1_usd")
            elif self._table_exists(conn, "lp_pool_state"):
                mark = _one(conn.execute(
                    "SELECT price0_usd,price1_usd FROM lp_pool_state WHERE pool_id=?",
                    (episode.get("pool_id"),)))
                if mark:
                    mark0, mark1 = mark.get("price0_usd"), mark.get("price1_usd")
        deposit = _finite_float(episode.get("deposit_usd")) if episode.get(
            "pricing_complete") else None
        proceeds = _finite_float(episode.get("proceeds_usd")) if episode.get(
            "pricing_complete") else None
        qualified = bool(
            episode.get("history_complete") and episode.get("identity_complete")
            and not episode.get("transferred_basis") and not episode.get("ambiguous_reentry")
            and episode.get("trace_complete") and episode.get("cashflow_complete")
            and episode.get("pricing_complete")
        )
        if episode.get("closed_at") is not None and episode.get("status") != "complete":
            qualified = False
        if episode.get("status") == "complete" and not episode.get("claims_complete"):
            qualified = False
        lp_value = proceeds + current_equity if proceeds is not None and current_equity is not None else None
        gross = lp_value - deposit if qualified and lp_value is not None and deposit is not None else None
        hold = None
        if qualified and mark0 is not None and mark1 is not None:
            hold = _token_value(
                int(episode.get("deposit0") or 0), int(episode.get("deposit1") or 0),
                mark0, mark1, _raw_int(episode.get("decimals0")),
                _raw_int(episode.get("decimals1")),
            )
        lp_vs_hold = lp_value - hold if lp_value is not None and hold is not None else None
        gas = _finite_float(episode.get("gas_usd"))
        net = gross - gas if gross is not None and gas is not None else None
        return_pct = gross / deposit * 100 if gross is not None and deposit and deposit > 0 else None
        conn.execute(
            "UPDATE lp_accounting_episodes SET current_equity_usd=?,lp_value_usd=?,"
            "hold_value_usd=?,lp_vs_hold_usd=?,gross_pnl_usd=?,net_pnl_usd=?,return_pct=? "
            "WHERE id=?",
            (current_equity, lp_value, hold, lp_vs_hold, gross, net, return_pct, episode_id),
        )

    def _rebuild_tx_costs(self, conn: sqlite3.Connection,
                          tx_hashes: set[str] | None) -> None:
        if tx_hashes is not None:
            for batch in _batches(sorted(tx_hashes)):
                self._rebuild_tx_cost_batch(conn, batch)
            return

        conn.execute("DELETE FROM lp_accounting_tx_costs")
        after = ""
        while True:
            values = [
                str(row[0])
                for row in conn.execute(
                    "SELECT tx_hash FROM lp_accounting_effects WHERE tx_hash>? "
                    "GROUP BY tx_hash ORDER BY tx_hash LIMIT 500",
                    (after,),
                ).fetchall()
            ]
            if not values:
                return
            self._rebuild_tx_cost_batch(conn, values)
            after = values[-1]

    def _rebuild_tx_cost_batch(
        self, conn: sqlite3.Connection, values: Sequence[str],
    ) -> None:
        if not values:
            return
        marks = ",".join("?" for _ in values)
        effects_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in _dict_rows(conn.execute(
            "SELECT DISTINCT tx_hash,episode_id,position_key,owner,block_number "
            f"FROM lp_accounting_effects WHERE tx_hash IN ({marks})", values,
        )):
            effects_by_tx[str(row["tx_hash"])].append(row)
        transactions = {
            str(row["tx_hash"]): row
            for row in _dict_rows(conn.execute(
                "SELECT tx_hash,block_number,payer,gas_usd FROM transactions "
                f"WHERE tx_hash IN ({marks})", values,
            ))
        }
        unmapped = {
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT e.tx_hash FROM events e INDEXED BY events_tx_log_idx "
                "LEFT JOIN lp_accounting_effects a ON a.event_id=e.id "
                f"WHERE e.tx_hash IN ({marks}) "
                "AND e.kind IN ('add','remove','collect','checkpoint','transfer') "
                "AND a.event_id IS NULL",
                values,
            ).fetchall()
        }
        episode_ids = sorted({
            str(effect["episode_id"])
            for effects in effects_by_tx.values()
            for effect in effects
            if effect.get("episode_id")
        })
        episode_owners = {}
        for batch in _batches(episode_ids):
            episode_marks = ",".join("?" for _ in batch)
            episode_owners.update({
                str(row[0]): _address(row[1])
                for row in conn.execute(
                    f"SELECT id,owner FROM lp_accounting_episodes "
                    f"WHERE id IN ({episode_marks})", batch,
                ).fetchall()
            })
        empty = [tx_hash for tx_hash in values if tx_hash not in effects_by_tx]
        if empty:
            empty_marks = ",".join("?" for _ in empty)
            conn.execute(
                f"DELETE FROM lp_accounting_tx_costs WHERE tx_hash IN ({empty_marks})",
                empty,
            )
        rows = []
        for tx_hash in values:
            effects = effects_by_tx.get(tx_hash)
            if not effects:
                continue
            transaction = transactions.get(tx_hash)
            episodes = {row["episode_id"] for row in effects if row.get("episode_id")}
            positions = {row["position_key"] for row in effects if row.get("position_key")}
            owners = {
                owner
                for row in effects
                if (owner := _address(row.get("owner"))) is not None
            }
            payer = _address(transaction.get("payer")) if transaction else None
            owner = payer if payer in owners else None
            gas = _finite_float(transaction.get("gas_usd")) if transaction else None
            position_key = next(iter(positions)) if len(positions) == 1 else None
            episode_id = next(iter(episodes)) if len(episodes) == 1 else None
            episode_owner = episode_owners.get(str(episode_id)) if episode_id else None
            if transaction is None:
                attribution = "missing_transaction"
            elif payer is None:
                attribution = "unknown_payer"
            elif owner is None:
                attribution = "payer_or_identity_mismatch"
            elif gas is None:
                attribution = "unpriced"
            elif tx_hash in unmapped:
                attribution = "shared_or_unmapped_position"
                episode_id = None
                position_key = None
            elif not episodes:
                attribution = "owner_only"
                episode_id = None
            elif len(episodes) != 1 or len(positions) != 1:
                attribution = "shared_position"
                episode_id = None
                position_key = None
            elif episode_owner != owner:
                attribution = "owner_only"
                episode_id = None
            else:
                attribution = "exact"
            rows.append((
                tx_hash, payer, owner, position_key, episode_id, gas, attribution,
                int((transaction or effects[0]).get("block_number") or 0),
            ))
        conn.executemany(
            "INSERT OR REPLACE INTO lp_accounting_tx_costs("
            "tx_hash,payer,owner,position_key,episode_id,gas_usd,attribution,block_number) "
            "VALUES(?,?,?,?,?,?,?,?)",
            rows,
        )

    def _refresh_episode_costs(
        self, conn: sqlite3.Connection, tx_hashes: set[str] | None, *,
        episode_ids: Iterable[str] = (),
    ) -> None:
        if tx_hashes is None:
            after = ""
            while True:
                batch = [
                    str(row[0])
                    for row in conn.execute(
                        "SELECT id FROM lp_accounting_episodes WHERE id>? "
                        "ORDER BY id LIMIT 500",
                        (after,),
                    ).fetchall()
                ]
                if not batch:
                    return
                self._refresh_episode_cost_batch(conn, batch)
                after = batch[-1]
        affected_episodes = {str(episode_id) for episode_id in episode_ids}
        for batch in _batches(sorted(tx_hashes)):
            marks = ",".join("?" for _ in batch)
            affected_episodes.update(str(row[0]) for row in conn.execute(
                f"SELECT DISTINCT episode_id FROM lp_accounting_effects "
                f"WHERE tx_hash IN ({marks}) AND episode_id IS NOT NULL", batch,
            ).fetchall())
        for batch in _batches(sorted(affected_episodes)):
            self._refresh_episode_cost_batch(conn, batch)

    def _refresh_episode_cost_batch(
        self, conn: sqlite3.Connection, batch: Sequence[str],
    ) -> None:
        if not batch:
            return
        marks = ",".join("?" for _ in batch)
        gas_by_episode: dict[str, float | None] = {
            str(episode_id): None for episode_id in batch
        }
        # One episode can contain tens of thousands of effects.  Group its
        # transaction hashes while streaming the covering effects index, then
        # return one aggregate row per episode.  The former DISTINCT join
        # materialized every historical transaction (and five wide columns)
        # into a temp B-tree and then into Python on every update.
        for row in conn.execute(
            "SELECT ep.id,(SELECT CASE WHEN COUNT(*)>0 AND "
            "MIN(CASE WHEN c.attribution='exact' AND c.episode_id=ep.id "
            "AND c.gas_usd IS NOT NULL THEN 1 ELSE 0 END)=1 "
            "THEN SUM(c.gas_usd) END FROM ("
            "SELECT x.tx_hash FROM lp_accounting_effects x "
            "WHERE x.episode_id=ep.id GROUP BY x.tx_hash"
            ") tx LEFT JOIN lp_accounting_tx_costs c ON c.tx_hash=tx.tx_hash"
            ") AS gas_usd FROM lp_accounting_episodes ep "
            f"WHERE ep.id IN ({marks})",
            batch,
        ).fetchall():
            gas_by_episode[str(row["id"])] = (
                None if row["gas_usd"] is None else float(row["gas_usd"])
            )
        conn.executemany(
            "UPDATE lp_accounting_episodes SET gas_usd=? WHERE id=?",
            (
                (gas_by_episode[str(episode_id)], episode_id)
                for episode_id in batch
            ),
        )
        episodes = {
            str(row["id"]): row
            for row in _dict_rows(conn.execute(
                "SELECT e.*,p.active_episode_id,m.decimals0,m.decimals1 "
                "FROM lp_accounting_episodes e "
                "LEFT JOIN lp_accounting_positions p "
                "ON p.position_key=e.position_key "
                "LEFT JOIN pools m ON m.id=e.pool_id "
                f"WHERE e.id IN ({marks})",
                batch,
            ))
        }
        active_keys = sorted({
            str(row["position_key"])
            for row in episodes.values()
            if row.get("active_episode_id") == row.get("id")
        })
        current_values = self._current_values(
            conn, position_keys=active_keys,
        )
        for episode_id in batch:
            episode = episodes.get(str(episode_id))
            if episode is not None:
                self._refresh_episode_value(
                    conn, str(episode_id), current_values=current_values,
                    episode=episode,
                )

    @contextmanager
    def _reader(self):
        connection = self.store.read()
        owns_snapshot = not connection.in_transaction
        if owns_snapshot:
            connection.execute("BEGIN")
        try:
            yield connection
        finally:
            if owns_snapshot and connection.in_transaction:
                connection.rollback()

    @staticmethod
    def _owner_cost_values(
            conn: sqlite3.Connection,
            episode_ids: Iterable[str],
            identity: str | None = None) -> dict[str, list[float | None]]:
        selected = sorted({str(episode_id) for episode_id in episode_ids})
        if not selected:
            return {}
        costs: dict[tuple[str, str], float | None] = {}
        for start in range(0, len(selected), 500):
            chunk = selected[start:start + 500]
            marks = ",".join("?" for _ in chunk)
            args: list[Any] = list(chunk)
            owner_clause = ""
            if identity is not None:
                owner_clause = " AND ep.owner=?"
                args.append(identity)
            rows = _dict_rows(conn.execute(
                "SELECT DISTINCT ep.owner,e.tx_hash,c.owner AS cost_owner,c.gas_usd "
                "FROM lp_accounting_effects e "
                "JOIN lp_accounting_episodes ep ON ep.id=e.episode_id "
                "LEFT JOIN lp_accounting_tx_costs c ON c.tx_hash=e.tx_hash "
                f"WHERE e.episode_id IN ({marks}){owner_clause}", args,
            ))
            for row in rows:
                owner = _address(row.get("owner"))
                tx_hash = str(row.get("tx_hash") or "")
                if owner is None or not tx_hash:
                    continue
                exact_owner = _address(row.get("cost_owner")) == owner
                costs[(owner, tx_hash)] = (
                    _finite_float(row.get("gas_usd")) if exact_owner else None
                )
        result: dict[str, list[float | None]] = defaultdict(list)
        for (owner, _), value in costs.items():
            result[owner].append(value)
        return result

    @staticmethod
    def _owner_gas_values(
            conn: sqlite3.Connection, identities: Iterable[Any],
            owner_clauses: Sequence[str], args: Sequence[Any],
    ) -> dict[str, float | None]:
        selected = sorted({
            str(identity) for identity in identities if identity is not None
        })
        if not selected:
            return {}
        predicate = " AND ".join(owner_clauses)
        result: dict[str, float | None] = {}
        for batch in _batches(selected, 400):
            values = ",".join("(?)" for _ in batch)
            rows = conn.execute(
                "WITH selected(owner) AS (VALUES " + values + ") "
                "SELECT selected.owner,CASE WHEN EXISTS ("
                "SELECT 1 FROM lp_accounting_episodes ep "
                "INDEXED BY lp_accounting_episodes_owner "
                "JOIN lp_accounting_effects fx "
                "INDEXED BY lp_accounting_effects_episode "
                "ON fx.episode_id=ep.id "
                "LEFT JOIN lp_accounting_tx_costs c "
                "ON c.tx_hash=fx.tx_hash AND c.owner=ep.owner "
                "LEFT JOIN pools p ON p.id=ep.pool_id "
                "WHERE ep.owner=selected.owner AND " + predicate +
                " AND c.gas_usd IS NULL) THEN NULL ELSE ("
                "SELECT SUM(c.gas_usd) FROM lp_accounting_tx_costs c "
                "INDEXED BY lp_accounting_tx_costs_owner "
                "WHERE c.owner=selected.owner AND EXISTS ("
                "SELECT 1 FROM lp_accounting_effects fx "
                "INDEXED BY lp_accounting_effects_tx "
                "JOIN lp_accounting_episodes ep ON ep.id=fx.episode_id "
                "LEFT JOIN pools p ON p.id=ep.pool_id "
                "WHERE fx.tx_hash=c.tx_hash AND ep.owner=selected.owner AND "
                + predicate + ")) END AS gas_usd FROM selected",
                [*batch, *args, *args],
            ).fetchall()
            for row in rows:
                result[str(row["owner"])] = row["gas_usd"]
        return result

    def _status_coverage(self) -> dict[str, Any]:
        try:
            status = dict(self.store.status())
        except Exception:
            status = {}
        accounting: dict[str, str] = {}
        if self.deferred:
            try:
                accounting = {
                    str(row[0]): str(row[1])
                    for row in self.store.read().execute(
                        "SELECT key,value FROM lp_accounting_meta "
                        "WHERE key IN ('applied_revision','applied_epoch','dirty',"
                        "'bootstrap_phase')"
                    ).fetchall()
                }
            except sqlite3.OperationalError:
                accounting = {}
        pending = int(status.get("pending_accounting", 0) or 0)
        complete = (
            not self.deferred
            or (
                pending == 0
                and accounting.get("dirty", "1") == "0"
                and accounting.get("bootstrap_phase", "complete") == "complete"
            )
        )
        coverage = status.get("coverage")
        starts = [
            lane["from_block"] for lane in coverage.values()
            if isinstance(lane, Mapping) and lane.get("from_block") is not None
        ] if isinstance(coverage, Mapping) else []
        history_from_block = min(starts) if starts else None
        return {
            "history_from": status.get("history_from"),
            "history_from_block": history_from_block,
            "history_to": status.get("history_to") if complete else None,
            "history_target": status.get("history_target"),
            "backfill": status.get("backfill"),
            "state": status.get("state"),
            "indexed_head": status.get("indexed_head"),
            "revision": status.get("revision"),
            "epoch": status.get("epoch"),
            "accounting_pending": pending,
            "accounting_complete": complete,
            "accounting_applied_revision": (
                int(accounting["applied_revision"])
                if accounting.get("applied_revision", "").isdigit() else None
            ),
            "accounting_applied_epoch": (
                int(accounting["applied_epoch"])
                if accounting.get("applied_epoch", "").isdigit() else None
            ),
        }

    @staticmethod
    def _row_coverage(row: Mapping[str, Any]) -> dict[str, Any]:
        reasons = _json_list(row.get("qualifiers"))
        gas_state = "complete" if row.get("gas_usd") is not None else "unknown_or_shared"
        if gas_state != "complete" and "gas_unattributed" not in reasons:
            reasons.append("gas_unattributed")
        fees_observed = bool(
            row.get("fees_complete") and row.get("pricing_complete")
            and row.get("fees_usd") is not None
        )
        if not row.get("fees_complete") and "fee_allocation_unknown" not in reasons:
            reasons.append("fee_allocation_unknown")
        return {
            "history": "full" if row.get("history_complete") else "partial",
            "identity": "verified" if row.get("identity_complete") else "unknown",
            "cashflows": "exact" if row.get("cashflow_complete") else "partial",
            "pricing": "complete" if row.get("pricing_complete") else "partial",
            "fees": "exact" if row.get("fees_complete") else "unknown",
            "fee_value": (
                "complete" if fees_observed and row.get("history_complete")
                else "observed_partial_history" if fees_observed else "unknown"
            ),
            "fee_value_unit": "USDG_quote",
            "trace": "traced" if row.get("trace_complete") else "missing",
            "claims": "settled" if row.get("claims_complete") else (
                "awaiting" if row.get("status") == "awaiting_claim" else "unknown"),
            "gas": gas_state,
            "qualified": row.get("gross_pnl_usd") is not None,
            "cost_qualified": row.get("net_pnl_usd") is not None,
            "reasons": reasons,
        }

    def _episode_view(self, row: Mapping[str, Any], pair: str | None = None) -> dict[str, Any]:
        duration = None
        if row.get("closed_at") is not None:
            duration = max(0, int(row["closed_at"]) - int(row["opened_at"]))
        return {
            "id": row["id"], "position_key": row["position_key"],
            "token_id": row.get("token_id"), "owner": row.get("owner"),
            "custody": row.get("custody"), "identity_basis": row.get("identity_basis"),
            "pool_id": row.get("pool_id"), "pair": pair, "protocol": row.get("protocol"),
            "tick_lower": row.get("tick_lower"), "tick_upper": row.get("tick_upper"),
            "opened_at": row.get("opened_at"), "closed_at": row.get("closed_at"),
            "deposit_usd": row.get("deposit_usd") if row.get("pricing_complete") and row.get(
                "history_complete") else None,
            "withdrawal_usd": row.get("withdrawal_usd") if row.get("pricing_complete") and row.get(
                "history_complete") else None,
            "fees_usd": row.get("fees_usd") if row.get("fees_complete") and row.get(
                "pricing_complete") and row.get("history_complete") else None,
            "observed_collected_fees_usd": (
                row.get("fees_usd")
                if row.get("fees_complete") and row.get("pricing_complete")
                else None
            ),
            "current_equity_usd": row.get("current_equity_usd"),
            "lp_value_usd": row.get("lp_value_usd"),
            "hold_value_usd": row.get("hold_value_usd"),
            "lp_vs_hold_usd": row.get("lp_vs_hold_usd"),
            "gross_pnl_usd": row.get("gross_pnl_usd"), "gas_usd": row.get("gas_usd"),
            "net_pnl_usd": row.get("net_pnl_usd"), "return_pct": row.get("return_pct"),
            "duration_s": duration, "status": row.get("status"),
            "accounting_basis": row.get("accounting_basis"),
            "coverage": self._row_coverage(row),
        }

    def _episode_rows(self, conn: sqlite3.Connection,
                      params: Mapping[str, Any] | None = None,
                      *, closed_only: bool = False,
                      identity: str | None = None) -> list[dict[str, Any]]:
        params = params or {}
        clauses: list[str] = []
        args: list[Any] = []
        if closed_only:
            clauses.append("e.closed_at IS NOT NULL")
        protocol = str(params.get("protocol") or "").lower()
        if protocol in ("v2", "v3", "v4"):
            clauses.append("e.protocol=?")
            args.append(protocol)
        pool_id = str(params.get("pool") or params.get("pool_id") or "").lower()
        if pool_id:
            clauses.append("e.pool_id=?")
            args.append(pool_id)
        if identity:
            clauses.append("(e.owner=? OR e.custody=?)")
            args.extend((identity, identity))
        cutoff = _cutoff(params)
        if cutoff is not None:
            clauses.append("e.last_timestamp>=CAST(strftime('%s','now') AS INTEGER)-?")
            args.append(cutoff)
        sql = (
            "SELECT e.*,s.active_episode_id,p.symbol0,p.symbol1,p.token0,p.token1 "
            "FROM lp_accounting_episodes e LEFT JOIN lp_accounting_positions s "
            "ON s.position_key=e.position_key LEFT JOIN pools p ON p.id=e.pool_id" +
            (" WHERE " + " AND ".join(clauses) if clauses else "") +
            " ORDER BY COALESCE(e.closed_at,e.last_timestamp) DESC,e.opened_block DESC,"
            "e.opened_tx_index DESC,e.opened_log_index DESC"
        )
        rows = _dict_rows(conn.execute(sql, args))
        active_keys = [
            str(row["position_key"]) for row in rows
            if row.get("closed_at") is None
            and row.get("active_episode_id") == row.get("id")
        ]
        current: dict[str, dict[str, Any]] = {}
        for batch in _batches(active_keys):
            current.update(self._current_values(conn, position_keys=batch))
        for row in rows:
            if row.get("closed_at") is None and row.get("active_episode_id") == row.get("id"):
                value = current.get(str(row["position_key"]))
                if value:
                    row["current_equity_usd"] = value.get("equity_usd")
                    proceeds = _finite_float(row.get("proceeds_usd"))
                    deposit = _finite_float(row.get("deposit_usd"))
                    qualified = not self._episode_qualifiers(row) and value.get("equity_usd") is not None
                    row["lp_value_usd"] = proceeds + value["equity_usd"] if proceeds is not None and value.get(
                        "equity_usd") is not None else None
                    row["gross_pnl_usd"] = row["lp_value_usd"] - deposit if qualified and deposit is not None and row.get(
                        "lp_value_usd") is not None else None
                    row["net_pnl_usd"] = row["gross_pnl_usd"] - row["gas_usd"] if row.get(
                        "gross_pnl_usd") is not None and row.get("gas_usd") is not None else None
                    hold = _token_value(
                        int(row.get("deposit0") or 0), int(row.get("deposit1") or 0),
                        value.get("_price0_usd"), value.get("_price1_usd"),
                        _raw_int(value.get("_decimals0")), _raw_int(value.get("_decimals1")),
                    ) if qualified else None
                    row["hold_value_usd"] = hold
                    row["lp_vs_hold_usd"] = (
                        row["lp_value_usd"] - hold
                        if row.get("lp_value_usd") is not None and hold is not None else None
                    )
        return rows

    @staticmethod
    def _copy_owner_row(row: Mapping[str, Any]) -> dict[str, Any]:
        copied = dict(row)
        if isinstance(row.get("coverage"), Mapping):
            copied["coverage"] = dict(row["coverage"])
        if isinstance(row.get("activity"), Mapping):
            copied["activity"] = dict(row["activity"])
        if isinstance(row.get("financial_through_order"), Mapping):
            copied["financial_through_order"] = dict(
                row["financial_through_order"]
            )
        return copied

    @staticmethod
    def _owner_sort(rows: list[dict[str, Any]], sort_key: str) -> None:
        field = {
            "fees": "fees_usd", "gross": "gross_pnl_usd", "gas": "gas_usd",
            "win": "win_rate", "volume": "volume_usd", "activity": "_activity_at",
        }.get(sort_key, "net_pnl_usd")
        rows.sort(
            key=lambda row: (
                row.get(field) is not None,
                row.get(field) if row.get(field) is not None else -math.inf,
                row.get("owner") or row.get("custody") or "",
            ),
            reverse=True,
        )

    def owner_count(self, window: str) -> int:
        """Count beneficial-owner and custody groups without enrichment."""
        cutoff_seconds = _cutoff({"window": str(window or "all").lower()})
        where = ""
        source = " FROM lp_accounting_episodes"
        args: tuple[Any, ...] = ()
        if cutoff_seconds is not None:
            source += " INDEXED BY lp_accounting_episodes_last_timestamp"
            where = " WHERE last_timestamp>=?"
            args = (int(time.time()) - cutoff_seconds,)
        with self._reader() as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT owner)+COUNT(DISTINCT custody)"
                + source + where,
                args,
            ).fetchone()
        return int(row[0] or 0)

    def owner_candidates(
            self, params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return cached owner aggregates before sorting and pagination.

        Financial rows only come from the durable accounting projection.  The
        service overlays provisional activity after this call; it never folds
        current-stream deltas across the gap into these totals.
        Candidate dictionaries are shared read-only; the returned list is owned
        by the caller. Copy only rows being decorated or overlaid.
        """
        params = params or {}
        window = str(params.get("window") or "all").lower()
        cutoff_seconds = _cutoff({"window": window})
        protocol = str(params.get("protocol") or "").lower()
        if protocol and protocol not in {"v2", "v3", "v4"}:
            raise ValueError("protocol must be v2, v3 or v4")
        identity_scope = str(params.get("identity_scope") or "all").lower()
        if identity_scope not in {"all", "wallets", "custody"}:
            raise ValueError("identity_scope must be all, wallets or custody")
        pool_id = str(params.get("pool") or params.get("pool_id") or "").lower()
        query = str(params.get("q") or "").strip().lower()[:128]
        now = int(time.time())
        valid_until: int | None = None
        with self._cache_lock:
            generation = self._owners_generation
            cache_key = (
                generation, window, protocol, pool_id, query, identity_scope,
            )
            cached = self._owners_cache.get(cache_key)
            if cached is not None and (
                now < cached[0] or cached[1] is not None and now >= cached[1]
            ):
                cached = None
            if cached is not None:
                self._owners_cache.move_to_end(cache_key)
                valid_until = cached[1]
                rows = list(cached[2])
                coverage = dict(cached[3])
            else:
                rows = []
        if cached is None:
            clauses: list[str] = [
                "(e.owner IS NOT NULL OR e.custody IS NOT NULL)"
            ]
            args: list[Any] = []
            if cutoff_seconds is not None:
                clauses.append("e.last_timestamp>=?")
                args.append(now - cutoff_seconds)
            if protocol:
                clauses.append("e.protocol=?")
                args.append(protocol)
            if pool_id:
                clauses.append("e.pool_id=?")
                args.append(pool_id)
            if query:
                escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                term = f"%{escaped}%"
                clauses.append(
                    "(LOWER(COALESCE(e.owner,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(e.custody,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(e.pool_id,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(e.protocol,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(p.symbol0,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(p.symbol1,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(p.token0,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(p.token1,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(p.symbol0,'')||'/'||"
                    "COALESCE(p.symbol1,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(p.symbol0,'')||' / '||"
                    "COALESCE(p.symbol1,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(e.position_key,'')) LIKE ? ESCAPE '\\' OR "
                    "LOWER(COALESCE(e.token_id,'')) LIKE ? ESCAPE '\\' OR "
                    "EXISTS (SELECT 1 FROM lp_accounting_effects fxq "
                    "WHERE fxq.episode_id=e.id AND "
                    "LOWER(fxq.tx_hash) LIKE ? ESCAPE '\\'))"
                )
                args.extend([term] * 13)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            aggregate = (
                "COUNT(DISTINCT e.position_key) AS positions,"
                "COUNT(DISTINCT CASE WHEN e.closed_at IS NULL THEN e.position_key END) "
                "AS open_positions,"
                "SUM(e.status='complete') AS closed_episodes,"
                "CASE WHEN COUNT(e.gross_pnl_usd)=COUNT(*) "
                "THEN SUM(e.gross_pnl_usd) END AS gross_pnl_usd,"
                "CASE WHEN COUNT(e.net_pnl_usd)=COUNT(*) "
                "THEN SUM(e.net_pnl_usd) END AS net_pnl_usd,"
                "CASE WHEN COUNT(e.gas_usd)=COUNT(*) "
                "THEN SUM(e.gas_usd) END AS gas_usd,"
                "CASE WHEN MIN(e.history_complete AND e.fees_complete "
                "AND e.pricing_complete)=1 AND COUNT(e.fees_usd)=COUNT(*) "
                "THEN SUM(e.fees_usd) END AS fees_usd,"
                "SUM(CASE WHEN e.fees_complete AND e.pricing_complete "
                "AND e.fees_usd IS NOT NULL THEN e.fees_usd END) "
                "AS observed_collected_fees_usd,"
                "SUM(e.fees_complete AND e.pricing_complete "
                "AND e.fees_usd IS NOT NULL) AS observed_fee_episodes,"
                "SUM(e.history_complete AND e.fees_complete "
                "AND e.pricing_complete AND e.fees_usd IS NOT NULL) "
                "AS complete_fee_episodes,"
                "CASE WHEN MIN(e.history_complete AND e.pricing_complete)=1 "
                "AND COUNT(e.deposit_usd)=COUNT(*) AND COUNT(e.proceeds_usd)=COUNT(*) "
                "THEN SUM(e.deposit_usd+e.proceeds_usd) END AS volume_usd,"
                "CASE WHEN SUM(e.status='complete')>0 AND "
                "SUM(e.status='complete' AND e.gross_pnl_usd IS NULL)=0 THEN "
                "100.0*SUM(e.status='complete' AND e.gross_pnl_usd>0)"
                "/SUM(e.status='complete') END AS win_rate,"
                "SUM(e.gross_pnl_usd IS NOT NULL) AS complete_episodes,"
                "COUNT(*) AS episodes,MAX(e.last_timestamp) AS _activity_at,"
                "MIN(e.last_timestamp) AS _retention_timestamp "
            )
            source = " FROM lp_accounting_episodes e "
            if cutoff_seconds is not None and not pool_id:
                source += (
                    "INDEXED BY lp_accounting_episodes_last_timestamp "
                )
            if query:
                source += "LEFT JOIN pools p ON p.id=e.pool_id"
            with self._reader() as conn:
                # Pin totals, gas attribution and coverage to one WAL snapshot.
                # Continuous appends must not restart this expensive materialization.
                if not conn.in_transaction:
                    conn.execute("BEGIN")
                coverage = self._status_coverage()
                coverage.update({
                    "window": window,
                    "episode_selection": (
                        "last_activity_within_window"
                        if cutoff_seconds is not None else "all_indexed_episodes"
                    ),
                    "financial_scope": "lifetime_of_selected_episodes",
                    "fee_value_unit": "USDG_quote",
                })
                beneficial = (
                    _dict_rows(conn.execute(
                        "SELECT e.owner,NULL AS custody,"
                        "'verified_owner' AS identity_basis,"
                        + aggregate + source + where
                        + " AND e.owner IS NOT NULL GROUP BY e.owner",
                        args,
                    ))
                    if identity_scope != "custody" else []
                )
                custody_rows: list[dict[str, Any]] = []
                if identity_scope != "wallets":
                    custody_clauses = [*clauses, "e.custody IS NOT NULL"]
                    custody_where = " WHERE " + " AND ".join(custody_clauses)
                    custody_rows = _dict_rows(conn.execute(
                        "SELECT NULL AS owner,e.custody,"
                        "CASE WHEN SUM(e.owner IS NOT NULL)>0 "
                        "THEN 'custody_aggregate' ELSE 'custody_only' END "
                        "AS identity_basis,"
                        + aggregate + source + custody_where
                        + " GROUP BY e.custody",
                        args,
                    ))
                gas_clauses = [
                    clause.replace("e.", "ep.") for clause in clauses
                ]
                gas_clauses.append("ep.owner IS NOT NULL")
                gas_by_owner = self._owner_gas_values(
                    conn, (row.get("owner") for row in beneficial),
                    gas_clauses, args,
                )
                financial_state = self._scoped_owner_financial_state(
                    conn, params,
                    now - cutoff_seconds
                    if cutoff_seconds is not None else None,
                )
            rows = beneficial
            (
                through_order, through_as_of, pending_owners,
                pending_custodies, mapping_complete,
            ) = financial_state
            for row in (*beneficial, *custody_rows):
                owner = _address(row.get("owner"))
                custody = _address(row.get("custody"))
                pending = (
                    not mapping_complete
                    or owner is not None and owner in pending_owners
                    or custody is not None and custody in pending_custodies
                )
                row["financial_pending"] = pending
                row["financial_through_order"] = (
                    None if pending or through_order is None
                    else dict(through_order)
                )
                row["financial_through_as_of"] = (
                    None if pending else through_as_of
                )
            for row in rows:
                timestamp = row.pop("_retention_timestamp", None)
                if cutoff_seconds is not None and timestamp is not None:
                    expires = int(timestamp) + cutoff_seconds + 1
                    valid_until = expires if valid_until is None else min(valid_until, expires)
                qualified = row.get("gross_pnl_usd") is not None
                row["gas_usd"] = gas_by_owner.get(str(row["owner"]))
                row["net_pnl_usd"] = (
                    row["gross_pnl_usd"] - row["gas_usd"]
                    if row.get("gross_pnl_usd") is not None
                    and row.get("gas_usd") is not None else None
                )
                episodes = int(row.pop("episodes") or 0)
                observed_fee_episodes = int(row.pop("observed_fee_episodes") or 0)
                complete_fee_episodes = int(row.pop("complete_fee_episodes") or 0)
                row["coverage"] = {
                    "qualified": qualified,
                    "cost_qualified": row.get("net_pnl_usd") is not None,
                    "complete_episodes": int(row.pop("complete_episodes") or 0),
                    "episodes": episodes,
                    "window": window,
                    "episode_selection": (
                        "last_activity_within_window"
                        if cutoff_seconds is not None else "all_indexed_episodes"
                    ),
                    "financial_scope": "lifetime_of_selected_episodes",
                    "observed_collected_fees": {
                        "unit": "USDG_quote",
                        "episodes": observed_fee_episodes,
                        "history_complete_episodes": complete_fee_episodes,
                        "total_episodes": episodes,
                        "complete": (
                            episodes > 0 and complete_fee_episodes == episodes
                        ),
                    },
                    "reasons": (
                        [] if qualified else ["incomplete_or_unpriced_episodes"]
                    ),
                }
            for row in custody_rows:
                timestamp = row.pop("_retention_timestamp", None)
                if cutoff_seconds is not None and timestamp is not None:
                    expires = int(timestamp) + cutoff_seconds + 1
                    valid_until = expires if valid_until is None else min(valid_until, expires)
                episodes = int(row.pop("episodes") or 0)
                row["gross_pnl_usd"] = None
                row["net_pnl_usd"] = None
                row["gas_usd"] = None
                row["fees_usd"] = None
                row["observed_collected_fees_usd"] = None
                row["win_rate"] = None
                row["coverage"] = {
                    "qualified": False, "cost_qualified": False,
                    "complete_episodes": 0,
                    "episodes": episodes,
                    "window": window,
                    "episode_selection": (
                        "last_activity_within_window"
                        if cutoff_seconds is not None else "all_indexed_episodes"
                    ),
                    "financial_scope": "not_attributed_to_custody",
                    "observed_collected_fees": {
                        "unit": "USDG_quote", "episodes": 0,
                        "history_complete_episodes": 0,
                        "total_episodes": episodes, "complete": False,
                    },
                    "reasons": [row["identity_basis"], "not_beneficial_owner"],
                }
                row.pop("observed_fee_episodes", None)
                row.pop("complete_fee_episodes", None)
                row.pop("complete_episodes", None)
                rows.append(row)
            if query:
                identity_matches = [
                    row for row in rows
                    if query in str(
                        row.get("owner") or row.get("custody") or ""
                    ).lower()
                ]
                if identity_matches:
                    rows = identity_matches
            frozen = tuple(rows)
            with self._cache_lock:
                if generation == self._owners_generation:
                    self._owners_cache[cache_key] = (now, valid_until, frozen, dict(coverage))
                    self._owners_cache.move_to_end(cache_key)
                    while len(self._owners_cache) > 24:
                        self._owners_cache.popitem(last=False)
        return {
            "rows": rows, "total": len(rows), "coverage": coverage,
            "accounting_as_of": coverage.get("history_to"),
            "accounting_revision": generation,
            "valid_until": valid_until,
        }

    @staticmethod
    def _activity_view(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "block_number": row.get("block_number"),
            "timestamp": row.get("timestamp"),
            "pool_id": row.get("pool_id"),
            "pair": _pair(row),
            "kind": row.get("kind"),
            "tx_hash": row.get("tx_hash"),
            "event_count": 1,
            "qualification": "durable_canonical_index",
            "_order": (
                int(row.get("block_number") or 0),
                int(row.get("tx_index") or 0),
                int(row.get("log_index") or 0),
            ),
        }

    @staticmethod
    def _attach_owner_activity(
            row: dict[str, Any], activity: Mapping[str, Any] | None,
    ) -> None:
        if activity is None:
            row["activity"] = None
            return
        view = dict(activity)
        activity_order = view.pop("_order", None)
        row["activity"] = view
        through = row.get("financial_through_order")
        if (
            isinstance(activity_order, (list, tuple))
            and (
                not isinstance(through, Mapping)
                or tuple(int(item) for item in activity_order) > tuple(
                    int(through.get(name) or 0)
                    for name in ("block_number", "tx_index", "log_index")
                )
            )
        ):
            row["financial_pending"] = True
            row["financial_through_order"] = None
            row["financial_through_as_of"] = None

    def _historical_owner_activity(
            self, conn: sqlite3.Connection, owner_row: Mapping[str, Any],
            params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        identity = _address(owner_row.get("owner") or owner_row.get("custody"))
        if identity is None:
            return None
        owner_match = owner_row.get("owner") is not None
        clauses = [
            "e.kind IN ('add','remove','collect','checkpoint','donate','fee','transfer')"
        ]
        args: list[Any] = []
        cutoff = _cutoff(params)
        if cutoff is not None:
            clauses.append("e.timestamp>=CAST(strftime('%s','now') AS INTEGER)-?")
            args.append(cutoff)
        protocol = str(params.get("protocol") or "").lower()
        if protocol:
            clauses.append(
                "(e.protocol=? OR (e.protocol='nft' AND "
                "json_extract(e.data,'$.manager_protocol')=?))"
            )
            args.extend((protocol, protocol))
        pool_id = str(params.get("pool") or params.get("pool_id") or "").lower()
        if pool_id:
            clauses.append("e.pool_id=?")
            args.append(pool_id)
        query = str(params.get("q") or "").strip().lower()[:128]
        if query and query not in identity:
            escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            term = f"%{escaped}%"
            clauses.append(
                "(LOWER(COALESCE(e.pool_id,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(e.protocol,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(p.symbol0,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(p.symbol1,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(p.token0,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(p.token1,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(p.symbol0,'')||'/'||"
                "COALESCE(p.symbol1,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(p.symbol0,'')||' / '||"
                "COALESCE(p.symbol1,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(e.tx_hash,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(e.position_key,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(e.token_id,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(e.owner,'')) LIKE ? ESCAPE '\\' OR "
                "LOWER(COALESCE(e.custody,'')) LIKE ? ESCAPE '\\')"
            )
            args.extend([term] * 13)
        columns = (
            "e.block_number,e.timestamp,e.pool_id,e.kind,e.tx_hash,e.tx_index,"
            "e.log_index,p.symbol0,p.symbol1,p.token0,p.token1"
        )
        identity_column = "e.owner" if owner_match else "e.custody"
        direct = _one(conn.execute(
            "SELECT " + columns + " FROM events e "
            "LEFT JOIN pools p ON p.id=e.pool_id WHERE "
            + identity_column + "=? AND " + " AND ".join(clauses)
            + " ORDER BY e.block_number DESC,e.tx_index DESC,e.log_index DESC LIMIT 1",
            [identity, *args],
        ))
        ended = None
        if owner_match:
            ended = _one(conn.execute(
                "SELECT " + columns + " FROM lp_ownership_intervals i "
                "JOIN lp_accounting_event_keys k ON k.position_key=i.position_key "
                "JOIN events e ON e.id=k.event_id AND e.block_number=i.end_block "
                "AND e.tx_index=i.end_tx_index AND e.log_index=i.end_log_index "
                "LEFT JOIN pools p ON p.id=e.pool_id WHERE i.owner=? "
                "AND i.end_block IS NOT NULL AND " + " AND ".join(clauses)
                + " ORDER BY e.block_number DESC,e.tx_index DESC,e.log_index DESC LIMIT 1",
                [identity, *args],
            ))
        latest = max(
            (row for row in (direct, ended) if row is not None),
            key=lambda row: (
                int(row.get("block_number") or 0), int(row.get("tx_index") or 0),
                int(row.get("log_index") or 0),
            ),
            default=None,
        )
        return self._activity_view(latest)

    def _scoped_owner_financial_state(
            self, conn: sqlite3.Connection, params: Mapping[str, Any],
            cutoff_timestamp: int | None,
    ) -> tuple[
        dict[str, int] | None, int | None, set[str], set[str], bool,
    ]:
        """Describe the immutable ledger snapshot backing owner aggregates."""
        cutoff = cutoff_timestamp
        protocol = str(params.get("protocol") or "").lower()
        pool_id = str(params.get("pool") or params.get("pool_id") or "").lower()
        event_clauses: list[str] = []
        event_args: list[Any] = []
        episode_clauses: list[str] = []
        episode_args: list[Any] = []
        if cutoff is not None:
            event_clauses.append("e.timestamp>=?")
            event_args.append(cutoff)
            episode_clauses.append("ep.last_timestamp>=?")
            episode_args.append(cutoff)
        if protocol:
            event_clauses.append(
                "(e.protocol=? OR (e.protocol='nft' AND "
                "json_extract(e.data,'$.manager_protocol')=?))"
            )
            event_args.extend((protocol, protocol))
            episode_clauses.append("ep.protocol=?")
            episode_args.append(protocol)
        if pool_id:
            event_clauses.append("e.pool_id=?")
            event_args.append(pool_id)
            episode_clauses.append("ep.pool_id=?")
            episode_args.append(pool_id)
        event_where = (
            " WHERE " + " AND ".join(event_clauses)
            if event_clauses else ""
        )
        through = conn.execute(
            "SELECT e.block_number,e.tx_index,e.log_index,e.timestamp "
            "FROM events e" + event_where
            + " ORDER BY e.block_number DESC,e.tx_index DESC,"
            "e.log_index DESC LIMIT 1",
            event_args,
        ).fetchone()
        through_order = (
            {
                "block_number": int(through["block_number"]),
                "tx_index": int(through["tx_index"]),
                "log_index": int(through["log_index"]),
            }
            if through is not None else None
        )
        through_as_of = (
            int(through["timestamp"]) if through is not None else None
        )
        bootstrap_complete = (
            self._accounting_meta(conn, "bootstrap_phase", "complete")
            == "complete"
        )
        pending_exists = conn.execute(
            "SELECT 1 FROM lp_accounting_pending LIMIT 1"
        ).fetchone() is not None
        dirty = self._accounting_meta(conn, "dirty", "1") != "0"
        if not bootstrap_complete or dirty and not pending_exists:
            return through_order, through_as_of, set(), set(), False
        if not pending_exists:
            return through_order, through_as_of, set(), set(), True
        if event_clauses:
            event_scope = " AND " + " AND ".join(event_clauses)
            episode_scope = " AND " + " AND ".join(episode_clauses)
            scoped_keys = (
                "SELECT q.position_key FROM lp_accounting_pending q WHERE "
                "EXISTS(SELECT 1 FROM lp_accounting_event_keys k "
                "INDEXED BY lp_accounting_event_keys_position "
                "JOIN events e ON e.id=k.event_id "
                "WHERE k.position_key=q.position_key" + event_scope + ") OR "
                "EXISTS(SELECT 1 FROM lp_accounting_episodes ep "
                "WHERE ep.position_key=q.position_key" + episode_scope + ")"
            )
            scoped_args = [*event_args, *episode_args]
        else:
            scoped_keys = (
                "SELECT q.position_key FROM lp_accounting_pending q"
            )
            scoped_args = []
        pending_rows = conn.execute(
            "WITH scoped_keys AS (" + scoped_keys + ") "
            "SELECT ep.owner,ep.custody,NULL AS data "
            "FROM scoped_keys s JOIN lp_accounting_episodes ep "
            "ON ep.position_key=s.position_key "
            "UNION "
            "SELECT e.owner,e.custody,CASE WHEN e.kind='transfer' THEN e.data END "
            "FROM scoped_keys s JOIN lp_accounting_event_keys k "
            "INDEXED BY lp_accounting_event_keys_position "
            "ON k.position_key=s.position_key "
            "JOIN events e ON e.id=k.event_id",
            scoped_args,
        )
        owners: set[str] = set()
        custodies: set[str] = set()
        for row in pending_rows:
            owner = _address(row["owner"])
            custody = _address(row["custody"])
            if owner is not None:
                owners.add(owner)
            if custody is not None:
                custodies.add(custody)
            if row["data"] is not None:
                source, target = self._transfer_parties(dict(row), _json(row["data"]))
                if source is not None and source != _ZERO_ADDRESS:
                    owners.add(source)
                if target is not None and target != _ZERO_ADDRESS:
                    owners.add(target)
        return through_order, through_as_of, owners, custodies, True

    def decorate_owner_activity(
            self, rows: Sequence[Mapping[str, Any]],
            params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params = params or {}
        output = [self._copy_owner_row(row) for row in rows]
        window = str(params.get("window") or "all").lower()
        bucket = int(time.time()) // 15 if _cutoff({"window": window}) is not None else 0
        protocol = str(params.get("protocol") or "").lower()
        pool_id = str(params.get("pool") or params.get("pool_id") or "").lower()
        query = str(params.get("q") or "").strip().lower()[:128]
        with self._cache_lock:
            generation = self._owners_generation
        missing: list[tuple[int, tuple[Any, ...]]] = []
        for index, row in enumerate(output):
            if isinstance(row.get("activity"), Mapping):
                continue
            key = (
                generation, bucket, window, protocol, pool_id, query,
                row.get("owner"), row.get("custody"),
            )
            with self._cache_lock:
                cached = self._owner_activity_cache.get(key, ...)
                if cached is not ...:
                    self._owner_activity_cache.move_to_end(key)
            if cached is ...:
                missing.append((index, key))
            else:
                self._attach_owner_activity(
                    row, cached if isinstance(cached, Mapping) else None,
                )
        if missing:
            with self._reader() as conn:
                for index, key in missing:
                    activity = self._historical_owner_activity(
                        conn, output[index], params,
                    )
                    self._attach_owner_activity(
                        output[index],
                        activity if isinstance(activity, Mapping) else None,
                    )
                    with self._cache_lock:
                        if generation == self._owners_generation:
                            self._owner_activity_cache[key] = (
                                dict(activity)
                                if isinstance(activity, Mapping) else None
                            )
                            self._owner_activity_cache.move_to_end(key)
                            while len(self._owner_activity_cache) > 2048:
                                self._owner_activity_cache.popitem(last=False)
        return output

    def owners(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        limit, offset = _limit_offset(params)
        candidates = self.owner_candidates(params)
        rows = candidates["rows"]
        self._owner_sort(rows, str(params.get("sort") or "net_pnl").lower())
        selected = self.decorate_owner_activity(rows[offset:offset + limit], params)
        for row in selected:
            if not isinstance(row.get("activity"), Mapping):
                row["activity"] = {
                    "block_number": None, "timestamp": row.get("_activity_at"),
                    "pool_id": None, "pair": None, "kind": None,
                    "tx_hash": None, "event_count": 1,
                    "qualification": "durable_canonical_index",
                }
            row.pop("_activity_at", None)
        coverage = dict(candidates["coverage"])
        coverage.update({
            "rows": len(selected),
            "qualified_rows": sum(
                bool(row["coverage"]["qualified"]) for row in selected
            ),
        })
        return {
            "rows": selected, "total": len(rows), "coverage": coverage,
            "accounting_as_of": candidates["accounting_as_of"],
        }

    def closed(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        limit, offset = _limit_offset(params)
        with self._reader() as conn:
            rows = self._episode_rows(conn, params, closed_only=True)
        query = str(params.get("q") or "").strip().lower()
        views = []
        for row in rows:
            pair = _pair(row)
            if query and query not in " ".join((str(row.get("owner") or ""),
                                                str(row.get("custody") or ""), pair.lower(),
                                                str(row.get("pool_id") or ""))):
                continue
            views.append(self._episode_view(row, pair))
        sort_key = str(params.get("sort") or "recent").lower()
        field = {"net": "net_pnl_usd", "gross": "gross_pnl_usd", "fees": "fees_usd",
                 "return": "return_pct", "duration": "duration_s"}.get(sort_key, "closed_at")
        views.sort(key=lambda row: (row.get(field) is not None,
                                   row.get(field) if row.get(field) is not None else -math.inf),
                   reverse=True)
        total = len(views)
        selected = views[offset:offset + limit]
        coverage = self._status_coverage()
        coverage.update({"rows": len(selected), "qualified_rows": sum(
            bool(row["coverage"]["qualified"]) for row in selected)})
        return {"rows": selected, "total": total, "coverage": coverage}

    def positions(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        limit, offset = _limit_offset(params)
        clauses: list[str] = []
        args: list[Any] = []
        identity = _address(params.get("owner"))
        if identity:
            clauses.append("(p.owner=? OR p.custody=?)")
            args.extend((identity, identity))
        protocol = str(params.get("protocol") or "").lower()
        if protocol in ("v2", "v3", "v4"):
            clauses.append("p.protocol=?")
            args.append(protocol)
        pool_id = str(params.get("pool") or params.get("pool_id") or "").lower()
        if pool_id:
            clauses.append("p.pool_id=?")
            args.append(pool_id)
        state_filter = str(params.get("status") or "").lower()
        if state_filter == "open":
            clauses.append("p.active_episode_id IS NOT NULL")
        elif state_filter == "closed":
            clauses.append("p.active_episode_id IS NULL")
        sql = (
            "SELECT p.*,m.symbol0,m.symbol1,m.token0,m.token1 FROM lp_accounting_positions p "
            "LEFT JOIN pools m ON m.id=p.pool_id" +
            (" WHERE " + " AND ".join(clauses) if clauses else "") +
            " ORDER BY p.last_timestamp DESC,p.position_key"
        )
        with self._reader() as conn:
            rows = _dict_rows(conn.execute(sql, args))
            active_keys = [
                str(row["position_key"]) for row in rows
                if row.get("active_episode_id") is not None
            ]
            values: dict[str, dict[str, Any]] = {}
            for batch in _batches(active_keys):
                values.update(self._current_values(conn, position_keys=batch))
        query = str(params.get("q") or "").strip().lower()
        output: list[dict[str, Any]] = []
        for row in rows:
            pair = _pair(row)
            if query and query not in " ".join((str(row.get("owner") or ""),
                                                str(row.get("custody") or ""), pair.lower(),
                                                str(row.get("position_key") or ""))):
                continue
            value = values.get(str(row["position_key"])) or {
                "principal0": row.get("principal0"), "principal1": row.get("principal1"),
                "principal_usd": row.get("principal_usd"),
                "claim_principal_usd": 0.0 if row.get("status") == "closed"
                and row.get("equity_usd") == 0.0 else None,
                "uncollected_fees_usd": row.get("uncollected_fees_usd"),
                "equity_usd": row.get("equity_usd"),
                "valuation_block": row.get("valuation_block"),
                "valuation_timestamp": row.get("valuation_timestamp"),
                "valuation_basis": row.get("valuation_basis"),
            }
            liquidity = row.get("liquidity") if row.get("liquidity_known") else None
            reasons: list[str] = []
            if not row.get("history_complete"):
                reasons.append("partial_history")
            if row.get("owner") is None:
                reasons.append("unknown_identity")
            if value.get("principal_usd") is None and row.get("active_episode_id"):
                reasons.append("principal_unvalued")
            elif value.get("equity_usd") is None and row.get("active_episode_id"):
                reasons.append("uncollected_fees_unknown")
            output.append({
                "position_key": row["position_key"], "token_id": row.get("token_id"),
                "owner": row.get("owner"), "custody": row.get("custody"),
                "identity_basis": row.get("identity_basis"), "pool_id": row.get("pool_id"),
                "identity_match": (
                    "beneficial_owner"
                    if identity and _address(row.get("owner")) == identity
                    else ("custody" if identity and _address(row.get("custody")) == identity else None)
                ),
                "pair": pair, "protocol": row.get("protocol"),
                "tick_lower": row.get("tick_lower"), "tick_upper": row.get("tick_upper"),
                "liquidity": liquidity,
                "pending_principal0": row.get("pending_principal0")
                if row.get("pending_known") else None,
                "pending_principal1": row.get("pending_principal1")
                if row.get("pending_known") else None,
                "tokens_owed0": row.get("tokens_owed0") if row.get("owed_known") else None,
                "tokens_owed1": row.get("tokens_owed1") if row.get("owed_known") else None,
                "principal0": value.get("principal0"), "principal1": value.get("principal1"),
                "principal_usd": value.get("principal_usd"),
                "claim_principal_usd": value.get("claim_principal_usd"),
                "uncollected_fees_usd": value.get("uncollected_fees_usd"),
                "equity_usd": value.get("equity_usd"),
                "valuation_block": value.get("valuation_block"),
                "valuation_timestamp": value.get("valuation_timestamp"),
                "valuation_basis": value.get("valuation_basis") or "unknown",
                "status": row.get("status"), "last_event_at": row.get("last_timestamp"),
                "coverage": {
                    "history": "full" if row.get("history_complete") else "partial",
                    "identity": "verified" if row.get("owner") else "unknown",
                    "qualified": not reasons, "reasons": reasons,
                },
            })
        total = len(output)
        selected = output[offset:offset + limit]
        coverage = self._status_coverage()
        coverage.update({"rows": len(selected), "qualified_rows": sum(
            bool(row["coverage"]["qualified"]) for row in selected)})
        return {"rows": selected, "total": total, "coverage": coverage}

    def owner(self, address: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        identity = _address(address)
        coverage = self._status_coverage()
        if identity is None:
            return {"owner": None, "summary": None, "positions": [], "closed": [],
                    "series": [], "coverage": {**coverage, "error": "invalid_owner"}}
        detail_params = dict(params or {})
        detail_params["owner"] = identity
        detail_params["limit"] = _MAX_LIMIT
        position_rows = self.positions(detail_params)["rows"]
        with self._reader() as conn:
            episodes = self._episode_rows(conn, detail_params, identity=identity)
            gas_values = self._owner_cost_values(
                conn, (str(episode["id"]) for episode in episodes), identity
            ).get(identity, [])
            ownership_rows = _dict_rows(conn.execute(
                "SELECT position_key,token_id,owner,custody,identity_basis,acquired_by,"
                "start_block,start_timestamp,end_block,end_timestamp,complete "
                "FROM lp_ownership_intervals WHERE owner=? OR custody=? "
                "ORDER BY start_block,start_tx_index,start_log_index",
                (identity, identity),
            ))
        views = [self._episode_view(row, _pair(row)) for row in episodes]
        for view in views:
            view["identity_match"] = (
                "beneficial_owner"
                if _address(view.get("owner")) == identity
                else "custody"
            )
        history_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for view in views:
            history_by_key[str(view["position_key"])].append(view)
        ownership_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for interval in ownership_rows:
            interval["complete"] = bool(interval.get("complete"))
            ownership_by_key[str(interval["position_key"])].append(interval)
        current_by_key = {str(row["position_key"]): row for row in position_rows}
        for key, history in history_by_key.items():
            if key in current_by_key:
                current_by_key[key]["episodes"] = history
                current_by_key[key]["ownership"] = ownership_by_key.get(key, [])
                continue
            latest = max(history, key=lambda row: int(
                row.get("closed_at") or row.get("opened_at") or 0))
            position_rows.append({
                "position_key": key,
                "token_id": latest.get("token_id"),
                "owner": identity if latest.get("owner") == identity else None,
                "custody": latest.get("custody"),
                "identity_basis": latest.get("identity_basis"),
                "identity_match": latest.get("identity_match"),
                "pool_id": latest.get("pool_id"),
                "pair": latest.get("pair"),
                "protocol": latest.get("protocol"),
                "tick_lower": latest.get("tick_lower"),
                "tick_upper": latest.get("tick_upper"),
                "liquidity": None,
                "principal_usd": None,
                "equity_usd": None,
                "status": "historical",
                "last_event_at": latest.get("closed_at"),
                "episodes": history,
                "ownership": ownership_by_key.get(key, []),
                "coverage": {
                    "history": "historical_ownership",
                    "identity": "verified",
                    "qualified": False,
                    "reasons": ["no_longer_owned"],
                },
            })
        for key, row in current_by_key.items():
            row.setdefault("episodes", history_by_key.get(key, []))
            row.setdefault("ownership", ownership_by_key.get(key, []))
        closed_rows = [row for row in views if row.get("closed_at") is not None]
        beneficial = [
            row for row in episodes if _address(row.get("owner")) == identity
        ]
        gross = (
            _nullable_sum(
                _finite_float(row.get("gross_pnl_usd")) for row in beneficial
            )
            if beneficial else None
        )
        gas = _nullable_sum(gas_values) if beneficial else None
        net = gross - gas if gross is not None and gas is not None else None
        observed_fee_values = [
            value for row in beneficial
            if row.get("fees_complete") and row.get("pricing_complete")
            if (value := _finite_float(row.get("fees_usd"))) is not None
        ]
        observed_fees = (
            float(sum(observed_fee_values)) if observed_fee_values else None
        )
        complete_fee_episodes = sum(
            bool(
                row.get("history_complete") and row.get("fees_complete")
                and row.get("pricing_complete") and row.get("fees_usd") is not None
            )
            for row in beneficial
        )
        fees = (
            observed_fees
            if beneficial and complete_fee_episodes == len(beneficial) else None
        )
        completed = [row for row in beneficial if row.get("status") == "complete"]
        wins = [_finite_float(row.get("gross_pnl_usd")) for row in completed]
        win_rate = (100.0 * sum(item > 0 for item in wins if item is not None) / len(wins)
                    if wins and all(item is not None for item in wins) else None)
        custody_matches = [
            row for row in episodes
            if _address(row.get("custody")) == identity
            and _address(row.get("owner")) != identity
        ]
        identity_basis = (
            "owner_and_custody"
            if beneficial and custody_matches
            else ("verified_owner" if beneficial else "custody_only")
        )
        summary = {
            "identity": identity,
            "owner": identity if beneficial else None,
            "custody": identity if custody_matches else None,
            "identity_basis": identity_basis,
            "positions": len({row["position_key"] for row in episodes}),
            "open_positions": len({row["position_key"] for row in episodes
                                   if row.get("closed_at") is None}),
            "closed_episodes": len(completed), "fees_usd": fees,
            "observed_collected_fees_usd": observed_fees,
            "gross_pnl_usd": gross, "gas_usd": gas, "net_pnl_usd": net,
            "win_rate": win_rate,
        }
        series: list[dict[str, Any]] = []
        cumulative_gross = 0.0
        cumulative_net = 0.0
        series_known = bool(beneficial)
        for row in sorted(
            (item for item in closed_rows if item.get("identity_match") == "beneficial_owner"),
            key=lambda item: int(item.get("closed_at") or 0),
        ):
            if row.get("gross_pnl_usd") is None or row.get("net_pnl_usd") is None:
                series_known = False
                continue
            cumulative_gross += float(row["gross_pnl_usd"])
            cumulative_net += float(row["net_pnl_usd"])
            series.append({"timestamp": row["closed_at"],
                           "gross_pnl_usd": cumulative_gross,
                           "net_pnl_usd": cumulative_net})
        reasons = sorted({reason for row in views for reason in row["coverage"]["reasons"]})
        if custody_matches:
            reasons.append("custody_positions_not_beneficial_owner")
            reasons = sorted(set(reasons))
        window = str(detail_params.get("window") or "all").lower()
        coverage.update({
            "qualified": gross is not None,
            "cost_qualified": net is not None,
            "series_complete": series_known,
            "identity_basis": identity_basis,
            "window": window,
            "episode_selection": (
                "last_activity_within_window"
                if _cutoff({"window": window}) is not None
                else "all_indexed_episodes"
            ),
            "financial_scope": "lifetime_of_selected_episodes",
            "observed_collected_fees": {
                "unit": "USDG_quote",
                "episodes": len(observed_fee_values),
                "history_complete_episodes": complete_fee_episodes,
                "total_episodes": len(beneficial),
                "complete": (
                    bool(beneficial)
                    and complete_fee_episodes == len(beneficial)
                ),
            },
            "reasons": reasons,
        })
        return {
            "identity": identity,
            "owner": identity if beneficial else None,
            "custody": identity if custody_matches else None,
            "identity_basis": identity_basis,
            "summary": summary,
            "positions": position_rows,
            "closed": closed_rows,
            "series": series,
            "coverage": coverage,
        }

    @staticmethod
    def _pool_inventory_position(row: Sequence[Any]) -> tuple[
        int | None, int | None, int | None
    ]:
        protocol = str(row[1] or "")
        liquidity = (
            _raw_int(row[2])
            if row[3] and protocol in ("v3", "v4")
            else None
        )
        return liquidity, _raw_int(row[4]), _raw_int(row[5])

    @staticmethod
    def _reduce_pool_inventory(
        positions: Iterable[tuple[int | None, int | None, int | None]],
        *,
        lp_count: int,
        history_complete: bool,
        mark: Mapping[str, Any],
        metadata: Mapping[str, Any],
        history_from: int | None,
        backfill_done: bool,
        retain_inventory: bool,
    ) -> tuple[dict[str, Any], _PoolInventory | None]:
        sqrt = _raw_int(mark.get("sqrt_price_x96"))
        mark_tick = _raw_int(mark.get("tick"))
        decimals0 = _raw_int(metadata.get("decimals0"))
        decimals1 = _raw_int(metadata.get("decimals1"))
        created = _raw_int(metadata.get("created_block"))
        retained: list[tuple[int | None, int | None, int | None]] | None = (
            [] if retain_inventory else None
        )
        open_positions = 0
        classified = 0
        active_positions = 0
        principal_known = False
        principal_total = 0.0
        principal_error = 0.0
        active_known = False
        active_total = 0.0
        active_error = 0.0
        for position in positions:
            open_positions += 1
            # Stop retaining as soon as one pool exceeds the global cache budget;
            # the remaining rows still flow through this single-pass reduction.
            if retained is not None:
                if len(retained) < _POOL_INVENTORY_CACHE_POSITIONS:
                    retained.append(position)
                else:
                    retained = None
            liquidity, lower, upper = position
            active = False
            if lower is not None and upper is not None and mark_tick is not None:
                classified += 1
                active = lower <= mark_tick < upper
                if active:
                    active_positions += 1
            principal: float | None = None
            if None not in (liquidity, sqrt, lower, upper):
                try:
                    amount0, amount1 = principal_raw(
                        liquidity, sqrt, lower, upper,  # type: ignore[arg-type]
                    )
                except (TypeError, ValueError, OverflowError):
                    pass
                else:
                    principal = _token_value(
                        amount0, amount1,
                        mark.get("price0_usd"), mark.get("price1_usd"),
                        decimals0, decimals1,
                    )
            if principal is not None:
                principal_known = True
                # Keep small positions in the total without retaining a value
                # list. This matches compensated float summation in sum().
                total = principal_total + principal
                principal_error += (
                    (principal_total - total) + principal
                    if abs(principal_total) >= abs(principal)
                    else (principal - total) + principal_total
                )
                principal_total = total
                if active:
                    active_known = True
                    total = active_total + principal
                    active_error += (
                        (active_total - total) + principal
                        if abs(active_total) >= abs(principal)
                        else (principal - total) + active_total
                    )
                    active_total = total
        if math.isfinite(principal_error):
            principal_total += principal_error
        if math.isfinite(active_error):
            active_total += active_error
        inventory_complete = bool(
            created is not None
            and history_from is not None
            and history_from <= created
            and backfill_done
            and history_complete
        )
        observed = (
            principal_total
            if principal_known
            else (0.0 if not open_positions and inventory_complete else None)
        )
        if active_known:
            observed_active: float | None = active_total
        elif active_positions:
            observed_active = None
        elif classified == open_positions:
            observed_active = 0.0
        else:
            observed_active = (
                0.0 if not open_positions and inventory_complete else None
            )
        inventory = (
            _PoolInventory(tuple(retained), lp_count, history_complete)
            if retained is not None
            else None
        )
        return {
            "observed_principal_usd": observed,
            "observed_active_tvl_usd": observed_active,
            "lp_count": lp_count,
            "open_positions": open_positions,
            "complete_inventory": inventory_complete,
        }, inventory

    def _cache_pool_stats(
        self,
        pool_id: str,
        generation: int,
        cache_key: tuple[Any, ...],
        result: Mapping[str, Any],
        inventory: _PoolInventory | None = None,
    ) -> None:
        with self._cache_lock:
            if inventory is not None:
                prior = self._pool_inventory_cache.get(pool_id)
                # An older snapshot may finish after a newer one. Its result key
                # is safe to retain, but it must not displace newer inventory.
                if prior is None or prior[0] <= generation:
                    if prior is not None:
                        self._pool_inventory_cache.pop(pool_id)
                        self._pool_inventory_positions -= len(
                            prior[1].positions
                        )
                    size = len(inventory.positions)
                    while self._pool_inventory_cache and (
                        len(self._pool_inventory_cache)
                        >= _POOL_INVENTORY_CACHE_POOLS
                        or self._pool_inventory_positions + size
                        > _POOL_INVENTORY_CACHE_POSITIONS
                    ):
                        _, evicted = self._pool_inventory_cache.popitem(
                            last=False
                        )
                        self._pool_inventory_positions -= len(
                            evicted[1].positions
                        )
                    self._pool_inventory_cache[pool_id] = (
                        generation, inventory,
                    )
                    self._pool_inventory_positions += size
            self._pool_cache[cache_key] = dict(result)
            self._pool_cache.move_to_end(cache_key)
            while len(self._pool_cache) > _POOL_RESULT_CACHE_ENTRIES:
                self._pool_cache.popitem(last=False)

    def pool_stats(self, pool_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Return observed position principal without pretending it is full TVL."""
        requested = list(dict.fromkeys(
            str(item).lower() for item in pool_ids if item
        ))
        if not requested:
            return {}
        empty = {
            "observed_principal_usd": None,
            "observed_active_tvl_usd": None,
            "lp_count": 0,
            "open_positions": 0,
            "complete_inventory": False,
        }
        results: dict[str, dict[str, Any]] = {}
        with self._reader() as conn:
            owns_snapshot = not conn.in_transaction
            try:
                marks = ",".join("?" for _ in requested)
                if owns_snapshot:
                    conn.execute("BEGIN")
                generations = {pool_id: 0 for pool_id in requested}
                generations.update({
                    str(row[0]).lower(): int(row[1])
                    for row in conn.execute(
                        "SELECT pool_id,generation "
                        "FROM lp_accounting_pool_generations "
                        f"WHERE pool_id IN ({marks})",
                        requested,
                    ).fetchall()
                })
                if not self._table_exists(conn, "lp_pool_state"):
                    return {pool_id: dict(empty) for pool_id in requested}
                state_rows = _dict_rows(conn.execute(
                    f"SELECT pool_id,tick,sqrt_price_x96,"
                    f"price0_usd,price1_usd "
                    f"FROM lp_pool_state WHERE pool_id IN ({marks})",
                    requested,
                ))
                status = self._status_coverage()
                state = {
                    str(row["pool_id"]).lower(): row for row in state_rows
                }
                metadata = {
                    str(row["id"]).lower(): row
                    for row in _dict_rows(conn.execute(
                        f"SELECT id,created_block,decimals0,decimals1 "
                        f"FROM pools WHERE id IN ({marks})",
                        requested,
                    ))
                }
                history_from = _raw_int(status.get("history_from_block"))
                backfill = status.get("backfill")
                backfill_done = (
                    backfill is False
                    or backfill == "complete"
                    or (
                        isinstance(backfill, Mapping)
                        and (
                            _flag(backfill.get("complete"))
                            or backfill.get("remaining") == 0
                        )
                    )
                )
                cache_keys: dict[str, tuple[Any, ...]] = {}
                inventory_hits: dict[str, _PoolInventory] = {}
                missing_results: list[str] = []
                missing_inventory: list[str] = []
                with self._cache_lock:
                    for pool_id in requested:
                        mark = state.get(pool_id, {})
                        pool = metadata.get(pool_id, {})
                        generation = generations[pool_id]
                        cache_key = (
                            generation, pool_id,
                            mark.get("tick"),
                            mark.get("sqrt_price_x96"),
                            mark.get("price0_usd"),
                            mark.get("price1_usd"),
                            pool.get("created_block"),
                            pool.get("decimals0"),
                            pool.get("decimals1"),
                            history_from,
                            backfill_done,
                        )
                        cache_keys[pool_id] = cache_key
                        cached = self._pool_cache.get(cache_key)
                        if cached is not None:
                            results[pool_id] = dict(cached)
                            self._pool_cache.move_to_end(cache_key)
                            continue
                        missing_results.append(pool_id)
                        inventory_entry = self._pool_inventory_cache.get(pool_id)
                        if (
                            inventory_entry is not None
                            and inventory_entry[0] == generation
                        ):
                            inventory_hits[pool_id] = inventory_entry[1]
                            self._pool_inventory_cache.move_to_end(pool_id)
                        else:
                            missing_inventory.append(pool_id)
                for pool_id in missing_results:
                    inventory = inventory_hits.get(pool_id)
                    if inventory is None:
                        continue
                    result, _ = self._reduce_pool_inventory(
                        inventory.positions,
                        lp_count=inventory.lp_count,
                        history_complete=inventory.history_complete,
                        mark=state.get(pool_id, {}),
                        metadata=metadata.get(pool_id, {}),
                        history_from=history_from,
                        backfill_done=backfill_done,
                        retain_inventory=False,
                    )
                    results[pool_id] = result
                    self._cache_pool_stats(
                        pool_id, generations[pool_id],
                        cache_keys[pool_id], result,
                    )
                if missing_inventory:
                    position_marks = ",".join("?" for _ in missing_inventory)
                    summaries = {
                        str(row["pool_id"]).lower(): (
                            int(row["lp_count"] or 0),
                            bool(row["history_complete"]),
                        )
                        for row in _dict_rows(conn.execute(
                            f"SELECT pool_id,"
                            f"COUNT(DISTINCT CASE "
                            f"WHEN owner IS NOT NULL AND owner<>'' THEN owner "
                            f"WHEN custody IS NOT NULL AND custody<>'' THEN custody "
                            f"END) AS lp_count,"
                            f"MIN(history_complete) AS history_complete "
                            f"FROM lp_accounting_positions "
                            f"INDEXED BY lp_accounting_positions_active_inventory "
                            f"WHERE pool_id IN ({position_marks}) "
                            f"AND active_episode_id IS NOT NULL GROUP BY pool_id",
                            missing_inventory,
                        ))
                    }
                    position_rows = conn.execute(
                        f"SELECT pool_id,protocol,liquidity,liquidity_known,"
                        f"tick_lower,tick_upper FROM lp_accounting_positions "
                        f"INDEXED BY lp_accounting_positions_active_inventory "
                        f"WHERE pool_id IN ({position_marks}) "
                        f"AND active_episode_id IS NOT NULL ORDER BY pool_id",
                        missing_inventory,
                    )
                    evaluated: set[str] = set()
                    for raw_pool_id, group in groupby(
                        position_rows, key=lambda row: str(row[0]).lower(),
                    ):
                        pool_id = str(raw_pool_id)
                        lp_count, history_complete = summaries.get(
                            pool_id, (0, True),
                        )
                        result, inventory = self._reduce_pool_inventory(
                            (
                                self._pool_inventory_position(row)
                                for row in group
                            ),
                            lp_count=lp_count,
                            history_complete=history_complete,
                            mark=state.get(pool_id, {}),
                            metadata=metadata.get(pool_id, {}),
                            history_from=history_from,
                            backfill_done=backfill_done,
                            retain_inventory=True,
                        )
                        evaluated.add(pool_id)
                        results[pool_id] = result
                        self._cache_pool_stats(
                            pool_id, generations[pool_id],
                            cache_keys[pool_id], result, inventory,
                        )
                    for pool_id in missing_inventory:
                        if pool_id in evaluated:
                            continue
                        result, inventory = self._reduce_pool_inventory(
                            (),
                            lp_count=0,
                            history_complete=True,
                            mark=state.get(pool_id, {}),
                            metadata=metadata.get(pool_id, {}),
                            history_from=history_from,
                            backfill_done=backfill_done,
                            retain_inventory=True,
                        )
                        results[pool_id] = result
                        self._cache_pool_stats(
                            pool_id, generations[pool_id],
                            cache_keys[pool_id], result, inventory,
                        )
            finally:
                if owns_snapshot and conn.in_transaction:
                    conn.rollback()
        return {
            pool_id: results.get(pool_id, dict(empty))
            for pool_id in requested
        }


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []
