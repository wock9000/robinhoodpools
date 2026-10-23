"""PONS / FOMO flow tags for live swap and LP events.

FOMO means a Relay-routed trade whose counterparty wallet carries FOMO's
EIP-7702 delegation code (Simple7702Account ``0xe6ca...555b``). Residual risk:
any wallet that delegates to the same implementation is indistinguishable on
chain; apollo's own copy wallets (``0x2fbd...4221`` verified) carry it too.
"""
from __future__ import annotations

import bisect
import importlib
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

PONS = "PONS"
FOMO = "FOMO"
BASIS_PONS_HOOK = "pons_hook"
BASIS_RELAY_FOOTPRINT = "relay_footprint"
BASIS_FOMO_DEPOSIT = "fomo_deposit"
BASIS_FOMO_FILL = "fomo_fill"
BASIS_FOMO_USER_OP = "fomo_user_op"
BASIS_FOMO_LISTENER = "fomo_listener"
CHAIN_FOMO_BASIS = frozenset({BASIS_FOMO_DEPOSIT, BASIS_FOMO_FILL, BASIS_FOMO_USER_OP})
ALL_BASIS = (
    BASIS_PONS_HOOK, BASIS_RELAY_FOOTPRINT, BASIS_FOMO_DEPOSIT, BASIS_FOMO_FILL,
    BASIS_FOMO_USER_OP, BASIS_FOMO_LISTENER,
)

PONS_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
LAUNCHES_SELECTOR = "0xad091230"
FOMO_ROUTER = "0xccc88a9d1b4ed6b0eaba998850414b24f1c315be"
DEPOSITORY = "0x4cd00e387622c35bddb9b4c962c136462338bc31"
EXECUTOR = "0xb92fe925dc43a0ecde6c8b1a2709c170ec4fff4f"
EXECUTOR_TOPIC = "0x" + EXECUTOR[2:].rjust(64, "0")
ENTRYPOINT = "0x4337084d9e255ff0702461cf8895ce9e3b5ff108"
POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
RELAY_CONTRACTS = frozenset({FOMO_ROUTER, DEPOSITORY, EXECUTOR, POOL_MANAGER})
FOMO_WALLET_CODE = "0xef0100e6cae83bde06e4c305530e199d7217f42808555b"
TOPIC_DEPOSIT = "0x49fed1d0b752ce30eee63c7a81133f3363b532fec5d4d7dd1ccfd005de4555e1"
TOPIC_NATIVE_DEPOSIT = "0x8032066556caf3967d8fec4ad22a2d9e1e9576556b2903a0fcd5b1fd201e3477"
TOPIC_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_USER_OPERATION = "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f"
LISTENER_FOMO_KINDS = frozenset({"payment", "destination_transfer"})

RETENTION_S = 7 * 24 * 3600
WALLET_CODE_TTL_S = 7 * 24 * 3600
LISTENER_WINDOW_S = 1800
LISTENER_STATEMENT_TIMEOUT_MS = 2000
LISTENER_RETRY_S = 30.0
LISTENER_TAIL_ROWS = 5000
LISTENER_TAIL_ROWS_PER_REFRESH = 50000


@dataclass(frozen=True, slots=True)
class PoolIdentity:
    pool_id: str
    protocol: str
    hook: str | None
    pons_registered: bool


@dataclass(frozen=True, slots=True)
class TxEnvelope:
    tx_hash: str
    block_number: int
    block_time: int
    sender: str
    to: str | None
    tx_type: int
    router_order_id: str | None
    deposit_order_ids: tuple[str, ...]
    deposit_wallets: tuple[str, ...]
    fill_recipients: tuple[str, ...]
    zero_gas_senders: tuple[str, ...]

    @property
    def relay_footprint(self) -> bool:
        return bool(self.deposit_wallets or self.fill_recipients)

    @property
    def wallets(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(
            (*self.deposit_wallets, *self.fill_recipients, *self.zero_gas_senders)
        ))

    @property
    def order_ids(self) -> tuple[str, ...]:
        ids = [self.router_order_id] if self.router_order_id else []
        ids.extend(self.deposit_order_ids)
        return tuple(dict.fromkeys(ids))


@dataclass(frozen=True, slots=True)
class ListenerObservation:
    event_index: str
    kind: str
    status: str
    order_id: str | None
    event_at_ms: int


@dataclass(frozen=True, slots=True)
class ListenerFacts:
    observations: tuple[ListenerObservation, ...]
    order_first_seen_ms: int | None
    covered: bool = True


@dataclass(frozen=True, slots=True)
class FlowTag:
    tx_hash: str
    pool_id: str
    tags: frozenset[str]
    basis: frozenset[str]
    early_ms: int | None

    @property
    def chain_fomo(self) -> bool:
        return bool(self.basis & CHAIN_FOMO_BASIS)

    @property
    def listener_fomo(self) -> bool:
        return BASIS_FOMO_LISTENER in self.basis

    def as_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "pool_id": self.pool_id,
            "tags": sorted(self.tags),
            "basis": sorted(self.basis),
        }


def classify(
    pool: PoolIdentity, env: TxEnvelope, observed: ListenerFacts | None,
    fomo_wallets: Collection[str] = (),
) -> FlowTag:
    basis: set[str] = set()
    if pool.pons_registered:
        basis.add(BASIS_PONS_HOOK)
    if env.relay_footprint:
        basis.add(BASIS_RELAY_FOOTPRINT)
    if any(wallet in fomo_wallets for wallet in env.deposit_wallets):
        basis.add(BASIS_FOMO_DEPOSIT)
    if any(wallet in fomo_wallets for wallet in env.fill_recipients):
        basis.add(BASIS_FOMO_FILL)
    if env.to == ENTRYPOINT and any(wallet in fomo_wallets for wallet in env.zero_gas_senders):
        basis.add(BASIS_FOMO_USER_OP)
    early_ms = None
    if observed is not None:
        if any(
            item.status == "observed" and item.kind in LISTENER_FOMO_KINDS
            for item in observed.observations
        ):
            basis.add(BASIS_FOMO_LISTENER)
        if observed.order_first_seen_ms is not None:
            delta = env.block_time * 1000 - observed.order_first_seen_ms
            early_ms = delta if delta > 0 else None
    tags: set[str] = set()
    if BASIS_PONS_HOOK in basis:
        tags.add(PONS)
    if basis & CHAIN_FOMO_BASIS:
        tags.add(FOMO)
    return FlowTag(
        env.tx_hash, pool.pool_id, frozenset(tags), frozenset(basis),
        early_ms if FOMO in tags else None,
    )


def _word(data: str, index: int) -> str:
    start = 2 + index * 64
    return data[start:start + 64]


@dataclass(frozen=True, slots=True)
class RelayFootprint:
    deposit_order_ids: tuple[str, ...]
    deposit_wallets: tuple[str, ...]
    fill_recipients: tuple[str, ...]
    zero_gas_senders: tuple[str, ...]


def relay_footprint(logs: Iterable[Mapping[str, Any]]) -> RelayFootprint:
    """Relay depository deposits, executor fills and EntryPoint operations: the
    depository/executor logs are what relay-listener keys its observations on;
    the zero-gas operation is how FOMO's bundler posts every op (upstream docs)."""
    order_ids: list[str] = []
    depositors: list[str] = []
    recipients: list[str] = []
    zero_gas: list[str] = []
    for log in logs:
        if log.get("removed"):
            continue
        topics = [str(item).lower() for item in log.get("topics") or ()]
        if not topics:
            continue
        address = str(log["address"]).lower()
        data = str(log.get("data") or "0x")
        if address == DEPOSITORY and topics[0] in (TOPIC_DEPOSIT, TOPIC_NATIVE_DEPOSIT):
            order_ids.append("0x" + _word(data, 3 if topics[0] == TOPIC_DEPOSIT else 2))
            depositors.append("0x" + _word(data, 0)[-40:])
        elif topics[0] == TOPIC_TRANSFER and len(topics) == 3 and topics[1] == EXECUTOR_TOPIC:
            recipient = "0x" + topics[2][-40:]
            if recipient not in RELAY_CONTRACTS:
                recipients.append(recipient)
        elif address == ENTRYPOINT and topics[0] == TOPIC_USER_OPERATION and len(topics) == 4:
            success = int(_word(data, 1) or "0", 16) == 1
            gas_cost = int(_word(data, 2) or "0", 16)
            if success and gas_cost == 0 and int(topics[3], 16) == 0:
                zero_gas.append("0x" + topics[2][-40:])
    return RelayFootprint(
        tuple(dict.fromkeys(order_ids)), tuple(dict.fromkeys(depositors)),
        tuple(dict.fromkeys(recipients)), tuple(dict.fromkeys(zero_gas)),
    )


def envelope(tx: Mapping[str, Any], block_time: int, logs: Iterable[Mapping[str, Any]]) -> TxEnvelope:
    to = str(tx["to"]).lower() if tx.get("to") else None
    data = str(tx.get("input") or "0x")
    router_order_id = "0x" + data[-64:] if to == FOMO_ROUTER and len(data) >= 66 else None
    footprint = relay_footprint(logs)
    return TxEnvelope(
        tx_hash=str(tx["hash"]).lower(),
        block_number=int(str(tx["blockNumber"]), 16),
        block_time=int(block_time),
        sender=str(tx["from"]).lower(),
        to=to,
        tx_type=int(str(tx.get("type") or "0x0"), 16),
        router_order_id=router_order_id,
        deposit_order_ids=footprint.deposit_order_ids,
        deposit_wallets=footprint.deposit_wallets,
        fill_recipients=footprint.fill_recipients,
        zero_gas_senders=footprint.zero_gas_senders,
    )


class Rpc(Protocol):
    def batch(self, calls: Iterable[tuple[str, Sequence[Any]]]) -> list[Any]: ...


def footprint_filters(block: int) -> list[tuple[str, list[Any]]]:
    window = {"fromBlock": hex(block), "toBlock": hex(block)}
    return [
        ("eth_getLogs", [{**window, "address": DEPOSITORY, "topics": [[TOPIC_DEPOSIT, TOPIC_NATIVE_DEPOSIT]]}]),
        ("eth_getLogs", [{**window, "topics": [TOPIC_TRANSFER, EXECUTOR_TOPIC]}]),
        ("eth_getLogs", [{**window, "address": ENTRYPOINT, "topics": [TOPIC_USER_OPERATION]}]),
    ]


def fetch_envelopes(rpc: Rpc, requests: Sequence[tuple[str, int, int]]) -> dict[str, TxEnvelope]:
    if not requests:
        return {}
    head = int(rpc.batch([("eth_blockNumber", [])])[0], 16)
    requests = [request for request in requests if int(request[1]) <= head]
    times = {tx_hash.lower(): int(block_time) for tx_hash, _block, block_time in requests}
    if not times:
        return {}
    hashes = list(times)
    blocks = dict.fromkeys(int(block) for _hash, block, _time in requests)
    calls = [("eth_getTransactionByHash", [tx_hash]) for tx_hash in hashes]
    calls.extend(call for block in blocks for call in footprint_filters(block))
    results = []
    for offset in range(0, len(calls), 50):
        results.extend(rpc.batch(calls[offset:offset + 50]))
    logs_by_tx: dict[str, list[Mapping[str, Any]]] = {}
    for batch in results[len(hashes):]:
        for log in batch or ():
            logs_by_tx.setdefault(str(log["transactionHash"]).lower(), []).append(log)
    return {
        tx_hash: envelope(tx, times[tx_hash], logs_by_tx.get(tx_hash, ()))
        for tx_hash, tx in zip(hashes, results[:len(hashes)])
        if isinstance(tx, Mapping) and tx.get("blockNumber")
    }


class TagStore:
    """Separate file: the market database has a single writer this store must never join."""

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS flow_tags(
            tx_hash TEXT NOT NULL, pool_id TEXT NOT NULL,
            block_number INTEGER NOT NULL, block_time INTEGER NOT NULL,
            tags TEXT NOT NULL, basis TEXT NOT NULL, early_ms INTEGER,
            chain_fomo INTEGER NOT NULL, listener_fomo INTEGER NOT NULL,
            listener_checked INTEGER NOT NULL, created_at INTEGER NOT NULL,
            PRIMARY KEY(tx_hash, pool_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS flow_tags_time_idx ON flow_tags(block_time);
        CREATE TABLE IF NOT EXISTS pons_pools(
            pool_id TEXT PRIMARY KEY, registered INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS wallet_codes(
            wallet TEXT PRIMARY KEY, fomo INTEGER NOT NULL, checked_at INTEGER NOT NULL
        ) WITHOUT ROWID;
    """

    def __init__(self, path: str, *, retention_s: int = RETENTION_S) -> None:
        self.path = path
        self.retention_s = int(retention_s)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.executescript(self.SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def get(self, keys: Iterable[tuple[str, str]]) -> dict[tuple[str, str], FlowTag]:
        found: dict[tuple[str, str], FlowTag] = {}
        with self._lock:
            for tx_hash, pool_id in dict.fromkeys(keys):
                row = self._connection.execute(
                    "SELECT tags,basis,early_ms FROM flow_tags WHERE tx_hash=? AND pool_id=?",
                    (tx_hash, pool_id),
                ).fetchone()
                if row is not None:
                    found[(tx_hash, pool_id)] = FlowTag(
                        tx_hash, pool_id, frozenset(row["tags"].split()),
                        frozenset(row["basis"].split()), row["early_ms"],
                    )
        return found

    def put(self, tags: Iterable[tuple[FlowTag, int, int, bool]], *, now: int) -> None:
        rows = [
            (
                tag.tx_hash, tag.pool_id, int(block_number), int(block_time),
                " ".join(sorted(tag.tags)), " ".join(sorted(tag.basis)), tag.early_ms,
                int(tag.chain_fomo), int(tag.listener_fomo), int(listener_checked), int(now),
            )
            for tag, block_number, block_time, listener_checked in tags
        ]
        if not rows:
            return
        with self._lock, self._connection:
            self._connection.executemany(
                "INSERT INTO flow_tags(tx_hash,pool_id,block_number,block_time,tags,basis,"
                "early_ms,chain_fomo,listener_fomo,listener_checked,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(tx_hash,pool_id) DO UPDATE SET "
                "tags=excluded.tags,basis=excluded.basis,early_ms=excluded.early_ms,"
                "chain_fomo=excluded.chain_fomo,listener_fomo=excluded.listener_fomo,"
                "listener_checked=excluded.listener_checked,created_at=excluded.created_at",
                rows,
            )

    def prune(self, now: int) -> int:
        with self._lock, self._connection:
            return self._connection.execute(
                "DELETE FROM flow_tags WHERE block_time<?", (int(now) - self.retention_s,),
            ).rowcount

    def pons_pools(self) -> dict[str, bool]:
        with self._lock:
            return {
                row["pool_id"]: bool(row["registered"])
                for row in self._connection.execute("SELECT pool_id,registered FROM pons_pools")
            }

    def remember_pons(self, registrations: Mapping[str, bool]) -> None:
        if not registrations:
            return
        with self._lock, self._connection:
            self._connection.executemany(
                "INSERT OR REPLACE INTO pons_pools(pool_id,registered) VALUES(?,?)",
                [(pool_id, int(flag)) for pool_id, flag in registrations.items()],
            )

    def wallet_codes(self) -> dict[str, tuple[bool, int]]:
        with self._lock:
            return {
                row["wallet"]: (bool(row["fomo"]), int(row["checked_at"]))
                for row in self._connection.execute("SELECT wallet,fomo,checked_at FROM wallet_codes")
            }

    def remember_codes(self, codes: Mapping[str, tuple[bool, int]]) -> None:
        if not codes:
            return
        with self._lock, self._connection:
            self._connection.executemany(
                "INSERT OR REPLACE INTO wallet_codes(wallet,fomo,checked_at) VALUES(?,?,?)",
                [(wallet, int(fomo), int(checked_at)) for wallet, (fomo, checked_at) in codes.items()],
            )

    def status(self) -> dict[str, Any]:
        basis_sums = ",".join(
            f"SUM(instr(' '||basis||' ',' {name} ')>0) AS b_{name}" for name in ALL_BASIS
        )
        with self._lock:
            counts = self._connection.execute(
                "SELECT COUNT(*) AS rows_total,"
                "SUM(tags LIKE '%PONS%') AS pons,SUM(tags LIKE '%FOMO%') AS fomo,"
                "SUM(listener_checked) AS compared,"
                "SUM(listener_checked AND chain_fomo AND listener_fomo) AS agree_fomo,"
                "SUM(listener_checked AND chain_fomo AND NOT listener_fomo) AS chain_only,"
                "SUM(listener_checked AND NOT chain_fomo AND listener_fomo) AS listener_only,"
                f"{basis_sums} FROM flow_tags"
            ).fetchone()
            pons_pools = self._connection.execute(
                "SELECT COUNT(*) AS total,SUM(registered) AS registered FROM pons_pools"
            ).fetchone()
            wallets = self._connection.execute(
                "SELECT COUNT(*) AS total,SUM(fomo) AS fomo FROM wallet_codes"
            ).fetchone()
        compared = int(counts["compared"] or 0)
        disagree = int(counts["chain_only"] or 0) + int(counts["listener_only"] or 0)
        return {
            "rows": int(counts["rows_total"] or 0),
            "pons": int(counts["pons"] or 0),
            "fomo": int(counts["fomo"] or 0),
            "basis": {name: int(counts[f"b_{name}"] or 0) for name in ALL_BASIS},
            "pons_pools": {
                "cached": int(pons_pools["total"] or 0),
                "registered": int(pons_pools["registered"] or 0),
            },
            "wallet_codes": {
                "cached": int(wallets["total"] or 0),
                "fomo": int(wallets["fomo"] or 0),
            },
            "fomo_agreement": {
                "compared": compared,
                "agree_fomo": int(counts["agree_fomo"] or 0),
                "chain_only": int(counts["chain_only"] or 0),
                "listener_only": int(counts["listener_only"] or 0),
                "disagreement_rate": (disagree / compared) if compared else None,
            },
        }


class PonsRegistry:
    """The hook writes launches(poolId) once at launch and never again, so word 0 is cached forever."""

    def __init__(self, rpc: Rpc, store: TagStore) -> None:
        self._rpc = rpc
        self._store = store
        self._cache = store.pons_pools()
        self._lock = threading.Lock()

    def identify(self, pools: Iterable[Mapping[str, Any]]) -> dict[str, PoolIdentity]:
        rows = {str(pool["id"]).lower(): pool for pool in pools}
        candidates = {
            pool_id for pool_id, pool in rows.items()
            if str(pool.get("protocol") or "").lower() == "v4"
            and str(pool.get("hook") or "").lower() == PONS_HOOK
        }
        with self._lock:
            unknown = sorted(candidates - self._cache.keys())
            if unknown:
                results = self._rpc.batch([
                    (
                        "eth_call",
                        [{"to": PONS_HOOK, "data": LAUNCHES_SELECTOR + pool_id[2:]}, "latest"],
                    )
                    for pool_id in unknown
                ])
                learned = {
                    pool_id: len(str(result)) >= 66 and int(str(result)[2:66], 16) != 0
                    for pool_id, result in zip(unknown, results)
                }
                self._store.remember_pons(learned)
                self._cache.update(learned)
            return {
                pool_id: PoolIdentity(
                    pool_id, str(pool.get("protocol") or "").lower(),
                    str(pool["hook"]).lower() if pool.get("hook") else None,
                    pool_id in candidates and self._cache.get(pool_id, False),
                )
                for pool_id, pool in rows.items()
            }


class WalletCodes:
    """eth_getCode per wallet, cached in the tag store; a delegation can change,
    so entries are re-read after ``ttl_s``."""

    def __init__(
        self, rpc: Rpc, store: TagStore, *, ttl_s: int = WALLET_CODE_TTL_S,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._rpc = rpc
        self._store = store
        self._ttl_s = int(ttl_s)
        self._clock = clock
        self._cache = store.wallet_codes()
        self._lock = threading.Lock()

    def fomo(self, wallets: Iterable[str]) -> frozenset[str]:
        wanted = sorted({wallet.lower() for wallet in wallets})
        now = int(self._clock())
        with self._lock:
            stale = [
                wallet for wallet in wanted
                if wallet not in self._cache or self._cache[wallet][1] + self._ttl_s <= now
            ]
            if stale:
                codes = self._rpc.batch([("eth_getCode", [wallet, "latest"]) for wallet in stale])
                learned = {
                    wallet: (str(code).lower() == FOMO_WALLET_CODE, now)
                    for wallet, code in zip(stale, codes)
                }
                self._store.remember_codes(learned)
                self._cache.update(learned)
            return frozenset(wallet for wallet in wanted if self._cache[wallet][0])


class ListenerSource(Protocol):
    def facts(self, envelopes: Sequence[TxEnvelope]) -> dict[str, ListenerFacts]: ...
    def state(self) -> dict[str, Any]: ...


class NoListener:
    def __init__(self, state: str = "unset", reason: str | None = None) -> None:
        self._state = state
        self._reason = reason

    def facts(self, envelopes: Sequence[TxEnvelope]) -> dict[str, ListenerFacts]:
        return {}

    def state(self) -> dict[str, Any]:
        return {"state": self._state, "reason": self._reason}


@dataclass(slots=True)
class ObservationWindow:
    window_s: int = LISTENER_WINDOW_S
    _by_hash: dict[str, dict[str, ListenerObservation]] = field(default_factory=dict)
    _order: list[tuple[int, str]] = field(default_factory=list)
    _latest_ms: int = 0
    _latest_block: int = 0

    def ingest(self, rows: Iterable[Mapping[str, Any]]) -> None:
        for row in rows:
            tx_hash = str(row["transaction_hash"]).lower()
            item = ListenerObservation(
                str(row["event_index"]), str(row["kind"]), str(row["status"]),
                str(row["order_id"]).lower() if row.get("order_id") else None,
                int(row["event_at_ms"]),
            )
            events = self._by_hash.setdefault(tx_hash, {})
            events[item.event_index] = item
            bisect.insort(self._order, (item.event_at_ms, tx_hash))
            self._latest_ms = max(self._latest_ms, item.event_at_ms)
            self._latest_block = max(self._latest_block, int(row.get("block_number") or 0))
        self._evict()

    def _evict(self) -> None:
        cutoff = self._latest_ms - self.window_s * 1000
        keep = bisect.bisect_left(self._order, (cutoff, ""))
        expired, self._order = self._order[:keep], self._order[keep:]
        for _at, tx_hash in expired:
            events = self._by_hash.get(tx_hash)
            if events is None:
                continue
            for index in [k for k, v in events.items() if v.event_at_ms < cutoff]:
                del events[index]
            if not events:
                del self._by_hash[tx_hash]

    def observations(self, tx_hash: str) -> tuple[ListenerObservation, ...]:
        events = self._by_hash.get(tx_hash.lower(), {})
        return tuple(sorted(events.values(), key=lambda item: item.event_index))

    @property
    def latest_ms(self) -> int:
        return self._latest_ms

    @property
    def latest_block(self) -> int:
        return self._latest_block

    def __len__(self) -> int:
        return sum(len(events) for events in self._by_hash.values())


_OBSERVATION_COLUMNS = (
    "sequence, event_at_unix_ms, order_id, record_kind, "
    "payload->'observation'->>'chain' AS chain, "
    "payload->'observation'->>'transaction_hash' AS transaction_hash, "
    "payload->'observation'->>'event_index' AS event_index, "
    "payload->'observation'->>'observation_kind' AS kind, "
    "payload->'observation'->>'status' AS status, "
    "payload->'observation'->>'block_number' AS block_number"
)


class PostgresListener:
    """The ledger has no transaction-hash index, so lookups by hash come from an
    in-memory window fed by tailing each live source instance along its
    primary key. Order first-seen times use the ``order_id`` index directly.
    """

    def __init__(
        self, dsn: str, *, window_s: int = LISTENER_WINDOW_S,
        statement_timeout_ms: int = LISTENER_STATEMENT_TIMEOUT_MS,
        retry_s: float = LISTENER_RETRY_S, tail_budget: int = LISTENER_TAIL_ROWS_PER_REFRESH,
        clock: Callable[[], float] = time.time, connect: Callable[[str], Any] | None = None,
    ) -> None:
        self._dsn = dsn
        self._window = ObservationWindow(int(window_s))
        self._timeout_ms = int(statement_timeout_ms)
        self._retry_s = float(retry_s)
        self._tail_budget = int(tail_budget)
        self._clock = clock
        self._connect = connect or _psycopg_connect
        self._connection: Any = None
        self._cursors: dict[str, int] = {}
        self._error: str | None = None
        self._retry_at = 0.0
        self._refreshed_at: float | None = None
        self._lock = threading.Lock()

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "state": "down" if self._error else ("connected" if self._connection else "idle"),
                "reason": self._error,
                "instances": len(self._cursors),
                "observations": len(self._window),
                "window_s": self._window.window_s,
                "latest_event_ms": self._window.latest_ms or None,
                "latest_block": self._window.latest_block or None,
                "refreshed_at": self._refreshed_at,
            }

    def close(self) -> None:
        with self._lock:
            self._drop()

    def refresh(self) -> bool:
        """Pull new ledger rows into the window; False while down or cooling off."""
        with self._lock:
            return self._refresh_locked()

    def _refresh_locked(self) -> bool:
        if self._connection is None and self._clock() < self._retry_at:
            return False
        try:
            self._refresh()
        except Exception as exc:
            self._fail(exc)
            return False
        return True

    def facts(self, envelopes: Sequence[TxEnvelope]) -> dict[str, ListenerFacts]:
        if not envelopes:
            return {}
        with self._lock:
            if not self._refresh_locked():
                return {}
            order_ids = sorted({order_id for env in envelopes for order_id in env.order_ids})
            try:
                first_seen = self._order_first_seen(order_ids) if order_ids else {}
            except Exception as exc:
                self._fail(exc)
                return {}
            covered_to = self._window.latest_block
            return {
                env.tx_hash: ListenerFacts(
                    self._window.observations(env.tx_hash),
                    min(
                        (first_seen[o] for o in env.order_ids if o in first_seen),
                        default=None,
                    ),
                    env.block_number <= covered_to,
                )
                for env in envelopes
            }

    def _fail(self, exc: BaseException) -> None:
        self._error = f"{type(exc).__name__}: {exc}"[:500]
        self._retry_at = self._clock() + self._retry_s
        self._drop()

    def _drop(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass

    def _query(self, sql: str, params: Sequence[Any] = ()) -> list[Any]:
        if self._connection is None:
            if self._clock() < self._retry_at:
                raise RuntimeError(self._error or "listener source is cooling down")
            self._connection = self._connect(self._dsn)
            self._cursors.clear()
        with self._connection.transaction():
            self._connection.execute("SET TRANSACTION READ ONLY")
            self._connection.execute(f"SET LOCAL statement_timeout = {self._timeout_ms}")
            return self._connection.execute(sql, list(params)).fetchall()

    def _refresh(self) -> None:
        window_ms = self._window.window_s * 1000
        since_ms = int(self._clock() * 1000) - window_ms
        live = self._query(
            "SELECT source_instance_id, last_sequence FROM relay_listener_research_source_state "
            "WHERE last_event_at_unix_ms >= %s",
            (since_ms,),
        )
        budget = self._tail_budget
        for instance, last_sequence in live:
            cursor = self._cursors.get(instance)
            if cursor is None:
                cursor = self._seek(instance, int(last_sequence), since_ms)
            while budget > 0:
                rows = self._query(
                    f"SELECT {_OBSERVATION_COLUMNS} FROM relay_listener_research_event "
                    "WHERE source_instance_id = %s AND sequence > %s ORDER BY sequence LIMIT %s",
                    (instance, cursor, min(LISTENER_TAIL_ROWS, budget)),
                )
                if not rows:
                    break
                budget -= len(rows)
                cursor = int(rows[-1][0])
                self._window.ingest(
                    {
                        "transaction_hash": row[5], "event_index": row[6], "kind": row[7],
                        "status": row[8], "order_id": row[2], "event_at_ms": row[1],
                        "block_number": row[9],
                    }
                    for row in rows
                    if row[3] == "fomo_observation" and row[4] == "robinhood" and row[5]
                )
                if len(rows) < LISTENER_TAIL_ROWS:
                    break
            self._cursors[instance] = cursor
        self._error = None
        self._refreshed_at = self._clock()

    def _seek(self, instance: str, last_sequence: int, since_ms: int) -> int:
        low, high = 0, last_sequence
        while low < high:
            middle = (low + high + 1) // 2
            rows = self._query(
                "SELECT event_at_unix_ms FROM relay_listener_research_event "
                "WHERE source_instance_id = %s AND sequence >= %s ORDER BY sequence LIMIT 1",
                (instance, middle),
            )
            if rows and int(rows[0][0]) < since_ms:
                low = middle
            else:
                high = middle - 1
        return low

    def _order_first_seen(self, order_ids: Sequence[str]) -> dict[str, int]:
        rows = self._query(
            "SELECT order_id, MIN(event_at_unix_ms) FROM relay_listener_research_event "
            "WHERE order_id = ANY(%s) GROUP BY order_id",
            (list(order_ids),),
        )
        return {str(order_id).lower(): int(first) for order_id, first in rows}


def _psycopg_connect(dsn: str) -> Any:
    import psycopg

    connection = psycopg.connect(
        dsn, connect_timeout=5, application_name="rhpools-flow-tags", autocommit=False,
    )
    connection.read_only = True
    return connection


def listener_from_env(environ: Mapping[str, str] = os.environ) -> ListenerSource:
    dsn = environ.get("RHP_LISTENER_DSN", "").strip()
    if not dsn:
        return NoListener("unset")
    try:
        importlib.import_module("psycopg")
    except ImportError:
        return NoListener("unavailable", "psycopg is not installed")
    window = environ.get("RHP_LISTENER_WINDOW_S", "").strip()
    return PostgresListener(dsn, window_s=int(window) if window else LISTENER_WINDOW_S)


class FlowTagger:
    def __init__(
        self, rpc: Rpc, store: TagStore, pools: Callable[[str], Mapping[str, Any] | None],
        listener: ListenerSource | None = None, *, clock: Callable[[], float] = time.time,
    ) -> None:
        self._rpc = rpc
        self._store = store
        self._pools = pools
        self._listener = listener or NoListener()
        self._clock = clock
        self._registry = PonsRegistry(rpc, store)
        self._codes = WalletCodes(rpc, store, clock=clock)

    def tag(self, rows: Iterable[Mapping[str, Any]]) -> list[FlowTag]:
        wanted: dict[tuple[str, str], tuple[int, int]] = {}
        for row in rows:
            tx_hash = str(row["tx_hash"]).lower()
            pool_id = str(row.get("pool_id") or "").lower()
            if not pool_id:
                continue
            wanted[(tx_hash, pool_id)] = (int(row["block_number"]), int(row["timestamp"]))
        now = int(self._clock())
        wanted = {key: value for key, value in wanted.items() if value[1] >= now - self._store.retention_s}
        found = self._store.get(wanted)
        missing = [key for key in wanted if key not in found]
        if missing:
            found.update(self._classify(
                {key: wanted[key] for key in missing},
            ))
        return [found[key] for key in wanted if key in found]

    def _classify(
        self, wanted: Mapping[tuple[str, str], tuple[int, int]],
    ) -> dict[tuple[str, str], FlowTag]:
        envelopes = fetch_envelopes(
            self._rpc,
            [
                (tx_hash, block_number, block_time)
                for (tx_hash, _pool), (block_number, block_time) in wanted.items()
            ],
        )
        pool_rows = []
        for pool_id in dict.fromkeys(pool_id for _tx, pool_id in wanted):
            row = self._pools(pool_id)
            pool_rows.append(row if row is not None else {"id": pool_id, "protocol": "", "hook": None})
        identities = self._registry.identify(pool_rows)
        fomo_wallets = self._codes.fomo(
            wallet for env in envelopes.values() for wallet in env.wallets
        )
        facts = self._listener.facts(list(envelopes.values()))
        tagged: dict[tuple[str, str], FlowTag] = {}
        stored: list[tuple[FlowTag, int, int, bool]] = []
        for (tx_hash, pool_id), (block_number, block_time) in wanted.items():
            env = envelopes.get(tx_hash)
            if env is None:
                continue
            observed = facts.get(tx_hash)
            tag = classify(identities[pool_id], env, observed, fomo_wallets)
            tagged[(tx_hash, pool_id)] = tag
            stored.append((tag, block_number, block_time, observed is not None and observed.covered))
        now = int(self._clock())
        self._store.put(stored, now=now)
        self._store.prune(now)
        return tagged

    def status(self) -> dict[str, Any]:
        return {"listener": self._listener.state(), **self._store.status()}


__all__ = [
    "ALL_BASIS",
    "BASIS_FOMO_DEPOSIT",
    "BASIS_FOMO_FILL",
    "BASIS_FOMO_LISTENER",
    "BASIS_FOMO_USER_OP",
    "BASIS_PONS_HOOK",
    "BASIS_RELAY_FOOTPRINT",
    "CHAIN_FOMO_BASIS",
    "DEPOSITORY",
    "ENTRYPOINT",
    "EXECUTOR",
    "FOMO",
    "FOMO_ROUTER",
    "FOMO_WALLET_CODE",
    "FlowTag",
    "FlowTagger",
    "ListenerFacts",
    "ListenerObservation",
    "ListenerSource",
    "NoListener",
    "ObservationWindow",
    "PONS",
    "PONS_HOOK",
    "POOL_MANAGER",
    "PonsRegistry",
    "PoolIdentity",
    "PostgresListener",
    "RELAY_CONTRACTS",
    "RelayFootprint",
    "TOPIC_USER_OPERATION",
    "TagStore",
    "TxEnvelope",
    "WalletCodes",
    "classify",
    "envelope",
    "fetch_envelopes",
    "footprint_filters",
    "listener_from_env",
    "relay_footprint",
]
