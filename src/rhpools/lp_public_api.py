"""Public, read-only token-to-pool snapshots and defensible asset aggregates.

The durable LP service owns catalog/history storage and the workbench owns the
capability-routed RPC client.  This adapter owns neither resource: it joins the
verified known catalogs, reads current state at one block, and confirms that
block's canonical hash before publishing a response.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any
import json
import re
import threading
import time


from . import _mc
from .lp_chain import CHAIN_ID
from .lp_market_protocols import (
    POOL_MANAGER,
    SLIPSTREAM_FACTORY,
    STATE_VIEW,
    V2_FACTORIES,
    V3_FACTORIES,
    resolve_v4_tick_spacing,
)
from .workbench_market import (
    LIQUIDITY_SELECTOR,
    NATIVE,
    RESERVES_SELECTOR,
    SV_LIQUIDITY_SELECTOR,
    SV_SLOT0_SELECTOR,
)

_ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}\Z")
_HASH_RE = re.compile(r"0x[0-9a-f]{64}\Z")
_VERIFIED_DISCOVERY_BASES = frozenset({
    "factory_creation_event",
    "verified_workbench_catalog",
    "pinned_factory_getPair_membership",
    "pinned_factory_getPool_membership",
})
_IDENTITY_FIELDS = (
    "protocol", "address", "token0", "token1", "tick_spacing", "hook", "factory",
)
_RESPONSE_TTL_SECONDS = 2.0
_MAX_CACHE_ENTRIES = 64
_DIRECT_BATCH_SIZE = 100
_MULTICALL_CHUNK_SIZE = _mc.MAX_PER_BATCH
# At most 1,920 contract reads per HTTP request; the RPC router accounts for
# every enclosed method against the existing per-source request budget.
_MULTICALL_RPC_BATCH_SIZE = 8


class PublicAPIError(RuntimeError):
    """Base class for failures that should be returned as HTTP 503."""


class PublicAPIUnavailable(PublicAPIError):
    """The current canonical snapshot or a required service dependency is unavailable."""


class PublicAPIReorg(PublicAPIError):
    """The pinned block changed before its response could be published."""


@dataclass(frozen=True, slots=True)
class _CallResult:
    ok: bool
    data: bytes | None
    reason: str | None
    transport: str


def _get(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _integer(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return None
    return None


def _address(value: Any) -> str | None:
    normalized = str(value or "").lower()
    return normalized if _ADDRESS_RE.fullmatch(normalized) else None


def _hash(value: Any) -> str | None:
    normalized = str(value or "").lower()
    return normalized if _HASH_RE.fullmatch(normalized) else None


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _format_units(raw: int, decimals: int) -> str:
    sign = "-" if raw < 0 else ""
    digits = str(abs(raw)).rjust(decimals + 1, "0")
    if decimals == 0:
        return sign + digits
    whole, fraction = digits[:-decimals], digits[-decimals:].rstrip("0")
    return sign + whole + ("." + fraction if fraction else "")




def _word(data: bytes | None, index: int = 0) -> int | None:
    if data is None or len(data) < (index + 1) * 32:
        return None
    return int.from_bytes(data[index * 32:(index + 1) * 32], "big")

def _uint_word(
    data: bytes | None, index: int, bits: int,
) -> int | None:
    value = _word(data, index)
    return value if value is not None and value < 1 << bits else None


class PublicMarketAPI:
    """Token lookup facade over one live :class:`LPMarketService`.

    Only ``lp_service`` is required.  Its store, workbench catalog, and
    capability-routed workbench RPC are borrowed and are never closed here.
    """

    def __init__(
        self,
        lp_service: Any,
        *,
        cache_ttl: float = _RESPONSE_TTL_SECONDS,
        max_cache_entries: int = _MAX_CACHE_ENTRIES,
    ) -> None:
        store = getattr(lp_service, "store", None)
        market = getattr(lp_service, "market", None)
        rpc = getattr(market, "rpc", None)
        if not callable(getattr(lp_service, "status", None)):
            raise TypeError("lp_service must provide status()")
        if not callable(getattr(store, "read", None)):
            raise TypeError("lp_service.store must provide read()")
        if getattr(market, "universe", None) is None:
            raise TypeError("lp_service.market must provide the verified workbench universe")
        if not callable(getattr(market, "catalog", None)):
            raise TypeError("lp_service.market must provide catalog(params)")
        if not callable(getattr(rpc, "call", None)) or not callable(getattr(rpc, "batch", None)):
            raise TypeError("lp_service.market.rpc must provide call() and batch()")
        if cache_ttl < 0:
            raise ValueError("cache_ttl must be nonnegative")
        if not 1 <= int(max_cache_entries) <= 256:
            raise ValueError("max_cache_entries must be between 1 and 256")
        self.lp_service = lp_service
        self.store = store
        self.market = market
        self.rpc = rpc
        self.cache_ttl = float(cache_ttl)
        self.max_cache_entries = int(max_cache_entries)
        self._cache: OrderedDict[tuple[Any, ...], tuple[float, dict[str, Any]]] = OrderedDict()
        self._pending: dict[tuple[Any, ...], Future[dict[str, Any]]] = {}
        self._cache_lock = threading.Lock()
        self._closed = False

    def close(self) -> None:
        """Clear adapter caches without closing the borrowed service or RPC."""
        with self._cache_lock:
            self._closed = True
            self._cache.clear()

    @staticmethod
    def _token_param(params: Mapping[str, Any]) -> str:
        if "limit" in params or "offset" in params:
            raise ValueError("public token lookup is not paginated; omit limit and offset")
        token = _address(params.get("token"))
        if token is None:
            raise ValueError("token must be a 20-byte hexadecimal address")
        return token

    def _cached(
        self, key: tuple[Any, ...], loader: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        now = time.monotonic()
        leader = False
        future: Future[dict[str, Any]] | None
        with self._cache_lock:
            if self._closed:
                raise PublicAPIUnavailable("public market API is closed")
            cached = self._cache.get(key)
            if cached is not None and now - cached[0] < self.cache_ttl:
                self._cache.move_to_end(key)
                return cached[1]
            future = self._pending.get(key)
            if future is None:
                leader = True
                if len(self._pending) < self.max_cache_entries:
                    future = Future()
                    self._pending[key] = future
        if not leader:
            assert future is not None
            return future.result()
        try:
            value = loader()
        except BaseException as exc:
            if future is not None:
                with self._cache_lock:
                    self._pending.pop(key, None)
                future.set_exception(exc)
            raise
        with self._cache_lock:
            if future is not None:
                self._pending.pop(key, None)
            if not self._closed:
                self._cache[key] = (time.monotonic(), value)
                self._cache.move_to_end(key)
                while len(self._cache) > self.max_cache_entries:
                    self._cache.popitem(last=False)
        if future is not None:
            future.set_result(value)
        return value

    def _close_reader(self) -> None:
        close_reader = getattr(self.store, "close_reader", None)
        if callable(close_reader):
            close_reader()

    def _stored_matches(self, token: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        catalog_ids: list[str] = []
        connection = self.store.read()
        try:
            stored = connection.execute(
                "SELECT p.*,pp.observed_block,pp.observed_hash,pp.basis AS provenance_basis "
                "FROM pools p LEFT JOIN pool_provenance pp ON pp.pool_id=p.id "
                "WHERE p.token0=? OR p.token1=? ORDER BY p.protocol,p.id",
                (token, token),
            ).fetchall()
            rows = [dict(row) for row in stored]
            status_method = getattr(self.store, "catalog_search_status", None)
            search_ready = False
            if callable(status_method):
                try:
                    search_ready = bool(status_method().get("ready"))
                except Exception:
                    search_ready = False
            if search_ready:
                indexed = connection.execute(
                    "SELECT id FROM lp_catalog_search WHERE token0=? OR token1=? ORDER BY id",
                    (token, token),
                ).fetchall()
                catalog_ids = [str(row["id"]).lower() for row in indexed]
        finally:
            self._close_reader()
        return rows, catalog_ids

    @staticmethod
    def _pool_from_source(raw: Any, *, verified_catalog: bool) -> dict[str, Any] | None:
        protocol = str(_get(raw, "protocol") or _get(raw, "kind") or "").lower()
        pool_id = str(_get(raw, "id") or "").lower()
        address = _address(_get(raw, "address"))
        token0 = _address(_get(raw, "token0"))
        token1 = _address(_get(raw, "token1"))
        if protocol not in {"v2", "v3", "v4"} or address is None or token0 is None or token1 is None:
            return None
        metadata = _metadata(_get(raw, "metadata_json"))
        dynamic_value = _get(raw, "dynamic_fee")
        if dynamic_value is None:
            dynamic_value = metadata.get("dynamic_fee")
        fee = _integer(_get(raw, "fee_ppm"))
        configured = _integer(metadata.get("configured_fee")) if protocol == "v4" else fee
        if protocol == "v4" and configured is None:
            configured = 0x800000 if dynamic_value is True else fee
        return {
            "id": pool_id,
            "protocol": protocol,
            "address": address,
            "token0": token0,
            "token1": token1,
            "symbol0": _get(raw, "symbol0"),
            "symbol1": _get(raw, "symbol1"),
            "decimals0": _integer(_get(raw, "decimals0")),
            "decimals1": _integer(_get(raw, "decimals1")),
            "fee_ppm": fee,
            "configured_fee": configured,
            "dynamic_fee": bool(dynamic_value) if dynamic_value is not None else None,
            "tick_spacing": _integer(_get(raw, "tick_spacing")),
            "hook": _address(_get(raw, "hook")),
            "factory": _address(_get(raw, "factory")),
            "created_block": _integer(_get(raw, "created_block")),
            "source": str(_get(raw, "source") or "unknown")[:128],
            "metadata": metadata,
            "observed_block": _integer(_get(raw, "observed_block")),
            "observed_hash": _hash(_get(raw, "observed_hash")),
            "provenance_basis": _get(raw, "provenance_basis"),
            "verified_catalog": verified_catalog,
        }

    @staticmethod
    def _stored_verified(pool: Mapping[str, Any]) -> bool:
        if pool["protocol"] == "v4":
            return True
        metadata = pool.get("metadata")
        basis = metadata.get("discovery_basis") if isinstance(metadata, Mapping) else None
        factory = pool.get("factory")
        if pool["protocol"] == "v2":
            return factory in V2_FACTORIES and basis in _VERIFIED_DISCOVERY_BASES
        return (
            factory in V3_FACTORIES or factory == SLIPSTREAM_FACTORY
        ) and basis in _VERIFIED_DISCOVERY_BASES

    @staticmethod
    def _merge_candidates(
        current: dict[str, Any], incoming: Mapping[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None]:
        for field in _IDENTITY_FIELDS:
            left, right = current.get(field), incoming.get(field)
            if left is not None and right is not None and left != right:
                return None, "conflicting_verified_identity"
        left_dynamic, right_dynamic = current.get("dynamic_fee"), incoming.get("dynamic_fee")
        if left_dynamic is not None and right_dynamic is not None and left_dynamic != right_dynamic:
            return None, "conflicting_dynamic_fee_mode"
        left_fee, right_fee = current.get("configured_fee"), incoming.get("configured_fee")
        if left_fee is not None and right_fee is not None and left_fee != right_fee:
            return None, "conflicting_fee_configuration"
        merged = dict(current)
        for field, value in incoming.items():
            if field == "metadata":
                merged[field] = {**dict(current.get(field) or {}), **dict(value or {})}
            elif field in {"symbol0", "symbol1", "decimals0", "decimals1"}:
                if value is not None:
                    merged[field] = value
            elif field == "created_block":
                old = merged.get(field)
                if value is not None:
                    merged[field] = value if old is None else min(old, value)
            elif merged.get(field) is None and value is not None:
                merged[field] = value
        merged["verified_catalog"] = bool(
            current.get("verified_catalog") or incoming.get("verified_catalog")
        )
        return merged, None

    @staticmethod
    def _token_metadata(pool: dict[str, Any], universe: Any) -> None:
        tokens = getattr(universe, "tokens", {})
        for side in (0, 1):
            metadata = tokens.get(pool[f"token{side}"]) if isinstance(tokens, Mapping) else None
            if metadata is None:
                continue
            if pool.get(f"symbol{side}") is None:
                pool[f"symbol{side}"] = _get(metadata, "symbol")
            if pool.get(f"decimals{side}") is None:
                pool[f"decimals{side}"] = _integer(_get(metadata, "decimals"))

    @staticmethod
    def _finalize_pool(pool: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        protocol = pool["protocol"]
        expected_id = _HASH_RE if protocol == "v4" else _ADDRESS_RE
        if not expected_id.fullmatch(pool["id"]):
            return None, "malformed_pool_id"
        if protocol != "v4" and pool["address"] != pool["id"]:
            return None, "pool_address_identity_mismatch"
        if protocol == "v4":
            if pool["address"] != POOL_MANAGER:
                return None, "v4_manager_address_mismatch"
            if pool["token0"] >= pool["token1"]:
                return None, "noncanonical_v4_currency_order"
            configured = _integer(pool.get("configured_fee"))
            if configured is None or not 0 <= configured < 1 << 24:
                return None, "missing_v4_configured_fee"
            dynamic = bool(configured & 0x800000)
            declared_dynamic = pool.get("dynamic_fee")
            if declared_dynamic is not None and bool(declared_dynamic) != dynamic:
                return None, "v4_dynamic_fee_flag_mismatch"
            hook = pool.get("hook") or NATIVE
            if _address(hook) is None:
                return None, "missing_v4_hooks_address"
            pool["hook"] = hook
            pool["configured_fee"] = configured
            spacing = resolve_v4_tick_spacing(
                pool_id=str(pool["id"]),
                currency0=str(pool["token0"]),
                currency1=str(pool["token1"]),
                fee=configured,
                hooks=hook,
                tick_spacing=_integer(pool.get("tick_spacing")),
            )
            if spacing is None:
                return None, "v4_pool_key_hash_mismatch"
            pool["tick_spacing"] = spacing
        elif protocol == "v3":
            fee = _integer(pool.get("fee_ppm"))
            if fee is not None and not 0 <= fee <= 1_000_000:
                return None, "invalid_v3_fee"
            pool["configured_fee"] = fee
            pool["dynamic_fee"] = False if fee is not None else None
        else:
            pool["configured_fee"] = None
            pool["dynamic_fee"] = None
            pool["tick_spacing"] = None
            pool["hook"] = None
        if not pool.get("verified_catalog") and not PublicMarketAPI._stored_verified(pool):
            return None, "unverified_pool_identity"
        return pool, None

    @staticmethod
    def _source_contains_token(raw: Any, token: str) -> bool:
        token0 = _get(raw, "token0")
        token1 = _get(raw, "token1")
        return (
            token0 == token
            or token1 == token
            or isinstance(token0, str) and token0.lower() == token
            or isinstance(token1, str) and token1.lower() == token
        )


    def _known_pools(self, token: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        stored, catalog_ids = self._stored_matches(token)
        lock = getattr(self.market, "_lock", None)
        if lock is not None:
            lock.acquire()
        try:
            universe = self.market.universe
            by_id = getattr(universe, "by_id", {}) or {}
            discovered = getattr(self.market, "_discovered", {}) or {}
            if catalog_ids:
                catalog_raw = [
                    by_id.get(pool_id) or discovered.get(pool_id)
                    for pool_id in catalog_ids
                ]
            else:
                catalog_raw = [
                    raw for raw in (getattr(universe, "pools", ()) or ())
                    if self._source_contains_token(raw, token)
                ]
            # The durable catalog-search projection may trail live discoveries.
            # Copy only matching identities while holding the producer's lock;
            # normalizing the complete live catalog made hub-token lookups scale
            # with every known pool rather than with their returned result.
            catalog_raw.extend(
                raw for raw in discovered.values()
                if self._source_contains_token(raw, token)
            )
        finally:
            if lock is not None:
                lock.release()

        candidates: dict[str, dict[str, Any] | None] = {}
        issues: list[str] = []
        for raw in catalog_raw:
            if raw is None:
                issues.append("catalog_identity_not_materialized")
                continue
            candidate = self._pool_from_source(raw, verified_catalog=True)
            if candidate is None or token not in (candidate["token0"], candidate["token1"]):
                continue
            previous = candidates.get(candidate["id"])
            if previous is None and candidate["id"] not in candidates:
                candidates[candidate["id"]] = candidate
            elif previous is not None:
                candidates[candidate["id"]], issue = self._merge_candidates(previous, candidate)
                if issue:
                    issues.append(issue)
        for raw in stored:
            candidate = self._pool_from_source(raw, verified_catalog=False)
            if candidate is None:
                issues.append("malformed_stored_pool")
                continue
            previous = candidates.get(candidate["id"])
            if previous is None and candidate["id"] not in candidates:
                candidates[candidate["id"]] = candidate
            elif previous is not None:
                candidates[candidate["id"]], issue = self._merge_candidates(previous, candidate)
                if issue:
                    issues.append(issue)

        pools: list[dict[str, Any]] = []
        for candidate in candidates.values():
            if candidate is None:
                continue
            self._token_metadata(candidate, universe)
            finalized, issue = self._finalize_pool(candidate)
            if finalized is None:
                issues.append(issue or "invalid_pool")
                continue
            pools.append(finalized)
        pools.sort(key=lambda row: (row["protocol"], row["id"]))
        return pools, {
            "omitted_records": len(issues),
            "omission_reasons": sorted(set(issues)),
        }

    def _header(self) -> dict[str, Any]:
        try:
            result = self.rpc.batch([
                ("eth_chainId", []),
                ("eth_getBlockByNumber", ["latest", False]),
            ])
        except Exception as exc:
            raise PublicAPIUnavailable("current chain head is unavailable") from exc
        if not isinstance(result, Sequence) or len(result) != 2:
            raise PublicAPIUnavailable("current chain head response is malformed")
        chain_id = _integer(result[0])
        raw = result[1]
        if chain_id != CHAIN_ID:
            raise PublicAPIUnavailable("current RPC has the wrong chain identity")
        if not isinstance(raw, Mapping):
            raise PublicAPIUnavailable("current chain head response is malformed")
        number = _integer(raw.get("number"))
        block_hash = _hash(raw.get("hash"))
        timestamp = _integer(raw.get("timestamp"))
        if number is None or number < 0 or block_hash is None or timestamp is None:
            raise PublicAPIUnavailable("current chain head response is malformed")
        return {"number": number, "hash": block_hash, "timestamp": timestamp}

    def _confirm(self, header: Mapping[str, Any]) -> None:
        try:
            current = self.rpc.call(
                "eth_getBlockByNumber", [hex(int(header["number"])), False],
            )
        except Exception as exc:
            raise PublicAPIUnavailable("pinned block confirmation is unavailable") from exc
        observed = _hash(current.get("hash")) if isinstance(current, Mapping) else None
        if observed != header["hash"]:
            raise PublicAPIReorg("pinned block changed before publication; retry")

    def _direct_batch(
        self, calls: Sequence[tuple[str, list[Any]]],
    ) -> list[_CallResult]:
        output: list[_CallResult] = []
        for offset in range(0, len(calls), _DIRECT_BATCH_SIZE):
            chunk = calls[offset:offset + _DIRECT_BATCH_SIZE]
            try:
                values = self.rpc.batch(chunk)
            except Exception:
                output.extend(
                    _CallResult(False, None, "json_rpc_batch_unavailable", "unavailable")
                    for _ in chunk
                )
                continue
            if not isinstance(values, Sequence) or len(values) != len(chunk):
                output.extend(
                    _CallResult(False, None, "json_rpc_batch_malformed", "json_rpc_batch")
                    for _ in chunk
                )
                continue
            for value in values:
                if isinstance(value, str) and value.startswith("0x"):
                    try:
                        decoded = bytes.fromhex(value[2:])
                    except ValueError:
                        decoded = None
                    if decoded is not None:
                        output.append(_CallResult(True, decoded, None, "json_rpc_batch"))
                        continue
                output.append(
                    _CallResult(False, None, "contract_call_malformed", "json_rpc_batch")
                )
        return output

    def _state_calls(
        self, calls: Sequence[tuple[str, list[Any]]], block_tag: str,
    ) -> list[_CallResult]:
        if not calls:
            return []
        chunks = [
            calls[offset:offset + _MULTICALL_CHUNK_SIZE]
            for offset in range(0, len(calls), _MULTICALL_CHUNK_SIZE)
        ]
        specifications: list[tuple[str, list[Any]]] = []
        for chunk in chunks:
            packed = [
                (str(params[0]["to"]), str(params[0]["data"]))
                for method, params in chunk
                if method == "eth_call" and len(params) == 2 and params[1] == block_tag
            ]
            if len(packed) != len(chunk):
                raise PublicAPIUnavailable("internal state call plan is not block-pinned")
            specifications.append(("eth_call", [{
                "to": _mc.MULTICALL3,
                "data": _mc.encode(packed),
            }, block_tag]))

        def decode_chunk(chunk: Sequence[Any], raw: Any) -> Sequence[tuple[bool, bytes]] | None:
            try:
                decoded = _mc.decode(raw) if isinstance(raw, str) and raw.startswith("0x") else ()
            except Exception:
                return None
            return decoded if len(decoded) == len(chunk) else None

        output: list[_CallResult] = []
        for offset in range(0, len(chunks), _MULTICALL_RPC_BATCH_SIZE):
            group = chunks[offset:offset + _MULTICALL_RPC_BATCH_SIZE]
            rpc_group = specifications[offset:offset + _MULTICALL_RPC_BATCH_SIZE]
            try:
                if len(rpc_group) == 1:
                    raw_values: Sequence[Any] = [self.rpc.call(*rpc_group[0])]
                else:
                    candidate_values = self.rpc.batch(rpc_group)
                    if (
                        not isinstance(candidate_values, Sequence)
                        or isinstance(candidate_values, (str, bytes, bytearray))
                        or len(candidate_values) != len(group)
                    ):
                        raise ValueError("multicall RPC batch arity")
                    raw_values = candidate_values
            except Exception:
                raw_values = [None] * len(group)
            for chunk, specification, raw in zip(group, rpc_group, raw_values):
                decoded = decode_chunk(chunk, raw)
                if decoded is None and len(group) > 1:
                    try:
                        decoded = decode_chunk(chunk, self.rpc.call(*specification))
                    except Exception:
                        decoded = None
                if decoded is None:
                    output.extend(self._direct_batch(chunk))
                    continue
                for ok, value in decoded:
                    if ok:
                        output.append(_CallResult(True, bytes(value), None, "multicall3"))
                    else:
                        output.append(_CallResult(
                            False, None, "contract_call_reverted_or_unavailable", "multicall3",
                        ))
        return output

    @staticmethod
    def _fee(pool: Mapping[str, Any]) -> dict[str, Any]:
        configured = pool.get("configured_fee")
        if pool["protocol"] == "v4" and pool.get("dynamic_fee"):
            mode = "dynamic"
            configured_ppm = None
        elif configured is not None:
            mode = "static"
            configured_ppm = str(configured)
        else:
            mode = "unknown"
            configured_ppm = None
        return {
            "mode": mode,
            "configured_raw": str(configured) if configured is not None else None,
            "configured_ppm": configured_ppm,
            "current_ppm": None,
            "current_status": (
                "pending_block_read" if mode == "dynamic"
                else "configured_static" if mode == "static"
                else "unavailable"
            ),
        }

    @staticmethod
    def _public_pool(pool: Mapping[str, Any], token: str) -> dict[str, Any]:
        side = 0 if pool["token0"] == token else 1
        protocol = str(pool["protocol"])
        token0 = {
            "address": pool["token0"], "symbol": pool.get("symbol0"),
            "decimals": pool.get("decimals0"),
        }
        token1 = {
            "address": pool["token1"], "symbol": pool.get("symbol1"),
            "decimals": pool.get("decimals1"),
        }
        row = {
            "pool_id": pool["id"],
            "protocol": protocol,
            "pool_address": None if protocol == "v4" else pool["address"],
            "manager_address": pool["address"] if protocol == "v4" else None,
            "currency0": token0,
            "currency1": token1,
            "matched_currency": f"currency{side}",
            "fee": PublicMarketAPI._fee(pool),
            "tick_spacing": (
                str(pool["tick_spacing"]) if pool.get("tick_spacing") is not None else None
            ),
            "hooks": pool.get("hook") if protocol == "v4" else None,
            "pool_key": None,
            "catalog": {
                "source": pool.get("source"),
                "created_block": (
                    str(pool["created_block"]) if pool.get("created_block") is not None else None
                ),
                "observed_block": (
                    str(pool["observed_block"]) if pool.get("observed_block") is not None else None
                ),
                "observed_hash": pool.get("observed_hash"),
                "verification": (
                    "verified_workbench_catalog"
                    if pool.get("verified_catalog")
                    else "verified_canonical_store"
                ),
            },
            "liquidity": {
                "type": "v2_reserves" if protocol == "v2" else "concentrated_active_liquidity",
                "status": "unavailable",
                "reserve0_raw": None,
                "reserve1_raw": None,
                "active_liquidity_raw": None,
                "unit": (
                    "currency raw units" if protocol == "v2"
                    else "protocol raw active L; comparable only within this pool"
                ),
                "unavailable_reason": "state_read_not_attempted",
            },
            "availability": {"state": "unavailable", "reasons": []},
        }
        if protocol == "v4":
            row["pool_key"] = {
                "currency0": pool["token0"],
                "currency1": pool["token1"],
                "fee_raw": str(pool["configured_fee"]),
                "dynamic_fee": bool(pool["dynamic_fee"]),
                "tick_spacing": str(pool["tick_spacing"]),
                "hooks": pool["hook"],
            }
        return row

    def _read_state(
        self, pools: Sequence[Mapping[str, Any]], header: Mapping[str, Any], token: str,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        block_tag = hex(int(header["number"]))
        rows = [self._public_pool(pool, token) for pool in pools]
        calls: list[tuple[str, list[Any]]] = []
        plan: list[tuple[int, str]] = []
        for index, pool in enumerate(pools):
            protocol = pool["protocol"]
            if protocol == "v2":
                calls.append(("eth_call", [{
                    "to": pool["address"], "data": RESERVES_SELECTOR,
                }, block_tag]))
                plan.append((index, "reserves"))
            elif protocol == "v3":
                calls.append(("eth_call", [{
                    "to": pool["address"], "data": LIQUIDITY_SELECTOR,
                }, block_tag]))
                plan.append((index, "liquidity"))
            else:
                calls.append(("eth_call", [{
                    "to": STATE_VIEW,
                    "data": SV_LIQUIDITY_SELECTOR + str(pool["id"])[2:],
                }, block_tag]))
                plan.append((index, "liquidity"))
                if pool.get("dynamic_fee"):
                    calls.append(("eth_call", [{
                        "to": STATE_VIEW,
                        "data": SV_SLOT0_SELECTOR + str(pool["id"])[2:],
                    }, block_tag]))
                    plan.append((index, "dynamic_fee"))
        results = self._state_calls(calls, block_tag)
        transports = sorted({result.transport for result in results})
        for (index, kind), result in zip(plan, results):
            row = rows[index]
            if kind == "dynamic_fee":
                current_fee = _uint_word(result.data, 3, 24) if result.ok else None
                if current_fee is None or current_fee > 1_000_000:
                    row["fee"]["current_status"] = "unavailable"
                    row["availability"]["reasons"].append("current_dynamic_fee_unavailable")
                else:
                    row["fee"]["current_ppm"] = str(current_fee)
                    row["fee"]["current_status"] = "block_pinned"
                continue
            liquidity = row["liquidity"]
            if not result.ok:
                liquidity["unavailable_reason"] = result.reason
                row["availability"]["reasons"].append(
                    "reserves_unavailable" if kind == "reserves" else "active_liquidity_unavailable"
                )
                continue
            if kind == "reserves":
                reserve0 = _uint_word(result.data, 0, 112)
                reserve1 = _uint_word(result.data, 1, 112)
                if reserve0 is None or reserve1 is None:
                    liquidity["unavailable_reason"] = "malformed_v2_reserves"
                    row["availability"]["reasons"].append("reserves_unavailable")
                    continue
                liquidity.update({
                    "status": "available",
                    "reserve0_raw": str(reserve0),
                    "reserve1_raw": str(reserve1),
                    "unavailable_reason": None,
                })
            else:
                active = _uint_word(result.data, 0, 128)
                if active is None:
                    liquidity["unavailable_reason"] = "malformed_active_liquidity"
                    row["availability"]["reasons"].append("active_liquidity_unavailable")
                    continue
                liquidity.update({
                    "status": "available",
                    "active_liquidity_raw": str(active),
                    "unavailable_reason": None,
                })
        for row in rows:
            reasons = row["availability"]["reasons"]
            liquid = row["liquidity"]["status"] == "available"
            row["availability"]["state"] = (
                "available" if liquid and not reasons else "partial" if liquid else "unavailable"
            )
        return rows, transports

    def _catalog_coverage(
        self, returned: int, issues: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            catalog = self.market.catalog({"limit": 1, "offset": 0})
        except Exception:
            catalog = {}
        coverage = catalog.get("coverage") if isinstance(catalog, Mapping) else None
        coverage = coverage if isinstance(coverage, Mapping) else {}
        counts = catalog.get("counts") if isinstance(catalog, Mapping) else None
        counts = counts if isinstance(counts, Mapping) else {}
        tail = coverage.get("factory_tail")
        tail = tail if isinstance(tail, Mapping) else {}
        historical_gap = coverage.get("historical_gap")
        gap_count = len(historical_gap) if isinstance(historical_gap, list) else None
        universe_state = coverage.get("universe")
        omitted = int(issues.get("omitted_records") or 0)
        return {
            "scope": "verified known supported factories and V4 PoolManager catalog",
            "returned_matching_pools": returned,
            "known_catalog_pools": sum(
                parsed for value in counts.values()
                if (parsed := _integer(value)) is not None
            ) if counts else None,
            "known_catalog_state": universe_state or "unknown",
            "complete_for_known_catalog": universe_state == "known-factory-complete" and omitted == 0,
            "full_chain_complete": False,
            "historical_continuity": coverage.get("historical_continuity") or "unknown",
            "historical_gap_count": gap_count,
            "factory_tail": {
                "state": tail.get("state") or "unknown",
                "through_block": (
                    str(tail["through_block"]) if _integer(tail.get("through_block")) is not None else None
                ),
                "through_hash": _hash(tail.get("through_hash")),
                "head_lag_blocks": (
                    str(tail["head_lag_blocks"])
                    if _integer(tail.get("head_lag_blocks")) is not None else None
                ),
            },
            "omitted_records": omitted,
            "omission_reasons": list(issues.get("omission_reasons") or ()),
            "limitation": (
                "coverage is the verified known catalog, not a claim that every pool "
                "or factory deployed on-chain is indexed"
            ),
        }

    def _history_coverage(self) -> dict[str, Any]:
        try:
            status = self.lp_service.status()
            cursor_method = getattr(self.store, "cursor", None)
            cursor = cursor_method("live") if callable(cursor_method) else None
        finally:
            self._close_reader()
        status = status if isinstance(status, Mapping) else {}
        cursor = cursor if isinstance(cursor, Mapping) else {}
        intervals = status.get("coverage")
        intervals = intervals if isinstance(intervals, Mapping) else {}

        def interval(name: str) -> dict[str, Any] | None:
            raw = intervals.get(name)
            if not isinstance(raw, Mapping):
                return None
            return {
                "from_block": str(raw["from_block"]) if _integer(raw.get("from_block")) is not None else None,
                "to_block": str(raw["to_block"]) if _integer(raw.get("to_block")) is not None else None,
                "from_timestamp": str(raw["from"]) if _integer(raw.get("from")) is not None else None,
                "to_timestamp": str(raw["to"]) if _integer(raw.get("to")) is not None else None,
            }

        indexed_head = _integer(status.get("indexed_head"))
        lag = _integer(status.get("lag_blocks"))
        return {
            "state": status.get("state") or "unknown",
            "indexed_head": str(indexed_head) if indexed_head is not None else None,
            "indexed_head_hash": _hash(cursor.get("block_hash")),
            "lag_blocks": str(lag) if lag is not None else None,
            "backfill_complete": (
                not bool(status["backfill"]) if "backfill" in status else None
            ),
            "live_interval": interval("live"),
            "history_interval": interval("history"),
            "role": "event-history coverage; independent of the current block-pinned state read",
        }

    def _load_pools(self, token: str) -> dict[str, Any]:
        pools, issues = self._known_pools(token)
        header = self._header()
        rows, transports = self._read_state(pools, header, token)
        self._confirm(header)
        coverage = {
            "catalog": self._catalog_coverage(len(rows), issues),
            "history": self._history_coverage(),
            "state": {
                "requested_pools": len(rows),
                "available_pools": sum(
                    row["liquidity"]["status"] == "available" for row in rows
                ),
                "unavailable_pools": sum(
                    row["liquidity"]["status"] != "available" for row in rows
                ),
            },
        }
        return {
            "chain_id": CHAIN_ID,
            "token": token,
            "snapshot": {
                "block_number": str(header["number"]),
                "block_hash": header["hash"],
                "timestamp": str(header["timestamp"]),
                "canonical": True,
                "read_basis": "exact block number, then canonical hash confirmation",
                "transports": transports,
            },
            "pool_count": len(rows),
            "pools": rows,
            "coverage": coverage,
        }

    def pools(self, params: Mapping[str, Any]) -> dict[str, Any]:
        """Return every verified known pool containing ``params['token']``."""
        token = self._token_param(params)
        return self._cached(("pools", token), lambda: self._load_pools(token))

    @staticmethod
    def _asset_identity(rows: Sequence[Mapping[str, Any]], token: str) -> dict[str, Any]:
        symbols: set[str] = set()
        decimals: set[int] = set()
        for row in rows:
            currency = row[row["matched_currency"]]
            if currency.get("address") != token:
                continue
            if currency.get("symbol") is not None:
                symbols.add(str(currency["symbol"]))
            value = _integer(currency.get("decimals"))
            if value is not None:
                decimals.add(value)
        return {
            "address": token,
            "symbol": next(iter(symbols)) if len(symbols) == 1 else None,
            "decimals": next(iter(decimals)) if len(decimals) == 1 else None,
            "metadata_status": (
                "known" if len(symbols) == 1 and len(decimals) == 1
                else "conflicting" if len(symbols) > 1 or len(decimals) > 1
                else "partial"
            ),
        }

    @staticmethod
    def _group_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        fee = row["fee"]
        return (
            row["protocol"], fee["mode"], fee["configured_raw"],
            row.get("tick_spacing"), row.get("hooks"),
        )

    @staticmethod
    def _aggregate_group(
        token: str, token_meta: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        first = rows[0]
        fee = first["fee"]
        available = [row for row in rows if row["liquidity"]["status"] == "available"]
        missing = len(rows) - len(available)
        subtotals: list[dict[str, Any]] = []
        limitations: list[str] = []
        if first["protocol"] == "v2":
            measured_values: list[int] = []
            for row in available:
                side = 0 if row["matched_currency"] == "currency0" else 1
                raw = _integer(row["liquidity"].get(f"reserve{side}_raw"))
                if raw is not None:
                    measured_values.append(raw)
            if measured_values:
                raw_total = sum(measured_values)
                decimals = _integer(token_meta.get("decimals"))
                subtotals.append({
                    "metric": "matched_currency_reserve",
                    "currency": token,
                    "value_raw": str(raw_total),
                    "value_decimal": (
                        _format_units(raw_total, decimals) if decimals is not None else None
                    ),
                    "decimals": decimals,
                    "unit": "queried token units",
                    "pools_measured": len(measured_values),
                    "pools_missing": len(rows) - len(measured_values),
                    "coverage": (
                        "complete" if len(measured_values) == len(rows) else "partial"
                    ),
                    "basis": "sum of the queried currency side in block-pinned V2 reserves",
                })
            else:
                limitations.append("no_v2_reserve_measurements_available")
            limitations.append("counterpart_reserves_are_different_currencies_and_are_not_summed")
        else:
            limitations.append("raw_active_liquidity_is_pool_specific_and_not_additive")
            if first["protocol"] == "v4":
                limitations.append("PoolManager_balances_are_not_pool_level_holdings")
        configuration = {
            "protocol": first["protocol"],
            "fee": {
                "mode": fee["mode"],
                "configured_raw": fee["configured_raw"],
                "configured_ppm": fee["configured_ppm"],
            },
            "tick_spacing": first.get("tick_spacing"),
            "hooks": first.get("hooks"),
        }
        return {
            "configuration": configuration,
            "pool_count": len(rows),
            "pool_ids": [str(row["pool_id"]) for row in rows],
            "state_coverage": {
                "state": "complete" if not missing else "partial" if available else "unavailable",
                "measured_pools": len(available),
                "missing_pools": missing,
            },
            "subtotals": subtotals,
            "limitations": limitations,
        }

    def _load_assets(self, token: str, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        rows = list(snapshot["pools"])
        token_meta = self._asset_identity(rows, token)
        grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(self._group_key(row), []).append(row)
        groups = [
            self._aggregate_group(token, token_meta, grouped[key])
            for key in sorted(grouped, key=lambda value: tuple(str(item) for item in value))
        ]
        comparable = sum(len(group["subtotals"]) for group in groups)
        return {
            "chain_id": snapshot["chain_id"],
            "asset": token_meta,
            "snapshot": snapshot["snapshot"],
            "pool_count": len(rows),
            "configuration_count": len(groups),
            "groups": groups,
            "coverage": {
                **snapshot["coverage"],
                "aggregation": {
                    "configuration_groups": len(groups),
                    "groups_with_comparable_subtotals": sum(
                        bool(group["subtotals"]) for group in groups
                    ),
                    "missing_state_pools": sum(
                        row["liquidity"]["status"] != "available" for row in rows
                    ),
                },
            },
            "limitations": [
                "concentrated-liquidity raw L is never summed across pools",
                "counterpart currencies are never combined without a common measured value basis",
                "V4 PoolManager token balances are never attributed to an individual pool",
            ],
        }

    def assets(self, params: Mapping[str, Any]) -> dict[str, Any]:
        """Group known token pools by normalized configuration and safe subtotals."""
        token = self._token_param(params)
        snapshot = self.pools({"token": token})
        block_hash = snapshot["snapshot"]["block_hash"]
        return self._cached(
            ("assets", token, block_hash),
            lambda: self._load_assets(token, snapshot),
        )


__all__ = [
    "PublicAPIError", "PublicAPIReorg", "PublicAPIUnavailable", "PublicMarketAPI",
]
