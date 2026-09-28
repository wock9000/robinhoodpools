"""Candidate swap routes from the market ``pools`` table (read-only) plus hook policy.

V3 routers use only pools created by their own factory. Bridged routes keep
both hops in the same router when using a protocol-specific SwapRouter.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .tx_chain import GIGA_V3_FACTORY, NATIVE, PANCAKE_V3_FACTORY, PONS_HOOK, POOL_MANAGER, SLIPSTREAM_FACTORY, UR_V2_FACTORY, UR_V3_FACTORY, USDG, WETH, PoolKey, pons_launches

MAX_CANDIDATES = 6
QUOTE_CURRENCIES = frozenset({NATIVE, WETH, USDG})


class Venue(str, Enum):
    V2 = "v2"
    V3 = "v3"
    V4 = "v4"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class Pool:
    venue: Venue
    id: str
    address: str
    token0: str
    token1: str
    fee_ppm: int
    tick_spacing: int
    hook: str
    factory: str

    @property
    def key(self) -> PoolKey:
        if self.venue is not Venue.V4:
            raise ValueError("only V4 pools have a PoolKey")
        return PoolKey(self.token0, self.token1, self.fee_ppm, self.tick_spacing, self.hook)

    def other(self, currency: str) -> str:
        if currency == self.token0:
            return self.token1
        if currency == self.token1:
            return self.token0
        raise ValueError("currency is not in the pool")

    @property
    def swappable(self) -> bool:
        if self.venue is Venue.V2:
            return self.factory == UR_V2_FACTORY
        if self.venue is Venue.V3:
            return self.factory in (UR_V3_FACTORY, GIGA_V3_FACTORY, PANCAKE_V3_FACTORY, SLIPSTREAM_FACTORY)
        return self.hook in (NATIVE, PONS_HOOK)


@dataclass(frozen=True)
class HookPolicy:
    hook_fee_bps: int = 0
    creator_tax_bps: int = 0

    @property
    def is_pons(self) -> bool:
        return self.hook_fee_bps > 0 or self.creator_tax_bps > 0


NO_HOOK = HookPolicy()


@dataclass(frozen=True)
class Hop:
    pool: Pool
    currency_in: str
    currency_out: str


def same_asset(a: str, b: str) -> bool:
    return a == b or {a, b} == {NATIVE, WETH}


@dataclass(frozen=True)
class Route:
    hops: tuple[Hop, ...]

    def __post_init__(self) -> None:
        if not 1 <= len(self.hops) <= 2:
            raise ValueError("a route has one or two hops")
        for hop in self.hops:
            if hop.pool.other(hop.currency_in) != hop.currency_out:
                raise ValueError("hop currencies do not match its pool")
        for a, b in zip(self.hops, self.hops[1:]):
            if not same_asset(a.currency_out, b.currency_in):
                raise ValueError("route hops are not contiguous")

    @property
    def currency_in(self) -> str:
        return self.hops[0].currency_in

    @property
    def currency_out(self) -> str:
        return self.hops[-1].currency_out

    def describe(self) -> str:
        parts = [self.hops[0].currency_in]
        for hop in self.hops:
            parts.append(f"{hop.pool.venue.value}:{hop.pool.id[:10]}")
            parts.append(hop.currency_out)
        return " > ".join(parts)


class UnknownHook(RuntimeError):
    """The pool's hook is registered as Pons but has no launch policy."""


class IncompletePool(ValueError):
    def __init__(self, field: str) -> None:
        self.field = field
        super().__init__(f"V4 pool has no verified {field}")


def _pool_from_row(row: tuple[Any, ...], *, strict: bool = False) -> Pool | None:
    id_, protocol, address, token0, token1, fee_ppm, tick_spacing, hook, factory, metadata_json = row[:10]
    if protocol == "v4":
        if tick_spacing is None:
            if strict:
                raise IncompletePool("tick_spacing")
            return None
        metadata = json.loads(metadata_json) if metadata_json else {}
        configured_fee = metadata.get("configured_fee")
        if configured_fee is None:
            configured_fee = fee_ppm
        if configured_fee is None:
            if strict:
                raise IncompletePool("configured_fee")
            return None
        fee_ppm = configured_fee
    pool = Pool(
        venue=Venue(protocol),
        id=id_.lower(),
        address=(address or "").lower(),
        token0=token0.lower(),
        token1=token1.lower(),
        fee_ppm=int(fee_ppm or 0),
        tick_spacing=int(tick_spacing or 0),
        hook=(hook or NATIVE).lower(),
        factory=(factory or "").lower(),
    )
    if pool.venue is Venue.V4 and pool.key.id() != pool.id:
        if strict:
            raise IncompletePool("PoolKey identity")
        return None
    return pool


def v4_pool(key: PoolKey) -> Pool:
    return Pool(Venue.V4, key.id(), POOL_MANAGER, key.currency0, key.currency1, key.fee, key.tick_spacing, key.hooks, POOL_MANAGER)


DEFAULT_BRIDGES: tuple[Pool, ...] = (
    Pool(Venue.V3, "0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca", "0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca", WETH, USDG, 100, 1, NATIVE, UR_V3_FACTORY),
    Pool(Venue.V3, "0xb2a6ad51b3ea3cdc8d3508cca147a43471382e53", "0xb2a6ad51b3ea3cdc8d3508cca147a43471382e53", WETH, USDG, 100, 1, NATIVE, GIGA_V3_FACTORY),
    Pool(Venue.V3, "0x88a8e96e7785d378825e8b5d7fc0e6f62487061e", "0x88a8e96e7785d378825e8b5d7fc0e6f62487061e", WETH, USDG, 500, 10, NATIVE, PANCAKE_V3_FACTORY),
    Pool(Venue.V3, "0x16679e2ac1a798865ecf1c1639e67693ddb1c220", "0x16679e2ac1a798865ecf1c1639e67693ddb1c220", WETH, USDG, 89, 10, NATIVE, SLIPSTREAM_FACTORY),
    Pool(Venue.V2, "0x8803c117ccae7b5146297876c2a25df135141c4d", "0x8803c117ccae7b5146297876c2a25df135141c4d", WETH, USDG, 0, 0, NATIVE, UR_V2_FACTORY),
    v4_pool(PoolKey(NATIVE, USDG, 500, 10, NATIVE)),
)

_POOL_COLUMNS = "p.id, p.protocol, p.address, p.token0, p.token1, p.fee_ppm, p.tick_spacing, p.hook, p.factory, p.metadata_json"
_CANDIDATE_SQL = (
    f"SELECT {_POOL_COLUMNS}, s.block_number FROM pools p "
    "LEFT JOIN lp_pool_state s ON s.pool_id = p.id "
    "WHERE p.token0 = ? OR p.token1 = ?"
)
_BY_ID_SQL = f"SELECT {_POOL_COLUMNS} FROM pools p WHERE p.id = ?"
_BY_PAIR_SQL = f"SELECT {_POOL_COLUMNS} FROM pools p WHERE p.protocol = ? AND p.token0 = ? AND p.token1 = ?"


class RouteBook:
    def __init__(
        self,
        reader: Callable[[], AbstractContextManager[sqlite3.Connection]],
        rpc: Any,
        *,
        bridges: tuple[Pool, ...] = DEFAULT_BRIDGES,
    ) -> None:
        self._reader = reader
        self._rpc = rpc
        self._bridges = bridges
        self._hooks: dict[str, HookPolicy] = {}
        self._lock = threading.Lock()

    def pool(self, pool_id: str) -> Pool | None:
        with self._reader() as connection:
            row = connection.execute(_BY_ID_SQL, (pool_id.lower(),)).fetchone()
        return _pool_from_row(row, strict=True) if row else None

    def position_candidates(self, wallet: str, pool_id: str, limit: int = 50) -> list[int]:
        """Token ids the accounting index attributes to wallet in pool; the chain is the authority."""
        with self._reader() as connection:
            try:
                rows = connection.execute(
                    "SELECT token_id FROM lp_accounting_positions WHERE owner=? AND pool_id=? AND token_id IS NOT NULL LIMIT ?",
                    (wallet.lower(), pool_id.lower(), limit),
                ).fetchall()
            except sqlite3.OperationalError:
                return []
        return sorted({int(row[0]) for row in rows if str(row[0]).isdigit()})

    def pool_for(self, venue: Venue, token_a: str, token_b: str, fee_ppm: int | None = None, factory: str | None = None, tick_spacing: int | None = None) -> Pool | None:
        """``factory`` narrows to one deployer; without it only supported pools match."""
        token0, token1 = sorted((token_a.lower(), token_b.lower()))

        def accepts(pool: Pool) -> bool:
            if fee_ppm is not None and pool.fee_ppm != fee_ppm:
                return False
            if tick_spacing is not None and pool.tick_spacing != tick_spacing:
                return False
            return pool.factory == factory if factory is not None else pool.swappable

        for bridge in self._bridges:
            if bridge.venue is venue and (bridge.token0, bridge.token1) == (token0, token1) and accepts(bridge):
                return bridge
        with self._reader() as connection:
            rows = connection.execute(_BY_PAIR_SQL, (venue.value, token0, token1)).fetchall()
        for row in rows:
            pool = _pool_from_row(row)
            if pool is not None and accepts(pool):
                return pool
        return None

    def hook_policy(self, pool: Pool) -> HookPolicy:
        if pool.venue is not Venue.V4 or pool.hook != PONS_HOOK:
            return NO_HOOK
        with self._lock:
            cached = self._hooks.get(pool.id)
        if cached is not None:
            return cached
        raw = self._rpc.call("eth_call", [{"to": PONS_HOOK, "data": "0x" + pons_launches(pool.id).hex()}, "latest"])
        words = [int(raw[2 + i:2 + i + 64], 16) for i in range(0, len(raw) - 2, 64)]
        if len(words) < 11 or words[0] == 0:
            raise UnknownHook(pool.id)
        policy = HookPolicy(hook_fee_bps=words[10], creator_tax_bps=words[7])
        with self._lock:
            self._hooks[pool.id] = policy
        return policy

    def token_pools(self, token: str) -> list[Pool]:
        with self._reader() as connection:
            rows = connection.execute(_CANDIDATE_SQL, (token, token)).fetchall()
        rows.sort(key=lambda row: -(row[10] or 0))
        return [pool for pool in map(_pool_from_row, rows) if pool is not None]

    def candidates(self, token: str, quote_currency: str, side: Side) -> list[Route]:
        token = token.lower()
        if quote_currency not in QUOTE_CURRENCIES or same_asset(token, quote_currency):
            return []
        direct: list[Route] = []
        bridged: list[Route] = []
        for pool in self.token_pools(token):
            if not pool.swappable:
                continue
            other = pool.other(token)
            if pool.hook == PONS_HOOK:
                try:
                    self.hook_policy(pool)
                except UnknownHook:
                    continue
            token_hop = Hop(pool, other, token) if side is Side.BUY else Hop(pool, token, other)
            if same_asset(other, quote_currency):
                direct.append(Route((token_hop,)))
                continue
            if other not in QUOTE_CURRENCIES:
                continue
            for bridge in self._bridges:
                ends = {bridge.token0, bridge.token1}
                near = next((c for c in ends if same_asset(c, other)), None)
                far = next((c for c in ends if same_asset(c, quote_currency)), None)
                if near is None or far is None or near == far:
                    continue
                if pool.factory in (GIGA_V3_FACTORY, PANCAKE_V3_FACTORY, SLIPSTREAM_FACTORY) and (
                    bridge.venue is not Venue.V3 or bridge.factory != pool.factory
                ):
                    continue
                if bridge.factory in (GIGA_V3_FACTORY, PANCAKE_V3_FACTORY, SLIPSTREAM_FACTORY) and pool.factory != bridge.factory:
                    continue
                bridge_hop = Hop(bridge, far, near) if side is Side.BUY else Hop(bridge, near, far)
                hops = (bridge_hop, token_hop) if side is Side.BUY else (token_hop, bridge_hop)
                bridged.append(Route(hops))
        return (direct + bridged)[:MAX_CANDIDATES]


__all__ = [
    "DEFAULT_BRIDGES", "Hop", "HookPolicy", "MAX_CANDIDATES", "NO_HOOK", "Pool", "QUOTE_CURRENCIES",
    "Route", "RouteBook", "Side", "UnknownHook", "IncompletePool", "Venue", "same_asset", "v4_pool",
]
