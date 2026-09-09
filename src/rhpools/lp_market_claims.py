"""Bounded wallet evidence recovery and block-pinned outstanding LP claims."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .lp_chain import STATE_VIEW
from .lp_math import fee_claim, fee_growth_amount
from .lp_market_protocols import (
    LIQUIDITY_SELECTOR, NFT_POSITIONS_SELECTOR, POSITIONS_SELECTOR, SLOT0_SELECTOR,
    STATE_VIEW_LIQUIDITY_SELECTOR, STATE_VIEW_POSITION_SELECTOR, STATE_VIEW_SLOT0_SELECTOR,
    V3_NFT_MANAGER_ADDRESSES, V4_POSITION_MANAGER,
    _decode_position_result, _selector, _sint, _uint, _v4_position_key,
    _word_address, _words, nft_position_key,
)
from .lp_market_store import CanonicalConflict
from .workbench_market import _price_from_sqrt

_FEE_GLOBAL0 = _selector("feeGrowthGlobal0X128()")
_FEE_GLOBAL1 = _selector("feeGrowthGlobal1X128()")
_TICKS = _selector("ticks(int24)")
_FEE_INSIDE = _selector("getFeeGrowthInside(bytes32,int24,int24)")
_OWNER_OF = _selector("ownerOf(uint256)")


def _word(value: int) -> str:
    return (value % (1 << 256)).to_bytes(32, "big").hex()


def fetch_position_claims(
    store, book, prices, client, positions: Sequence[Mapping[str, Any]], *,
    admit: Callable[[int], None] | None = None,
):
    """Read principal and outstanding claims at one canonical end-of-block pin."""
    if not positions:
        return []
    with book._reader() as conn:
        cursor = store._metadata(conn, "cursor:live", {})
        if not cursor:
            return []
        number = int(cursor["block_number"])
        header = conn.execute(
            "SELECT hash,timestamp FROM blocks WHERE number=?", (number,),
        ).fetchone()
        if header is None:
            return []
        block_hash, timestamp = str(header["hash"]), int(header["timestamp"])
        epoch = int(store._metadata(conn, "epoch", 0))
        pools = {}
        for position in positions:
            pool_id = position.get("pool_id")
            if pool_id and pool_id not in pools:
                pool = conn.execute("SELECT * FROM pools WHERE id=?", (pool_id,)).fetchone()
                if pool is not None:
                    pools[pool_id] = dict(pool)
    calls = []
    call_indexes = {}

    def request(target, data):
        key = (target, data)
        if key not in call_indexes:
            call_indexes[key] = len(calls)
            calls.append(("eth_call", [{"to": target, "data": data}, hex(number)]))
        return call_indexes[key]

    planned = []
    for position in positions:
        pool = pools.get(position.get("pool_id"))
        if pool is None or not position.get("liquidity_known"):
            continue
        if position.get("tick_lower") is None or position.get("tick_upper") is None:
            continue
        if int(position["last_block"]) > number:
            continue
        protocol = position.get("protocol")
        lower, upper = int(position["tick_lower"]), int(position["tick_upper"])
        if lower >= upper:
            continue
        key = str(position["position_key"])
        manager, token_id = position.get("custody"), position.get("token_id")
        indices = {}
        if key.startswith("nft:"):
            if token_id is None or key != nft_position_key(manager, token_id):
                continue
            if manager not in V3_NFT_MANAGER_ADDRESSES and manager != V4_POSITION_MANAGER:
                continue
            indices["owner"] = request(manager, _OWNER_OF + _word(int(token_id)))
        if protocol == "v3":
            if key.startswith("nft:"):
                if manager not in V3_NFT_MANAGER_ADDRESSES:
                    continue
                decoder = "v3_nfpm_position"
                indices["position"] = request(manager, NFT_POSITIONS_SELECTOR + _word(int(token_id)))
            else:
                prefix = f"v3:{pool['id']}:"
                if not key.startswith(prefix):
                    continue
                key = key[len(prefix):]
                if len(key) != 66 or not key.startswith("0x"):
                    continue
                decoder = "v3_core_position"
                indices["position"] = request(pool["id"], POSITIONS_SELECTOR + key[2:])
            indices["slot"] = request(pool["id"], SLOT0_SELECTOR)
            indices["active_liquidity"] = request(pool["id"], LIQUIDITY_SELECTOR)
            indices["global0"] = request(pool["id"], _FEE_GLOBAL0)
            indices["global1"] = request(pool["id"], _FEE_GLOBAL1)
            indices["lower"] = request(pool["id"], _TICKS + _word(lower))
            indices["upper"] = request(pool["id"], _TICKS + _word(upper))
        elif protocol == "v4":
            if key.startswith("nft:"):
                if manager != V4_POSITION_MANAGER:
                    continue
                key = _v4_position_key(manager, lower, upper, "0x" + _word(int(token_id)))
            else:
                prefix = f"v4:{pool['id']}:"
                if not key.startswith(prefix):
                    continue
                key = key[len(prefix):]
            if len(key) != 66 or not key.startswith("0x"):
                continue
            decoder = "v4_state_view_position"
            indices["position"] = request(
                STATE_VIEW, STATE_VIEW_POSITION_SELECTOR + pool["id"][2:] + key[2:],
            )
            indices["slot"] = request(STATE_VIEW, STATE_VIEW_SLOT0_SELECTOR + pool["id"][2:])
            indices["active_liquidity"] = request(
                STATE_VIEW, STATE_VIEW_LIQUIDITY_SELECTOR + pool["id"][2:],
            )
            indices["inside"] = request(
                STATE_VIEW, _FEE_INSIDE + pool["id"][2:] + _word(lower) + _word(upper),
            )
        else:
            continue
        planned.append((position, pool, decoder, indices))
    if not planned:
        return []
    results = []
    for offset in range(0, len(calls), 100):
        if admit is not None:
            admit(min(100, len(calls) - offset))
        results.extend(client.batch(calls[offset:offset + 100], allow_reverts=True))
    if len(results) != len(calls):
        raise ValueError("claim request/result count mismatch")
    verified = client.call("eth_getBlockByNumber", [hex(number), False])
    if not isinstance(verified, Mapping) or str(verified.get("hash", "")).lower() != block_hash:
        raise CanonicalConflict("claim snapshot block is no longer canonical")
    output = []
    for position, pool, decoder, indices in planned:
        # A reverted/burned NFT cannot prevent healthy siblings from refreshing.
        if any(isinstance(results[index], Mapping) for index in indices.values()):
            continue
        state = _decode_position_result(decoder, results[indices["position"]])
        liquidity = int(state["liquidity"])
        if liquidity != int(position["liquidity"]):
            continue
        if "owner" in indices:
            owner = _word_address(_words(results[indices["owner"]], "claim owner", 1)[0], "claim owner")
            if owner != position.get("owner"):
                continue
        if decoder == "v3_nfpm_position" and any(
            state[field] != expected for field, expected in (
                ("tick_lower", int(position["tick_lower"])),
                ("tick_upper", int(position["tick_upper"])),
                ("token0", pool["token0"]), ("token1", pool["token1"]),
                ("fee_ppm", pool["fee_ppm"]),
            )
        ):
            continue
        slot = _words(results[indices["slot"]], "claim pool slot")
        if len(slot) < 2:
            raise ValueError("claim pool slot is incomplete")
        sqrt = _uint(slot[0], 160, "claim pool sqrt price")
        tick = _sint(slot[1], 24, "claim pool tick")
        if sqrt <= 0:
            continue
        last0 = int(state["fee_growth_inside0_last_x128"])
        last1 = int(state["fee_growth_inside1_last_x128"])
        if position["protocol"] == "v3":
            global0 = _words(results[indices["global0"]], "claim fee growth0", 1)[0]
            global1 = _words(results[indices["global1"]], "claim fee growth1", 1)[0]
            lower = _words(results[indices["lower"]], "claim lower tick")
            upper = _words(results[indices["upper"]], "claim upper tick")
            if len(lower) < 4 or len(upper) < 4:
                raise ValueError("claim tick fee state is incomplete")
            claim0, claim1, _lazy0, _lazy1 = fee_claim(
                liquidity, last0, last1, int(state["tokens_owed0"]), int(state["tokens_owed1"]),
                global0, global1, lower[2], lower[3], upper[2], upper[3], tick,
                int(position["tick_lower"]), int(position["tick_upper"]),
            )
        else:
            inside = _words(results[indices["inside"]], "V4 claim fee growth", 2)
            claim0 = fee_growth_amount(liquidity, inside[0], last0)
            claim1 = fee_growth_amount(liquidity, inside[1], last1)
        active_liquidity = _uint(
            _words(results[indices["active_liquidity"]], "claim pool liquidity", 1)[0],
            128, "claim pool liquidity",
        )
        ratio = (
            _price_from_sqrt(sqrt, pool["decimals0"], pool["decimals1"])
            if active_liquidity > 0 else None
        )
        mark = {"block_number": number, "tx_index": 2**31 - 1,
                "log_index": 2**31 - 1, "timestamp": timestamp}
        with book._reader() as conn:
            price0, price1, _basis = prices._quote(conn, pool, mark, ratio)
        output.append({
            "position_key": position["position_key"], "epoch": epoch,
            "block_number": number, "block_hash": block_hash, "timestamp": timestamp,
            "position_last_block": int(position["last_block"]),
            "position_last_tx_index": int(position["last_tx_index"]),
            "position_last_log_index": int(position["last_log_index"]),
            "position_pending_principal0": position["pending_principal0"],
            "position_pending_principal1": position["pending_principal1"],
            "position_pending_known": int(position["pending_known"]),
            "position_state_json": position["state_json"],
            "liquidity": str(liquidity), "sqrt_price_x96": str(sqrt), "tick": tick,
            "price0_usd": price0, "price1_usd": price1,
            "claim0": str(claim0), "claim1": str(claim1),
        })
    return output


class PositionClaims:
    """Refresh visible wallets without synchronous RPC or request-side writes."""

    def __init__(self, store, book, prices, rpc: Callable[[], Any]):
        self.store, self.book, self.prices, self.rpc = store, book, prices, rpc
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._owners: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._event_cursors: OrderedDict[str, int] = OrderedDict()
        self._thread: threading.Thread | None = None
        self._rpc_next = 0.0
        self._status = {"refreshed_positions": 0, "last_success": None, "last_error": None}

    def request(self, owners: Sequence[str], *, priority: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            for owner in owners:
                if not isinstance(owner, str) or len(owner) != 42 or not owner.startswith("0x"):
                    continue
                owner = owner.lower()
                if owner not in self._owners:
                    self._owners[owner] = {"expires": now + 180, "due": 0.0,
                                           "positions": "", "episodes": ""}
                else:
                    self._owners[owner]["expires"] = now + 180
                if priority:
                    self._owners.move_to_end(owner, last=False)
            while len(self._owners) > 512:
                self._owners.popitem(last=priority)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="lp-market-claims", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {**self._status, "requested_wallets": len(self._owners)}

    def _next(self):
        now = time.monotonic()
        with self._lock:
            for owner, state in tuple(self._owners.items()):
                if state["expires"] <= now:
                    del self._owners[owner]
                elif state["due"] <= now:
                    state["due"] = now + 5.0
                    self._owners.move_to_end(owner)
                    return owner, state
        return None

    def _admit(self, items: int) -> None:
        # Current claims borrow at most 8 of the shared 80 state items/second.
        if self._stop.wait(max(0.0, self._rpc_next - time.monotonic())):
            raise RuntimeError("claim refresh stopped")
        self._rpc_next = time.monotonic() + items / 8.0

    def _refresh(self, owner, state):
        with self.book._reader() as conn:
            positions = [dict(row) for row in conn.execute(
                "SELECT p.*,c.timestamp AS claim_timestamp,c.epoch AS claim_epoch "
                "FROM lp_accounting_positions p "
                "INDEXED BY lp_accounting_positions_owner_active_key "
                "LEFT JOIN lp_accounting_claims c ON c.position_key=p.position_key "
                "AND c.position_state_json=p.state_json "
                "WHERE p.owner=? AND p.active_episode_id IS NOT NULL AND p.position_key>? "
                "ORDER BY p.position_key LIMIT 4", (owner, state["positions"]),
            ).fetchall()]
            state["positions"] = positions[-1]["position_key"] if len(positions) == 4 else ""
            historical = [str(row[0]) for row in conn.execute(
                "SELECT DISTINCT position_key FROM lp_accounting_episodes "
                "INDEXED BY lp_accounting_episodes_owner_position "
                "WHERE owner=? AND position_key>? ORDER BY position_key LIMIT 4",
                (owner, state["episodes"]),
            ).fetchall()]
            state["episodes"] = historical[-1] if len(historical) == 4 else ""
            keys = set(historical) | {row["position_key"] for row in positions}
            pending = set()
            tx_hashes = set()
            for key in keys:
                if conn.execute(
                    "SELECT 1 FROM lp_accounting_pending WHERE position_key=?", (key,),
                ).fetchone() is not None:
                    pending.add(key)
                cursor = self._event_cursors.get(key, 0)
                page = conn.execute(
                    "SELECT event_id FROM lp_accounting_event_keys "
                    "WHERE position_key=? AND event_id>? ORDER BY event_id LIMIT 64",
                    (key, cursor),
                ).fetchall()
                self._event_cursors[key] = int(page[-1][0]) if len(page) == 64 else 0
                self._event_cursors.move_to_end(key)
                if page:
                    marks = ",".join("?" for _ in page)
                    tx_hashes.update(str(row[0]) for row in conn.execute(
                        "SELECT DISTINCT p.tx_hash FROM events e "
                        "JOIN pending_enrichment p ON p.tx_hash=e.tx_hash "
                        f"WHERE e.id IN ({marks})", tuple(int(row[0]) for row in page),
                    ))
            epoch = int(self.store._metadata(conn, "epoch", 0))
        while len(self._event_cursors) > 2048:
            self._event_cursors.popitem(last=False)
        self.store.prioritize_enrichment(tx_hashes)
        self.book.prioritize_positions(keys)
        positions = [row for row in positions
                     if row["position_key"] not in pending and row["liquidity_known"]
                     and (row["claim_epoch"] != epoch or row["claim_timestamp"] is None
                          or time.time() - row["claim_timestamp"] >= 10)]
        if not positions or self._stop.is_set():
            return
        claims = fetch_position_claims(
            self.store, self.book, self.prices, self.rpc(), positions, admit=self._admit,
        )
        if self._stop.is_set():
            return
        if claims and self.book.publish_claims(claims):
            with self._lock:
                self._status.update(
                    last_success=time.time(),
                    last_error=(
                        None if len(claims) == len(positions)
                        else "some positions await matching pinned claim evidence"
                    ),
                )
                self._status["refreshed_positions"] += len(claims)

    def _run(self):
        try:
            while not self._stop.is_set():
                item = self._next()
                if item is None:
                    self._stop.wait(0.25)
                    continue
                try:
                    self._refresh(*item)
                except Exception as exc:
                    with self._lock:
                        self._status["last_error"] = str(exc)
        finally:
            self.store.close_reader()
