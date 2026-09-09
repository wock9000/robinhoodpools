"""USDG-quoted, materialized LP market read model. No RPC in request handlers.

Pool trading fees are estimates from swap input and the observed fee, not an
LP's realized earnings. Complete position cashflows belong to AccountBook.
"""
from __future__ import annotations

import json
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from .lp_chain import NATIVE, UNISWAP_V3_FACTORY, USDG, WETH, WETH_USDG_POOL
from .lp_market_store import _insert_rows
from .workbench_market import _price_from_sqrt

WINDOWS = {"1h": 3600, "24h": 86400, "7d": 604800, "30d": 2592000, "all": None}
PRICING_BASIS = "USDG quote (1 USDG = 1 quote dollar); not a fiat oracle"
PRICE_PROJECTION_VERSION = 1
BUCKET_FIELDS = (
    "events", "swaps", "adds", "removes", "collects", "volume_usd", "fees_usd",
    "deposit_usd", "withdrawal_usd", "priced_swaps", "priced_fees", "priced_flows", "flows",
)
_SUM_FIELDS = ",".join(f"SUM({name}) AS {name}" for name in BUCKET_FIELDS)

_MISSING = object()


def _batches(values, size=500):
    for start in range(0, len(values), size):
        yield values[start:start + size]



def _data(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _units(raw: Any, decimals: Any) -> float | None:
    if raw is None or decimals is None:
        return None
    try:
        return _number(int(raw) / 10 ** int(decimals))
    except (ValueError, TypeError, OverflowError, ZeroDivisionError):
        return None


def _value(raw: Any, decimals: Any, price: float | None) -> float | None:
    if raw is not None and int(raw) == 0:
        return 0.0
    amount = _units(raw, decimals)
    return _number(amount * price) if amount is not None and price is not None else None


def _basket(raw0, raw1, pool, price0, price1):
    value0 = _value(raw0, pool["decimals0"], price0)
    value1 = _value(raw1, pool["decimals1"], price1)
    return value0 + value1 if value0 is not None and value1 is not None else None


def _static_pool_fee(pool):
    try:
        fee = int(pool.get("fee_ppm")) if pool.get("fee_ppm") is not None else None
    except (TypeError, ValueError):
        return None, "unavailable"
    if fee is None:
        return None, "dynamic_or_unavailable"
    if str(pool.get("protocol") or "").lower() == "v4" and fee & 0x800000:
        return None, "dynamic_fee_flag_not_a_rate"
    if not 0 <= fee <= 1_000_000:
        return None, "invalid_or_unavailable"
    return fee, "verified_static_pool_fee"


def _swap_inputs(event, pool, extra):
    raw0, raw1 = int(event.get("amount0") or 0), int(event.get("amount1") or 0)
    protocol = str(event.get("protocol") or pool.get("protocol") or "").lower()
    if protocol == "v4":
        input0, input1 = max(0, -raw0), max(0, -raw1)
    else:
        input0, input1 = max(0, raw0), max(0, raw1)
    if "input0" in extra and "input1" in extra:
        input0, input1 = int(extra["input0"]), int(extra["input1"])
    return input0, input1


def _value_fees(event, pool, price0, price1, extra):
    """Value only attributable event fees; transaction settlement is never a fee."""
    event["fees_usd"] = None
    kind = str(event.get("kind") or "")
    protocol = str(event.get("protocol") or pool.get("protocol") or "").lower()
    if kind == "swap":
        input0, input1 = _swap_inputs(event, pool, extra)
        fee = event.get("fee_ppm")
        if fee is None and protocol != "v4":
            fee = pool.get("fee_ppm")
        try:
            fee = int(fee) if fee is not None else None
        except (TypeError, ValueError):
            fee = None
        if fee is not None and 0 <= fee <= 1_000_000:
            input_value = _basket(input0, input1, pool, price0, price1)
            if input_value is not None:
                event["fees_usd"] = input_value * fee / 1_000_000
                extra["fees_scope"] = "gross_swap"
                extra["fees_qualification"] = (
                    "observed_input_delta_x_event_fee"
                    if event.get("fee_ppm") is not None
                    else "observed_input_delta_x_verified_pool_fee"
                )
                extra["fees_basis"] = (
                    "gross swap-input fee estimate; protocol and hook cuts not deducted"
                )
        elif fee is None:
            extra["fees_qualification"] = "swap_fee_unavailable"
        else:
            extra["fees_qualification"] = "swap_fee_out_of_range"

    fee0, fee1 = event.get("fee_amount0"), event.get("fee_amount1")
    exact_position = (
        fee0 is not None and fee1 is not None
        and event.get("position_key") is not None
        and extra.get("fees_accrued_exact") is True
    )
    exact_pool_event = (
        fee0 is not None and fee1 is not None
        and (
            kind == "donate"
            or event.get("accounting_basis") == "flash_paid_exact_fee_and_overpayment"
        )
    )
    if exact_position or exact_pool_event:
        event["fees_usd"] = _basket(fee0, fee1, pool, price0, price1)
        if exact_position:
            extra["fees_scope"] = "position_event"
            extra["fees_qualification"] = (
                "observed_signed_position_fee_delta"
            )
            extra.setdefault(
                "fees_basis",
                "exact position fee delta; V4 value may include donated fees",
            )
        else:
            extra["fees_scope"] = "pool_event_unallocated"
            extra["fees_qualification"] = "observed_signed_pool_fee_amounts"
            extra.setdefault("fees_basis", "exact pool fee event; not position earnings")
    elif kind in {"add", "remove", "collect"}:
        extra.setdefault(
            "fees_qualification",
            "unavailable_without_position_fee_attribution",
        )

    for key in ("fees_scope", "fees_qualification", "fees_basis"):
        if extra.get(key) is not None:
            event[key] = extra[key]
    return event["fees_usd"]


def _order(event):
    return (int(event["block_number"]), int(event["tx_index"]), int(event["log_index"]))


class PriceProjection:
    """Block-ordered marks and minute/hour buckets, committed with canonical logs."""

    def __init__(self, store):
        self.store = store
        fields = ",".join(f"{name} REAL NOT NULL DEFAULT 0" for name in BUCKET_FIELDS)
        schema = f"""
        CREATE TABLE IF NOT EXISTS lp_pool_state (
            pool_id TEXT PRIMARY KEY, block_number INTEGER NOT NULL, tx_index INTEGER NOT NULL,
            log_index INTEGER NOT NULL, timestamp INTEGER NOT NULL, sqrt_price_x96 TEXT,
            tick INTEGER, liquidity TEXT, price0_usd REAL, price1_usd REAL, price REAL,
            fee_ppm INTEGER, pricing_basis TEXT
        );
        CREATE TABLE IF NOT EXISTS lp_price_samples (
            event_id INTEGER PRIMARY KEY, pool_id TEXT NOT NULL, block_number INTEGER NOT NULL,
            tx_index INTEGER NOT NULL, log_index INTEGER NOT NULL, timestamp INTEGER NOT NULL,
            sqrt_price_x96 TEXT NOT NULL, price REAL, price0_usd REAL, price1_usd REAL
        );
        CREATE INDEX IF NOT EXISTS lp_samples_order ON lp_price_samples
            (pool_id,block_number,tx_index,log_index);
        CREATE TABLE IF NOT EXISTS lp_v2_reserve_samples (
            event_id INTEGER PRIMARY KEY,pool_id TEXT NOT NULL,block_number INTEGER NOT NULL,
            tx_index INTEGER NOT NULL,log_index INTEGER NOT NULL,timestamp INTEGER NOT NULL,
            reserve0 TEXT NOT NULL,reserve1 TEXT NOT NULL,price REAL
        );
        CREATE INDEX IF NOT EXISTS lp_v2_samples_order ON lp_v2_reserve_samples
            (pool_id,block_number,tx_index,log_index);
        CREATE INDEX IF NOT EXISTS lp_state_order ON lp_pool_state(block_number,tx_index,log_index);
        CREATE TABLE IF NOT EXISTS lp_price_marks (
            token TEXT NOT NULL,event_id INTEGER NOT NULL,pool_id TEXT NOT NULL,
            block_number INTEGER NOT NULL,tx_index INTEGER NOT NULL,log_index INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,price_usd REAL NOT NULL,basis TEXT NOT NULL,
            PRIMARY KEY(token,event_id)
        );
        CREATE INDEX IF NOT EXISTS lp_marks_order ON lp_price_marks
            (token,block_number,tx_index,log_index);
        CREATE TABLE IF NOT EXISTS lp_pool_buckets (
            resolution INTEGER NOT NULL,bucket INTEGER NOT NULL,pool_id TEXT NOT NULL,
            {fields},max_block INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(resolution,bucket,pool_id)
        );
        CREATE INDEX IF NOT EXISTS lp_buckets_pool ON lp_pool_buckets(pool_id,resolution,bucket);
        CREATE INDEX IF NOT EXISTS lp_events_time ON events(timestamp,block_number,tx_index,log_index);
        CREATE INDEX IF NOT EXISTS lp_events_pool_order ON events(pool_id,block_number,tx_index,log_index);
        """
        with store.transaction() as conn:
            for statement in schema.split(";"):
                if statement.strip():
                    conn.execute(statement)
            if int(store._metadata(conn, "price_projection_version", 0)) < PRICE_PROJECTION_VERSION:
                zero_depth = (
                    "p.protocol IN ('v3','v4') AND (p.token0=? OR p.token1=?) AND "
                    "CAST(COALESCE(e.liquidity,"
                    "json_extract(e.data,'$.pool_state_before.liquidity'),0) AS INTEGER)<=0"
                )
                queued = conn.execute(
                    "INSERT OR IGNORE INTO pending_reprojection"
                    "(event_id,block_number,tx_index,log_index) "
                    "SELECT e.id,e.block_number,e.tx_index,e.log_index "
                    "FROM events e JOIN pools p ON p.id=e.pool_id "
                    f"WHERE {zero_depth}",
                    (USDG, USDG),
                ).rowcount
                if queued:
                    store._bump(conn, "pending_reprojection", queued)
                store._set_metadata(
                    conn, "price_projection_version", PRICE_PROJECTION_VERSION,
                )
        store.register_projection(
            self.apply, self.rollback, persists_events=True,
        )

    @staticmethod
    def _anchor(conn, token, event, heads=None):
        if token == USDG:
            return 1.0
        if token == NATIVE:
            token = WETH
        if heads is not None and token in heads:
            head = heads[token]
            if head is None or _order(event) > _order(head):
                if head is not None and (
                    0 <= int(event["timestamp"]) - int(head["timestamp"]) <= 300
                ):
                    return head["price_usd"]
                return None
        row = conn.execute(
            "SELECT price_usd,timestamp FROM lp_price_marks WHERE token=? AND "
            "(block_number,tx_index,log_index)<=(?,?,?) ORDER BY block_number DESC,"
            "tx_index DESC,log_index DESC LIMIT 1", (token, *_order(event)),
        ).fetchone()
        if row is not None and 0 <= int(event["timestamp"]) - row["timestamp"] <= 300:
            return row["price_usd"]
        return None

    def _quote(self, conn, pool, event, ratio, heads=None):
        token0, token1 = pool["token0"], pool["token1"]
        basis = "at-or-before USDG anchor (max age 300s)"
        if token0 == USDG:
            price1 = _number(1.0 / ratio) if ratio else self._anchor(
                conn, token1, event, heads,
            )
            return 1.0, price1, "direct USDG pool spot" if ratio else basis
        if token1 == USDG:
            price0 = ratio if ratio else self._anchor(conn, token0, event, heads)
            return price0, 1.0, "direct USDG pool spot" if ratio else basis
        price0 = self._anchor(conn, token0, event, heads)
        price1 = self._anchor(conn, token1, event, heads)
        if ratio and price0 is not None and price1 is None:
            price1 = _number(price0 / ratio)
        elif ratio and price1 is not None and price0 is None:
            price0 = _number(price1 * ratio)
        return price0, price1, basis

    @staticmethod
    def _anchor_token(pool):
        if pool["protocol"] != "v3" or USDG not in (pool["token0"], pool["token1"]):
            return None
        if pool["factory"] != UNISWAP_V3_FACTORY and pool["id"] != WETH_USDG_POOL:
            return None
        token = pool["token1"] if pool["token0"] == USDG else pool["token0"]
        return None if token == WETH and pool["id"] != WETH_USDG_POOL else token

    @staticmethod
    def _active_liquidity(conn, pool, event, pool_state=_MISSING):
        if pool["protocol"] not in {"v3", "v4"}:
            return None
        if event.get("liquidity") is not None:
            return int(event["liquidity"])
        order = _order(event)
        if pool_state is _MISSING:
            old = conn.execute(
                "SELECT block_number,tx_index,log_index,tick,liquidity "
                "FROM lp_pool_state WHERE pool_id=? AND "
                "(block_number,tx_index,log_index)<=(?,?,?)",
                (pool["id"], *order),
            ).fetchone()
        else:
            old = pool_state
            if old is not None and (
                int(old["block_number"]), int(old["tx_index"]), int(old["log_index"])
            ) > order:
                old = None
        old_order = (
            (old["block_number"], old["tx_index"], old["log_index"])
            if old is not None else None
        )
        if old_order == order and old["liquidity"] is not None:
            return int(old["liquidity"])
        before = _data(event.get("data")).get("pool_state_before") or {}
        liquidity = before.get("liquidity")
        if liquidity is None and old is not None:
            liquidity = old["liquidity"]
        if liquidity is None:
            return None
        tick = event.get("tick")
        if tick is None:
            tick = before.get("tick")
        if tick is None and old is not None:
            tick = old["tick"]
        if (
            tick is not None
            and event.get("liquidity_delta") is not None
            and event.get("tick_lower") is not None
            and event.get("tick_upper") is not None
            and int(event["tick_lower"]) <= int(tick) < int(event["tick_upper"])
            and (old_order is None or order > old_order)
        ):
            liquidity = max(0, int(liquidity) + int(event["liquidity_delta"]))
        return int(liquidity)

    def _price(
        self, conn, pool, event, *, persist=True, persist_event=True,
        pool_state=_MISSING, old_anchor=_MISSING, anchor_heads=None,
        sample_rows=None, reserve_rows=None, mark_rows=None,
    ):
        extra = dict(_data(event.get("data")))
        sqrt = event.get("sqrt_price_x96")
        ratio = None
        valuation_ratio = None
        if pool["protocol"] == "v2":
            if extra.get("reserve0") is not None and extra.get("reserve1") is not None:
                reserve0 = _units(extra["reserve0"], pool["decimals0"])
                reserve1 = _units(extra["reserve1"], pool["decimals1"])
                ratio = _number(reserve1 / reserve0) if reserve0 and reserve1 is not None else None
                if int(extra["reserve0"]) > 0 and int(extra["reserve1"]) > 0:
                    valuation_ratio = ratio
                if persist:
                    reserve_row = (
                        event["id"], pool["id"], *_order(event), event["timestamp"],
                        extra["reserve0"], extra["reserve1"], ratio,
                    )
                    if reserve_rows is None:
                        conn.execute(
                            "INSERT OR REPLACE INTO lp_v2_reserve_samples "
                            "VALUES(?,?,?,?,?,?,?,?,?)",
                            reserve_row,
                        )
                    else:
                        reserve_rows.append(reserve_row)
            else:
                cached = pool_state if (
                    pool_state is not _MISSING
                    and pool_state is not None
                    and _order(pool_state) <= _order(event)
                    and pool_state.get("price") is not None
                ) else None
                sample = cached or conn.execute(
                    "SELECT reserve0,reserve1,price FROM lp_v2_reserve_samples WHERE pool_id=? AND "
                    "(block_number,tx_index,log_index)<=(?,?,?) ORDER BY block_number DESC,"
                    "tx_index DESC,log_index DESC LIMIT 1", (pool["id"], *_order(event)),
                ).fetchone()
                if sample is not None:
                    ratio = sample["price"]
                    if cached is not None or (
                        int(sample["reserve0"]) > 0 and int(sample["reserve1"]) > 0
                    ):
                        valuation_ratio = ratio
        elif sqrt is not None:
            ratio = _price_from_sqrt(int(sqrt), pool["decimals0"], pool["decimals1"])
        else:
            cached = pool_state if (
                pool_state is not _MISSING
                and pool_state is not None
                and _order(pool_state) <= _order(event)
                and pool_state.get("sqrt_price_x96") is not None
            ) else None
            sample = cached or conn.execute(
                "SELECT sqrt_price_x96,price,timestamp FROM lp_price_samples WHERE pool_id=? AND "
                "(block_number,tx_index,log_index)<=(?,?,?) ORDER BY block_number DESC,"
                "tx_index DESC,log_index DESC LIMIT 1", (pool["id"], *_order(event)),
            ).fetchone()
            sample_is_current = sample is not None and (
                persist or 0 <= int(event["timestamp"]) - int(sample["timestamp"]) <= 300
            )
            if sample_is_current:
                sqrt, ratio = sample["sqrt_price_x96"], sample["price"]
            else:
                before = extra.get("pool_state_before") or {}
                sqrt = before.get("sqrt_price_x96") or before.get("sqrt")
                if sqrt:
                    ratio = _price_from_sqrt(int(sqrt), pool["decimals0"], pool["decimals1"])
        if pool["protocol"] in {"v3", "v4"}:
            active_liquidity = self._active_liquidity(
                conn, pool, event, pool_state,
            )
            if active_liquidity is not None and active_liquidity > 0:
                valuation_ratio = ratio
        anchor_token = (
            self._anchor_token(pool)
            if event.get("sqrt_price_x96") is not None else None
        )
        if (
            persist and anchor_token is not None
            and (old_anchor is _MISSING or old_anchor is not None)
        ):
            # Never let a prior rendering of this event anchor its own rebuild.
            conn.execute(
                "DELETE FROM lp_price_marks WHERE token=? AND event_id=?",
                (anchor_token, event["id"]),
            )
            if (
                anchor_heads is not None
                and anchor_token in anchor_heads
                and anchor_heads[anchor_token] is not None
                and int(anchor_heads[anchor_token]["event_id"]) == int(event["id"])
            ):
                row = conn.execute(
                    "SELECT event_id,block_number,tx_index,log_index,timestamp,price_usd "
                    "FROM lp_price_marks WHERE token=? ORDER BY block_number DESC,"
                    "tx_index DESC,log_index DESC LIMIT 1",
                    (anchor_token,),
                ).fetchone()
                anchor_heads[anchor_token] = dict(row) if row is not None else None
        price0, price1, basis = self._quote(
            conn, pool, event, valuation_ratio, anchor_heads,
        )
        event["price0_usd"], event["price1_usd"] = price0, price1
        event["pricing_basis"] = basis if price0 is not None or price1 is not None else None
        event["volume_usd"] = None
        event["deposit_usd"] = event["withdrawal_usd"] = None
        if event["kind"] == "swap":
            raw0, raw1 = int(event.get("amount0") or 0), int(event.get("amount1") or 0)
            input0, input1 = _swap_inputs(event, pool, extra)
            if pool["token0"] == USDG:
                turnover = max(input0, input0 - raw0) if pool["protocol"] == "v2" else abs(raw0)
                event["volume_usd"] = _units(turnover, pool["decimals0"])
            elif pool["token1"] == USDG:
                turnover = max(input1, input1 - raw1) if pool["protocol"] == "v2" else abs(raw1)
                event["volume_usd"] = _units(turnover, pool["decimals1"])
            else:
                event["volume_usd"] = _basket(input0, input1, pool, price0, price1)
        _value_fees(event, pool, price0, price1, extra)
        if event.get("cashflow0") is not None and event.get("cashflow1") is not None:
            raw0, raw1 = int(event["cashflow0"]), int(event["cashflow1"])
            event["deposit_usd"] = _basket(max(-raw0, 0), max(-raw1, 0), pool, price0, price1)
            event["withdrawal_usd"] = _basket(max(raw0, 0), max(raw1, 0), pool, price0, price1)
        event["data"] = extra
        if persist and persist_event:
            conn.execute(
                "UPDATE events SET price0_usd=?,price1_usd=?,volume_usd=?,fees_usd=?,deposit_usd=?,"
                "withdrawal_usd=?,pricing_basis=?,data=? WHERE id=?",
                (price0, price1, event["volume_usd"], event["fees_usd"], event["deposit_usd"],
                 event["withdrawal_usd"], event["pricing_basis"],
                 json.dumps(extra, separators=(",", ":")), event["id"]),
            )
        new_anchor = None
        if persist and event.get("sqrt_price_x96") is not None:
            sample_row = (
                event["id"], pool["id"], *_order(event), event["timestamp"],
                str(sqrt), ratio, price0, price1,
            )
            if sample_rows is None:
                conn.execute(
                    "INSERT OR REPLACE INTO lp_price_samples VALUES(?,?,?,?,?,?,?,?,?,?)",
                    sample_row,
                )
            else:
                sample_rows.append(sample_row)
            # Only active direct USDG pools seed cross-pair marks. Arbitrary
            # graph cycles and empty-pool geometry never become dollar oracles.
            if anchor_token is not None and valuation_ratio is not None:
                price = price0 if anchor_token == pool["token0"] else price1
                if price and 0 < price <= 1e9:
                    mark_row = (
                        anchor_token, event["id"], pool["id"], *_order(event),
                        event["timestamp"], price, basis,
                    )
                    if mark_rows is None:
                        conn.execute(
                            "INSERT OR REPLACE INTO lp_price_marks VALUES(?,?,?,?,?,?,?,?,?)",
                            mark_row,
                        )
                    else:
                        mark_rows.append(mark_row)
                    new_anchor = (price, basis)
                    if anchor_heads is not None and anchor_token in anchor_heads:
                        head = anchor_heads[anchor_token]
                        if head is None or _order(event) >= _order(head):
                            anchor_heads[anchor_token] = {
                                "event_id": event["id"],
                                "block_number": _order(event)[0],
                                "tx_index": _order(event)[1],
                                "log_index": _order(event)[2],
                                "timestamp": event["timestamp"],
                                "price_usd": price,
                            }
        return sqrt, ratio, new_anchor

    def value_current(self, raw, pool):
        """Value a provisional current event without mutating durable projections."""
        event = dict(raw)
        flows = (event.get("transaction_flow0"), event.get("transaction_flow1"))
        exact_flows = flows if event.get("flow_scope") == "transaction" else (
            event.get("cashflow0"), event.get("cashflow1"),
        )
        for side in (0, 1):
            decimals = pool.get(f"decimals{side}")
            if (
                pool.get(f"token{side}") == USDG
                and exact_flows[side] is not None
                and decimals is not None
            ):
                event["usdg_flow_usd"] = _units(exact_flows[side], decimals)
        required = ("id", "protocol", "token0", "token1", "decimals0", "decimals1")
        if any(pool.get(key) is None for key in required):
            extra = dict(_data(event.get("data")))
            price0 = 1.0 if (
                pool.get("token0") == USDG and pool.get("decimals0") is not None
            ) else _number(event.get("price0_usd"))
            price1 = 1.0 if (
                pool.get("token1") == USDG and pool.get("decimals1") is not None
            ) else _number(event.get("price1_usd"))
            event["price0_usd"], event["price1_usd"] = price0, price1
            if (
                event.get("pricing_basis") is None
                and (price0 is not None or price1 is not None)
            ):
                event["pricing_basis"] = (
                    "direct USDG leg only; countertoken price unavailable"
                )
            _value_fees(event, pool, price0, price1, extra)
            event["data"] = extra
            return event
        conn = self.store.read()
        self._price(conn, pool, event, persist=False)
        if all(value is not None for value in flows):
            raw0, raw1 = int(flows[0]), int(flows[1])
            event["deposit_usd"] = _basket(
                max(-raw0, 0), max(-raw1, 0), pool,
                event.get("price0_usd"), event.get("price1_usd"),
            )
            event["withdrawal_usd"] = _basket(
                max(raw0, 0), max(raw1, 0), pool,
                event.get("price0_usd"), event.get("price1_usd"),
            )
            if event["deposit_usd"] is not None and event["withdrawal_usd"] is not None:
                event["cashflow_usd"] = event["withdrawal_usd"] - event["deposit_usd"]
        return event

    @staticmethod
    def _state(conn, pool, event, sqrt, ratio, old=_MISSING, *, persist=True):
        if old is _MISSING:
            row = conn.execute(
                "SELECT * FROM lp_pool_state WHERE pool_id=?", (pool["id"],),
            ).fetchone()
            old = dict(row) if row is not None else None
        if old is not None and _order(event) < (
            old["block_number"], old["tx_index"], old["log_index"],
        ):
            return old
        if sqrt is None and not (pool["protocol"] == "v2" and ratio is not None):
            return old
        extra = _data(event.get("data"))
        before = extra.get("pool_state_before") or {}
        tick = event.get("tick")
        if tick is None:
            tick = old["tick"] if old else before.get("tick")
        liquidity = event.get("liquidity")
        if liquidity is None:
            liquidity = old["liquidity"] if old else before.get("liquidity")
            if (liquidity is not None and tick is not None and event.get("liquidity_delta") is not None
                    and event.get("tick_lower") is not None and event.get("tick_upper") is not None
                    and int(event["tick_lower"]) <= int(tick) < int(event["tick_upper"])
                    and (old is None or _order(event) > (
                        old["block_number"], old["tx_index"], old["log_index"],
                    ))):
                liquidity = str(max(0, int(liquidity) + int(event["liquidity_delta"])))
        fee = event.get("fee_ppm")
        if fee is None:
            fee = old["fee_ppm"] if old else pool["fee_ppm"]
        state = {
            "pool_id": pool["id"],
            "block_number": _order(event)[0],
            "tx_index": _order(event)[1],
            "log_index": _order(event)[2],
            "timestamp": event["timestamp"],
            "sqrt_price_x96": str(sqrt) if sqrt is not None else None,
            "tick": tick,
            "liquidity": liquidity,
            "price0_usd": event.get("price0_usd"),
            "price1_usd": event.get("price1_usd"),
            "price": ratio,
            "fee_ppm": fee,
            "pricing_basis": event.get("pricing_basis"),
        }
        if persist:
            conn.execute(
                "INSERT OR REPLACE INTO lp_pool_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(state[name] for name in (
                    "pool_id", "block_number", "tx_index", "log_index", "timestamp",
                    "sqrt_price_x96", "tick", "liquidity", "price0_usd",
                    "price1_usd", "price", "fee_ppm", "pricing_basis",
                )),
            )
        return state

    @staticmethod
    def _bucket(conn, pool_id, minute, ancestor=None):
        old = conn.execute("SELECT * FROM lp_pool_buckets WHERE resolution=60 AND bucket=? AND pool_id=?",
                           (minute, pool_id)).fetchone()
        row = conn.execute(
            "SELECT COUNT(*) AS events,SUM(kind='swap') AS swaps,SUM(kind='add') AS adds,"
            "SUM(kind='remove') AS removes,SUM(kind='collect') AS collects,"
            "SUM(CASE WHEN kind='swap' THEN volume_usd ELSE 0 END) AS volume_usd,"
            "SUM(CASE WHEN kind='swap' THEN fees_usd ELSE 0 END) AS fees_usd,"
            "SUM(deposit_usd) AS deposit_usd,SUM(withdrawal_usd) AS withdrawal_usd,"
            "SUM(kind='swap' AND volume_usd IS NOT NULL) AS priced_swaps,"
            "SUM(kind='swap' AND fees_usd IS NOT NULL) AS priced_fees,"
            "SUM((kind IN ('add','remove','collect') OR (kind='checkpoint' AND position_key IS NOT NULL)) "
            "AND deposit_usd IS NOT NULL AND withdrawal_usd IS NOT NULL) AS priced_flows,"
            "SUM(kind IN ('add','remove','collect') OR (kind='checkpoint' AND position_key IS NOT NULL)) "
            "AS flows,MAX(block_number) AS max_block "
            "FROM events WHERE pool_id=? AND timestamp>=? AND timestamp<?"
            + (" AND block_number<=?" if ancestor is not None else ""),
            (pool_id, minute, minute + 60, ancestor) if ancestor is not None else (pool_id, minute, minute + 60),
        ).fetchone()
        values = [row[name] or 0 for name in BUCKET_FIELDS]
        max_block = row["max_block"] or 0
        if row["events"]:
            placeholders = ",".join("?" for _ in range(len(values) + 4))
            conn.execute(f"INSERT OR REPLACE INTO lp_pool_buckets VALUES({placeholders})",
                         (60, minute, pool_id, *values, max_block))
        else:
            conn.execute("DELETE FROM lp_pool_buckets WHERE resolution=60 AND bucket=? AND pool_id=?",
                         (minute, pool_id))
        delta = [value - (old[name] if old else 0) for name, value in zip(BUCKET_FIELDS, values)]
        hour = minute // 3600 * 3600
        columns = ",".join(BUCKET_FIELDS)
        update = ",".join(f"{name}={name}+excluded.{name}" for name in BUCKET_FIELDS)
        placeholders = ",".join("?" for _ in range(len(delta) + 4))
        conn.execute(
            f"INSERT INTO lp_pool_buckets(resolution,bucket,pool_id,{columns},max_block) "
            f"VALUES({placeholders}) ON CONFLICT(resolution,bucket,pool_id) DO UPDATE SET "
            f"{update},max_block=MAX(max_block,excluded.max_block)",
            (3600, hour, pool_id, *delta, max_block),
        )
        conn.execute("DELETE FROM lp_pool_buckets WHERE resolution=3600 AND bucket=? AND pool_id=? AND events<=0",
                     (hour, pool_id))

    @staticmethod
    def _buckets(conn, keys):
        ordered = sorted(keys)
        if not ordered:
            return
        old_rows = {}
        aggregates = {}
        for batch in _batches(ordered, 200):
            values = ",".join("(?,?)" for _ in batch)
            args = tuple(value for key in batch for value in key)
            for row in conn.execute(
                f"WITH affected(pool_id,minute) AS (VALUES {values}) "
                "SELECT b.* FROM affected a JOIN lp_pool_buckets b "
                "ON b.resolution=60 AND b.pool_id=a.pool_id AND b.bucket=a.minute",
                args,
            ).fetchall():
                old_rows[(row["pool_id"], row["bucket"])] = row
            for row in conn.execute(
                f"WITH affected(pool_id,minute) AS (VALUES {values}) "
                "SELECT a.pool_id,a.minute AS bucket,"
                "COUNT(*) AS events,SUM(e.kind='swap') AS swaps,"
                "SUM(e.kind='add') AS adds,SUM(e.kind='remove') AS removes,"
                "SUM(e.kind='collect') AS collects,"
                "SUM(CASE WHEN e.kind='swap' THEN e.volume_usd ELSE 0 END) AS volume_usd,"
                "SUM(CASE WHEN e.kind='swap' THEN e.fees_usd ELSE 0 END) AS fees_usd,"
                "SUM(e.deposit_usd) AS deposit_usd,"
                "SUM(e.withdrawal_usd) AS withdrawal_usd,"
                "SUM(e.kind='swap' AND e.volume_usd IS NOT NULL) AS priced_swaps,"
                "SUM(e.kind='swap' AND e.fees_usd IS NOT NULL) AS priced_fees,"
                "SUM((e.kind IN ('add','remove','collect') OR "
                "(e.kind='checkpoint' AND e.position_key IS NOT NULL)) "
                "AND e.deposit_usd IS NOT NULL AND e.withdrawal_usd IS NOT NULL) "
                "AS priced_flows,"
                "SUM(e.kind IN ('add','remove','collect') OR "
                "(e.kind='checkpoint' AND e.position_key IS NOT NULL)) AS flows,"
                "MAX(e.block_number) AS max_block "
                "FROM affected a JOIN events e ON e.pool_id=a.pool_id "
                "AND e.timestamp>=a.minute AND e.timestamp<a.minute+60 "
                "GROUP BY a.pool_id,a.minute",
                args,
            ).fetchall():
                aggregates[(row["pool_id"], row["bucket"])] = row
        minute_rows = []
        missing = []
        hour_deltas = {}
        hour_max = {}
        for pool_id, minute in ordered:
            row = aggregates.get((pool_id, minute))
            values = [row[name] or 0 for name in BUCKET_FIELDS] if row else [
                0 for _name in BUCKET_FIELDS
            ]
            max_block = row["max_block"] or 0 if row else 0
            if row and row["events"]:
                minute_rows.append((60, minute, pool_id, *values, max_block))
            else:
                missing.append((minute, pool_id))
            old = old_rows.get((pool_id, minute))
            delta = [
                value - (old[name] if old else 0)
                for name, value in zip(BUCKET_FIELDS, values)
            ]
            hour_key = (pool_id, minute // 3600 * 3600)
            totals = hour_deltas.setdefault(hour_key, [0 for _name in BUCKET_FIELDS])
            for index, value in enumerate(delta):
                totals[index] += value
            hour_max[hour_key] = max(hour_max.get(hour_key, 0), max_block)
        placeholders = ",".join("?" for _ in range(len(BUCKET_FIELDS) + 4))
        conn.executemany(
            f"INSERT OR REPLACE INTO lp_pool_buckets VALUES({placeholders})",
            minute_rows,
        )
        conn.executemany(
            "DELETE FROM lp_pool_buckets WHERE resolution=60 AND bucket=? AND pool_id=?",
            missing,
        )
        columns = ",".join(BUCKET_FIELDS)
        update = ",".join(f"{name}={name}+excluded.{name}" for name in BUCKET_FIELDS)
        conn.executemany(
            f"INSERT INTO lp_pool_buckets(resolution,bucket,pool_id,{columns},max_block) "
            f"VALUES({placeholders}) ON CONFLICT(resolution,bucket,pool_id) DO UPDATE SET "
            f"{update},max_block=MAX(max_block,excluded.max_block)",
            (
                (3600, hour, pool_id, *delta, hour_max[(pool_id, hour)])
                for (pool_id, hour), delta in hour_deltas.items()
            ),
        )
        conn.executemany(
            "DELETE FROM lp_pool_buckets WHERE resolution=3600 "
            "AND bucket=? AND pool_id=? AND events<=0",
            ((hour, pool_id) for pool_id, hour in hour_deltas),
        )

    def _queue_successors(self, conn, sources, revision):
        """Repair old consumers, never rows just priced in this delivery."""
        token_bounds = {}
        for pool, changes in sources.values():
            low, high = min(changes, key=_order), max(changes, key=_order)
            table = "lp_v2_reserve_samples" if pool["protocol"] == "v2" else "lp_price_samples"
            following = conn.execute(
                f"SELECT block_number,tx_index,log_index FROM {table} WHERE pool_id=? AND "
                "(block_number,tx_index,log_index)>(?,?,?) ORDER BY block_number,tx_index,log_index LIMIT 1",
                (pool["id"], *_order(high)),
            ).fetchone()
            upper = tuple(following) if following else (2**63 - 1, 0, 0)
            queued = conn.execute(
                "INSERT OR IGNORE INTO pending_reprojection"
                "(event_id,block_number,tx_index,log_index) "
                "SELECT id,block_number,tx_index,log_index FROM events "
                "WHERE pool_id=? AND (block_number,tx_index,log_index)>(?,?,?) "
                "AND (block_number,tx_index,log_index)<(?,?,?) AND revision<>?",
                (pool["id"], *_order(low), *upper, revision),
            ).rowcount
            if queued:
                self.store._bump(conn, "pending_reprojection", queued)
            token = self._anchor_token(pool)
            if token is not None:
                token_bounds.setdefault(token, []).extend(changes)
        for token, changes in token_bounds.items():
            low, high = min(changes, key=_order), max(changes, key=_order)
            following = conn.execute(
                "SELECT block_number,tx_index,log_index FROM lp_price_marks WHERE token=? AND "
                "(block_number,tx_index,log_index)>(?,?,?) ORDER BY block_number,tx_index,log_index LIMIT 1",
                (token, *_order(high)),
            ).fetchone()
            upper = tuple(following) if following else (2**63 - 1, 0, 0)
            # WETH anchors also value gas on pools with neither native token leg.
            gas_clause = (" OR EXISTS(SELECT 1 FROM transactions t WHERE t.tx_hash=e.tx_hash "
                          "AND t.gas_native IS NOT NULL)") if token == WETH else ""
            queued = conn.execute(
                "INSERT OR IGNORE INTO pending_reprojection"
                "(event_id,block_number,tx_index,log_index) "
                "SELECT e.id,e.block_number,e.tx_index,e.log_index FROM events e "
                "JOIN pools p ON p.id=e.pool_id WHERE (e.block_number,e.tx_index,e.log_index)>(?,?,?) "
                "AND (e.block_number,e.tx_index,e.log_index)<(?,?,?) AND e.timestamp<=? AND e.revision<>? "
                "AND ((p.token0<>? AND p.token1<>? AND (p.token0 IN (?,?) OR p.token1 IN (?,?)))"
                + gas_clause + ")",
                (*_order(low), *upper, int(high["timestamp"]) + 300, revision, USDG, USDG,
                 token, NATIVE if token == WETH else token, token, NATIVE if token == WETH else token),
            ).rowcount
            if queued:
                self.store._bump(conn, "pending_reprojection", queued)

    def apply(self, conn, events):
        ordered = sorted(events, key=_order)
        revision = int(events[0]["revision"])
        prior = conn.execute(
            "SELECT block_number,tx_index,log_index FROM events WHERE revision<>? "
            "ORDER BY block_number DESC,tx_index DESC,log_index DESC LIMIT 1",
            (revision,),
        ).fetchone()
        forward = prior is None or _order(ordered[0]) > tuple(prior)
        pool_ids = list(dict.fromkeys(
            str(event["pool_id"]) for event in ordered if event.get("pool_id")
        ))
        pools = {}
        for batch in _batches(pool_ids):
            marks = ",".join("?" for _ in batch)
            pools.update({
                str(row["id"]): dict(row)
                for row in conn.execute(
                    f"SELECT * FROM pools WHERE id IN ({marks})", batch,
                ).fetchall()
            })
        state_heads = {pool_id: None for pool_id in pool_ids}
        for batch in _batches(pool_ids):
            marks = ",".join("?" for _ in batch)
            for row in conn.execute(
                f"SELECT * FROM lp_pool_state WHERE pool_id IN ({marks})", batch,
            ).fetchall():
                state_heads[str(row["pool_id"])] = dict(row)

        event_info = []
        source_ids = {"lp_price_samples": [], "lp_v2_reserve_samples": []}
        anchor_ids = {}
        transaction_events = {}
        anchor_uses = {}
        for event in ordered:
            pool_id = event.get("pool_id")
            pool = pools.get(str(pool_id)) if pool_id else None
            if pool is None:
                continue
            extra = _data(event.get("data"))
            source = event.get("sqrt_price_x96") is not None or (
                pool["protocol"] == "v2"
                and extra.get("reserve0") is not None
                and extra.get("reserve1") is not None
            )
            table = "lp_v2_reserve_samples" if pool["protocol"] == "v2" else "lp_price_samples"
            token = self._anchor_token(pool) if source else None
            event_info.append((event, pool, source, table, token))
            if source:
                source_ids[table].append(int(event["id"]))
            if token is not None:
                anchor_ids.setdefault(token, []).append(int(event["id"]))
            for candidate in (pool["token0"], pool["token1"]):
                if candidate != USDG:
                    candidate = WETH if candidate == NATIVE else candidate
                    anchor_uses[candidate] = anchor_uses.get(candidate, 0) + 1
            transaction_events[str(event["tx_hash"])] = event

        old_sources = {}
        for table, ids in source_ids.items():
            for batch in _batches(ids):
                marks = ",".join("?" for _ in batch)
                old_sources.update({
                    (table, int(row["event_id"])): row["price"]
                    for row in conn.execute(
                        f"SELECT event_id,price FROM {table} "
                        f"WHERE event_id IN ({marks})", batch,
                    ).fetchall()
                })
        old_anchors = {}
        for token, ids in anchor_ids.items():
            for batch in _batches(ids):
                marks = ",".join("?" for _ in batch)
                old_anchors.update({
                    (token, int(row["event_id"])): (row["price_usd"], row["basis"])
                    for row in conn.execute(
                        "SELECT event_id,price_usd,basis FROM lp_price_marks "
                        f"WHERE token=? AND event_id IN ({marks})",
                        (token, *batch),
                    ).fetchall()
                })

        gas_rows = {}
        transaction_hashes = list(transaction_events)
        for batch in _batches(transaction_hashes):
            marks = ",".join("?" for _ in batch)
            gas_rows.update({
                str(row["tx_hash"]): row["gas_native"]
                for row in conn.execute(
                    "SELECT tx_hash,gas_native FROM transactions "
                    f"WHERE gas_native IS NOT NULL AND tx_hash IN ({marks})", batch,
                ).fetchall()
            })
        if gas_rows:
            anchor_uses[WETH] = anchor_uses.get(WETH, 0) + len(gas_rows)
        anchor_heads = {}
        for token, uses in anchor_uses.items():
            if uses <= 1:
                continue
            row = conn.execute(
                "SELECT event_id,block_number,tx_index,log_index,timestamp,price_usd "
                "FROM lp_price_marks WHERE token=? ORDER BY block_number DESC,"
                "tx_index DESC,log_index DESC LIMIT 1",
                (token,),
            ).fetchone()
            anchor_heads[token] = dict(row) if row is not None else None

        buckets = set()
        sources = {}
        dirty_states = set()
        event_updates = []
        sample_rows = [] if forward else None
        reserve_rows = [] if forward else None
        mark_rows = [] if forward else None
        for event, pool, source, table, token in event_info:
            pool_id = str(pool["id"])
            old_anchor = old_anchors.get((token, int(event["id"]))) if token else None
            sqrt, ratio, new_anchor = self._price(
                conn, pool, event, persist_event=False,
                pool_state=state_heads[pool_id], old_anchor=old_anchor,
                anchor_heads=anchor_heads, sample_rows=sample_rows,
                reserve_rows=reserve_rows, mark_rows=mark_rows,
            )
            old_state = state_heads[pool_id]
            new_state = self._state(
                conn, pool, event, sqrt, ratio, old_state, persist=False,
            )
            if new_state is not old_state:
                state_heads[pool_id] = new_state
                dirty_states.add(pool_id)
            old_source_key = (table, int(event["id"]))
            if source and (
                old_source_key not in old_sources
                or old_sources[old_source_key] != ratio
                or old_anchor != new_anchor
            ):
                sources.setdefault(pool_id, (pool, []))[1].append(event)
            buckets.add((pool_id, int(event["timestamp"]) // 60 * 60))
            event_updates.append((
                event.get("price0_usd"), event.get("price1_usd"),
                event.get("volume_usd"), event.get("fees_usd"),
                event.get("deposit_usd"), event.get("withdrawal_usd"),
                event.get("pricing_basis"),
                self.store._event_data(event),
                int(event["id"]),
            ))
        if sample_rows is not None:
            _insert_rows(
                conn, "INSERT OR REPLACE INTO lp_price_samples",
                sample_rows, columns=10,
            )
        if reserve_rows is not None:
            _insert_rows(
                conn, "INSERT OR REPLACE INTO lp_v2_reserve_samples",
                reserve_rows, columns=9,
            )
        if mark_rows is not None:
            _insert_rows(
                conn, "INSERT OR REPLACE INTO lp_price_marks",
                mark_rows, columns=9,
            )
        conn.executemany(
            "UPDATE events SET price0_usd=?,price1_usd=?,volume_usd=?,fees_usd=?,"
            "deposit_usd=?,withdrawal_usd=?,pricing_basis=?,data=? WHERE id=?",
            event_updates,
        )
        _insert_rows(
            conn, "INSERT OR REPLACE INTO lp_pool_state",
            (
                tuple(state_heads[pool_id][name] for name in (
                    "pool_id", "block_number", "tx_index", "log_index",
                    "timestamp", "sqrt_price_x96", "tick", "liquidity",
                    "price0_usd", "price1_usd", "price", "fee_ppm",
                    "pricing_basis",
                ))
                for pool_id in dirty_states
            ),
            columns=13,
        )
        if sources and not forward:
            self._queue_successors(conn, sources, revision)
        self._buckets(conn, buckets)
        gas_updates = [
            (
                _value(gas_native, 18, self._anchor(
                    conn, WETH, transaction_events[tx_hash], anchor_heads,
                )),
                tx_hash,
            )
            for tx_hash, gas_native in gas_rows.items()
        ]
        conn.executemany(
            "UPDATE transactions SET gas_usd=? WHERE tx_hash=?",
            gas_updates,
        )

    def rollback(self, conn, ancestor):
        buckets = conn.execute(
            "SELECT bucket,pool_id FROM lp_pool_buckets WHERE resolution=60 AND max_block>?", (ancestor,),
        ).fetchall()
        affected = conn.execute("SELECT pool_id FROM lp_pool_state WHERE block_number>?", (ancestor,)).fetchall()
        conn.execute("DELETE FROM lp_price_marks WHERE block_number>?", (ancestor,))
        conn.execute("DELETE FROM lp_price_samples WHERE block_number>?", (ancestor,))
        conn.execute("DELETE FROM lp_v2_reserve_samples WHERE block_number>?", (ancestor,))
        conn.execute("DELETE FROM lp_pool_state WHERE block_number>?", (ancestor,))
        for row in buckets:
            self._bucket(conn, row["pool_id"], row["bucket"], ancestor)
        for row in affected:
            event = conn.execute(
                "SELECT * FROM events WHERE pool_id=? AND block_number<=? AND "
                "(sqrt_price_x96 IS NOT NULL OR (protocol='v2' AND kind='checkpoint')) "
                "ORDER BY block_number DESC,tx_index DESC,log_index DESC LIMIT 1", (row["pool_id"], ancestor),
            ).fetchone()
            pool = conn.execute("SELECT * FROM pools WHERE id=?", (row["pool_id"],)).fetchone()
            if event and pool:
                event = dict(event)
                pool = dict(pool)
                sqrt, ratio, _anchor = self._price(conn, pool, event)
                self._state(conn, pool, event, sqrt, ratio)
                following = conn.execute(
                    "SELECT * FROM events WHERE pool_id=? AND block_number<=? "
                    "AND (block_number,tx_index,log_index)>(?,?,?) "
                    "ORDER BY block_number,tx_index,log_index", (pool["id"], ancestor, *_order(event)),
                ).fetchall()
                for change in following:
                    self._state(conn, pool, dict(change), sqrt, ratio)


class LPMarketService:
    """Cached, bounded HTTP read surface over one durable event index."""

    def __init__(self, market, rpc_url, path: str | Path, *, history_days=30,
                 history_disk_reserve_bytes=4 * 1024**3, start=True, deferred_start=True):
        from .lp_market_store import MarketStore
        from .lp_market_index import MarketIndexer
        from .lp_market_accounting import AccountBook
        self.market = market
        self.store = MarketStore(path, checkpoint_on_commit=not start)
        self.prices = PriceProjection(self.store)
        self.book = AccountBook(self.store)
        self.book.install()
        self._current_activity_lock = threading.RLock()
        self._current_activity_headers: OrderedDict[int, dict[str, Any]] = OrderedDict()
        self._current_activity_events: OrderedDict[
            tuple[str, str, int], dict[str, Any]
        ] = OrderedDict()
        self._current_activity_keys_by_hash: dict[
            str, set[tuple[str, str, int]]
        ] = {}
        self._current_activity_revision = 0
        self._current_activity_epoch = 0
        self._current_owner_snapshots: OrderedDict[
            tuple[Any, ...], dict[str, Any]
        ] = OrderedDict()
        self.indexer = MarketIndexer(
            self.store, market, rpc_url, history_days=history_days,
            history_disk_reserve_bytes=history_disk_reserve_bytes, v3_balances=True,
            current_observer=self,
        )
        # Restore the durable catalog before serving requests. Exact cold
        # inspectors can then reuse an already-qualified stored identity even
        # when the workbench checkpoint is absent and no new event arrives.
        try:
            while self.indexer._publish_stored_pool_page():
                pass
        finally:
            self.store.close_reader()
        self._cache = OrderedDict()
        self._cache_lock = threading.Lock()
        self._cache_futures: dict[tuple[Any, ...], Future] = {}
        self._status_cache_lock = threading.Lock()
        self._status_refresh_lock = threading.Lock()
        self._status_cache_at = 0.0
        self._status_cache_value: dict[str, Any] | None = None
        self._status_cache_token = self.store.change_token
        self._search_stop = threading.Event()
        self._search_error: str | None = None
        self._search_thread: threading.Thread | None = None
        self._catalog_source_cache = None
        self._catalog_discovered_ids: set[str] = set()
        self._catalog_discovery_marker = None
        self._frame_lock = threading.Lock()
        self._frame_futures: dict[tuple[Any, ...], Future] = {}
        self._frame_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="lp-market-frame",
        )
        self._owner_results: OrderedDict[
            tuple[Any, ...], tuple[
                tuple[int, int, int, int], float | None, dict[str, Any]
            ]
        ] = OrderedDict()
        self._owner_last_started: dict[tuple[Any, ...], float] = {}
        self._owner_result_revision = 0
        self._current_view_lock = threading.Lock()
        # Direct-mapped publication makes the overwhelmingly common feed-cache
        # hit lock-free while retaining a strict memory bound. A miss is still
        # serialized so only one SSE worker opens a temporary SQLite reader.
        self._current_view_cache: list[
            tuple[tuple[str, int, int], tuple[dict[str, Any], ...]] | None
        ] = [None] * 1024
        if start:
            self.indexer.start(deferred=deferred_start)
            self._search_thread = threading.Thread(
                target=self._search_index_run,
                name="lp-market-search-index",
                daemon=True,
            )
            self._search_thread.start()
        else:
            self.store.ensure_search_index()

    @staticmethod
    def _observer_integer(value: Any) -> int:
        if isinstance(value, str) and value.startswith("0x"):
            return int(value, 16)
        return int(value)

    @staticmethod
    def _observer_address(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        address = value.lower()
        if (
            len(address) != 42 or not address.startswith("0x")
            or address == "0x" + "0" * 40
        ):
            return None
        try:
            int(address[2:], 16)
        except ValueError:
            return None
        return address

    def _drop_current_activity_hash_locked(self, block_hash: str) -> bool:
        changed = False
        for key in self._current_activity_keys_by_hash.pop(block_hash, ()):
            changed = self._current_activity_events.pop(key, None) is not None or changed
        return changed

    def _clear_current_activity_locked(self) -> bool:
        changed = bool(self._current_activity_events)
        self._current_activity_headers.clear()
        self._current_activity_events.clear()
        self._current_activity_keys_by_hash.clear()
        return changed

    def observe_current_block(self, raw: Mapping[str, Any]) -> None:
        """Track only a continuous canonical suffix of current headers in memory."""
        number = self._observer_integer(raw["number"])
        block_hash = str(raw["hash"]).lower()
        parent_hash = str(raw.get("parentHash") or raw.get("parent_hash") or "").lower()
        timestamp = self._observer_integer(raw["timestamp"])
        header = {
            "number": number, "hash": block_hash,
            "parent_hash": parent_hash, "timestamp": timestamp,
        }
        with self._current_activity_lock:
            content_changed = False
            canonical_reset = False
            prior = next(reversed(self._current_activity_headers.values()), None)
            if prior is not None:
                prior_number = int(prior["number"])
                existing = self._current_activity_headers.get(number)
                if existing is not None and existing["hash"] == block_hash:
                    return
                if number <= prior_number:
                    canonical_reset = True
                    for orphan_number in tuple(self._current_activity_headers):
                        if orphan_number >= number:
                            orphan = self._current_activity_headers.pop(orphan_number)
                            content_changed = (
                                self._drop_current_activity_hash_locked(
                                    str(orphan["hash"])
                                )
                                or content_changed
                            )
                    retained_parent = next(
                        reversed(self._current_activity_headers.values()), None,
                    )
                    if (
                        retained_parent is not None
                        and (
                            int(retained_parent["number"]) != number - 1
                            or str(retained_parent["hash"]) != parent_hash
                        )
                    ):
                        content_changed = (
                            self._clear_current_activity_locked() or content_changed
                        )
                elif (
                    number != prior_number + 1
                    or parent_hash != str(prior["hash"])
                ):
                    canonical_reset = True
                    # Without a verified continuous parent, retaining older
                    # activity would claim coverage across an unknown gap.
                    content_changed = (
                        self._clear_current_activity_locked() or content_changed
                    )
            self._current_activity_headers[number] = header
            self._current_activity_headers.move_to_end(number)
            while len(self._current_activity_headers) > 2048:
                _, expired = self._current_activity_headers.popitem(last=False)
                content_changed = (
                    self._drop_current_activity_hash_locked(str(expired["hash"]))
                    or content_changed
                )
            if content_changed:
                self._current_activity_revision += 1
            if canonical_reset:
                self._current_activity_epoch += 1

    def observe_current_events(
            self, raw_header: Mapping[str, Any],
            events: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]],
    ) -> None:
        """Coalesce canonical current events and late enrichment by log identity."""
        number = self._observer_integer(raw_header["number"])
        block_hash = str(raw_header["hash"]).lower()
        timestamp = self._observer_integer(raw_header["timestamp"])
        changed = False
        with self._current_activity_lock:
            canonical = self._current_activity_headers.get(number)
            if canonical is None or canonical["hash"] != block_hash:
                return
            for raw in events:
                event = dict(raw)
                event_hash = str(event.get("block_hash") or block_hash).lower()
                event_number = int(event.get("block_number") or number)
                event_timestamp = int(event.get("timestamp") or timestamp)
                if (
                    event_hash != block_hash or event_number != number
                    or event_timestamp != timestamp
                ):
                    continue
                kind = str(event.get("kind") or "").lower()
                if kind not in {
                    "add", "remove", "collect", "checkpoint",
                    "donate", "fee", "transfer",
                }:
                    continue
                tx_hash = str(event.get("tx_hash") or "").lower()
                if not tx_hash:
                    continue
                key = (block_hash, tx_hash, int(event.get("log_index") or 0))
                previous = self._current_activity_events.get(key)
                if previous is not None:
                    previous_data = previous.get("data")
                    incoming_data = event.get("data")
                    event["data"] = {
                        **(dict(previous_data) if isinstance(previous_data, Mapping) else {}),
                        **(dict(incoming_data) if isinstance(incoming_data, Mapping) else {}),
                    }
                    previous_pool = previous.get("pool")
                    incoming_pool = event.get("pool")
                    if isinstance(previous_pool, Mapping) or isinstance(incoming_pool, Mapping):
                        event["pool"] = {
                            **(dict(previous_pool) if isinstance(previous_pool, Mapping) else {}),
                            **(dict(incoming_pool) if isinstance(incoming_pool, Mapping) else {}),
                        }
                    merged = {**previous, **event}
                else:
                    merged = event
                merged.update({
                    "block_number": number, "block_hash": block_hash,
                    "tx_hash": tx_hash, "timestamp": timestamp, "kind": kind,
                })
                if previous == merged:
                    continue
                self._current_activity_events[key] = merged
                self._current_activity_keys_by_hash.setdefault(block_hash, set()).add(key)
                changed = True
            if changed:
                self._current_activity_revision += 1

    def _pool_metadata_revision(self) -> int:
        return int(getattr(
            self.store, "pool_metadata_token",
            getattr(self.market, "pool_publication_revision", 0),
        ))

    def _current_pool_metadata(
            self, event: Mapping[str, Any],
            cache: dict[str, dict[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        embedded = event.get("pool")
        pool = dict(embedded) if isinstance(embedded, Mapping) else {}
        pool_id = str(event.get("pool_id") or pool.get("id") or "").lower()
        if not pool_id:
            return pool
        incomplete = any(
            pool.get(key) is None
            for key in (
                "token0", "token1", "symbol0", "symbol1",
                "decimals0", "decimals1",
            )
        )
        if not incomplete:
            return pool
        if cache is not None and pool_id in cache:
            stored = cache[pool_id]
        else:
            stored = self.store.pool(pool_id)
            if cache is not None:
                cache[pool_id] = stored
        if not isinstance(stored, Mapping):
            return pool
        # Never join token metadata across disagreeing identities. The durable
        # row may enrich only the exact pool referenced by the observed event.
        if any(
            pool.get(key) is not None
            and stored.get(key) is not None
            and str(pool[key]).lower() != str(stored[key]).lower()
            for key in ("id", "protocol", "token0", "token1")
        ):
            return pool
        for key in (
            "id", "protocol", "token0", "token1", "symbol0", "symbol1",
            "decimals0", "decimals1", "fee_ppm",
        ):
            if pool.get(key) is None and stored.get(key) is not None:
                pool[key] = stored[key]
        return pool

    @staticmethod
    def _observer_pair(
            event: Mapping[str, Any], metadata: Mapping[str, Any] | None = None,
    ) -> str:
        pool = metadata if metadata is not None else event.get("pool")
        pool = pool if isinstance(pool, Mapping) else {}
        symbol0 = event.get("symbol0") or pool.get("symbol0")
        symbol1 = event.get("symbol1") or pool.get("symbol1")
        token0 = event.get("token0") or pool.get("token0")
        token1 = event.get("token1") or pool.get("token1")
        if token0 is None or token1 is None:
            return str(event.get("pool_id") or pool.get("id") or "unresolved pool")
        return f"{symbol0 or token0} / {symbol1 or token1}"

    def _current_activity_status(self, revision: int | None = None) -> dict[str, Any]:
        with self._current_activity_lock:
            headers = self._current_activity_headers
            head = next(reversed(headers.values()), None)
            observed_from = next(iter(headers.values()), None)
            content_revision = self._current_activity_revision
            activity_epoch = self._current_activity_epoch
            head_number = head["number"] if head is not None else None
            observed_number = (
                observed_from["number"] if observed_from is not None else None
            )
        return {
            "revision": content_revision if revision is None else int(revision),
            "epoch": activity_epoch,
            "head": head_number,
            "observed_from": observed_number,
            "qualification": "provisional_canonical",
        }

    def _current_owner_snapshot(self, params: Mapping[str, Any]) -> dict[str, Any]:
        protocol = str(params.get("protocol") or "").lower()
        if protocol and protocol not in {"v2", "v3", "v4"}:
            raise ValueError("protocol must be v2, v3 or v4")
        pool_id = str(params.get("pool") or params.get("pool_id") or "").lower()
        query = str(params.get("q") or "").strip().lower()[:128]
        window = str(params.get("window") or "24h").lower()
        if window not in WINDOWS:
            raise ValueError("window must be 1h, 24h, 7d, 30d or all")
        seconds = WINDOWS[window]
        snapshot_key = (window, protocol, pool_id, query)
        now = time.time()
        metadata_revision = self._pool_metadata_revision()
        with self._current_activity_lock:
            revision = self._current_activity_revision
            cached = self._current_owner_snapshots.get(snapshot_key)
            if (
                cached is not None
                and cached["revision"] == revision
                and cached["status"]["epoch"] == self._current_activity_epoch
                and cached.get("metadata_revision") == metadata_revision
                and (
                    cached["valid_until"] is None
                    or now < cached["valid_until"]
                )
            ):
                self._current_owner_snapshots.move_to_end(snapshot_key)
                return cached
            events = tuple(self._current_activity_events.values())
            activity_status = self._current_activity_status(revision)
        cutoff = int(now) - seconds if seconds is not None else None
        retention_at: float | None = None
        grouped: dict[tuple[str | None, str | None], dict[str, Any]] = {}
        pool_cache: dict[str, dict[str, Any] | None] = {}
        for event in events:
            event_timestamp = int(event.get("timestamp") or 0)
            if cutoff is not None and event_timestamp < cutoff:
                continue
            pool = self._current_pool_metadata(event, pool_cache)
            data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
            raw_protocol = str(event.get("protocol") or "").lower()
            event_protocol = str(
                (
                    data.get("manager_protocol")
                    if raw_protocol == "nft" else raw_protocol
                )
                or pool.get("protocol") or raw_protocol
            ).lower()
            if protocol and event_protocol != protocol:
                continue
            event_pool = str(
                event.get("pool_id") or pool.get("id") or ""
            ).lower()
            if pool_id and event_pool != pool_id:
                continue
            pair = self._observer_pair(event, pool)
            owner = self._observer_address(event.get("owner"))
            custody = self._observer_address(event.get("custody"))
            owners = {owner} if owner is not None else set()
            if str(event.get("kind") or "").lower() == "transfer":
                prior_owner = self._observer_address(
                    data.get("prior_owner") or data.get("previous_owner")
                    or data.get("from") or data.get("from_address")
                )
                if prior_owner is not None:
                    owners.add(prior_owner)
            haystack = " ".join(str(value or "").lower() for value in (
                event_pool, pair, pair.replace(" ", ""), event_protocol,
                owner, custody, event.get("tx_hash"), event.get("position_key"),
                event.get("token_id"), event.get("token0"), event.get("token1"),
                pool.get("token0"), pool.get("token1"), *owners,
            ))
            if query and query not in haystack:
                continue
            identities = [
                ((identity, None), "beneficial_owner") for identity in owners
            ]
            if custody is not None:
                identities.append(((None, custody), "custody"))
            if not identities:
                continue
            if seconds is not None:
                expires = float(event_timestamp + seconds + 1)
                retention_at = (
                    expires if retention_at is None else min(retention_at, expires)
                )
            order = (
                int(event.get("block_number") or 0),
                int(event.get("tx_index") or 0),
                int(event.get("log_index") or 0),
            )
            for identity_key, identity_match in identities:
                current = grouped.get(identity_key)
                if current is None:
                    current = {
                        "order": order, "event_count": 0,
                        "owner": identity_key[0], "custody": identity_key[1],
                        "identity_match": identity_match, "event": event, "pair": pair,
                    }
                    grouped[identity_key] = current
                current["event_count"] += 1
                if order >= current["order"]:
                    current.update(
                        order=order, identity_match=identity_match,
                        event=event, pair=pair,
                    )
        rows: list[dict[str, Any]] = []
        for item in grouped.values():
            event = item["event"]
            rows.append({
                "owner": item["owner"], "custody": item["custody"],
                "identity_basis": (
                    "verified_owner" if item["owner"] is not None
                    else "custody_aggregate"
                ),
                "activity": {
                    "block_number": event.get("block_number"),
                    "timestamp": event.get("timestamp"),
                    "pool_id": event.get("pool_id") or (
                        event.get("pool", {}).get("id")
                        if isinstance(event.get("pool"), Mapping) else None
                    ),
                    "pair": item["pair"],
                    "kind": event.get("kind"),
                    "tx_hash": event.get("tx_hash"),
                    "event_count": item["event_count"],
                    "qualification": "provisional_canonical",
                    "identity_match": item["identity_match"],
                },
                "_activity_order": item["order"],
            })
        if query:
            identity_matches = [
                row for row in rows
                if query in str(
                    row.get("owner") or row.get("custody") or ""
                ).lower()
            ]
            if identity_matches:
                rows = identity_matches
        snapshot = {
            "revision": revision,
            "metadata_revision": metadata_revision,
            "valid_until": retention_at,
            "status": activity_status,
            "rows": tuple(rows),
        }
        with self._current_activity_lock:
            if revision == self._current_activity_revision:
                self._current_owner_snapshots[snapshot_key] = snapshot
                self._current_owner_snapshots.move_to_end(snapshot_key)
                while len(self._current_owner_snapshots) > 64:
                    self._current_owner_snapshots.popitem(last=False)
        return snapshot

    def _catalog_source(self):
        if self._catalog_source_cache is not None:
            return self._catalog_source_cache
        universe = getattr(self.market, "universe", None)
        if universe is None:
            return None
        lock = getattr(self.market, "_lock", None)
        if lock is not None:
            with lock:
                universe = self.market.universe
                discovered = tuple(getattr(self.market, "_discovered", {}).values())
                marker = (
                    int(getattr(self.market, "_checkpoint_revision", 0)),
                    len(discovered),
                )
        else:
            discovered = ()
            marker = (0, 0)
        pools = universe.pools + discovered
        counts = dict(universe.counts)
        for pool in discovered:
            counts[pool.kind] = counts.get(pool.kind, 0) + 1
        self._catalog_discovered_ids = {str(pool.id).lower() for pool in discovered}
        self._catalog_discovery_marker = marker
        signature = json.dumps(
            {"format": 3, "census_counts": universe.counts, "sources": universe.sources},
            sort_keys=True, separators=(",", ":"),
        )
        self._catalog_source_cache = (pools, universe.tokens, counts, signature)
        return self._catalog_source_cache
    def _sync_catalog_discovery(self) -> None:
        source = self._catalog_source_cache
        lock = getattr(self.market, "_lock", None)
        if source is None or lock is None:
            return
        with lock:
            discovered = tuple(getattr(self.market, "_discovered", {}).values())
            marker = (
                int(getattr(self.market, "_checkpoint_revision", 0)),
                len(discovered),
            )
            if marker == self._catalog_discovery_marker:
                return
            tokens = self.market.universe.tokens
            additions = tuple(
                pool for pool in discovered
                if str(pool.id).lower() not in self._catalog_discovered_ids
            )
        for offset in range(0, len(additions), 250):
            if self._search_stop.is_set():
                return
            batch = additions[offset:offset + 250]
            self.store.upsert_catalog_pools(batch, tokens)
            self._catalog_discovered_ids.update(str(pool.id).lower() for pool in batch)
            pools, cached_tokens, counts, signature = self._catalog_source_cache
            updated_counts = dict(counts)
            for pool in batch:
                updated_counts[pool.kind] = updated_counts.get(pool.kind, 0) + 1
            self._catalog_source_cache = (pools, cached_tokens, updated_counts, signature)
            self._search_stop.wait(0.02)
        self._catalog_discovery_marker = marker


    def _search_index_run(self):
        while not self._search_stop.is_set():
            try:
                migration = self.store.search_index_status()
                if not migration["ready"]:
                    self.store.build_search_index(self._search_stop)
                    self._search_error = None
                    continue
                source = self._catalog_source()
                if source is not None:
                    pools, tokens, _counts, signature = source
                    catalog = self.store.catalog_search_status(signature)
                    if not catalog["ready"]:
                        self.store.build_catalog_search(
                            self._search_stop, pools, tokens, signature,
                        )
                        self._search_error = None
                        continue
                    self._sync_catalog_discovery()
                self._search_error = None
                self._search_stop.wait(1.0)
            except Exception as exc:
                self._search_error = str(exc)[:500]
                self._search_stop.wait(1.0)

    def close(self):
        self._search_stop.set()
        if self._search_thread is not None:
            self._search_thread.join()
        self.indexer.close()
        self._frame_executor.shutdown(wait=True, cancel_futures=True)
        self.store.close()

    def _load_status(self) -> dict[str, Any]:
        out = dict(self.store.status())
        out.update(self.indexer.runtime_status())
        feed = self.indexer.feed_status()
        if feed.get("observed_head") is not None:
            out["head"] = feed["observed_head"]
            out["head_hash"] = feed.get("observed_head_hash")
            out["head_timestamp"] = feed.get("observed_head_timestamp")
        out.update(feed)
        head, indexed_head = out.get("head"), out.get("indexed_head")
        if isinstance(head, int) and isinstance(indexed_head, int):
            out["lag_blocks"] = max(0, head - indexed_head)
        head_timestamp, history_to = out.get("head_timestamp"), out.get("history_to")
        if isinstance(head_timestamp, (int, float)) and isinstance(history_to, (int, float)):
            out["lag_s"] = max(0, head_timestamp - history_to)
        out.setdefault("chain_id", 4663)
        out.setdefault("state", "warming")
        if out["state"] == "live" and (out.get("lag_s") or 0) > 2:
            out["state"] = "catching_up"
        out.setdefault("revision", 0)
        out.setdefault("events_revision", 0)
        out.setdefault("live_revision", 0)
        out.setdefault("epoch", 0)
        out["pricing_basis"] = PRICING_BASIS
        out["fees_basis"] = "Pool trading fees estimated before unknown protocol/hook cuts; not realized LP returns"
        out["as_of"] = time.time()
        providers = self.indexer.source_status()
        out["providers"] = providers
        trace = providers.get("trace") or {}
        out["source_coverage"] = {
            "head": "independent newHeads subscription with bounded public-RPC gap reconciliation",
            "state": "independent RPC failover with block-pinned reads",
            "logs": "independent RPC failover; bounded log ranges",
            "receipts": "separate receipt/body providers",
            "trace": (
                "available"
                if trace.get("active")
                else (
                    "configured but unavailable; trace-dependent LP accounting remains pending/null"
                    if trace.get("configured")
                    else "unavailable; trace-dependent LP accounting remains pending/null"
                )
            ),
        }
        return out

    def status(self):
        now = time.monotonic()
        change_token = self.store.change_token
        with self._status_cache_lock:
            cached = self._status_cache_value
            if (
                cached is not None
                and self._status_cache_token == change_token
                and now - self._status_cache_at < 0.20
            ):
                return {**cached, "as_of": time.time()}
        # Only a cache miss serializes. Hits never queue on the refresh lock.
        with self._status_refresh_lock:
            now = time.monotonic()
            change_token = self.store.change_token
            with self._status_cache_lock:
                cached = self._status_cache_value
                if (
                    cached is not None
                    and self._status_cache_token == change_token
                    and now - self._status_cache_at < 0.20
                ):
                    return {**cached, "as_of": time.time()}
            out = self._load_status()
            with self._status_cache_lock:
                self._status_cache_at = time.monotonic()
                self._status_cache_value = dict(out)
                self._status_cache_token = change_token
            return out

    def _cached(self, key, loader, ttl=3.0, *, epoch=None):
        now = time.monotonic()
        if epoch is None:
            epoch = self.status()["epoch"]
        key = (epoch, *key)
        leader = False
        future = None
        with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None and now - cached[0] < ttl:
                self._cache.move_to_end(key)
                return cached[1]
            future = self._cache_futures.get(key)
            if future is None and len(self._cache_futures) < 64:
                future = Future()
                self._cache_futures[key] = future
                leader = True
        if future is not None and not leader:
            return future.result()
        try:
            value = loader()
        except BaseException as exc:
            if future is not None:
                with self._cache_lock:
                    if self._cache_futures.get(key) is future:
                        self._cache_futures.pop(key, None)
                future.set_exception(exc)
            raise
        completed_at = time.monotonic()
        with self._cache_lock:
            self._cache[key] = (completed_at, value)
            self._cache.move_to_end(key)
            while len(self._cache) > 64:
                self._cache.popitem(last=False)
            if future is not None and self._cache_futures.get(key) is future:
                self._cache_futures.pop(key, None)
        if future is not None:
            future.set_result(value)
        return value

    def _window(self, params, status=None):
        name = str(params.get("window") or "24h")
        if name not in WINDOWS:
            raise ValueError("window must be 1h, 24h, 7d, 30d or all")
        if status is None:
            status = self.status()
        end = int(status.get("history_to") or time.time())
        start = max(0, end - WINDOWS[name]) if WINDOWS[name] else 0
        floor = status.get("history_from")
        observed = max(start, int(floor or end))
        coverage = {
            "window": name, "requested_from": start or floor, "from": floor, "to": status.get("history_to"),
            "observed_s": max(0, end - observed), "complete": bool(floor is not None and start and int(floor) <= start),
            "basis": "all indexed history" if name == "all" else "canonical indexed interval; partial until backfill completes",
            "pricing_basis": PRICING_BASIS, "bucket_seconds": 60,
        }
        return name, start, end, coverage

    @staticmethod
    def _bucket_clause(start, end):
        # Full hours plus only the boundary minutes: bounded read amplification.
        low_minute, high_minute = start // 60 * 60, end // 60 * 60
        low_hour = (low_minute + 3599) // 3600 * 3600
        high_hour = high_minute // 3600 * 3600
        if low_hour >= high_hour:
            return "resolution=60 AND bucket>=? AND bucket<=?", [low_minute, high_minute]
        return ("((resolution=3600 AND bucket>=? AND bucket<?) OR "
                "(resolution=60 AND ((bucket>=? AND bucket<?) OR (bucket>=? AND bucket<=?))))",
                [low_hour, high_hour, low_minute, low_hour, high_hour, high_minute])

    @staticmethod
    def _filters(params, prefix="p"):
        conditions, values = [], []
        protocol = str(params.get("protocol") or "").lower()
        if protocol:
            if protocol not in ("v2", "v3", "v4"):
                raise ValueError("protocol must be v2, v3 or v4")
            conditions.append(f"{prefix}.protocol=?")
            values.append(protocol)
        query = str(params.get("q") or "").strip().lower()[:128]
        if query:
            escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            term = "%" + escaped + "%"
            columns = ("id", "token0", "token1", "symbol0", "symbol1")
            conditions.append("(" + " OR ".join(f"{prefix}.{column} LIKE ? ESCAPE '\\'" for column in columns) + ")")
            values.extend([term] * len(columns))
        return " AND ".join(conditions) or "1", values

    @staticmethod
    def _page(params, default=50):
        limit = min(150, max(1, int(params.get("limit") or default)))
        offset = min(100_000, max(0, int(params.get("offset") or 0)))
        return limit, offset

    def overview(self, params):
        status = self.status()
        name, start, end, coverage = self._window(params, status)
        def load():
            clause, args = self._bucket_clause(start, end)
            conn = self.store.read()
            row = conn.execute(f"SELECT {_SUM_FIELDS},COUNT(DISTINCT pool_id) AS active_pools "
                               f"FROM lp_pool_buckets WHERE {clause}", args).fetchone()
            totals = {field: row[field] or 0 for field in BUCKET_FIELDS}
            priced_swaps = int(totals["priced_swaps"])
            priced_fees = int(totals["priced_fees"])
            priced_flows = int(totals["priced_flows"])
            return {
                "window": name, "volume_usd": totals["volume_usd"] if priced_swaps else None,
                "fees_usd": totals["fees_usd"] if priced_fees else None,
                "swaps": int(totals["swaps"]), "adds": int(totals["adds"]),
                "removes": int(totals["removes"]), "collects": int(totals["collects"]),
                "active_pools": row["active_pools"],
                "active_owners": self.book.owner_count(name),
                "net_deposits_usd": totals["deposit_usd"] - totals["withdrawal_usd"] if priced_flows else None,
                "coverage": {**coverage, "priced_swaps": priced_swaps, "unpriced_swaps": int(totals["swaps"]) - priced_swaps,
                             "priced_flows": priced_flows, "unpriced_flows": int(totals["flows"]) - priced_flows},
            }
        revision = int(status.get("revision") or 0)
        aggregate = self._cached(
            ("overview", revision, self.book.owners_revision, name),
            load, ttl=3.0, epoch=int(status.get("epoch") or 0),
        )
        return {**aggregate, "status": status}

    def pools(self, params):
        status = self.status()
        name, start, end, coverage = self._window(params, status)
        limit, offset = self._page(params)
        where, filters = self._filters(params)
        sort = str(params.get("sort") or "fees")
        order = str(params.get("order") or "desc").lower()
        if order not in {"asc", "desc"}:
            raise ValueError("order must be asc or desc")
        v2_tvl = (
            "(SELECT CAST(r.reserve0 AS REAL)/POWER(10,p.decimals0)*s.price0_usd+"
            "CAST(r.reserve1 AS REAL)/POWER(10,p.decimals1)*s.price1_usd "
            "FROM lp_v2_reserve_samples r WHERE r.pool_id=p.id "
            "ORDER BY r.block_number DESC,r.tx_index DESC,r.log_index DESC LIMIT 1)"
        )
        v3_tvl = (
            "(SELECT CAST(b.balance0 AS REAL)/POWER(10,p.decimals0)*s.price0_usd+"
            "CAST(b.balance1 AS REAL)/POWER(10,p.decimals1)*s.price1_usd "
            "FROM pool_balances b JOIN blocks k ON k.number=b.block_number "
            "AND k.hash=b.block_hash WHERE b.pool_id=p.id "
            "ORDER BY b.block_number DESC LIMIT 1)"
        )
        ordering_values = {
            "fee": "COALESCE(s.fee_ppm,p.fee_ppm)",
            "tvl": f"CASE WHEN p.protocol='v2' THEN {v2_tvl} WHEN p.protocol='v3' THEN {v3_tvl} END",
            "active_tvl": f"CASE WHEN p.protocol='v2' THEN {v2_tvl} WHEN a.all_history_complete=1 THEN a.observed_active_tvl_usd END",
            "observed_active_tvl": f"CASE WHEN p.protocol='v2' THEN {v2_tvl} ELSE a.observed_active_tvl_usd END",
            "volume": "CASE WHEN t.priced_swaps>0 THEN t.volume_usd END",
            "fees": "CASE WHEN t.priced_fees>0 THEN t.fees_usd END",
            "flow": "CASE WHEN t.priced_flows>0 THEN (t.deposit_usd-t.withdrawal_usd) END",
            "swaps": "COALESCE(t.swaps,0)",
            "adds": "COALESCE(t.adds,0)",
            "removes": "COALESCE(t.removes,0)",
            "lps": "COALESCE(a.lp_count,0)",
            "price": "s.price",
            "created": "p.created_block",
            "new": "p.created_block",
            "activity": "COALESCE(t.events,0)",
        }
        bucket_sort_fields = {
            "volume": ("priced_swaps", "volume_usd"),
            "fees": ("priced_fees", "fees_usd"),
            "flow": ("priced_flows", "deposit_usd", "withdrawal_usd"),
            "swaps": ("swaps",),
            "adds": ("adds",),
            "removes": ("removes",),
            "activity": ("events",),
        }
        row_bucket_fields = (
            "swaps", "adds", "removes", "volume_usd", "fees_usd",
            "deposit_usd", "withdrawal_usd", "priced_swaps", "priced_fees",
            "priced_flows",
        )
        ordering_value = ordering_values.get(sort)
        ordering_args: list[Any] = []
        if sort == "change":
            ordering_value = (
                "CASE WHEN s.price IS NULL THEN NULL ELSE s.price/"
                "(CASE WHEN p.protocol='v2' THEN COALESCE("
                "(SELECT price FROM lp_v2_reserve_samples b WHERE b.pool_id=p.id "
                "AND b.timestamp<=? ORDER BY b.block_number DESC,b.tx_index DESC,b.log_index DESC LIMIT 1),"
                "(SELECT price FROM lp_v2_reserve_samples b WHERE b.pool_id=p.id "
                "AND b.timestamp>=? ORDER BY b.block_number,b.tx_index,b.log_index LIMIT 1)) ELSE COALESCE("
                "(SELECT price FROM lp_price_samples b WHERE b.pool_id=p.id "
                "AND b.timestamp<=? ORDER BY b.block_number DESC,b.tx_index DESC,b.log_index DESC LIMIT 1),"
                "(SELECT price FROM lp_price_samples b WHERE b.pool_id=p.id "
                "AND b.timestamp>=? ORDER BY b.block_number,b.tx_index,b.log_index LIMIT 1)) END)-1 END"
            )
            ordering_args = [start, start, start, start]
        if ordering_value is None:
            raise ValueError(
                "sort must be fee, tvl, active_tvl, observed_active_tvl, volume, "
                "fees, flow, swaps, adds, removes, lps, price, change or created"
            )
        capital_sort = sort in {"active_tvl", "observed_active_tvl", "lps"}
        snapshot_revision = int(status.get("revision") or 0)
        owner_revision = self.book.owners_revision
        metric_revision = (
            snapshot_revision,
            owner_revision if capital_sort else None,
            start,
            end,
        )
        def load():
            clause, args = self._bucket_clause(start, end)
            conn = self.store.read()
            metric_ctes = []
            metric_args: list[Any] = []
            aggregate_fields = bucket_sort_fields.get(sort)
            if aggregate_fields is not None:
                sums = ",".join(
                    f"SUM({field}) AS {field}" for field in aggregate_fields
                )
                metric_ctes.append(
                    f"t AS (SELECT pool_id,{sums} FROM lp_pool_buckets "
                    f"WHERE {clause} GROUP BY pool_id)"
                )
                metric_args.extend(args)
            if sort == "lps":
                metric_ctes.append(
                    "a AS (SELECT pool_id,"
                    "COUNT(DISTINCT CASE "
                    "WHEN owner IS NOT NULL AND owner<>'' THEN owner "
                    "WHEN custody IS NOT NULL AND custody<>'' THEN custody "
                    "END) AS lp_count "
                    "FROM lp_accounting_positions "
                    "WHERE active_episode_id IS NOT NULL GROUP BY pool_id)"
                )
            elif capital_sort:
                history_complete = (
                    ",MIN(ap.history_complete) AS all_history_complete"
                    if sort == "active_tvl" else ""
                )
                metric_ctes.append(
                    "a AS (SELECT ap.pool_id,"
                    "SUM(CASE WHEN ap.tick_lower IS NULL OR ps.tick IS NULL OR "
                    "(ap.tick_lower<=ps.tick AND ps.tick<ap.tick_upper) "
                    "THEN ap.principal_usd END) AS observed_active_tvl_usd"
                    + history_complete
                    + " FROM lp_accounting_positions ap "
                    "LEFT JOIN lp_pool_state ps ON ps.pool_id=ap.pool_id "
                    "WHERE ap.active_episode_id IS NOT NULL GROUP BY ap.pool_id)"
                )
            metric_prefix = (
                "WITH " + ",".join(metric_ctes) + " " if metric_ctes else ""
            )
            metric_joins = (
                "FROM pools p "
                + ("LEFT JOIN t ON t.pool_id=p.id " if aggregate_fields is not None else "")
                + "LEFT JOIN lp_pool_state s ON s.pool_id=p.id "
                + ("LEFT JOIN a ON a.pool_id=p.id " if capital_sort else "")
            )
            def metric_rows():
                return [
                    (str(row["id"]), row["sort_value"])
                    for row in conn.execute(
                        metric_prefix + f"SELECT p.id,{ordering_value} AS sort_value "
                        + metric_joins + f"WHERE {where}",
                        [*metric_args, *ordering_args, *filters],
                    ).fetchall()
                ]
            metrics = self._cached(
                ("pool-sort-base", metric_revision, name, where, *filters, sort),
                metric_rows, ttl=float("inf"),
                epoch=int(status.get("epoch") or 0),
            )
            valued = sorted(
                ((pool_id, value) for pool_id, value in metrics if value is not None),
                key=lambda item: item[0],
            )
            valued.sort(key=lambda item: item[1], reverse=order == "desc")
            nulls = sorted(pool_id for pool_id, value in metrics if value is None)
            ordered_ids = [pool_id for pool_id, _value in valued] + nulls
            total = len(ordered_ids)
            page_ids = ordered_ids[offset:offset + limit]
            if page_ids:
                marks = ",".join("?" for _ in page_ids)
                row_sums = ",".join(
                    f"SUM({field}) AS {field}" for field in row_bucket_fields
                )
                page_totals = (
                    f"WITH t AS (SELECT pool_id,{row_sums} FROM lp_pool_buckets "
                    f"WHERE {clause} AND pool_id IN ({marks}) GROUP BY pool_id) "
                )
                page_joins = (
                    "FROM pools p LEFT JOIN t ON t.pool_id=p.id "
                    "LEFT JOIN lp_pool_state s ON s.pool_id=p.id "
                )
                raw_rows = conn.execute(
                    page_totals
                    + "SELECT p.*,s.price,s.price0_usd,s.price1_usd,"
                    "s.timestamp AS last_event_at,s.liquidity AS active_liquidity,"
                    "s.fee_ppm AS current_fee,"
                    + ",".join(f"t.{field}" for field in row_bucket_fields)
                    + " " + page_joins + f"WHERE p.id IN ({marks})",
                    [*args, *page_ids, *page_ids],
                ).fetchall()
                by_id = {str(row["id"]): row for row in raw_rows}
                rows = [by_id[pool_id] for pool_id in page_ids if pool_id in by_id]
            else:
                rows = []
            capital = self.book.pool_stats([row["id"] for row in rows])
            baseline_row = conn.execute(
                "SELECT block_number FROM events WHERE timestamp<=? "
                "ORDER BY timestamp DESC,block_number DESC,tx_index DESC,log_index DESC LIMIT 1",
                (start,),
            ).fetchone()
            baseline_block = int(baseline_row[0]) if baseline_row is not None else None
            result = []
            for row in rows:
                row = dict(row)
                for field in row_bucket_fields:
                    row[field] = row.get(field) or 0
                stats = capital.get(row["id"], {})
                risks = []
                if not coverage["complete"]:
                    risks.append("partial window history")
                if row["priced_swaps"] < row["swaps"]:
                    risks.append("unpriced swaps")
                if row.get("hook") and row["hook"] != NATIVE:
                    risks.append("hook behavior requires review")
                if row["protocol"] == "v4":
                    risks.append("singleton pool; manager balance is not TVL")
                if row.get("factory") not in (UNISWAP_V3_FACTORY,) and row["protocol"] != "v4":
                    risks.append("non-canonical factory; verify token and fee semantics")
                complete_inventory = bool(stats.get("complete_inventory"))
                if row["protocol"] == "v2":
                    reserve = conn.execute(
                        "SELECT reserve0,reserve1 FROM lp_v2_reserve_samples WHERE pool_id=? "
                        "ORDER BY block_number DESC,tx_index DESC,log_index DESC LIMIT 1", (row["id"],),
                    ).fetchone()
                    if reserve is not None:
                        tvl = _basket(reserve["reserve0"], reserve["reserve1"], row, row["price0_usd"], row["price1_usd"])
                        complete_inventory = tvl is not None
                        stats = {**stats, "observed_principal_usd": tvl, "observed_active_tvl_usd": tvl}
                tvl = stats.get("observed_principal_usd") if complete_inventory else None
                tvl_basis = ("canonical V2 Sync reserves" if row["protocol"] == "v2"
                             else "complete indexed position principal; fees excluded") if tvl is not None else None
                tvl_block = None
                if row["protocol"] == "v3":
                    balance = conn.execute(
                        "SELECT b.*,k.timestamp FROM pool_balances b JOIN blocks k "
                        "ON k.number=b.block_number AND k.hash=b.block_hash WHERE b.pool_id=? "
                        "ORDER BY b.block_number DESC LIMIT 1", (row["id"],),
                    ).fetchone()
                    if balance is not None:
                        sample = conn.execute(
                            "SELECT price FROM lp_price_samples WHERE pool_id=? AND block_number<=? "
                            "ORDER BY block_number DESC,tx_index DESC,log_index DESC LIMIT 1",
                            (row["id"], balance["block_number"]),
                        ).fetchone()
                        mark = {"block_number": balance["block_number"], "tx_index": 2**63 - 1,
                                "log_index": 2**63 - 1, "timestamp": balance["timestamp"]}
                        price0, price1, _ = self.prices._quote(conn, row, mark, sample["price"] if sample else None)
                        value = _basket(balance["balance0"], balance["balance1"], row, price0, price1)
                        if value is not None:
                            tvl, tvl_block = value, balance["block_number"]
                            tvl_basis = "canonical block-pinned pool token balances; includes unsettled fees"
                sample_table = "lp_v2_reserve_samples" if row["protocol"] == "v2" else "lp_price_samples"
                if baseline_block is None:
                    baseline = conn.execute(
                        f"SELECT price,timestamp FROM {sample_table} WHERE pool_id=? "
                        "ORDER BY block_number,tx_index,log_index LIMIT 1", (row["id"],),
                    ).fetchone()
                else:
                    baseline = conn.execute(
                        f"SELECT price,timestamp FROM {sample_table} WHERE pool_id=? AND block_number<=? "
                        "ORDER BY block_number DESC,tx_index DESC,log_index DESC LIMIT 1",
                        (row["id"], baseline_block),
                    ).fetchone()
                    if baseline is None:
                        baseline = conn.execute(
                            f"SELECT price,timestamp FROM {sample_table} WHERE pool_id=? AND block_number>=? "
                            "ORDER BY block_number,tx_index,log_index LIMIT 1",
                            (row["id"], baseline_block),
                        ).fetchone()
                change = None
                if baseline and baseline["price"] and row.get("price") is not None:
                    change = _number((row["price"] / baseline["price"] - 1) * 100)
                result.append({
                    "id": row["id"], "pair": self._pair(row), "protocol": row["protocol"],
                    "token0": self._token(row, 0), "token1": self._token(row, 1),
                    "fee_ppm": row.get("current_fee") if row.get("current_fee") is not None else row.get("fee_ppm"),
                    "tvl_usd": tvl, "tvl_basis": tvl_basis, "tvl_block": tvl_block,
                    "active_tvl_usd": stats.get("observed_active_tvl_usd") if complete_inventory else None,
                    "observed_active_tvl_usd": stats.get("observed_active_tvl_usd"),
                    "observed_principal_usd": stats.get("observed_principal_usd"),
                    "active_liquidity": row.get("active_liquidity"),
                    "volume_usd": row["volume_usd"] if row["priced_swaps"] else None,
                    "fees_usd": row["fees_usd"] if row["priced_fees"] else None,
                    "swaps": int(row["swaps"]), "adds": int(row["adds"]), "removes": int(row["removes"]),
                    "net_deposits_usd": row["deposit_usd"] - row["withdrawal_usd"] if row["priced_flows"] else None,
                    "lp_count": stats.get("lp_count"), "price": row.get("price"), "price_change_pct": change,
                    "price_change_from": baseline["timestamp"] if baseline else None,
                    "created_at": self._created_at(conn, row), "created_block": row.get("created_block"),
                    "last_event_at": row.get("last_event_at"), "risks": risks,

                    "coverage": {**coverage, "inventory_complete": complete_inventory,
                                 "priced_swaps": int(row["priced_swaps"]), "swaps": int(row["swaps"]),
                                 "fees_basis": "gross trading-fee estimate, not LP net earnings"},
                })
            return {
                "rows": result, "total": total, "window": name,
                "sort": sort, "order": order, "coverage": coverage,
                "revision": snapshot_revision,
                "epoch": int(status.get("epoch") or 0),
                "as_of": status["as_of"],
            }
        return self._cached(
            (
                "pools", snapshot_revision, owner_revision, name, where,
                *filters, sort, order, limit, offset,
            ),
            load, ttl=3.0, epoch=int(status.get("epoch") or 0),
        )

    @staticmethod
    def _created_at(conn, row):
        if row.get("created_block") is None:
            return None
        block = conn.execute("SELECT timestamp FROM blocks WHERE number=?", (row["created_block"],)).fetchone()
        return block[0] if block else None

    @staticmethod
    def _token(row, side):
        address = row.get(f"token{side}")
        symbol = row.get(f"symbol{side}")
        decimals = row.get(f"decimals{side}")
        if symbol is not None and decimals is not None:
            metadata_state = "complete"
        elif symbol is not None or decimals is not None:
            metadata_state = "partial"
        else:
            metadata_state = "unavailable"
        return {
            "address": address,
            "symbol": symbol,
            "decimals": decimals,
            "metadata_state": metadata_state,
        }

    @staticmethod
    def _pair(row):
        token0, token1 = row.get("token0"), row.get("token1")
        if token0 is None or token1 is None:
            return str(row.get("pool_id") or row.get("id") or "unresolved pool")
        return f"{row.get('symbol0') or token0} / {row.get('symbol1') or token1}"
    def search(self, params):
        query = str(params.get("q") or "").strip()[:128]
        try:
            limit = min(30, max(1, int(params.get("limit") or 30)))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer from 1 to 30") from exc
        rows, total = self.store.search(query, 30)
        source = self._catalog_source()
        signature = source[3] if source is not None else None
        catalog_rows, catalog_total = self.store.search_catalog(query, 30)
        catalog = self.store.catalog_search_status(signature)
        combined = []
        seen = set()
        for position, row in enumerate([*rows, *catalog_rows]):
            key = (str(row.get("kind") or ""), str(row.get("id") or ""))
            if key in seen:
                continue
            seen.add(key)
            combined.append((position, row))
        duplicate_count = len(rows) + len(catalog_rows) - len(seen)
        exact = query.strip().lower()
        label_shape = "".join(character for character in exact if character.isalnum())
        kind_rank = {
            "pool": 0, "transaction": 0, "token": 1, "position": 2,
            "protocol": 3, "owner": 4, "custody": 5,
        }
        combined.sort(key=lambda item: (
            str(item[1].get("id") or "").lower() != exact,
            "".join(character for character in str(item[1].get("label") or "").lower()
                    if character.isalnum()) != label_shape,
            kind_rank.get(str(item[1].get("kind") or ""), 9),
            item[0],
        ))
        status = self.status()
        migration = self.store.search_index_status()
        catalog_ready = catalog["ready"] if source is not None else True
        return {
            "rows": [row for _position, row in combined[:limit]],
            "total": max(len(seen), total + catalog_total - duplicate_count),
            "coverage": {
                "basis": "canonical durable LP index plus verified workbench census",
                "indexed_only": True,
                "complete": False,
                "state": "ready" if migration["ready"] and catalog_ready else "warming",
                "phase": migration["phase"] if not migration["ready"] else catalog["state"],
                "ready": migration["ready"] and catalog_ready,
                "indexed_through_event": migration["indexed_through_event"],
                "events_total": migration["events_total"],
                "catalog_pools_indexed": catalog["indexed_pools"],
                "catalog_pools_total": sum(source[2].values()) if source is not None else 0,
                "error": self._search_error,
                "categories": [
                    "pools", "tokens", "protocols", "owners", "custodies",
                    "transactions", "positions",
                ],
                "indexed_head": status.get("indexed_head"),
                "history_from": status.get("history_from"),
                "history_to": status.get("history_to"),
                "caveat": "Only verified catalog or durably indexed chain entities are returned",
            },
        }

    def _current_event_view(
            self, raw: Mapping[str, Any],
            pool_cache: dict[str, dict[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        event = dict(raw)
        pool = self._current_pool_metadata(event, pool_cache)
        if pool:
            event = self.prices.value_current(event, pool)
        event.pop("pool", None)
        if pool:
            event["pool_fee_ppm"], event["pool_fee_basis"] = _static_pool_fee(pool)
            for key in (
                "pool_id", "protocol", "token0", "token1", "symbol0", "symbol1",
                "decimals0", "decimals1",
            ):
                source_key = "id" if key == "pool_id" else key
                if event.get(key) is None and pool.get(source_key) is not None:
                    event[key] = pool[source_key]
        event["pair"] = self._pair(event)
        event["token0"], event["token1"] = self._token(event, 0), self._token(event, 1)
        event["data"] = {
            key: value for key, value in _data(event.get("data")).items()
            if (value is None or isinstance(value, (str, int, float, bool)))
            and (not isinstance(value, str) or len(value) <= 512)
        }
        cashflows = (event.get("cashflow0"), event.get("cashflow1"))
        amounts = (event.get("amount0"), event.get("amount1"))
        if event.get("flow_scope") == "transaction":
            event.setdefault("flow_complete", False)
            event.setdefault(
                "flow_qualification",
                "verified_receipt_transfer_logs_not_position_attributed",
            )
        elif all(value is not None for value in cashflows):
            event["flow_scope"] = "position_event"
            event["flow_complete"] = True
            event["flow_qualification"] = (
                event.get("accounting_basis") or "event_exact"
            )
        elif all(value is not None for value in amounts):
            event["flow_scope"] = "pool_event"
            event["flow_complete"] = True
            event["flow_qualification"] = (
                event.get("accounting_basis") or "pool_event_amounts"
            )
        else:
            event["flow_scope"] = "unknown"
            event["flow_complete"] = False
            event["flow_qualification"] = (
                event["data"].get("cashflow_basis")
                or event.get("accounting_basis")
                or "token_amounts_not_emitted_by_event"
            )
        deposit = event.get("deposit_usd")
        withdrawal = event.get("withdrawal_usd")
        event["size_usd"] = (
            deposit + withdrawal
            if deposit is not None and withdrawal is not None
            else event.get("volume_usd")
        )
        event["id"] = "current:" + ":".join((
            str(event.get("block_hash") or ""), str(event.get("tx_hash") or ""),
            str(event.get("log_index") or 0),
        ))
        event["lane"] = "current"
        event["qualification"] = "provisional_canonical"
        return event

    def _current_feed_rows(
            self, item: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        data = item.get("data") if isinstance(item.get("data"), Mapping) else {}
        key = (
            str(data.get("feed_epoch") or ""),
            int(item.get("sequence") or data.get("sequence") or 0),
            self._pool_metadata_revision(),
        )
        slot = key[1] % len(self._current_view_cache)
        entry = self._current_view_cache[slot]
        if entry is not None and entry[0] == key:
            return entry[1]
        with self._current_view_lock:
            entry = self._current_view_cache[slot]
            if entry is not None and entry[0] == key:
                return entry[1]
            pool_cache: dict[str, dict[str, Any] | None] = {}
            try:
                cached = tuple(
                    self._current_event_view(row, pool_cache)
                    for row in data.get("rows") or ()
                )
            finally:
                # A cache miss may consult durable price marks. The connection
                # must not then live for the lifetime of an SSE worker thread.
                self.store.close_reader()
            self._current_view_cache[slot] = (key, cached)
            return cached

    def _current_event_matches(self, event: Mapping[str, Any], params) -> bool:
        kind = str(params.get("kind") or "lp")
        if kind == "lp" and not (
            event.get("kind") in {"add", "remove", "collect"}
            or event.get("kind") == "checkpoint" and event.get("position_key")
        ):
            return False
        if kind not in {"lp", "all"}:
            raise ValueError("kind must be lp or all")
        protocol = str(params.get("protocol") or "").lower()
        if protocol and str(event.get("protocol") or "").lower() != protocol:
            return False
        pool = str(params.get("pool") or "").lower()
        if pool and str(event.get("pool_id") or "").lower() != pool:
            return False
        owner = str(params.get("owner") or "").lower()
        if owner and owner not in {
            str(event.get("owner") or "").lower(),
            str(event.get("custody") or "").lower(),
        }:
            return False
        query = str(params.get("q") or "").strip().lower()[:128]
        if query:
            haystack = " ".join(str(event.get(key) or "").lower() for key in (
                "pool_id", "pair", "protocol", "owner", "custody", "tx_hash",
                "position_key", "token_id",
            ))
            if query not in haystack:
                return False
        return True

    def stream_updates(self, params, after=0, feed_epoch=None):
        updates = self.indexer.feed_updates(after, feed_epoch)
        channel = str(params.get("channel") or "both").lower()
        if channel not in {"both", "heads", "activity"}:
            raise ValueError("channel must be heads or activity")
        output = []
        for item in updates["events"]:
            event_name = item.get("event")
            if channel == "heads" and event_name == "activity":
                continue
            if channel == "activity" and event_name == "block":
                continue
            if item.get("event") != "activity":
                output.append(item)
                continue
            data = dict(item["data"])
            rows = self._current_feed_rows(item)
            rows = [row for row in rows if self._current_event_matches(row, params)]
            if rows:
                data["rows"] = rows
                output.append({**item, "data": data})
        return {**updates, "events": output}

    def wait_stream(self, after, timeout=1.0):
        return self.indexer.wait_feed(after, timeout)

    def tape(self, params, *, _status=None):
        status = _status if _status is not None else self.status()
        name, start, end, coverage = self._window(params, status)
        limit, _ = self._page(params, 100)
        where, args = self._filters({**params, "q": ""})
        conditions = [where]
        query = str(params.get("q") or "").strip().lower()[:128]
        if query:
            pool_where, pool_args = self._filters({"q": query})
            term = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            columns = ("owner", "custody", "tx_hash", "position_key", "token_id")
            event_where = " OR ".join(f"e.{column} LIKE ? ESCAPE '\\'" for column in columns)
            conditions.append(f"({pool_where} OR {event_where})")
            args.extend([*pool_args, *([term] * len(columns))])
        kind = str(params.get("kind") or "lp")
        if kind == "lp":
            conditions.append("(e.kind IN ('add','remove','collect') OR "
                              "(e.kind='checkpoint' AND e.position_key IS NOT NULL))")
        elif kind != "all":
            raise ValueError("kind must be lp or all")
        for key, column in (("pool", "e.pool_id"), ("owner", "COALESCE(e.owner,e.custody)")):
            if params.get(key):
                conditions.append(f"{column}=?")
                args.append(str(params[key]).lower()[:66])
        before = params.get("before")
        if before:
            # Cursor identifies a row, but chronological order is block/tx/log.
            # A backfilled high insertion id must never jump ahead of live data.
            row = self.store.read().execute("SELECT block_number,tx_index,log_index FROM events WHERE id=?", (int(before),)).fetchone()
            if row is None:
                raise ValueError("tape cursor is no longer canonical; reload the snapshot")
            conditions.append("(e.block_number,e.tx_index,e.log_index)<(?,?,?)")
            args.extend(row)
        snapshot_revision = int(status.get("revision") or 0)
        snapshot_events_revision = int(status.get("events_revision") or 0)
        snapshot_pool_metadata_token = self.store.pool_metadata_token
        snapshot_epoch = int(status.get("epoch") or 0)
        # Only an exact pool predicate has an index that also satisfies the
        # canonical feed order. Leading-wildcard search, protocol, and
        # COALESCE(owner,custody) predicates do not. Letting SQLite choose an
        # index for those predicates can sort every match before LIMIT.
        event_source = "events e INDEXED BY events_block_idx"
        if params.get("pool"):
            event_source = "events e INDEXED BY lp_events_pool_order"
        cache_args = tuple(args)
        window_floor = None
        if start:
            # Event timestamps come from canonical block headers and increase
            # with block order. Resolve the moving wall-clock boundary to an
            # event block so repeated reads share a stable cache key until an
            # event actually enters or leaves the window. Keep the timestamp
            # predicate as an independent guard on the requested boundary.
            floor = self.store.read().execute(
                "SELECT block_number FROM events INDEXED BY lp_events_time "
                "WHERE timestamp>=? ORDER BY timestamp,block_number LIMIT 1",
                (start,),
            ).fetchone()
            conditions.append("e.timestamp>=?")
            args.append(start)
            if floor is None:
                conditions.append("0")
            else:
                window_floor = int(floor["block_number"])
                conditions.append("e.block_number>=?")
                args.append(window_floor)

        def load():
            rows = self.store.read().execute(
                "SELECT e.*,p.token0,p.token1,p.symbol0,p.symbol1,p.decimals0,p.decimals1,"
                "p.protocol AS pool_protocol,p.fee_ppm AS pool_fee_raw "
                f"FROM {event_source} LEFT JOIN pools p ON p.id=e.pool_id WHERE "
                + " AND ".join(conditions)
                + " ORDER BY e.block_number DESC,e.tx_index DESC,e.log_index DESC LIMIT ?",
                [*args, limit],
            ).fetchall()
            output = []
            for raw in rows:
                event = dict(raw)
                event["pool_fee_ppm"], event["pool_fee_basis"] = _static_pool_fee({
                    "protocol": event.pop("pool_protocol", event.get("protocol")),
                    "fee_ppm": event.pop("pool_fee_raw", None),
                })
                event["pair"] = self._pair(event)
                event["token0"], event["token1"] = self._token(event, 0), self._token(event, 1)
                event["data"] = {
                    key: value for key, value in _data(event["data"]).items()
                    if value is None or isinstance(value, (str, int, float, bool))
                }
                for key in ("fees_scope", "fees_qualification", "fees_basis"):
                    if event.get(key) is None and event["data"].get(key) is not None:
                        event[key] = event["data"][key]
                event["size_usd"] = (
                    event["deposit_usd"] + event["withdrawal_usd"]
                    if event["deposit_usd"] is not None and event["withdrawal_usd"] is not None
                    else event["volume_usd"]
                )
                output.append(event)
            return {"rows": output, "cursor": output[-1]["id"] if output else None}

        # Durable event and pool-metadata versions identify when these rows can
        # change. The response still carries the current revision and coverage.
        materialized = self._cached(
            (
                "tape", snapshot_events_revision, snapshot_pool_metadata_token,
                name, *conditions, *cache_args, window_floor, limit,
            ),
            load, ttl=float("inf"), epoch=snapshot_epoch,
        )
        return {
            **materialized, "coverage": coverage, "revision": snapshot_revision,
            "epoch": snapshot_epoch,
        }

    @staticmethod
    def _owners_result_sort(
            rows: list[dict[str, Any]], sort_key: str,
    ) -> None:
        field = {
            "fees": "fees_usd", "gross": "gross_pnl_usd",
            "gas": "gas_usd", "win": "win_rate", "volume": "volume_usd",
            "activity": "_activity_at",
        }.get(sort_key, "net_pnl_usd")
        rows.sort(
            key=lambda row: (
                row.get(field) is not None,
                row.get(field) if row.get(field) is not None else -math.inf,
                row.get("_activity_order") or (0, 0, 0),
                row.get("owner") or row.get("custody") or "",
            ),
            reverse=True,
        )

    @staticmethod
    def _current_only_owner(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "owner": row.get("owner"), "custody": row.get("custody"),
            "identity_basis": row.get("identity_basis"),
            "positions": None, "open_positions": None, "closed_episodes": None,
            "fees_usd": None, "observed_collected_fees_usd": None,
            "volume_usd": None, "gross_pnl_usd": None,
            "gas_usd": None, "net_pnl_usd": None, "win_rate": None,
            "coverage": {
                "qualified": False, "cost_qualified": False,
                "complete_episodes": None, "episodes": None,
                "reasons": [
                    "current_activity_only", "durable_accounting_pending",
                ],
            },
            "activity": dict(row["activity"]),
            "_activity_at": row["activity"].get("timestamp"),
            "_activity_order": row.get("_activity_order") or (0, 0, 0),
        }

    @staticmethod
    def _owner_projection_params(params: Mapping[str, Any]) -> dict[str, Any]:
        window = str(params.get("window") or "24h").lower()
        if window not in WINDOWS:
            raise ValueError("window must be 1h, 24h, 7d, 30d or all")
        protocol = str(params.get("protocol") or "").lower()
        if protocol and protocol not in {"v2", "v3", "v4"}:
            raise ValueError("protocol must be v2, v3 or v4")
        identity_scope = str(params.get("identity_scope") or "all").lower()
        if identity_scope not in {"all", "wallets", "custody"}:
            raise ValueError("identity_scope must be all, wallets or custody")
        raw_limit = params.get("owner_limit", params.get("limit", 50))
        raw_sort = params.get("owner_sort", params.get("sort", "net_pnl"))
        try:
            limit = max(1, min(200, int(raw_limit or 50)))
            offset = max(0, int(params.get("offset") or 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("owner limit and offset must be integers") from exc
        return {
            "window": window,
            "protocol": protocol,
            "identity_scope": identity_scope,
            "pool": str(
                params.get("pool") or params.get("pool_id") or ""
            ).lower(),
            "q": str(params.get("q") or "").strip().lower()[:128],
            "sort": str(raw_sort or "net_pnl").lower(),
            "limit": limit,
            "offset": offset,
        }



    def _owners_result(
            self, params: Mapping[str, Any],
    ) -> tuple[tuple[int, int, int, int], float | None, dict[str, Any]]:
        candidates = self.book.owner_candidates(params)
        current = self._current_owner_snapshot(params)
        rows = candidates["rows"]
        by_identity: dict[
            tuple[str | None, str | None], list[int]
        ] = {}
        for index, row in enumerate(rows):
            by_identity.setdefault(
                (row.get("owner"), row.get("custody")), [],
            ).append(index)
        for current_row in current["rows"]:
            identity = (
                current_row.get("owner"), current_row.get("custody"),
            )
            matches = by_identity.get(identity)
            if not matches:
                added = self._current_only_owner(current_row)
                rows.append(added)
                by_identity[identity] = [len(rows) - 1]
                continue
            current_at = int(current_row["activity"].get("timestamp") or 0)
            for index in matches:
                row = rows[index]
                durable_at = int(row.get("_activity_at") or 0)
                if current_at >= durable_at:
                    rows[index] = {
                        **row,
                        "activity": dict(current_row["activity"]),
                        "_activity_at": current_at,
                        "_activity_order": current_row.get("_activity_order"),
                    }
        identity_scope = params.get("identity_scope", "all")
        if identity_scope == "wallets":
            rows = [row for row in rows if row.get("owner")]
        elif identity_scope == "custody":
            rows = [row for row in rows if not row.get("owner") and row.get("custody")]
        query = str(params.get("q") or "").strip().lower()[:128]
        if query:
            identity_matches = [
                row for row in rows
                if query in str(
                    row.get("owner") or row.get("custody") or ""
                ).lower()
            ]
            if identity_matches:
                rows = identity_matches
        self._owners_result_sort(
            rows, str(params.get("sort") or "net_pnl").lower(),
        )
        limit = int(params["limit"])
        offset = int(params["offset"])
        selected = self.book.decorate_owner_activity(
            rows[offset:offset + limit], params,
        )
        accounting_as_of = candidates.get("accounting_as_of")
        for row in selected:
            if not isinstance(row.get("activity"), Mapping):
                row["activity"] = {
                    "block_number": None, "timestamp": row.get("_activity_at"),
                    "pool_id": None, "pair": None, "kind": None,
                    "tx_hash": None, "event_count": 1,
                    "qualification": "durable_canonical_index",
                }
            current_at = int(row["activity"].get("timestamp") or 0)
            pending = (
                row["activity"].get("qualification") == "provisional_canonical"
                and (
                    accounting_as_of is None
                    or current_at > int(accounting_as_of)
                )
            )
            row["coverage"] = {
                **row["coverage"],
                "reasons": list(row["coverage"].get("reasons") or ()),
                "post_accounting_activity": (
                    "pending" if pending else "accounted"
                ),
            }
            if pending and "post_accounting_activity_pending" not in row["coverage"]["reasons"]:
                row["coverage"]["reasons"].append(
                    "post_accounting_activity_pending"
                )
            row["financials"] = {
                "qualification": "durable_canonical_accounting",
                "as_of": accounting_as_of,
                "current_activity_included": False,
                "pending": pending,
            }
            row.pop("_activity_at", None)
            row.pop("_activity_order", None)
        activity_status = current["status"]
        coverage = dict(candidates["coverage"])
        indexed_head = coverage.get("indexed_head")
        if indexed_head is not None and int(activity_status.get("head") or 0) > int(indexed_head):
            coverage["state"] = "catching_up"
        coverage.update({
            "rows": len(selected),
            "qualified_rows": sum(
                bool(row["coverage"]["qualified"]) for row in selected
            ),
            "accounting_as_of": accounting_as_of,
            "current_activity": activity_status,
            "financials": {
                "qualification": "durable_canonical_accounting",
                "as_of": accounting_as_of,
                "current_stream_combined": False,
                "pending_rows": sum(
                    bool(row["financials"]["pending"]) for row in selected
                ),
            },
        })
        envelope = {
            "rows": selected, "total": len(rows), "coverage": coverage,
            "current_activity": activity_status,
            "accounting_as_of": accounting_as_of,
            "financial_revision": int(candidates["accounting_revision"]),
        }
        valid_until = min(
            (
                float(value) for value in (
                    current.get("valid_until"), candidates.get("valid_until"),
                )
                if value is not None
            ),
            default=None,
        )
        version = (
            int(candidates["accounting_revision"]), int(current["revision"]),
            int(activity_status["epoch"]), int(coverage.get("epoch") or 0),
        )
        return version, valid_until, envelope

    @staticmethod
    def _owner_view_key(params: Mapping[str, Any]) -> tuple[Any, ...]:
        return tuple(params.items())

    def _owner_target_revision(self) -> tuple[int, int, int, int]:
        financial_revision = self.book.owners_revision
        with self._current_activity_lock:
            activity_revision = self._current_activity_revision
            activity_epoch = self._current_activity_epoch
        durable_epoch = self.store.cursor_state("live")[1]
        return int(financial_revision), int(activity_revision), int(activity_epoch), durable_epoch

    def _fresh_owner_envelope(
            self, envelope: Mapping[str, Any],
    ) -> dict[str, Any]:
        current_activity = self._current_activity_status()
        captured = envelope["current_activity"]
        if (
            current_activity["revision"] != captured["revision"]
            or current_activity["epoch"] != captured["epoch"]
        ):
            return dict(envelope)
        coverage = {
            **envelope["coverage"],
            "current_activity": current_activity,
        }
        return {
            **envelope,
            "coverage": coverage,
            "current_activity": current_activity,
        }


    def _owner_projection(
            self, raw_params: Mapping[str, Any], *, wait: bool,
    ) -> dict[str, Any] | None:
        params = self._owner_projection_params(raw_params)
        view_key = self._owner_view_key(params)
        future_key = ("owners", view_key)
        while True:
            target = self._owner_target_revision()
            now = time.monotonic()
            wall_now = time.time()
            ready: tuple[
                tuple[int, int, int, int], float | None, dict[str, Any]
            ] | None = None
            pending: Future | None = None
            delay = 0.0
            failure: BaseException | None = None
            with self._frame_lock:
                pending = self._frame_futures.get(future_key)
                if pending is not None and pending.done():
                    self._frame_futures.pop(future_key, None)
                    try:
                        version, valid_until, envelope = pending.result()
                    except BaseException as exc:
                        failure = exc
                    else:
                        self._owner_result_revision += 1
                        envelope = {
                            **envelope,
                            "revision": self._owner_result_revision,
                        }
                        ready = (version, valid_until, envelope)
                        self._owner_results[view_key] = ready
                        self._owner_results.move_to_end(view_key)
                        self._owner_last_started[view_key] = time.monotonic()
                        while len(self._owner_results) > 64:
                            stale_view, _ = self._owner_results.popitem(last=False)
                            if ("owners", stale_view) not in self._frame_futures:
                                self._owner_last_started.pop(stale_view, None)
                        pending = None
                if failure is None:
                    cached = self._owner_results.get(view_key)
                    if ready is None and cached is not None:
                        version, valid_until, envelope = cached
                        if (
                            version == target
                            and (
                                valid_until is None
                                or wall_now < valid_until
                            )
                        ):
                            self._owner_results.move_to_end(view_key)
                            ready = cached
                        else:
                            ready = None
                    if ready is None and pending is None:
                        earliest = self._owner_last_started.get(view_key, 0.0) + 1.0
                        delay = max(0.0, earliest - now)
                        if delay == 0.0:
                            pending = self._frame_executor.submit(
                                self._owners_result, dict(params),
                            )
                            self._frame_futures[future_key] = pending
                            self._owner_last_started[view_key] = now
            if failure is not None:
                raise failure
            if ready is not None:
                version, _valid_until, envelope = ready
                # Continuous appends cannot starve completed, qualified snapshots.
                # A changed canonical branch still invalidates the whole result.
                if version[2:] != self._owner_target_revision()[2:]:
                    with self._frame_lock:
                        if self._owner_results.get(view_key) is ready:
                            self._owner_results.pop(view_key, None)
                    if not wait:
                        return None
                    continue
                fresh = self._fresh_owner_envelope(envelope)
                return fresh
            if not wait:
                return None
            if pending is not None:
                try:
                    pending.result()
                except BaseException:
                    with self._frame_lock:
                        if self._frame_futures.get(future_key) is pending:
                            self._frame_futures.pop(future_key, None)
                    raise
                continue
            if delay:
                time.sleep(delay)

    def owners(self, params):
        result = self._owner_projection(params, wait=True)
        assert result is not None
        return result

    def poll_owners(self, params, after_revision=None):
        result = self._owner_projection(params, wait=False)
        if result is None:
            return None
        current = result.get("current_activity") or {}
        result_cursor = (
            int(result["revision"]), int(current.get("epoch") or 0),
        )
        if isinstance(after_revision, tuple):
            if after_revision == result_cursor:
                return None
        elif (
            after_revision is not None
            and int(after_revision) == result_cursor[0]
        ):
            return None
        return result

    def closed(self, params):
        self._window(params)
        return self._cached(
            ("closed", *sorted(params.items())),
            lambda: self.book.closed(params), ttl=15.0,
        )

    def owner(self, params):
        owner = str(params.get("owner") or "").lower()
        if len(owner) != 42 or not owner.startswith("0x"):
            raise ValueError("owner must be a 20-byte address")
        try:
            int(owner[2:], 16)
        except ValueError as exc:
            raise ValueError("owner must be a hexadecimal address") from exc
        return self._cached(
            ("owner", *sorted(params.items())),
            lambda: self.book.owner(owner, params), ttl=5.0,
        )

    def poll_frame(self, params, after, epoch):
        current_only = str(params.get("current_only") or "").lower() in {
            "1", "true", "yes",
        }
        if (
            current_only
            or str(params.get("channel") or "").lower() == "heads"
            or str(params.get("view") or "").lower() == "terminal"
        ):
            return None
        status = self.status()
        revision = int(status.get("revision") or 0)
        snapshot_epoch = int(status.get("epoch") or 0)
        if int(after) == revision and int(epoch) == snapshot_epoch:
            return None

        # Transport cursors and channel selection do not change durable tape.
        names = ("window", "protocol", "q", "kind", "pool", "owner", "before")
        params_key = tuple(
            (name, str(params[name])) for name in names if name in params
        )
        key = ("frame", params_key, snapshot_epoch, revision)
        with self._frame_lock:
            stale = [
                candidate for candidate, pending in self._frame_futures.items()
                if (
                    len(candidate) == 4 and candidate[0] == "frame"
                    and candidate[1] == params_key and candidate != key
                    and pending.done()
                )
            ]
            for candidate in stale:
                self._frame_futures.pop(candidate, None)
            future = self._frame_futures.get(key)
            if future is None:
                future = self._frame_executor.submit(
                    self.frame, dict(params), int(after), int(epoch),
                    _status=status,
                )
                self._frame_futures[key] = future
                return None
            if not future.done():
                return None
        try:
            result = dict(future.result())
        except BaseException:
            with self._frame_lock:
                if self._frame_futures.get(key) is future:
                    self._frame_futures.pop(key, None)
            raise
        result["reset"] = (
            int(epoch) != int(result["epoch"])
            or int(after) > int(result["revision"])
        )
        return result

    def frame(self, params, after, epoch, *, _status=None):
        # Bounded canonical snapshots deliberately include enrichment changes.
        # The browser patches keyed rows; it does not rebuild the table.
        status = _status if _status is not None else self.status()
        tape = self.tape({**params, "limit": 150}, _status=status)
        revision = int(status.get("revision") or 0)
        return {"type": "tape", "revision": revision, "epoch": status["epoch"],
                "rows": tape["rows"], "status": status, "coverage": tape["coverage"],
                "reset": epoch != status["epoch"] or after > revision, "snapshot": True,
                "qualification": "durable_canonical_index"}
