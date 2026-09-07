"""Protocol-neutral concentrated-liquidity allocation and persisted-swap replay.

The token math is the exact floor-rounded V3/V4 liquidity geometry used by
the public integer-math module. Historical replay uses bounded persisted Swap rows and
only exposes LP-fee/net counterfactuals when observed protocol cuts or policy,
constant competing liquidity, indexed coverage, and user cost inputs make
those figures defensible.
"""
from __future__ import annotations

import json
import math
import sqlite3
import time
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Mapping, Sequence

from .lp_math import MAX_TICK, principal_raw, sqrt_ratio_at_tick

Q192 = 1 << 192
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
MAX_BANDS = 12
MAX_REPLAY_SWAPS = 20_000
WINDOW_SECONDS = {
    "1h": 60 * 60,
    "24h": 24 * 60 * 60,
    "7d": 7 * 24 * 60 * 60,
    "30d": 30 * 24 * 60 * 60,
    "all": None,
}
SHAPES = {"uniform", "curve", "bid_ask"}


def _decimal(value: Any, name: str, *, allow_none: bool = False) -> Decimal | None:
    if value in (None, "") and allow_none:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a decimal number") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if str(value).strip() not in {str(result), f"+{result}"} and not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return result


def _raw_units(value: Decimal, decimals: int) -> int:
    scaled = value * (Decimal(10) ** decimals)
    if scaled != scaled.to_integral_value():
        raise ValueError(f"capital_usd has more than {decimals} USDG decimals")
    return int(scaled)


def _format_units(raw: int, decimals: int) -> str:
    sign = "-" if raw < 0 else ""
    digits = str(abs(raw)).rjust(decimals + 1, "0")
    if decimals == 0:
        return sign + digits
    whole, fraction = digits[:-decimals], digits[-decimals:]
    fraction = fraction.rstrip("0")
    return sign + whole + (f".{fraction}" if fraction else "")


def _money(value: Decimal | None) -> str | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = max(100, len(value.as_tuple().digits) + max(0, value.adjusted()) + 12)
        rounded = value.quantize(Decimal("0.000001"))
    text = format(rounded, "f").rstrip("0").rstrip(".")
    return text or "0"


def _price(sqrt_price_x96: int, decimals0: int, decimals1: int) -> Decimal:
    with localcontext() as context:
        context.prec = 80
        return (
            Decimal(sqrt_price_x96) * Decimal(sqrt_price_x96) / Decimal(Q192)
            * (Decimal(10) ** (decimals0 - decimals1))
        )


def _price_text(value: Decimal) -> str:
    with localcontext() as context:
        context.prec = 24
        normalized = +value
    return format(normalized, ".18g")


def _stable_side(token0: str, token1: str) -> int | None:
    if token0.lower() == USDG:
        return 0
    if token1.lower() == USDG:
        return 1
    return None


def _stable_leg_values_raw(
    amount0: int, amount1: int, sqrt_price_x96: int, stable_side: int
) -> tuple[int, int]:
    square = sqrt_price_x96 * sqrt_price_x96
    if stable_side == 1:
        return amount0 * square // Q192, amount1
    return amount0, amount1 * Q192 // square


def _stable_value_raw(
    amount0: int, amount1: int, sqrt_price_x96: int, stable_side: int
) -> int:
    leg0, leg1 = _stable_leg_values_raw(amount0, amount1, sqrt_price_x96, stable_side)
    return leg0 + leg1


def _liquidity_for_budget(
    budget_raw: int,
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
    stable_side: int,
) -> tuple[int, int, int]:
    """Return the greatest integer L whose floor-rounded principal fits budget."""
    if budget_raw <= 0:
        return 0, 0, 0

    def amounts(liquidity: int) -> tuple[int, int]:
        return principal_raw(liquidity, sqrt_price_x96, tick_lower, tick_upper)

    def fits(liquidity: int) -> bool:
        amount0, amount1 = amounts(liquidity)
        return _stable_value_raw(amount0, amount1, sqrt_price_x96, stable_side) <= budget_raw

    high = 1
    while fits(high):
        high <<= 1
        if high.bit_length() > 256:
            raise ValueError("capital produces liquidity outside uint256 bounds")
    low = high >> 1
    while low + 1 < high:
        middle = (low + high) >> 1
        if fits(middle):
            low = middle
        else:
            high = middle
    amount0, amount1 = amounts(low)
    return low, amount0, amount1


def _snap_lower(tick: int, spacing: int) -> int:
    return max(math.ceil(-MAX_TICK / spacing) * spacing, (tick // spacing) * spacing)


def _snap_upper(tick: int, spacing: int) -> int:
    return min(math.floor(MAX_TICK / spacing) * spacing, -((-tick) // spacing) * spacing)


def _partition(tick_lower: int, tick_upper: int, requested: int, spacing: int) -> list[tuple[int, int]]:
    units = (tick_upper - tick_lower) // spacing
    count = max(1, min(requested, units, MAX_BANDS))
    boundaries = [tick_lower + (units * index // count) * spacing for index in range(count)]
    boundaries.append(tick_upper)
    return [
        (boundaries[index], boundaries[index + 1])
        for index in range(count)
        if boundaries[index] < boundaries[index + 1]
    ]


def _weights(shape: str, count: int) -> list[int]:
    if shape == "uniform" or count == 1:
        return [1] * count
    center = count - 1
    if shape == "curve":
        return [count - abs(2 * index - center) for index in range(count)]
    return [1 + abs(2 * index - center) for index in range(count)]


def _budgets(total: int, weights: Sequence[int]) -> list[int]:
    denominator = sum(weights)
    rows: list[int] = []
    assigned = 0
    cumulative = 0
    for index, weight in enumerate(weights):
        cumulative += weight
        target = total if index == len(weights) - 1 else total * cumulative // denominator
        rows.append(target - assigned)
        assigned = target
    return rows


def _query(connection: Any, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, tuple(params))
    names = [item[0] for item in cursor.description or ()]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _latest_store_spot(store: Any, pool_id: str) -> tuple[int, int] | None:
    if store is None:
        return None
    try:
        rows = _query(
            store.read(),
            "SELECT sqrt_price_x96,tick FROM events "
            "WHERE pool_id=? AND kind='swap' AND sqrt_price_x96 IS NOT NULL AND tick IS NOT NULL "
            "ORDER BY block_number DESC,tx_index DESC,log_index DESC LIMIT 1",
            (pool_id,),
        )
    except Exception:
        return None
    if not rows:
        return None
    try:
        return int(rows[0]["sqrt_price_x96"]), int(rows[0]["tick"])
    except (TypeError, ValueError):
        return None


def _pool_context(payload: Mapping[str, Any], market: Any, store: Any) -> dict[str, Any]:
    pool_id = str(payload.get("pool_id") or "").strip().lower()
    if not pool_id:
        raise ValueError("pool_id is required")
    pool = market._pool_by_id(pool_id)
    if pool is None:
        raise ValueError(f"unknown pool id {pool_id!r}")
    protocol = str(getattr(pool, "kind", "")).lower()
    if protocol not in {"v3", "v4"}:
        raise ValueError("allocation preview is available only for V3/V4 concentrated-liquidity pools")
    token0 = market._token_object(pool.token0)
    token1 = market._token_object(pool.token1)
    decimals0 = token0.get("decimals")
    decimals1 = token1.get("decimals")
    if decimals0 is None or decimals1 is None:
        raise ValueError("token decimals are not indexed for this pool")
    decimals0, decimals1 = int(decimals0), int(decimals1)
    if not 0 <= decimals0 <= 255 or not 0 <= decimals1 <= 255:
        raise ValueError("token decimals must be between 0 and 255")

    detail = market.detail(pool_id)
    spot = detail.get("spot") if isinstance(detail, Mapping) else None
    sqrt_price_x96: int | None = None
    tick: int | None = None
    if isinstance(spot, Mapping):
        try:
            sqrt_price_x96 = int(spot.get("sqrt_price_x96"))
            tick = int(spot.get("tick"))
        except (TypeError, ValueError):
            sqrt_price_x96 = tick = None
    if not sqrt_price_x96 or tick is None:
        persisted = _latest_store_spot(store, pool_id)
        if persisted is not None:
            sqrt_price_x96, tick = persisted
    if not sqrt_price_x96 or tick is None:
        raise ValueError("a real current or persisted swap price is required for allocation math")
    if sqrt_price_x96 >= 1 << 160 or not -MAX_TICK <= tick <= MAX_TICK:
        raise ValueError("current pool price is outside canonical concentrated-liquidity bounds")

    spacing = int(getattr(pool, "tick_spacing", None) or 1)
    if spacing <= 0 or spacing > 2 * MAX_TICK:
        raise ValueError("pool tick spacing is unavailable")
    return {
        "id": pool_id,
        "protocol": protocol,
        "address": pool.address,
        "token0": token0,
        "token1": token1,
        "decimals0": decimals0,
        "decimals1": decimals1,
        "fee_ppm": getattr(pool, "fee_ppm", None),
        "tick_spacing": spacing,
        "hook": getattr(pool, "hook", None),
        "sqrt_price_x96": sqrt_price_x96,
        "tick": tick,
        "stable_side": _stable_side(pool.token0, pool.token1),
    }


def _band_plan(
    context: Mapping[str, Any],
    *,
    capital: Decimal,
    tick_lower: int,
    tick_upper: int,
    shape: str,
    requested_bands: int,
    sqrt_price_x96: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sqrt_price_x96 = int(sqrt_price_x96 or context["sqrt_price_x96"])
    stable_side = context["stable_side"]
    stable_decimals = (
        context["decimals0"] if stable_side == 0 else context["decimals1"]
        if stable_side == 1 else None
    )
    ranges = _partition(tick_lower, tick_upper, 1 if shape == "uniform" else requested_bands, context["tick_spacing"])
    weights = _weights(shape, len(ranges))
    capital_raw = _raw_units(capital, stable_decimals) if stable_decimals is not None else None
    budgets = _budgets(capital_raw, weights) if capital_raw is not None else [None] * len(ranges)
    total0 = total1 = total_liquidity = total_leg0 = total_leg1 = 0
    bands: list[dict[str, Any]] = []
    for index, ((lower, upper), weight, budget) in enumerate(zip(ranges, weights, budgets), 1):
        liquidity: int | None = None
        amount0: int | None = None
        amount1: int | None = None
        if budget is not None:
            liquidity, amount0, amount1 = _liquidity_for_budget(
                budget, sqrt_price_x96, lower, upper, stable_side
            )
            total0 += amount0
            total1 += amount1
            total_liquidity += liquidity
            leg0, leg1 = _stable_leg_values_raw(amount0, amount1, sqrt_price_x96, stable_side)
            total_leg0 += leg0
            total_leg1 += leg1
        current_tick = int(context["tick"])
        side = "token0_only" if current_tick < lower else "token1_only" if current_tick >= upper else "two_sided"
        bands.append({
            "index": index,
            "tick_lower": lower,
            "tick_upper": upper,
            "price_lower": _price_text(_price(sqrt_ratio_at_tick(lower), context["decimals0"], context["decimals1"])),
            "price_upper": _price_text(_price(sqrt_ratio_at_tick(upper), context["decimals0"], context["decimals1"])),
            "weight": str(weight),
            "weight_pct": _money(Decimal(weight) * 100 / Decimal(sum(weights))),
            "capital_usd": _format_units(budget, stable_decimals) if budget is not None else None,
            "liquidity": str(liquidity) if liquidity is not None else None,
            "amount0": _format_units(amount0, context["decimals0"]) if amount0 is not None else None,
            "amount1": _format_units(amount1, context["decimals1"]) if amount1 is not None else None,
            "amount0_raw": str(amount0) if amount0 is not None else None,
            "amount1_raw": str(amount1) if amount1 is not None else None,
            "in_range": lower <= current_tick < upper,
            "side": side,
        })

    token0_pct = token1_pct = None
    allocated_raw = None
    if stable_side is not None:
        allocated_raw = total_leg0 + total_leg1
        if allocated_raw:
            token0_pct = _money(Decimal(total_leg0) * 100 / Decimal(allocated_raw))
            token1_pct = _money(Decimal(total_leg1) * 100 / Decimal(allocated_raw))
    totals = {
        "amount0": _format_units(total0, context["decimals0"]) if capital_raw is not None else None,
        "amount1": _format_units(total1, context["decimals1"]) if capital_raw is not None else None,
        "amount0_raw": str(total0) if capital_raw is not None else None,
        "amount1_raw": str(total1) if capital_raw is not None else None,
        "liquidity_sum": str(total_liquidity) if capital_raw is not None else None,
        "allocated_usd": _format_units(allocated_raw, stable_decimals) if allocated_raw is not None else None,
        "token0_pct": token0_pct,
        "token1_pct": token1_pct,
    }
    return bands, totals


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None

def _event_order(row: Mapping[str, Any]) -> tuple[int, int, int]:
    return (
        _int_or_none(row.get("block_number")) or 0,
        _int_or_none(row.get("tx_index")) or 0,
        _int_or_none(row.get("log_index")) or 0,
    )


def _event_data(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("data")
    if isinstance(value, Mapping):
        return dict(value)
    try:
        decoded = json.loads(value) if value else {}
    except (TypeError, ValueError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _fee_policy_timeline(rows: Sequence[Mapping[str, Any]], protocol: str) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    for row in sorted(rows, key=_event_order):
        data = _event_data(row)
        state = data.get("pool_state_before")
        policy: dict[str, Any] | None = None
        policy_order = _event_order(row)
        if isinstance(state, Mapping):
            if protocol == "v3":
                encoding = str(state.get("fee_protocol_encoding") or "")
                protocol0 = _int_or_none(state.get("fee_protocol0_ppm"))
                protocol1 = _int_or_none(state.get("fee_protocol1_ppm"))
                if encoding == "pancake_uint16_ppm_each" or protocol0 is not None or protocol1 is not None:
                    if protocol0 is not None and protocol1 is not None:
                        policy = {"input0_protocol_ppm": protocol0, "input1_protocol_ppm": protocol1, "basis": "parent_block_pancake_protocol_ppm"}
                else:
                    packed = _int_or_none(state.get("fee_protocol", state.get("protocol_fee")))
                    divisor0 = _int_or_none(state.get("fee_protocol0_divisor"))
                    divisor1 = _int_or_none(state.get("fee_protocol1_divisor"))
                    if packed is not None:
                        divisor0 = packed & 0x0F if divisor0 is None else divisor0
                        divisor1 = (packed >> 4) & 0x0F if divisor1 is None else divisor1
                    if divisor0 is not None and divisor1 is not None:
                        policy = {"input0_divisor": divisor0, "input1_divisor": divisor1, "basis": "parent_block_uniswap_protocol_divisor"}
            else:
                packed = _int_or_none(state.get("protocol_fee"))
                fee0 = _int_or_none(state.get("protocol_fee_zero_for_one"))
                fee1 = _int_or_none(state.get("protocol_fee_one_for_zero"))
                if packed is not None:
                    fee0 = packed & 0xFFF if fee0 is None else fee0
                    fee1 = packed >> 12 if fee1 is None else fee1
                if fee0 is not None and fee1 is not None:
                    policy = {"input0_protocol_ppm": fee0, "input1_protocol_ppm": fee1, "basis": "parent_block_pool_state"}
            # The enriched value was pinned to the parent block, so it is effective
            # from the start of this block, not merely after the Add log.
            policy_order = ((_int_or_none(row.get("block_number")) or 0), -1, -1)
        if row.get("accounting_basis") == "protocol_configuration":
            if protocol == "v3":
                encoding = str(data.get("fee_protocol_encoding") or "")
                protocol0 = _int_or_none(data.get("fee_protocol0_ppm_new"))
                protocol1 = _int_or_none(data.get("fee_protocol1_ppm_new"))
                if encoding == "pancake_uint16_ppm_each" or protocol0 is not None or protocol1 is not None:
                    if protocol0 is not None and protocol1 is not None:
                        policy = {"input0_protocol_ppm": protocol0, "input1_protocol_ppm": protocol1, "basis": "pancake_fee_update_event"}
                else:
                    divisor0 = _int_or_none(data.get("fee_protocol0_divisor_new", data.get("fee_protocol0_new")))
                    divisor1 = _int_or_none(data.get("fee_protocol1_divisor_new", data.get("fee_protocol1_new")))
                    if divisor0 is not None and divisor1 is not None:
                        policy = {"input0_divisor": divisor0, "input1_divisor": divisor1, "basis": "uniswap_fee_update_event"}
            else:
                packed = _int_or_none(data.get("protocol_fee"))
                fee0 = _int_or_none(data.get("protocol_fee_zero_for_one"))
                fee1 = _int_or_none(data.get("protocol_fee_one_for_zero"))
                if packed is not None:
                    fee0 = packed & 0xFFF if fee0 is None else fee0
                    fee1 = packed >> 12 if fee1 is None else fee1
                if fee0 is not None and fee1 is not None:
                    policy = {"input0_protocol_ppm": fee0, "input1_protocol_ppm": fee1, "basis": "fee_update_event"}
            policy_order = _event_order(row)
        if policy is not None:
            policy["order"] = policy_order
            timeline.append(policy)
    return sorted(timeline, key=lambda policy: policy["order"])


def _partial_range_fee_raw(
    previous_sqrtp: int,
    sqrtp: int,
    lower_sqrtp: int,
    upper_sqrtp: int,
    amount0: int,
    amount1: int,
    fee_rate: float,
    share: float,
) -> tuple[float, float, bool]:
    """Apply the lpsim path-overlap convention without losing raw Q96 precision."""
    if previous_sqrtp == sqrtp:
        if not lower_sqrtp <= sqrtp < upper_sqrtp:
            return 0.0, 0.0, False
        fraction = 1.0
    else:
        interval_low, interval_high = sorted((previous_sqrtp, sqrtp))
        overlap_low = max(interval_low, lower_sqrtp)
        overlap_high = min(interval_high, upper_sqrtp)
        if overlap_high <= overlap_low:
            return 0.0, 0.0, False
        if amount1 > 0:
            fraction = (overlap_high - overlap_low) / (interval_high - interval_low)
        elif amount0 > 0:
            fraction = (
                (overlap_high - overlap_low) * interval_low * interval_high
                / ((interval_high - interval_low) * overlap_low * overlap_high)
            )
        else:
            fraction = 0.0
    fraction = max(0.0, min(1.0, fraction))
    return (
        max(amount0, 0) * fee_rate * fraction * share,
        max(amount1, 0) * fee_rate * fraction * share,
        True,
    )


def _usd_value(
    amount0_raw: Decimal,
    amount1_raw: Decimal,
    sqrt_price_x96: int,
    context: Mapping[str, Any],
) -> Decimal | None:
    side = context["stable_side"]
    if side is None:
        return None
    with localcontext() as decimal_context:
        decimal_context.prec = 60
        units0 = amount0_raw / (Decimal(10) ** context["decimals0"])
        units1 = amount1_raw / (Decimal(10) ** context["decimals1"])
        price = _price(sqrt_price_x96, context["decimals0"], context["decimals1"])
        return units0 + units1 / price if side == 0 else units0 * price + units1


def _store_status(store: Any) -> dict[str, Any]:
    try:
        status = store.status()
    except Exception:
        return {}
    return dict(status) if isinstance(status, Mapping) else {}


def _history_rows(store: Any, pool_id: str, window: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if store is None:
        return [], {"available": False, "state": "unavailable", "reason": "persistent market store is not configured"}
    try:
        connection = store.read()
        latest_rows = _query(
            connection,
            "SELECT MAX(timestamp) AS latest FROM events WHERE pool_id=? AND kind='swap'",
            (pool_id,),
        )
        latest = _int_or_none(latest_rows[0]["latest"] if latest_rows else None)
        if latest is None:
            return [], {"available": False, "state": "unavailable", "reason": "no persisted swaps are indexed for this pool"}
        seconds = WINDOW_SECONDS[window]
        requested_from = latest - seconds if seconds is not None else None
        where = "pool_id=? AND kind='swap' AND sqrt_price_x96 IS NOT NULL AND tick IS NOT NULL"
        params: list[Any] = [pool_id]
        if requested_from is not None:
            where += " AND timestamp>=?"
            params.append(requested_from)
        rows = _query(
            connection,
            "SELECT id,block_number,block_hash,tx_hash,tx_index,log_index,timestamp,"
            "sqrt_price_x96,tick,liquidity,fee_ppm,amount0,amount1,fee_amount0,fee_amount1,"
            "price0_usd,price1_usd,pricing_basis,data FROM events WHERE " + where +
            " ORDER BY timestamp DESC,block_number DESC,tx_index DESC,log_index DESC LIMIT ?",
            (*params, MAX_REPLAY_SWAPS + 1),
        )
    except Exception as exc:
        return [], {"available": False, "state": "unavailable", "reason": f"persisted swap query unavailable: {exc}"}

    truncated = len(rows) > MAX_REPLAY_SWAPS
    if truncated:
        rows = rows[:MAX_REPLAY_SWAPS]
    rows.sort(key=lambda row: (
        _int_or_none(row.get("block_number")) or 0,
        _int_or_none(row.get("tx_index")) or 0,
        _int_or_none(row.get("log_index")) or 0,
    ))
    predecessor = None
    if rows:
        first = rows[0]
        try:
            preceding = _query(
                connection,
                "SELECT id,block_number,block_hash,tx_hash,tx_index,log_index,timestamp,"
                "sqrt_price_x96,tick,liquidity,fee_ppm,amount0,amount1,fee_amount0,fee_amount1,"
                "price0_usd,price1_usd,pricing_basis,data FROM events WHERE pool_id=? AND kind='swap' "
                "AND sqrt_price_x96 IS NOT NULL AND tick IS NOT NULL AND ("
                "block_number<? OR (block_number=? AND tx_index<?) OR "
                "(block_number=? AND tx_index=? AND log_index<?)) "
                "ORDER BY block_number DESC,tx_index DESC,log_index DESC LIMIT 1",
                (
                    pool_id,
                    first["block_number"], first["block_number"], first["tx_index"],
                    first["block_number"], first["tx_index"], first["log_index"],
                ),
            )
            predecessor = preceding[0] if preceding else None
        except sqlite3.Error:
            predecessor = None
    policy_limit = 1_024
    policy_rows: list[dict[str, Any]] = []
    policy_truncated = False
    policy_error = None
    try:
        columns = "id,block_number,tx_index,log_index,timestamp,protocol,kind,accounting_basis,data"
        configurations = _query(
            connection,
            f"SELECT {columns} FROM events WHERE pool_id=? AND accounting_basis='protocol_configuration' "
            "AND timestamp<=? ORDER BY timestamp DESC,id DESC LIMIT ?",
            (pool_id, latest, policy_limit + 1),
        )
        anchors = _query(
            connection,
            f"SELECT {columns} FROM events WHERE pool_id=? AND kind='add' AND timestamp<=? "
            "AND data LIKE ? ORDER BY timestamp DESC,id DESC LIMIT ?",
            (pool_id, latest, '%\"pool_state_before\"%', policy_limit + 1),
        )
        policy_truncated = len(configurations) > policy_limit or len(anchors) > policy_limit
        merged = {row["id"]: row for row in configurations[:policy_limit]}
        merged.update({row["id"]: row for row in anchors[:policy_limit]})
        policy_rows = sorted(merged.values(), key=_event_order)
    except Exception as exc:
        policy_error = str(exc)

    coverage = {
        "available": bool(rows),
        "state": "partial" if truncated else "complete",
        "requested_window": window,
        "requested_from": requested_from,
        "as_of": latest,
        "covered_from": _int_or_none(rows[0].get("timestamp")) if rows else None,
        "covered_to": _int_or_none(rows[-1].get("timestamp")) if rows else None,
        "row_limit": MAX_REPLAY_SWAPS,
        "truncated": truncated,
        "predecessor_available": predecessor is not None,
        "fee_policy_rows": len(policy_rows),
        "fee_policy_truncated": policy_truncated,
        "fee_policy_error": policy_error,
        "_fee_policy_rows": policy_rows,
    }
    if predecessor is not None:
        rows.insert(0, {**predecessor, "_predecessor": True})
    return rows, coverage


def _event_fee_rates(
    row: Mapping[str, Any],
    amount0: int,
    amount1: int,
    protocol: str,
    policy: Mapping[str, Any] | None,
) -> tuple[float | None, float | None, str | None]:
    input_amount = amount0 if amount0 > 0 else amount1 if amount1 > 0 else 0
    if input_amount <= 0:
        return None, None, None
    explicit = _int_or_none(row.get("fee_amount0")) if amount0 > 0 else _int_or_none(row.get("fee_amount1"))
    if explicit is not None and explicit >= 0:
        gross_rate = explicit / input_amount
    else:
        fee_ppm = _int_or_none(row.get("fee_ppm"))
        if fee_ppm is None or fee_ppm < 0:
            return None, None, None
        gross_rate = fee_ppm / 1_000_000
    data = _event_data(row)
    if data.get("protocol_fee_observed") is True:
        protocol_amount = _int_or_none(data.get(
            "protocol_fee_amount0" if amount0 > 0 else "protocol_fee_amount1"
        ))
        protocol_rate = protocol_amount / input_amount if protocol_amount is not None and protocol_amount >= 0 else None
        if protocol_rate is not None and protocol_rate <= gross_rate:
            return gross_rate, gross_rate - protocol_rate, "exact_swap_protocol_amount"
        return gross_rate, None, None
    if policy is None:
        return gross_rate, None, None
    if protocol == "v3" and (
        "input0_protocol_ppm" not in policy and "input1_protocol_ppm" not in policy
    ):
        divisor = _int_or_none(policy.get("input0_divisor" if amount0 > 0 else "input1_divisor"))
        if divisor is None or divisor < 0 or divisor == 1:
            return gross_rate, None, None
        keep = 1.0 if divisor == 0 else 1.0 - 1.0 / divisor
        return gross_rate, gross_rate * keep, str(policy.get("basis") or "observed_v3_protocol_policy")
    protocol_ppm = _int_or_none(policy.get(
        "input0_protocol_ppm" if amount0 > 0 else "input1_protocol_ppm"
    ))
    if protocol_ppm is None or not 0 <= protocol_ppm <= 1_000_000:
        return gross_rate, None, None
    return (
        gross_rate,
        max(0.0, gross_rate - protocol_ppm / 1_000_000),
        str(policy.get("basis") or "observed_v4_protocol_policy"),
    )


def _replay(
    context: Mapping[str, Any],
    bands: Sequence[Mapping[str, Any]],
    window: str,
    store: Any,
    entry_gas: Decimal | None,
    exit_gas: Decimal | None,
) -> dict[str, Any]:
    rows, coverage = _history_rows(store, context["id"], window)
    policy_rows = coverage.pop("_fee_policy_rows", [])
    if not coverage.get("available") or len(rows) < 2 or context["stable_side"] is None:
        reason = coverage.get("reason")
        if context["stable_side"] is None:
            reason = "pool has no direct USDG leg; USD fees and divergence cannot be valued defensibly"
        elif coverage.get("available") and len(rows) < 2:
            reason = "at least two persisted swap prices are required"
        return {
            "available": False,
            "state": "unavailable",
            "reason": reason,
            "coverage": coverage,
            "lp_fees_usd": None,
            "net_usd": None,
        }
    predecessor = bool(rows[0].pop("_predecessor", False))
    entry_observation = rows[0]
    entry_sqrt = int(entry_observation["sqrt_price_x96"])
    coverage["entry_observation_at"] = _int_or_none(entry_observation.get("timestamp"))
    coverage["entry_precedes_requested_window"] = predecessor
    # Preserve the selected shape's exact capital budgets/ranges when sizing at entry.
    historical: list[dict[str, Any]] = []
    for source in bands:
        budget = Decimal(str(source["capital_usd"]))
        stable_decimals = context["decimals0"] if context["stable_side"] == 0 else context["decimals1"]
        liquidity, amount0, amount1 = _liquidity_for_budget(
            _raw_units(budget, stable_decimals), entry_sqrt,
            int(source["tick_lower"]), int(source["tick_upper"]), context["stable_side"],
        )
        historical.append({
            "tick_lower": int(source["tick_lower"]),
            "tick_upper": int(source["tick_upper"]),
            "liquidity": liquidity,
            "hold0": amount0,
            "hold1": amount1,
        })

    timeline = _fee_policy_timeline(policy_rows, context["protocol"])
    policy_index = 0
    active_policy: Mapping[str, Any] | None = None
    gross_fee_usd = Decimal(0)
    lp_fee_known_usd = Decimal(0)
    fee_bases: set[str] = set()
    crossing_upper_usd = Decimal(0)
    no_crossing_swaps = crossing_swaps = skipped_swaps = overlapped_swaps = 0
    policy_covered_swaps = policy_uncovered_swaps = crossing_overlapped_swaps = 0
    zero_hook = "0x" + "0" * 40
    hooked = context["protocol"] == "v4" and str(context.get("hook") or zero_hook).lower() != zero_hook
    previous = rows[0]
    for row in rows[1:]:
        order = _event_order(row)
        while policy_index < len(timeline) and timeline[policy_index]["order"] <= order:
            active_policy = timeline[policy_index]
            policy_index += 1
        previous_sqrt = _int_or_none(previous.get("sqrt_price_x96"))
        sqrt_price = _int_or_none(row.get("sqrt_price_x96"))
        delta0 = _int_or_none(row.get("amount0")) or 0
        delta1 = _int_or_none(row.get("amount1")) or 0
        if context["protocol"] == "v4":
            amount0, amount1 = max(-delta0, 0), max(-delta1, 0)
        else:
            amount0, amount1 = max(delta0, 0), max(delta1, 0)
        pool_liquidity = _int_or_none(row.get("liquidity"))
        previous_liquidity = _int_or_none(previous.get("liquidity"))
        previous_tick = _int_or_none(previous.get("tick"))
        tick = _int_or_none(row.get("tick"))
        gross_rate, lp_rate, fee_basis = _event_fee_rates(
            row, amount0, amount1, context["protocol"], active_policy
        )
        if hooked:
            lp_rate = None
            fee_basis = None
        if not previous_sqrt or not sqrt_price or gross_rate is None or pool_liquidity is None or pool_liquidity <= 0:
            skipped_swaps += 1
            previous = row
            continue
        crossing = previous_tick != tick or previous_liquidity != pool_liquidity
        swap_overlapped = False
        for band in historical:
            lower_sqrt = sqrt_ratio_at_tick(band["tick_lower"])
            upper_sqrt = sqrt_ratio_at_tick(band["tick_upper"])
            liquidity = band["liquidity"]
            if crossing:
                fee0, fee1, overlapped = _partial_range_fee_raw(
                    previous_sqrt, sqrt_price, lower_sqrt, upper_sqrt,
                    amount0, amount1, gross_rate, 1.0,
                )
                value = _usd_value(Decimal(str(fee0)), Decimal(str(fee1)), sqrt_price, context)
                if value is not None:
                    crossing_upper_usd += value
            else:
                share = liquidity / (pool_liquidity + liquidity) if liquidity > 0 else 0.0
                fee0, fee1, overlapped = _partial_range_fee_raw(
                    previous_sqrt, sqrt_price, lower_sqrt, upper_sqrt,
                    amount0, amount1, gross_rate, share,
                )
                value = _usd_value(Decimal(str(fee0)), Decimal(str(fee1)), sqrt_price, context)
                if value is not None:
                    gross_fee_usd += value
                if lp_rate is not None:
                    lp0, lp1, _ = _partial_range_fee_raw(
                        previous_sqrt, sqrt_price, lower_sqrt, upper_sqrt,
                        amount0, amount1, lp_rate, share,
                    )
                    lp_value = _usd_value(Decimal(str(lp0)), Decimal(str(lp1)), sqrt_price, context)
                    if lp_value is not None:
                        lp_fee_known_usd += lp_value
            swap_overlapped = swap_overlapped or overlapped
        if swap_overlapped:
            overlapped_swaps += 1
            if crossing:
                crossing_overlapped_swaps += 1
            elif lp_rate is None:
                policy_uncovered_swaps += 1
            else:
                policy_covered_swaps += 1
                if fee_basis:
                    fee_bases.add(fee_basis)
        if crossing:
            crossing_swaps += 1
        else:
            no_crossing_swaps += 1
        previous = row

    end_sqrt = int(rows[-1]["sqrt_price_x96"])
    hold0 = sum(item["hold0"] for item in historical)
    hold1 = sum(item["hold1"] for item in historical)
    end0 = end1 = 0
    for band in historical:
        amount0, amount1 = principal_raw(
            band["liquidity"], end_sqrt, band["tick_lower"], band["tick_upper"]
        )
        end0 += amount0
        end1 += amount1
    hold_value = _usd_value(Decimal(hold0), Decimal(hold1), end_sqrt, context)
    lp_value = _usd_value(Decimal(end0), Decimal(end1), end_sqrt, context)
    divergence = lp_value - hold_value if lp_value is not None and hold_value is not None else None

    count = len(bands)
    entry_total = entry_gas * count if entry_gas is not None else None
    exit_total = exit_gas * count if exit_gas is not None else None
    gas_total = entry_total + exit_total if entry_total is not None and exit_total is not None else None
    store_status = _store_status(store)
    coverage["store"] = store_status
    requested_from = _int_or_none(coverage.get("requested_from"))
    history_from = _int_or_none(store_status.get("history_from"))
    index_covers_start = history_from is not None and (
        requested_from is None or history_from <= requested_from
    )
    requested_start_covered = bool(coverage.get("predecessor_available") and index_covers_start)
    coverage["index_covers_requested_start"] = index_covers_start
    coverage["requested_start_covered"] = requested_start_covered
    incomplete_coverage = bool(
        coverage.get("truncated") or coverage.get("fee_policy_truncated")
        or coverage.get("fee_policy_error") or store_status.get("backfill")
        or not requested_start_covered
    )
    if incomplete_coverage:
        coverage["state"] = "partial"
    fee_complete = not (
        incomplete_coverage or crossing_overlapped_swaps or skipped_swaps
        or policy_uncovered_swaps or hooked
    )
    lp_fees = lp_fee_known_usd if fee_complete else None
    fees_plus_divergence = (
        lp_fees + divergence if lp_fees is not None and divergence is not None else None
    )
    net = fees_plus_divergence - gas_total if fees_plus_divergence is not None and gas_total is not None else None
    unavailable_reasons: list[str] = []
    if incomplete_coverage:
        unavailable_reasons.append("requested swap or fee-policy coverage is incomplete")
    if not requested_start_covered:
        unavailable_reasons.append("no indexed pre-window pool state proves the requested replay start")
    if crossing_overlapped_swaps:
        unavailable_reasons.append("in-range tick crossings lack per-step competing liquidity")
    if skipped_swaps:
        unavailable_reasons.append("some persisted swaps lack price, fee, or active-liquidity inputs")
    if policy_uncovered_swaps:
        unavailable_reasons.append("no observed at-or-before protocol fee policy covers some in-range swaps")
    if hooked:
        unavailable_reasons.append("hooked V4 fee retention is not inferred from core Swap fees")
    if gas_total is None:
        unavailable_reasons.append("both entry and exit gas-per-position inputs are required for net")

    return {
        "available": True,
        "state": "complete" if fee_complete else "partial",
        "coverage": coverage,
        "swaps": max(0, len(rows) - 1),
        "overlapped_swaps": overlapped_swaps,
        "constant_liquidity_swaps": no_crossing_swaps,
        "crossing_or_liquidity_change_swaps": crossing_swaps,
        "crossing_overlapped_swaps": crossing_overlapped_swaps,
        "skipped_swaps": skipped_swaps,
        "fee_policy": {
            "basis": sorted(fee_bases),
            "coverage_rule": "latest observed parent-block state or configuration event at-or-before each swap",
            "observations": len(timeline),
            "covered_swaps": policy_covered_swaps,
            "uncovered_swaps": policy_uncovered_swaps,
        },
        "entry_price": _price_text(_price(entry_sqrt, context["decimals0"], context["decimals1"])),
        "exit_price": _price_text(_price(end_sqrt, context["decimals0"], context["decimals1"])),
        "entry_amount0": _format_units(hold0, context["decimals0"]),
        "entry_amount1": _format_units(hold1, context["decimals1"]),
        "ending_amount0": _format_units(end0, context["decimals0"]),
        "ending_amount1": _format_units(end1, context["decimals1"]),
        "hold_ending_value_usd": _money(hold_value),
        "lp_ending_value_usd": _money(lp_value),
        "divergence_usd": _money(divergence),
        "gross_fee_share_constant_liquidity_usd": _money(gross_fee_usd),
        "crossing_gross_fee_upper_bound_usd": _money(crossing_upper_usd),
        "lp_fee_share_known_segments_usd": _money(lp_fee_known_usd),
        "lp_fees_usd": _money(lp_fees),
        "lp_fees_basis": "counterfactual estimate using observed directional protocol cut" if lp_fees is not None else "unavailable",
        "fees_vs_divergence_usd": {
            "lp_fee_estimate": _money(lp_fees),
            "gross_fee_share_known_segments": _money(gross_fee_usd),
            "divergence": _money(divergence),
            "combined_before_costs": _money(fees_plus_divergence),
        },
        "costs": {
            "source": "user_input" if entry_gas is not None or exit_gas is not None else "unavailable",
            "entry_per_position_usd": _money(entry_gas),
            "exit_per_position_usd": _money(exit_gas),
            "entry_total_usd": _money(entry_total),
            "exit_total_usd": _money(exit_total),
            "total_usd": _money(gas_total),
        },
        "net_usd": _money(net),
        "net_basis": "exogenous persisted-swap counterfactual with observed fee policy and user gas inputs" if net is not None else "unavailable",
        "unavailable_reasons": unavailable_reasons,
        "limitations": [
            "Reported Swap fees are gross input fees. LP fee estimates subtract only protocol cuts observed at-or-before each swap; absent policy is never treated as zero.",
            "Sqrt-path overlap is exact for the recorded endpoints and input direction. Fee share is assigned only where consecutive rows keep the same tick and active liquidity.",
            "In-range tick-crossing or active-liquidity-changing swaps are excluded from earned fees; their gross fee is shown only as a 100%-share upper bound because per-step competing liquidity was not persisted.",
            "The replay holds the selected ranges and capital fixed and treats recorded swap sizes/prices as exogenous; adding liquidity could have changed that path.",
        ],
    }


def preview(payload: Mapping[str, Any], market: Any, store: Any) -> dict[str, Any]:
    """Build an exact current allocation and a bounded persisted-swap replay."""
    if not isinstance(payload, Mapping):
        raise ValueError("allocation payload must be an object")
    context = _pool_context(payload, market, store)
    capital = _decimal(payload.get("capital_usd", "10000"), "capital_usd")
    if capital is None or capital <= 0 or capital > Decimal("1000000000000000"):
        raise ValueError("capital_usd must be greater than zero and at most 1 quadrillion USDG")
    shape = str(payload.get("shape") or "uniform").strip().lower().replace("-", "_")
    if shape not in SHAPES:
        raise ValueError("shape must be uniform, curve, or bid_ask")
    requested_bands = _integer(payload.get("bands", 5), "bands")
    if not 1 <= requested_bands <= MAX_BANDS:
        raise ValueError(f"bands must be between 1 and {MAX_BANDS}")
    if shape == "uniform":
        requested_bands = 1
    window = str(payload.get("history_window") or "24h").lower()
    if window not in WINDOW_SECONDS:
        raise ValueError("history_window must be 1h, 24h, 7d, 30d, or all")
    entry_gas = _decimal(payload.get("entry_gas_per_position_usd"), "entry gas", allow_none=True)
    exit_gas = _decimal(payload.get("exit_gas_per_position_usd"), "exit gas", allow_none=True)
    if entry_gas is not None and (entry_gas < 0 or entry_gas > Decimal("1000000000")):
        raise ValueError("entry gas must be between zero and 1 billion USDG")
    if exit_gas is not None and (exit_gas < 0 or exit_gas > Decimal("1000000000")):
        raise ValueError("exit gas must be between zero and 1 billion USDG")

    lower_input = _integer(payload.get("tick_lower"), "tick_lower")
    upper_input = _integer(payload.get("tick_upper"), "tick_upper")
    tick_lower = _snap_lower(lower_input, context["tick_spacing"])
    tick_upper = _snap_upper(upper_input, context["tick_spacing"])
    if tick_lower >= tick_upper:
        raise ValueError("snapped tick_lower must be below tick_upper by at least one tick spacing")

    bands, totals = _band_plan(
        context,
        capital=capital,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        shape=shape,
        requested_bands=requested_bands,
    )
    history = _replay(context, bands, window, store, entry_gas, exit_gas)
    stable_available = context["stable_side"] is not None
    return {
        "as_of": int(time.time()),
        "source": "server_exact",
        "pool": {
            "id": context["id"],
            "protocol": context["protocol"],
            "address": context["address"],
            "token0": context["token0"],
            "token1": context["token1"],
            "fee_ppm": context["fee_ppm"],
            "tick_spacing": context["tick_spacing"],
            "hook": context["hook"],
        },
        "spot": {
            "tick": context["tick"],
            "sqrt_price_x96": str(context["sqrt_price_x96"]),
            "price_token1_per_token0": _price_text(_price(context["sqrt_price_x96"], context["decimals0"], context["decimals1"])),
        },
        "shape": shape,
        "tick_lower": tick_lower,
        "tick_upper": tick_upper,
        "requested_positions": requested_bands,
        "actual_positions": len(bands),
        "capital": {
            "usd": format(capital, "f"),
            "basis": "USDG" if stable_available else "unavailable",
            "valuation_available": stable_available,
        },
        "bands": bands,
        "totals": totals,
        "history": history,
        "assumptions": [
            "Every returned band is one independent concentrated-liquidity position with its own token budget.",
            "Uniform is one position spanning the full selected range. Curve and bid/ask partition the selected ticks into contiguous real ranges with center-heavy or edge-heavy capital weights.",
            "Current amounts use exact TickMath sqrt ratios and floor-rounded V3/V4 LiquidityAmounts geometry; unused sub-raw-unit capital remains unallocated.",
            "A direct USDG pool leg is required for USD capital sizing; no unrelated token price is fabricated.",
        ],
        "limitations": [
            "This endpoint is analytical only and creates no transaction, signature, quote, or gas estimate.",
            "Multiple bands require multiple real positions. It does not imply DLMM bins or atomic multi-range execution from one V3 mint.",
            "Execution capability remains protocol- and deployment-gated by the existing explicit wallet action flow.",
        ],
    }
