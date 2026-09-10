"""Pure integer concentrated-liquidity math used by the public LP runtime.

The constants and rounding semantics are documented by MIT-licensed Uniswap
v4-core TickMath and SqrtPriceMath. See THIRD_PARTY_NOTICES.txt for the pinned
reference and retained notice.
"""
from __future__ import annotations

from typing import Any

Q96 = 1 << 96
MIN_TICK = -887272
MAX_TICK = 887272
MIN_SQRT_RATIO = 4295128739
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342
_MAX_UINT256 = (1 << 256) - 1

_TICK_MULTIPLIERS = (
    0xfffcb933bd6fad37aa2d162d1a594001,
    0xfff97272373d413259a46990580e213a,
    0xfff2e50f5f656932ef12357cf3c7fdcc,
    0xffe5caca7e10e4e61c3624eaa0941cd0,
    0xffcb9843d60f6159c9db58835c926644,
    0xff973b41fa98c081472e6896dfb254c0,
    0xff2ea16466c96a3843ec78b326b52861,
    0xfe5dee046a99a2a811c461f1969c3053,
    0xfcbe86c7900a88aedcffc83b479aa3a4,
    0xf987a7253ac413176f2b074cf7815e54,
    0xf3392b0822b70005940c7a398e4b70f3,
    0xe7159475a2c29b7443b29c7fa6e889d9,
    0xd097f3bdfd2022b8845ad8f792aa5825,
    0xa9f746462d870fdf8a65dc1f90e061e5,
    0x70d869a156d2a1b890bb3df62baf32f7,
    0x31be135f97d08fd981231505542fcfa6,
    0x9aa508b5b7a84e1c677de54f3e99bc9,
    0x5d6af8dedb81196699c329225ee604,
    0x2216e584f5fa1ea926041bedfe98,
    0x48a170391f7dc42444e8fa2,
)


def wire_int(value: Any) -> int:
    """Parse a non-boolean contract integer without a float round trip."""
    if isinstance(value, bool):
        raise ValueError("boolean is not a contract integer")
    if isinstance(value, int):
        return value
    if not isinstance(value, str) or not value or not value.isdecimal():
        raise ValueError(f"invalid contract integer: {value!r}")
    return int(value)


def sqrt_ratio_at_tick(tick: int) -> int:
    """Return exact, upward-rounded ``sqrt(1.0001**tick) * 2**96``."""
    tick = int(tick)
    absolute = abs(tick)
    if absolute > MAX_TICK:
        raise ValueError(f"tick outside V3 domain: {tick}")
    ratio = 1 << 128
    for bit, multiplier in enumerate(_TICK_MULTIPLIERS):
        if absolute & (1 << bit):
            ratio = (ratio * multiplier) >> 128
    if tick > 0:
        ratio = _MAX_UINT256 // ratio
    remainder_mask = (1 << 32) - 1
    return (ratio >> 32) + (1 if ratio & remainder_mask else 0)


def tick_at_sqrt_price_x96(value: int | str) -> int:
    """Return the greatest protocol tick whose sqrt ratio is not above value."""
    sqrt_price = wire_int(value)
    if not MIN_SQRT_RATIO < sqrt_price < MAX_SQRT_RATIO:
        raise ValueError("sqrt_price_x96 is outside the protocol range")
    low, high = MIN_TICK, MAX_TICK
    while low <= high:
        middle = (low + high) // 2
        if sqrt_ratio_at_tick(middle) <= sqrt_price:
            low = middle + 1
        else:
            high = middle - 1
    return high


def _ceil_div(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def amount0_delta(
    sqrt_a_x96: int, sqrt_b_x96: int, liquidity: int, *, round_up: bool
) -> int:
    if sqrt_a_x96 > sqrt_b_x96:
        sqrt_a_x96, sqrt_b_x96 = sqrt_b_x96, sqrt_a_x96
    numerator1 = liquidity << 96
    numerator2 = sqrt_b_x96 - sqrt_a_x96
    if round_up:
        return _ceil_div(_ceil_div(numerator1 * numerator2, sqrt_b_x96), sqrt_a_x96)
    return (numerator1 * numerator2 // sqrt_b_x96) // sqrt_a_x96


def amount1_delta(
    sqrt_a_x96: int, sqrt_b_x96: int, liquidity: int, *, round_up: bool
) -> int:
    if sqrt_a_x96 > sqrt_b_x96:
        sqrt_a_x96, sqrt_b_x96 = sqrt_b_x96, sqrt_a_x96
    numerator = liquidity * (sqrt_b_x96 - sqrt_a_x96)
    return _ceil_div(numerator, Q96) if round_up else numerator // Q96


def principal_raw(liquidity: int, sqrt: int, lo: int, hi: int) -> tuple[int, int]:
    """Return exact, floor-rounded V3 principal in token raw units."""
    liquidity = wire_int(liquidity)
    sqrt = wire_int(sqrt)
    lower = sqrt_ratio_at_tick(lo)
    upper = sqrt_ratio_at_tick(hi)
    if sqrt <= lower:
        return (((liquidity << 96) * (upper - lower)) // upper) // lower, 0
    if sqrt >= upper:
        return 0, liquidity * (upper - lower) // Q96
    amount0 = (((liquidity << 96) * (upper - sqrt)) // upper) // sqrt
    amount1 = liquidity * (sqrt - lower) // Q96
    return amount0, amount1


def fee_growth_amount(liquidity: int, current: int, previous: int) -> int:
    """Convert wrapped Q128 fee growth into floor-rounded token units."""
    return liquidity * ((current - previous) & _MAX_UINT256) // (1 << 128)


def fee_claim(
    liquidity: int,
    fee_growth0_last: int,
    fee_growth1_last: int,
    owed0: int,
    owed1: int,
    global0: int,
    global1: int,
    lower_outside0: int,
    lower_outside1: int,
    upper_outside0: int,
    upper_outside1: int,
    tick: int,
    lower: int,
    upper: int,
) -> tuple[int, int, int, int]:
    """Return total claim and still-lazy fees using V3 uint256 wrapping."""
    below0 = lower_outside0 if tick >= lower else (global0 - lower_outside0) & _MAX_UINT256
    below1 = lower_outside1 if tick >= lower else (global1 - lower_outside1) & _MAX_UINT256
    above0 = upper_outside0 if tick < upper else (global0 - upper_outside0) & _MAX_UINT256
    above1 = upper_outside1 if tick < upper else (global1 - upper_outside1) & _MAX_UINT256
    inside0 = (global0 - below0 - above0) & _MAX_UINT256
    inside1 = (global1 - below1 - above1) & _MAX_UINT256
    lazy0 = fee_growth_amount(liquidity, inside0, fee_growth0_last)
    lazy1 = fee_growth_amount(liquidity, inside1, fee_growth1_last)
    return owed0 + lazy0, owed1 + lazy1, lazy0, lazy1


__all__ = [
    "MAX_SQRT_RATIO",
    "MAX_TICK",
    "MIN_SQRT_RATIO",
    "MIN_TICK",
    "Q96",
    "amount0_delta",
    "amount1_delta",
    "fee_claim",
    "fee_growth_amount",
    "principal_raw",
    "sqrt_ratio_at_tick",
    "tick_at_sqrt_price_x96",
    "wire_int",
]
