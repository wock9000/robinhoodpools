"""Read-only LP action quoting and unsigned browser-wallet transactions.

Only the canonical chain-4663 Uniswap V3 factory is supported.  Adding
liquidity uses the byte-for-byte verified MiniRouter2 deployment; positions
remain keyed to the connected EOA.  Removing liquidity and collecting call the
pool directly from that EOA: MiniRouter2 cannot burn an EOA-owned position.

This module never signs or submits a transaction.  ``prepare`` returns the
same allowlisted calldata quoted by ``simulate`` only after a fresh chain
preflight.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any

from eth_utils import keccak

from .lp_math import (
    MAX_SQRT_RATIO, MIN_SQRT_RATIO, amount0_delta, amount1_delta,
    sqrt_ratio_at_tick, tick_at_sqrt_price_x96,
)

CHAIN_ID = 4663
V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
MINI_ROUTER2 = "0x5295e633dfb504298d4a1896ba0738acb6c89e6a"
# eth_getCode(0x5295..., chain 4663), verified against
# contracts/out/MiniRouter2.sol/MiniRouter2.json deployedBytecode.
MINI_ROUTER2_CODE_HASH = (
    "0xcd18201e5301c03549d7f4f92d9b6d657bf4a6df29f94add961bf8f46e81671f"
)
QUOTE_TTL_S = 120
MAX_QUOTES = 512
MIN_TICK = -887272
MAX_TICK = 887272
MAX_UINT128 = (1 << 128) - 1
MAX_UINT256 = (1 << 256) - 1
ZERO_ADDRESS = "0x" + "00" * 20

_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_DECIMAL_RE = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.([0-9]+))?$")


def _selector(signature: str) -> str:
    return keccak(text=signature)[:4].hex()


SEL_FACTORY = _selector("factory()")
SEL_TOKEN0 = _selector("token0()")
SEL_TOKEN1 = _selector("token1()")
SEL_FEE = _selector("fee()")
SEL_TICK_SPACING = _selector("tickSpacing()")
SEL_SLOT0 = _selector("slot0()")
SEL_LIQUIDITY = _selector("liquidity()")
SEL_GET_POOL = _selector("getPool(address,address,uint24)")
SEL_DECIMALS = _selector("decimals()")
SEL_BALANCE_OF = _selector("balanceOf(address)")
SEL_ALLOWANCE = _selector("allowance(address,address)")
SEL_APPROVE = _selector("approve(address,uint256)")
SEL_POSITIONS = _selector("positions(bytes32)")
SEL_FEE_GROWTH0 = _selector("feeGrowthGlobal0X128()")
SEL_FEE_GROWTH1 = _selector("feeGrowthGlobal1X128()")
SEL_TICKS = _selector("ticks(int24)")
SEL_MINT = _selector("mint(address,int24,int24,uint128)")
SEL_BURN = _selector("burn(int24,int24,uint128)")
SEL_COLLECT = _selector("collect(address,int24,int24,uint128,uint128)")
SEL_GUARD = _selector("guard()")


class ActionError(ValueError):
    """An actionable, client-correctable request or quote error."""


@dataclass(frozen=True)
class _PoolState:
    address: str
    token0: str
    token1: str
    decimals0: int
    decimals1: int
    fee: int
    tick_spacing: int
    sqrt_price_x96: int
    tick: int
    active_liquidity: int


@dataclass(frozen=True)
class _Position:
    liquidity: int
    fee_growth0_last: int
    fee_growth1_last: int
    owed0: int
    owed1: int


def _address(value: Any, field: str) -> str:
    if not isinstance(value, str) or _ADDRESS_RE.fullmatch(value) is None:
        raise ActionError(f"{field} must be a 20-byte 0x address")
    result = value.lower()
    if result == ZERO_ADDRESS:
        raise ActionError(f"{field} cannot be the zero address")
    return result


def _integer(value: Any, field: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ActionError(f"{field} must be an integer")
    if not low <= value <= high:
        raise ActionError(f"{field} must be between {low} and {high}")
    return value


def _decimal_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) > 96 or _DECIMAL_RE.fullmatch(value) is None:
        raise ActionError(f"{field} must be a non-negative base-10 string without exponent notation")
    return value


def _raw_amount(text: str, decimals: int, field: str) -> int:
    whole, dot, fraction = text.partition(".")
    if len(fraction) > decimals:
        raise ActionError(f"{field} has more than token decimals ({decimals})")
    raw = int(whole) * 10**decimals
    if dot:
        raw += int(fraction.ljust(decimals, "0") or "0")
    if raw > MAX_UINT256:
        raise ActionError(f"{field} exceeds uint256")
    return raw


def _format_units(raw: int, decimals: int) -> str:
    if decimals == 0:
        return str(raw)
    whole, fraction = divmod(raw, 10**decimals)
    if fraction == 0:
        return str(whole)
    return f"{whole}.{fraction:0{decimals}d}".rstrip("0")


def _word_uint(value: int) -> str:
    if not 0 <= value <= MAX_UINT256:
        raise ActionError("ABI uint256 value is out of range")
    return f"{value:064x}"


def _word_int(value: int, bits: int = 24) -> str:
    if not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
        raise ActionError(f"ABI int{bits} value is out of range")
    return f"{value & MAX_UINT256:064x}"


def _word_address(value: str) -> str:
    return value[2:].rjust(64, "0")


def _calldata(selector: str, *words: str) -> str:
    return "0x" + selector + "".join(words)


def _tx(owner: str, to: str, data: str) -> dict[str, Any]:
    return {
        "from": owner,
        "to": to,
        "data": data,
        "value": "0x0",
        "chainId": CHAIN_ID,
    }


def _rpc_tx(transaction: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in transaction.items() if key != "chainId"}


def _hex_int(value: Any, field: str) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise RuntimeError(f"RPC returned malformed {field}")
    try:
        return int(value, 16)
    except ValueError as exc:
        raise RuntimeError(f"RPC returned malformed {field}") from exc


def _words(value: Any, field: str, minimum: int = 1) -> list[int]:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise RuntimeError(f"RPC returned malformed {field}")
    body = value[2:]
    if len(body) % 64 or len(body) < minimum * 64:
        raise RuntimeError(f"RPC returned malformed {field}")
    try:
        return [int(body[offset : offset + 64], 16) for offset in range(0, len(body), 64)]
    except ValueError as exc:
        raise RuntimeError(f"RPC returned malformed {field}") from exc


def _decoded_address(value: Any, field: str) -> str:
    words = _words(value, field)
    if words[0] >> 160:
        raise RuntimeError(f"RPC returned malformed {field}")
    return "0x" + f"{words[0]:040x}"


def _signed_word(value: int) -> int:
    return value - (1 << 256) if value >= 1 << 255 else value


def _position_key(owner: str, lower: int, upper: int) -> bytes:
    return keccak(
        bytes.fromhex(owner[2:])
        + lower.to_bytes(3, "big", signed=True)
        + upper.to_bytes(3, "big", signed=True)
    )




def _price_at_tick(tick: int, decimals0: int, decimals1: int) -> str:
    with localcontext() as context:
        context.prec = 48
        price = Decimal("1.0001") ** tick
        price *= Decimal(10) ** (decimals0 - decimals1)
        return format(price, ".18g")


def _price_within_bps(old_sqrt: int, new_sqrt: int, bps: int) -> bool:
    old_squared = old_sqrt * old_sqrt
    new_squared = new_sqrt * new_sqrt
    return abs(new_squared - old_squared) * 10_000 <= old_squared * bps


def _amounts_for_liquidity(sqrt_price: int, sqrt_lower: int, sqrt_upper: int, liquidity: int) -> tuple[int, int]:
    if sqrt_price <= sqrt_lower:
        return amount0_delta(sqrt_lower, sqrt_upper, liquidity, round_up=True), 0
    if sqrt_price < sqrt_upper:
        return (
            amount0_delta(sqrt_price, sqrt_upper, liquidity, round_up=True),
            amount1_delta(sqrt_lower, sqrt_price, liquidity, round_up=True),
        )
    return 0, amount1_delta(sqrt_lower, sqrt_upper, liquidity, round_up=True)


def _liquidity_for_budgets(
    sqrt_price: int, sqrt_lower: int, sqrt_upper: int, amount0: int, amount1: int
) -> int:
    candidates: list[int] = []
    if sqrt_price <= sqrt_lower:
        if amount0 == 0:
            return 0
        candidates.append(amount0 * sqrt_lower * sqrt_upper // ((1 << 96) * (sqrt_upper - sqrt_lower)))
    elif sqrt_price < sqrt_upper:
        if amount0 == 0 or amount1 == 0:
            return 0
        candidates.append(amount0 * sqrt_price * sqrt_upper // ((1 << 96) * (sqrt_upper - sqrt_price)))
        candidates.append(amount1 * (1 << 96) // (sqrt_price - sqrt_lower))
    else:
        if amount1 == 0:
            return 0
        candidates.append(amount1 * (1 << 96) // (sqrt_upper - sqrt_lower))
    return min(min(candidates), MAX_UINT128)


def _error_text(exc: Exception) -> str:
    text = " ".join(str(exc).split())
    return text[:320] or exc.__class__.__name__


class ActionService:
    """Quote and prepare allowlisted, unsigned canonical-V3 LP operations."""

    _SIMULATE_FIELDS = frozenset(
        {
            "pool_id",
            "owner",
            "action",
            "tick_lower",
            "tick_upper",
            "amount0",
            "amount1",
            "liquidity_bps",
            "slippage_bps",
        }
    )
    _PREPARE_FIELDS = frozenset({"simulation_id", "owner", "step_id"})

    def __init__(self, rpc: Any) -> None:
        if not hasattr(rpc, "call"):
            raise TypeError("rpc must provide call(method, params)")
        self.rpc = rpc
        self._quotes: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self._quotes.clear()

    def _rpc(self, method: str, params: list[Any]) -> Any:
        return self.rpc.call(method, params)

    def _eth_call(self, to: str, data: str, block_tag: str, owner: str | None = None) -> str:
        request: dict[str, Any] = {"to": to, "data": data}
        if owner is not None:
            request["from"] = owner
        return self._rpc("eth_call", [request, block_tag])

    def _chain_id(self) -> int:
        actual = _hex_int(self._rpc("eth_chainId", []), "chain id")
        if actual != CHAIN_ID:
            raise ActionError(f"wrong chain: wallet RPC is on {actual}, expected {CHAIN_ID}")
        return actual

    def _header(self, block: str) -> dict[str, Any]:
        header = self._rpc("eth_getBlockByNumber", [block, False])
        if not isinstance(header, dict):
            raise RuntimeError("RPC returned no block header")
        number = _hex_int(header.get("number"), "block number")
        block_hash = header.get("hash")
        if not isinstance(block_hash, str) or re.fullmatch(r"0x[0-9a-fA-F]{64}", block_hash) is None:
            raise RuntimeError("RPC returned malformed block hash")
        return {"number": number, "hash": block_hash.lower()}

    def _read_uint(self, target: str, data: str, block_tag: str, field: str) -> int:
        return _words(self._eth_call(target, data, block_tag), field)[0]

    def _verify_router(self, block_tag: str) -> None:
        code = self._rpc("eth_getCode", [MINI_ROUTER2, block_tag])
        if not isinstance(code, str) or re.fullmatch(r"0x(?:[0-9a-fA-F]{2})+", code) is None:
            raise ActionError("MiniRouter2 is not deployed at the allowlisted address")
        digest = "0x" + keccak(bytes.fromhex(code[2:])).hex()
        if digest != MINI_ROUTER2_CODE_HASH:
            raise ActionError("MiniRouter2 deployed bytecode does not match the verified implementation")
        guard = self._read_uint(MINI_ROUTER2, "0x" + SEL_GUARD, block_tag, "MiniRouter2 guard")
        if guard != 0:
            raise ActionError("MiniRouter2 is currently busy; rebuild the quote")

    def _load_pool(self, address: str, block_tag: str, *, require_router: bool) -> _PoolState:
        pool_code = self._rpc("eth_getCode", [address, block_tag])
        if not isinstance(pool_code, str) or pool_code in ("0x", "0x0"):
            raise ActionError("pool_id has no deployed contract code")
        factory_code = self._rpc("eth_getCode", [V3_FACTORY, block_tag])
        if not isinstance(factory_code, str) or factory_code in ("0x", "0x0"):
            raise RuntimeError("canonical V3 factory has no deployed code")

        factory = _decoded_address(self._eth_call(address, "0x" + SEL_FACTORY, block_tag), "pool factory")
        if factory != V3_FACTORY:
            raise ActionError("pool is not a member of the allowlisted canonical V3 factory")
        token0 = _decoded_address(self._eth_call(address, "0x" + SEL_TOKEN0, block_tag), "token0")
        token1 = _decoded_address(self._eth_call(address, "0x" + SEL_TOKEN1, block_tag), "token1")
        if token0 in (ZERO_ADDRESS, token1) or token1 == ZERO_ADDRESS:
            raise ActionError("pool returned invalid token addresses")
        fee = self._read_uint(address, "0x" + SEL_FEE, block_tag, "pool fee")
        if not 0 <= fee < 1_000_000:
            raise ActionError("pool returned an invalid fee")

        membership_data = _calldata(
            SEL_GET_POOL, _word_address(token0), _word_address(token1), _word_uint(fee)
        )
        factory_pool = _decoded_address(
            self._eth_call(V3_FACTORY, membership_data, block_tag), "factory getPool"
        )
        if factory_pool != address:
            raise ActionError("factory getPool does not resolve to pool_id")

        tick_spacing = _signed_word(
            self._read_uint(address, "0x" + SEL_TICK_SPACING, block_tag, "tick spacing")
        )
        if not 0 < tick_spacing <= MAX_TICK:
            raise ActionError("pool returned an invalid tick spacing")
        slot = _words(self._eth_call(address, "0x" + SEL_SLOT0, block_tag), "slot0", 7)
        sqrt_price = slot[0]
        tick = _signed_word(slot[1])
        if not MIN_SQRT_RATIO < sqrt_price < MAX_SQRT_RATIO:
            raise ActionError("pool sqrt price is outside the V3 protocol range")
        computed_tick = tick_at_sqrt_price_x96(sqrt_price)
        boundary = (
            computed_tick > MIN_TICK
            and sqrt_price == sqrt_ratio_at_tick(computed_tick)
            and tick == computed_tick - 1
        )
        if tick != computed_tick and not boundary:
            raise ActionError("pool tick and sqrt price are inconsistent")
        if slot[6] == 0:
            raise ActionError("pool is currently locked")
        active = self._read_uint(address, "0x" + SEL_LIQUIDITY, block_tag, "active liquidity")
        if active > MAX_UINT128:
            raise ActionError("pool active liquidity exceeds uint128")

        decimals: list[int] = []
        for index, token in enumerate((token0, token1)):
            token_code = self._rpc("eth_getCode", [token, block_tag])
            if not isinstance(token_code, str) or token_code in ("0x", "0x0"):
                raise ActionError(f"token{index} has no deployed contract code")
            value = self._read_uint(token, "0x" + SEL_DECIMALS, block_tag, f"token{index} decimals")
            if value > 36:
                raise ActionError(f"token{index} decimals above 36 are not supported")
            decimals.append(value)

        if require_router:
            self._verify_router(block_tag)
        return _PoolState(
            address=address,
            token0=token0,
            token1=token1,
            decimals0=decimals[0],
            decimals1=decimals[1],
            fee=fee,
            tick_spacing=tick_spacing,
            sqrt_price_x96=sqrt_price,
            tick=tick,
            active_liquidity=active,
        )

    def _position(self, pool: _PoolState, owner: str, lower: int, upper: int, block_tag: str) -> _Position:
        key = _position_key(owner, lower, upper)
        result = _words(
            self._eth_call(pool.address, _calldata(SEL_POSITIONS, key.hex()), block_tag),
            "position",
            5,
        )
        liquidity, owed0, owed1 = result[0], result[3], result[4]
        if liquidity > MAX_UINT128 or owed0 > MAX_UINT128 or owed1 > MAX_UINT128:
            raise RuntimeError("pool returned malformed position values")
        return _Position(
            liquidity=liquidity,
            fee_growth0_last=result[1],
            fee_growth1_last=result[2],
            owed0=owed0,
            owed1=owed1,
        )

    def _uncollected_fees(
        self,
        pool: _PoolState,
        position: _Position,
        lower: int,
        upper: int,
        block_tag: str,
    ) -> tuple[int, int]:
        """Exact post-zero-burn tokens owed from pinned V3 fee accumulators."""
        if position.liquidity == 0:
            return position.owed0, position.owed1
        global0 = self._read_uint(
            pool.address, "0x" + SEL_FEE_GROWTH0, block_tag, "feeGrowthGlobal0X128"
        )
        global1 = self._read_uint(
            pool.address, "0x" + SEL_FEE_GROWTH1, block_tag, "feeGrowthGlobal1X128"
        )
        lower_state = _words(
            self._eth_call(
                pool.address, _calldata(SEL_TICKS, _word_int(lower)), block_tag
            ),
            "lower tick",
            4,
        )
        upper_state = _words(
            self._eth_call(
                pool.address, _calldata(SEL_TICKS, _word_int(upper)), block_tag
            ),
            "upper tick",
            4,
        )
        mask = MAX_UINT256
        below0 = lower_state[2] if pool.tick >= lower else (global0 - lower_state[2]) & mask
        below1 = lower_state[3] if pool.tick >= lower else (global1 - lower_state[3]) & mask
        above0 = upper_state[2] if pool.tick < upper else (global0 - upper_state[2]) & mask
        above1 = upper_state[3] if pool.tick < upper else (global1 - upper_state[3]) & mask
        inside0 = (global0 - below0 - above0) & mask
        inside1 = (global1 - below1 - above1) & mask
        delta0 = (inside0 - position.fee_growth0_last) & mask
        delta1 = (inside1 - position.fee_growth1_last) & mask
        return (
            position.owed0 + position.liquidity * delta0 // (1 << 128),
            position.owed1 + position.liquidity * delta1 // (1 << 128),
        )

    def _balance(self, token: str, owner: str, block_tag: str) -> int:
        data = _calldata(SEL_BALANCE_OF, _word_address(owner))
        return self._read_uint(token, data, block_tag, "token balance")

    def _allowance(self, token: str, owner: str, block_tag: str) -> int:
        data = _calldata(SEL_ALLOWANCE, _word_address(owner), _word_address(MINI_ROUTER2))
        return self._read_uint(token, data, block_tag, "token allowance")

    def _simulate_transaction(
        self, transaction: dict[str, Any], block_tag: str
    ) -> tuple[dict[str, Any], str | None]:
        request = _rpc_tx(transaction)
        try:
            result = self._rpc("eth_call", [request, block_tag])
            if not isinstance(result, str) or not result.startswith("0x"):
                raise RuntimeError("eth_call returned malformed data")
        except Exception as exc:
            error = _error_text(exc)
            return {"success": False, "gas_estimate": None, "error": error}, None
        try:
            gas = _hex_int(
                self._rpc("eth_estimateGas", [request, block_tag]), "gas estimate"
            )
        except Exception as exc:
            error = f"gas estimation failed: {_error_text(exc)}"
            return {"success": False, "gas_estimate": None, "error": error}, result
        return {"success": True, "gas_estimate": gas, "error": None}, result

    @staticmethod
    def _validate_range(lower: int, upper: int, spacing: int) -> None:
        if not MIN_TICK <= lower < upper <= MAX_TICK:
            raise ActionError(f"tick range must satisfy {MIN_TICK} <= lower < upper <= {MAX_TICK}")
        if lower % spacing or upper % spacing:
            raise ActionError(f"ticks must be exact multiples of pool tick spacing {spacing}")

    @staticmethod
    def _metadata(pool: _PoolState) -> dict[str, Any]:
        return {
            "address": pool.address,
            "token0": pool.token0,
            "token1": pool.token1,
            "decimals0": pool.decimals0,
            "decimals1": pool.decimals1,
            "fee": pool.fee,
            "tick_spacing": pool.tick_spacing,
        }

    @staticmethod
    def _step(
        step_id: str,
        kind: str,
        description: str,
        transaction: dict[str, Any],
        simulation: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "id": step_id,
            "kind": kind,
            "description": description,
            "transaction": transaction,
            "simulation": simulation,
        }

    def _approval_step(
        self,
        token: str,
        owner: str,
        amount: int,
        index: int,
        block_tag: str,
    ) -> dict[str, Any]:
        transaction = _tx(
            owner,
            token,
            _calldata(SEL_APPROVE, _word_address(MINI_ROUTER2), _word_uint(amount)),
        )
        simulation, result = self._simulate_transaction(transaction, block_tag)
        if simulation["success"] and result not in ("0x", "0x" + _word_uint(1)):
            simulation = {
                "success": False,
                "gas_estimate": simulation["gas_estimate"],
                "error": "token approve did not return true",
            }
        return self._step(
            f"approve-token{index}",
            "approval",
            f"Set token{index} allowance to the exact quoted spend ceiling ({amount} raw units)",
            transaction,
            simulation,
        )

    def _base_summary(self, pool: _PoolState, lower: int, upper: int) -> dict[str, Any]:
        return {
            "amount0": "0",
            "amount1": "0",
            "liquidity": "0",
            "share_pct": 0.0,
            "price_lower": _price_at_tick(lower, pool.decimals0, pool.decimals1),
            "price_upper": _price_at_tick(upper, pool.decimals0, pool.decimals1),
            "gas_estimate": None,
        }

    def simulate(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ActionError("simulation payload must be an object")
        missing = self._SIMULATE_FIELDS - payload.keys()
        extra = payload.keys() - self._SIMULATE_FIELDS
        if missing:
            raise ActionError("missing simulation fields: " + ", ".join(sorted(missing)))
        if extra:
            raise ActionError("unexpected simulation fields: " + ", ".join(sorted(extra)))

        pool_id = _address(payload["pool_id"], "pool_id")
        owner = _address(payload["owner"], "owner")
        action = payload["action"]
        if action not in ("add", "remove", "collect"):
            raise ActionError("action must be add, remove, or collect")
        lower = _integer(payload["tick_lower"], "tick_lower", -(1 << 23), (1 << 23) - 1)
        upper = _integer(payload["tick_upper"], "tick_upper", -(1 << 23), (1 << 23) - 1)
        amount0_text = _decimal_text(payload["amount0"], "amount0")
        amount1_text = _decimal_text(payload["amount1"], "amount1")
        liquidity_bps = _integer(payload["liquidity_bps"], "liquidity_bps", 0, 10_000)
        slippage_bps = _integer(payload["slippage_bps"], "slippage_bps", 0, 10_000)
        if action == "remove" and liquidity_bps == 0:
            raise ActionError("liquidity_bps must be positive for remove")

        self._chain_id()
        header = self._header("latest")
        block = header["number"]
        block_tag = hex(block)
        pool = self._load_pool(pool_id, block_tag, require_router=action == "add")
        self._validate_range(lower, upper, pool.tick_spacing)
        amount0_raw = _raw_amount(amount0_text, pool.decimals0, "amount0")
        amount1_raw = _raw_amount(amount1_text, pool.decimals1, "amount1")
        if action != "add" and (amount0_raw or amount1_raw):
            raise ActionError(
                "amount0 and amount1 must be zero for direct remove/collect; this pool surface has no on-chain minimum-output parameter"
            )

        summary = self._base_summary(pool, lower, upper)
        warnings: list[str] = []
        if action == "add":
            warnings.append(
                "This router has no on-chain deadline or price guard. Exact allowances "
                "cap token spending; quote expiry and price checks apply before signing, "
                "not after submission."
            )
        elif action == "remove":
            warnings.append(
                "Direct pool burns have no on-chain minimum output or deadline. Price "
                "is checked before signing only; token composition can change before mining."
            )
        steps: list[dict[str, Any]] = []
        record_extra: dict[str, Any] = {}

        if action == "add":
            if amount0_raw == 0 and amount1_raw == 0:
                raise ActionError("at least one add amount must be positive")
            sqrt_lower = sqrt_ratio_at_tick(lower)
            sqrt_upper = sqrt_ratio_at_tick(upper)
            scale = 10_000 + slippage_bps
            budget0 = amount0_raw * 10_000 // scale
            budget1 = amount1_raw * 10_000 // scale
            liquidity = _liquidity_for_budgets(
                pool.sqrt_price_x96, sqrt_lower, sqrt_upper, budget0, budget1
            )
            if liquidity == 0:
                raise ActionError(
                    "amounts are too small, or the token required at the current range side has a zero budget"
                )
            quoted0, quoted1 = _amounts_for_liquidity(
                pool.sqrt_price_x96, sqrt_lower, sqrt_upper, liquidity
            )
            # User-entered maxima are stable across approval receipts.  Deriving
            # allowances from the spot quote would change them after every
            # price move and trap the wallet in an approval/re-simulation loop.
            # A leg unused at this quote is forced to zero so crossing the range
            # cannot silently activate a previously broad allowance.
            ceiling0 = amount0_raw if quoted0 else 0
            ceiling1 = amount1_raw if quoted1 else 0
            if quoted0 > ceiling0 or quoted1 > ceiling1:
                raise ActionError("integer rounding leaves no liquidity inside the requested spend ceilings")
            balances = (
                self._balance(pool.token0, owner, block_tag),
                self._balance(pool.token1, owner, block_tag),
            )
            ceilings = (ceiling0, ceiling1)
            summary.update(
                amount0=_format_units(quoted0, pool.decimals0),
                amount1=_format_units(quoted1, pool.decimals1),
                liquidity=str(liquidity),
                share_pct=(
                    round(liquidity / (pool.active_liquidity + liquidity) * 100, 8)
                    if lower <= pool.tick < upper and pool.active_liquidity + liquidity
                    else 0.0
                ),
            )
            if any(balance < ceiling for balance, ceiling in zip(balances, ceilings)):
                missing_tokens = [
                    f"token{index} balance {balance} is below spend ceiling {ceiling}"
                    for index, (balance, ceiling) in enumerate(zip(balances, ceilings))
                    if balance < ceiling
                ]
                warnings.extend(missing_tokens)
            else:
                allowances = (
                    self._allowance(pool.token0, owner, block_tag),
                    self._allowance(pool.token1, owner, block_tag),
                )
                for index, (token, allowance, ceiling) in enumerate(
                    zip((pool.token0, pool.token1), allowances, ceilings)
                ):
                    if allowance != ceiling:
                        steps.append(
                            self._approval_step(token, owner, ceiling, index, block_tag)
                        )
                if steps:
                    warnings.append(
                        "Mint is intentionally withheld until every router allowance equals its bounded ceiling; approve, wait for receipts, then re-simulate"
                    )
                else:
                    mint_tx = _tx(
                        owner,
                        MINI_ROUTER2,
                        _calldata(
                            SEL_MINT,
                            _word_address(pool.address),
                            _word_int(lower),
                            _word_int(upper),
                            _word_uint(liquidity),
                        ),
                    )
                    simulation, _ = self._simulate_transaction(mint_tx, block_tag)
                    steps.append(
                        self._step(
                            "mint",
                            "add",
                            "Mint EOA-owned V3 liquidity through verified MiniRouter2 within exact token allowance ceilings",
                            mint_tx,
                            simulation,
                        )
                    )
            record_extra.update(
                liquidity=liquidity,
                ceilings=[ceiling0, ceiling1],
                quoted_amounts=[quoted0, quoted1],
            )
        else:
            position = self._position(pool, owner, lower, upper, block_tag)
            fees0, fees1 = self._uncollected_fees(
                pool, position, lower, upper, block_tag
            )
            collect_tx = _tx(
                owner,
                pool.address,
                _calldata(
                    SEL_COLLECT,
                    _word_address(owner),
                    _word_int(lower),
                    _word_int(upper),
                    _word_uint(MAX_UINT128),
                    _word_uint(MAX_UINT128),
                ),
            )
            if action == "remove":
                if position.liquidity == 0:
                    raise ActionError("owner has no liquidity in this exact pool range")
                liquidity = position.liquidity * liquidity_bps // 10_000
                if liquidity == 0:
                    raise ActionError("liquidity_bps rounds to zero raw liquidity")
                burn_tx = _tx(
                    owner,
                    pool.address,
                    _calldata(
                        SEL_BURN, _word_int(lower), _word_int(upper), _word_uint(liquidity)
                    ),
                )
                burn_sim, burn_result = self._simulate_transaction(burn_tx, block_tag)
                burn0 = burn1 = 0
                if burn_sim["success"] and burn_result is not None:
                    burn_words = _words(burn_result, "burn result", 2)
                    burn0, burn1 = burn_words[0], burn_words[1]
                collect_sim, _ = self._simulate_transaction(collect_tx, block_tag)
                steps.extend(
                    (
                        self._step(
                            "burn",
                            "remove",
                            "Burn EOA-owned liquidity directly on the pool; this also realizes lazy fees into the position",
                            burn_tx,
                            burn_sim,
                        ),
                        self._step(
                            "collect",
                            "collect",
                            "After burn is mined, collect all credited principal and fees directly to the owner",
                            collect_tx,
                            collect_sim,
                        ),
                    )
                )
                summary.update(
                    amount0=_format_units(burn0 + fees0, pool.decimals0),
                    amount1=_format_units(burn1 + fees1, pool.decimals1),
                    liquidity=str(liquidity),
                    share_pct=round(liquidity / position.liquidity * 100, 8),
                )
                record_extra.update(
                    liquidity=liquidity, ceilings=[0, 0], requires_poke=False
                )
            else:
                if fees0 == 0 and fees1 == 0:
                    raise ActionError(
                        "this exact owner range has no accrued or credited fees to collect"
                    )
                requires_poke = position.liquidity > 0
                if requires_poke:
                    poke_tx = _tx(
                        owner,
                        pool.address,
                        _calldata(
                            SEL_BURN,
                            _word_int(lower),
                            _word_int(upper),
                            _word_uint(0),
                        ),
                    )
                    poke_sim, _ = self._simulate_transaction(poke_tx, block_tag)
                    steps.append(
                        self._step(
                            "poke",
                            "poke",
                            "Realize lazy V3 fee growth with a zero-liquidity burn; position liquidity is unchanged",
                            poke_tx,
                            poke_sim,
                        )
                    )
                collect_sim, _ = self._simulate_transaction(collect_tx, block_tag)
                steps.append(
                    self._step(
                        "collect",
                        "collect",
                        (
                            "After poke is mined, collect all credited token0/token1 directly to the owner"
                            if requires_poke
                            else "Collect all credited token0/token1 directly to the owner"
                        ),
                        collect_tx,
                        collect_sim,
                    )
                )
                summary.update(
                    amount0=_format_units(fees0, pool.decimals0),
                    amount1=_format_units(fees1, pool.decimals1),
                )
                record_extra.update(
                    liquidity=0, ceilings=[0, 0], requires_poke=requires_poke
                )

        failed = [step for step in steps if not step["simulation"]["success"]]
        if failed:
            warnings.extend(
                f"{step['id']} preflight failed: {step['simulation']['error']}" for step in failed
            )
        summary["gas_estimate"] = (
            sum(step["simulation"]["gas_estimate"] for step in steps if step["simulation"]["gas_estimate"] is not None)
            or None
        )
        operation_ids = {
            "add": {"mint"},
            "remove": {"burn", "collect"},
            "collect": (
                {"poke", "collect"} if record_extra.get("requires_poke") else {"collect"}
            ),
        }[action]
        ready = operation_ids.issubset({step["id"] for step in steps}) and not failed

        expires_at = int(time.time()) + QUOTE_TTL_S
        binding = {
            "chain_id": CHAIN_ID,
            "owner": owner,
            "pool": pool.address,
            "action": action,
            "tick_lower": lower,
            "tick_upper": upper,
            "amount0": amount0_text,
            "amount1": amount1_text,
            "amount0_raw": str(amount0_raw),
            "amount1_raw": str(amount1_raw),
            "liquidity_bps": liquidity_bps,
            "slippage_bps": slippage_bps,
            "block": block,
            "block_hash": header["hash"],
            "expires_at": expires_at,
            "pool_metadata": self._metadata(pool),
            "sqrt_price_x96": str(pool.sqrt_price_x96),
            **record_extra,
        }
        canonical = json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
        binding_hash = hashlib.sha256(canonical).hexdigest()
        simulation_id = hashlib.sha256(os.urandom(32) + canonical).hexdigest()
        record = {
            "binding": binding,
            "binding_hash": binding_hash,
            "steps": {step["id"]: step for step in steps},
        }
        now = int(time.time())
        with self._lock:
            expired = [
                key
                for key, value in self._quotes.items()
                if value["binding"]["expires_at"] <= now
            ]
            for key in expired:
                self._quotes.pop(key, None)
            while len(self._quotes) >= MAX_QUOTES:
                self._quotes.pop(next(iter(self._quotes)))
            self._quotes[simulation_id] = record

        return {
            "simulation_id": simulation_id,
            "expires_at": expires_at,
            "block": block,
            "chain_id": CHAIN_ID,
            "owner": owner,
            "action": action,
            "ready": ready,
            "summary": summary,
            "warnings": warnings,
            "steps": steps,
        }

    def _verify_record(self, simulation_id: str, owner: str, step_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        if not isinstance(simulation_id, str) or re.fullmatch(r"[0-9a-f]{64}", simulation_id) is None:
            raise ActionError("simulation_id is invalid")
        if not isinstance(step_id, str) or not step_id:
            raise ActionError("step_id is required")
        with self._lock:
            record = self._quotes.get(simulation_id)
        if record is None:
            raise ActionError("simulation is unknown; rebuild the quote")
        binding = record["binding"]
        if int(time.time()) >= binding["expires_at"]:
            with self._lock:
                self._quotes.pop(simulation_id, None)
            raise ActionError("simulation expired; rebuild the quote")
        if owner != binding["owner"]:
            raise ActionError("connected owner does not match the simulation owner")
        canonical = json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
        if hashlib.sha256(canonical).hexdigest() != record["binding_hash"]:
            raise RuntimeError("stored simulation integrity check failed")
        step = record["steps"].get(step_id)
        if step is None:
            raise ActionError("step_id is not part of this simulation; rebuild after any approval")
        if not step["simulation"]["success"]:
            raise ActionError(
                f"step {step_id} was not executable in the quote: {step['simulation']['error']}"
            )
        return record, step

    def prepare(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ActionError("prepare payload must be an object")
        missing = self._PREPARE_FIELDS - payload.keys()
        extra = payload.keys() - self._PREPARE_FIELDS
        if missing:
            raise ActionError("missing prepare fields: " + ", ".join(sorted(missing)))
        if extra:
            raise ActionError("unexpected prepare fields: " + ", ".join(sorted(extra)))
        owner = _address(payload["owner"], "owner")
        record, step = self._verify_record(payload["simulation_id"], owner, payload["step_id"])
        binding = record["binding"]

        self._chain_id()
        quoted_header = self._header(hex(binding["block"]))
        if quoted_header["hash"] != binding["block_hash"]:
            raise ActionError("quoted block was reorganized; rebuild the quote")
        latest = self._header("latest")
        latest_tag = hex(latest["number"])
        pool = self._load_pool(
            binding["pool"], latest_tag, require_router=binding["action"] == "add"
        )
        if self._metadata(pool) != binding["pool_metadata"]:
            raise ActionError("pool metadata changed since simulation; rebuild the quote")

        step_id = step["id"]
        if step_id.startswith("approve-token"):
            index = int(step_id[-1])
            if index not in (0, 1):
                raise RuntimeError("stored approval step is malformed")
            token = (pool.token0, pool.token1)[index]
            ceiling = binding["ceilings"][index]
            expected = _tx(
                owner,
                token,
                _calldata(SEL_APPROVE, _word_address(MINI_ROUTER2), _word_uint(ceiling)),
            )
            if step["transaction"] != expected:
                raise RuntimeError("stored approval transaction is malformed")
        else:
            if step_id in ("mint", "burn") and not _price_within_bps(
                int(binding["sqrt_price_x96"]), pool.sqrt_price_x96, binding["slippage_bps"]
            ):
                raise ActionError("pool price moved beyond slippage_bps; rebuild the quote")
            lower, upper = binding["tick_lower"], binding["tick_upper"]
            self._validate_range(lower, upper, pool.tick_spacing)
            if step_id == "mint":
                if binding["action"] != "add":
                    raise RuntimeError("stored mint action is malformed")
                expected = _tx(
                    owner,
                    MINI_ROUTER2,
                    _calldata(
                        SEL_MINT,
                        _word_address(pool.address),
                        _word_int(lower),
                        _word_int(upper),
                        _word_uint(binding["liquidity"]),
                    ),
                )
                ceilings = tuple(binding["ceilings"])
                allowances = (
                    self._allowance(pool.token0, owner, latest_tag),
                    self._allowance(pool.token1, owner, latest_tag),
                )
                if allowances != ceilings:
                    raise ActionError(
                        "router allowances no longer equal the exact spend ceilings; rebuild the quote"
                    )
                balances = (
                    self._balance(pool.token0, owner, latest_tag),
                    self._balance(pool.token1, owner, latest_tag),
                )
                if any(balance < ceiling for balance, ceiling in zip(balances, ceilings)):
                    raise ActionError("token balance fell below the quoted spend ceiling")
                current_amounts = _amounts_for_liquidity(
                    pool.sqrt_price_x96,
                    sqrt_ratio_at_tick(lower),
                    sqrt_ratio_at_tick(upper),
                    binding["liquidity"],
                )
                if any(amount > ceiling for amount, ceiling in zip(current_amounts, ceilings)):
                    raise ActionError("current mint amounts exceed the exact spend ceilings")
            elif step_id == "burn":
                if binding["action"] != "remove":
                    raise RuntimeError("stored burn action is malformed")
                expected = _tx(
                    owner,
                    pool.address,
                    _calldata(
                        SEL_BURN,
                        _word_int(lower),
                        _word_int(upper),
                        _word_uint(binding["liquidity"]),
                    ),
                )
                position = self._position(pool, owner, lower, upper, latest_tag)
                if position.liquidity < binding["liquidity"]:
                    raise ActionError(
                        "owner position liquidity is now below the quoted burn amount; rebuild the quote"
                    )
            elif step_id == "poke":
                if binding["action"] != "collect" or not binding.get("requires_poke"):
                    raise RuntimeError("stored poke action is malformed")
                expected = _tx(
                    owner,
                    pool.address,
                    _calldata(
                        SEL_BURN,
                        _word_int(lower),
                        _word_int(upper),
                        _word_uint(0),
                    ),
                )
            elif step_id == "collect":
                if binding["action"] not in ("remove", "collect"):
                    raise RuntimeError("stored collect action is malformed")
                expected = _tx(
                    owner,
                    pool.address,
                    _calldata(
                        SEL_COLLECT,
                        _word_address(owner),
                        _word_int(lower),
                        _word_int(upper),
                        _word_uint(MAX_UINT128),
                        _word_uint(MAX_UINT128),
                    ),
                )
            else:
                raise RuntimeError("stored operation step is malformed")
            if step["transaction"] != expected:
                raise RuntimeError("stored operation transaction is malformed")

        simulation, _ = self._simulate_transaction(step["transaction"], latest_tag)
        if not simulation["success"]:
            raise ActionError(
                f"fresh {step_id} preflight failed: {simulation['error']}; rebuild the quote"
            )
        return {
            "transaction": dict(step["transaction"]),
            "simulation": {
                "success": True,
                "gas_estimate": simulation["gas_estimate"],
            },
            "expires_at": binding["expires_at"],
        }
